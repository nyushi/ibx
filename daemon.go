package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var ibxVersion = "2.0.0"

const protocolVersion = 1
const startupTimeout = 5 * time.Second
const restartLockEnv = "_IBX_RESTART_LOCK_FD"

type packet struct {
	Version  string   `json:"version,omitempty"`
	Protocol int      `json:"protocol,omitempty"`
	Args     []string `json:"args,omitempty"`
	Env      []string `json:"env,omitempty"`
	Cwd      string   `json:"cwd,omitempty"`
	Data     []byte   `json:"data,omitempty"`
	Kind     string   `json:"kind,omitempty"`
	Error    string   `json:"error,omitempty"`
	Code     int      `json:"code,omitempty"`
}

func localInvocation() invocation {
	cwd, _ := os.Getwd()
	return invocation{os.Stdin, os.Stdout, os.Stderr, os.Environ(), cwd, context.Background()}
}

func bodyLimit(maxSize int) int64 {
	limit := int64(maxSize)
	if limit < int64(^uint64(0)>>1) {
		limit++
	}
	return limit
}

func commandPath(o options, name string) (string, error) {
	if strings.Contains(name, "/") {
		return absolute(o.cwd, name), nil
	}
	for _, dir := range filepath.SplitList(o.getenv("PATH")) {
		path := absolute(absolute(o.cwd, dir), name)
		if st, err := os.Stat(path); err == nil && st.Mode().IsRegular() && st.Mode().Perm()&0111 != 0 {
			if !filepath.IsAbs(dir) {
				return "", fmt.Errorf("%s: %w", name, exec.ErrDot)
			}
			return path, nil
		}
	}
	return "", fmt.Errorf("%s: %w", name, exec.ErrNotFound)
}

func socketPath(o options) string {
	if path := o.getenv("IBX_SOCKET"); path != "" {
		return absolute(o.cwd, path)
	}
	return filepath.Join(o.root, "ibx.sock")
}

func peerUID(c *net.UnixConn) (uint32, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, err
	}
	var cred *syscall.Ucred
	var controlErr error
	if err = raw.Control(func(fd uintptr) {
		cred, controlErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return 0, err
	}
	if controlErr != nil {
		return 0, controlErr
	}
	return cred.Uid, nil
}

func connect(path string) (*net.UnixConn, error) {
	c, err := net.DialTimeout("unix", path, 500*time.Millisecond)
	if err != nil {
		return nil, err
	}
	conn := c.(*net.UnixConn)
	uid, err := peerUID(conn)
	if err != nil || uid != uint32(os.Geteuid()) {
		conn.Close()
		return nil, fmt.Errorf("refusing daemon peer UID %d (expected %d): %v", uid, os.Geteuid(), err)
	}
	return conn, nil
}

func launchDaemon(o options, path string) (<-chan error, error) {
	if err := os.MkdirAll(o.root, 0700); err != nil {
		return nil, err
	}
	log, err := os.OpenFile(filepath.Join(o.root, "daemon.log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	defer log.Close()
	binary, err := os.Executable()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(binary, "daemon")
	cmd.Env = daemonEnv(o, path)
	cmd.Stdout, cmd.Stderr = log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	return exited, nil
}

func daemonEnv(o options, path string) []string {
	env := make([]string, 0, len(o.env)+2)
	for _, value := range o.env {
		if !strings.HasPrefix(value, "IBX_ROOT=") && !strings.HasPrefix(value, "IBX_SOCKET=") && !strings.HasPrefix(value, restartLockEnv+"=") {
			env = append(env, value)
		}
	}
	return append(env, "IBX_ROOT="+o.root, "IBX_SOCKET="+path)
}

func waitForSocket(path string, exited <-chan error, logPath string) (*net.UnixConn, error) {
	deadline := time.Now().Add(startupTimeout)
	for {
		conn, err := connect(path)
		if err == nil || time.Now().After(deadline) {
			return conn, err
		}
		select {
		case result := <-exited:
			// A competing autostart can win the listener lock.
			exited = nil
			lock, lockErr := os.Open(path + ".lock")
			if lockErr == nil {
				lockErr = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
				lock.Close()
			}
			if !errors.Is(lockErr, syscall.EWOULDBLOCK) {
				if conn, err = connect(path); err == nil {
					return conn, nil
				}
				var diagnostic string
				if f, e := os.Open(logPath); e == nil {
					if st, e := f.Stat(); e == nil {
						f.Seek(max(0, st.Size()-2048), io.SeekStart)
					}
					b, _ := io.ReadAll(io.LimitReader(f, 2048))
					f.Close()
					lines := strings.Split(strings.TrimSpace(string(b)), "\n")
					diagnostic = lines[len(lines)-1]
				}
				return nil, fmt.Errorf("daemon exited (%v): %s", result, diagnostic)
			}
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func client(o options, args []string) (int, error) {
	path := socketPath(o)
	conn, err := connect(path)
	if err != nil {
		if o.command == "status" || o.command == "daemon" {
			return 1, fmt.Errorf("daemon is not running: %w", err)
		}
		if o.getenv("IBX_AUTOSTART") == "0" {
			return 1, fmt.Errorf("cannot connect to daemon (IBX_AUTOSTART=0): %w", err)
		}
		exited, startErr := launchDaemon(o, path)
		if startErr != nil {
			return 1, fmt.Errorf("start daemon: %w", startErr)
		}
		if conn, err = waitForSocket(path, exited, filepath.Join(o.root, "daemon.log")); err != nil {
			return 1, fmt.Errorf("daemon did not become ready (see %s): %w", filepath.Join(o.root, "daemon.log"), err)
		}
	}
	defer conn.Close()
	code, err := exchange(conn, conn.CloseWrite, o, args)
	if err == nil && code == 0 && o.command == "daemon" && o.sub == "restart" {
		ready, e := waitForSocket(path, nil, "")
		if e != nil {
			return 1, fmt.Errorf("daemon restart: %w", e)
		}
		ready.Close()
	}
	return code, err
}

func exchange(conn net.Conn, closeInput func() error, o options, args []string) (int, error) {
	var err error
	enc, dec := json.NewEncoder(conn), json.NewDecoder(conn)
	conn.SetDeadline(time.Now().Add(startupTimeout))
	if err = enc.Encode(packet{Version: ibxVersion, Protocol: protocolVersion}); err != nil {
		return 1, err
	}
	var hello packet
	if err = dec.Decode(&hello); err != nil {
		return 1, fmt.Errorf("daemon handshake: %w", err)
	}
	if hello.Error != "" {
		return 1, errors.New(hello.Error)
	}
	if hello.Version != ibxVersion || hello.Protocol != protocolVersion {
		return 1, fmt.Errorf("daemon version mismatch: client %s/%d, daemon %s/%d", ibxVersion, protocolVersion, hello.Version, hello.Protocol)
	}
	conn.SetDeadline(time.Time{})
	request := packet{Args: args, Env: o.env, Cwd: o.cwd}
	if !o.lines && (o.command == "put" || o.command == "send") && (!o.hasBody || o.body == "-") {
		request.Data, err = io.ReadAll(io.LimitReader(o.stdin, bodyLimit(o.maxSize)))
		if err != nil {
			return 1, err
		}
	}
	if err = enc.Encode(request); err != nil {
		return 1, err
	}
	rawInput := o.lines || (o.command == "own" && o.sub == "run")
	if rawInput {
		go func() { io.Copy(conn, o.stdin); closeInput() }()
	}
	for {
		var reply packet
		if err = dec.Decode(&reply); err != nil {
			return 1, fmt.Errorf("daemon disconnected: %w", err)
		}
		switch reply.Kind {
		case "exit":
			return reply.Code, nil
		case "stdout", "stderr":
			out := o.stdout
			if reply.Kind == "stderr" {
				out = o.stderr
			}
			if _, err = out.Write(reply.Data); err != nil {
				if o.command == "stream" && errors.Is(err, syscall.EPIPE) {
					return 0, nil
				}
				return 1, err
			}
			if !rawInput {
				if err = enc.Encode(packet{Kind: "ack"}); err != nil {
					return 1, err
				}
			}
		default:
			return 1, fmt.Errorf("unexpected daemon response %q", reply.Kind)
		}
	}
}

type response struct {
	enc             *json.Encoder
	acknowledgments <-chan struct{}
	ctx             context.Context
	mu              sync.Mutex
}

type responseWriter struct {
	*response
	kind string
}

func (w responseWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	select {
	case <-w.ctx.Done():
		return 0, syscall.EPIPE
	default:
	}
	if err := w.enc.Encode(packet{Kind: w.kind, Data: data}); err != nil {
		return 0, err
	}
	if w.acknowledgments != nil {
		select {
		case <-w.ctx.Done():
			return 0, syscall.EPIPE
		case <-w.acknowledgments:
		}
	}
	return len(data), nil
}

type shutdownRequest struct {
	restart       bool
	done, replied chan struct{}
}

type daemonServer struct {
	root     string
	start    time.Time
	shutdown chan shutdownRequest
	stopping context.Context
	wake     chan struct{}
	jobs     *jobScheduler
}

func (d daemonServer) serve(conn *net.UnixConn, finished func()) {
	defer func() {
		if finished != nil {
			finished()
		}
	}()
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(startupTimeout))
	enc := json.NewEncoder(conn)
	uid, err := peerUID(conn)
	if err != nil || uid != uint32(os.Geteuid()) {
		enc.Encode(packet{Error: fmt.Sprintf("refusing peer UID %d (expected %d)", uid, os.Geteuid())})
		return
	}
	d.serveVerified(conn, func() {
		finished()
		finished = nil
	})
}

func (d daemonServer) serveVerified(conn net.Conn, finishControl func()) {
	enc, input := json.NewEncoder(conn), bufio.NewReader(conn)
	var hello packet
	if err := decodeLine(input, &hello); err != nil {
		return
	}
	if hello.Version != ibxVersion || hello.Protocol != protocolVersion {
		enc.Encode(packet{Error: fmt.Sprintf("version mismatch: client %s/%d, daemon %s/%d", hello.Version, hello.Protocol, ibxVersion, protocolVersion)})
		return
	}
	if err := enc.Encode(packet{Version: ibxVersion, Protocol: protocolVersion}); err != nil {
		return
	}
	// The client may be reading stdin before it sends the request.
	conn.SetReadDeadline(time.Time{})
	var request packet
	requestReady := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-requestReady:
		case <-d.stopping.Done():
			// Waiting for a request is not in-flight command execution.
			select {
			case <-requestReady:
			default:
				conn.SetReadDeadline(time.Now())
			}
		}
	}()
	err := decodeLine(input, &request)
	close(requestReady)
	<-watchDone
	if err != nil {
		return
	}
	conn.SetDeadline(time.Time{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := &response{enc: enc, ctx: ctx}
	call := invocation{bytes.NewReader(request.Data), responseWriter{res, "stdout"}, responseWriter{res, "stderr"}, request.Env, request.Cwd, ctx}
	o, err := parse(request.Args, call)
	if err == nil && o.command == "daemon" {
		if o.sub == "" {
			enc.Encode(packet{Kind: "exit", Code: 2})
			return
		}
		req := shutdownRequest{restart: o.sub == "restart", done: make(chan struct{}), replied: make(chan struct{})}
		select {
		case d.shutdown <- req:
			finishControl()
			<-req.done
			enc.Encode(packet{Kind: "exit"})
			close(req.replied)
		case <-d.stopping.Done():
			enc.Encode(packet{Kind: "exit", Code: 1})
		}
		return
	}
	if err == nil && (o.lines || o.command == "stream" || o.command == "wait") {
		go func() {
			select {
			case <-d.stopping.Done():
				cancel()
				conn.Close()
			case <-ctx.Done():
			}
		}()
	}
	if err == nil && o.lines {
		o.stdin = input
	} else if err == nil && o.command == "own" && o.sub == "run" {
		r, w, pipeErr := os.Pipe()
		if pipeErr != nil {
			fmt.Fprintln(call.stderr, "error:", pipeErr)
			enc.Encode(packet{Kind: "exit", Code: 1})
			return
		}
		defer r.Close()
		defer w.Close()
		o.stdin = r
		go func() { io.Copy(w, input); w.Close() }()
	} else {
		ack := make(chan struct{})
		res.acknowledgments = ack
		go func() {
			defer cancel()
			for {
				var p packet
				if decodeLine(input, &p) != nil || p.Kind != "ack" {
					return
				}
				select {
				case ack <- struct{}{}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	code := 0
	if err != nil {
		code = parseResult(call, err)
	} else {
		o.root = d.root
		o.wake = d.wake
		switch o.command {
		case "job":
			if err := d.jobCommand(o); err != nil {
				var usage usageError
				if errors.As(err, &usage) {
					code = parseResult(call, err)
				} else {
					fmt.Fprintln(o.stderr, "error:", err)
					code = 1
				}
			}
		case "status":
			if err := d.status(o.stdout); err != nil {
				fmt.Fprintln(o.stderr, "error:", err)
				code = 1
			}
		case "log":
			code = 2
		default:
			code = commandResult(o)
		}
	}
	enc.Encode(packet{Kind: "exit", Code: code})
}

func (d daemonServer) status(out io.Writer) error {
	boxes, jobs := 0, 0
	if err := eachBox(d.root, func(box string) {
		if !archived(box) {
			boxes++
		}
	}); err != nil {
		return err
	}
	definitions, err := d.jobDefinitions()
	if err != nil {
		return err
	}
	jobs = len(definitions)
	_, err = fmt.Fprintf(out, "version: %s\nstarted: %s\nboxes: %d\njobs: %d\n", ibxVersion, d.start.Format(time.RFC3339), boxes, jobs)
	return err
}

func listen(o options, path string) (*net.UnixListener, *os.File, error) {
	if err := os.MkdirAll(o.root, 0700); err != nil {
		return nil, nil, err
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return nil, nil, err
	}
	if !parent.IsDir() || parent.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return nil, nil, fmt.Errorf("socket directory must be owned by UID %s", strconv.Itoa(os.Geteuid()))
	}
	lock, err := socketLock(o, path+".lock")
	if err != nil {
		return nil, nil, err
	}
	listener, err := func() (*net.UnixListener, error) {
		if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			return nil, fmt.Errorf("daemon already running or starting at %s: %w", path, err)
		}
		if st, err := os.Lstat(path); err == nil {
			if st.Mode()&os.ModeSocket == 0 {
				return nil, fmt.Errorf("socket path %s is not a socket", path)
			}
			c, dialErr := net.DialTimeout("unix", path, 500*time.Millisecond)
			if dialErr == nil {
				c.Close()
				return nil, fmt.Errorf("daemon already running at %s", path)
			}
			if !errors.Is(dialErr, syscall.ECONNREFUSED) && !errors.Is(dialErr, fs.ErrNotExist) {
				return nil, dialErr
			}
			if err := unlink(path); err != nil {
				return nil, err
			}
		} else if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		mask := syscall.Umask(0177)
		ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		syscall.Umask(mask)
		return ln, err
	}()
	if err != nil {
		lock.Close()
		return nil, nil, err
	}
	return listener, lock, nil
}

// Preserve the listener lock across exec so a simultaneous autostart
// cannot replace the restarting process between shutdown and bind.
func socketLock(o options, path string) (*os.File, error) {
	value := o.getenv(restartLockEnv)
	if value == "" {
		return os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	}
	os.Unsetenv(restartLockEnv)
	fd, err := strconv.Atoi(value)
	if err != nil || fd < 3 {
		return nil, fmt.Errorf("invalid restart socket lock descriptor")
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		return nil, fmt.Errorf("invalid restart socket lock descriptor")
	}
	syscall.CloseOnExec(fd)
	want, err := os.Lstat(path)
	if err != nil {
		file.Close()
		return nil, err
	}
	got, err := file.Stat()
	if err != nil || !os.SameFile(got, want) {
		file.Close()
		return nil, fmt.Errorf("restart socket lock descriptor does not match %s", path)
	}
	return file, nil
}

func runDaemon(o options) error {
	path := socketPath(o)
	ln, lock, err := listen(o, path)
	if err != nil {
		return err
	}
	defer lock.Close()
	defer ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := daemonServer{root: o.root, start: time.Now(), shutdown: make(chan shutdownRequest), stopping: ctx, wake: make(chan struct{}, 1)}
	d.initJobs()
	jobsDone := make(chan struct{})
	go func() {
		defer close(jobsDone)
		d.runJobs(ctx)
	}()
	o.wake = d.wake
	signalCtx, stopSignals := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()
	var requests sync.WaitGroup
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			conn, err := ln.AcceptUnix()
			if err != nil {
				return
			}
			requests.Add(1)
			go d.serve(conn, requests.Done)
		}
	}()
	swept := make(chan struct{})
	go func() { defer close(swept); sweepNotifications(ctx, o) }()
	var shutdown shutdownRequest
	select {
	case shutdown = <-d.shutdown:
	case <-signalCtx.Done():
	case <-accepted:
	}
	ln.Close()
	cancel()
	<-accepted
	<-swept
	<-jobsDone
	requests.Wait()
	if shutdown.done != nil {
		close(shutdown.done)
		<-shutdown.replied
	}
	if shutdown.restart {
		binary, err := os.Executable()
		if err != nil {
			return err
		}
		if _, _, errno := syscall.Syscall(syscall.SYS_FCNTL, lock.Fd(), syscall.F_SETFD, 0); errno != 0 {
			return errno
		}
		env := append(daemonEnv(o, path), restartLockEnv+"="+strconv.FormatUint(uint64(lock.Fd()), 10))
		return syscall.Exec(binary, []string{binary, "daemon"}, env)
	}
	return nil
}

func decodeLine(reader *bufio.Reader, p *packet) error {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	return json.Unmarshal(line, p)
}

func readNotifyLog(o options) error {
	f, err := os.Open(filepath.Join(o.root, "notify.log"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			var row struct {
				Box string `json:"box"`
			}
			if o.box == "" || (json.Unmarshal(line, &row) == nil && row.Box == o.box) {
				if _, e := o.stdout.Write(line); e != nil {
					return e
				}
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func main() {
	signal.Ignore(syscall.SIGPIPE)
	call := localInvocation()
	args := os.Args[1:]
	o, err := parse(args, call)
	if err != nil {
		os.Exit(parseResult(call, err))
	}
	code := 0
	switch {
	case o.command == "daemon" && o.sub == "":
		err = runDaemon(o)
	case o.command == "log":
		err = readNotifyLog(o)
	default:
		code, err = client(o, args)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		code = 1
	}
	os.Exit(code)
}
