package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// stage2Pipe is a full-duplex connection with independent read and write
// halves. Batch input can signal EOF while the daemon still sends responses.
type stage2Pipe struct {
	in, out *os.File
}

func (p stage2Pipe) Read(b []byte) (int, error)  { return p.in.Read(b) }
func (p stage2Pipe) Write(b []byte) (int, error) { return p.out.Write(b) }
func (p stage2Pipe) Close() error                { p.out.Close(); return p.in.Close() }
func (p stage2Pipe) LocalAddr() net.Addr         { return nil }
func (p stage2Pipe) RemoteAddr() net.Addr        { return nil }
func (p stage2Pipe) SetDeadline(t time.Time) error {
	if err := p.SetReadDeadline(t); err != nil {
		return err
	}
	return p.SetWriteDeadline(t)
}
func (p stage2Pipe) SetReadDeadline(t time.Time) error  { return p.in.SetReadDeadline(t) }
func (p stage2Pipe) SetWriteDeadline(t time.Time) error { return p.out.SetWriteDeadline(t) }

func (d daemonServer) stage2Connect(t *testing.T) stage2Pipe {
	t.Helper()
	in, response, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	request, out, err := os.Pipe()
	if err != nil {
		in.Close()
		response.Close()
		t.Fatal(err)
	}
	server := stage2Pipe{request, response}
	go func() { defer server.Close(); d.serveVerified(server, func() {}) }()
	return stage2Pipe{in, out}
}

func stage2Server(t *testing.T) daemonServer {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return daemonServer{root: t.TempDir(), start: time.Now(), stopping: ctx, wake: make(chan struct{}, 1)}
}

func (d daemonServer) stage2Invoke(t *testing.T, code int, body []byte, args ...string) (string, string) {
	t.Helper()
	var out, stderr strings.Builder
	call := invocation{
		stdin: strings.NewReader(string(body)), stdout: &out, stderr: &stderr,
		cwd: d.root, env: append(os.Environ(), "IBX_ROOT="+d.root, "IBX_NAME=alice", "IBX_BOX=", "IBX_NOTIFY_CMD="), ctx: d.stopping,
	}
	o, err := parse(args, call)
	got := 0
	if err != nil {
		got = parseResult(call, err)
	} else {
		client := d.stage2Connect(t)
		defer client.Close()
		got, err = exchange(client, client.out.Close, o, args)
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}
	if got != code {
		t.Fatalf("%v: code %d, want %d: %s", args, got, code, stderr.String())
	}
	return out.String(), stderr.String()
}

func (d daemonServer) stage2Run(t *testing.T, box string, code int, body []byte, args ...string) string {
	t.Helper()
	if box != "" {
		args = append([]string{"--box", box}, args...)
	}
	out, _ := d.stage2Invoke(t, code, body, args...)
	return out
}

func TestStage2Lines(t *testing.T) {
	d := stage2Server(t)
	b := "a"
	ids := d.stage2Run(t, b, 0, []byte("first\n\nsecond\n"), "send", "--lines", "--id-from-body")
	eq(t, ids, fmt.Sprintf("%x", sha256.Sum256([]byte("first")))[:16]+"\n"+
		fmt.Sprintf("%x", sha256.Sum256([]byte("second")))[:16]+"\n")
	d.stage2Run(t, b, 2, nil, "send", "--lines", "--id", "bad")
	d.stage2Run(t, b, 2, nil, "send", "--id", "bad", "--id-from-body", "body")
	eq(t, d.stage2Run(t, b, 0, []byte("first\n"), "send", "--lines", "--id-from-body"), "")
	eq(t, d.stage2Run(t, b, 0, []byte("b\tx\twith\ttabs\nc\ty\tother\n"), "send", "--lines", "--tsv"), "x\ny\n")
	out, stderr := d.stage2Invoke(t, 2, []byte("c\tz\tbefore\nbroken\nc\tw\tafter\n"), "send", "--lines", "--tsv")
	eq(t, out, "z\n")
	if !strings.Contains(stderr, "line 2:") {
		t.Fatal(stderr)
	}
	eq(t, d.stage2Run(t, "b", 0, nil, "list"), "x\twith\\ttabs\n")
	eq(t, d.stage2Run(t, "c", 0, nil, "list"), "y\tother\nz\tbefore\n")
	eq(t, d.stage2Run(t, b, 3, []byte("retry\tx\ta\nfail\ty\tb\n"), "send", "--lines", "--tsv",
		"--notify-cmd", `case "$IBX_BOX" in */retry) exit 75;; *) exit 1;; esac`), "x\ny\n")
}

func TestStage2DedupeReceiptRetention(t *testing.T) {
	d := stage2Server(t)
	root := d.root
	b := "box"
	send := func(extra ...string) string {
		return d.stage2Run(t, b, 0, nil, append([]string{"send", "--id", "item", "body"}, extra...)...)
	}
	eq(t, send(), "item\n")
	d.stage2Run(t, b, 0, nil, "take", "--hold")
	eq(t, send(), "")
	d.stage2Run(t, b, 0, nil, "ack", "item", "--note", "first\nignored")
	eq(t, send(), "")
	d.stage2Run(t, b, 0, nil, "mark", "item", "reported")
	if out := d.stage2Run(t, b, 0, nil, "list", "--done"); !strings.Contains(out, "\tfirst\t") || !strings.HasSuffix(out, "\treported\n") {
		t.Fatal(out)
	}
	if out := d.stage2Run(t, b, 0, nil, "list", "--done", "--json"); !strings.Contains(out, `"reported":true`) || strings.Contains(out, "ignored") {
		t.Fatal(out)
	}
	time.Sleep(10 * time.Millisecond)
	eq(t, send("--dedupe-ttl", "1ms"), "item\n")
	d.stage2Run(t, b, 0, nil, "take", "--hold")
	d.stage2Run(t, b, 0, nil, "ack", "item", "--note", "changed")
	if out := d.stage2Run(t, b, 0, nil, "list", "--done"); !strings.Contains(out, "\tchanged\t") || strings.Contains(out, "reported") {
		t.Fatal(out)
	}
	eq(t, d.stage2Run(t, b, 0, nil, "gc", "--keep", "1h"), "0\n")
	eq(t, d.stage2Run(t, b, 0, nil, "gc", "--keep", "0"), "1\n")
	if _, err := os.Stat(filepath.Join(root, b, "state", "meta", "item.json")); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	d.stage2Run(t, b, 1, nil, "mark", "item", "reported")
}

func TestStage2ScheduledRestart(t *testing.T) {
	root := stageRoot(t)
	b := "box"
	run(t, b, 0, nil, "send", "--id", "late", "--at", "+1100ms", "later")
	run(t, b, 0, nil, "send", "--id", "early", "--at", "+700ms", "earlier")
	run(t, b, 0, nil, "send", "--id", "middle", "--at", "+900ms", "middle")
	if out := run(t, b, 0, nil, "list", "--scheduled"); !strings.HasPrefix(out, "early\t") || !strings.Contains(out, "\tearlier\n") {
		t.Fatal(out)
	}
	eq(t, run(t, b, 0, nil, "take"), "")
	c, err := net.Dial("unix", filepath.Join(root, "ibx.sock"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := c.(*net.UnixConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var peer *syscall.Ucred
	if e := raw.Control(func(fd uintptr) {
		peer, err = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); e != nil || err != nil {
		t.Fatal(e, err)
	}
	c.Close()
	if err := syscall.Kill(int(peer.Pid), syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := net.Dial("unix", filepath.Join(root, "ibx.sock"))
		if err != nil {
			break
		}
		c.Close()
		if time.Now().After(deadline) {
			t.Fatal("daemon still alive")
		}
		time.Sleep(time.Millisecond)
	}
	eq(t, run(t, b, 0, nil, "list"), "")
	deadline = time.Now().Add(5 * time.Second)
	for {
		out := run(t, b, 0, nil, "list")
		if strings.Count(out, "\n") == 3 {
			eq(t, out, "early\tearlier\nmiddle\tmiddle\nlate\tlater\n")
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(out)
		}
		time.Sleep(20 * time.Millisecond)
	}
	eq(t, run(t, b, 0, nil, "take"), "early\tearlier\nmiddle\tmiddle\nlate\tlater\n")
	eq(t, run(t, b, 0, nil, "list", "--scheduled"), "")
	eq(t, run(t, b, 0, nil, "take"), "")
}

func TestStage2ReplyTee(t *testing.T) {
	d := stage2Server(t)
	d.stage2Run(t, "", 0, nil, "tee", "b", "--to", "c")
	d.stage2Run(t, "", 0, nil, "tee", "a", "--to", "b", "--match", "(?s)^want")
	d.stage2Run(t, "", 1, nil, "tee", "b", "--to", "a")
	d.stage2Run(t, "", 1, nil, "tee", "c", "--to", "a", "--match", "[")
	if out := d.stage2Run(t, "", 0, nil, "tee", "--list"); strings.Count(out, "\n") != 2 {
		t.Fatal(out)
	}
	d.stage2Run(t, "a", 0, nil, "send", "--id", "request", "--reply-to", "replybox", "want\nbody")
	d.stage2Run(t, "a", 0, nil, "put", "--id", "skip", "skip")
	d.stage2Run(t, "a", 0, nil, "put", "--id", "also", "want too")
	eq(t, d.stage2Run(t, "b", 0, nil, "list"), "request\twant\\nbody\nalso\twant too\n")
	eq(t, d.stage2Run(t, "c", 0, nil, "list"), "")
	d.stage2Run(t, "", 0, nil, "reply", "request", "answer")
	if out := d.stage2Run(t, "replybox", 0, nil, "list"); !strings.HasSuffix(out, "\tanswer\n") {
		t.Fatal(out)
	}
	d.stage2Run(t, "", 1, nil, "reply", "missing", "answer")
	d.stage2Run(t, "", 1, nil, "reply", "skip", "answer")
	d.stage2Run(t, "", 0, nil, "tee", "a", "--to", "b", "--remove", "--match", "(?s)^want")
}

func TestStage2BoxesArchiveHeaders(t *testing.T) {
	d := stage2Server(t)
	root := d.root
	b := "box"
	d.stage2Run(t, b, 0, nil, "own", "claim")
	d.stage2Run(t, "empty", 0, nil, "list")
	d.stage2Run(t, "", 0, nil, "archive", b)
	if out := d.stage2Run(t, "", 0, nil, "boxes"); out != "empty\t0\t\n" {
		t.Fatal(out)
	}
	eq(t, d.stage2Run(t, "", 0, nil, "own", "list"), "")
	eq(t, d.stage2Run(t, "", 0, nil, "own", "pass-all", "--to", "bob"), "")
	eq(t, d.stage2Run(t, "", 0, nil, "own", "drop-all"), "")
	if out := d.stage2Run(t, "", 0, nil, "status"); !strings.Contains(out, "boxes: 1\n") {
		t.Fatal(out)
	}
	d.stage2Run(t, b, 0, nil, "send", "--id", "first", "--notify-cmd", `touch "$IBX_BOX/hint"`, "line\nrest")
	if _, err := os.Stat(filepath.Join(root, b, "hint")); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if out := d.stage2Run(t, "", 0, nil, "boxes", "--all", "--unread", "--json"); !strings.Contains(out, `"box":"box","unread":1,"oldest_age_seconds":`) {
		t.Fatal(out)
	}
	d.stage2Run(t, "", 0, nil, "unarchive", b)
	if out := d.stage2Run(t, "", 0, nil, "boxes", "--unread"); !strings.HasPrefix(out, "box\t1\t") {
		t.Fatal(out)
	}
	eq(t, d.stage2Run(t, "", 0, nil, "own", "list"), "box\t1\n")
	client := d.stage2Connect(t)
	reader, writer := io.Pipe()
	args := []string{"--box", b, "stream", "--headers"}
	call := invocation{stdin: strings.NewReader(""), stdout: writer, stderr: io.Discard,
		env: []string{"IBX_ROOT=" + root}, cwd: root, ctx: d.stopping}
	o, err := parse(args, call)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer writer.Close()
		exchange(client, client.out.Close, o, args)
	}()
	line, err := bufio.NewReader(reader).ReadString('\n')
	reader.Close()
	client.Close()
	<-done
	if err != nil {
		t.Fatal(err)
	}
	eq(t, line, "first\tline\n")
	eq(t, d.stage2Run(t, b, 0, nil, "list"), "")
	d.stage2Run(t, b, 0, nil, "send", "--id", "second", "--notify-cmd", `touch "$IBX_BOX/hint"`, "body")
	if _, err := os.Stat(filepath.Join(root, b, "hint")); err != nil {
		t.Fatal(err)
	}
}

// This process exercises the daemon's scheduler across SIGKILL even in a
// sandbox that cannot bind its own Unix socket. The CLI integration above
// also covers killing and replacing the actual listener when available.
func TestStage2SchedulerProcess(t *testing.T) {
	root := os.Getenv("IBX_TEST_SCHEDULER_ROOT")
	if root == "" {
		return
	}
	fmt.Println("ready")
	o := options{root: root, renotify: 300, notifyTimeout: 10,
		invocation: invocation{env: os.Environ(), ctx: context.Background()}}
	sweepNotifications(o.ctx, o)
}

func startStage2Scheduler(t *testing.T, root string) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStage2SchedulerProcess$")
	cmd.Env = append(os.Environ(), "IBX_TEST_SCHEDULER_ROOT="+root)
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	stop := sync.OnceFunc(func() {
		killErr := cmd.Process.Signal(syscall.SIGKILL)
		waitErr := cmd.Wait()
		cancel()
		if killErr != nil || waitErr == nil {
			t.Errorf("kill scheduler: signal=%v wait=%v", killErr, waitErr)
		}
	})
	t.Cleanup(stop)
	if line, err := bufio.NewReader(out).ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("scheduler readiness: %q %v", line, err)
	}
	return stop
}

func TestStage2ScheduledFromDisk(t *testing.T) {
	d := stage2Server(t)
	config := `{"keep":"1h","dedupe_ttl":"1h","notify_cmd":"touch \"$IBX_BOX/hint\"","tee":[{"from":"box","to":"copy","match":"^yes"}]}`
	if err := os.WriteFile(filepath.Join(d.root, "ibx.json"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	d.stage2Run(t, "", 0, nil, "archive", "copy")
	base := time.Now().Add(900 * time.Millisecond).UTC()
	late := base.Add(40 * time.Millisecond).Format(time.RFC3339Nano)
	early := base.Format(time.RFC3339Nano)
	for _, item := range []struct{ id, at, body string }{
		{"late", late, "no"}, {"z", early, "yes first"}, {"a", early, "yes second"},
	} {
		d.stage2Run(t, "box", 0, nil, "send", "--id", item.id, "--at", item.at,
			"--notify-cmd", `touch "$IBX_BOX/hint"`, item.body)
	}
	eq(t, d.stage2Run(t, "box", 0, nil, "send", "--id", "a", "duplicate"), "")
	eq(t, d.stage2Run(t, "box", 0, nil, "take"), "")
	if out := d.stage2Run(t, "box", 0, nil, "list", "--scheduled", "--json"); !strings.HasPrefix(out, `{"id":"z"`) ||
		!strings.Contains(out, `"deliver_at":`) {
		t.Fatal(out)
	}
	kill := startStage2Scheduler(t, d.root)
	eq(t, d.stage2Run(t, "box", 0, nil, "list"), "")
	kill()
	// Restart after all messages are overdue, which tests both startup
	// catch-up ordering and persistence across an abrupt process exit.
	time.Sleep(max(0, time.Until(base.Add(40*time.Millisecond))))
	startStage2Scheduler(t, d.root)
	deadline := time.Now().Add(5 * time.Second)
	for {
		out := d.stage2Run(t, "box", 0, nil, "list")
		_, notified := os.Stat(filepath.Join(d.root, "box", "hint"))
		copy := d.stage2Run(t, "copy", 0, nil, "list")
		if strings.Count(out, "\n") == 3 && notified == nil && strings.Count(copy, "\n") == 2 {
			eq(t, out, "z\tyes first\na\tyes second\nlate\tno\n")
			eq(t, copy, "z\tyes first\na\tyes second\n")
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("primary: %q, tee: %q, notification: %v", out, copy, notified)
		}
		time.Sleep(5 * time.Millisecond)
	}
	eq(t, d.stage2Run(t, "box", 0, nil, "list", "--scheduled"), "")
	if _, err := os.Stat(filepath.Join(d.root, "copy", "hint")); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	d.stage2Run(t, "box", 0, nil, "take")
	eq(t, d.stage2Run(t, "box", 0, nil, "gc"), "0\n")
}

func TestStage2AckPublishFailure(t *testing.T) {
	d := stage2Server(t)
	d.stage2Run(t, "box", 0, nil, "send", "--id", "item", "--reply-to", "return", "body")
	d.stage2Run(t, "box", 0, nil, "take", "--hold")
	dest := filepath.Join(d.root, "box", "done", "item.msg")
	if err := os.Mkdir(dest, 0700); err != nil {
		t.Fatal(err)
	}
	d.stage2Run(t, "box", 1, nil, "ack", "item", "--note", "failed")
	json := d.stage2Run(t, "box", 0, nil, "list", "--holding", "--json")
	if strings.Contains(json, "ack_note") || !strings.Contains(json, `"reply_to":"return"`) {
		t.Fatal(json)
	}
	if err := os.Remove(dest); err != nil {
		t.Fatal(err)
	}
	d.stage2Run(t, "box", 0, nil, "ack", "item", "--note", "retried")
}
