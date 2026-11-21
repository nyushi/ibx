package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
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

func stageRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("", "s1-")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("IBX_ROOT", root)
	t.Setenv("IBX_SOCKET", filepath.Join(root, "ibx.sock"))
	t.Setenv("IBX_BOX", "")
	t.Setenv("IBX_NAME", "alice")
	t.Setenv("IBX_AUTOSTART", "1")
	t.Cleanup(func() { exec.Command(binary, "daemon", "stop").Run(); os.RemoveAll(root) })
	return root
}

func stageRun(t *testing.T, code int, args ...string) string {
	t.Helper()
	c := exec.Command(binary, args...)
	c.Stdin = strings.NewReader("body")
	out, err := c.CombinedOutput()
	got := 0
	if err != nil {
		if e, ok := err.(*exec.ExitError); ok {
			got = e.ExitCode()
		} else {
			t.Fatal(err)
		}
	}
	if got != code {
		t.Fatalf("%v: got %d, want %d: %s", args, got, code, out)
	}
	return string(out)
}

func TestStageLifecycle(t *testing.T) {
	root := stageRoot(t)
	t.Setenv("IBX_AUTOSTART", "0")
	if out := stageRun(t, 1, "send", "case/x/box"); !strings.Contains(out, "IBX_AUTOSTART=0") {
		t.Fatal(out)
	}
	stageRun(t, 1, "status")
	t.Setenv("IBX_AUTOSTART", "1")
	stageRun(t, 0, "send", "case/x/box", "body")
	stageRun(t, 0, "--box", "case/x/box", "own", "claim")
	stageRun(t, 0, "--box", "case/x/box", "take", "--hold")
	stageRun(t, 0, "put", "case/x/box", "second")
	if out := stageRun(t, 0, "status"); !strings.Contains(out, "boxes: 1") {
		t.Fatal(out)
	}
	conn, err := net.Dial("unix", filepath.Join(root, "ibx.sock"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := conn.(*net.UnixConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var peer *syscall.Ucred
	if err := raw.Control(func(fd uintptr) {
		peer, err = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if err != nil || peer == nil {
		t.Fatal(err)
	}
	if err = syscall.Kill(int(peer.Pid), syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		live, e := net.Dial("unix", filepath.Join(root, "ibx.sock"))
		if e != nil {
			break
		}
		live.Close()
		if time.Now().After(deadline) {
			t.Fatal("killed daemon still answers")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if out := stageRun(t, 0, "--box", "case/x/box", "own", "show"); !strings.Contains(out, "alice") {
		t.Fatal(out)
	}
	if out := stageRun(t, 0, "--box", "case/x/box", "list", "--holding"); !strings.Contains(out, "body") {
		t.Fatal(out)
	}
	if out := stageRun(t, 0, "--box", "case/x/box", "take"); !strings.Contains(out, "second") {
		t.Fatal(out)
	}
	eq(t, stageRun(t, 0, "--box", "case/x/box", "take"), "")
	stageRun(t, 0, "daemon", "restart")
	stageRun(t, 0, "status")
	stageRun(t, 0, "daemon", "stop")
	stageRun(t, 1, "status")
}

func TestStageVersion(t *testing.T) {
	root := stageRoot(t)
	stageRun(t, 0, "put", "a/box", "body")
	conn, err := net.Dial("unix", filepath.Join(root, "ibx.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	fmt.Fprintln(conn, `{"version":"incompatible","protocol":1}`)
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil || !strings.Contains(line, "mismatch") {
		t.Fatalf("%q %v", line, err)
	}
	other := filepath.Join(root, "other")
	if out, err := exec.Command("go", "build", "-ldflags", "-X main.ibxVersion=incompatible", "-o", other, ".").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	out, err := exec.Command(other, "status").CombinedOutput()
	if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 || !strings.Contains(string(out), "mismatch") {
		t.Fatalf("%v: %s", err, out)
	}
}

func TestStageOwners(t *testing.T) {
	root := stageRoot(t)
	for _, b := range []string{"a/box", "b/box", "c/box"} {
		stageRun(t, 0, "put", b, "body")
		stageRun(t, 0, "--box", b, "own", "claim")
	}
	out := stageRun(t, 0, "own", "list", "--json")
	if strings.Count(out, "\n") != 3 || !strings.Contains(out, `"unread":1`) {
		t.Fatal(out)
	}
	p := filepath.Join(root, "b/box/state/generation")
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(p, 0700); err != nil {
		t.Fatal(err)
	}
	out = stageRun(t, 1, "own", "pass-all", "--to", "bob")
	if !strings.Contains(out, "a/box\tok") || !strings.Contains(out, "c/box\tok") || !strings.Contains(out, "b/box\t") {
		t.Fatal(out)
	}
	if out := stageRun(t, 0, "own", "list"); !strings.Contains(out, "b/box\t1") {
		t.Fatal(out)
	}
	stageRun(t, 0, "own", "drop-all", "--as", "bob")
	eq(t, stageRun(t, 0, "own", "list", "--as", "bob"), "")
}

func TestStageConcurrentClaim(t *testing.T) {
	root := stageRoot(t)
	var wg sync.WaitGroup
	codes := make(chan int, 2)
	for _, name := range []string{"one", "two"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := exec.Command(binary, "--box", "box", "own", "claim", "--as", name)
			cmd.Dir = root
			err := cmd.Run()
			if err == nil {
				codes <- 0
			} else if e, ok := err.(*exec.ExitError); ok {
				codes <- e.ExitCode()
			} else {
				codes <- -1
			}
		}()
	}
	wg.Wait()
	a, b := <-codes, <-codes
	if !((a == 0 && b == 5) || (a == 5 && b == 0)) {
		t.Fatalf("claim codes: %d, %d", a, b)
	}
}

func TestStageNotify(t *testing.T) {
	root := stageRoot(t)
	config := `{"sweep_every":"30ms","sweep_after":"0s","boxes":{"a/box":{"notify_cmd":"exit 1","fallback":"b/box"},"b/box":{"notify_cmd":"exit 2","fallback":"c/box"},"retry/box":{"notify_cmd":"if test -f \"$IBX_BOX/ran\"; then exit 0; fi; touch \"$IBX_BOX/ran\"; exit 75","retry_after":"30ms","sweep_every":"1h","sweep_after":"1h"},"empty/box":{"notify_cmd":"touch \"$IBX_BOX/unexpected\""}}}`
	if err := os.WriteFile(filepath.Join(root, "ibx.json"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	stageRun(t, 0, "--box", "empty/box", "list")
	stageRun(t, 3, "send", "a/box", "first")
	stageRun(t, 3, "send", "a/box", "second")
	stageRun(t, 75, "send", "retry/box", "retry")
	deadline := time.Now().Add(3 * time.Second)
	for {
		out := stageRun(t, 0, "log", "notify")
		if strings.Contains(out, `"exit_code":0`) && strings.Contains(out, `"exit_code":2`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(out)
		}
		time.Sleep(20 * time.Millisecond)
	}
	eq(t, count(t, filepath.Join(root, "b/box/pending")), 1)
	for _, p := range []string{"c/box/pending", "empty/box/unexpected"} {
		if _, err := os.Stat(filepath.Join(root, p)); !os.IsNotExist(err) {
			t.Fatalf("%s: %v", p, err)
		}
	}
	stageRun(t, 0, "daemon", "stop")
	out := stageRun(t, 0, "log", "notify", "--box", "retry/box")
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		eq(t, row["box"], filepath.Join(root, "retry/box"))
		eq(t, row["pending"], float64(1))
		if _, ok := row["duration_ms"]; !ok {
			t.Fatal(row)
		}
	}
}

func TestStageNotifySendWaitsForAttempt(t *testing.T) {
	const helperRoot = "IBX_TEST_NOTIFY_WAIT_ROOT"
	if root := os.Getenv(helperRoot); root != "" {
		call := localInvocation()
		o, err := parse([]string{"--root", root, "send", "wait/box", "--id", "waiting", "--notify-cmd", "exit 1", "body"}, call)
		if err != nil {
			os.Exit(parseResult(call, err))
		}
		os.Exit(commandResult(o))
	}

	root := t.TempDir()
	box := filepath.Join(root, "wait/box")
	if err := setup(box, false); err != nil {
		t.Fatal(err)
	}
	lock, err := os.OpenFile(filepath.Join(box, "state", "notify.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStageNotifySendWaitsForAttempt$")
	cmd.Env = append(os.Environ(), helperRoot+"="+root)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// The send prints its ID after publishing and before attempting notification.
	line, readErr := bufio.NewReader(stdout).ReadString('\n')
	time.Sleep(100 * time.Millisecond)
	unlockErr := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if unlockErr != nil {
		cancel()
	}
	waitErr := cmd.Wait()
	if unlockErr != nil {
		t.Fatal(unlockErr)
	}
	if readErr != nil || line != "waiting\n" {
		t.Fatalf("send publication: got %q, %v, stderr: %s", line, readErr, &stderr)
	}
	if exit, ok := waitErr.(*exec.ExitError); !ok || exit.ExitCode() != 3 {
		t.Fatalf("send: want exit 3, got %v, stderr: %s", waitErr, &stderr)
	}

	log, err := os.Open(filepath.Join(root, "notify.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	var row struct {
		Box      string `json:"box"`
		Pending  int    `json:"pending"`
		ExitCode int    `json:"exit_code"`
	}
	if err := json.NewDecoder(log).Decode(&row); err != nil {
		t.Fatal(err)
	}
	if row.Box != box || row.Pending != 1 || row.ExitCode != 1 {
		t.Fatalf("unexpected notification log entry: %+v", row)
	}
}

func TestStagePeerUID(t *testing.T) {
	if os.Getenv("IBX_UID_PROBE") == "1" {
		c, err := net.Dial("unix", "ibx.sock")
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		c.SetReadDeadline(time.Now().Add(time.Second))
		line, err := bufio.NewReader(c).ReadString('\n')
		if err != nil || !strings.Contains(line, "refusing peer UID 65534") {
			t.Fatalf("%q %v", line, err)
		}
		return
	}
	if os.Geteuid() != 0 {
		t.Skip("changing UID requires root")
	}
	// The probe runs as uid 65534 and Go applies the credential
	// before chdir, so the child must be able to traverse the path
	// to the root; the workspace TMPDIR does not allow that. Put
	// this test's root directly under /tmp.
	t.Setenv("TMPDIR", "/tmp")
	root := stageRoot(t)
	stageRun(t, 0, "put", "a/box", "body")
	socket := filepath.Join(root, "ibx.sock")
	info, err := os.Stat(socket)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, info.Mode().Perm(), os.FileMode(0600))
	// Relax only the test socket so that SO_PEERCRED, rather than Unix
	// filesystem permissions, is what rejects the different UID.
	for path, mode := range map[string]os.FileMode{root: 0711, socket: 0666} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "probe"), data, 0755); err != nil {
		t.Fatal(err)
	}
	// WriteFile is subject to the umask (027 here turns 0755 into
	// 0750, which the probe UID cannot execute); chmod is not.
	if err = os.Chmod(filepath.Join(root, "probe"), 0755); err != nil {
		t.Fatal(err)
	}
	// Use paths relative to the child's working directory so the probe
	// UID does not need access to the workspace's parent directories.
	c := exec.Command("./probe", "-test.run=^TestStagePeerUID$")
	c.Dir = root
	c.Env = append(os.Environ(), "IBX_UID_PROBE=1")
	c.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534}}
	out, err := c.CombinedOutput()
	if err != nil {
		t.Fatalf("different UID probe: %v: %s", err, out)
	}
}

func TestStageProtocolWithoutUnixSocket(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	daemon := daemonServer{root: root, start: time.Now(), stopping: ctx, wake: make(chan struct{}, 1)}
	run := func(code int, args ...string) (string, string) {
		t.Helper()
		client, server := net.Pipe()
		defer client.Close()
		go func() {
			defer server.Close()
			daemon.serveVerified(server, func() {})
		}()
		var out, stderr strings.Builder
		call := invocation{
			stdin: strings.NewReader("from stdin"), stdout: &out, stderr: &stderr,
			cwd: root, env: append(os.Environ(), "IBX_ROOT="+root, "IBX_NAME=alice", "IBX_NOTIFY_CMD="), ctx: ctx,
		}
		o, err := parse(args, call)
		if err != nil {
			t.Fatal(err)
		}
		got, err := exchange(client, func() error { return nil }, o, args)
		if err != nil || got != code {
			t.Fatalf("%v: %d, %v, %s", args, got, err, stderr.String())
		}
		return out.String(), stderr.String()
	}
	client, server := net.Pipe()
	go func() { defer server.Close(); daemon.serveVerified(server, func() {}) }()
	client.SetDeadline(time.Now().Add(time.Second))
	if err := json.NewEncoder(client).Encode(packet{Version: "other", Protocol: protocolVersion}); err != nil {
		client.Close()
		t.Fatal(err)
	}
	var mismatch packet
	err := json.NewDecoder(client).Decode(&mismatch)
	client.Close()
	if err != nil || !strings.Contains(mismatch.Error, "version mismatch") {
		t.Fatalf("%+v: %v", mismatch, err)
	}
	if err := os.WriteFile(filepath.Join(root, "ibx.json"), []byte(`{
 "sweep_after":"0s","sweep_every":"20ms",
 "boxes":{
  "a/box":{"notify_cmd":"exit 1","fallback":"b/box"},
  "b/box":{"notify_cmd":"exit 2","fallback":"c/box"},
  "retry/box":{"notify_cmd":"test -f \"$IBX_BOX/ran\" && exit 0; touch \"$IBX_BOX/ran\"; exit 75","retry_after":"20ms","sweep_every":"1h"},
  "empty/box":{"notify_cmd":"touch \"$IBX_BOX/unexpected\""}
 }
 }`), 0600); err != nil {
		t.Fatal(err)
	}
	run(0, "--box", "empty/box", "list")
	run(3, "send", "a/box", "first")
	run(3, "send", "a/box", "second")
	run(75, "send", "retry/box")
	run(0, "--box", "retry/box", "own", "claim")
	out, _ := run(0, "own", "list", "--json")
	if !strings.Contains(out, `"unread":1`) {
		t.Fatal(out)
	}
	run(0, "own", "pass-all", "--to", "bob")
	run(0, "own", "drop-all", "--as", "bob")
	run(0, "--box", "retry/box", "own", "claim")
	out, _ = run(0, "--box", "retry/box", "own", "run", "--", "sh", "-c", "printf 'wrapped'")
	eq(t, out, "wrapped")
	out, _ = run(0, "status")
	if !strings.Contains(out, "boxes: 4") {
		t.Fatal(out)
	}
	sweeper, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); sweepNotifications(sweeper, options{root: root, wake: daemon.wake}) }()
	defer func() { stop(); <-done }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		data, err := os.ReadFile(filepath.Join(root, "notify.log"))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), `"exit_code":0`) && strings.Contains(string(data), `"exit_code":2`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(string(data))
		}
		time.Sleep(5 * time.Millisecond)
	}
	eq(t, count(t, filepath.Join(root, "b/box/pending")), 1)
	for _, p := range []string{"c/box", "empty/box/unexpected"} {
		if _, err := os.Stat(filepath.Join(root, p)); !os.IsNotExist(err) {
			t.Fatalf("%s: %v", p, err)
		}
	}
	out, _ = run(0, "--box", "retry/box", "take")
	if !strings.Contains(out, "from stdin") {
		t.Fatal(out)
	}
	out, _ = run(0, "--box", "retry/box", "take")
	eq(t, out, "")
}
