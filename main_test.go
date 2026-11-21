package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

var binary string

func TestMain(m *testing.M) {
	if os.Getenv("IBX_UID_PROBE") == "1" || os.Getenv("IBX_TEST_SCHEDULER_ROOT") != "" {
		os.Exit(m.Run())
	}
	for _, key := range []string{"IBX_ROOT", "IBX_SOCKET", "IBX_BOX", "IBX_NAME", "IBX_AUTOSTART", "IBX_AUTORESTART", "IBX_NOTIFY_CMD"} {
		os.Unsetenv(key)
	}
	area, err := os.MkdirTemp("", "ibx-tests-")
	if err != nil {
		panic(err)
	}
	binary = filepath.Join(area, "ibx")
	c := exec.Command("go", "build", "-o", binary, ".")
	if out, err := c.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build: %v\n%s", err, out)
		os.RemoveAll(area)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(area)
	os.Exit(code)
}
func box(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp(filepath.Dir(binary), "b-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		exec.Command(binary, "--root", root, "daemon", "stop").Run()
		os.RemoveAll(root)
	})
	return filepath.Join(root, "box")
}
func command(b string, args ...string) *exec.Cmd {
	return exec.Command(binary, append([]string{"--box", b}, args...)...)
}
func run(t *testing.T, b string, code int, body []byte, args ...string) string {
	t.Helper()
	c := command(b, args...)
	c.Stdin = bytes.NewReader(body)
	var out, stderr bytes.Buffer
	c.Stdout = &out
	c.Stderr = &stderr
	err := c.Run()
	got := 0
	if err != nil {
		if e, ok := err.(*exec.ExitError); ok {
			got = e.ExitCode()
		} else {
			t.Fatal(err)
		}
	}
	if got != code {
		t.Fatalf("%v: code %d want %d: %s", args, got, code, stderr.String())
	}
	return out.String()
}
func putTest(t *testing.T, b, id string) {
	t.Helper()
	run(t, b, 0, nil, "put", "--id", id, "build finished")
}
func read(t *testing.T, p string) string {
	t.Helper()
	v, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	return string(v)
}
func count(t *testing.T, p string) int {
	t.Helper()
	v, e := os.ReadDir(p)
	if e != nil {
		t.Fatal(e)
	}
	return len(v)
}
func eq(t *testing.T, a, b any) {
	t.Helper()
	if fmt.Sprint(a) != fmt.Sprint(b) {
		t.Fatalf("got %q want %q", a, b)
	}
}
func old(t *testing.T, p string) {
	t.Helper()
	if e := os.Chtimes(p, time.Unix(1, 0), time.Unix(1, 0)); e != nil {
		t.Fatal(e)
	}
}
func TestInvalidIDs(t *testing.T) {
	b := box(t)
	for _, id := range []string{"../x", ".hidden", "", strings.Repeat("x", 129)} {
		run(t, b, 2, nil, "put", "--id", id, "body")
	}
}
func TestBodyValidation(t *testing.T) {
	b := box(t)
	run(t, b, 2, nil, "put")
	run(t, b, 2, nil, "put", "--max-size", "3", "four")
	run(t, b, 2, bytes.Repeat([]byte("x"), 1024*1024+1), "put")
}
func TestDuplicateAndHash(t *testing.T) {
	b := box(t)
	body := []byte("build finished")
	eq(t, run(t, b, 0, body, "put"), fmt.Sprintf("%x\n", sha256.Sum256(body))[:16]+"\n")
	putTest(t, b, "same")
	c := command(b, "put", "--id", "same", "changed")
	var er bytes.Buffer
	c.Stderr = &er
	out, e := c.Output()
	if e != nil {
		t.Fatal(e)
	}
	eq(t, string(out), "same\n")
	if !strings.Contains(er.String(), "duplicate") {
		t.Fatal(er.String())
	}
	eq(t, read(t, filepath.Join(b, "pending/same.msg")), string(body))
	eq(t, count(t, filepath.Join(b, "pending")), 2)
}
func TestConcurrentPut(t *testing.T) {
	b := box(t)
	cs := make([]*exec.Cmd, 20)
	outs := make([]bytes.Buffer, 20)
	for i := range cs {
		cs[i] = command(b, "put", "--id", "same", "body")
		cs[i].Stdout = &outs[i]
		if e := cs[i].Start(); e != nil {
			t.Fatal(e)
		}
	}
	for i, c := range cs {
		if e := c.Wait(); e != nil {
			t.Fatal(e)
		}
		eq(t, outs[i].String(), "same\n")
	}
	eq(t, count(t, filepath.Join(b, "pending")), 1)
}
func TestConcurrentTake(t *testing.T) {
	b := box(t)
	for i := 0; i < 200; i++ {
		putTest(t, b, fmt.Sprint("item", i))
	}
	cs := make([]*exec.Cmd, 20)
	outs := make([]bytes.Buffer, 20)
	for i := range cs {
		cs[i] = command(b, "--format", "json", "take")
		cs[i].Stdout = &outs[i]
		if e := cs[i].Start(); e != nil {
			t.Fatal(e)
		}
	}
	seen := map[string]int{}
	for i, c := range cs {
		if e := c.Wait(); e != nil {
			t.Fatal(e)
		}
		d := json.NewDecoder(&outs[i])
		for {
			var m map[string]string
			e := d.Decode(&m)
			if e == io.EOF {
				break
			}
			if e != nil {
				t.Fatal(e)
			}
			seen[m["id"]]++
		}
	}
	eq(t, len(seen), 200)
	for i := 0; i < 200; i++ {
		eq(t, seen[fmt.Sprint("item", i)], 1)
	}
}
func TestStoppedHelper(t *testing.T) {
	if os.Getenv("IBX_TEST_STOP") != "1" {
		return
	}
	linkFile = func(a, b string) error { syscall.Kill(os.Getpid(), syscall.SIGSTOP); return os.Link(a, b) }
	os.Exit(runCLI([]string{"--box", os.Getenv("IBX_TEST_BOX"), "put", "body"}))
}
func TestKilledAfterTmpWrite(t *testing.T) {
	b := box(t)
	c := exec.Command(os.Args[0], "-test.run=^TestStoppedHelper$")
	c.Env = append(os.Environ(), "IBX_TEST_STOP=1", "IBX_TEST_BOX="+b)
	if e := c.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { c.Process.Kill(); c.Wait() }()
	var status syscall.WaitStatus
	_, e := syscall.Wait4(c.Process.Pid, &status, syscall.WUNTRACED, nil)
	if e != nil || !status.Stopped() {
		t.Fatalf("not stopped: %v %v", status, e)
	}
	eq(t, count(t, filepath.Join(b, "pending")), 0)
	eq(t, count(t, filepath.Join(b, "tmp")), 1)
	eq(t, read(t, filepath.Join(b, "tmp", func() string { es, _ := os.ReadDir(filepath.Join(b, "tmp")); return es[0].Name() }())), "body")
}
func TestRoundTrips(t *testing.T) {
	b := box(t)
	body := []byte("line one\nline\ttwo\\end\r")
	for _, f := range []string{"tsv", "json"} {
		run(t, b, 0, body, "put", "--id", "item")
		out := run(t, b, 0, nil, "--format", f, "take")
		if f == "tsv" {
			eq(t, out, "item\tline one\\nline\\ttwo\\\\end\\r\n")
		} else {
			var m map[string]string
			if e := json.Unmarshal([]byte(out), &m); e != nil {
				t.Fatal(e)
			}
			eq(t, m["id"], "item")
			eq(t, m["body"], string(body))
		}
	}
}
func TestListTakeMaxSinceAndOrder(t *testing.T) {
	b := box(t)
	putTest(t, b, "second")
	putTest(t, b, "first")
	for _, id := range []string{"first", "second"} {
		old(t, filepath.Join(b, "pending", id+".msg"))
	}
	out := run(t, b, 0, nil, "list")
	if !strings.HasPrefix(out, "first\t") {
		t.Fatal(out)
	}
	eq(t, count(t, filepath.Join(b, "pending")), 2)
	eq(t, run(t, b, 0, nil, "take", "--max", "1"), "first\tbuild finished\n")
	if !strings.Contains(run(t, b, 0, nil, "list", "--done", "--since", "10m"), "first") {
		t.Fatal("missing done")
	}
	eq(t, run(t, b, 0, nil, "list", "--done", "--since", "0"), "")
	run(t, b, 0, nil, "take")
	eq(t, run(t, b, 0, nil, "take"), "")
}
func TestWaitTimeoutAndArrival(t *testing.T) {
	b := box(t)
	eq(t, run(t, b, 4, nil, "wait", "--timeout", ".05", "--poll", ".01"), "")
	c := command(b, "wait", "--timeout", "5", "--poll", ".01")
	var out bytes.Buffer
	c.Stdout = &out
	if e := c.Start(); e != nil {
		t.Fatal(e)
	}
	putTest(t, b, "item")
	if e := c.Wait(); e != nil {
		t.Fatal(e)
	}
	eq(t, out.String(), "item\tbuild finished\n")
	putTest(t, b, "ready")
	eq(t, run(t, b, 0, nil, "wait", "--timeout", "0"), "ready\tbuild finished\n")
}
func TestStream(t *testing.T) {
	b := box(t)
	c := command(b, "stream", "--poll", ".01")
	p, e := c.StdoutPipe()
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { c.Process.Kill() }()
	for _, id := range []string{"first", "second"} {
		putTest(t, b, id)
		ch := make(chan string, 1)
		go func() {
			v := make([]byte, len(id)+16)
			_, err := io.ReadFull(p, v)
			if err != nil {
				ch <- err.Error()
			} else {
				ch <- string(v)
			}
		}()
		select {
		case v := <-ch:
			eq(t, v, id+"\tbuild finished\n")
		case <-time.After(5 * time.Second):
			t.Fatal("stream stalled")
		}
	}
	p.Close()
	putTest(t, b, "third")
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not exit")
	}
}
func notification(t *testing.T, b, id string, extra ...string) {
	t.Helper()
	args := []string{"send", "--id", id, "--notify-cmd", `printf 'hint\n' >> "$IBX_BOX/hints"`}
	args = append(args, extra...)
	run(t, b, 0, nil, append(args, "body")...)
}
func TestSendSuccessAndCoalescing(t *testing.T) {
	b := box(t)
	notification(t, b, "item")
	notification(t, b, "second")
	eq(t, read(t, filepath.Join(b, "hints")), "hint\n")
	run(t, b, 0, nil, "take")
	notification(t, b, "third")
	eq(t, read(t, filepath.Join(b, "hints")), "hint\nhint\n")
}
func TestSendFailure(t *testing.T) {
	b := box(t)
	c := command(b, "send", "--notify-cmd", "exit 1", "body")
	var er bytes.Buffer
	c.Stderr = &er
	e := c.Run()
	if e == nil || e.(*exec.ExitError).ExitCode() != 3 {
		t.Fatal(e)
	}
	if !strings.Contains(er.String(), "warning") {
		t.Fatal(er.String())
	}
	eq(t, count(t, filepath.Join(b, "pending")), 1)
	if _, e := os.Stat(filepath.Join(b, "state/notified")); !os.IsNotExist(e) {
		t.Fatal(e)
	}
}
func TestSendStale(t *testing.T) {
	b := box(t)
	notification(t, b, "item")
	old(t, filepath.Join(b, "state/notified"))
	notification(t, b, "second", "--renotify-after", "1s")
	eq(t, read(t, filepath.Join(b, "hints")), "hint\nhint\n")
}
func TestSendEnvironment(t *testing.T) {
	b := box(t)
	secret := "private body text"
	run(t, b, 0, nil, "send", "--id", "item", "--notify-cmd", `env > "$IBX_BOX/environment"`, secret)
	env := read(t, filepath.Join(b, "environment"))
	if !strings.Contains(env, "IBX_FIRST_LINE="+secret+"\n") {
		t.Fatal("missing first-line metadata")
	}
	for _, line := range strings.Split(env, "\n") {
		if !strings.HasPrefix(line, "IBX_FIRST_LINE=") && strings.Contains(line, secret) {
			t.Fatal("body leaked outside IBX_FIRST_LINE")
		}
	}
	for _, v := range []string{"IBX_ID=item\n", "IBX_PENDING=1\n", "IBX_BOX=" + b + "\n"} {
		if !strings.Contains(env, v) {
			t.Fatalf("missing %s", v)
		}
	}
}
func TestSendTimeoutAndNoCommand(t *testing.T) {
	b := box(t)
	run(t, b, 3, nil, "send", "--notify-cmd", "sleep 1", "--notify-timeout", ".02", "body")
	run(t, b, 0, nil, "send", "--notify-cmd", "", "other")
	eq(t, count(t, filepath.Join(b, "pending")), 2)
}
func TestGC(t *testing.T) {
	b := box(t)
	putTest(t, b, "stale")
	old(t, filepath.Join(b, "pending/stale.msg"))
	run(t, b, 0, nil, "take")
	putTest(t, b, "pending")
	for _, n := range []string{"done/old.msg", "tmp/old", "tmp/new", "state/other"} {
		if e := os.WriteFile(filepath.Join(b, n), []byte("body"), 0600); e != nil {
			t.Fatal(e)
		}
	}
	for _, n := range []string{"tmp/old", "pending/pending.msg", "state/other"} {
		old(t, filepath.Join(b, n))
	}
	eq(t, run(t, b, 0, nil, "gc", "--keep", "1h"), "1\n")
	for _, n := range []string{"done/stale.msg", "done/old.msg", "tmp/new", "pending/pending.msg", "state/other"} {
		read(t, filepath.Join(b, n))
	}
	if _, e := os.Stat(filepath.Join(b, "tmp/old")); !os.IsNotExist(e) {
		t.Fatal("old tmp survived")
	}
	eq(t, run(t, b, 0, nil, "gc", "--keep", "0"), "2\n")
	eq(t, count(t, filepath.Join(b, "done")), 0)
	for _, n := range []string{"tmp/new", "pending/pending.msg", "state/other"} {
		read(t, filepath.Join(b, n))
	}
}
func TestPermissionsBytesAndReuse(t *testing.T) {
	b := box(t)
	run(t, b, 0, []byte("raw\xff"), "put", "--id", "item")
	eq(t, read(t, filepath.Join(b, "pending/item.msg")), "raw\xff")
	for _, n := range []string{"", "tmp", "pending", "done", "state", "pending/item.msg"} {
		s, e := os.Stat(filepath.Join(b, n))
		if e != nil {
			t.Fatal(e)
		}
		want := os.FileMode(0700)
		if n == "pending/item.msg" {
			want = 0600
		}
		eq(t, s.Mode().Perm(), want)
	}
	if !strings.Contains(run(t, b, 0, nil, "take"), "\ufffd") {
		t.Fatal("missing replacement")
	}
	run(t, b, 0, []byte("again"), "put", "--id", "item")
	eq(t, run(t, b, 0, nil, "take"), "item\tagain\n")
}
func TestMissingBox(t *testing.T) {
	c := exec.Command(binary, "list")
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "IBX_BOX=") {
			c.Env = append(c.Env, v)
		}
	}
	e := c.Run()
	if e == nil || e.(*exec.ExitError).ExitCode() != 2 {
		t.Fatal(e)
	}
}
func TestUsage(t *testing.T) {
	b := box(t)
	for _, args := range [][]string{{"list", "--since", "1h"}, {"take", "--max", "0"}, {"put", "--max-size", "-1", "x"}, {"wait", "--poll", "NaN"}, {"gc", "--keep", "-1"}, {"list", "--format", "xml"}, {"take", "--id", "x"}, {"unknown"}} {
		run(t, b, 2, nil, args...)
	}
}

func TestUnicodeOutputCompatibility(t *testing.T) {
	b := box(t)
	body := []byte("café 😀 <>&\x00\xe2\x82!")
	run(t, b, 0, body, "put", "--id", "unicode")
	eq(t, run(t, b, 0, nil, "list", "--format", "tsv"), "unicode\tcafé 😀 <>&\x00�!\n")
	eq(t, run(t, b, 0, nil, "list", "--format", "json"), "{\"id\": \"unicode\", \"body\": \"caf\\u00e9 \\ud83d\\ude00 <>&\\u0000\\ufffd!\"}\n")
}
