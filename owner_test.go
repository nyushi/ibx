package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func runStderr(t *testing.T, b string, code int, args ...string) (string, string) {
	t.Helper()
	c := command(b, args...)
	var out, er bytes.Buffer
	c.Stdout = &out
	c.Stderr = &er
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
		t.Fatalf("%v: code %d want %d: stdout=%q stderr=%q", args, got, code, out.String(), er.String())
	}
	return out.String(), er.String()
}

func TestOwnClaimShowDrop(t *testing.T) {
	b := box(t)
	out := run(t, b, 0, nil, "own", "claim", "--as", "alice")
	if !strings.HasPrefix(out, "alice\t1\t") {
		t.Fatalf("claim output: %q", out)
	}
	if show := run(t, b, 0, nil, "own", "show"); !strings.HasPrefix(show, "alice\t1\t") {
		t.Fatalf("show output: %q", show)
	}
	// Another name is refused while alice holds the box, naming the owner.
	_, er := runStderr(t, b, 5, "own", "claim", "--as", "bob")
	if !strings.Contains(er, "alice") {
		t.Fatalf("conflict must name the owner: %q", er)
	}
	// Re-claiming as the same owner only extends; generation stays 1.
	out = run(t, b, 0, nil, "own", "claim", "--as", "alice")
	if !strings.HasPrefix(out, "alice\t1\t") {
		t.Fatalf("extend output: %q", out)
	}
	run(t, b, 5, nil, "own", "drop", "--as", "bob")
	run(t, b, 0, nil, "own", "drop", "--as", "alice")
	run(t, b, 6, nil, "own", "show")
	run(t, b, 6, nil, "own", "drop", "--as", "alice")
}

func TestOwnClaimExpiredStealAndLazyExpiry(t *testing.T) {
	b := box(t)
	run(t, b, 0, nil, "own", "claim", "--as", "alice", "--ttl", "0.3s")
	time.Sleep(500 * time.Millisecond)
	// After expiry another name may claim without --steal; generation grows.
	out := run(t, b, 0, nil, "own", "claim", "--as", "bob")
	if !strings.HasPrefix(out, "bob\t2\t") {
		t.Fatalf("post-expiry claim: %q", out)
	}
	// --steal takes the box inside the TTL.
	out = run(t, b, 0, nil, "own", "claim", "--as", "carol", "--steal")
	if !strings.HasPrefix(out, "carol\t3\t") {
		t.Fatalf("steal claim: %q", out)
	}
	run(t, b, 5, nil, "take", "--as", "alice")

	// Expiry alone does not end ownership: the expired owner still
	// operates, and that operation extends the term again.
	b2 := box(t)
	run(t, b2, 0, nil, "own", "claim", "--as", "alice", "--ttl", "0.3s")
	time.Sleep(500 * time.Millisecond)
	putTest(t, b2, "item")
	eq(t, run(t, b2, 0, nil, "take", "--as", "alice"), "item\tbuild finished\n")
	run(t, b2, 5, nil, "own", "claim", "--as", "bob")
}

func TestOwnPass(t *testing.T) {
	b := box(t)
	run(t, b, 6, nil, "own", "pass", "--as", "alice", "--to", "bob")
	run(t, b, 0, nil, "own", "claim", "--as", "alice")
	out := run(t, b, 0, nil, "own", "pass", "--as", "alice", "--to", "bob")
	if !strings.HasPrefix(out, "bob\t2\t") {
		t.Fatalf("pass output: %q", out)
	}
	if show := run(t, b, 0, nil, "own", "show"); !strings.HasPrefix(show, "bob\t2\t") {
		t.Fatalf("show output: %q", show)
	}
	putTest(t, b, "item")
	run(t, b, 5, nil, "take", "--as", "alice")
	// The old owner's own run is refused too, with no side effect.
	marker := filepath.Join(t.TempDir(), "ran")
	run(t, b, 5, nil, "own", "run", "--as", "alice", "--", "sh", "-c", "touch "+marker)
	if _, e := os.Stat(marker); !os.IsNotExist(e) {
		t.Fatal("old owner's command ran after pass")
	}
	run(t, b, 5, nil, "own", "pass", "--as", "alice", "--to", "carol")
	eq(t, run(t, b, 0, nil, "take", "--as", "bob"), "item\tbuild finished\n")
}

func TestOwnRun(t *testing.T) {
	b := box(t)
	marker := filepath.Join(t.TempDir(), "ran")
	// No owner at all: exit 6 and the command must not run.
	run(t, b, 6, nil, "own", "run", "--as", "alice", "--", "sh", "-c", "touch "+marker)
	if _, e := os.Stat(marker); !os.IsNotExist(e) {
		t.Fatal("command ran without an owner")
	}
	run(t, b, 0, nil, "own", "claim", "--as", "alice")
	run(t, b, 0, nil, "own", "run", "--as", "alice", "--", "sh", "-c", "touch "+marker)
	if _, e := os.Stat(marker); e != nil {
		t.Fatal(e)
	}
	if e := os.Remove(marker); e != nil {
		t.Fatal(e)
	}
	// A non-owner is refused with 5 and the command must not run.
	run(t, b, 5, nil, "own", "run", "--as", "bob", "--", "sh", "-c", "touch "+marker)
	if _, e := os.Stat(marker); !os.IsNotExist(e) {
		t.Fatal("command ran for a non-owner")
	}
	// The wrapped command's own exit code is passed through.
	run(t, b, 3, nil, "own", "run", "--as", "alice", "--", "sh", "-c", "exit 3")
}

func TestConcurrentClaim(t *testing.T) {
	b := box(t)
	const n = 20
	cs := make([]*exec.Cmd, n)
	for i := range cs {
		cs[i] = command(b, "own", "claim", "--as", fmt.Sprint("agent", i))
		if e := cs[i].Start(); e != nil {
			t.Fatal(e)
		}
	}
	ok, conflict := 0, 0
	for _, c := range cs {
		e := c.Wait()
		if e == nil {
			ok++
			continue
		}
		if ee, is := e.(*exec.ExitError); is && ee.ExitCode() == 5 {
			conflict++
			continue
		}
		t.Fatal(e)
	}
	eq(t, ok, 1)
	eq(t, conflict, n-1)
}

func TestTakeAsCompatibility(t *testing.T) {
	b := box(t)
	putTest(t, b, "free")
	// No owner recorded: --as behaves exactly as before.
	eq(t, run(t, b, 0, nil, "take", "--as", "anyone"), "free\tbuild finished\n")
	run(t, b, 0, nil, "own", "claim", "--as", "alice")
	putTest(t, b, "guarded")
	// Without --as the legacy behaviour is unchanged even with an owner.
	eq(t, run(t, b, 0, nil, "take"), "guarded\tbuild finished\n")
	putTest(t, b, "guarded2")
	run(t, b, 5, nil, "take", "--as", "bob")
	eq(t, count(t, filepath.Join(b, "pending")), 1)
	eq(t, run(t, b, 0, nil, "take", "--as", "alice"), "guarded2\tbuild finished\n")
}

func TestStreamEndsWhenOwnershipPasses(t *testing.T) {
	b := box(t)
	run(t, b, 0, nil, "own", "claim", "--as", "alice")
	putTest(t, b, "first")
	c := command(b, "stream", "--as", "alice", "--poll", ".01")
	p, e := c.StdoutPipe()
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { c.Process.Kill() }()
	ch := make(chan string, 1)
	go func() {
		v := make([]byte, len("first")+16)
		_, err := io.ReadFull(p, v)
		if err != nil {
			ch <- err.Error()
		} else {
			ch <- string(v)
		}
	}()
	select {
	case v := <-ch:
		eq(t, v, "first\tbuild finished\n")
	case <-time.After(5 * time.Second):
		t.Fatal("stream stalled")
	}
	run(t, b, 0, nil, "own", "pass", "--as", "alice", "--to", "bob")
	putTest(t, b, "second")
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	select {
	case e := <-done:
		ee, ok := e.(*exec.ExitError)
		if !ok || ee.ExitCode() != 5 {
			t.Fatalf("stream exit: %v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not end after ownership passed")
	}
	// The second message was not taken by the old owner's stream.
	eq(t, count(t, filepath.Join(b, "pending")), 1)
	eq(t, run(t, b, 0, nil, "take", "--as", "bob"), "second\tbuild finished\n")
}

func TestOwnUsageErrors(t *testing.T) {
	b := box(t)
	for _, args := range [][]string{
		{"own"},
		{"own", "bogus"},
		{"own", "claim"},
		{"own", "claim", "--as", "alice", "--ttl", "-1"},
		{"own", "pass", "--as", "alice"},
		{"own", "drop"},
		{"own", "run", "--as", "alice"},
		{"own", "run", "--as", "alice", "--"},
		{"own", "show", "--as", "alice"},
		{"ack"},
		{"ack", "../x"},
		{"release"},
	} {
		run(t, b, 2, nil, args...)
	}
}

func TestHoldAckRelease(t *testing.T) {
	b := box(t)
	putTest(t, b, "item")
	eq(t, run(t, b, 0, nil, "take", "--hold"), "item\tbuild finished\n")
	eq(t, count(t, filepath.Join(b, "pending")), 0)
	eq(t, count(t, filepath.Join(b, "holding")), 1)
	eq(t, count(t, filepath.Join(b, "done")), 0)
	// A held message is not taken again, but can be listed.
	eq(t, run(t, b, 0, nil, "take"), "")
	eq(t, run(t, b, 0, nil, "list", "--holding"), "item\tbuild finished\n")
	run(t, b, 0, nil, "ack", "item")
	eq(t, count(t, filepath.Join(b, "holding")), 0)
	eq(t, count(t, filepath.Join(b, "done")), 1)
	// Acking what is not held is a runtime error, not a silent success.
	run(t, b, 1, nil, "ack", "item")
	run(t, b, 1, nil, "release", "item")
	// A receiver that dies after take --hold leaves the message in
	// holding; release puts it back so it can be taken again.
	putTest(t, b, "retry")
	run(t, b, 0, nil, "take", "--hold")
	run(t, b, 0, nil, "release", "retry")
	eq(t, count(t, filepath.Join(b, "pending")), 1)
	eq(t, run(t, b, 0, nil, "take"), "retry\tbuild finished\n")
	// gc never removes held messages, however old.
	putTest(t, b, "kept")
	run(t, b, 0, nil, "take", "--hold")
	old(t, filepath.Join(b, "holding/kept.msg"))
	run(t, b, 0, nil, "gc", "--keep", "0")
	eq(t, count(t, filepath.Join(b, "holding")), 1)
}

func TestHoldWithOwner(t *testing.T) {
	b := box(t)
	run(t, b, 0, nil, "own", "claim", "--as", "alice")
	putTest(t, b, "item")
	run(t, b, 5, nil, "take", "--hold", "--as", "bob")
	eq(t, count(t, filepath.Join(b, "pending")), 1)
	eq(t, run(t, b, 0, nil, "take", "--hold", "--as", "alice"), "item\tbuild finished\n")
	run(t, b, 5, nil, "ack", "--as", "bob", "item")
	run(t, b, 5, nil, "release", "--as", "bob", "item")
	run(t, b, 0, nil, "ack", "--as", "alice", "item")
	eq(t, count(t, filepath.Join(b, "done")), 1)
}

// Regression tests from the independent Codex review.

func TestHoldSameIDDoesNotOverwrite(t *testing.T) {
	b := box(t)
	run(t, b, 0, nil, "put", "--id", "p", "first body")
	run(t, b, 0, nil, "take", "--hold")
	run(t, b, 0, nil, "put", "--id", "p", "second body")
	// Holding the same id again must not destroy the held first body:
	// the second message stays pending and nothing is printed for it.
	eq(t, run(t, b, 0, nil, "take", "--hold"), "")
	eq(t, read(t, filepath.Join(b, "holding/p.msg")), "first body")
	eq(t, count(t, filepath.Join(b, "pending")), 1)
	run(t, b, 0, nil, "ack", "p")
	eq(t, read(t, filepath.Join(b, "done/p.msg")), "first body")
	eq(t, run(t, b, 0, nil, "take", "--hold"), "p\tsecond body\n")
	run(t, b, 0, nil, "ack", "p")
	eq(t, read(t, filepath.Join(b, "done/p.msg")), "second body")
}

func TestAckReleaseHyphenID(t *testing.T) {
	b := box(t)
	run(t, b, 0, nil, "put", "--id=-dash", "body")
	run(t, b, 0, nil, "take", "--hold")
	run(t, b, 0, nil, "release", "--", "-dash")
	eq(t, count(t, filepath.Join(b, "pending")), 1)
	run(t, b, 0, nil, "take", "--hold")
	run(t, b, 0, nil, "ack", "--", "-dash")
	eq(t, count(t, filepath.Join(b, "done")), 1)
}

func TestStreamAndTakeStopAtBrokenPipe(t *testing.T) {
	for _, mode := range []string{"take", "stream"} {
		b := box(t)
		for _, id := range []string{"first", "second", "third"} {
			putTest(t, b, id)
		}
		r, w, e := os.Pipe()
		if e != nil {
			t.Fatal(e)
		}
		r.Close() // no reader: the first write hits a broken pipe
		args := []string{"--box", b, mode}
		if mode == "stream" {
			args = append(args, "--poll", ".01")
		}
		c := exec.Command(binary, args...)
		c.Stdout = w
		err := c.Run()
		w.Close()
		code := 0
		if err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				code = ee.ExitCode()
			} else {
				t.Fatal(err)
			}
		}
		want := 1
		if mode == "stream" {
			want = 0
		}
		eq(t, code, want)
		// Only the first message may have moved; the rest stay pending.
		eq(t, count(t, filepath.Join(b, "done")), 1)
		eq(t, count(t, filepath.Join(b, "pending")), 2)
	}
}

func TestStreamEndsWhenOwnerDrops(t *testing.T) {
	b := box(t)
	run(t, b, 0, nil, "own", "claim", "--as", "alice")
	putTest(t, b, "first")
	c := command(b, "stream", "--as", "alice", "--poll", ".01")
	p, e := c.StdoutPipe()
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { c.Process.Kill() }()
	ch := make(chan string, 1)
	go func() {
		v := make([]byte, len("first")+16)
		_, err := io.ReadFull(p, v)
		if err != nil {
			ch <- err.Error()
		} else {
			ch <- string(v)
		}
	}()
	select {
	case v := <-ch:
		eq(t, v, "first\tbuild finished\n")
	case <-time.After(5 * time.Second):
		t.Fatal("stream stalled")
	}
	run(t, b, 0, nil, "own", "drop", "--as", "alice")
	putTest(t, b, "second")
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	select {
	case e := <-done:
		ee, ok := e.(*exec.ExitError)
		if !ok || ee.ExitCode() != 6 {
			t.Fatalf("stream exit after drop: %v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not end after owner dropped the box")
	}
	eq(t, count(t, filepath.Join(b, "pending")), 1)
}

func TestEmptyAsRejected(t *testing.T) {
	b := box(t)
	run(t, b, 2, nil, "take", "--as=")
	run(t, b, 2, nil, "take", "--as", "")
	run(t, b, 2, nil, "stream", "--as=")
	run(t, b, 2, nil, "own", "claim", "--as", "")
	run(t, b, 2, nil, "ack", "--as=", "x")
}

func TestGenerationSurvivesDrop(t *testing.T) {
	b := box(t)
	run(t, b, 0, nil, "own", "claim", "--as", "alice")
	run(t, b, 0, nil, "own", "drop", "--as", "alice")
	out := run(t, b, 0, nil, "own", "claim", "--as", "bob")
	if !strings.HasPrefix(out, "bob\t2\t") {
		t.Fatalf("generation reset after drop: %q", out)
	}
}

func TestTTLZeroAndHuge(t *testing.T) {
	b := box(t)
	run(t, b, 2, nil, "own", "claim", "--as", "alice", "--ttl", "10000000000s")
	run(t, b, 0, nil, "own", "claim", "--as", "alice", "--ttl", "0")
	// A zero TTL is already expired, so bob may claim at once.
	out := run(t, b, 0, nil, "own", "claim", "--as", "bob", "--ttl", "0")
	if !strings.HasPrefix(out, "bob\t2\t") {
		t.Fatalf("zero-ttl claim: %q", out)
	}
	putTest(t, b, "item")
	// Renewal keeps the stored zero instead of silently using 30m:
	// bob operates, and alice can still claim immediately afterwards.
	eq(t, run(t, b, 0, nil, "take", "--as", "bob"), "item\tbuild finished\n")
	run(t, b, 0, nil, "own", "claim", "--as", "alice")
}

func TestCorruptOwnerRecord(t *testing.T) {
	b := box(t)
	putTest(t, b, "item")
	if e := os.WriteFile(filepath.Join(b, "state/owner"), []byte("null"), 0600); e != nil {
		t.Fatal(e)
	}
	run(t, b, 1, nil, "own", "show")
	run(t, b, 1, nil, "take", "--as", "alice")
	if e := os.WriteFile(filepath.Join(b, "state/owner"), []byte("{broken"), 0600); e != nil {
		t.Fatal(e)
	}
	run(t, b, 1, nil, "own", "claim", "--as", "alice")
	// Unnamed legacy calls still work with a corrupt record.
	eq(t, run(t, b, 0, nil, "take"), "item\tbuild finished\n")
}

func TestOwnRunSignalExit(t *testing.T) {
	b := box(t)
	run(t, b, 0, nil, "own", "claim", "--as", "alice")
	run(t, b, 143, nil, "own", "run", "--as", "alice", "--", "sh", "-c", "kill -TERM $$")
}

func TestHoldingBlockedOnlyAffectsNewCommands(t *testing.T) {
	b := box(t)
	putTest(t, b, "item")
	run(t, b, 0, nil, "take")
	// Replace the holding directory with a plain file.
	if e := os.Remove(filepath.Join(b, "holding")); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(b, "holding"), []byte("x"), 0600); e != nil {
		t.Fatal(e)
	}
	// Legacy commands keep working; only the new ones fail.
	putTest(t, b, "legacy")
	eq(t, run(t, b, 0, nil, "take"), "legacy\tbuild finished\n")
	putTest(t, b, "held")
	run(t, b, 1, nil, "take", "--hold")
	run(t, b, 1, nil, "ack", "held")
}

func TestOwnerNameEscapedInOutput(t *testing.T) {
	b := box(t)
	out := run(t, b, 0, nil, "own", "claim", "--as", "a\tb\nc")
	if strings.Count(out, "\n") != 1 {
		t.Fatalf("owner output must be one line: %q", out)
	}
	fields := strings.Split(strings.TrimRight(out, "\n"), "\t")
	eq(t, len(fields), 3)
	eq(t, fields[0], `a\tb\nc`)
	eq(t, fields[1], "1")
}
