package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func jobFixture(t *testing.T, settings map[string]any) daemonServer {
	t.Helper()
	d := stage2Server(t)
	bin := filepath.Join(d.root, "bin")
	if err := os.Mkdir(bin, 0750); err != nil {
		t.Fatal(err)
	}
	settings["job_path"] = []string{bin}
	data, err := json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	jobWrite(t, filepath.Join(d.root, "ibx.json"), string(data), 0600)
	if init, ok := any(&d).(interface{ initJobs() }); ok {
		init.initJobs()
	}
	return d
}

func jobWrite(t *testing.T, path, contents string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), mode); err != nil {
		t.Fatal(err)
	}
}

func jobScript(t *testing.T, d daemonServer, contents string) {
	t.Helper()
	jobWrite(t, filepath.Join(d.root, "bin", "probe"), "#!/bin/sh\n"+contents+"\n", 0750)
}

func jobSet(t *testing.T, d daemonServer, name string, flags ...string) {
	t.Helper()
	found := false
	for _, flag := range flags {
		found = found || flag == "--"
	}
	if !found {
		flags = append(flags, "--", "probe")
	}
	args := append([]string{"job", "set", name, "--box", "box", "--every", "5s"}, flags...)
	d.stage2Invoke(t, 0, nil, args...)
}

func TestStage3JobCLI(t *testing.T) {
	d := jobFixture(t, map[string]any{})
	jobScript(t, d, "exit 0")
	for _, test := range []struct {
		code                            int
		name, interval, command, reason string
	}{
		{1, "watch", "5s", "missing", "executable"},
		{2, "watch", "5s", "./probe", "bare"},
		{2, "watch", "4s", "probe", "5s"},
		{2, ".hidden", "5s", "probe", "name"},
	} {
		_, stderr := d.stage2Invoke(t, test.code, nil, "job", "set", test.name, "--box", "box", "--every", test.interval, "--", test.command)
		if !strings.Contains(stderr, test.reason) {
			t.Errorf("expected %q in %q", test.reason, stderr)
		}
	}
	jobSet(t, d, "watch.state")
	out, _ := d.stage2Invoke(t, 0, nil, "job", "ls", "--json")
	var row map[string]any
	if err := json.Unmarshal([]byte(out), &row); err != nil {
		t.Fatal(err)
	}
	if row["name"] != "watch.state" || row["box"] != filepath.Join(d.root, "box") || row["every"] != "5s" ||
		row["last_run"] != "-" || row["consecutive_failures"] != float64(0) {
		t.Fatalf("unexpected listing: %v", row)
	}
	if _, err := time.Parse(time.RFC3339, row["next_run"].(string)); err != nil {
		t.Fatal(err)
	}
	out, _ = d.stage2Invoke(t, 0, nil, "status")
	if !strings.Contains(out, "jobs: 1\n") {
		t.Fatal(out)
	}
	d.stage2Invoke(t, 0, nil, "job", "rm", "watch.state")
	d.stage2Invoke(t, 1, nil, "job", "rm", "watch.state")
}

type stage3Driver interface {
	jobTick(context.Context, time.Time)
	jobWait()
}

func jobDriver(t *testing.T, d daemonServer) stage3Driver {
	t.Helper()
	driver, ok := any(d).(stage3Driver)
	if !ok {
		t.Fatal("daemon has no job scheduler")
	}
	return driver
}

func jobBodies(t *testing.T, d daemonServer, box string) []string {
	t.Helper()
	path := filepath.Join(d.root, box, "pending")
	files, err := os.ReadDir(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var bodies []string
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".msg") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(path, file.Name()))
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, string(body))
	}
	return bodies
}

func TestStage3JobOutput(t *testing.T) {
	t.Setenv("JOB_ALLOWED", "yes")
	t.Setenv("JOB_HIDDEN", "secret")
	d := jobFixture(t, map[string]any{"job_env": []string{"JOB_ALLOWED"}})
	jobScript(t, d, `IFS= read -r previous || :
if [ ! -f input ]; then
	printf '%s|%s|%s|%s|%s\n' "$1" "$JOB_ALLOWED" "${JOB_HIDDEN-unset}" "$IBX_JOB" "$IBX_BOX"
elif [ -s input ]; then
	printf '%s\n' "$previous"
	/bin/cat input
fi`)
	jobSet(t, d, "watch", "--stdin-prev", "--first-silent", "--", "probe", "a;$(touch bad)")
	driver := jobDriver(t, d)
	now := time.Now().Add(time.Second)
	tick := func() {
		driver.jobTick(context.Background(), now)
		driver.jobWait()
		now = now.Add(5 * time.Second)
	}
	tick()
	if got := jobBodies(t, d, "box"); len(got) != 0 {
		t.Fatal(got)
	}
	prev := "a;$(touch bad)|yes|unset|watch|" + filepath.Join(d.root, "box") + "\n"
	data, err := os.ReadFile(filepath.Join(d.root, "jobs", "watch.prev"))
	if err != nil || string(data) != prev {
		t.Fatalf("prev = %q: %v", data, err)
	}
	tick()
	jobWrite(t, filepath.Join(d.root, "input"), "change\n", 0600)
	tick()
	if got := jobBodies(t, d, "box"); len(got) != 1 || got[0] != prev+"change\n" {
		t.Fatal(got)
	}
	tick()
	jobWrite(t, filepath.Join(d.root, "input"), "", 0600)
	tick()
	if got := jobBodies(t, d, "box"); len(got) != 1 {
		t.Fatal(got)
	}
	jobScript(t, d, "printf 'other\\tjob-id\\tbody\\nmalformed\\n'")
	jobSet(t, d, "watch", "--tsv", "--once")
	tick()
	if got := jobBodies(t, d, "other"); len(got) != 1 || got[0] != "body" {
		t.Fatal(got)
	}
}

func TestStage3JobFailuresAndEnd(t *testing.T) {
	d := jobFixture(t, map[string]any{"job_timeout": "300ms", "job_max_output": 16})
	jobScript(t, d, `case "$(/bin/cat mode)" in
 timeout) /bin/sleep 20 ;;
 overflow) printf '01234567890123456789'; /bin/sleep 20 ;;
 recovery) exit 0 ;;
 stop) printf 'ignored'; exit 10 ;;
 *) exit 7 ;;
esac`)
	jobSet(t, d, "watch", "--fail-after", "2")
	driver := jobDriver(t, d)
	now := time.Now().Add(time.Second)
	tick := func(mode string) {
		jobWrite(t, filepath.Join(d.root, "mode"), mode, 0600)
		driver.jobTick(context.Background(), now)
		driver.jobWait()
		now = now.Add(5 * time.Second)
	}
	tick("timeout")
	out, _ := d.stage2Invoke(t, 0, nil, "job", "ls", "--json")
	if !strings.Contains(out, `"consecutive_failures":1`) {
		t.Fatal(out)
	}
	started := time.Now()
	tick("overflow")
	if time.Since(started) >= 250*time.Millisecond {
		t.Error("overflow waited for timeout")
	}
	tick("failure")
	if got := jobBodies(t, d, "box"); len(got) != 1 || !strings.Contains(got[0], "overflow") {
		t.Fatal(got)
	}
	tick("recovery")
	tick("failure")
	tick("failure")
	if got := jobBodies(t, d, "box"); len(got) != 2 {
		t.Fatal(got)
	}
	tick("stop")
	jobSet(t, d, "once", "--once", "--fail-after", "1")
	tick("failure")
	if got := jobBodies(t, d, "box"); len(got) != 3 {
		t.Fatal(got)
	}
	jobSet(t, d, "ttl", "--ttl", "1ms")
	jobSet(t, d, "deadline", "--deadline", "+1h")
	driver.jobTick(context.Background(), now.Add(2*time.Hour))
	driver.jobWait()
	out, _ = d.stage2Invoke(t, 0, nil, "job", "ls")
	if out != "" {
		t.Fatal(out)
	}
	d.stage2Invoke(t, 2, nil, "job", "set", "past", "--box", "box", "--every", "5s", "--deadline", "2000-01-01T00:00:00Z", "--", "probe")
}

func jobAwaitFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStage3JobScheduling(t *testing.T) {
	d := jobFixture(t, map[string]any{"job_concurrency": 1, "job_timeout": "10s"})
	jobScript(t, d, `if [ "$IBX_JOB" = a ]; then
	printf 'a\n' >> runs
	: > started
	while [ ! -f release ]; do /bin/sleep .01; done
else
	printf 'b\n' >> runs
fi`)
	jobSet(t, d, "a")
	jobSet(t, d, "b")
	driver := jobDriver(t, d)
	now := time.Now().Add(time.Hour)
	driver.jobTick(context.Background(), now)
	jobAwaitFile(t, filepath.Join(d.root, "started"))
	driver.jobTick(context.Background(), now.Add(5*time.Second))
	data, err := os.ReadFile(filepath.Join(d.root, "runs"))
	if err != nil || string(data) != "a\n" {
		t.Fatalf("overlap: %q %v", data, err)
	}
	jobWrite(t, filepath.Join(d.root, "release"), "", 0600)
	driver.jobWait()
	driver.jobTick(context.Background(), now.Add(5*time.Second))
	driver.jobWait()
	data, err = os.ReadFile(filepath.Join(d.root, "runs"))
	if err != nil || string(data) != "a\nb\n" {
		t.Fatalf("catch-up or deferred run: %q %v", data, err)
	}
	jobSet(t, d, "a")
	jobSet(t, d, "a.state")
	d.stage2Invoke(t, 0, nil, "job", "rm", "a")
	out, _ := d.stage2Invoke(t, 0, nil, "job", "ls")
	if !strings.Contains(out, "a.state\t") {
		t.Fatalf("colliding name lost: %q", out)
	}
}

func TestStage3JobProcess(t *testing.T) {
	root := os.Getenv("IBX_STAGE3_ROOT")
	if root == "" {
		return
	}
	d := daemonServer{root: root}
	init, ok := any(&d).(interface{ initJobs() })
	if !ok {
		t.Fatal("daemon has no job scheduler")
	}
	init.initJobs()
	driver := jobDriver(t, d)
	driver.jobTick(context.Background(), time.Now())
	driver.jobWait()
}

func TestStage3JobSIGKILL(t *testing.T) {
	d := jobFixture(t, map[string]any{})
	jobScript(t, d, ": > started; exec /bin/sleep 30")
	// The restart assertion must observe the saved schedule before the next
	// tick, even when starting a race-instrumented helper is slow.
	d.stage2Invoke(t, 0, nil, "job", "set", "watch", "--box", "box", "--every", "5m", "--", "probe")
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	run := func() *exec.Cmd {
		cmd := exec.Command(self, "-test.run=^TestStage3JobProcess$")
		cmd.Env = append(os.Environ(), "IBX_STAGE3_ROOT="+d.root)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
		return cmd
	}
	cmd := run()
	jobAwaitFile(t, filepath.Join(d.root, "started"))
	state := filepath.Join(d.root, "jobs", "watch.state.json")
	readState := func() map[string]any {
		t.Helper()
		data, err := os.ReadFile(state)
		if err != nil {
			t.Fatal(err)
		}
		var row map[string]any
		if err := json.Unmarshal(data, &row); err != nil {
			t.Fatal(err)
		}
		return row
	}
	before := readState()
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()
	if err := run().Wait(); err != nil {
		t.Fatal(err)
	}
	after := readState()
	if before["next_run"] != after["next_run"] || before["run_started"] == after["run_started"] ||
		after["consecutive_failures"] != float64(0) {
		t.Fatalf("before %v, after %v", before, after)
	}
	jobScript(t, d, "printf resumed")
	// Force the restored schedule due without waiting for a real interval.
	driver := jobDriver(t, d)
	driver.jobTick(context.Background(), time.Now().Add(time.Hour))
	driver.jobWait()
	if got := jobBodies(t, d, "box"); len(got) != 1 || got[0] != "resumed" {
		t.Fatal(got)
	}
}
