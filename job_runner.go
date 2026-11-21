package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"
)

func jobExecutableAccess(path string) bool {
	return syscall.Access(path, 1) == nil // X_OK
}

func (d *daemonServer) initJobs() {
	d.jobs = &jobScheduler{active: make(map[string]*jobRun), wake: make(chan struct{}, 1)}
}

func (d daemonServer) wakeJobs() {
	select {
	case d.jobs.wake <- struct{}{}:
	default:
	}
}

func (d daemonServer) runJobs(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	defer d.jobWait()
	for ctx.Err() == nil {
		d.jobTick(ctx, time.Now())
		select {
		case <-ctx.Done():
		case <-ticker.C:
		case <-d.jobs.wake:
		}
	}
}

func (d daemonServer) jobWait() {
	d.jobs.workers.Wait()
}

func jobExpired(j jobDefinition, now time.Time) bool {
	ttl, _ := time.ParseDuration(j.TTL)
	return (!j.Deadline.IsZero() && !now.Before(j.Deadline)) ||
		(j.TTL != "" && !now.Before(j.CreatedAt.Add(ttl)))
}

func (d daemonServer) jobTick(ctx context.Context, now time.Time) {
	d.jobs.mu.Lock()
	defer d.jobs.mu.Unlock()
	definitions, err := d.jobDefinitions()
	if err != nil {
		fmt.Fprintln(os.Stderr, "job scan:", err)
		return
	}
	type dueJob struct {
		namedJob
		state jobState
		every time.Duration
	}
	var due []dueJob
	for _, job := range definitions {
		if ctx.Err() != nil {
			return
		}
		name, j := job.name, job.def
		active := d.jobs.active[name]
		s, err := d.readJobState(name, j)
		if err != nil {
			fmt.Fprintln(os.Stderr, "job state:", name, err)
			continue
		}
		if active == nil && !s.RunStarted.IsZero() {
			s.RunStarted = time.Time{}
			if err := d.saveJobState(name, s); err != nil {
				fmt.Fprintln(os.Stderr, "job recovery:", name, err)
				continue
			}
		}
		if s.PendingFailure != "" {
			if err := d.publishJobFailure(ctx, name, j, &s); err != nil {
				fmt.Fprintln(os.Stderr, "job failure notification:", name, err)
				continue
			}
		}
		if (j.Once && !s.LastRun.IsZero()) || jobExpired(j, now) {
			d.invalidateJob(name)
			if err := d.removeJob(name); err != nil {
				fmt.Fprintln(os.Stderr, "job remove:", name, err)
			}
			continue
		}
		every, err := time.ParseDuration(j.Every)
		if err != nil || every < 5*time.Second {
			fmt.Fprintln(os.Stderr, "job has invalid interval:", name)
			continue
		}
		if !now.Before(s.NextRun) {
			due = append(due, dueJob{job, s, every})
		}
	}
	if len(due) == 0 {
		return
	}
	config, err := loadJobs(d.root)
	if err != nil {
		fmt.Fprintln(os.Stderr, "job config:", err)
		return
	}
	sort.SliceStable(due, func(i, j int) bool { return due[i].state.NextRun.Before(due[j].state.NextRun) })
	for _, job := range due {
		if ctx.Err() != nil {
			break
		}
		s := job.state
		active := d.jobs.active[job.name]
		if active == nil && len(d.jobs.active) >= config.Concurrency {
			continue
		}
		if active != nil && active.stale {
			continue // A replacement remains due while the old process exits.
		}
		s.NextRun = now.Add(job.every - now.Sub(s.NextRun)%job.every)
		if active == nil {
			s.RunStarted = now
		}
		if err := d.saveJobState(job.name, s); err != nil {
			fmt.Fprintln(os.Stderr, "job scheduling:", job.name, err)
			continue
		}
		if active != nil {
			continue
		}
		runCtx, cancel := context.WithCancel(ctx)
		run := &jobRun{cancel: cancel}
		d.jobs.active[job.name] = run
		d.jobs.workers.Add(1)
		go d.runJob(runCtx, job.namedJob, config, run)
	}
}

type cappedJobOutput struct {
	buffer bytes.Buffer
	limit  int
	over   bool
	abort  context.CancelFunc
}

func (w *cappedJobOutput) Write(p []byte) (int, error) {
	if len(p) > w.limit-w.buffer.Len() {
		w.over = true
		w.abort()
		return 0, errors.New("output overflow")
	}
	return w.buffer.Write(p)
}

func (d daemonServer) executeJob(ctx context.Context, job namedJob, c jobConfig, previous []byte) (out []byte, stop bool, err error) {
	j := job.def
	path, err := c.executable(d.root, j.Argv[0])
	if err != nil {
		return nil, false, err
	}
	timeout, _ := time.ParseDuration(c.Timeout)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	output := &cappedJobOutput{limit: c.MaxOutput, abort: cancel}
	cmd := exec.CommandContext(ctx, path, j.Argv[1:]...)
	cmd.Dir = d.root
	cmd.Env = make([]string, 0, len(c.Env)+2)
	for _, name := range c.Env {
		if name == "" || strings.ContainsAny(name, "=\x00") {
			continue
		}
		if value, ok := os.LookupEnv(name); ok {
			cmd.Env = append(cmd.Env, name+"="+value)
		}
	}
	cmd.Env = append(cmd.Env, "IBX_JOB="+job.name, "IBX_BOX="+j.Box)
	if j.StdinPrev {
		cmd.Stdin = bytes.NewReader(previous)
	}
	cmd.Stdout = output
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	killGroup := func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.Cancel = killGroup
	cmd.WaitDelay = 100 * time.Millisecond
	// Linux delivers Pdeathsig on death of the thread that started the child.
	runtime.LockOSThread()
	err = cmd.Run()
	runtime.UnlockOSThread()
	switch {
	case output.over:
		_ = killGroup()
		return nil, false, errors.New("output overflow")
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		if cmd.Process != nil {
			_ = killGroup()
		}
		return nil, false, errors.New("timeout")
	case errors.Is(err, exec.ErrWaitDelay):
		_ = killGroup()
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 10 {
		return nil, true, nil
	}
	return output.buffer.Bytes(), false, err
}

func (d daemonServer) runJob(ctx context.Context, job namedJob, c jobConfig, run *jobRun) {
	defer d.jobs.workers.Done()
	defer run.cancel()
	var previous []byte
	first := true
	var runErr error
	// Read previous output before replacement can delete or overwrite it.
	d.jobs.mu.Lock()
	if !run.stale {
		previous, runErr = os.ReadFile(d.jobFile(job.name, ".prev"))
		first = os.IsNotExist(runErr)
		if first {
			runErr = nil
		}
	}
	d.jobs.mu.Unlock()
	var output []byte
	var stop bool
	if runErr == nil && ctx.Err() == nil {
		output, stop, runErr = d.executeJob(ctx, job, c, previous)
	}
	d.jobs.mu.Lock()
	defer d.jobs.mu.Unlock()
	defer d.wakeJobs()
	delete(d.jobs.active, job.name)
	if run.stale {
		return
	}
	if err := d.finishJob(ctx, job, previous, output, first, stop, runErr); err != nil {
		fmt.Fprintln(os.Stderr, "job completion:", job.name, err)
	}
}

func (d daemonServer) finishJob(ctx context.Context, job namedJob, previous, output []byte, first, stop bool, runErr error) error {
	name, j := job.name, job.def
	s, err := d.readJobState(name, j)
	if err != nil {
		return err
	}
	s.RunStarted = time.Time{}
	if ctx.Err() != nil {
		return d.saveJobState(name, s)
	}
	if stop {
		return d.removeJob(name)
	}
	s.LastRun = time.Now()
	var outputErr error
	if runErr == nil {
		s.ConsecutiveFailures = 0
		s.FailNotified = false
		s.FirstFailure = time.Time{}
		outputErr = d.publishJobOutput(ctx, job, previous, output, first)
	} else {
		if s.ConsecutiveFailures == 0 || s.FirstFailure.IsZero() {
			s.FirstFailure = s.LastRun
		}
		s.ConsecutiveFailures++
		if j.FailAfter > 0 && s.ConsecutiveFailures >= j.FailAfter && !s.FailNotified {
			s.PendingFailure = fmt.Sprintf("Job %s failed %d consecutive times at %s: %v",
				name, s.ConsecutiveFailures, s.LastRun.Format(time.RFC3339Nano), runErr)
		}
	}
	if err := d.saveJobState(name, s); err != nil {
		return errors.Join(outputErr, err)
	}
	if s.PendingFailure != "" {
		if err := d.publishJobFailure(ctx, name, j, &s); err != nil {
			return err
		}
	}
	if j.Once {
		return errors.Join(outputErr, d.removeJob(name))
	}
	return outputErr
}

func (d daemonServer) publishJobFailure(ctx context.Context, name string, j jobDefinition, s *jobState) error {
	id := "job-failure-" + bodyID([]byte(name+"\x00"+s.FirstFailure.UTC().Format(time.RFC3339Nano)))
	body := []byte(s.PendingFailure)
	if err := d.publishJobMessage(ctx, j.Box, id, body, false, true); err != nil {
		return err
	}
	s.FailNotified = true
	s.PendingFailure = ""
	return d.saveJobState(name, *s)
}

func (d daemonServer) publishJobOutput(ctx context.Context, job namedJob, previous, output []byte, first bool) error {
	path := d.jobFile(job.name, ".prev")
	if !first && bytes.Equal(previous, output) {
		return nil
	}
	if err := atomicWrite(path, output); err != nil {
		return err
	}
	j := job.def
	if len(output) == 0 || (j.FirstSilent && first) {
		return nil
	}
	if !j.TSV {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return err
		}
		return d.publishJobMessage(ctx, j.Box, fmt.Sprintf("%x", id), output, false, false)
	}
	for line, body := range bytes.Split(output, []byte{'\n'}) {
		if len(body) == 0 {
			continue
		}
		box, id, payload, err := parseSendTSV(d.root, body, line+1)
		if err != nil {
			fmt.Fprintln(os.Stderr, "job TSV:", job.name, err)
			continue
		}
		if err := d.publishJobMessage(ctx, box, id, payload, true, false); err != nil {
			fmt.Fprintln(os.Stderr, "job TSV:", job.name, "line", line+1, err)
		}
	}
	return nil
}

// Store under the scheduler lock so replacement/removal cannot race with
// publication. Run notification hooks separately from the job process slots.
func (d daemonServer) publishJobMessage(ctx context.Context, box, id string, body []byte, allowEmpty, failure bool) error {
	call := invocation{stdout: io.Discard, stderr: os.Stderr, ctx: ctx, cwd: d.root, env: os.Environ()}
	o := options{invocation: call, command: "send", root: d.root, box: box, id: id, hasID: true, maxSize: len(body), renotify: 300, notifyTimeout: 10}
	config, err := loadNotify(d.root)
	if err != nil {
		return err
	}
	if !failure {
		if err := o.useDedupeConfig(config); err != nil {
			return err
		}
	}
	stored, err := publishSend(o, id, body, allowEmpty)
	if err != nil || !stored {
		return err
	}
	d.jobs.workers.Add(1)
	go func() {
		defer d.jobs.workers.Done()
		if _, err := notifyArrival(o, config, id, body, true); err != nil {
			fmt.Fprintln(os.Stderr, "job notify:", err)
		}
	}()
	return nil
}
