package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var jobNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9_.-]*$`)

type jobDefinition struct {
	Box         string    `json:"box"`
	Every       string    `json:"every"`
	Argv        []string  `json:"argv"`
	TSV         bool      `json:"tsv"`
	StdinPrev   bool      `json:"stdin_prev"`
	FirstSilent bool      `json:"first_silent"`
	FailAfter   int       `json:"fail_after"`
	Deadline    time.Time `json:"deadline"`
	TTL         string    `json:"ttl"`
	Once        bool      `json:"once"`
	CreatedAt   time.Time `json:"created_at"`
}

type jobState struct {
	LastRun             time.Time `json:"last_run"`
	NextRun             time.Time `json:"next_run"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	FailNotified        bool      `json:"fail_notified"`
	RunStarted          time.Time `json:"run_started"`
	FirstFailure        time.Time `json:"first_failure,omitempty"`
	PendingFailure      string    `json:"pending_failure,omitempty"`
}

type jobConfig struct {
	Path        []string `json:"job_path"`
	Env         []string `json:"job_env"`
	MaxOutput   int      `json:"job_max_output"`
	Concurrency int      `json:"job_concurrency"`
	Timeout     string   `json:"job_timeout"`
}

func loadJobs(root string) (jobConfig, error) {
	c := jobConfig{MaxOutput: 1 << 20, Concurrency: 4, Timeout: "30s"}
	err := readJobJSON(filepath.Join(root, "ibx.json"), &c)
	if err != nil && !os.IsNotExist(err) {
		return c, err
	}
	timeout, err := time.ParseDuration(c.Timeout)
	if err != nil || timeout <= 0 || c.MaxOutput <= 0 || c.Concurrency <= 0 {
		return c, errors.New("ibx.json: job_timeout, job_max_output, and job_concurrency must be positive")
	}
	return c, nil
}

func (o *options) jobFlag(key, value string) error {
	switch key {
	case "--every", "--ttl":
		d, err := time.ParseDuration(value)
		if key == "--every" {
			if err != nil || d < 5*time.Second {
				return usageError("--every requires an interval of at least 5s")
			}
			o.job.Every = d.String()
		} else {
			if err != nil || d < 0 {
				return usageError("--ttl requires a nonnegative duration")
			}
			o.job.TTL = d.String()
		}
	case "--fail-after":
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 {
			return usageError("--fail-after requires a nonnegative integer")
		}
		o.job.FailAfter = n
	case "--deadline":
		_, err := parseDelivery(value, time.Now())
		if err != nil {
			return usageError("invalid --deadline: expected RFC 3339 or +duration")
		}
		o.at = value
	}
	return nil
}

func (o *options) validateJob() error {
	switch o.sub {
	case "ls":
		return nil
	case "rm", "set":
		if o.arg == "" {
			return usageError("a job name is required")
		}
	default:
		return usageError("expected job set, rm, or ls")
	}
	if o.sub == "rm" {
		return nil
	}
	if o.box == "" || o.job.Every == "" || len(o.runArgs) == 0 {
		return usageError("job set requires --box, --every, and a command after --")
	}
	if !bareJobCommand(o.runArgs[0]) {
		return usageError("job executable must be a bare file name")
	}
	return nil
}

func bareJobCommand(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, "/\x00")
}

func (c jobConfig) executable(root, name string) (string, error) {
	if !bareJobCommand(name) {
		return "", errors.New("job executable must be a bare file name")
	}
	if len(c.Path) == 0 {
		return "", errors.New("job_path is empty: no job executables are allowed")
	}
	for _, dir := range c.Path {
		if dir == "" {
			continue
		}
		path := absolute(absolute(root, dir), name)
		if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0 && jobExecutableAccess(path) {
			return path, nil
		}
	}
	return "", fmt.Errorf("executable %q not found or not executable in job_path", name)
}

type jobScheduler struct {
	mu      sync.Mutex
	workers sync.WaitGroup
	active  map[string]*jobRun
	wake    chan struct{}
}

type jobRun struct {
	cancel context.CancelFunc
	stale  bool
}

func (d daemonServer) jobFile(name, suffix string) string {
	return filepath.Join(d.root, "jobs", name+suffix)
}

func readJobJSON(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func (d daemonServer) jobDefinition(name string) (j jobDefinition, err error) {
	err = readJobJSON(d.jobFile(name, ".json"), &j)
	if err == nil && len(j.Argv) == 0 {
		err = os.ErrNotExist // This filename may hold another job's state.
	}
	return j, err
}

type namedJob struct {
	name string
	def  jobDefinition
}

func (d daemonServer) jobDefinitions() ([]namedJob, error) {
	entries, err := os.ReadDir(filepath.Join(d.root, "jobs"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var definitions []namedJob
	for _, entry := range entries {
		name, ok := strings.CutSuffix(entry.Name(), ".json")
		if !ok || !entry.Type().IsRegular() || !jobNamePattern.MatchString(name) {
			continue
		}
		j, err := d.jobDefinition(name)
		if err == nil {
			definitions = append(definitions, namedJob{name, j})
		} else if !os.IsNotExist(err) {
			fmt.Fprintln(os.Stderr, "job definition:", name, err)
		}
	}
	return definitions, nil
}

// The alternate path is needed when <name>.state is itself a job name.
func (d daemonServer) alternateJobState(name string) string {
	return d.jobFile("."+name, ".state.json")
}

func (d daemonServer) jobStatePath(name string) string {
	alternate := d.alternateJobState(name)
	if _, err := os.Stat(alternate); !os.IsNotExist(err) {
		return alternate
	}
	if _, err := d.jobDefinition(name + ".state"); !os.IsNotExist(err) {
		return alternate
	}
	return d.jobFile(name, ".state.json")
}

func (d daemonServer) readJobState(name string, j jobDefinition) (jobState, error) {
	s := jobState{NextRun: j.CreatedAt}
	err := readJobJSON(d.jobStatePath(name), &s)
	if os.IsNotExist(err) {
		err = nil
	}
	return s, err
}

func (d daemonServer) saveJobState(name string, s jobState) error {
	return writeJSON(d.jobStatePath(name), s)
}

func (d daemonServer) removeJob(name string) error {
	if err := unlink(d.jobFile(name, ".json")); err != nil {
		return err
	}
	return errors.Join(unlink(d.jobStatePath(name)), unlink(d.jobFile(name, ".prev")))
}

func (d daemonServer) invalidateJob(name string) {
	if run := d.jobs.active[name]; run != nil {
		run.stale = true
		run.cancel()
	}
}

func (d daemonServer) jobCommand(o options) error {
	if d.jobs == nil {
		return errors.New("job scheduler is unavailable")
	}
	d.jobs.mu.Lock()
	defer d.jobs.mu.Unlock()
	switch o.sub {
	case "ls":
		return d.listJobs(o)
	case "rm":
		if _, err := d.jobDefinition(o.arg); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("unknown job %q", o.arg)
			}
			return err
		}
		d.invalidateJob(o.arg)
		return d.removeJob(o.arg)
	}
	j := o.job
	j.CreatedAt = time.Now()
	j.Box, j.Argv = o.box, o.runArgs
	if o.at != "" {
		deadline, err := parseDelivery(o.at, j.CreatedAt)
		if err != nil || deadline.Before(j.CreatedAt) {
			return usageError("--deadline is already past")
		}
		j.Deadline = deadline
	}
	c, err := loadJobs(d.root)
	if err != nil {
		return err
	}
	if _, err := c.executable(d.root, j.Argv[0]); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(d.root, "jobs"), 0700); err != nil {
		return err
	}
	if parent, ok := strings.CutSuffix(o.arg, ".state"); ok && jobNamePattern.MatchString(parent) {
		if _, err := d.jobDefinition(parent); err == nil && d.jobStatePath(parent) != d.alternateJobState(parent) {
			if err := os.Rename(d.jobFile(parent, ".state.json"), d.alternateJobState(parent)); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	if err := unlink(d.jobFile(o.arg, ".prev")); err != nil {
		return err
	}
	if err := d.saveJobState(o.arg, jobState{NextRun: j.CreatedAt}); err != nil {
		return err
	}
	if err := writeJSON(d.jobFile(o.arg, ".json"), j); err != nil {
		return err
	}
	d.invalidateJob(o.arg)
	d.wakeJobs()
	return nil
}

type jobListRow struct {
	Name                string `json:"name"`
	Box                 string `json:"box"`
	Every               string `json:"every"`
	LastRun             string `json:"last_run"`
	NextRun             string `json:"next_run"`
	ConsecutiveFailures int    `json:"consecutive_failures"`
}

func jobTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format(time.RFC3339)
}

func (d daemonServer) listJobs(o options) error {
	definitions, err := d.jobDefinitions()
	if err != nil {
		return err
	}
	enc := json.NewEncoder(o.stdout)
	for _, job := range definitions {
		s, err := d.readJobState(job.name, job.def)
		if err != nil {
			return err
		}
		row := jobListRow{job.name, job.def.Box, job.def.Every, jobTime(s.LastRun), jobTime(s.NextRun), s.ConsecutiveFailures}
		if o.listJSON {
			err = enc.Encode(row)
		} else {
			_, err = fmt.Fprintf(o.stdout, "%s\t%s\t%s\t%s\t%s\t%d\n", row.Name, row.Box, row.Every, row.LastRun, row.NextRun, row.ConsecutiveFailures)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
