package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type notifySettings struct {
	Command    string `json:"notify_cmd"`
	Retry      string `json:"retry_after"`
	SweepAfter string `json:"sweep_after"`
	SweepEvery string `json:"sweep_every"`
	Fallback   string `json:"fallback"`
	Timeout    string `json:"notify_timeout"`
}

type notifyConfig struct {
	notifySettings
	Boxes     map[string]notifySettings `json:"boxes"`
	DedupeTTL string                    `json:"dedupe_ttl"`
	Keep      string                    `json:"keep"`
	Tee       []teeRule                 `json:"tee"`
}

func loadNotify(root string) (notifyConfig, error) {
	var c notifyConfig
	data, err := os.ReadFile(filepath.Join(root, "ibx.json"))
	if os.IsNotExist(err) {
		return c, nil
	}
	if err != nil {
		return c, err
	}
	if err = json.Unmarshal(data, &c); err != nil {
		return c, fmt.Errorf("ibx.json: %w", err)
	}
	return c, nil
}

func (c notifyConfig) forBox(root, box string) notifySettings {
	s := c.notifySettings
	b := c.Boxes[relativeBox(root, box)]
	for _, field := range []struct {
		dst   *string
		value string
	}{
		{&s.Command, b.Command}, {&s.Retry, b.Retry}, {&s.SweepAfter, b.SweepAfter},
		{&s.SweepEvery, b.SweepEvery}, {&s.Fallback, b.Fallback}, {&s.Timeout, b.Timeout},
	} {
		if field.value != "" {
			*field.dst = field.value
		}
	}
	return s
}

func settingDuration(value string, fallback time.Duration) time.Duration {
	if d, err := time.ParseDuration(value); err == nil && d >= 0 {
		return d
	}
	return fallback
}

func sweepPeriod(value string) time.Duration {
	return max(time.Millisecond, settingDuration(value, 5*time.Minute))
}

type notifyOverride struct {
	Command  *string `json:"command,omitempty"`
	Retry    string  `json:"retry,omitempty"`
	Fallback string  `json:"fallback,omitempty"`
	Timeout  string  `json:"timeout,omitempty"`
	ID       string  `json:"id,omitempty"`
}

func (v notifyOverride) apply(s notifySettings) notifySettings {
	if v.Command != nil {
		s.Command = *v.Command
	}
	if v.Retry != "" {
		s.Retry = v.Retry
	}
	if v.Fallback != "" {
		s.Fallback = v.Fallback
	}
	if v.Timeout != "" {
		s.Timeout = v.Timeout
	}
	return s
}

type notifyState struct {
	Episode   string         `json:"episode,omitempty"`
	Pending   string         `json:"pending,omitempty"`
	Attempt   time.Time      `json:"attempt,omitempty"`
	RetryAt   time.Time      `json:"retry_at,omitempty"`
	Override  notifyOverride `json:"override,omitempty"`
	Closed    bool           `json:"closed,omitempty"`
	Forwarded bool           `json:"forwarded,omitempty"`
}

func statePath(box string) string { return filepath.Join(box, "state", "notify-state.json") }

func readNotifyState(box string) (notifyState, error) {
	var s notifyState
	b, err := os.ReadFile(statePath(box))
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return s, err
	}
	return s, json.Unmarshal(b, &s)
}

func atomicWrite(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".write-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(data)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(f.Name(), path)
}

func saveNotifyState(box string, s notifyState) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return atomicWrite(statePath(box), b)
}

type notifyAttempt struct {
	pending          []entry
	owner, firstLine string
	state            notifyState
	settings         notifySettings
	override         notifyOverride
	cmd              *exec.Cmd
	started          time.Time
	launchErr        error
}

func prepareNotify(o options, id string, send bool, config notifyConfig) (a notifyAttempt, err error) {
	err = locked(o.box, func() error {
		if archived(o.box) {
			return nil
		}
		var e error
		a.pending, e = entries(filepath.Join(o.box, "pending"))
		if e != nil {
			return e
		}
		if len(a.pending) == 0 {
			return errors.Join(unlink(statePath(o.box)), unlink(filepath.Join(o.box, "state", "notified")))
		}
		a.state, e = readNotifyState(o.box)
		if e != nil {
			return e
		}
		s := &a.state
		now := time.Now()
		a.settings = config.forBox(o.root, o.box)
		if send {
			if now.Before(s.RetryAt) {
				a.pending = nil
				return nil
			}
			a.override.ID = id
			if o.hasNotify {
				a.override.Command = &o.notify
			}
			if o.hasRetry {
				a.override.Retry = o.retryAfter.String()
			}
			a.override.Fallback = o.fallback
			if o.hasNotifyTimeout {
				a.override.Timeout = sleepDuration(o.notifyTimeout).String()
			}
		} else if !s.RetryAt.IsZero() {
			if now.Before(s.RetryAt) {
				a.pending = nil
				return nil
			}
			a.override = s.Override
		}
		a.settings = a.override.apply(a.settings)
		if a.settings.Command == "" {
			a.pending = nil
			return nil
		}

		hash := sha256.New()
		for _, v := range a.pending {
			fmt.Fprintf(hash, "%s:%d\n", v.id, v.mtime)
		}
		key := fmt.Sprintf("%x", hash.Sum(nil))
		if send {
			marker := filepath.Join(o.box, "state", "notified")
			info, e := os.Stat(marker)
			if e == nil && now.Sub(info.ModTime()).Seconds() < o.renotify {
				a.pending = nil
				return nil
			}
			if e != nil && !os.IsNotExist(e) {
				return e
			}
		} else if s.RetryAt.IsZero() {
			due := !s.Attempt.IsZero() && now.Sub(s.Attempt) >= sweepPeriod(a.settings.SweepEvery)
			if s.Pending != key {
				due = due || now.Sub(time.Unix(0, a.pending[0].mtime)) >= settingDuration(a.settings.SweepAfter, 5*time.Minute)
			}
			if !due {
				a.pending = nil
				return nil
			}
		}

		rec, e := readOwner(o.box)
		if e != nil {
			return e
		}
		if rec != nil {
			a.owner = rec.Name
		}
		body, e := os.ReadFile(a.pending[0].path)
		if e != nil {
			return e
		}
		first, _, _ := strings.Cut(sanitize(body), "\n")
		runes := []rune(first)
		a.firstLine = string(runes[:min(200, len(runes))])
		if s.Episode == "" || s.Closed {
			var nonce [16]byte
			if _, e = rand.Read(nonce[:]); e != nil {
				return e
			}
			*s = notifyState{Episode: fmt.Sprintf("%x", nonce)}
		}
		s.Pending, s.Attempt = key, now
		s.RetryAt = time.Time{}
		s.Override = a.override
		if e = saveNotifyState(o.box, *s); e != nil {
			return e
		}
		if send {
			if e = os.WriteFile(filepath.Join(o.box, "state", "notified"), nil, 0600); e != nil {
				return e
			}
		}
		// Start under the pending lock so the count and first message
		// correspond to this invocation. The hook runs after lock release.
		a.started = time.Now()
		a.cmd, a.launchErr = startNotify(o, a)
		return nil
	})
	return
}

func startNotify(o options, a notifyAttempt) (*exec.Cmd, error) {
	cmd := exec.Command("sh", "-c", a.settings.Command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Dir = o.root
	cmd.Env = append(os.Environ(),
		"IBX_BOX="+o.box,
		"IBX_OWNER="+a.owner,
		"IBX_PENDING="+strconv.Itoa(len(a.pending)),
		"IBX_FIRST_ID="+a.pending[0].id,
		"IBX_FIRST_LINE="+strings.ReplaceAll(a.firstLine, "\x00", "�"),
		"IBX_ID="+a.override.ID,
	)
	cmd.Stdout, cmd.Stderr = o.stdout, o.stderr
	return cmd, cmd.Start()
}

func waitNotify(a notifyAttempt) (int, time.Duration) {
	if a.launchErr != nil {
		return 127, time.Since(a.started)
	}
	cmd := a.cmd
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	timer := time.NewTimer(time.Until(a.started.Add(settingDuration(a.settings.Timeout, 10*time.Second))))
	defer timer.Stop()
	code := 0
	select {
	case err := <-done:
		if err != nil {
			code = cmd.ProcessState.ExitCode()
			if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				code = 128 + int(status.Signal())
			}
		}
	case <-timer.C:
		syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
		code = 124
	}
	return code, time.Since(a.started)
}

func appendNotifyLog(o options, a notifyAttempt, code int, elapsed time.Duration) error {
	f, err := os.OpenFile(filepath.Join(o.root, "notify.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(struct {
		Time     string `json:"time"`
		Box      string `json:"box"`
		Owner    string `json:"owner"`
		Pending  int    `json:"pending"`
		Code     int    `json:"exit_code"`
		Duration int64  `json:"duration_ms"`
	}{a.started.Format(time.RFC3339), o.box, a.owner, len(a.pending), code, elapsed.Milliseconds()})
}

func notifyBox(o options, id string, send bool) (int, error) {
	f, err := os.OpenFile(filepath.Join(o.box, "state", "notify.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		if !send && os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()
	config, err := loadNotify(o.root)
	if err != nil {
		return 0, err
	}
	// Acquire the attempt lock before taking the box flock for the snapshot.
	// Callers publish and release their locks before entering notifyBox.
	mode := syscall.LOCK_EX
	if !send {
		mode |= syscall.LOCK_NB
	}
	for {
		err = syscall.Flock(int(f.Fd()), mode)
		if !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	if !send && errors.Is(err, syscall.EWOULDBLOCK) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	a, err := prepareNotify(o, id, send, config)
	if err != nil || len(a.pending) == 0 {
		return 0, err
	}
	code, elapsed := waitNotify(a)
	var logErr error
	if a.launchErr == nil {
		logErr = appendNotifyLog(o, a, code, elapsed)
	}
	s := a.state
	if code == 0 {
		s.Closed = true
	}
	if code == 75 {
		s.RetryAt = time.Now().Add(settingDuration(a.settings.Retry, 30*time.Second))
	}
	var fallbackErr error
	if code != 0 && code != 75 && !s.Forwarded && a.settings.Fallback != "" {
		s.Forwarded, fallbackErr = storeFallback(o, a, code)
	}
	stateErr := locked(o.box, func() error {
		current, e := readNotifyState(o.box)
		if e != nil || current.Episode != s.Episode {
			return e
		}
		if code != 0 {
			if e = unlink(filepath.Join(o.box, "state", "notified")); e != nil {
				return e
			}
		}
		return saveNotifyState(o.box, s)
	})
	if send && code == 75 {
		// Release the attempt lock before waking the retry scheduler.
		f.Close()
		select {
		case o.wake <- struct{}{}:
		default:
		}
	}
	if err = errors.Join(a.launchErr, logErr, fallbackErr, stateErr); err != nil {
		if send && code != 0 {
			fmt.Fprintln(o.stderr, "warning:", err)
		} else {
			return 0, err
		}
	}
	switch code {
	case 0:
		return 0, nil
	case 75:
		return 75, nil
	default:
		if send {
			fmt.Fprintln(o.stderr, "warning: stored, notification failed")
		}
		return 3, nil
	}
}

func storeFallback(o options, a notifyAttempt, code int) (bool, error) {
	ids, err := os.ReadFile(filepath.Join(o.box, "state", "fallback-ids"))
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	ledger := "\n" + string(ids)
	onlyFallback := true
	for _, v := range a.pending {
		if !strings.Contains(ledger, "\n"+v.id+"\n") {
			onlyFallback = false
			break
		}
	}
	if onlyFallback {
		return false, nil
	}
	box := absolute(o.root, a.settings.Fallback)
	id := fmt.Sprintf("notify-%x", sha256.Sum256([]byte(o.box+"\x00"+a.state.Episode)))
	if err = setup(box, false); err != nil {
		return false, err
	}
	temp, err := stageMessage(box, []byte(fmt.Sprintf("notification failed for %s at %s (exit %d)\n",
		o.box, a.started.Format(time.RFC3339), code)))
	if err != nil {
		return false, err
	}
	defer os.Remove(temp)
	err = locked(box, func() error {
		path := filepath.Join(box, "state", "fallback-ids")
		b, e := os.ReadFile(path)
		if e != nil && !os.IsNotExist(e) {
			return e
		}
		if !strings.Contains("\n"+string(b), "\n"+id+"\n") {
			if e = atomicWrite(path, append(b, []byte(id+"\n")...)); e != nil {
				return e
			}
		}
		for _, dir := range []string{"pending", "holding", "done"} {
			if _, e = os.Stat(filepath.Join(box, dir, id+".msg")); e == nil {
				return nil
			}
			if !os.IsNotExist(e) {
				return e
			}
		}
		if e = resetNotifyIfEmpty(box); e != nil {
			return e
		}
		return linkFile(temp, filepath.Join(box, "pending", id+".msg"))
	})
	return err == nil, err
}

func sweepNotifications(ctx context.Context, o options) {
	o.stdout, o.stderr = io.Discard, io.Discard
	var running sync.Map
	var hooks sync.WaitGroup
	defer hooks.Wait()

	for {
		delay := deliverScheduled(ctx, o, &hooks)
		c, err := loadNotify(o.root)
		if err != nil {
			fmt.Fprintln(os.Stderr, "notification sweep:", err)
		} else {
			delay = min(delay, sweepPeriod(c.SweepEvery))
			for box := range c.Boxes {
				delay = min(delay, sweepPeriod(c.forBox(o.root, absolute(o.root, box)).SweepEvery))
			}
			err = eachBox(o.root, func(box string) {
				if ctx.Err() != nil || archived(box) {
					return
				}
				settings := c.forBox(o.root, box)
				state, e := readNotifyState(box)
				if e != nil {
					fmt.Fprintln(os.Stderr, "notification sweep:", e)
					return
				}
				if settings.Command == "" && state.RetryAt.IsZero() {
					return
				}
				if !state.RetryAt.IsZero() && time.Now().Before(state.RetryAt) {
					delay = min(delay, time.Until(state.RetryAt))
					return
				}
				if _, busy := running.LoadOrStore(box, struct{}{}); busy {
					return
				}
				hooks.Add(1)
				go func() {
					defer hooks.Done()
					perBox := o
					perBox.box = box
					code, e := notifyBox(perBox, "", false)
					if e != nil {
						fmt.Fprintln(os.Stderr, "notification sweep:", e)
					}
					running.Delete(box)
					if code == 75 {
						select {
						case o.wake <- struct{}{}:
						default:
						}
					}
				}()
			})
			if err != nil {
				fmt.Fprintln(os.Stderr, "notification sweep:", err)
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-o.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// resetNotifyIfEmpty runs under the pending-directory flock.
func resetNotifyIfEmpty(box string) error {
	if _, err := os.Stat(statePath(box)); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	pending, err := entries(filepath.Join(box, "pending"))
	if err != nil || len(pending) > 0 {
		return err
	}
	if err = unlink(statePath(box)); err != nil {
		return err
	}
	return unlink(filepath.Join(box, "state", "notified"))
}
