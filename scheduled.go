package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const scheduleTimeFormat = "2006-01-02T15:04:05.000000000Z"

var lastScheduledStorage atomic.Int64

func scheduledStorageTime() time.Time {
	for {
		last := lastScheduledStorage.Load()
		next := max(time.Now().UnixNano(), last+1)
		if lastScheduledStorage.CompareAndSwap(last, next) {
			return time.Unix(0, next).UTC()
		}
	}
}

type scheduledEntry struct {
	id, path, box string
	due, stored   time.Time
}

func parseDelivery(value string, now time.Time) (time.Time, error) {
	if value, ok := strings.CutPrefix(value, "+"); ok {
		d, err := time.ParseDuration(value)
		if err == nil && d >= 0 && !strings.HasPrefix(value, "+") {
			return now.Add(d), nil
		}
	} else if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t, nil
	}
	return time.Time{}, usageError("invalid --at: expected RFC 3339 or +duration")
}

type scheduledNotify struct {
	Override notifyOverride `json:"override"`
	Renotify float64        `json:"renotify_after_seconds"`
}

func storeScheduled(o options, id, temp string) error {
	dir := filepath.Join(o.box, "scheduled")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	dest := filepath.Join(dir, o.delivery.UTC().Format(scheduleTimeFormat)+"!"+
		scheduledStorageTime().Format(scheduleTimeFormat)+"!"+id+".msg")
	var override notifyOverride
	if o.hasNotify {
		override.Command = &o.notify
	}
	if o.hasRetry {
		override.Retry = o.retryAfter.String()
	}
	if o.hasNotifyTimeout {
		override.Timeout = sleepDuration(o.notifyTimeout).String()
	}
	override.Fallback = o.fallback
	if override != (notifyOverride{}) || o.renotify != 300 {
		if err := writeJSON(dest+".notify.json", scheduledNotify{override, o.renotify}); err != nil {
			return err
		}
	}
	if err := linkFile(temp, dest); err != nil {
		unlink(dest + ".notify.json")
		return err
	}
	return nil
}

func scheduledEntries(box string) ([]scheduledEntry, error) {
	dir := filepath.Join(box, "scheduled")
	files, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var es []scheduledEntry
	for _, f := range files {
		base, ok := strings.CutSuffix(f.Name(), ".msg")
		if !ok || !f.Type().IsRegular() {
			continue
		}
		parts := strings.SplitN(base, "!", 3)
		if len(parts) != 3 || !idPattern.MatchString(parts[2]) {
			continue
		}
		due, e1 := time.Parse(scheduleTimeFormat, parts[0])
		stored, e2 := time.Parse(scheduleTimeFormat, parts[1])
		if e1 != nil || e2 != nil {
			continue
		}
		es = append(es, scheduledEntry{parts[2], filepath.Join(dir, f.Name()), box, due, stored})
	}
	return es, nil
}

func sortScheduled(es []scheduledEntry) {
	sort.Slice(es, func(i, j int) bool {
		a, b := es[i], es[j]
		if !a.due.Equal(b.due) {
			return a.due.Before(b.due)
		}
		if !a.stored.Equal(b.stored) {
			return a.stored.Before(b.stored)
		}
		return a.path < b.path
	})
}

// deliverScheduled performs publication in time order before the notification
// sweep. A slow hook cannot delay publication of the next scheduled message.
func deliverScheduled(ctx context.Context, o options, hooks *sync.WaitGroup) time.Duration {
	delay := 10 * time.Second
	var es []scheduledEntry
	err := eachBox(o.root, func(box string) {
		found, err := scheduledEntries(box)
		if err != nil {
			fmt.Fprintln(os.Stderr, "scheduled delivery:", err)
		}
		es = append(es, found...)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "scheduled delivery:", err)
	}
	sortScheduled(es)
	// Run this batch's notification attempts in publication order. The
	// next scheduler iteration can still publish while the hooks run.
	type hint struct {
		o  options
		id string
	}
	var hints []hint
	config, configErr := loadNotify(o.root)
	for _, entry := range es {
		if ctx.Err() != nil {
			break
		}
		if wait := time.Until(entry.due); wait > 0 {
			delay = min(delay, wait)
			break
		}
		perBox := o
		perBox.box = entry.box
		// Only the originating send's persisted overrides apply to this hint.
		perBox.hasNotify = false
		body, err := publishScheduled(&perBox, entry)
		if errors.Is(err, errSkip) || os.IsNotExist(err) {
			continue
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "scheduled delivery:", err)
			continue
		}
		hints = append(hints, hint{perBox, entry.id})
		if configErr != nil {
			fmt.Fprintln(os.Stderr, "scheduled tee:", configErr)
			continue
		}
		destinations, err := copyTee(perBox, config, entry.id, body)
		if err != nil {
			fmt.Fprintln(os.Stderr, "scheduled tee:", err)
		}
		for _, box := range destinations {
			copyOptions := teeOptions(perBox, box)
			hints = append(hints, hint{copyOptions, entry.id})
		}
	}
	if len(hints) != 0 {
		hooks.Add(1)
		go func() {
			defer hooks.Done()
			for _, hint := range hints {
				if ctx.Err() != nil {
					return
				}
				if _, err := notifyBox(hint.o, hint.id, true); err != nil {
					fmt.Fprintln(os.Stderr, "scheduled notify:", err)
				}
			}
		}()
	}
	return delay
}

func publishScheduled(o *options, entry scheduledEntry) (body []byte, err error) {
	err = locked(o.box, func() error {
		dest := filepath.Join(o.box, "pending", entry.id+".msg")
		if _, err := os.Lstat(dest); err == nil {
			// Legacy put or TTL-based reuse can introduce the same ID
			// while it is scheduled. Keep both bodies until pending clears.
			return errSkip
		} else if !os.IsNotExist(err) {
			return err
		}
		var err error
		body, err = os.ReadFile(entry.path)
		if err != nil {
			return err
		}
		notifyPath := entry.path + ".notify.json"
		notifyData, err := os.ReadFile(notifyPath)
		if err == nil {
			var settings scheduledNotify
			if err := json.Unmarshal(notifyData, &settings); err != nil {
				return err
			}
			if settings.Override.Command != nil {
				o.notify, o.hasNotify = *settings.Override.Command, true
			}
			if settings.Override.Retry != "" {
				o.retryAfter, o.hasRetry = settingDuration(settings.Override.Retry, 0), true
			}
			if settings.Override.Timeout != "" {
				o.notifyTimeout, o.hasNotifyTimeout = settingDuration(settings.Override.Timeout, 0).Seconds(), true
			}
			o.fallback, o.renotify = settings.Override.Fallback, settings.Renotify
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := resetNotifyIfEmpty(o.box); err != nil {
			return err
		}
		now := time.Now()
		if err := os.Chtimes(entry.path, now, now); err != nil {
			return err
		}
		// Rename removes the scheduled name in the same atomic operation
		// that creates the pending name. A restart cannot replay it.
		if err := os.Rename(entry.path, dest); err != nil {
			return err
		}
		if err := unlink(notifyPath); err != nil {
			fmt.Fprintln(os.Stderr, "scheduled notify cleanup:", err)
		}
		return nil
	})
	return body, err
}

func printScheduled(w io.Writer, entry scheduledEntry, body []byte) error {
	_, err := fmt.Fprintf(w, "%s\t%s\t%s\n", entry.id, entry.due.Format(time.RFC3339Nano), escapeTSV(firstLine(sanitize(body))))
	return err
}
