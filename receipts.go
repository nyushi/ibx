package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type messageMeta struct {
	ReplyTo  string     `json:"reply_to,omitempty"`
	AckNote  *string    `json:"ack_note,omitempty"`
	AckTime  *time.Time `json:"ack_time,omitempty"`
	Reported bool       `json:"reported"`
}

func metaPath(box, id string) string {
	return filepath.Join(box, "state", "meta", id+".json")
}

func writeJSON(path string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return atomicWrite(path, append(b, '\n'))
}

func readMeta(box, id string) (*messageMeta, error) {
	b, err := os.ReadFile(metaPath(box, id))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var meta messageMeta
	if err := json.Unmarshal(b, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func writeMeta(box, id string, meta *messageMeta) error {
	path := metaPath(box, id)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return writeJSON(path, meta)
}

// publishWithMeta writes metadata first so a successful publication has
// it even if the daemon is killed immediately afterward. Restore previous
// metadata if publication fails while the daemon is still running.
func publishWithMeta(box, id string, previous, next *messageMeta, publish func() error) error {
	replace := func(meta *messageMeta) error {
		if meta == nil {
			return unlink(metaPath(box, id))
		}
		return writeMeta(box, id, meta)
	}
	if err := replace(next); err != nil {
		return err
	}
	if err := publish(); err != nil {
		return errors.Join(err, replace(previous))
	}
	return nil
}

func publishWithReplyTo(box, id, target string, publish func() error) error {
	previous, err := readMeta(box, id)
	if err != nil {
		return err
	}
	var next messageMeta
	if previous != nil {
		next = *previous
	}
	if target == next.ReplyTo {
		return publish()
	}
	next.ReplyTo = target
	if target == "" && next.AckTime == nil && !next.Reported {
		return publishWithMeta(box, id, previous, nil, publish)
	}
	return publishWithMeta(box, id, previous, &next, publish)
}

func saveReceipt(box, id, note string, move func() error) error {
	previous, err := readMeta(box, id)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	next := messageMeta{AckNote: &note, AckTime: &now}
	if previous != nil {
		next.ReplyTo = previous.ReplyTo
	}
	return publishWithMeta(box, id, previous, &next, move)
}

func missingEntry(err error) bool {
	return os.IsNotExist(err) || errors.Is(err, syscall.ENOTDIR)
}

func findMessage(o options, visit func(box string) (bool, error)) (bool, error) {
	var found bool
	var visitErr error
	err := eachBox(o.root, func(box string) {
		if !found && visitErr == nil {
			found, visitErr = visit(box)
		}
	})
	return found, errors.Join(visitErr, err)
}

func markReported(o options) (int, error) {
	mark := func(box string) (bool, error) {
		var marked bool
		err := locked(box, func() error {
			if info, err := os.Stat(filepath.Join(box, "done", o.arg+".msg")); missingEntry(err) {
				return nil
			} else if err != nil {
				return err
			} else if !info.Mode().IsRegular() {
				return nil
			}
			meta, err := readMeta(box, o.arg)
			if err != nil {
				return err
			}
			if meta == nil {
				meta = &messageMeta{}
			}
			meta.Reported = true
			if err := writeMeta(box, o.arg, meta); err != nil {
				return err
			}
			marked = true
			return nil
		})
		return marked, err
	}
	var found bool
	var err error
	if o.box == "" {
		found, err = findMessage(o, mark)
	} else {
		found, err = mark(o.box)
		if os.IsNotExist(err) {
			err = nil
		}
	}
	if err == nil && !found {
		err = fmt.Errorf("not done: %s", o.arg)
	}
	return 0, err
}

func reply(o options) (int, error) {
	var target string
	var exists bool
	found, err := findMessage(o, func(box string) (bool, error) {
		err := locked(box, func() error {
			present, err := hasDuplicate(box, o.arg, 0, time.Now())
			if err != nil || !present {
				return err
			}
			exists = true
			meta, err := readMeta(box, o.arg)
			if err == nil && meta != nil {
				target = meta.ReplyTo
			}
			return err
		})
		return target != "", err
	})
	if err != nil {
		return 0, err
	}
	if !found {
		if exists {
			return 0, fmt.Errorf("message %s has no reply destination", o.arg)
		}
		return 0, fmt.Errorf("unknown message id: %s", o.arg)
	}
	o.box = absolute(o.root, target)
	// An explicit reply body of "-" is data, not a stdin sentinel.
	config, err := loadNotify(o.root)
	if err != nil {
		return 0, err
	}
	if err := o.useDedupeConfig(config); err != nil {
		return 0, err
	}
	return sendOne(o, config, []byte(o.body), false)
}

func listMessages(o options) error {
	if o.scheduled {
		es, err := scheduledEntries(o.box)
		if err != nil {
			return err
		}
		sortScheduled(es)
		for _, entry := range es {
			if err := listEntry(o, entry.id, entry.path, &entry); err != nil {
				return err
			}
		}
		return nil
	}
	dir := "pending"
	if o.done {
		dir = "done"
	} else if o.holding {
		dir = "holding"
	}
	es, err := entries(filepath.Join(o.box, dir))
	if err != nil {
		return err
	}
	cutoff := nowSeconds() - o.since
	for _, entry := range es {
		if o.hasSince && entry.changed < cutoff {
			continue
		}
		if err := listEntry(o, entry.id, entry.path, nil); err != nil {
			return err
		}
	}
	return nil
}

func listEntry(o options, id, path string, scheduled *scheduledEntry) error {
	var body []byte
	var meta *messageMeta
	err := locked(o.box, func() error {
		var err error
		body, err = os.ReadFile(path)
		if err != nil {
			return err
		}
		if o.listJSON || o.done || o.holding {
			meta, err = readMeta(o.box, id)
		}
		return err
	})
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if o.listJSON {
		record := struct {
			ID        string     `json:"id"`
			FirstLine string     `json:"first_line"`
			DeliverAt *time.Time `json:"deliver_at,omitempty"`
			*messageMeta
		}{ID: id, FirstLine: firstLine(sanitize(body)), messageMeta: meta}
		if scheduled != nil {
			record.DeliverAt = &scheduled.due
		}
		return json.NewEncoder(o.stdout).Encode(record)
	}
	if scheduled != nil {
		return printScheduled(o.stdout, *scheduled, body)
	}
	if meta == nil || o.format == "json" {
		return output(o, id, body)
	}
	var note, ackTime, reported string
	if meta.AckNote != nil {
		note = escapeTSV(*meta.AckNote)
	}
	if meta.AckTime != nil {
		ackTime = meta.AckTime.Format(time.RFC3339Nano)
	}
	if meta.Reported {
		reported = "reported"
	}
	_, err = fmt.Fprintf(o.stdout, "%s\t%s\t%s\t%s\t%s\n", id, escapeTSV(sanitize(body)), note, ackTime, reported)
	return err
}

func collect(o options) error {
	if !o.hasKeep {
		config, err := loadNotify(o.root)
		if err != nil {
			return err
		}
		if config.Keep != "" {
			o.keep, err = retentionDuration(config.Keep)
			if err != nil {
				return fmt.Errorf("ibx.json: invalid keep: %v", err)
			}
		}
	}
	n := 0
	err := locked(o.box, func() error {
		now := nowSeconds()
		for _, dir := range []string{"done", "tmp"} {
			dirPath := filepath.Join(o.box, dir)
			files, err := os.ReadDir(dirPath)
			if err != nil {
				return err
			}
			for _, f := range files {
				id, message := strings.CutSuffix(f.Name(), ".msg")
				if dir == "done" && !message {
					continue
				}
				p := filepath.Join(dirPath, f.Name())
				info, err := os.Stat(p)
				if os.IsNotExist(err) {
					continue
				}
				if err != nil {
					return err
				}
				age, stamp := 3600.0, float64(info.ModTime().UnixNano())/1e9
				if dir == "done" {
					age, stamp = o.keep, changed(info)
				}
				if !info.Mode().IsRegular() || now-stamp <= age {
					continue
				}
				if err := unlink(p); err != nil {
					return err
				}
				n++
				if dir == "done" {
					if err := collectMeta(o.box, id); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(o.stdout, n)
	return err
}

func collectMeta(box, id string) error {
	present, err := hasDuplicate(box, id, 0, time.Now())
	if err != nil || present {
		return err
	}
	return unlink(metaPath(box, id))
}

func retentionDuration(value string) (float64, error) {
	if d, err := time.ParseDuration(value); err == nil && d >= 0 {
		return d.Seconds(), nil
	}
	return duration(value)
}
