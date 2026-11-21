package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func bodyID(body []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(body))[:16]
}

func (o *options) useDedupeConfig(config notifyConfig) error {
	if o.hasDedupeTTL || config.DedupeTTL == "" {
		return nil
	}
	ttl, err := time.ParseDuration(config.DedupeTTL)
	if err != nil || ttl < 0 {
		return errors.New("ibx.json: invalid dedupe_ttl")
	}
	o.dedupeTTL = ttl
	return nil
}

func sendMessages(o options) (int, error) {
	config, err := loadNotify(o.root)
	if err != nil {
		return 0, err
	}
	if o.command != "put" || len(config.Tee) != 0 {
		if err := o.useDedupeConfig(config); err != nil {
			return 0, err
		}
	}
	if o.at != "" {
		o.delivery, err = parseDelivery(o.at, time.Now())
		if err != nil {
			return 0, err
		}
	}
	if !o.lines {
		body, err := readBody(o)
		if err != nil {
			return 0, err
		}
		return sendOne(o, config, body, false)
	}

	scanner := bufio.NewScanner(o.stdin)
	scanner.Split(splitBodyLine)
	// Allow room for a TSV destination and ID in addition to the body.
	limit := int(min(bodyLimit(o.maxSize), int64(int(^uint(0)>>1))-8192)) + 8192
	scanner.Buffer(make([]byte, 4096), limit)
	code, line := 0, 0
	for scanner.Scan() {
		line++
		if err := o.ctx.Err(); err != nil {
			return code, err
		}
		body := scanner.Bytes()
		if len(body) == 0 {
			continue
		}
		perLine := o
		if o.tsv {
			box, id, payload, err := parseSendTSV(o.root, body, line)
			if err != nil {
				return code, err
			}
			perLine.box, perLine.id, perLine.hasID = box, id, true
			body = payload
		}
		result, err := sendOne(perLine, config, body, o.tsv)
		if err != nil {
			return code, fmt.Errorf("line %d: %w", line, err)
		}
		code = notificationResult(code, result)
	}
	if err := scanner.Err(); err != nil {
		return code, usageError(fmt.Sprintf("line %d: %v", line+1, err))
	}
	return code, nil
}

func parseSendTSV(root string, body []byte, line int) (string, string, []byte, error) {
	path, rest, first := bytes.Cut(body, []byte{'\t'})
	id, payload, second := bytes.Cut(rest, []byte{'\t'})
	if !first || !second || len(path) == 0 {
		return "", "", nil, usageError(fmt.Sprintf("line %d: expected box, id, and body separated by tabs", line))
	}
	return absolute(root, string(path)), string(id), payload, nil
}

// Split on LF alone so the body hash includes every other byte, including CR.
func splitBodyLine(data []byte, atEOF bool) (int, []byte, error) {
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[:i], nil
	}
	if atEOF && len(data) != 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}

func notificationResult(a, b int) int {
	switch {
	case a == 3 || b == 3:
		return 3
	case a == 75 || b == 75:
		return 75
	default:
		return 0
	}
}

func sendOne(o options, config notifyConfig, body []byte, allowEmpty bool) (int, error) {
	id := o.id
	if !o.hasID {
		if (o.lines || o.command == "reply") && !o.idFromBody {
			var randomID [16]byte
			if _, err := rand.Read(randomID[:]); err != nil {
				return 0, err
			}
			id = fmt.Sprintf("%x", randomID)
		} else {
			id = bodyID(body)
		}
	}
	isPut := o.command == "put"
	stored, err := publishSend(o, id, body, allowEmpty)
	if err != nil {
		return 0, err
	}
	if !stored {
		if isPut {
			fmt.Fprintln(o.stderr, "duplicate")
		} else {
			return 0, nil
		}
	}
	_, outputErr := fmt.Fprintln(o.stdout, id)
	if !stored || o.at != "" {
		return 0, outputErr
	}
	code, err := notifyArrival(o, config, id, body, !isPut)
	return code, errors.Join(outputErr, err)
}

func publishSend(o options, id string, body []byte, allowEmpty bool) (bool, error) {
	if (!allowEmpty && len(body) == 0) || len(body) > o.maxSize {
		return false, usageError("body is empty or exceeds maximum size")
	}
	if !idPattern.MatchString(id) {
		return false, usageError("invalid id")
	}
	return storeMessage(o, id, body, o.command == "put")
}

func notifyArrival(o options, config notifyConfig, id string, body []byte, primary bool) (int, error) {
	destinations, err := copyTee(o, config, id, body)
	var code int
	if primary {
		result, notifyErr := notifyBox(o, id, true)
		code = result
		err = errors.Join(err, notifyErr)
	}
	for _, box := range destinations {
		copyOptions := teeOptions(o, box)
		result, notifyErr := notifyBox(copyOptions, id, true)
		code = notificationResult(code, result)
		err = errors.Join(err, notifyErr)
	}
	return code, err
}

// storeMessage is shared by direct sends and tee copies. Legacy put retains
// pending-only dedupe and never replaces an existing pending file.
func storeMessage(o options, id string, body []byte, isPut bool) (stored bool, err error) {
	if err = setup(o.box, false); err != nil {
		return false, err
	}
	temp, err := stageMessage(o.box, body)
	if err != nil {
		return false, err
	}
	defer os.Remove(temp)
	err = locked(o.box, func() error {
		if !isPut {
			duplicate, err := hasDuplicate(o.box, id, o.dedupeTTL, time.Now())
			if err != nil {
				return err
			}
			if duplicate {
				return errSkip
			}
		}

		if o.at == "" {
			if err := resetNotifyIfEmpty(o.box); err != nil {
				return err
			}
		}
		publish := func() error {
			if o.at != "" {
				return storeScheduled(o, id, temp)
			}
			dest := filepath.Join(o.box, "pending", id+".msg")
			if !isPut && o.dedupeTTL > 0 {
				// Replacement of an expired pending entry must have no gap.
				return os.Rename(temp, dest)
			}
			err := linkFile(temp, dest)
			if isPut && os.IsExist(err) {
				return errSkip
			}
			return err
		}
		if isPut {
			return publish()
		}
		return publishWithReplyTo(o.box, id, o.replyTo, publish)
	})
	if errors.Is(err, errSkip) {
		return false, nil
	}
	if err == nil {
		stored = true
		if o.at != "" {
			wakeDaemon(o)
		}
	}
	return stored, err
}

func hasDuplicate(box, id string, ttl time.Duration, now time.Time) (bool, error) {
	for _, dir := range []string{"pending", "holding", "done"} {
		info, err := os.Stat(filepath.Join(box, dir, id+".msg"))
		if missingEntry(err) {
			continue
		}
		if err != nil {
			return false, err
		}
		fresh := now.Sub(info.ModTime()) <= ttl
		if dir == "done" {
			fresh = float64(now.UnixNano())/1e9-changed(info) <= ttl.Seconds()
		}
		if ttl == 0 || fresh {
			return true, nil
		}
	}
	es, err := scheduledEntries(box)
	if err != nil {
		return false, err
	}
	for _, entry := range es {
		if entry.id == id && (ttl == 0 || now.Sub(entry.stored) <= ttl) {
			return true, nil
		}
	}
	return false, nil
}

func wakeDaemon(o options) {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

func firstLine(s string) string {
	first, _, _ := strings.Cut(s, "\n")
	return first
}

var tsvEscaper = strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\r", "\\r", "\t", "\\t")

func escapeTSV(s string) string { return tsvEscaper.Replace(s) }
