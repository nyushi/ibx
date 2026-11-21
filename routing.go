package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

type teeRule struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Match string `json:"match"`
}

func teePath(root, path string) (string, error) {
	rel, err := filepath.Rel(root, absolute(root, path))
	if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", fmt.Errorf("tee path must be under the root: %s", path)
	}
	return rel, nil
}

func manageTee(o options) (int, error) {
	if o.teeList {
		config, err := loadNotify(o.root)
		if err != nil {
			return 0, err
		}
		for _, rule := range config.Tee {
			if _, err := fmt.Fprintf(o.stdout, "%s\t%s\t%s\n", rule.From, rule.To, escapeTSV(rule.Match)); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}
	from, err := teePath(o.root, o.box)
	if err != nil {
		return 0, err
	}
	to, err := teePath(o.root, o.to)
	if err != nil {
		return 0, err
	}
	if _, err := regexp.Compile(o.match); err != nil {
		return 0, fmt.Errorf("invalid tee match: %w", err)
	}
	return 0, changeTee(o, teeRule{from, to, o.match})
}

func changeTee(o options, candidate teeRule) error {
	root, err := os.Open(o.root)
	if err != nil {
		return err
	}
	defer root.Close()
	if err := syscall.Flock(int(root.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	path := filepath.Join(o.root, "ibx.json")
	data, err := os.ReadFile(path)
	var config map[string]json.RawMessage
	if err == nil {
		if err := json.Unmarshal(data, &config); err != nil {
			return fmt.Errorf("ibx.json: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if config == nil {
		config = make(map[string]json.RawMessage)
	}
	rules := []teeRule{}
	if raw := config["tee"]; raw != nil {
		if err := json.Unmarshal(raw, &rules); err != nil {
			return fmt.Errorf("ibx.json: %w", err)
		}
	}
	if o.remove {
		kept := make([]teeRule, 0, len(rules))
		for _, rule := range rules {
			if filepath.Clean(rule.From) != candidate.From || filepath.Clean(rule.To) != candidate.To ||
				(o.hasMatch && candidate.Match != rule.Match) {
				kept = append(kept, rule)
			}
		}
		rules = kept
	} else {
		for _, rule := range rules {
			if rule == candidate {
				return nil
			}
		}
		rules = append(rules, candidate)
		if teeHasCycle(o.root, rules) {
			return errors.New("tee rule would create a cycle")
		}
	}
	config["tee"], err = json.Marshal(rules)
	if err != nil {
		return err
	}
	return writeJSON(path, config)
}

func teeHasCycle(root string, rules []teeRule) bool {
	graph := make(map[string][]string)
	for _, rule := range rules {
		from, to := absolute(root, rule.From), absolute(root, rule.To)
		graph[from] = append(graph[from], to)
	}
	visiting, visited := make(map[string]bool), make(map[string]bool)
	var cycle func(string) bool
	cycle = func(box string) bool {
		if visiting[box] {
			return true
		}
		if visited[box] {
			return false
		}
		visiting[box] = true
		for _, to := range graph[box] {
			if cycle(to) {
				return true
			}
		}
		delete(visiting, box)
		visited[box] = true
		return false
	}
	for from := range graph {
		if cycle(from) {
			return true
		}
	}
	return false
}

func teeOptions(o options, box string) options {
	o.box, o.replyTo, o.fallback = box, "", ""
	o.hasNotify, o.hasRetry, o.hasNotifyTimeout = false, false, false
	o.at = ""
	o.renotify = 300
	return o
}

func copyTee(o options, config notifyConfig, id string, body []byte) ([]string, error) {
	var boxes []string
	var errs []error
	copyOptions := teeOptions(o, "")
	copyOptions.dedupeTTL, copyOptions.hasDedupeTTL = 0, false
	for _, rule := range config.Tee {
		if absolute(o.root, rule.From) != o.box {
			continue
		}
		if rule.Match != "" {
			expr, err := regexp.Compile(rule.Match)
			if err != nil {
				errs = append(errs, fmt.Errorf("invalid tee match: %w", err))
				continue
			}
			if !expr.Match(body) {
				continue
			}
		}
		box := absolute(o.root, rule.To)
		copyOptions.box = box
		if err := copyOptions.useDedupeConfig(config); err != nil {
			return boxes, err
		}
		stored, err := storeMessage(copyOptions, id, body, false)
		if err != nil {
			errs = append(errs, fmt.Errorf("tee to %s: %w", rule.To, err))
		}
		if stored {
			boxes = append(boxes, box)
		}
	}
	return boxes, errors.Join(errs...)
}

func archivePath(box string) string {
	return filepath.Join(box, "state", "archived")
}

func archived(box string) bool {
	_, err := os.Stat(archivePath(box))
	return err == nil
}

func setArchive(o options) error {
	err := locked(o.box, func() error {
		if o.command == "archive" {
			return atomicWrite(archivePath(o.box), nil)
		}
		return unlink(archivePath(o.box))
	})
	if err == nil && o.command == "unarchive" {
		wakeDaemon(o)
	}
	return err
}

func listBoxes(o options) (int, error) {
	type record struct {
		Box    string `json:"box"`
		Unread int    `json:"unread"`
		Age    *int64 `json:"oldest_age_seconds"`
	}
	var resultErr error
	now := time.Now()
	err := eachBox(o.root, func(box string) {
		if resultErr != nil || (!o.all && archived(box)) {
			return
		}
		es, err := entries(filepath.Join(box, "pending"))
		if err != nil {
			resultErr = err
			return
		}
		if o.unread && len(es) == 0 {
			return
		}
		item := record{Box: relativeBox(o.root, box), Unread: len(es)}
		if len(es) != 0 {
			age := int64(max(0, now.Sub(time.Unix(0, es[0].mtime))/time.Second))
			item.Age = &age
		}
		switch {
		case o.listJSON || o.format == "json":
			resultErr = json.NewEncoder(o.stdout).Encode(item)
		case item.Age == nil:
			_, resultErr = fmt.Fprintf(o.stdout, "%s\t%d\t\n", item.Box, item.Unread)
		default:
			_, resultErr = fmt.Fprintf(o.stdout, "%s\t%d\t%d\n", item.Box, item.Unread, *item.Age)
		}
	})
	return 0, errors.Join(err, resultErr)
}
