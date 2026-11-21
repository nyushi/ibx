package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func absolute(dir, path string) string {
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(dir, path)
}

func (o options) bulkOwner() bool {
	return o.command == "own" && (o.sub == "list" || o.sub == "pass-all" || o.sub == "drop-all")
}

func eachBox(root string, visit func(string)) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if info, e := os.Stat(filepath.Join(path, "pending")); e == nil && info.IsDir() {
			visit(path)
		}

		return nil
	})
}

func relativeBox(root, box string) string {
	rel, e := filepath.Rel(root, box)
	if e != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return box
	}
	return rel
}

func bulkOwner(o options) (int, error) {
	code := 0
	var outputErr error
	err := eachBox(o.root, func(box string) {
		if outputErr != nil || archived(box) {
			return
		}
		path := relativeBox(o.root, box)
		var rec *owner
		var unread int
		// Read the record atomically, including for boxes whose lock cannot
		// be opened. A matching owner must get a result for those boxes too.
		rec, e := readOwner(box)
		if e == nil && (rec == nil || rec.Name != o.as) {
			return
		}
		switch {
		case e != nil:
		case o.sub == "list":
			e = locked(box, func() error {
				var err error
				rec, err = readOwner(box)
				if err != nil || rec == nil || rec.Name != o.as {
					return err
				}
				es, err := entries(filepath.Join(box, "pending"))
				unread = len(es)
				return err
			})
			if e == nil && rec != nil && rec.Name == o.as {
				if o.format == "json" {
					outputErr = json.NewEncoder(o.stdout).Encode(struct {
						Box    string `json:"box"`
						Owner  string `json:"owner"`
						Unread int    `json:"unread"`
					}{path, o.as, unread})
				} else {
					_, outputErr = fmt.Fprintf(o.stdout, "%s\t%d\n", path, unread)
				}
				return
			}
		default:
			perBox := o
			perBox.box, perBox.stdout = box, io.Discard
			if o.sub == "pass-all" {
				_, e = ownPass(perBox)
			} else {
				_, e = ownDrop(perBox)
			}
		}
		if o.sub == "list" {
			if e != nil {
				code = 1
				_, outputErr = fmt.Fprintf(o.stderr, "%s: %v\n", path, e)
			}
			return
		}
		result := "ok"
		if e != nil {
			code = 1
			result = e.Error()
		}
		_, outputErr = fmt.Fprintf(o.stdout, "%s\t%s\n", path, result)
	})
	if outputErr != nil {
		return 1, outputErr
	}
	return code, err
}
