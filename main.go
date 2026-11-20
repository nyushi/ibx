// ibx stores messages atomically in a local directory inbox.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

// Tests may stop publication after the temporary file has been synced.
var linkFile = os.Link
var idPattern = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._-]{0,127}$`)
var durationPattern = regexp.MustCompile(`^(\d+(?:\.\d*)?|\.\d+)([smhd]?)$`)

type options struct {
	command, box, format, id, body, notify, sub, as, to, arg string
	hasID, hasBody, done, hasSince, hasTimeout, hasTTL       bool
	steal, hold, holding                                     bool
	runArgs                                                  []string
	maxSize, maximum                                         int
	poll, timeout, since, keep, renotify, notifyTimeout, ttl float64
}
type usageError string

func (e usageError) Error() string { return string(e) }

// exitError carries a specific process exit code (owner conflicts and
// ownership absence use 5 and 6, past the original 0-4 assignments).
type exitError struct {
	code int
	msg  string
}

func (e *exitError) Error() string { return e.msg }
func duration(s string) (float64, error) {
	m := durationPattern.FindStringSubmatch(s)
	if m == nil {
		return 0, usageError("expected a nonnegative duration")
	}
	v, e := seconds(m[1])
	if e != nil {
		return 0, e
	}
	v *= map[string]float64{"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}[m[2]]
	if math.IsInf(v, 0) {
		return 0, usageError("duration is too large")
	}
	return v, nil
}
func seconds(s string) (float64, error) {
	v, e := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if e != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0, usageError("expected finite nonnegative seconds")
	}
	return v, nil
}
func parse(args []string) (o options, err error) {
	o = options{box: os.Getenv("IBX_BOX"), format: "tsv", maxSize: 1024 * 1024, poll: 1, keep: 7 * 86400, renotify: 300, notifyTimeout: 10, notify: os.Getenv("IBX_NOTIFY_CMD")}
	literal := false
	runCollect := false
	for i := 0; i < len(args); i++ {
		a := args[i]
		if runCollect {
			o.runArgs = append(o.runArgs, a)
			continue
		}
		if !literal && a == "--" {
			if o.command == "own" && o.sub == "run" {
				runCollect = true
			} else {
				literal = true
			}
			continue
		}
		if !literal && (a == "-h" || a == "--help") {
			return o, usageError("help")
		}
		if !literal && strings.HasPrefix(a, "--") {
			key, val, has := strings.Cut(a, "=")
			common := key == "--box" || key == "--format"
			allowed := common
			switch o.command {
			case "put", "send":
				allowed = allowed || key == "--id" || key == "--max-size"
			case "take":
				allowed = allowed || key == "--max" || key == "--as" || key == "--hold"
			case "wait":
				allowed = allowed || key == "--max" || key == "--poll" || key == "--timeout"
			case "stream":
				allowed = allowed || key == "--poll" || key == "--as"
			case "list":
				allowed = allowed || key == "--done" || key == "--since" || key == "--holding"
			case "gc":
				allowed = allowed || key == "--keep"
			case "ack", "release":
				allowed = allowed || key == "--as"
			case "own":
				switch o.sub {
				case "claim":
					allowed = allowed || key == "--as" || key == "--ttl" || key == "--steal"
				case "pass":
					allowed = allowed || key == "--as" || key == "--to"
				case "drop":
					allowed = allowed || key == "--as"
				case "run":
					allowed = allowed || key == "--as" || key == "--ttl"
				}
			}
			if o.command == "send" {
				allowed = allowed || key == "--notify-cmd" || key == "--notify-timeout" || key == "--renotify-after"
			}
			if !allowed {
				return o, usageError("unrecognized argument: " + key)
			}
			if key == "--done" || key == "--hold" || key == "--holding" || key == "--steal" {
				if has {
					return o, usageError(key + " takes no value")
				}
				switch key {
				case "--done":
					o.done = true
				case "--hold":
					o.hold = true
				case "--holding":
					o.holding = true
				case "--steal":
					o.steal = true
				}
				continue
			}
			if !has {
				i++
				if i >= len(args) {
					return o, usageError("missing value: " + key)
				}
				val = args[i]
			}
			switch key {
			case "--box":
				o.box = val
			case "--format":
				if val != "tsv" && val != "json" {
					return o, usageError("invalid format")
				}
				o.format = val
			case "--id":
				o.id = val
				o.hasID = true
			case "--as":
				if val == "" {
					return o, usageError("--as must not be empty")
				}
				o.as = val
			case "--to":
				if val == "" {
					return o, usageError("--to must not be empty")
				}
				o.to = val
			case "--notify-cmd":
				o.notify = val
			case "--max", "--max-size":
				v, e := strconv.Atoi(strings.TrimSpace(val))
				if e != nil || v <= 0 {
					return o, usageError("expected a positive integer")
				}
				if key == "--max" {
					o.maximum = v
				} else {
					o.maxSize = v
				}
			default:
				var v float64
				var e error
				if key == "--keep" || key == "--since" || key == "--renotify-after" || key == "--ttl" {
					v, e = duration(val)
				} else {
					v, e = seconds(val)
				}
				if e != nil {
					return o, e
				}
				switch key {
				case "--keep":
					o.keep = v
				case "--ttl":
					if v > maxOwnerTTL {
						return o, usageError("ttl is too large")
					}
					o.ttl = v
					o.hasTTL = true
				case "--since":
					o.since = v
					o.hasSince = true
				case "--renotify-after":
					o.renotify = v
				case "--poll":
					o.poll = v
				case "--timeout":
					o.timeout = v
					o.hasTimeout = true
				case "--notify-timeout":
					o.notifyTimeout = v
				}
			}
			continue
		}
		if o.command == "" {
			switch a {
			case "put", "take", "list", "wait", "stream", "send", "gc", "own", "ack", "release":
				o.command = a
			default:
				return o, usageError("invalid command")
			}
		} else if o.command == "own" && o.sub == "" {
			switch a {
			case "claim", "pass", "drop", "show", "run":
				o.sub = a
			default:
				return o, usageError("invalid own subcommand")
			}
		} else if (o.command == "ack" || o.command == "release") && o.arg == "" && (!strings.HasPrefix(a, "-") || literal) {
			o.arg = a
		} else if (o.command == "put" || o.command == "send") && !o.hasBody && (!strings.HasPrefix(a, "-") || a == "-" || literal) {
			o.body = a
			o.hasBody = true
		} else {
			return o, usageError("unrecognized argument: " + a)
		}
	}
	if o.command == "" {
		return o, usageError("a command is required")
	}
	if o.box == "" {
		o.box = os.Getenv("IBX_BOX")
	}
	if o.box == "" {
		return o, usageError("a box is required")
	}
	if o.hasSince && !o.done {
		return o, usageError("--since requires --done")
	}
	if o.done && o.holding {
		return o, usageError("--done and --holding conflict")
	}
	if o.command == "own" {
		if o.sub == "" {
			return o, usageError("an own subcommand is required")
		}
		if o.sub != "show" && o.as == "" {
			return o, usageError("--as is required")
		}
		if o.sub == "pass" && o.to == "" {
			return o, usageError("--to is required")
		}
		if o.sub == "run" && len(o.runArgs) == 0 {
			return o, usageError("own run requires a command after --")
		}
	}
	if o.command == "ack" || o.command == "release" {
		if o.arg == "" {
			return o, usageError("a message id is required")
		}
		if !idPattern.MatchString(o.arg) {
			return o, usageError("invalid id")
		}
	}
	o.box, err = filepath.Abs(o.box)
	return
}
func setup(box string, needHolding bool) error {
	for _, n := range []string{"", "tmp", "pending", "done", "state"} {
		if e := os.MkdirAll(filepath.Join(box, n), 0700); e != nil {
			return e
		}
	}
	// holding/ is created on a best-effort basis for commands that do
	// not use it, so a pre-existing non-directory named "holding"
	// cannot break the legacy commands.
	if e := os.MkdirAll(filepath.Join(box, "holding"), 0700); e != nil && needHolding {
		return e
	}
	return nil
}
func unlink(p string) error {
	e := os.Remove(p)
	if os.IsNotExist(e) {
		return nil
	}
	return e
}
func locked(box string, fn func() error) error {
	f, e := os.Open(filepath.Join(box, "pending"))
	if e != nil {
		return e
	}
	defer f.Close()
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); e != nil {
		return e
	}
	return fn()
}
func put(o options) (string, error) {
	if o.hasID && !idPattern.MatchString(o.id) {
		return "", usageError("invalid id")
	}
	var body []byte
	var e error
	if !o.hasBody || o.body == "-" {
		body, e = io.ReadAll(io.LimitReader(os.Stdin, int64(o.maxSize)+1))
	} else {
		body = []byte(o.body)
	}
	if e != nil {
		return "", e
	}
	if len(body) == 0 || len(body) > o.maxSize {
		return "", usageError("body is empty or exceeds maximum size")
	}
	id := o.id
	if !o.hasID {
		id = fmt.Sprintf("%x", sha256.Sum256(body))[:16]
	}
	f, e := os.CreateTemp(filepath.Join(o.box, "tmp"), "")
	if e != nil {
		return "", e
	}
	defer os.Remove(f.Name())
	_, e = f.Write(body)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return "", e
	}
	e = locked(o.box, func() error { return linkFile(f.Name(), filepath.Join(o.box, "pending", id+".msg")) })
	if os.IsExist(e) {
		fmt.Fprintln(os.Stderr, "duplicate")
	} else if e != nil {
		return "", e
	}
	if e = unlink(f.Name()); e != nil {
		return "", e
	}
	_, e = fmt.Fprintln(os.Stdout, id)
	return id, e
}

type entry struct {
	id, path string
	mtime    int64
	changed  float64
}

func changed(s os.FileInfo) float64 {
	v := s.Sys().(*syscall.Stat_t).Ctim
	return float64(v.Sec) + float64(v.Nsec)/1e9
}
func entries(dir string) ([]entry, error) {
	ds, e := os.ReadDir(dir)
	if e != nil {
		return nil, e
	}
	var out []entry
	for _, d := range ds {
		if !strings.HasSuffix(d.Name(), ".msg") {
			continue
		}
		p := filepath.Join(dir, d.Name())
		s, e := os.Stat(p)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return nil, e
		}
		if !s.Mode().IsRegular() {
			continue
		}
		out = append(out, entry{strings.TrimSuffix(d.Name(), ".msg"), p, s.ModTime().UnixNano(), changed(s)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].mtime == out[j].mtime {
			return out[i].id < out[j].id
		}
		return out[i].mtime < out[j].mtime
	})
	return out, nil
}
func output(id string, body []byte, format string) error {
	bodyText := sanitize(body)
	var line string
	if format == "json" {
		line = "{\"id\": " + jsonString(id) + ", \"body\": " + jsonString(bodyText) + "}"
	} else {
		line = id + "\t" + strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\r", "\\r", "\t", "\\t").Replace(bodyText)
	}
	_, e := fmt.Fprintln(os.Stdout, line)
	return e
}

// Consume valid prefixes of malformed sequences as one replacement, as Python does.
func sanitize(b []byte) string {
	var out strings.Builder
	for len(b) > 0 {
		r, n := utf8.DecodeRune(b)
		if r == utf8.RuneError && n == 1 {
			n = 1
			expected := 0
			switch {
			case b[0] >= 0xc2 && b[0] <= 0xdf:
				expected = 2
			case b[0] >= 0xe0 && b[0] <= 0xef:
				expected = 3
			case b[0] >= 0xf0 && b[0] <= 0xf4:
				expected = 4
			}
			for n < expected && n < len(b) {
				v := b[n]
				if v < 0x80 || v > 0xbf {
					break
				}
				if n == 1 && ((b[0] == 0xe0 && v < 0xa0) || (b[0] == 0xed && v >= 0xa0) || (b[0] == 0xf0 && v < 0x90) || (b[0] == 0xf4 && v >= 0x90)) {
					break
				}
				n++
			}
		}
		out.WriteRune(r)
		b = b[n:]
	}
	return out.String()
}

// Match json.dumps's ASCII escaping and separators.
func jsonString(s string) string {
	var out strings.Builder
	out.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"', '\\':
			out.WriteByte('\\')
			out.WriteRune(r)
		case '\b':
			out.WriteString(`\b`)
		case '\f':
			out.WriteString(`\f`)
		case '\n':
			out.WriteString(`\n`)
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		default:
			if r < 32 || r >= 127 {
				if r <= 0xffff {
					fmt.Fprintf(&out, `\u%04x`, r)
				} else {
					r -= 0x10000
					fmt.Fprintf(&out, `\u%04x\u%04x`, 0xd800+(r>>10), 0xdc00+(r&0x3ff))
				}
			} else {
				out.WriteRune(r)
			}
		}
	}
	out.WriteByte('"')
	return out.String()
}

// The box owner record lives in state/owner as JSON. All reads and
// writes happen inside locked(), so a claim race resolves to exactly
// one winner. Ownership is fencing between cooperating callers that
// name themselves with --as; it is not authentication.
type owner struct {
	Name      string  `json:"name"`
	Gen       uint64  `json:"gen"`
	ClaimedAt int64   `json:"claimed_at"`
	RenewedAt int64   `json:"renewed_at"`
	ExpiresAt int64   `json:"expires_at"`
	TTL       float64 `json:"ttl_seconds"`
}

const defaultOwnerTTL = 1800 // 30m

// maxOwnerTTL bounds the TTL so that now+TTL stays far inside the
// int64 nanosecond range: 100 years.
const maxOwnerTTL = 100 * 365.25 * 86400

func ownerPath(box string) string { return filepath.Join(box, "state", "owner") }

func readOwner(box string) (*owner, error) {
	b, e := os.ReadFile(ownerPath(box))
	if os.IsNotExist(e) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	var rec owner
	if e := json.Unmarshal(b, &rec); e != nil {
		return nil, fmt.Errorf("invalid owner record in %s: %w", ownerPath(box), e)
	}
	if rec.Name == "" || rec.Gen == 0 || rec.TTL < 0 || rec.TTL > maxOwnerTTL {
		return nil, fmt.Errorf("invalid owner record in %s", ownerPath(box))
	}
	return &rec, nil
}

// The generation counter survives a drop: state/generation keeps the
// last generation handed out, so a generation value never names two
// different ownership terms.
func readLastGen(box string) (uint64, error) {
	b, e := os.ReadFile(filepath.Join(box, "state", "generation"))
	if os.IsNotExist(e) {
		return 0, nil
	}
	if e != nil {
		return 0, e
	}
	v, e := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if e != nil {
		return 0, fmt.Errorf("invalid generation record in %s", filepath.Join(box, "state", "generation"))
	}
	return v, nil
}

func writeLastGen(box string, gen uint64) error {
	return os.WriteFile(filepath.Join(box, "state", "generation"), []byte(strconv.FormatUint(gen, 10)+"\n"), 0600)
}

func writeOwner(box string, rec *owner) error {
	b, e := json.Marshal(rec)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Join(box, "state"), "owner-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(b); e == nil {
		e = f.Sync()
	}
	if ce := f.Close(); e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	return os.Rename(f.Name(), ownerPath(box))
}

// extendOwner renews rec in place and stores it. The stored TTL is
// kept as-is (zero stays zero); only an explicit --ttl overrides it.
// Caller holds the lock.
func extendOwner(box string, rec *owner, o options, now time.Time) error {
	if o.hasTTL {
		rec.TTL = o.ttl
	}
	rec.RenewedAt = now.UnixNano()
	rec.ExpiresAt = now.Add(sleepDuration(rec.TTL)).UnixNano()
	return writeOwner(box, rec)
}

func ownerLine(rec *owner, now time.Time) string {
	// The name is escaped like a message body, so the line stays a
	// single three-field TSV line for any plain-string name.
	name := strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\r", "\\r", "\t", "\\t").Replace(rec.Name)
	remain := time.Duration(rec.ExpiresAt - now.UnixNano()).Round(time.Millisecond)
	return fmt.Sprintf("%s\t%d\t%s\n", name, rec.Gen, remain)
}

// checkOwner applies the --as rule while the caller holds the lock.
// Without --as, or when no owner is recorded (unless require is set),
// the call proceeds exactly as before. A matching owner is extended.
// A different recorded owner is exit 5; a required but missing owner
// is exit 6.
func checkOwner(box string, o options, require bool) error {
	if o.as == "" && !require {
		return nil
	}
	cur, e := readOwner(box)
	if e != nil {
		return e
	}
	if cur == nil {
		if require {
			return &exitError{6, "no owner"}
		}
		return nil
	}
	if cur.Name != o.as {
		return &exitError{5, "owned by " + cur.Name}
	}
	return extendOwner(box, cur, o, time.Now())
}

// verifyOwner is checkOwner without the extension: it reports whether
// the recorded owner still matches --as, for use inside per-message
// locks after the term has already been extended once.
func verifyOwner(box string, o options) error {
	if o.as == "" {
		return nil
	}
	cur, e := readOwner(box)
	if e != nil {
		return e
	}
	if cur == nil {
		return nil
	}
	if cur.Name != o.as {
		return &exitError{5, "owned by " + cur.Name}
	}
	return nil
}

var errSkip = errors.New("skip")

func ownClaim(o options) (int, error) {
	var rec *owner
	e := locked(o.box, func() error {
		now := time.Now()
		cur, e := readOwner(o.box)
		if e != nil {
			return e
		}
		if cur != nil && cur.Name != o.as && now.UnixNano() < cur.ExpiresAt && !o.steal {
			return &exitError{5, "owned by " + cur.Name}
		}
		if cur != nil && cur.Name == o.as {
			// Re-claiming as the current owner only extends the term.
			if e := extendOwner(o.box, cur, o, now); e != nil {
				return e
			}
			rec = cur
			return nil
		}
		nr := &owner{Name: o.as, ClaimedAt: now.UnixNano(), TTL: defaultOwnerTTL}
		if cur != nil {
			nr.Gen = cur.Gen + 1
		} else {
			last, e := readLastGen(o.box)
			if e != nil {
				return e
			}
			nr.Gen = last + 1
		}
		if e := extendOwner(o.box, nr, o, now); e != nil {
			return e
		}
		if e := writeLastGen(o.box, nr.Gen); e != nil {
			return e
		}
		rec = nr
		return nil
	})
	if e != nil {
		return 0, e
	}
	_, e = fmt.Fprint(os.Stdout, ownerLine(rec, time.Now()))
	return 0, e
}

func ownPass(o options) (int, error) {
	var rec *owner
	e := locked(o.box, func() error {
		now := time.Now()
		cur, e := readOwner(o.box)
		if e != nil {
			return e
		}
		if cur == nil {
			return &exitError{6, "no owner"}
		}
		if cur.Name != o.as {
			return &exitError{5, "owned by " + cur.Name}
		}
		if o.to == cur.Name {
			if e := extendOwner(o.box, cur, o, now); e != nil {
				return e
			}
			rec = cur
			return nil
		}
		nr := &owner{Name: o.to, Gen: cur.Gen + 1, ClaimedAt: now.UnixNano(), TTL: cur.TTL}
		if e := extendOwner(o.box, nr, o, now); e != nil {
			return e
		}
		if e := writeLastGen(o.box, nr.Gen); e != nil {
			return e
		}
		rec = nr
		return nil
	})
	if e != nil {
		return 0, e
	}
	_, e = fmt.Fprint(os.Stdout, ownerLine(rec, time.Now()))
	return 0, e
}

func ownDrop(o options) (int, error) {
	e := locked(o.box, func() error {
		cur, e := readOwner(o.box)
		if e != nil {
			return e
		}
		if cur == nil {
			return &exitError{6, "no owner"}
		}
		if cur.Name != o.as {
			return &exitError{5, "owned by " + cur.Name}
		}
		return unlink(ownerPath(o.box))
	})
	return 0, e
}

func ownShow(o options) (int, error) {
	var rec *owner
	e := locked(o.box, func() error {
		cur, e := readOwner(o.box)
		if e != nil {
			return e
		}
		if cur == nil {
			return &exitError{6, "no owner"}
		}
		rec = cur
		return nil
	})
	if e != nil {
		return 0, e
	}
	_, e = fmt.Fprint(os.Stdout, ownerLine(rec, time.Now()))
	return 0, e
}

// ownRun verifies and extends ownership under the lock, then runs the
// command without holding the lock: a command that itself calls ibx
// would deadlock on a held flock. The command's exit code is passed
// through; codes 5 and 6 are only produced before it starts.
func ownRun(o options) (int, error) {
	if e := locked(o.box, func() error { return checkOwner(o.box, o, true) }); e != nil {
		return 0, e
	}
	c := exec.Command(o.runArgs[0], o.runArgs[1:]...)
	c.Stdin = os.Stdin
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if e := c.Run(); e != nil {
		var ee *exec.ExitError
		if errors.As(e, &ee) {
			if ws, ok := ee.ProcessState.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
				return 128 + int(ws.Signal()), nil
			}
			if code := ee.ExitCode(); code >= 0 {
				return code, nil
			}
			return 1, nil
		}
		return 0, e
	}
	return 0, nil
}

func take(o options) (int, error) {
	if e := unlink(filepath.Join(o.box, "state/notified")); e != nil {
		return 0, e
	}
	// Verify and extend ownership once, atomically, before the batch.
	if e := locked(o.box, func() error { return checkOwner(o.box, o, false) }); e != nil {
		return 0, e
	}
	es, e := entries(filepath.Join(o.box, "pending"))
	if e != nil {
		return 0, e
	}
	dest := "done"
	if o.hold {
		dest = "holding"
	}
	n := 0
	for _, v := range es {
		if o.maximum > 0 && n >= o.maximum {
			break
		}
		var body []byte
		e = locked(o.box, func() error {
			if e := verifyOwner(o.box, o); e != nil {
				return e
			}
			dst := filepath.Join(o.box, dest, v.id+".msg")
			if o.hold {
				// A held message is never overwritten: the same id
				// already in holding keeps its body, and the newer
				// pending copy stays pending for a later hold.
				if _, e := os.Stat(dst); e == nil {
					return errSkip
				} else if !os.IsNotExist(e) {
					return e
				}
			}
			f, e := os.Open(v.path)
			if e != nil {
				return e
			}
			defer f.Close()
			if e = os.Rename(v.path, dst); e != nil {
				return e
			}
			body, e = io.ReadAll(f)
			return e
		})
		if errors.Is(e, errSkip) || os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return n, e
		}
		if e = output(v.id, body, o.format); e != nil {
			return n, e
		}
		n++
	}
	return n, nil
}

// moveHeld implements ack (holding -> done) and release
// (holding -> pending). The optional --as check matches take's rule.
func moveHeld(o options, toDone bool) (int, error) {
	e := locked(o.box, func() error {
		if e := checkOwner(o.box, o, false); e != nil {
			return e
		}
		src := filepath.Join(o.box, "holding", o.arg+".msg")
		if _, e := os.Stat(src); os.IsNotExist(e) {
			return errors.New("not held: " + o.arg)
		} else if e != nil {
			return e
		}
		dir := "pending"
		if toDone {
			dir = "done"
		}
		dst := filepath.Join(o.box, dir, o.arg+".msg")
		if !toDone {
			if _, e := os.Stat(dst); e == nil {
				return errors.New("already pending: " + o.arg)
			} else if !os.IsNotExist(e) {
				return e
			}
		}
		return os.Rename(src, dst)
	})
	return 0, e
}
func nowSeconds() float64 { return float64(time.Now().UnixNano()) / 1e9 }
func notify(o options, id string) (int, error) {
	if o.notify == "" {
		return 0, nil
	}
	marker := filepath.Join(o.box, "state/notified")
	f, e := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if os.IsExist(e) {
		s, e := os.Stat(marker)
		if os.IsNotExist(e) {
			return notify(o, id)
		}
		if e != nil {
			return 0, e
		}
		if time.Since(s.ModTime()).Seconds() < o.renotify {
			return 0, nil
		}
		now := time.Now()
		if e = os.Chtimes(marker, now, now); os.IsNotExist(e) {
			return notify(o, id)
		} else if e != nil {
			return 0, e
		}
	} else if e != nil {
		return 0, e
	} else {
		f.Close()
	}
	es, e := entries(filepath.Join(o.box, "pending"))
	if e != nil {
		return 0, e
	}
	c := exec.Command("sh", "-c", o.notify)
	c.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	c.Env = append(os.Environ(), "IBX_BOX="+o.box, "IBX_ID="+id, "IBX_PENDING="+strconv.Itoa(len(es)))
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	e = c.Start()
	if e == nil {
		done := make(chan error, 1)
		go func() { done <- c.Wait() }()
		timer := time.NewTimer(sleepDuration(o.notifyTimeout))
		select {
		case e = <-done:
			timer.Stop()
		case <-timer.C:
			syscall.Kill(-c.Process.Pid, syscall.SIGKILL)
			<-done
			e = errors.New("notification timed out")
		}
	}
	if e != nil {
		if e = unlink(marker); e != nil {
			return 0, e
		}
		fmt.Fprintln(os.Stderr, "warning: stored, notification failed")
		return 3, nil
	}
	return 0, nil
}
func sleepDuration(s float64) time.Duration {
	if s >= float64(math.MaxInt64)/1e9 {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(s * 1e9)
}

// streamGuard tracks a named stream's ownership across polls. A
// stream that starts on an unowned box runs as before, but once it
// has operated under its own recorded ownership, disappearance of
// the record (a drop) ends it with exit 6 instead of silently
// continuing as an unnamed consumer.
func streamGuard(o options, seen *bool) error {
	return locked(o.box, func() error {
		cur, e := readOwner(o.box)
		if e != nil {
			return e
		}
		if cur == nil {
			if *seen {
				return &exitError{6, "owner dropped"}
			}
			return nil
		}
		if cur.Name != o.as {
			return &exitError{5, "owned by " + cur.Name}
		}
		*seen = true
		return nil
	})
}

func execute(o options) (int, error) {
	needHolding := o.command == "ack" || o.command == "release" ||
		(o.command == "take" && o.hold) || (o.command == "list" && o.holding)
	if e := setup(o.box, needHolding); e != nil {
		return 0, e
	}
	switch o.command {
	case "put", "send":
		id, e := put(o)
		if e != nil {
			return 0, e
		}
		if o.command == "send" {
			return notify(o, id)
		}
	case "take":
		_, e := take(o)
		return 0, e
	case "own":
		switch o.sub {
		case "claim":
			return ownClaim(o)
		case "pass":
			return ownPass(o)
		case "drop":
			return ownDrop(o)
		case "show":
			return ownShow(o)
		case "run":
			return ownRun(o)
		}
		return 0, usageError("invalid own subcommand")
	case "ack":
		return moveHeld(o, true)
	case "release":
		return moveHeld(o, false)
	case "list":
		dir := "pending"
		if o.done {
			dir = "done"
		}
		if o.holding {
			dir = "holding"
		}
		es, e := entries(filepath.Join(o.box, dir))
		if e != nil {
			return 0, e
		}
		cutoff := nowSeconds() - o.since
		for _, v := range es {
			if o.hasSince && v.changed < cutoff {
				continue
			}
			b, e := os.ReadFile(v.path)
			if os.IsNotExist(e) {
				continue
			}
			if e != nil {
				return 0, e
			}
			if e = output(v.id, b, o.format); e != nil {
				return 0, e
			}
		}
	case "wait", "stream":
		start := time.Now()
		seenOwner := false
		for {
			if o.command == "stream" {
				if o.as != "" {
					if e := streamGuard(o, &seenOwner); e != nil {
						return 0, e
					}
				}
				if _, e := take(o); e != nil {
					return 0, e
				}
			} else {
				es, e := entries(filepath.Join(o.box, "pending"))
				if e != nil {
					return 0, e
				}
				if len(es) > 0 {
					n, e := take(o)
					if e != nil {
						return 0, e
					}
					if n > 0 {
						return 0, nil
					}
				}
			}
			delay := o.poll
			if o.command == "wait" && o.hasTimeout {
				remaining := o.timeout - time.Since(start).Seconds()
				if remaining <= 0 {
					return 4, nil
				}
				delay = math.Min(delay, remaining)
			}
			time.Sleep(sleepDuration(delay))
		}
	case "gc":
		now := nowSeconds()
		n := 0
		for _, dir := range []string{"done", "tmp"} {
			ds, e := os.ReadDir(filepath.Join(o.box, dir))
			if e != nil {
				return 0, e
			}
			for _, d := range ds {
				p := filepath.Join(o.box, dir, d.Name())
				s, e := os.Stat(p)
				if os.IsNotExist(e) {
					continue
				}
				if e != nil {
					return 0, e
				}
				age := 3600.0
				stamp := float64(s.ModTime().UnixNano()) / 1e9
				if dir == "done" {
					age = o.keep
					stamp = changed(s)
				}
				if s.Mode().IsRegular() && (dir == "tmp" || strings.HasSuffix(d.Name(), ".msg")) && now-stamp > age {
					e = os.Remove(p)
					if os.IsNotExist(e) {
						continue
					}
					if e != nil {
						return 0, e
					}
					n++
				}
			}
		}
		_, e := fmt.Fprintln(os.Stdout, n)
		return 0, e
	}
	return 0, nil
}
func runCLI(args []string) int {
	o, e := parse(args)
	if e != nil {
		if e.Error() == "help" {
			fmt.Fprintln(os.Stdout, "usage: ibx [--box DIR] [--format tsv|json] {put,take,list,wait,stream,send,gc,own,ack,release}")
			return 0
		}
		fmt.Fprintln(os.Stderr, "error:", e)
		return 2
	}
	code, e := execute(o)
	if e != nil {
		var xe *exitError
		if errors.As(e, &xe) {
			fmt.Fprintln(os.Stderr, "error:", e)
			return xe.code
		}
		var usage usageError
		if errors.As(e, &usage) {
			fmt.Fprintln(os.Stderr, "error:", e)
			return 2
		}
		if errors.Is(e, syscall.EPIPE) && o.command == "stream" {
			return 0
		}
		fmt.Fprintln(os.Stderr, "error:", e)
		return 1
	}
	return code
}
func main() { signal.Ignore(syscall.SIGPIPE); os.Exit(runCLI(os.Args[1:])) }
