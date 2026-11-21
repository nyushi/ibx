# ibx

ibx is a Linux directory inbox for scripts and agents. Its client and daemon
share one binary. Messages, owners, and reservations live in files on a local
filesystem. Build with Go 1.23 or later. There are no third-party dependencies.

## Install

Install with Go:

```sh
go install github.com/nyushi/ibx@latest
```

Or build from the source directory with `go build -o ibx .` and copy the
resulting binary to a directory on your command search path.

## Daemon

A daemon serves one root. The CLI uses `--root DIR` first, then
`IBX_ROOT`, then the resolved box's parent directory, then the working
directory if there is no box. On autostart with an inferred root, the CLI
prints the root and socket path to stderr.

The default socket is `<root>/ibx.sock`. If that path exceeds 107 bytes,
ibx uses `ibx-<hash>.sock` in an existing `XDG_RUNTIME_DIR` directory
or in the OS temporary directory (on Linux, `TMPDIR` or `/tmp`). The
hash is the first 16 hexadecimal characters of the SHA-256 of the
absolute root path. Set `IBX_SOCKET` to choose an explicit path. If the
explicit path or the fallback exceeds 107 bytes, set `IBX_SOCKET` to a
shorter location to fit the Unix `sun_path` limit. The socket mode is
0600, and both sides verify the peer UID. The socket directory must be
owned by the daemon user. An automatic fallback can also use a shared
sticky directory such as `/tmp`.

- `ibx daemon` runs in the foreground. Starting a second daemon on a live
  socket fails. A dead daemon's stale socket is replaced when the next daemon starts.
- Most clients start a detached daemon on demand and wait up to about five
  seconds for it. Startup output is appended to `<root>/daemon.log`.
  Set `IBX_AUTOSTART=0` to require a daemon to be running already.
- `ibx daemon stop` and `ibx daemon restart` drain active commands before
  stopping or replacing the daemon. Long-running `wait`, `stream`, and
  batch input connections may be closed during either operation.
- `ibx status` prints the daemon version, RFC 3339 start time, box count,
  and job definition count. It exits 1 when stopped and never autostarts.
  The box count excludes archived boxes.
- `ibx --version` and `ibx version` print the client version without
  contacting a daemon.

Normal commands require matching client and daemon versions and protocol
versions. On a mismatch, run `ibx daemon restart`. A daemon that supports
cross-version control drains active work and re-execs with the requesting
client's binary. `ibx daemon stop` also works across versions with that
support. Set `IBX_AUTORESTART=1` to restart automatically, verify the new
daemon's version and retry the original command once. Daemons from before
cross-version control was introduced must be stopped with their matching
client before the new version can start.

## Commands

Select a box with `--box DIR` or `IBX_BOX`. With `IBX_ROOT` set, relative
box paths resolve against the root. `put` accepts a first positional box
path containing a slash, and `send` accepts a positional box path. For
example, `ibx send case/x/box` reads its body from stdin. With `IBX_ROOT` unset, relative box paths resolve
against the caller's working directory. Common options work before or after
the command. New directories have mode 0700 and files have mode 0600.
Existing directory permissions are preserved. With an explicit root
from `--root` or `IBX_ROOT`, a box supplied by flag, environment or
positional argument must resolve inside that root. An outside box
returns exit 2. Configured notification fallback and tee destinations
can use absolute paths outside the root.

- `put [--id ID] [--max-size BYTES] [BODY | -]` stores a body and prints its id.
  Omitted BODY or `-` reads stdin. Empty bodies are rejected; the default
  maximum is 1 MiB. Default ids are the first 16 hex SHA-256 characters.
  Ids match `[A-Za-z0-9._-]{1,128}` and cannot start with a dot.
  A duplicate pending id succeeds with `duplicate` on stderr.
- `take [--max N] [--as NAME] [--hold]` moves pending messages into done
  (or into holding with `--hold`, see below) and prints them, ordered
  by modification time in nanoseconds, then id. An empty inbox succeeds.
- `list [--holding | --done [--since DUR] | --scheduled] [--json]` prints
  without taking. The default lists pending messages. `--since` filters done
  entries by file change time. Scheduled output is id, delivery time, and
  first line, sorted by delivery time and then storage order.
- `wait [--timeout SEC] [--poll SEC] [--max N]` takes on arrival. Default
  polling is every second; without a timeout it waits indefinitely.
- `stream [--poll SEC] [--as NAME] [--headers]` repeats take until writing
  detects a closed output pipe. `--headers` prints only the id and escaped
  first line, separated by a tab. With `--as` the stream also ends when
  ownership is lost (see Ownership).
- `gc [--keep DUR]` removes done messages older than the configured root
  `keep` duration (default 7d), along with their metadata when no other live
  entry shares the id. The existing `--keep` option overrides the config for
  that run. Done age uses the file change time updated by the move into done.
  GC also removes tmp files older than one hour. It prints the number of
  removed message and tmp files. Held and scheduled messages are retained.

## Sending

`send` stores before notifying and prints the id of each stored message.
An immediate single send without an explicit id keeps the usual
16-character SHA-256 body id. `--id-from-body` uses that same hash of the
exact body bytes in any single or plain line send. It conflicts with
`--id`. Sending the same id to the same box is suppressed while there is a
matching entry in pending, holding, done, or scheduled. `--dedupe-ttl DUR`
overrides the root `dedupe_ttl` setting. A zero TTL (the default) suppresses
duplicates for as long as the entry exists. A positive TTL compares against
pending or held mtime, done receipt time (file change time), or scheduled
storage time. Reusing an expired pending id replaces that pending body.

- `ibx send BOX --lines` reads one message per nonempty stdin line and
  assigns fresh ids unless `--id-from-body` is set. LF separates bodies.
  `--id` is a usage error with `--lines`.
- `ibx send --lines --tsv` routes each nonempty line as
  `BOX<TAB>ID<TAB>BODY`, with any additional tabs kept in the body. The
  destination may be root-relative or absolute. A malformed line reports
  its 1-based line number and exits 2. Earlier lines stay stored. TSV
  already supplies ids, so it cannot be combined with `--id-from-body`.
  In both batch modes duplicates are silent. Notification failure takes
  precedence (exit 3), followed by retry (exit 75), then success (exit 0).
- `ibx send BOX --at TIME BODY` schedules delivery. Use RFC 3339 or a
  relative Go duration with a leading plus, such as `+30m`. The send
  reports storage immediately. The daemon publishes due messages on
  startup and during its periodic wake loop, then attempts notification.
  The scheduled filename shows the UTC due time, UTC storage time, and
  message id. `list --scheduled` can inspect the queue before delivery.
- `ibx send BOX --reply-to DEST BODY` records a reply destination.
  `ibx reply ID BODY` searches the daemon root across pending, holding,
  done, and scheduled, and sends to that destination with a fresh id.
  Relative destinations resolve under the daemon root. An unknown id or
  missing reply destination exits 1.
- `ibx tee BOX --to DEST [--match REGEXP]` adds a persistent rule to the
  root's `ibx.json`. A rule without a regex copies every arrival through
  send, put, or scheduled delivery. A regex searches the full body as a
  single input. The copy keeps the id, follows the destination's dedupe
  policy, and omits the reply destination. Copies do not trigger further
  rules. Invalid regexes and cycles exit 1. `ibx tee --list` prints
  source, destination, and regex. `ibx tee BOX --to DEST --remove`
  removes rules for the pair. Add `--match REGEXP` to remove only the
  matching rule.

## Periodic jobs

```sh
ibx job set watch --box alerts --every 30s -- watch-status arg
ibx job ls --json
ibx job rm watch
```

Set `job_path` in the root's `ibx.json` to the directories containing
allowed executable names. The daemon runs jobs in the root directory
with literal arguments. It passes `IBX_JOB`, `IBX_BOX`, and any variables
listed in `job_env`. The default allowlist and environment list are empty.
The defaults for `job_timeout`, `job_max_output`, and `job_concurrency`
are `"30s"`, 1048576 bytes of stdout, and four concurrent runs.

Jobs are due immediately after registration and then on their original
schedule, at intervals of at least 5 seconds. An overlapping tick for the
same job is skipped. Jobs waiting for a global slot remain due and catch
up once when a slot opens. After downtime, a job also catches up once.
Setting a name again replaces the job and resets its runtime state,
including the failure count and previous stdout.

By default, only changed, nonempty stdout is sent. `--tsv` routes
`box<TAB>ID<TAB>body` lines individually. `--stdin-prev` supplies the
previous successful stdout as input, and `--first-silent` records the
first successful output without sending it. `--fail-after N` sends one
notification per streak of N or more failures. A success resets the
streak. `--once` removes the job after one completed run, and exit code
10 removes it without sending stdout. `--ttl` expires it relative to
registration, while `--deadline` accepts RFC 3339 or `+duration`.

## Boxes and archiving

`ibx boxes [--unread] [--all] [--json]` lists root-relative box paths,
unread counts, and the oldest pending message's age in whole seconds.
The age is empty in TSV and null in JSON for an empty box. Use
`--unread` to list only boxes with pending messages. JSON fields are
`box`, `unread`, and `oldest_age_seconds`.

`ibx archive BOX` writes `state/archived`. Archived boxes are hidden
from `boxes` unless `--all` is present, omitted by `status` and bulk
owner scans, and skipped by notification. They still accept messages,
tee copies, scheduled deliveries, and direct commands.
`ibx unarchive BOX` removes the marker and wakes the sweep.

## Configuration reference

Put a JSON object in `<root>/ibx.json`. Duration strings use Go duration
syntax, with units such as `ms`, `s`, `m`, and `h`.

| Key | JSON type | Default | Meaning |
| --- | --- | --- | --- |
| `notify_cmd` | String | Empty | Shell command to run for a notification attempt. Empty disables configured notifications. |
| `retry_after` | Duration string | `"30s"` | Delay before retrying a command that returned exit 75. |
| `sweep_after` | Duration string | `"5m"` | Minimum pending-message age for a sweep to start an ordinary notification attempt. |
| `sweep_every` | Duration string | `"5m"` | Time between sweeps for the box, with a 1 ms minimum. |
| `fallback` | String | Empty | Absolute or root-relative destination for a failure message. Empty disables forwarding. |
| `notify_timeout` | Duration string | `"10s"` | Time allowed for a notification command. |
| `dedupe_ttl` | Duration string | `"0s"` | Maximum age of an existing entry for duplicate suppression. Zero suppresses duplicates for as long as the entry exists. |
| `keep` | Duration string | `"168h"` | Retention age for completed messages removed by `gc`. Temporary-file retention is fixed at one hour. |
| `job_path` | Array of strings | `[]` | Directories searched in order for job executables. Relative directories are relative to the root. |
| `job_env` | Array of strings | `[]` | Names of daemon environment variables to pass to job subprocesses, which also receive `IBX_JOB` and `IBX_BOX`. |
| `job_timeout` | Duration string | `"30s"` | Maximum runtime for each job execution. Must be positive. |
| `job_max_output` | Integer | `1048576` | Maximum job stdout in bytes. Exceeding the cap fails the run. Must be positive. |
| `job_concurrency` | Integer | `4` | Maximum number of jobs running at once. Must be positive. |
| `boxes` | Object | `{}` | Overrides keyed by root-relative box path. Each value is an object with optional `notify_cmd`, `retry_after`, `sweep_after`, `sweep_every`, `fallback`, and `notify_timeout` fields. Omitted or empty values inherit the root setting. |
| `tee` | Array of objects | `[]` | Copy rules for arrivals. Each rule has the fields below. |

| Tee field | JSON type | Default | Meaning |
| --- | --- | --- | --- |
| `from` | String | Required | Absolute or root-relative source box path. |
| `to` | String | Required | Destination box path. Can be absolute or root-relative. |
| `match` | String | Empty | Go regular expression applied to the message body. Empty matches every arrival. |

```json
{
  "notify_cmd": "printf '%s has %s pending\\n' \"$IBX_BOX\" \"$IBX_PENDING\"",
  "retry_after": "30s",
  "sweep_after": "5m",
  "sweep_every": "5m",
  "fallback": "alerts/box",
  "notify_timeout": "10s",
  "dedupe_ttl": "24h",
  "keep": "168h",
  "job_path": ["bin", "/opt/ibx/jobs"],
  "job_env": ["LANG", "TZ"],
  "job_timeout": "30s",
  "job_max_output": 1048576,
  "job_concurrency": 4,
  "boxes": {
    "case/x/box": {"fallback": "case/x/alerts", "retry_after": "10s"}
  },
  "tee": [
    {"from": "case/x/box", "to": "audit/box", "match": "^build"}
  ]
}
```

## Notifications

The daemon reads the file at each notification decision. Per-box settings
override root settings. On `send`, `--notify-cmd` overrides
`IBX_NOTIFY_CMD` and the file. An explicit empty `--notify-cmd` disables
that send's hint. `--retry-after`, `--fallback`, and the existing
`--notify-timeout SEC` also override configuration for that send.
Fallback paths may be absolute or root-relative. Configuration durations
use Go duration syntax, including units such as `ms`, `s`, `m`, and `h`.

Send hints coalesce until `take` clears the marker or
`--renotify-after DUR` expires (default 5m). The daemon counts pending
messages under the box lock and only invokes a command when the count is
positive. It runs `sh -c` in the root with its own environment plus:

- `IBX_BOX` and `IBX_OWNER` contain the absolute box path and the recorded
  owner name, which can be empty.
- `IBX_PENDING` is the pending count at the start of the attempt.
- `IBX_FIRST_ID` and `IBX_FIRST_LINE` describe the oldest pending message.
  The first line is limited to 200 runes. `IBX_ID` also carries the triggering
  message ID for compatibility with existing send hooks.

Exit 0 marks the episode successful. Exit 75 requests another attempt when
pending remains. A triggering send returns 75 in that case. Other nonzero
statuses make a triggering send return 3 and write one failure message per
episode to its configured fallback box. A box containing only daemon-generated
fallback messages never forwards a failure. The daemon also sweeps boxes with
configured commands after `sweep_after` and at intervals of `sweep_every`.
Defaults are 30 seconds for retries, five minutes for both sweep timers, and
10 seconds for command timeout.

Each invocation appends a JSON line to `<root>/notify.log` with time, box,
owner, pending count, exit code, and duration in milliseconds. Run
`ibx log notify [--box DIR]` to read it even when the daemon is stopped.

## Ownership

A box may have one recorded owner, so agents that hand a box over
between sessions can fence off the previous holder. The record (name,
generation, claim and renewal times, expiry) lives in `state/owner`
and is read and written under the box lock, so simultaneous claims
resolve to exactly one winner.

- `own claim --as NAME [--ttl DUR] [--steal]` takes the box when it is
  unowned, expired, or already yours (yours only extends the term).
  While another name holds it inside the TTL it fails with exit 5 and
  names the owner; `--steal` takes it anyway. The default TTL is 30m,
  the maximum is 100 years, and a TTL of 0 means the term is already
  expired (anyone may claim, including right after your own renewal).
  The TTL is stored in the record; renewals reuse it, `own pass`
  inherits it, and `--ttl` on a later claim or `own run` replaces it.
- `own pass --as NAME --to NEXT` hands the box to the next name in one
  locked step, only when NAME is the current owner. The generation
  increases by one on every change of hands and is never reused, even
  after a drop.
- `own drop --as NAME` gives up the box (owner only).
- `own claim`, `own pass`, and `own show` print one tab-separated
  line: the owner's name (escaped like a message body, so the line
  stays a single line for any name), the generation, and the time
  left until expiry (negative once expired). `own show` exits 6 when
  there is no owner; `own drop` prints nothing.
- `own run --as NAME [--ttl DUR] -- COMMAND...` checks and extends
  ownership under the lock, then runs the command and passes its
  exit code through; a command killed by a signal is reported as
  128 plus the signal number. The lock is not held while the command
  runs (see limits below). The wrapped command runs on the daemon side
  as the same local user, with the client's environment and working directory.
- `own list --as NAME [--json]` lists owned boxes under the root with their
  unread counts. The default output is path and count separated by a tab.
  JSON output has one object per box with `box`, `owner`, and `unread`.
- `own pass-all --as OLD --to NEW` and `own drop-all --as NAME` operate on
  every matching box under the root. Each result line contains the path and
  either `ok` or an error, separated by a tab. They exit 1 if any box fails.

`IBX_NAME` supplies the default for commands that accept `--as`. The daemon
does not infer owner liveness from processes or notification results.

Expiry is lazy: passing the deadline does not by itself end
ownership. The recorded owner keeps the box until someone else
claims it, and the owner's next operation extends the term again.

`take` and `stream` accept `--as NAME`; an empty name is a usage
error, never a silent opt-out. When an owner is recorded and
the name differs, the call fails with exit 5 and takes nothing. A
`stream --as NAME` ends with exit 5 when the box passes to another
name, and with exit 6 when a box it had been holding is dropped.
Calls without `--as`, and calls on a box with no recorded owner,
behave exactly as before, so existing scripts are unaffected.
`put` and `send` never check ownership.

## Two-stage take

- `take --hold` moves pending messages to `holding/` instead of
  `done/` and prints them as usual. An id that is already held is
  skipped, never overwritten: the newer copy stays pending until the
  held one is acked (release refuses while a pending copy exists).
- `ack ID [--note TEXT]` moves a held message into `done/`, records the
  note and acknowledgment time, and clears its reported mark. The note
  is truncated at the first LF. A new receipt replaces a done file of
  the same id. Acking an id that is not held exits 1. Use `--` before an
  id that starts with a dash, as in `ack -- -dash`.
- `ibx mark ID reported` marks a done message as reported. It searches the
  root unless a box is selected. An id without a done entry exits 1.
- `release ID` moves a held message back to `pending/`, so a receiver
  that crashed after `take --hold` can be retried by whoever takes
  over. It fails if the id is already pending again.
- `list --holding` and `list --done` append ack note, ack time, and the
  literal `reported` (when set) after the usual TSV columns for messages
  with metadata. Messages without metadata keep their usual columns.
  `list --json` works in every list mode and emits one object per message
  with `id`, `first_line`, and any available `reply_to`, `ack_note`,
  `ack_time`, and `reported` fields. Scheduled records include `deliver_at`.
  Metadata is in `state/meta/<ID>.json`.
- `ack` and `release` accept the same optional `--as` check as `take`.

```sh
ibx --box /tmp/inbox put 'build finished'
printf 'job 42 done' | ibx --box /tmp/inbox put --id job-42 -
ibx --box /tmp/inbox take --max 10
ibx --box /tmp/inbox list --done --since 10m
ibx --box /tmp/inbox wait --timeout 30 --poll 0.5
ibx --box /tmp/inbox stream --poll 0.5
ibx --box /tmp/inbox send --notify-cmd 'printf "inbox ready\n" >&2' 'build finished'
ibx --box /tmp/inbox gc --keep 7d
```

Durations accept nonnegative integers or decimals with `s`, `m`, `h`, or `d`,
or bare seconds. Poll and timeout options accept decimal seconds. Counts
and maximum sizes must be positive integers.

`--format tsv` is the default: id, tab, escaped body, one message per line.
Backslash, newline, carriage return, and tab become `\\`, `\n`, `\r`, `\t`.
`--format json` emits one object per line with `id` and `body`.
Stored bytes are exact, with no appended newline. Both output formats
replace invalid UTF-8. Put and send print plain ids; gc prints a plain count.

Exit codes: 0 success (including duplicates); 1 I/O or runtime error;
2 usage error; 3 send stored the message but notification failed;
4 wait timed out; 5 the box is owned by another name; 6 an operation
that requires an owner found none; 75 send stored the message and scheduled
a notification retry. `own run` produces 5 and 6 only
before the wrapped command starts; once it starts, the exit code is
the command's own. Callers that only need storage may ignore notification
failure, for example `ibx --box /tmp/inbox send 'build finished' || true`.

## Guarantees and limits

Readers never see partial publications: write and fsync in tmp, then publish
with a hard link. Concurrent puts of one pending id store one message.
Only a successful rename to done outputs a message; short directory locks
also protect overlapping id reuse. Legacy put deduplication covers pending
only. Send and tee deduplication also check holding, done, and scheduled.

Pending, held, done, and scheduled messages survive a daemon crash, as do
written metadata, owner records, archive markers, and notification state.
Scheduled delivery atomically renames the due file into pending, so a
restart cannot republish that scheduled entry. If another pending entry
has the same id, the scheduled entry waits for the pending slot to clear.
Tee copies and notify commands run separately from publication. A daemon
crash during fan-out can leave only some destination copies. `stream` and `wait` poll
the files on the daemon side and relay output over the socket as it
becomes available. Notifications are best effort and can repeat during
retries and periodic sweeps, or if the daemon dies after a hook runs but
before it records the result. A notify command should tolerate repeated
invocations. Receivers can list, take, or wait regardless of notification
outcome.

Delivery is at most once to the process. A receiver crash after moving a
message but before handling it leaves it in done; recover it with
`list --done` until gc removes it. Reusing an id may overwrite its old done
file. Order across senders at the same instant, handling after receiver
crashes, and operation on network filesystems are not guaranteed. Windows
and delivery between machines are outside scope. File fsync alone does
not promise recovery from every power-loss scenario.

Ownership is fencing, not authentication. A name is a plain string,
any local caller may use any name, and `--steal` is deliberately
unrestricted: who may steal is a rule for callers, not something ibx
can verify on one machine. Callers that omit `--as` are never fenced.
Names should be valid UTF-8; other bytes are not preserved exactly.
`own run` checks ownership at one locked instant before starting the
command; a takeover during a long-running command is not detected
until the command's next ibx call made with the old name while a
different owner is recorded. Two-stage take changes the crash
story only for callers that use it: a crash after `take --hold` leaves
the message in `holding/`, visible with `list --holding` and
recoverable with `release`, until someone acks or releases it; ibx
never times held messages out on its own.

## Comparison

Maildir-style queues use the same storage idea: write in tmp, atomically
publish, one file per message. That idea is not new. ibx adds atomic put
deduplication, taking everything in one call, store-then-notify with
coalescing, and output intended for programs.

Job spoolers such as filespooler run a command for each queued item in
sequence order. ibx jobs run on a schedule and publish changed output to
boxes. Sending a message can trigger a notification command.

Message brokers such as RabbitMQ, Redis Streams, and SQS need a server.
ibx uses a local daemon and inspectable files. Use `ls` and `cat` to view
its on-disk queue.

## Tests

From the repository root:

```sh
go test ./...
```

## License

MIT License. See [LICENSE](LICENSE).


## Compatibility with pre-daemon (1.x) clients

Mixing old and new clients on one box is **not a guaranteed
configuration**. They share file formats and the box lock, so basic
`put`, `send`, `take`, `list`, and `own` operations interleave without
corruption in practice. Old clients' sends run outside the daemon's
notification attempt lock and are invisible to episode, fallback, and
retry state and to sweeps. Double notification is possible. Scheduled
delivery, jobs, extended deduplication, and metadata cleanup do not apply
to old clients.

Use a single version per box. If old clients are unavoidable, restrict
them to `put`, `take`, and `list`.
