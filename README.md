# ibx

ibx is a daemonless directory inbox for scripts and agents. It stores message
bytes before issuing an optional notification. It requires a local Unix-like filesystem. Build with Go 1.23 or later;
there are no third-party dependencies.

## Install

Install with Go:

```sh
go install github.com/nyushi/ibx@latest
```

Or build from the source directory with `go build -o ibx .` and copy the
resulting binary to a directory on your command search path.

## Commands

Select a box with `--box DIR` or `IBX_BOX`. Common options work before or
after the command. New directories have mode 0700; files have mode 0600.
Existing directory permissions are preserved.

- `put [--id ID] [--max-size BYTES] [BODY | -]` stores a body and prints its id.
  Omitted BODY or `-` reads stdin. Empty bodies are rejected; the default
  maximum is 1 MiB. Default ids are the first 16 hex SHA-256 characters.
  Ids match `[A-Za-z0-9._-]{1,128}` and cannot start with a dot.
  A duplicate pending id succeeds with `duplicate` on stderr.
- `take [--max N] [--as NAME] [--hold]` moves pending messages into done
  (or into holding with `--hold`, see below) and prints them, ordered
  by modification time in nanoseconds, then id. An empty inbox succeeds.
- `list [--done] [--since DUR] [--holding]` prints without taking. `--since` requires
  `--done` and filters by file change time. `--done` and `--holding`
  cannot be combined. Ordering matches take.
- `wait [--timeout SEC] [--poll SEC] [--max N]` takes on arrival. Default
  polling is every second; without a timeout it waits indefinitely.
- `stream [--poll SEC] [--as NAME]` repeats take, flushing each message,
  until writing detects a closed output pipe. With `--as` it also ends
  when ownership is lost (see Ownership).
- `send` accepts put options and stores before notifying. `--notify-cmd CMD`
  overrides `IBX_NOTIFY_CMD`; an empty command disables notification.
  Hints coalesce until take clears the marker or `--renotify-after DUR`
  expires (default 5m). Commands run through `sh -c`, receiving `IBX_BOX`,
  `IBX_ID`, and `IBX_PENDING` in the environment, never the body.
  `--notify-timeout SEC` defaults to 10 seconds. Failure clears the marker.
- `gc [--keep DUR]` removes done message files taken more than 7d ago by
  default (aged by file change time, which the move into done updates) and
  tmp files older than one hour by modification time. It prints just
  the removal count and leaves other directories untouched. Held
  messages (below) are never removed by gc.

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
  runs (see limits below).

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
- `ack ID` moves a held message from `holding/` to `done/`, replacing
  an existing done file of the same id just like a plain take does.
  Acking an id that is not held is an error (exit 1). Ids that start
  with `-` need the `--` separator, as in `ack -- -dash`.
- `release ID` moves a held message back to `pending/`, so a receiver
  that crashed after `take --hold` can be retried by whoever takes
  over. It fails if the id is already pending again.
- `list --holding` lists held messages without moving them.
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
that requires an owner found none. `own run` produces 5 and 6 only
before the wrapped command starts; once it starts, the exit code is
the command's own. Callers that only need storage may ignore notification
failure, for example `ibx --box /tmp/inbox send 'build finished' || true`.

## Guarantees and limits

Readers never see partial publications: write and fsync in tmp, then publish
with a hard link. Concurrent puts of one pending id store one message.
Only a successful rename to done outputs a message; short directory locks
also protect overlapping id reuse. Deduplication covers pending only.

Failed or lost notifications do not lose messages: receivers can list,
take, or wait at any time. The notified marker bounds notification floods
by coalescing hints and allowing stale retries. Simultaneous stale retries
may issue more than one hint; notifications are best effort.

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
sequence order. ibx hands text to the reader rather than executing jobs;
send can execute a notification command.

Message brokers such as RabbitMQ, Redis Streams, and SQS need a server.
ibx stays on one machine with no server; inspect it with `ls` and `cat`.
No claims are made about other directory-queue libraries.

## Tests

From the repository root:

```sh
go test ./...
```

## License

MIT License. See [LICENSE](LICENSE).
