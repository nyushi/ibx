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
- `take [--max N]` moves pending messages into done and prints them, ordered
  by modification time in nanoseconds, then id. An empty inbox succeeds.
- `list [--done] [--since DUR]` prints without taking. `--since` requires
  `--done` and filters by file change time. Ordering matches take.
- `wait [--timeout SEC] [--poll SEC] [--max N]` takes on arrival. Default
  polling is every second; without a timeout it waits indefinitely.
- `stream [--poll SEC]` repeats take, flushing each message, until writing
  detects a closed output pipe.
- `send` accepts put options and stores before notifying. `--notify-cmd CMD`
  overrides `IBX_NOTIFY_CMD`; an empty command disables notification.
  Hints coalesce until take clears the marker or `--renotify-after DUR`
  expires (default 5m). Commands run through `sh -c`, receiving `IBX_BOX`,
  `IBX_ID`, and `IBX_PENDING` in the environment, never the body.
  `--notify-timeout SEC` defaults to 10 seconds. Failure clears the marker.
- `gc [--keep DUR]` removes done message files taken more than 7d ago by
  default (aged by file change time, which the move into done updates) and
  tmp files older than one hour by modification time. It prints just
  the removal count and leaves other directories untouched.

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
4 wait timed out. Callers that only need storage may ignore notification
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
