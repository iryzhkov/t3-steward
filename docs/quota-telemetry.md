# Quota telemetry (phase 0 of the quota controller)

The quota telemetry recorder is observe-only. It joins the quota readings the
coordinator holds to the work that consumed them, so a later quota controller
can be tuned from real data. It changes neither dispatch nor quota policy:
admission, quota waits, the worker protocol and every coordinator table are
exactly as they were.

## Where it runs and where the data lives

The recorder runs inside the coordinator process, in its own goroutine started
once beside the watchdog (outside configuration reloads). Its first tick runs
10 seconds after the coordinator starts, out of the way of startup, and then
every 30 seconds it appends to its own SQLite file:

```
<directory of the state database>/quota-telemetry/recorder.sqlite
```

The directory is created 0700 (an existing one loses any group and other
access) and the file 0600, in write-ahead-log mode. A symlink in place of the
directory or the file is refused, not followed. Only the recorder creates or
writes it. Nothing else depends on it, and it is not
part of coordinator backups: a lost or deleted file is recreated on the next
tick, with a new `recorder` `started` event that marks where coverage begins.

The recorder reads the coordinator database through a second, query-only
connection (`mode=ro`, `query_only`), never through the coordinator's own
connection. It therefore cannot write coordinator state, and in WAL mode it
neither waits for nor blocks the coordinator's writer.

## Reading it

On the coordinator host:

```
t3-steward quota telemetry [--since DUR|RFC3339] [--route TEXT] [--pool ID] [--kind K[,K]] [--limit N] [--json]
```

- `--since`: an age such as `6h`, `90m` or `7d`, or an RFC3339 instant;
  default 24 hours.
- `--route`: a substring of `instance/model`; for a reading, of its bucket
  key.
- `--pool`: the `quotaPoolId` of work events, plus readings of the provider
  instances the loaded configuration binds to that pool.
- `--kind`: any of `reading,dispatch,start,finish,check,recorder` (the
  default is all of them).
- `--limit`: keep the newest N matching events, 1 to 10,000, default 200; they
  print oldest first.

The command opens the file read-only and never creates it or its sidecar
files: when the recorder is not running (no `-wal` or `-shm` file), the file
is read as immutable. On a host without
the file it exits 1 with "no quota telemetry store at PATH; the recorder runs
inside the coordinator, so run this on the coordinator host (for example: ssh
<coordinator-host> t3-steward quota telemetry)". A file written by a newer
schema is refused, naming both versions. Otherwise it exits 0, including when
nothing matches.

The text output is a header line (event count, the since time, the store
path, schema version, the oldest retained event and the recorder's failure
count) and a table:

```
TIME  KIND  ROUTE  EFFORT  TYPE(derived)  TASK  DURATION  DETAIL
```

## Event kinds

| Kind | Recorded when | Event time |
|---|---|---|
| `reading` | a distinct quota reading is seen | the reading's `observedAt` |
| `dispatch` | the audit stream shows `assignment-offered` | the offer |
| `start` | the audit stream shows `assignment-claimed` | the claim |
| `finish` | an open assignment's attempt is terminal, or the assignment is released, superseded by a later epoch, or completed with no retained attempt | the attempt's `completedAt`, otherwise the assignment's `updatedAt` |
| `check` | a finish is recorded and the attempt has verification or gate reports | the command's completion |
| `recorder` | the store begins coverage (`started`), or a span could not be recorded (`gap`) | the recorder's clock |

Dispatch is not start: the time between them is `dispatchToStartMs` on the
start event. A retry is a new assignment with its own dispatch, start and
finish. An assignment offered again keeps its id at a later epoch; each
epoch has its own events, keyed `assignment:epoch`. The earlier epoch ends
with outcome `superseded` at the later epoch's offer, records no checks, and
takes its route and worker from the binding frozen with its own dispatch; when
none was frozen they are left empty with `routeUnknown: true`, never copied
from the later epoch. An attempt parked on an external wait keeps its assignment open; its
finish comes when the attempt ends. A completed assignment with a known
nonterminal attempt stays open through result import, so the finish includes
the final outcome, completion time and available check reports. Each tick
re-reads at most 200 assignments for finishes, combining retained open work
with new dispatches and starts. Remaining work is retained and scanned in
later batches; the scan cursor persists across restarts.

## JSON schema

`--json` prints one document:

```
{
  "schemaVersion": 1,
  "kind": "t3-steward.quota-telemetry/v1",
  "generatedAt": "...",
  "store": {"path", "schemaVersion", "oldestRetained", "rows"},
  "recorder": {"ticks", "failures", "lastError", "lastErrorAt", "lastSuccessAt",
               "coverageFrom", "auditWatermark", "skippedReadings", "skippedChecks", "gaps": [...]},
  "filters": {"since", "route", "pool", "poolInstances", "kinds", "limit"},
  "events": [...]
}
```

`events` is never null. Every event carries its own `schemaVersion`, an
`eventId`, `kind`, `at`, `recordedAt` and `quotaPoolId`, and one of:

- `reading`: `source` (`worker:<id>` or `coordinator-host`), `bucketKey` and
  its parts (`providerInstanceId`, `accountId`, `limitId`, `window`),
  `usedPercent`, `resetsAt`, `observedAt`, `phase`, `healthy`, `epoch`,
  `limitName`, and `ageSeconds` when first recorded.
- `work` (dispatch, start, finish, check): `runId`, `taskId`, `taskName`,
  `attemptId`, `attemptNumber`, `assignmentId`, `assignmentEpoch`, `workerId`,
  `project`, `route` `{providerInstanceId, model, effort, effortAbsent,
  quotaPoolId}`, `executionRole`, `taskType`, `dispatchedAt`, `startedAt`,
  `finishedAt`, `dispatchToStartMs`, `durationMs` and `outcome`.
- `check` (with `work`): `stage` (`verification` or `gate`), `index`,
  `exitCode`, `startedAt`, `completedAt`, `durationMs`, `command` (credential
  shapes such as forge and provider tokens, `password=` values and URL
  passwords replaced with `[redacted]`, then cut to 256 bytes, with
  `commandTruncated`).
- `recorder`: `state` (`started` or `gap`), `coverageFrom`, `from`, `to`,
  `failedTicks`, `lastError`, `reason`.

A finish event read through the command also carries `deltas` (below).

Nulls represent a missing reset, usage or timestamp; a zero is never
substituted. `effort` is null with `effortAbsent: true` when the route names
no effort. `durationMs` is finish minus start, wall clock with parked time
included; model-active time is not available. It is null when the start was
not recorded or the finish is earlier than the start. The outcome is the attempt's progress, or `released` or
`superseded` for an assignment that ended without its attempt. No failure text
is stored.

## Derived task type

No manifest declares a task type, so each work event carries a provisional
category derived from structured fields that already exist:

```
"taskType": {"category", "derived": true, "rule", "provenance"}
```

`provenance` keeps the raw inputs (`taskName`, `executionRole`,
`reviewOutputDeclared`, `reviewJudge`, `gateDeclared`, `verificationCount`,
`class`, `supervisionActivationId`) so records can be re-labelled later. The
first matching rule wins:

1. a supervision activation id, or role `supervisor-activation`: `supervision`
   (rule `supervision`);
2. role `repair-executor`: `fix`; role `gate-reviewer`: `review` (rule
   `role`);
3. a review judge: `review-judge` (rule `review-judge`);
4. a declared review output: `review` (rule `review-output`);
5. the task name, lowercased, cut at the first `-`, `_` or `.`, with trailing
   digits removed, when it is `implement`, `review`, `fix` or `gate` (rule
   `name`);
6. otherwise `unknown` (rule `none`).

The text table shows it as `category:rule`, for example `implement:name`.
Prompts, prompt artifacts and any other free text are never read.

## Quota deltas (method `window-before-after/v1`)

Deltas are computed by the read command from the retained readings, not
stored, so a later method can re-derive them. For each finish, for every
retained bucket key of the route's provider instance, including a key with
no reading near the interval:

- `before` is the latest reading at or before the start, and `after` the
  earliest at or after the finish, each from any source and within 30 minutes;
- `deltaPp` is after minus before, in percentage points;
- it is null with an `absence`: `no-reading-before`, `no-reading-after`,
  `pending` (no later reading yet and the finish is under 30 minutes old), or
  `reset-crossed` (the reset times differ by more than a minute, or usage
  fell).

`attribution` is `exclusive` when no other recorded assignment of the same
quota pool (the provider instance when the route names no pool) overlaps
`[before.observedAt, after.observedAt]`, otherwise `shared` with
`concurrentCount` and up to 16 `concurrent` attempt ids; it is `unavailable`
when there is no delta. Starts older than 48 hours before the interval are not
searched for overlap.

Limits: the delta is the window's change over the interval, never a per-task
share; with concurrent work every overlapping assignment shows the same
number, labelled shared. Interactive and unmanaged use of the same account is
not observed and is included in the change. A finish without a recorded start
has no deltas.

## Gate and test durations

When a finish is recorded, the recorder lists the attempt's `verification`
and `gate` artifacts, reads each report (at most 4 MiB) and records one
`check` event per command: the verification report's `NNN`, or the gate
command's position, its exit code, times and duration. The verification
output, the gate's error text and the gate log (`gate/log.txt`) are never
read into the store; the log is not opened at all. A missing, oversized or
malformed report is skipped and counted in `skippedChecks`.

A chain's model-run gate task, whose `make check-review` output is free text,
is measured by its own `finish` (category `gate`). Test durations inside one
command are not captured.

## What is not captured

- Readings that change on a worker host and are overwritten before the next
  exchange: worker readings reach the coordinator as the latest value per
  bucket, so only what the coordinator holds at a tick is recorded. A worker
  without the `quota-observations-v1` capability contributes no readings; its
  work is still recorded.
- Work before the recorder's first start: the first start begins at the
  current audit sequence and does not backfill.
- Readings during a recorder gap (they are latest-only); audit-driven events
  of the gap are recovered from the stored watermark.
- Prompts, tool arguments, transcript content, credentials, lease or dispatch
  tokens, failure text and command output.
- Interactive or unmanaged use of the same provider account.

## Retention

Events older than 30 days are deleted, and beyond 500,000 rows the oldest are
deleted. Pruning runs at recorder start and hourly, oldest first, at most
5,000 rows per transaction and 20 transactions per pass, only in the
telemetry file. Readings are most of the rows, so on a busy fleet the row cap
can be reached before thirty days. Each event record is capped at 4 KiB (long strings are cut and
`truncated` is set). The header's "retained from" (`store.oldestRetained`)
makes truncation visible.

## Failure semantics

A recorder failure never reaches the coordinator. Each tick has a 10-second
deadline; an error or a panic is logged at WARN (the first time its text is
seen, then at most once per 10 minutes), counted, written best-effort to the
meta table, and the tick ends. The coordinator keeps its current fixed-cap
execution. A state path of `:memory:` or an unopenable telemetry file
disables recording for that tick only.

After one or more failed ticks, the first successful tick records a
`recorder` `gap` event with `from`, `to`, `failedTicks` and `lastError`.
Readings in that span are lost; dispatch, start and finish events of the span
are recorded afterwards, because the audit stream is tailed from a watermark
committed in the same transaction as the events. A restart therefore replays
without loss or duplication. A watermark above the coordinator's highest audit
sequence (a restored database) is reset to it and recorded as a `gap`.

A coordinator row that does not decode, or names an id longer than 256 bytes,
is skipped and counted in `skippedRecords` rather than failing every tick. A
reading with no observation time, one before 2000 or more than a day ahead of
the recorder's clock, or a usage that is not a finite number is skipped and
counted in `skippedReadings` (on each tick it is seen). The failed span is
persisted, so a restart in the middle of it still records its gap.

Persistent counters in the meta table: ticks, failures, skipped readings,
skipped check reports, skipped records, last error and its time, last
success, coverage start, the audit watermark and any failed span not yet
recorded as a gap. The read command shows them in its header and under
`recorder` in JSON.
