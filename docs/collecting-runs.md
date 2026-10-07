# Collecting runs: what a coordinating session still has to act on

A coordinating session submits campaigns and single `task run` runs with
`--notify-thread`. After a restart or a compaction it has to find which of
those runs finished or need attention and whose results it has not yet acted
on, without re-reading every run. Two campaign verbs answer that:

- `t3-steward campaign uncollected [--thread current|ID] [--json]` lists them;
- `t3-steward campaign collect RUN... [--thread current|ID] [--json]` records
  that the session has acted on finished runs, so they stop being listed.

## Before

```
$ t3-steward campaign list --thread current
RUN            WORKFLOW        PROJECT  CLASS     STATE      TASKS
run-4be1...    impl-review     steward  required  failed     1/3 succeeded
run-91c0...    findings-scan   steward  required  succeeded  1/1 succeeded
run-77aa...    implement-x     steward  required  active     0/2 succeeded
run-0d3e...    older-run       steward  required  succeeded  2/2 succeeded   <- read last week, still listed
$ t3-steward campaign collect run-91c0... --thread current
unknown campaign command "collect"; ...
```

There was no finished-at, no attention reason (an open ask on an active run
looked like any active run), and nothing distinguished a run whose result the
session had already read.

## After

```
$ t3-steward campaign uncollected --thread current
thread 9f5effaa-...: 3 runs to collect (2 collected and 4 active without attention not shown)
RUN          NAME           STATE      FINISHED              WAKE       ATTENTION
run-77aa...  implement-x    active     -                     pending    ask w-ask-12 on implement: "Which base?" (since 2026-10-07T15:10:02Z)
run-4be1...  impl-review    failed     2026-10-07T14:02:11Z  delivered  failed
run-91c0...  findings-scan  succeeded  2026-10-07T13:40:05Z  delivered  -
Read a result with "t3-steward task result RUN", then record it with "t3-steward campaign collect RUN... --thread current".
$ t3-steward campaign collect run-91c0... run-4be1... --thread current
collected run-91c0... (succeeded, finished 2026-10-07T13:40:05Z) for thread 9f5effaa-...
collected run-4be1... (failed, finished 2026-10-07T14:02:11Z) for thread 9f5effaa-...
$ t3-steward campaign collect run-77aa... --thread current
... run run-77aa... is active and cannot be collected; only a finished run is collected. Its attention clears when the ask is answered: t3-steward ask answer w-ask-12 --option ...
```

## The rules

**Ownership.** A run belongs to a thread when a node wait of that thread
targets it. That is the notification `campaign submit`, `task run` and
`review` register for `--notify-thread`, and the same rule `campaign list
--thread` uses. A run submitted with `--no-notify` belongs to no thread and is
never listed.

**Collected means acted on.** A run is collected for a thread only by an
explicit `campaign collect`. Nothing is collected implicitly: not by
`task result`, `campaign show`, `--wait` or a delivered wake. The restart this
view exists for is exactly the case where a session read a result and then
lost its context before acting on it, so a read is not evidence that the
result was used. Collect a run after acting on it.

**Only finished runs are collected.** `collect` refuses a run that is still
running, naming its attention and the command that clears it. Attention on a
running run goes away when its cause is resolved, not by collecting. A
collection covers the run as it finished: the coordinator records the run's
terminal progress and completion time, and the run counts as collected only
while it still has them. A run that finishes again, for example after
`campaign rerun` amends it, is listed again.

**What is listed.** Every run the thread owns that is finished and not
collected, or still running and needing attention. The attention kinds are:

- `failed`, `cancelled`: how a finished run ended (succeeded and skipped runs
  have none);
- `ask`: an open ask of one of the run's tasks, with its question;
- `attention`: an open attention request;
- `needs-input`: the run needs input and no open ask or request explains it;
- `task-failed`: a running run with failed or cancelled tasks whose
  dependents are blocked.

Running runs come first, then finished runs newest first. Running runs that
need nothing, collected runs and runs the coordinator no longer lists are
counted on the summary line rather than shown. WAKE is the delivery state of
the thread's newest node wait on the run.

**Storage.** Collections live in the coordinator's database, under
`run-collection/v1/<thread>/<run>` in its `kv` table, so they survive session
restarts, host state pruning and client upgrades. Recording the same finished
run again changes nothing and keeps the first `collectedAt`.

## JSON

`campaign uncollected --json` prints one document:

```json
{
  "schemaVersion": 1,
  "kind": "t3-steward.uncollected/v1",
  "thread": "9f5effaa-...",
  "generatedAt": "2026-10-07T16:00:00Z",
  "collectionSupported": true,
  "note": "",
  "runs": [
    {
      "run": "run-4be1...",
      "name": "impl-review",
      "project": "steward",
      "state": "failed",
      "finishedAt": "2026-10-07T14:02:11Z",
      "attention": [{"kind": "failed", "subject": "run-4be1...", "task": "", "detail": "the run failed", "since": "2026-10-07T14:02:11Z"}],
      "wake": {"waitId": "nw-campaign-...", "delivery": "delivered", "deliveredAt": "2026-10-07T14:02:40Z"},
      "result": "t3-steward task result run-4be1...",
      "collect": "t3-steward campaign collect run-4be1... --thread 9f5effaa-..."
    }
  ],
  "omitted": {"collected": 2, "active": 4, "unknown": 0}
}
```

`runs` and each `attention` are arrays, including when empty.

`campaign collect --json` prints
`{"schemaVersion":1,"thread":...,"collected":[{"run","progress","completedAt","collectedAt","changed"}]}`;
`changed` is false when the run was already collected as it finished.

## Exit codes

`campaign uncollected` exits 0 whenever every source answered, including an
empty view ("nothing to collect for thread T"). `campaign collect` exits 0 when
every run was recorded, 1 for a refusal before sending (a bad run ID, an
unresolved `--thread current`), 8 when the coordinator refused a run (unknown,
not finished, or with no node wait of the thread), and the transport's code for
any other failure. It sends one request per run in argument order and stops at
the first refusal, naming the runs recorded before it.

## Older coordinators

A coordinator older than this feature answers both new requests with
`unknown native wait action`. Then:

- `campaign collect` exits 7 with "the coordinator does not record collected
  runs; upgrade it" and records nothing;
- `campaign uncollected` still lists every finished and attention-needing run
  of the thread as uncollected, says on its first line (and in JSON with
  `collectionSupported: false`) that the coordinator must be upgraded, and
  exits 0.

Every other failure keeps its own transport exit code.
