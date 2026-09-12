# Core reliability audit (2026-09-11)

Scope: the deployed backlog-v2 coordinator and worker on normandy at
`dd438c5`, its journal over the previous 24 hours (94 coordinator starts,
about 6,000 error lines), the SQLite state, the worker journal, and the
custody outbox. The goal was to explain why the steward stalls, why work
appears to lose its outputs, and what makes the system need constant
manual recovery.

## Summary

The system fails closed at the wrong granularity. Almost every loop treats
one inconsistent record as a reason to abort the whole cycle, so a single
bad row or one unobservable T3 thread stops all planning, all worker
reconciliation, or all artifact import. Combined with an epoch that
advances on every coordinator restart and is bound into packages,
commands, and custody records, ordinary restarts turn in-flight work into
"unknown" state that only an operator with SHA-256 evidence can clear.

## Findings

### A. One bad record stalls everything

1. `workerruntime.Runtime.Reconcile` returns on the first attempt whose
   reconciliation fails. `WorkerService.Serve` runs it before reading the
   request, so one failing record makes every exchange exit non-zero, the
   coordinator sees "session is unusable", no lease is renewed, and after
   two minutes every running assignment is stopped and marked unknown.
2. `DeriveQuotaPlanningState` fails the whole quota reconstruction on one
   contradictory attempt (`control "paused" contradicts latest throttle
   control "paused-uncheckpointed"`, `names unknown quota pool`, `uses
   settled assignment`). While it fails, planning and admin command
   execution are deferred: 1,834 deferrals in 24 hours.
3. `ReconcileWorker` aborts before commands and throttle delivery when one
   offered assignment cannot be packaged. At the time of the audit the
   live system was blocked on `assignment-57c50a20…` (an offered row with
   an empty thread ID written by an older binary), every ten seconds.
4. `PlanWorkerStateTransitions` and `PlanWorkerCommands` abort on any
   observation they do not understand (`invalid observed state
   "unknown"`, `invalid observed control`).
5. `CustodyStore.PendingUploadByPurpose` returns an error for the first
   outbox entry that fails validation, including any upload manifest older
   than its 24-hour lifetime. One stale entry blocks result discovery for
   every assignment, and the result itself becomes unimportable.
6. `CommitAssignmentPlan` is one transaction for all proposals; one attempt
   that still owns a non-released assignment row (from a previous binary)
   fails the plan for every other task (UNIQUE constraint errors, 568 in
   24 hours).

### B. Epochs make restarts destructive

7. `AcquireCoordinator` advances the epoch on every start, and the worker
   config carries `local_worker.coordinator_epoch` as a hard requirement.
   The protocol server rejects any envelope whose epoch differs, so the
   worker config had to be edited by hand after each of the 94 restarts.
8. Execution packages, custody manifests, and worker commands embed the
   coordinator epoch. After a restart the worker refuses to publish
   results for packages created under the old epoch ("custody epoch
   binding mismatch"), rejects re-offers of the same assignment ("changed
   replay"), and the coordinator cannot re-issue pending commands ("replay
   changes identity") because the stable command ID has no epoch in it.
9. The worker treats a lease that expired while the coordinator was away
   as an instruction to stop the T3 thread and mark the assignment
   unknown. The coordinator then never re-claims it even when the next
   snapshot shows the worker still owns and observes it.
10. The catalog revision hashes the whole worker configuration; any
    configuration change makes in-flight packages "stale" at collection
    time, which loses their results.

### C. "Unknown" is used for ordinary failures

11. Preparation failures (clone errors, missing dependency output, bad
    ref), T3 project resolution failures, and thread creation failures
    where no thread exists are all recorded as unknown execution, which by
    design needs `backlog recover` with reviewed evidence. These are
    deterministic failures with no external effect and should simply fail
    the attempt with a reason.
12. Collection failures are also recorded as unknown, even though the
    thread has stopped and the workspace is still on disk. Nothing retries
    the collection.

### D. Missing services in coordinator mode

13. `cmdRun` returns after `runBacklogV2` in coordinator mode, so the
    quota watchdog, `t3-wait` polling, and archiving never run on this
    host. Provider bucket observations go stale (the newest codex reading
    was from 16:38 with a one-hour maximum age), so metered pools stay
    closed, and every registered wait stays at `runs: 1` forever.

### E. Output retention

14. The worker deletes the attempt workspace immediately after
    collection. Declared outputs survive in custody, but every other file
    and every local git commit the agent made is gone before anyone can
    look at it.

## Fixes applied on `fix/core-reliability-20260911`

- Per-record isolation in the worker reconcile loop, quota planning-state
  reconstruction, planning input, worker state transitions, worker
  command planning, offer building, and custody outbox discovery. Bad
  records are logged and skipped; the rest of the system keeps moving.
- The worker adopts the coordinator epoch from an authenticated envelope
  (monotonic, never lower than its journal), so
  `local_worker.coordinator_epoch` is optional and restarts need no manual
  edit.
- Packages and custody accept any coordinator epoch not newer than the
  current one; command identity includes the coordinator epoch so pending
  commands are re-issued after a restart; re-offers of an assignment the
  worker already holds are accepted when only the epoch differs.
- Lease expiry no longer stops threads on the worker. The coordinator
  re-claims an unknown assignment when a fresh worker snapshot still
  reports it.
- Deterministic failures become a worker `failed` phase that publishes a
  synthetic result with a `BACKLOG STATUS: failed` marker; the coordinator
  imports it as a terminal failed attempt with the reason. Unknown records
  are re-observed on every reconcile and resolve themselves when the
  thread is proven missing, stopped, or running.
- Upload manifests are valid for 30 days and never rejected for age on the
  import path.
- The quota watchdog, waits, and archive run alongside the coordinator.
- Workspaces and terminal journal records are retained for
  `backlog_v2.storage.retention` (default 72h) and pruned afterwards.
- A startup repair pass fixes offered assignments without a thread ID and
  releases assignment rows that their attempt no longer references.
- The T3 shell snapshot is cached per worker exchange so reconciliation of
  many attempts does not fetch it once per attempt.

## Defects found only under live load (fixed on the same branch)

The toy chains exposed a second layer of fail-closed fences that the log
review could not show because the earlier stalls masked them:

15. Worker command acknowledgements were rejected as "not current" when
    the worker sequence had advanced past the planning snapshot, which it
    always does because every exchange reconciles. Only the epochs fence an
    acknowledgement now, and one bad acknowledgement no longer drops the
    others.
16. Lease renewals carried the same sequence fence and failed the whole
    batch; they are now validated per renewal by epoch and lease token.
17. `coordinator_worker_commands` has a unique index on
    (assignment, epoch, kind). A fresh command under a new coordinator
    epoch collided with the unacknowledged one from the previous epoch;
    the stale record is superseded before insert.
18. The lease-expiry audit event was keyed only by assignment and epoch,
    so a re-claimed assignment that expired twice failed the whole worker
    reconciliation; the key now includes the expired lease time.
19. Result discovery served the outbox one entry at a time in name order;
    a deferred upload hid every result behind it. The poll request now
    carries an exclude list and the coordinator walks past deferred
    entries within one pass.
20. Uploads for attempts that already reached a different terminal
    outcome were re-reported every tick; they are acknowledged and
    discarded.
21. `AttemptFinalizer` was not idempotent (the second capture of the same
    attempt failed on `rename`), and a repeated capture published under
    fresh artifact IDs was rejected as "changed immutable content". The
    finalizer replaces its previous capture and a pending upload is
    replaced by the newer one.
22. T3 rejects a settle command whose deterministic ID it has already
    consumed; settlement now retries once under a fresh ID, and an
    unproven settlement no longer blocks completion (it is retried later
    from the completed record).
23. T3 marks a turn completed slightly before the final assistant message
    is projected, so a collection could capture an empty message and fail
    the task; the message is re-read a few times when the marker is
    absent.
24. A worker process killed mid-exchange left an in-flight replay record
    that refused every later request ("another durable request must be
    resumed first"); in-flight records older than two minutes are
    abandoned.
25. Reconciliation ran with no time budget before answering a request, so
    slow provider calls made the whole exchange time out; it now runs
    under two thirds of the request deadline and resumes next exchange.
26. Workflow run progress was never persisted after task outcomes (every
    run stayed "queued"); a projection pass now publishes run progress and
    dependent readiness each planning tick.
27. The worker had no durable log; it now appends to `worker.log` beside
    its journal.

## Live verification (2026-09-11, normandy, Muse free model)

- Serial chain of five dependent tasks: succeeded end to end, final output
  `ABCDE`.
- Fan-out/fan-in (root, three parallel tasks, join): succeeded.
- Deterministic verification failure: task failed with the verification
  reason, dependent stayed blocked, assignment completed (not unknown).
- Coordinator restart with tasks preparing and running: the worker adopted
  the new epoch without a configuration edit and the chains continued.
- Admin `retry` of the two tasks that failed on the collection race:
  attempt 2 ran in an epoch-suffixed workspace and succeeded.
