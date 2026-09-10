# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 through M7.
- Continued M8 from the versioned `backlog.admin/v1` read contract through immutable, revision-fenced command submission and durable asynchronous outcomes.
- Added schema version 10 with an append-only, globally sequenced coordinator audit-event table and run/target indexes.
- Added typed attempt, workflow-run, and schedule admin targets plus start, delay, pause, resume, cancel, retry, skip, schedule-run, delay-next, enable, and disable command kinds.
- Added an atomic `SubmitAdminCommand` transaction. It validates kind/target compatibility, loads the target and its current revision, inserts exactly one immutable command, and appends its audit event in the same transaction.
- A matching revision produces a pending command. A stale revision durably produces a rejected command, failure reason, audit event, and the exact current target revision/record for the caller.
- Exact submission replay returns the original durable decision even after time passes or the target advances. Reusing an ID for different immutable command content fails.
- Added an atomic `CompleteAdminCommand` transition from pending to applied, rejected, or failed. Exact outcome replay returns the original timestamped outcome; conflicting terminal outcomes fail.
- General coordinator snapshot saves can insert but cannot mutate admin commands. Audit events are immutable and receive a durable monotonic sequence.
- Extended `BacklogAdmin` with authorized mutation and completion calls, logical workflow-run/task resolution to the latest revisioned attempt, command queries, durable audit-event projections, and a versioned mutation response.
- Authorization runs before any coordinator read or write for both queries and mutations.
- Added temporary-SQLite tests for accepted and stale commands, exact and conflicting replay, concurrent submission, transaction rollback, restart persistence, immutable general saves, authorization denial, async outcomes, query visibility, migration coverage, and frozen mutation JSON.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- User-facing task controls target the latest attempt in a workflow run. The SQLite transaction fences the attempt revision again, so state cannot change between API resolution and durable submission without producing a stale rejection.
- Staleness is a durable rejected command rather than an unrecorded error. This preserves the attempted operator action, reason, current target state, and audit sequence.
- Coordinator-generated submission and outcome timestamps are not part of replay identity. A retry after a lost response therefore returns the original record rather than conflicting merely because time passed.
- Admin command IDs are the idempotency keys. Submission and outcome event IDs derive deterministically from them.
- The coordinator only records pending intent and terminal outcome in this increment. A later command executor applies scheduler-specific semantics and reports the asynchronous result.
- The audit schema is generic, but only admin submission/outcome paths append native events so far. Read views retain projection-derived compatibility events for older coordinator state.
- `backlog.admin/v1` mutation DTOs expose command state and safe target snapshots without exposing assignment lease/dispatch capabilities or artifact storage paths.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/backlogadmin ./internal/store/sqlite -count=1 -v`
- `go test ./internal/store/sqlite -run TestAdminCommand -count=20`
- `go test ./internal/backlogadmin -count=20`
- `go test ./...`
- `go build ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Pending commands are durable intent, not yet an executor: start/delay/pause/resume/cancel/retry/skip and schedule-specific state transitions still need coordinator handlers.
- Non-admin operations do not yet append native audit events. Their admin event views remain deterministic reconstructions of current projections rather than full historical streams.
- The current CLI still reads legacy task files/state directly and directly mutates legacy retry/cancel state.
- The CLI does not yet expose the coordinator status, filters, DAG, task, explanation, event, artifact, command, or schedule DTOs.
- Schedule `run` must continue to use the existing transactional trigger path, and all executor handlers must recheck current state and hard quota constraints before applying pending intent.
- The top-level coordinator/worker service loop still needs concrete transport binding. No development code has contacted workers or dispatched work.
- No development code has opened live state, installed or restarted a service, pushed, or deployed.

## Exact next increment

Continue M8 with a CLI adapter over `BacklogAdmin` for read-only coordinator administration: status, filtered workflow list with `--json`, workflow show, DAG graph, task show, events, explanations, artifact list/show, command visibility, and schedule list/show/history. Keep rendering and argument parsing testable without opening live state, preserve the legacy task-file helpers (`path`, `check`, `receive`, and `new`), and route every coordinator read through the admin service rather than direct SQL. Defer command execution and control/schedule mutations to the following coherent increment.
