# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, M2, M3, M4, and the first four M5 increments.
- Added deterministic reconciliation for structured `done`, `continue`, and missing turn outcomes against the latest active throttle projection.
- Explicit `done` wins during drain, hard-stop, paused, and pending-resume control. It records verified success or failure canonically and cancels an unsettled throttle command.
- Active drain, hard-stop, paused, or pending-resume control wins over ordinary `continue` and missing status, so a closed pool cannot redispatch another turn.
- Added per-attempt optimistic revisions plus durable outcome ID and marker fields. Exact outcome replay is a no-op and a reused outcome ID with a changed marker is rejected.
- Canonical attempt state and the determining throttle projection commit in one SQLite transaction. Stale attempt or throttle revisions roll the whole batch back.
- Added a schema-v5 migration for indexed attempt revisions while retaining the full JSON record.
- Throttle delivery validation now accepts the existing cancelled delivery state, so completion can make pending commands ineligible for replay.
- Covered done during drain, continue during drain, missing status under closure, late completion after hard-stop intent, pending resume, verification failure, duplicate outcomes, stale revisions, atomic rollback, and reordered inputs.
- Marked the fourth M5 checklist item complete.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Turn observations have stable IDs and are reconciled at most once per attempt revision.
- An explicit done outcome includes the verification result; only verified done succeeds and releases later DAG work.
- Ordinary continue and missing markers remain on their existing path when there is no active throttle conflict. This seam owns only completion and throttle precedence.
- Completion sets control to stopped and makes any pending latest throttle command cancelled. Acknowledged historical commands remain immutable.
- Nonterminal outcomes under active throttle retain workflow progress while copying the throttle projection's control state.
- Terminal attempts ignore later distinct outcomes, preventing a delayed worker report from resurrecting completed work.
- Attempt and throttle optimistic checks complete before either record is written.
- Existing databases migrate forward transactionally to schema version 5; tests use temporary databases only.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/backlog ./internal/store/sqlite -run 'Test(PlanTurnOutcomes|ReconcileTurnOutcomes|CommitTurnOutcomeTransitions|MigrationFromVersionOne)' -count=1`
- `go test -race ./internal/backlog ./internal/store/sqlite -run 'Test(PlanTurnOutcomes|ReconcileTurnOutcomes|CommitTurnOutcomeTransitions|MigrationFromVersionOne|CoordinatorRecordsRoundTrip)' -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Paused required cost is derived but has not yet been folded from durable paused throttle records into planner quota-window reservations.
- The new resume-reservation seam is scheduler-owned and is not yet populated from paused throttle attempt records.
- Recovery ordering, user-interaction handling, and surplus expiry behavior remain future M5 work.
- The turn-outcome seam is transport-neutral and not yet connected to worker/T3 completion reports; real worker protocol integration remains M7.
- Assignment completion and workflow-run revision changes are still handled by their existing coordinator paths rather than this focused attempt/throttle transaction.
- The legacy host-local watchdog still owns its independent bucket, warning, drain, stop, and resume machinery; this increment does not alter or invoke it.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Implement the fifth M5 checklist item: derive planner reservations and runtime-slot occupancy from durable paused throttle attempts. Keep paused and paused-uncheckpointed required work visible through its remaining-cost reservation while ensuring both release provider concurrency slots; resuming work must reacquire a slot without changing its assignment, worker, thread, workspace, or provider route. Populate the existing quota resume-reservation seam from canonical attempt/throttle state, reject stale or contradictory records conservatively, and cover checkpointed and forced pauses, shared quota pools, restart reconstruction, resumed slot contention, duplicate records, and reordered inputs. Keep all worker/T3 communication behind fakes and disconnected from the deployed daemon. Run targeted and race tests, then `go test ./...`, `go vet ./...`, and `git diff --check`. If implementation remains, queue the one successor with `--ungated` as required by the session prompt.
