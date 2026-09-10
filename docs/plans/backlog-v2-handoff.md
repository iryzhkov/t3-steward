# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, M2, M3, M4, and the first three M5 increments.
- Added a transport-neutral throttle command and acknowledgement state machine for warn, drain, hard-stop, and resume handling.
- Persisted every affected-attempt command intent before transport, using optimistic per-record revisions and an atomic schema-v4 SQLite batch.
- Added deterministic command identities and canonical per-worker delivery so lost responses and repeated reconciliation resend the same command safely.
- Partial worker responses retain unacknowledged commands as pending while persisting every valid acknowledgement received.
- Structured drain acknowledgements capture validated checkpoint metadata or retain the attempt in draining state after checkpoint failure.
- Drain deadline expiry creates a new hard-stop command only for attempts still draining. Late acknowledgements for superseded commands are ignored.
- Acknowledged hard stops become paused-uncheckpointed; acknowledged checkpoints become paused. Both release their provider runtime slot through the existing control-state contract.
- Open or recovering admission creates a resume command that retains the same assignment, epoch, thread, workspace, worker, provider route, and checkpoint metadata.
- Added reconciliation entry points for initial directives, deadline expiration, and recovery resume, all behind fakeable store and worker-transport interfaces.
- Covered persisted-before-delivery ordering, delivery loss, partial response, duplicate acknowledgement, checkpoint success/failure, deadline timing, hard stop, recovery admission, execution-identity retention, multiple workers/pools, and reordered inputs.
- Marked the third M5 checklist item complete.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Worker commands are immutable and idempotent. Their IDs include directive, attempt, command kind, and record revision.
- Command intent commits before a worker sees it; acknowledgement state commits after transport returns.
- Valid acknowledgements returned alongside a transport error still commit, while missing responses remain pending for replay.
- A checkpoint is accepted only with artifact ID, safe relative path, checksum, positive size, and capture time.
- Checkpoint failure does not immediately hard-stop. The attempt remains draining until the directive deadline.
- A hard-stop command supersedes the drain command at the exact deadline and records the prior command ID so delayed drain acknowledgements cannot reverse it.
- Resume is allowed only from paused or paused-uncheckpointed state when the matching quota pool is open or recovering.
- Resume rejection restores the appropriate paused state; successful resume returns to running.
- The durable throttle projection records control intent separately from workflow progress. Completion precedence remains the next increment.
- Existing databases migrate forward transactionally to schema version 4; tests use temporary databases only.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/backlog -run 'Test(ReconcileThrottleDeliveries|PlanThrottle)' -count=1`
- `go test ./internal/store/sqlite -run 'Test(CommitThrottleAttemptTransitions|MigrationFromVersionOne)' -count=1`
- `go test ./internal/backlog -run 'Test(ReconcileThrottle|PlanThrottle|ThrottleCheckpoint)' -count=1`
- `go test -race ./internal/backlog ./internal/store/sqlite -run 'Test(ReconcileThrottle|PlanThrottle|ThrottleCheckpoint|CommitThrottleAttemptTransitions|MigrationFromVersionOne)' -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Completion markers are not yet reconciled against active or superseded throttle intent; ordinary continue or missing status could still conflict with a drain.
- The durable throttle projection is not yet folded into canonical attempt/assignment persistence in one transaction.
- The new resume-reservation seam is scheduler-owned and is not yet populated from paused throttle attempt records.
- Paused required cost is derived but has not yet been wired into planner quota-window construction.
- Recovery ordering, user-interaction handling, and surplus expiry behavior remain future M5 work.
- A worker protocol adapter and real acknowledgements remain M7 work; this increment uses interfaces and fakes only.
- The legacy host-local watchdog still owns its independent bucket, warning, drain, stop, and resume machinery; this increment does not alter or invoke it.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Implement the fourth M5 checklist item: reconcile completion markers against active throttle intents in the documented order. Add a deterministic turn-outcome seam where explicit done wins and cancels pending resume intent, while active drain, hard-stop, or resume control overrides ordinary continue and missing status. Persist the canonical attempt/control update together with the throttle projection so replay cannot redispatch a drained turn or resurrect a completed attempt. Cover done during drain, continue during drain, missing status under closure, late completion after hard-stop intent, duplicate outcomes, stale revisions, and reordered inputs. Keep worker and T3 communication behind fakes and disconnected from the deployed daemon. Run targeted and race tests, then `go test ./...`, `go vet ./...`, and `git diff --check`. If implementation remains, queue the one successor with `--ungated` as required by the session prompt.
