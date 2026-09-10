# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 through M6.
- Continued M7 through authoritative planning, worker snapshots, epoch-bound claims and leases, durable idempotent worker commands, and optimistic worker-state reconciliation.
- Added deterministic assignment/attempt transition planning from fresh complete worker snapshots and immutable command acknowledgements.
- Added an atomic SQLite transition boundary fenced by coordinator epoch, exact worker snapshot identity, assignment identity/state, and attempt revision.
- Fresh present observations recover unknown assignments and rebind them after a worker process-epoch change; fresh stopped or authoritative absent observations safely release ownership for reassignment.
- Completed observations now advance through durable collection before the assignment becomes complete and the attempt enters verification.
- Rejected prepare or dispatch commands release work safely. Accepted dispatch, stop, and collect acknowledgements advance their corresponding projections.
- Delayed immutable acknowledgements remain admissible after a newer sequence from the same worker process, while worker and coordinator epoch changes remain fenced.
- Pending commands are revalidated against current assignment ownership and state immediately before delivery, preventing stale prepare or dispatch after release or process-epoch rebinding.
- Added fake-worker coverage for a lost dispatch acknowledgement followed by worker reconnect and replay of the same command ID with one underlying execution.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Worker snapshot assignment lists are complete. Omission is authoritative only for lease-expired unknown ownership or after a worker process-epoch change; omission from the claim snapshot does not release a newly claimed assignment.
- A present observation may recover an unknown assignment onto the current worker process epoch and extends its safety horizon to the snapshot validity deadline.
- A completed observation first produces a collect command. Only an accepted collect acknowledgement makes the assignment complete and moves the attempt to verification.
- Command acknowledgements are immutable. A delayed response from an older sequence of the same current worker process is accepted and stored; a conflicting later response for the same command is rejected.
- Reconciliation commits before command derivation. The delivery query independently filters commands against the resulting assignment state and worker epoch as a final safety fence.
- Worker transports must continue deduplicating command IDs because an accepted command whose response was lost is intentionally replayed.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/backlog ./internal/store/sqlite ./internal/domain -run 'TestPlanWorkerStateTransitions|TestCommitWorkerStateTransitions|TestReleasedWorkerState|TestFleetCoordinator|TestPlanWorkerCommands|TestWorkerCommands' -count=1 -v`
- `go test ./internal/backlog ./internal/store/sqlite ./internal/domain -count=20`
- `go build ./...`
- `go vet ./...`
- `go test ./...`
- `git diff --check`

All passed.

## Remaining risks

- Centrally retained artifacts and cross-worker dependency transfer remain the final incomplete M7 implementation item.
- Snapshot ingestion remains a separate protocol call; the eventual top-level service loop must preserve snapshot persistence before reconciliation and command delivery.
- Assignment reconciliation updates current projections but does not yet append the full audit-event stream required by the observability milestone.
- Schedule projection/version advancement outside the trigger operation is not yet an optimistic coordinator transaction.
- Failure-hold acknowledgement/retry/skip/cancel commands remain part of the later admin module.
- The legacy host-local watchdog still owns its independent scheduling and dispatch machinery.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Finish M7 with coordinator-owned artifact retention and cross-worker dependency transfer. Define a transport-neutral artifact publication/fetch protocol with immutable metadata and checksum verification; stage declared outputs, checkpoints, preparation logs, final messages, Git state/diffs/commits, and verification reports in coordinator-owned storage; materialize declared predecessor artifacts under the documented dependency path on another worker; fence publication to the current attempt/assignment identity; and add temporary-state integration tests for cross-worker transfer, checksum mismatch, partial upload, replay, offline producers, retention, and path/symlink safety. Do not contact live workers or T3.
