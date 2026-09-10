# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 through M6.
- Continued M7 with a transport-neutral authoritative coordinator seam.
- Added deterministic planner-to-assignment translation that replaces caller-supplied worker inventory with current durable epoch-bound snapshots and commits the complete proposed assignment batch atomically.
- Assignment, lease, dispatch, and lifecycle command identities are deterministic across coordinator reconstruction.
- Added stable prepare, dispatch, stop, and collect command derivation from durable assignment, attempt, acknowledgement, and worker-observation projections.
- Added durable command history queries and a delivery loop that persists commands before transport, reloads only currently deliverable pending commands, and records every returned acknowledgement even alongside a partial transport error.
- Added a fake idempotent worker integration test covering assignment planning, claiming, a lost prepare response, coordinator reconstruction, command replay, and dispatch without duplicate worker execution.
- Added lifecycle derivation coverage for all command kinds and repeated transactional tests using temporary databases only.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- The coordinator obtains worker inventory exclusively from durable snapshots in the current coordinator epoch. Disconnected or expired snapshots are exposed to the deterministic planner as offline, preserving useful blocker explanations while preventing assignment commit.
- Planner proposals are translated into deterministic assignment, lease, and dispatch identities keyed by attempt. Atomic store validation remains the final fence against stale attempt revisions or changed worker snapshots.
- Worker command IDs are deterministic per assignment epoch and command kind. Existing durable command records, including immutable acknowledgements, determine lifecycle advancement after restart.
- Commands are committed before transport and reloaded through the exact current worker snapshot. A transport may return acknowledgements with an error; the coordinator persists every acknowledgement it did receive before returning that error.
- Prepare must be accepted before dispatch is created. A stopped attempt produces stop, and an explicit completed worker observation produces collect. Unknown assignments never receive prepare or dispatch.
- Worker transport implementations must deduplicate command IDs. The fake transport demonstrates a lost response followed by replay of the same ID with one underlying execution.
- Worker observations do not yet mutate assignment or attempt projections. That mutation needs its own optimistic transactional transition API rather than using the generic coordinator-record upsert.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/backlog -run 'TestFleetCoordinator|TestPlanWorkerCommands' -count=1 -v`
- `go test ./internal/backlog ./internal/store/sqlite ./internal/domain -count=20`
- `go build ./...`
- `go vet ./...`
- `go test ./...`
- `git diff --check`

All passed.

## Remaining risks

- Worker assignment observations and command acknowledgements still need optimistic transactional reconciliation into assignment, attempt, preparation, dispatch, stop, and collection projections.
- Lease-expired unknown assignments still need authoritative present/stopped/absent proofs before recovery or reassignment.
- Snapshot ingestion remains a separate protocol call; a higher coordinator cycle still needs to order snapshot persistence, observation reconciliation, admission derivation, planning, lease expiry, and command delivery.
- Centrally retained artifacts and cross-worker dependency transfer remain part of M7.
- Schedule projection/version advancement outside the trigger operation is not yet an optimistic coordinator transaction.
- Failure-hold acknowledgement/retry/skip/cancel commands remain part of the later admin module.
- The legacy host-local watchdog still owns its independent scheduling and dispatch machinery.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Continue M7 with optimistic worker-state reconciliation. Define explicit assignment/attempt transitions planned from current command acknowledgements and worker assignment observations, commit them atomically against assignment identity/state and attempt revision, and make fresh present/stopped/absent observations recover or safely release lease-expired unknown assignments. Integrate this ahead of command derivation in the coordinator cycle. Exercise worker reconnect, process-epoch change, stale snapshots, rejected commands, lease-expired unknown assignments, lost acknowledgements, and no-reassignment/no-duplicate-dispatch guarantees with temporary state and fake workers only; do not contact live workers or T3.
