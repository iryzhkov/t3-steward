# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 through M6.
- Continued M7 with transport-neutral, durable worker command and lease lifecycle primitives.
- Retained the existing atomic assignment-plan transaction, monotonic worker snapshots, epoch-bound claims, and deterministic dispatch identities.
- Added SQLite schema version 9 with durable worker commands, immutable acknowledgements, a pending-delivery index, a uniqueness fence allowing only one command kind per assignment epoch, and an assignment lease-expiry projection.
- Commands are committed before delivery and can be loaded only through the exact current, healthy worker snapshot. Worker or coordinator restarts fence old commands.
- Exact command and acknowledgement replay is idempotent; conflicting replay and duplicate dispatch command identities are rejected.
- Added exact-snapshot assignment lease renewal. Elapsed claims transition to `unknown` without changing the attempt control projection, so the coordinator cannot mistake lease loss for proof that execution stopped.
- Added migration coverage that reconstructs the lease-expiry projection from version 8 assignment records.
- Added worker-restart, coordinator-restart, stale acknowledgement, duplicate dispatch, lease-renewal, lease-expiry, idempotent replay, and schema migration tests using temporary databases only.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Coordinator epochs remain durable and explicitly advanced by a coordinator session; workers must publish a snapshot bound to the new epoch before receiving commands.
- Worker process epochs remain opaque strings. A new process epoch fences every command addressed to the prior process.
- Command creation is atomic and durable before transport. Delivery requires the request to name the exact latest snapshot sequence, while a command created against an earlier sequence remains deliverable through a later sequence in the same worker and coordinator epochs.
- One prepare, dispatch, stop, or collect command may exist for an assignment epoch. Retries use a new assignment epoch rather than creating a second identity for the same side effect.
- Acknowledgements are immutable. Exact replay succeeds even after a later snapshot, but a previously unrecorded acknowledgement must match the exact current snapshot and every coordinator, worker, and assignment identity.
- Lease renewal must arrive before the current lease expires, extend the expiry, and match the exact current worker snapshot.
- Lease expiry records uncertainty, not termination: assignment state becomes `unknown`; the active attempt and its ownership remain intact until reconciliation proves the worker stopped or finds the original execution.
- RFC3339 text is retained as a query projection, but expiry decisions compare decoded Go timestamps so optional fractional seconds cannot produce incorrect lexical ordering.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/domain ./internal/store/sqlite -count=1`
- `go test ./internal/store/sqlite ./internal/domain -count=20`
- `go build ./...`
- `go test ./...`
- `go test -race ./internal/store/sqlite ./internal/domain -count=1`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- The higher coordinator service still needs to translate deterministic planner proposals into assignment-plan commits and drive command creation/delivery through a worker transport.
- Worker assignment observations are stored in snapshots but are not yet reconciled into assignment, attempt, preparation, or dispatch transitions.
- Lease expiry becomes `unknown`, but reconnect reconciliation still needs authoritative present/stopped/absent proofs before recovery or reassignment.
- Dispatch state is durable and command duplication is fenced at storage, but the fleet coordinator loop does not yet orchestrate prepare/dispatch/collect transitions.
- Centrally retained artifacts and cross-worker dependency transfer remain part of M7.
- Schedule projection/version advancement outside the trigger operation is not yet an optimistic coordinator transaction.
- Failure-hold acknowledgement/retry/skip/cancel commands remain part of the later admin module.
- The legacy host-local watchdog still owns its independent scheduling and dispatch machinery.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Continue M7 with the transport-neutral coordinator reconciliation service: translate deterministic planner proposals into assignment-plan commits, derive stable prepare/dispatch/stop/collect commands, deliver persisted pending commands through a fake worker transport, and reconcile acknowledgements plus worker assignment observations. Exercise coordinator restart, worker reconnect, lease-expired unknown assignments, lost responses, and no-duplicate dispatch end to end with temporary state and fake workers only; do not contact live workers or T3.
