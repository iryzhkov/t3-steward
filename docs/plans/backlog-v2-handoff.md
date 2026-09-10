# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 through M6.
- Started M7 with transport-neutral worker snapshot, assignment observation, command, acknowledgement, plan-commit, and claim records.
- Added SQLite schema version 8 with a durable coordinator epoch, monotonic worker snapshots, and indexed assignment worker/epoch/state projections.
- Added an atomic coordinator plan transaction that validates every optimistic attempt revision and the exact healthy worker snapshot used for placement before attaching and publishing any assignment.
- Made exact assignment-plan replay return the existing durable assignments, including after a lost response, without requiring the old worker snapshot to remain current.
- Added epoch-bound worker claims that atomically activate an offered assignment and move its attempt to active/preparing.
- Added coordinator-restart fencing: advancing the durable coordinator epoch rejects delayed snapshots and claims until the worker reconnects against the new epoch.
- Kept assignment dispatch projections and indexes consistent with worker/assignment identity and state.
- Added concurrent-claim, atomic rollback, stale-attempt, disconnected/stale-worker, monotonic snapshot, restart, exact-replay, migration, JSON round-trip, and race tests using temporary databases only.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Coordinator epochs are durable and explicitly advanced by a coordinator session; a worker snapshot names the coordinator epoch it acknowledges.
- Worker process epochs are opaque strings. Snapshot sequence is strictly monotonic inside an epoch, and a new worker epoch must begin at sequence one.
- An assignment is bound to both an assignment epoch and the exact worker epoch selected by the coordinator.
- Planner proposals become offered assignments first. Only a valid worker claim changes the assignment to claimed and the attempt to active/preparing.
- The lease and dispatch tokens are committed with the offer. The worker proves possession of the lease token when claiming; its lease expiry is committed atomically with the attempt transition.
- Plan commits fail closed unless worker inventory is connected, fresh, backlog-enabled, and ready at commit time.
- Batch plan commit is all-or-nothing across assignment inserts and attempt revisions. An identical replay is idempotent; an identity change or a different assignment already attached to the attempt is rejected.
- Existing dispatch state remains separate from lease state, and dispatch identity now also includes the worker epoch.

## Verification

- Baseline: go test ./...
- go test ./internal/store/sqlite ./internal/domain ./internal/backlog -count=1
- go test ./internal/store/sqlite ./internal/domain ./internal/backlog -count=20
- go test -race ./internal/store/sqlite ./internal/domain -count=1
- go test ./...
- go vet ./...
- git diff --check

All passed.

## Remaining risks

- The durable transaction accepts already-built assignment records; the higher coordinator service still needs to translate deterministic planner proposals into those records and own the full planning cycle.
- Worker commands and acknowledgements are defined but not yet persisted, delivered, or reconciled.
- Lease renewal, lease expiry, reconnect reconciliation, and authoritative stopped/absent proofs remain unimplemented.
- Dispatch is protected by claim and snapshot fences at the storage boundary, but the dispatch state machine is not yet driven by a fleet coordinator loop.
- Centrally retained artifacts and cross-worker dependency transfer remain part of M7.
- Schedule projection/version advancement outside the trigger operation is not yet an optimistic coordinator transaction.
- Failure-hold acknowledgement/retry/skip/cancel commands remain part of the later admin module.
- The legacy host-local watchdog still owns its independent scheduling and dispatch machinery.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Continue M7 by implementing the coordinator reconciliation service over the durable protocol: translate planner proposals into assignment-plan commits, persist worker commands and acknowledgements, renew or expire epoch-bound leases, and refuse command delivery/dispatch across stale snapshots or coordinator/worker epoch changes. Add worker reconnect, stale acknowledgement, lease-expiry, coordinator-restart, and no-duplicate dispatch tests with temporary state and fake workers only; do not contact live workers or T3.
