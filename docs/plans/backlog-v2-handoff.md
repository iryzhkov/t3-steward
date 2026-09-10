# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 through M6.
- Added a transport-neutral assignment-dispatch state machine that persists one deterministic T3 thread ID and dispatch token before any worker create call.
- Added durable dispatch projections (`prepared`, `creating`, `confirmed`, `unknown`, and `stopped`) with optimistic revisions and exact-replay handling in SQLite schema version 7.
- Reconciled lost create responses by observing the same thread ID, including the created-but-not-started case, and retried only with the original identity after coordinator restart.
- Kept assignments owned and `unknown` while a worker is unavailable; release occurs only after the worker authoritatively reports the execution stopped or a previously confirmed thread is absent.
- Derived stable, purpose-specific T3 create, turn-start, and message command IDs from the persisted dispatch token.
- Added migration, stale-write, replay, lost-response, restart, deterministic-command, and ambiguous-worker-loss tests using temporary databases and fake/local HTTP workers only.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Dispatch state is separate from lease state, while an uncertain dispatch projects the assignment itself as `unknown` so later planning cannot reassign it.
- `DispatchConfirmedAt` survives worker outages and distinguishes an unconfirmed create retry from a previously confirmed execution that is now proven absent.
- A worker observation distinguishes `missing`, `created`, `active`, and `stopped`; `created` means the deterministic thread exists but its turn is not yet confirmed, so reconciliation safely resends the same dispatch.
- Dispatch preparation accepts only a claimed assignment with empty dispatch state. Repeated preparation compares immutable assignment, route, lease, thread, and token identity and returns the durable projection.
- T3 command and message IDs are deterministic UUID-shaped SHA-256 derivations of the dispatch token plus a purpose label; callers that omit a token retain the legacy random-ID behavior.
- SQLite dispatch transitions use optimistic revisions, validate legal state/assignment projections, reject identity changes, and treat exact replay as success.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/backlog ./internal/store/sqlite ./internal/domain ./internal/control/t3 -count=1`
- `go test ./internal/backlog ./internal/store/sqlite ./internal/control/t3 ./internal/domain -count=20`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- The new dispatch contract is not yet wired into an authoritative fleet coordinator or a real worker protocol; that belongs to M7.
- Worker snapshots, assignment claims, leases, epochs, acknowledgements, partition handling, and coordinator/worker reconciliation remain unimplemented.
- A future worker adapter must classify a present thread without the deterministic turn as `created`, not `active`, before using this state machine.
- Centrally retained artifacts and cross-worker dependency transfer remain part of M7.
- Schedule projection/version advancement outside the trigger operation is not yet an optimistic coordinator transaction.
- Failure-hold acknowledgement/retry/skip/cancel commands remain part of the later admin module.
- The legacy host-local watchdog still owns its independent scheduling and dispatch machinery; this increment did not wire the orchestrator into it.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Start M7 by defining the coordinator/worker protocol records and implementing the authoritative coordinator transaction that commits planner assignments before epoch-bound worker claims. Add concurrent-claim, stale-epoch, restart, and disconnected/stale-worker refusal tests with temporary state and fake workers only; do not contact live workers or T3.
