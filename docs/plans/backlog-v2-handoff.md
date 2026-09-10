# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 and M2.
- Continued M3 through worker placement, project catalogs/setup profiles, clean per-attempt Git environments, and the T3 `worktreePath` control/API prototype.
- `NewThreadInput` now accepts caller-selected `threadId`, `branch`, and `worktreePath` values while unchanged legacy calls continue to serialize the two checkout fields as explicit `null` values.
- The control adapter preserves one caller-selected thread identity across `thread.create` and `thread.turn.start`.
- A failed create returns the supplied thread ID for reconciliation; legacy generated IDs retain their previous behavior and are not exposed after a rejected create.
- Added a hermetic HTTP integration harness that verifies authentication, command ordering, exact branch/path serialization, stable thread identity, tagged endpoint rejection, transport failure, and legacy null fields.
- Protocol rejection and transport failure leave the prepared checkout untouched and never send the initial turn after a failed create.
- Documented the prepared-checkout command shape and compatibility boundary in `docs/t3-protocol.md`.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Thread identity may be supplied by a durable scheduler. Random ID generation remains the compatibility fallback for the current host-local runner.
- A supplied thread ID is returned on any create error so a future persisted dispatch record can reconcile an ambiguous response instead of selecting another identity.
- `branch` and `worktreePath` remain nullable on the wire. Nonempty input strings are serialized unchanged; empty values are explicit JSON `null`.
- The control adapter does not validate or clean a workspace. Workspace preparation and retention own the directory; T3 owns acceptance of the command.
- The integration harness uses only `httptest` endpoints. It establishes the steward-side protocol and ownership behavior without contacting the live T3 daemon.
- Support for a new T3 release must still check that release's contracts and execute the documented disposable compatibility verification before deployment.

## Verification

- `go test ./internal/control/t3 -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Workflow-scoped checkout ownership and serialization, resource-lock lifecycle, retention cleanup, and user-systemd/cgroup process containment remain unimplemented.
- Setup timeout terminates the direct command but does not yet provide the hard descendant-process guarantee required from a user systemd scope/cgroup.
- Cache refresh lacks an interprocess lock and crash-orphan reconciliation.
- Worker inventory, project catalogs, and prepared-workspace state are not yet loaded/persisted through coordinator/worker protocol.
- Stable thread IDs are accepted at the API seam but are not yet generated and persisted with dispatch tokens; that remains the M6 idempotency increment.
- Placement establishes worker eligibility only; provider routing, quota, concurrency, reservations, and ordering remain M4.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Continue M3 with workflow-scoped environment ownership and deterministic resource locks. Pin every task sharing a workflow checkout to one worker, serialize checkout mutation, acquire declared locks in canonical order, retain ownership through nonterminal states, and release it only under explicit terminal or pause policy. Add table-driven tests for concurrent ready tasks, lock contention, worker mismatch, retry, cancellation, and cleanup. Keep all tests on temporary repositories and state; do not install binaries, contact workers, or touch live T3 configuration/state.
