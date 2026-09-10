# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 and M2.
- Continued M3 through worker placement, project catalogs/setup profiles, clean per-attempt Git environments, the T3 `worktreePath` control/API prototype, and an atomic environment reservation seam.
- Added `EnvironmentCoordinator` to reserve workflow checkouts and declared resource locks before preparation or dispatch.
- Workflow-scoped reservations pin a workflow run to its first worker until explicit workflow cleanup and allow only one checkout-mutating attempt at a time.
- Resource locks are validated, sorted canonically, checked as one atomic set, and never partially retained after contention.
- Reservations are idempotent for an unchanged attempt and immutable after creation.
- Terminal completion and cancellation release attempt resources. Pause policy explicitly chooses whether a paused attempt retains or releases resources; either pause form keeps the nonterminal reservation and workflow worker pin.
- A paused attempt that released resources can reacquire them on resume. A retry can reserve after the prior attempt becomes terminal.
- Workflow cleanup refuses while any active or paused attempt remains, then releases the worker pin after every attempt is terminal.
- Added concurrent and table-driven tests for ready workflow branches, deterministic lock contention, atomic acquisition failure, worker mismatch, pause policies, resume, retry, cancellation, and cleanup.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Environment reservation is a small in-memory coordinator seam for M3. Durable coordinator storage, leases, and recovery remain M7 work.
- Workflow worker ownership outlives individual terminal attempts and ends only at explicit workflow cleanup. This prevents later tasks or retries from silently moving a shared checkout.
- Checkout serialization is represented separately from named resource locks so operators can distinguish a busy mutable checkout from an external resource conflict.
- Lock sets are acquired atomically after canonical sorting. A conflict reports the lexicographically first unavailable declared lock.
- `pause-retain-resources` preserves checkout mutation ownership and named locks. `pause-release-resources` releases them but keeps the paused reservation, allowing the same attempt to reacquire on resume.
- Cancellation is an explicit terminal release policy; cleanup never infers terminal state or discards a paused reservation.
- The coordinator supports its zero value as well as `NewEnvironmentCoordinator`.

## Verification

- `go test ./internal/backlog -count=1`
- `go test -race ./internal/backlog -run EnvironmentCoordinator -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- The reservation seam is not yet wired into a planner, persisted assignment transaction, or workspace preparation dispatch path.
- Workflow-scoped checkouts are not yet materialized or retained on disk; the existing `WorkspacePreparer` still intentionally rejects workflow scope.
- Retention cleanup and user-systemd/cgroup process containment remain unimplemented.
- Setup timeout terminates the direct command but does not yet provide the hard descendant-process guarantee required from a user systemd scope/cgroup.
- Cache refresh lacks an interprocess lock and crash-orphan reconciliation.
- Worker inventory, project catalogs, prepared-workspace state, environment reservations, and lock ownership are not yet loaded/persisted through coordinator/worker protocol.
- Stable thread IDs are accepted at the API seam but are not yet generated and persisted with dispatch tokens; that remains the M6 idempotency increment.
- Placement establishes worker eligibility only; provider routing, quota, concurrency, reservations, and ordering remain M4.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Continue M3 by materializing one retained workflow-scoped Git checkout on its pinned worker and integrating it with the environment reservation lifecycle. Prepare and run setup once, expose immutable workflow inputs plus per-task dependency views without allowing a second mutator, preserve the checkout across pause and retry, and remove it only through an explicit terminal retention/cleanup decision. Add temporary-repository tests for sequential task mutation, setup failure, pause/resume, retry, worker mismatch at preparation, and cleanup. Do not install binaries, contact workers, or touch live T3 configuration/state.
