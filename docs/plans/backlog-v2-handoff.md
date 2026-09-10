# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 and M2.
- Continued M3 through worker placement, project catalogs/setup profiles, clean per-attempt Git environments, the T3 `worktreePath` control/API prototype, atomic environment reservations, and materialized workflow-scoped Git checkouts.
- Added `WorkflowWorkspaceManager`, which reserves a workflow environment before preparation and publishes one shared checkout at `<runs>/<workflow-run>/workflow` on its pinned worker.
- Initial preparation pins the commit, copies immutable workflow inputs, runs setup once, retains its log, and records the preparation contract outside the checkout.
- Sequential tasks and retries reuse checkout mutations. Each attempt receives a separate immutable dependency view, selected through an atomically replaced `.t3/dependencies` symlink while checkout ownership is held.
- Preparation failure retains the log, removes the incomplete checkout, and releases the failed attempt's reservation without consuming a model dispatch.
- Pause with released resources can reacquire the same attempt and checkout. Retries use the retained checkout, while a different worker remains rejected until terminal workflow cleanup.
- Terminal cleanup requires all attempts to be terminal and an explicit `retain` or `remove` decision; active or paused attempts prevent cleanup.
- Completed M3 coverage for alternate workers, GPU-only placement, offline workers, setup failure, and workspace cleanup.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- The workflow workspace is coordinator-owned state adjacent to, rather than inside, per-attempt views. T3 still receives only the shared `workspace` path.
- Immutable static inputs live once at workflow scope. Dependency artifacts live under `tasks/<task>/<attempt>/dependencies`, so retry and prior-attempt evidence remain available without replacing the checkout.
- The initial pinned repository, ref, setup recipe, timeout, and input artifact fingerprints are recorded in read-only `environment.json`. Later attempts fail closed if that workflow preparation contract changes.
- Setup executes only while publishing the first complete workflow checkout. A failed first setup publishes nothing, so a later retry may prepare from scratch.
- Filesystem publication is serialized separately from environment reservation locking. The existing coordinator remains authoritative for worker pins, checkout mutation ownership, named locks, pause, and terminal state.
- Existing per-attempt preparation behavior and layout remain unchanged.
- Explicit retention policy is intentionally separate from terminal attempt release. Process containment and crash-orphan reconciliation remain M3 work.
- The coordinator and workflow manager remain in-memory seams; durable coordinator state and recovery remain M7 work.

## Verification

- `go test ./internal/backlog -run 'WorkflowWorkspaceManager' -count=1`
- `go test ./internal/backlog -count=1`
- `go test -race ./internal/backlog -run 'WorkflowWorkspaceManager|EnvironmentCoordinator' -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Preparation and task child processes are not yet placed in a user systemd scope/cgroup, so setup timeout terminates the direct command but does not guarantee descendant cleanup.
- Repository cache refresh lacks an interprocess lock, and interrupted workflow/cache preparation has no crash-orphan reconciliation policy.
- Workflow workspace metadata and environment reservations are not persisted through coordinator restart; that remains M7 work.
- The reservation and workspace seams are not yet wired into a planner, persisted assignment transaction, or worker protocol.
- Stable thread IDs are accepted at the API seam but are not yet generated and persisted with dispatch tokens; that remains the M6 idempotency increment.
- Placement establishes worker eligibility only; provider routing, quota, concurrency, reservations, and ordering remain M4.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Finish M3 by adding a testable process-execution seam that runs setup and task child processes in a user systemd scope/cgroup and guarantees hard-stop cleanup of descendants. Add interprocess repository-cache preparation locking plus reconciliation/cleanup for interrupted staging directories and retained workflow workspaces. Cover descendant termination, concurrent cache preparation, stale staging recovery, retention expiry/cleanup, and failure logs with temporary directories and fake process-control boundaries. Then run the full M3 test, race, vet, and diff gates. Do not install binaries, contact workers, or touch live T3 configuration/state.
