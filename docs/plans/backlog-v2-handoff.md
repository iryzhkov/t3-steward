# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, M2, and M3.
- M3 now includes worker capability placement, project catalogs and setup profiles, isolated per-attempt Git environments, the T3 `worktreePath` prototype, workflow-scoped checkouts, atomic environment/resource reservations, retained preparation logs, explicit retention, contained setup/verification processes, and filesystem reconciliation.
- Added context-aware advisory file locks under each storage root's `.locks` directory. Lock identities are hashed and stable across processes.
- Repository cache preparation now serializes clone, refresh, and publication per canonical repository. A lock holder removes only stale stages for that repository before proceeding.
- Workflow checkout publication and cleanup now serialize per workflow run across manager processes. Workflow preparation stages live inside their run directory and are reconciled only while that workflow lock is held.
- Explicit `retain` cleanup writes an atomic `retention.json` timestamp. Reconciliation removes an expired checkout only when the coordinator reports that workflow run inactive.
- Active workflow checkouts, unmarked workspaces, and malformed retention state are preserved fail-closed.
- Added concurrent tests for one-mirror cache publication and one-time workflow setup, plus lock cancellation, stale-stage cleanup, retained expiry, active-workflow protection, and malformed-marker cleanup failure.
- The complete backlog package passes under the race detector.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- File locks are persistent inode-backed lock files rather than create/delete sentinels. This avoids the waiter/new-inode race that occurs when a lock file is removed after unlock.
- Lock acquisition uses nonblocking `flock` with a short context-aware retry interval, so shutdown or assignment cancellation does not wait indefinitely.
- Cache stale-stage names include the repository digest. Cleanup cannot remove another repository's in-progress clone even when both share a cache root.
- Workflow stages are placed below `<runs>/<workflow-run>/`; the per-workflow lock therefore protects both publication and stale-stage removal without a global cleanup lock.
- Retention is two-step: terminal cleanup records an explicit retain decision and timestamp; later reconciliation applies an operator/configured cutoff.
- `EnvironmentCoordinator.WorkflowActive` checks both workflow ownership and any surviving reservation. Retention reconciliation never infers inactivity from filesystem age alone.
- Invalid retention metadata stops reconciliation and preserves the checkout for operator inspection.
- Lock and workspace coordinator state is still reconstructed only in memory; durable assignment, lease, and recovery state remains M7 work.

## Verification

- `go test ./internal/backlog -run 'LocalRepositoryCache|FileLock|WorkflowWorkspace' -count=1`
- `go test ./internal/backlog -count=1`
- `go test -race ./internal/backlog -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- The M3 seams are not yet driven by an authoritative planner or persisted assignment transaction.
- Provider routing, quota-pool admission, concurrency, forecast reservations, deadlines, expiry, fairness, and starvation protection remain M4.
- Workflow workspace metadata and environment reservations are not persisted through coordinator restart; durable coordinator recovery remains M7.
- Stable thread IDs are accepted at the API seam but are not yet generated and persisted with dispatch tokens; that remains M6.
- A missing or unhealthy user systemd manager causes setup/verification to fail closed; deployment readiness must verify user-manager availability and lingering on each worker.
- Retention cutoff configuration and scheduled reconciliation wiring remain future coordinator/worker integration rather than M3 filesystem behavior.
- Advisory locking relies on Linux `flock`, matching the supported worker platform.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Begin M4 by extracting a deterministic planner whose input is immutable fleet state plus queued workflow/DAG state and whose output is an ordered batch of proposed task assignments with machine-readable blocker explanations. Reuse dependency readiness and worker placement, but keep provider/quota policy behind explicit input interfaces for later M4 increments. Cover deterministic ordering, independent ready branches, dependency blockers, offline/capability exclusions, resource contention, and dry-run purity with table-driven tests. Do not dispatch work, contact workers, open live state, or modify deployed configuration.
