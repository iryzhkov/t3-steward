# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 and M2.
- Continued M3 through worker placement, project catalogs/setup profiles, clean per-attempt Git environments, the T3 `worktreePath` prototype, environment reservations, materialized workflow-scoped checkouts, and contained child-process execution.
- Added the transport-neutral `ProcessRunner` seam with structured output and exit-code results.
- Added `SystemdScopeRunner`, which uses a deterministic transient user scope with `KillMode=control-group`, waits for completion, captures combined output, and records commands in preparation/verification logs.
- Context cancellation issues `systemctl --user kill --kill-who=all --signal=KILL <scope>` before returning, then also terminates the local `systemd-run` client as a defensive fallback.
- Workspace setup commands and declared verification commands now use the containment seam by default. Tests inject a direct local runner, so no test contacts the live user manager.
- Added tests for scope construction, deterministic unit identity, working-directory propagation, output and nonzero-exit reporting, pre-canceled requests, and cancellation of a spawned descendant.
- Existing workspace, workflow checkout, artifact, verification, and compatibility behavior remains covered and passing.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Process identity is hashed into a stable `t3-steward-<digest>.scope` unit name. This avoids unsafe user-controlled unit characters while making cancellation and later reconciliation deterministic.
- `systemd-run --user --scope --wait --collect --pipe` keeps output attached to steward logging and automatically unloads completed units.
- A nonzero child exit is represented by `ProcessExitError` plus `ProcessResult`, allowing setup to fail operationally while verification preserves the command output and exit code as an artifact.
- The default is contained execution. Direct execution exists only as a test implementation of the interface.
- Git metadata operations remain direct short-lived child commands; setup and verification are the potentially descendant-spawning commands placed behind the scope boundary.
- T3 thread processes remain owned by T3 rather than launched as steward children. This increment does not modify or restart T3.
- Interprocess cache/workflow filesystem coordination and retention reconciliation remain the last M3 work.
- Durable coordinator state and worker recovery remain M7 work.

## Verification

- `go test ./internal/backlog -run 'SystemdScopeRunner|WorkspacePreparer|WorkflowWorkspaceManager|AttemptFinalizer' -count=1`
- `go test ./internal/backlog -count=1`
- `go test -race ./internal/backlog -run 'SystemdScopeRunner|WorkspacePreparer|WorkflowWorkspaceManager|AttemptFinalizer' -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Repository cache refresh lacks an interprocess lock. Concurrent daemon/process preparation can refresh or publish the same mirror simultaneously.
- Interrupted cache and workflow preparation stages have no age-based crash-orphan reconciliation.
- Explicitly retained workflow workspaces have no persisted retention timestamp or expiry cleanup.
- Workflow workspace metadata and environment reservations are not persisted through coordinator restart; durable coordinator recovery remains M7 work.
- The reservation, workspace, and process seams are not yet wired into a planner, persisted assignment transaction, or worker protocol.
- Stable thread IDs are accepted at the API seam but are not yet generated and persisted with dispatch tokens; that remains M6.
- Placement establishes worker eligibility only; provider routing, quota, concurrency, reservations, and ordering remain M4.
- A missing or unhealthy user systemd manager causes setup/verification to fail closed; deployment readiness must verify user-manager availability and lingering on each worker.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Complete M3 by adding context-aware interprocess locks for repository cache preparation and workflow filesystem publication. Reconcile only lock-protected stale staging directories, persist explicit retained-workspace timestamps, and remove expired retained workspaces only when the coordinator reports them inactive. Add concurrent cache/preparation tests plus stale-stage, active-workflow, retained-expiry, and cleanup-failure coverage using temporary repositories and directories. Run the complete M3 package, race, vet, full-suite, and diff gates. Do not install binaries, contact workers, or touch live T3 configuration/state.
