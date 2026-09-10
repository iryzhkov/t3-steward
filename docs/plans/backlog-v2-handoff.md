# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, the domain and compatibility foundation.
- Completed M2, workflow bundles and DAG execution.
- Began M3 by completing worker inventory and deterministic capability matching.
- Added durable worker inventory records for health, backlog acceptance, capabilities, project availability/revision, installed provider instances/models/quota pools, observation time, and stable T3 web base URL.
- Added a pure `MatchWorkers` seam that evaluates a task against an immutable worker snapshot without consulting live fleet or provider quota state.
- Placement filters task host allowlists, fleet backlog acceptance, worker health, snapshot freshness, required capabilities, and project availability.
- A worker with `accept_backlog: false` is excluded by default but becomes eligible when the task explicitly names that host, preserving the documented laptop opt-in behavior.
- Provider inventory is retained but deliberately does not affect worker placement; provider/model/quota selection remains a separate routing decision.
- Results sort worker IDs and exclusion reasons deterministically and report every independent exclusion instead of stopping at the first failure.
- Added table-driven and focused tests for alternate hosts, GPU-only placement, disabled backlog acceptance and explicit opt-in, offline and stale workers, project absence, provider independence, invalid inventory, and JSON round trips.
- Legacy version 1 workflows and runner behavior remain unchanged.

## Decisions

- Worker health uses explicit `ready`, `degraded`, and `offline` states. Only `ready` workers can receive new assignments.
- Snapshot freshness is supplied as a matching input rather than hard-coded, keeping policy deterministic and testable.
- Host allowlists and capability requirements are conjunctive. Capability matching is exact and case-sensitive, consistent with manifest validation.
- Explicitly naming a normally disabled worker is the task-level opt-in; it does not bypass health, freshness, capability, or project checks.
- Missing or unavailable project inventory excludes a worker when the workflow names a project.
- Duplicate worker IDs, capabilities, projects, provider instances, or provider models are rejected as ambiguous inventory rather than silently deduplicated.
- Placement returns stable reason codes plus human-readable detail for future planner explanations and admin JSON.
- No worker was contacted and no live fleet snapshot was read.

## Verification

- `go test ./internal/backlog/... -run MatchWorkers -count=1`
- `go test ./internal/backlog/... ./internal/domain -count=1`
- `go test ./...`
- `git diff --check`

All passed.

## Remaining risks

- Worker inventory is not yet persisted or populated by a coordinator/worker protocol; that belongs to M7.
- Placement currently establishes worker eligibility only. Provider routes, quota pools, concurrency, reservations, and ordering belong to the deterministic planner in M4.
- The logical project catalog, named setup profiles, and credential requirements remain unimplemented.
- Per-attempt Git workspace preparation, pinned revisions, caches, process containment, resource locks, and cleanup remain unimplemented.
- Finalized artifact metadata is not yet persisted through dedicated revision-checked coordinator commands.
- Workflow orchestration components remain unwired from the live runner and live state.
- No development code has opened the live state database or dispatched work.

## Exact next increment

Add the logical project catalog and named setup profiles. Define validated project entries for canonical Git repository, default ref, T3 project template, setup profile, resource locks, and named credential requirements; resolve task workflow environments deterministically; reject unknown projects/profiles and unsafe repository/setup definitions; and add table-driven catalog and resolution tests. Keep secrets as names only, perform no clone or setup execution, and do not read live configuration.
