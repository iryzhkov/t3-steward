# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, M2, and M3.
- Began M4 by adding a deterministic, side-effect-free planner over immutable workflow/DAG snapshots, worker inventory, and current ownership snapshots.
- The planner emits an ordered proposal batch plus a decision for every nonterminal task, including machine-readable dependency, attempt-state, placement, resource-lock, workflow-checkout, and policy blockers.
- Workflow runs, tasks, workers, candidate evaluations, locks, and blockers have stable ordering independent of input order.
- Independent ready DAG branches can be proposed together. Proposed reservations are reflected only in the planner's private batch snapshot, so resource and workflow-checkout contention is explained without mutating coordinator state.
- Existing DAG readiness and worker placement logic are reused. Optional deterministic candidate constraints provide an explicit seam for provider and quota policy in later M4 increments.
- Tests cover input-order determinism, independent branches, workflow-run ordering, input immutability, dependency blockers, offline/capability exclusions, resource contention, workflow-checkout contention, and policy-based candidate fallback.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Planning consumes value snapshots and clones all mutable task data exposed to constraints. It never dispatches, reserves, or calls a worker.
- A plan includes decisions for blocked tasks, not only assignments, so administrative explain output can reuse the same result.
- Placement evaluates every worker before provider/quota constraints. Candidate policy is applied only to placement-eligible workers and may select the next deterministic worker.
- Resource and checkout ownership maps use attempt IDs. An attempt may retain ownership of its own reservation without blocking itself.
- Earlier proposals claim locks in a planner-local copy. This makes one batch internally consistent while leaving the input and live coordinators untouched.
- Workflow-scoped checkouts currently serialize ready tasks within a run; later integration can use persisted ownership to preserve the pinned worker across turns.
- Provider routing, quotas, task classes, and scheduling windows remain policy inputs rather than being coupled to worker placement.

## Verification

- `go test ./internal/backlog -run 'TestBuildPlan' -count=1`
- `go test -race ./internal/backlog -run 'TestBuildPlan' -count=1`
- `go test ./internal/backlog -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Provider-route selection, quota-pool admission, concurrency, forecast reservations, remaining-cost accounting, deadlines, expiry, fairness, and starvation protection remain M4.
- The planner is not yet connected to dispatch or a persisted assignment transaction; those integrations belong to later milestones.
- Workflow workspace metadata and environment reservations are not persisted through coordinator restart; durable coordinator recovery remains M7.
- Stable thread IDs are accepted at the API seam but are not yet generated and persisted with dispatch tokens; that remains M6.
- A missing or unhealthy user systemd manager causes setup/verification to fail closed; deployment readiness must verify user-manager availability and lingering on each worker.
- Retention cutoff configuration and scheduled reconciliation wiring remain future coordinator/worker integration rather than M3 filesystem behavior.
- Advisory locking relies on Linux `flock`, matching the supported worker platform.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Continue M4 by implementing required/surplus admission policy behind the planner constraint seam: model immutable quota-window input, hard closure, forecast and committed reservations, paused required-work remainder, safety margin, remaining task cost, expiry, and runtime-versus-drain checks. Add table-driven simulations for open/closed quota, required deadline pressure, late-window surplus, expired occurrences, and insufficient drain runway. Keep route selection and fleet-provider concurrency as the following M4 increment. Do not dispatch work, contact workers, open live state, or modify deployed configuration.
