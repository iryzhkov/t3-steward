# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, M2, M3, and M4.
- Added table-driven fleet planning simulations for low quota, late-week surplus, competing provider pools, and stale quota observations.
- Every simulation asserts selected proposals, route-specific candidate exclusions, reservation effects through exact remaining capacity, deterministic ordering explanations, and identical output after reversing task, worker, provider, pool, window, and estimate inputs.
- Quota windows now carry an explicit observation timestamp, and quota admission requires a positive maximum observation age.
- Planning fails closed with a structured `quota-observation-stale` blocker when an applicable observation is older than the configured limit or is timestamped after planning time.
- Freshness is evaluated per applicable quota-pool window. A stale preferred route remains excluded while a fresh ordered alternative may be selected.
- Freshness boundary, stale, future-timestamp, missing-timestamp, and invalid-age behavior is covered by unit tests.
- No configured/live repository, T3 thread, worker, service, or live database was touched.
- By user direction, the remaining implementation-chain backlog submissions are ungated: use `t3-backlog --ungated`. Hard quota-health controls still apply.

## Decisions

- Observation freshness is distinct from capacity and admission state. Staleness receives its own blocker rather than being hidden as unavailable capacity.
- Every value in one quota window is treated as one immutable snapshot collected at `ObservedAt`.
- The freshness limit is required input to quota policy construction; missing or non-positive limits and missing observation timestamps are configuration errors.
- An observation is usable through the exact maximum-age boundary. Older observations and observations from the future fail closed.
- If several windows apply to one route, every applicable window must be fresh. Reservations continue to be isolated by quota pool and window.
- Structured stale blockers include the quota pool/window, observation time, configured maximum age, route estimate, and the otherwise computed available capacity for deterministic explanation.
- Existing deadline/fairness ordering, ordered provider fallback, concurrency accounting, and resource reservation semantics remain unchanged.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/backlog -run 'TestBuildPlanFleetQuotaSimulations|TestQuotaAdmissionPolicy|TestBuildPlanQuotaAdmission|TestBuildPlanFallsBackUsingRouteSpecificQuotaEstimate' -count=1`
- `go test -race ./internal/backlog -run 'TestBuildPlanFleetQuotaSimulations|TestQuotaAdmissionPolicy|TestBuildPlanQuotaAdmission|TestBuildPlanFallsBackUsingRouteSpecificQuotaEstimate' -count=1`
- `go test ./internal/backlog -count=1`
- `go test -race ./internal/backlog -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Quota freshness currently applies to planner admission snapshots. M5 must derive admission states from bucket observations and resume intents before those snapshots can be built by the coordinator.
- Deferral history and ready timestamps remain planner inputs; the future coordinator must persist and advance them transactionally after each committed plan.
- Deadline slack currently means wall-clock time until the declared deadline. Runtime-aware admission remains route-specific and is enforced after ordering by quota policy.
- Route-free legacy tasks still need coordinator-side default route materialization before they can become complete assignment records.
- The planner is not yet connected to dispatch or a persisted assignment transaction; those integrations belong to later milestones.
- Workflow workspace metadata, environment reservations, route reservations, ordering history, and quota reservations are not persisted through coordinator restart; durable coordinator recovery remains M7.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Begin M5 by deriving deterministic quota-pool admission states from immutable bucket observations and resume intents. Define the input and output seam, precedence across multiple bucket windows, freshness and epoch handling, and the transition reasons needed by later worker directives. Cover open, constrained, draining, closed, and recovering states; paused required-work reservations; stale or conflicting observations; and reordered-input determinism. Keep the derivation pure and disconnected from dispatch, workers, live state, and the deployed throttle controller. Run targeted and race tests, then `go test ./...`, `go vet ./...`, and `git diff --check`. If implementation remains, queue the one successor with `--ungated` as required by the session prompt.
