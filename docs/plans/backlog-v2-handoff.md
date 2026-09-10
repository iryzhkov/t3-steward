# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, M2, and M3.
- Completed the first two M4 checklist items: deterministic planning and required/surplus quota admission policy.
- Added immutable quota-window budgets covering capacity, current usage, interactive forecast, active consumption, paused required-work remainder, committed reservations, and safety margin.
- Added immutable per-attempt remaining-cost, expected-runtime, and checkpoint-margin estimates.
- Required and surplus tasks now obey hard admission closure, task not-before/expiry, deadline runway, and quota drain runway. Constrained/recovering admission reserves starts for required work; surplus additionally obeys its eligibility horizon.
- The planner creates a private policy session for every dry run. Accepted proposals reserve quota only within that plan, preventing later proposals from spending the same headroom without mutating reusable policy input.
- Planning blockers now carry machine-readable quota pool/window, admission state, required/available cost, earliest eligibility, and deadline cutoff fields.
- Policy sessions receive isolated task and attempt values, preventing one policy from mutating planner input or another policy's evaluation.
- Table-driven tests cover open, constrained, recovering, draining, and closed admission; required deadline pressure; late-window and premature surplus; forecast/reservation pressure; missing estimates; expiry; not-before; deadline/drain runway; validation; batch reservation; and repeatable dry runs.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- The documented quota formula applies to all work: capacity minus current usage, forecast interactive usage, active consumption, paused required-work remainder, committed reservations, and safety margin.
- Required work differs from surplus through urgency and admission eligibility, not by consuming capacity reserved for interactive or paused required work.
- Empty task class remains compatible with legacy single-task records and is treated as required. Unknown nonempty classes fail closed with an explanation.
- Missing remaining-cost or runtime estimates fail closed as an explainable planning blocker.
- Expected runtime plus checkpoint margin must fit both the task deadline and the window drain cutoff. Equality is allowed.
- A quota policy can include multiple applicable windows; a candidate must fit every window, and a proposal reserves its remaining cost in every window for the rest of the batch.
- Constraint configuration is immutable. `StartPlan(now)` creates private mutable accounting, while `Reserve` records only proposals accepted by all placement, lock, and policy checks.
- Provider routes are deliberately not inferred in this increment. The current policy receives the quota windows applicable to a candidate; route construction will bind workers, instances, models, pools, and route-specific estimates next.

## Verification

- `go test ./internal/backlog -run 'QuotaAdmission|BuildPlan' -count=1`
- `go test -race ./internal/backlog -run 'QuotaAdmission|BuildPlan' -count=1`
- `go test ./internal/backlog -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Provider-route selection, fleet quota-pool binding, provider concurrency, route-specific estimates, fairness, starvation protection, and stale-observation simulations remain M4.
- Quota cost units are normalized by the future route/observation adapter; this policy validates consistent finite units but does not convert provider percentages or tokens itself.
- The planner is not yet connected to dispatch or a persisted assignment transaction; those integrations belong to later milestones.
- Workflow workspace metadata, environment reservations, and quota reservations are not persisted through coordinator restart; durable coordinator recovery remains M7.
- Stable thread IDs are accepted at the API seam but are not yet generated and persisted with dispatch tokens; that remains M6.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Continue M4 by adding ordered alternative provider-route selection to the deterministic planner. Combine each placement-eligible worker's provider inventory with task route preferences, bind provider instances to fleet quota pools, apply route-specific remaining-cost/runtime estimates and shared-pool concurrency limits, and record the selected route in proposals and candidate explanations. Cover ordered fallback, unavailable instance/model exclusions, worker-specific routes, competing providers, shared-pool concurrency, and batch reservation with table-driven tests. Keep fairness/starvation ordering and stale-observation simulations as subsequent M4 increments. Do not dispatch work, contact workers, open live state, or modify deployed configuration.
