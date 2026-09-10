# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, M2, and M3.
- Completed the first four M4 checklist items: deterministic planning, required/surplus quota admission, ordered provider routing with fleet quota pools, and deterministic fairness with starvation protection.
- Planner callers now provide immutable per-attempt ordering history: ready time and prior deferral count, plus the planning-wide deadline-risk window.
- The planner builds one fleet-wide task order before evaluating routes and reserving quota, workers, workflow checkouts, or resource locks.
- Required tasks inside the deadline-risk window sort first by deadline slack. Normal ordering then uses importance, prior deferrals, ready age, workflow round, workflow-run identity, task name, and task ID.
- Equal-priority tasks are interleaved by workflow round, preventing a workflow with several ready tasks from consuming the whole batch ahead of another equal-priority workflow.
- Every planning decision carries a structured ordering explanation with deadline risk/slack, importance, ready time, prior deferrals, workflow round, and a human-readable reason.
- Deadline-risk tasks that are route- or quota-blocked remain fully explained; the next eligible fairness candidate may be proposed and its reservations affect later decisions normally.
- Tests cover deadline precedence, importance, ready age, repeated deferrals, stable workflow rotation, input-order independence, invalid history, quota-blocked fallback, and resource contention.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Deadline risk is restricted to required tasks whose deadline is within the explicit positive planning window. Surplus work and required work outside the window remain subject to normal fairness ordering.
- A deadline-risk task outranks importance, accumulated deferrals, ready age, and workflow rotation. Among at-risk tasks, the smallest deadline slack wins.
- Importance remains authoritative outside deadline risk. Starvation protection applies among equally important work: more prior deferrals first, then the oldest ready time.
- Workflow rotation is deterministic rather than stateful: tasks receive a per-workflow round after local priority ordering, and equal-priority rounds are interleaved by workflow-run ID.
- Ordering history is required for every nonterminal attempt and fails closed when missing, future-dated, or negative. The planner never mutates the history map.
- Blocked tasks remain in decisions for explanation, but dependency-ready tasks sort ahead of them and alone can consume reservations.
- Provider routing, quota constraints, and resource reservation semantics are unchanged; only the deterministic order in which candidates reach those seams changed.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/backlog -run 'BuildPlan(OrdersByDeadlineThenFairness|RotatesEqualPriority|FairnessInteracts|RejectsInvalidOrdering|Deterministic|OrdersWorkflow|Accounts|Applies)' -count=1`
- `go test -race ./internal/backlog -run 'BuildPlan(OrdersByDeadlineThenFairness|RotatesEqualPriority|FairnessInteracts|RejectsInvalidOrdering|Deterministic|OrdersWorkflow|Accounts|Applies)' -count=1`
- `go test ./internal/backlog -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- The final M4 low-quota, late-week surplus, competing-provider, and stale-observation simulation matrix remains.
- Deferral history and ready timestamps are planner inputs; the future coordinator must persist and advance them transactionally after each committed plan.
- Deadline slack currently means wall-clock time until the declared deadline. Runtime-aware admission remains route-specific and is enforced after ordering by quota policy.
- Route-free legacy tasks still need coordinator-side default route materialization before they can become complete assignment records.
- The planner is not yet connected to dispatch or a persisted assignment transaction; those integrations belong to later milestones.
- Workflow workspace metadata, environment reservations, route reservations, ordering history, and quota reservations are not persisted through coordinator restart; durable coordinator recovery remains M7.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Complete M4 with table-driven fleet planning simulations for low quota, late-week surplus admission, competing provider pools, and stale quota observations. Extend the immutable planning input only where the simulations expose a missing seam; preserve the new deadline/fairness ordering and ordered-route semantics. Each scenario must assert proposals, candidate exclusions, quota reservations, and deterministic explanations across reordered inputs. Define an explicit fail-closed freshness rule for quota observations rather than hiding staleness inside generic capacity. Run targeted tests and race tests, then the full M4 test set, `go test ./...`, `go vet ./...`, and `git diff --check`. Do not dispatch work, contact workers, open live state, or modify deployed configuration.
