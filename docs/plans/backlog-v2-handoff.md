# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, M2, and M3.
- Completed the first three M4 checklist items: deterministic planning, required/surplus quota admission, and ordered provider routing with fleet quota pools.
- The planner now expands each task route preference across placement-eligible workers in route order, using worker ID only as the deterministic tie-break within one preference.
- Provider candidates validate host pinning, installed/available instances, exact model availability, fleet pool binding, route-specific remaining-cost/runtime estimates, pool admission windows, and shared-pool concurrency.
- Accepted proposals contain the resolved worker-specific provider route and estimate. Candidate decisions retain every evaluated route and machine-readable exclusion.
- Host-local provider instance names may map to different quota pools on different workers. Worker inventory disambiguates those accounts; a fleet-level fallback is used only when exactly one pool contains the instance.
- Route estimates include attempt, worker, provider instance, model, and canonicalized options. Quota admission now evaluates and reserves only windows belonging to the candidate's resolved pool.
- Active assignments and accepted proposals share one pool concurrency counter, including when different provider instances consume the same account pool.
- Planner and routing inputs are copied, sorted, and evaluated without mutation. Reordered workers, pools, and estimates produce the same plan.
- Tests cover ordered fallback, host-pinned routes, unavailable instances/models, missing estimates, pool conflicts, unique and ambiguous pool mapping, worker-specific accounts, competing providers, route-specific quota estimates, existing concurrency, and within-batch shared-pool reservation.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Ordered preference is authoritative: route ordinal precedes worker ordering. The first fully eligible route/worker combination is proposed.
- A route without an available model, unambiguous pool, or route-specific estimate fails closed and remains visible in candidate explanations.
- Provider instance identifiers are host-local. Duplicate names across fleet pools are valid when worker inventory names the pool; an empty worker binding is accepted only when fleet mapping is unique.
- Quota pool concurrency is shared across every worker and provider instance bound to that pool. Paused attempts are expected to be excluded from `ActiveAssignments` by the future coordinator because they release runtime slots.
- Route estimate identity includes canonical option key/value pairs, so estimates for materially different provider options cannot be substituted.
- Tasks with no route retain the prior route-free planning behavior for legacy compatibility. New version 2 workflows are expected to provide materialized routes before assignment.
- Quota admission configuration remains an immutable planning constraint. Provider routing supplies the resolved route and estimate; admission applies only that pool's windows and reserves only that pool after selection.
- Route and policy sessions mutate private batch counters only. They do not create assignments or contact workers.

## Verification

- `go test ./internal/backlog -run 'ProviderRouting|ProviderRoute|RouteSpecific|SharedPool|QuotaAdmission|BuildPlan' -count=1`
- `go test -race ./internal/backlog -run 'ProviderRouting|ProviderRoute|RouteSpecific|SharedPool|QuotaAdmission|BuildPlan' -count=1`
- `go test ./internal/backlog -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Fairness, starvation protection, and the final low-quota/late-week/competing-provider/stale-observation simulation matrix remain M4.
- Route-free legacy tasks still need coordinator-side default route materialization before they can become complete assignment records.
- Quota cost units and observation freshness are normalized by future coordinator adapters; routing validates consistent finite estimates but does not convert provider percentages or infer stale admission state.
- The planner is not yet connected to dispatch or a persisted assignment transaction; those integrations belong to later milestones.
- Workflow workspace metadata, environment reservations, route reservations, and quota reservations are not persisted through coordinator restart; durable coordinator recovery remains M7.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Continue M4 by adding deterministic fairness and starvation protection without weakening required-task deadlines. Define explicit immutable ordering inputs (importance, deadline slack, ready age, prior deferrals, and workflow identity), order tasks before batch selection, and expose the ordering reason in planning decisions. Required tasks at deadline risk must outrank fairness rotation; otherwise prevent one workflow or repeatedly blocked ready task from monopolizing capacity. Add table-driven tests for deadline precedence, equal-priority workflow rotation, repeated deferrals, stable tie-breaking, and interaction with route/quota/resource contention. Keep the final low-quota, late-week surplus, competing-provider, and stale-observation simulation matrix as the following M4 increment. Do not dispatch work, contact workers, open live state, or modify deployed configuration.
