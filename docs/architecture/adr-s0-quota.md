# S0 ADR: coordinator-owned quota buckets and measured planning

Status: accepted design; implementation belongs to S6. S5 owns the fault fixtures.
The provider classifications below are user decisions, not hypotheses.

## Failure and ownership

`QuotaBridge.ReconcileState` reconstructs all attempt throttle controls before
planning. `coordinatorBoundaryCycle.tick` defers planning and pending admin
execution if that reconstruction fails. One contradictory record therefore blocks
unrelated routes and recovery commands. Pool names duplicated on workers create
another fatal reconciliation path. The September 12 plan records the live counts.

The watchdog owns collecting raw provider observations. The coordinator owns
bucket observations, derived admission decisions, active reservations and policy
transitions in its SQLite database. A worker receives bucket/route directives and
reports effects. It does not independently reconstruct fleet quota policy from
attempt history. Interactive watchdog safety and backlog reservations share the
same bucket identity/observation stream, but have distinct effect ownership.

## Bucket identity and classes

A bucket is provider/account identity, provider window, class and scope. The account
identity prevents either adding duplicate host observations or conflating different
subscriptions. Routes declare every bucket they consume.

| Provider window | Class | Scope and policy |
| --- | --- | --- |
| Claude five_hour | short shared | Interactive reserve, history forecast; rearm after observed reset |
| Claude seven_day | long shared | Daily pacing and hard ceiling |
| Codex primary | long shared | Weekly pacing and hard ceiling |
| Claude seven_day_overage_included | long model-scoped | Fable only; pacing and ceiling |
| Claude seven_day_opus | long model-scoped | Opus only; pacing and ceiling |
| Claude seven_day_sonnet | long model-scoped | Sonnet only; pacing and ceiling |
| Codex secondary | excluded | Spark; record if seen, never admit or block fleet routes |

A Fable route consumes five_hour, seven_day and its Fable bucket. Unknown route
mapping blocks that route with an explanation. An unobserved, stale, future or
contradictory observation blocks its own bucket, not the provider or whole fleet.
A model-scoped limit never blocks another model unless a shared bucket also blocks it.

## Records and transactions

Persist bucket key, observation/reset epoch, source event identity, observed/reset
times, used percentage, policy revision and confidence. Deduplicate observations
by account/window/event; never sum host copies of a percentage. Within an epoch
use the newest credible reading, retaining conflicts as evidence.

A decision stores used, remaining, reserve, bounded interactive forecast with
inputs, active reservation total, pacing allowance, headroom, phase and reason.
An assignment transaction fences all touched decision revisions and reserves cost
in every consumed bucket atomically. Reservations bind assignment/lease identity;
expiry releases the financial reservation, not execution custody. Unknown live
execution stays isolated and cannot be reassigned merely because a reservation
expired. The next observation and recovery incident account for ongoing consumption.

Warn, drain, stop and rearm target buckets, then affected routes/assignments.
Close admission before publishing directives. Persist effect IDs; workers
acknowledge actual containment/checkpoint evidence. Short-window rearm requires an
observed reset epoch, never wall-clock expiry alone. Long-window phases enforce
remaining allowance and ceiling; an observed reset starts a new pacing epoch.

A contradictory throttle record marks only its attempt recovery-required, retaining
its execution binding and evidence. An unknown pool does likewise. Read/status,
cancel/stop and unrelated planning continue. A database corruption or loss of
coordinator authority still stops mutations globally; bucket isolation does not
pretend a broken authoritative database is usable.

## Forecast, cost and dimensions

Every number is percentage points of the named bucket, never generic tokens or a
percentage copied between short and weekly windows. Clamp valid used to [0,100];
invalid source values are incidents rather than silently normalized observations.
Remaining is max(0,100-used). Interactive forecast is finite and between zero and
remaining. Headroom is max(0,remaining-reserve-forecast-active reservations), with
the unclamped terms retained for diagnosis.

Short-window forecast uses at least 14 days of reset-aware history, separating
known steward activity from interactive burn. Missing hours use the observed
window average, not a flat fallback multiplied over the reset horizon. Before
14 days are available, report provisional coverage and use a bounded observed
average plus reserve; an empty history yields conservative closed surplus admission,
not a fictitious high-confidence prediction.

Long windows use remaining divided by days to reset, with a minimum one-day
denominator for the final partial day and reset-aware daily accounting. Allocate
that allowance between interactive demand and backlog using measured proportions
and the configured reserve. Do not multiply a short-window hourly fallback by
hundreds of hours. Store the interactive share used for the remaining-horizon
forecast separately from today's remaining backlog allowance. Admission must pass
both headroom and today's allowance; this avoids treating the entire daily
allowance as both interactive demand and backlog budget.

The provider tailer's Usage channel supplies per-attempt tokens by project/model.
Persist event IDs and committed read offsets with usage deduplication. Convert
usage to bucket percentage only with measured provider/account/window calibration.
A shared quota delta during overlapping interactive work is not attributable task
cost: mark uncertainty, do not call it measured truth. Isolated S5 tasks supply
calibration. Keep both raw usage and the conversion version/source.

Use a bounded percentile (initially p75) of comparable project/model/window costs.
Broaden the cohort only with visible provenance. Cold fallback is an explicit
small per-bucket policy value, bounded by capacity, with uncalibrated status; there
is no universal difficulty-to-weekly scalar. Reconcile reservations with measured
consumption without charging observed usage twice. Runtime estimates describe the
whole attempt, separately from cost and permitted turn count.

## Branch disposition and visibility

Supersede F02 `2f74779` (universal tenfold weekly cold-cost reduction) and
`194911d` (forecast horizon capped to observation freshness). They reduce symptoms
but neither supplies bucket dimensions, measured calibration or a forecast over
the correct horizon. `e094f3d` correctly distinguishes whole-task runtime from
max-turn count; port its intent with measured runtime evidence in S6. Do not merge
the branch wholesale. The S0 review records all 17 commits.

S6 adds `quota status [--json]` and `quota explain <run>/<task>`: class, scope,
phase, age, used, reserve, forecast/coverage/inputs, headroom, pacing, reservations,
cost cohort/calibration and blocked routes. Tick samples belong in status/metrics;
logs name state changes and first incidents with counts, not every successful tick.

## Sequencing and evidence

S5 can author/run synthetic bucket fault fixtures and collect token-efficient
Haiku/free-route measurements before S6. It cannot honestly certify the replacement
quota implementation before S6 exists. Preserve failing expectations as explicit
S6 entry evidence; do not call them a passing qualification or reorder stages.
S6 must rerun the quota fixtures and calibration accuracy gate. S7 owns enforcement
qualification, including the week without quota-reconstruction failures.

Required cases: each class at warn/drain/stop/reset; stale one-bucket data; model
isolation; contradictory attempt; unknown pool; reservation lease expiry; restart
at directive commit; >100%/NaN forecast input; missing history; duplicate usage
events; reset boundaries; and factor-of-two estimated/measured cost and forecast
accuracy on attributable measurements. Boundedness alone is not accuracy.
