# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, M2, M3, M4, and the first six M5 increments.
- Added deterministic recovery planning that arbitrates provider slots between paused attempts and already-routed new required work.
- Interactive resumptions rank first; a new required attempt inside the configured deadline-risk window may precede ordinary paused required work; other paused required work precedes ordinary new required work.
- Valid paused surplus work ranks after required work and resumes only when the pool is open and the caller's surplus-admission decision remains true.
- Expired or otherwise ineligible surplus occurrences produce optimistic skipped/stopped transitions without artifact fields, allowing persistence to retain checkpoints and captured artifacts.
- Recovery reconstruction now carries detached task identity, attempt revision, deadline, and expiry metadata alongside fixed-route remaining-cost reservations.
- Existing resuming commands remain replayable without reacquiring their already-counted provider slot.
- Recovery remains deterministic across reordered restart snapshots and pool contention.
- Removed three accidentally duplicated cases from the prior recovery identity table while preserving their unique coverage.
- Marked the sixth M5 checklist item complete.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Recovery policy is transport-neutral and consumes the reconstructed quota snapshot plus an explicit per-pool surplus-eligibility projection from the ordinary quota planner.
- Slot priority is: user-requested interactive resume, immediate-deadline new required work, paused required work, ordinary new required work, then eligible paused surplus work.
- Deadline exceptions apply only to new required work inside the configured risk window; attempt ID is the stable final tie-breaker.
- A recovering pool admits required recovery but not surplus recovery; surplus requires fully open admission and a still-valid occurrence.
- Surplus skip transitions carry only optimistic revision, terminal progress/control, reason, and completion time. They deliberately cannot overwrite checkpoint or artifact references.
- Resume commands continue to be created by the existing replay-safe throttle seam, preserving assignment, worker, thread, workspace, provider route, and provider options.
- Existing pending resume commands are replayed and do not consume a second slot.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/backlog -run 'Test(DeriveQuotaPlanningState|PlanQuotaRecovery|PlanThrottleResumes)' -count=1`
- `go test -race ./internal/backlog -run 'Test(DeriveQuotaPlanningState|PlanQuotaRecovery|PlanThrottleResumes)' -count=1`
- `go test ./...`
- `go vet ./...`
- `go test -race ./internal/backlog -run 'Test(DeriveQuotaPlanningState|PlanQuotaRecovery|PlanThrottleResumes|ThrottleCheckpoint|ReconcileThrottleDeadline)' -count=1`
- `git diff --check`

All passed.

## Remaining risks

- The recovery seam accepts already-routed new required candidates and a surplus-eligibility projection; full atomic coordinator plan persistence and route claims remain M7 work.
- Skip transitions are policy output only until the coordinator persistence layer consumes them; their narrow shape prevents artifact loss by construction.
- Drain races, multiple buckets, recovery probes, user interaction delivery, and repeated throttle epochs still need the final M5 fault-oriented test increment.
- The reconstruction seam is transport-neutral and not yet wired to a full coordinator planning cycle; fleet worker protocol integration remains M7.
- The legacy host-local watchdog still owns its independent bucket, warning, drain, stop, and resume machinery; this increment does not alter or invoke it.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Complete the seventh M5 checklist item with fault-oriented coverage across the existing admission, throttle delivery, turn-outcome, reconstruction, and recovery seams. Add deterministic tests for drain/completion races, hard-stop acknowledgement races, multiple quota buckets and pools, recovery probes with gradual slot admission, interactive resume contention, and repeated throttle epochs including stale or duplicate commands. Fix any defects those tests expose without wiring live worker transport. Run targeted and race tests, then `go test ./...`, `go vet ./...`, and `git diff --check`. If M5 becomes complete, mark its final checklist item and queue the one ungated successor for M6.
