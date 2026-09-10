# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, M2, M3, M4, and the first five M5 increments.
- Added deterministic quota-planning reconstruction from canonical tasks, attempts, assignments, route estimates, quota pools, quota windows, and latest throttle records.
- Checkpointed and forced-paused attempts now retain route-specific remaining-cost reservations while releasing their provider concurrency slots.
- Required paused and resuming remainder is copied into every quota window for the shared pool; paused surplus work remains visible for recovery without consuming required-work budget.
- Runtime occupancy is recomputed from attempt control state, counting preparing, running, draining, and resuming work instead of trusting stale pool counters.
- Reconstruction fails closed on duplicate identities, missing fixed-route estimates, settled assignments, and contradictory attempt/throttle/assignment execution identity.
- Resume planning now requires a valid pool snapshot, reacquires provider capacity before creating a resume command, and chooses deterministically under contention.
- Resume commands preserve assignment, worker, T3 thread, workspace, provider route, and provider options.
- Covered checkpointed and forced pauses, shared pools and multiple windows, restart reconstruction, reordered inputs, duplicate records, contradictory durable state, and resumed slot contention.
- Marked the fifth M5 checklist item complete.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- The coordinator supplies a complete durable snapshot to quota reconstruction; derived pool occupancy replaces any stored active-assignment counter in that snapshot.
- A nonterminal attempt must have a matching unsettled assignment before it can hold a provider slot or retain a paused reservation.
- Paused and paused-uncheckpointed work use the latest throttle projection as the authoritative execution identity; resuming work remains reserved and also holds a slot.
- Required remaining cost is reserved in each applicable quota window. Surplus remainder stays in the resume queue but does not reduce the required-work budget.
- Route estimates are keyed to the attempt's fixed worker, provider instance, model, and options; recovery never selects a replacement route.
- Duplicate and contradictory durable records are errors rather than guessed through.
- Pool contention is resolved in canonical attempt-ID order in this increment; recovery priority and expiry policy remain the next M5 item.
- Existing pending resume commands are replayable without trying to acquire their already-counted slot again.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/backlog -run 'Test(DeriveQuotaPlanningState|PlanThrottleResumes|ThrottleCheckpoint|ReconcileThrottleDeadline)' -count=1`
- `go test -race ./internal/backlog -run 'Test(DeriveQuotaPlanningState|PlanThrottleResumes|ThrottleCheckpoint|ReconcileThrottleDeadline)' -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Recovery ordering still uses canonical attempt IDs; required-before-new-required, imminent deadline exceptions, interactive priority, and surplus expiry remain future M5 work.
- The reconstruction seam is transport-neutral and not yet wired to a full coordinator planning cycle; fleet worker protocol integration remains M7.
- Resume planning trusts the reconstructed pool snapshot to be transactionally current. Atomic slot claims with coordinator plan persistence remain part of later coordinator integration.
- Assignment completion and workflow-run revision changes are still handled by their existing coordinator paths.
- The legacy host-local watchdog still owns its independent bucket, warning, drain, stop, and resume machinery; this increment does not alter or invoke it.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Implement the sixth M5 checklist item: deterministic recovery priority and expiry behavior for paused attempts. Required paused work should normally precede new required work while allowing an immediate-deadline exception; interactive resumptions outrank backlog recovery; surplus resumes only while surplus admission and occurrence validity remain, otherwise it becomes skipped with artifacts retained. Integrate these decisions with the quota reconstruction and slot-reacquisition seams without changing assignment, worker, thread, workspace, or provider route. Cover required-versus-new work, deadline risk, interactive priority, surplus expiry and closure, stable ordering, restart reconstruction, and contention. Keep all worker/T3 communication behind fakes and disconnected from the deployed daemon. Run targeted and race tests, then `go test ./...`, `go vet ./...`, and `git diff --check`. If implementation remains, queue the one successor with `--ungated` as required by the session prompt.
