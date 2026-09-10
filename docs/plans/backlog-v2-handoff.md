# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, M2, M3, M4, and the first M5 increment.
- Added a pure `DeriveQuotaPoolAdmissions` seam that projects each fleet quota pool to `open`, `constrained`, `draining`, `closed`, or `recovering`.
- Pool admission is the conservative maximum across all configured bucket windows: normal maps to open, warned to constrained, draining to draining, and stopped to closed.
- Healthy pools with active pending, eligible, or resuming attempt reservations enter recovering; terminal resume records do not affect admission.
- Active required-work resume reservations contribute their remaining cost to `PausedRequiredWorkRemainder`; surplus reservations remain visible in the recovery count without consuming required-work capacity.
- Shared-account bucket projections are reconciled by freshest observation. Equal-time observations in one epoch use the highest credible phase and conservative health; equal-time epoch conflicts fail closed.
- Missing, stale, future-dated, expired-epoch, epoch-inconsistent, and irreconcilably conflicting observations fail closed with structured, deterministic issues.
- Results, issue order, bucket epoch order, and reservation accounting are invariant to reordered inputs, and caller-owned slices and bucket states are detached.
- Marked the first M5 checklist item complete.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Admission derivation consumes immutable coordinator snapshots and has no dispatch, worker, database, or deployed throttle-controller side effects.
- Observation freshness is evaluated at a required explicit derivation time and maximum age. The exact age boundary remains usable.
- When several workers report one shared bucket, a newer observation supersedes older observations. Same-time observations in the same epoch reconcile to the highest severity and require all reporters to agree that the bucket is healthy.
- Same-time observations that identify different epochs are not safe to order and close admission.
- A bucket epoch must equal `domain.EpochFor(ResetsAt)`; a reset time at or before derivation time is no longer a current epoch.
- A normal bucket whose persisted health flag is false constrains admission, preventing resume while still distinguishing that condition from a hard closure.
- Required paused remainder includes pending, eligible, and resuming attempts until their resume record becomes terminal.
- Input/configuration defects return errors; uncertain runtime quota state produces a closed admission projection with structured issues.
- Pool and bucket ordering is canonical so later atomic planning and directive generation can compare projections reliably.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/backlog -run TestDeriveQuotaPoolAdmissions -count=1`
- `go test -race ./internal/backlog -run TestDeriveQuotaPoolAdmissions -count=1`
- `go test ./internal/backlog -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Admission projections are not yet persisted atomically ahead of warn/drain worker directives; that is the next M5 increment.
- The new resume-reservation seam is scheduler-owned and is not yet populated from durable attempts or assignments.
- Paused required cost is derived but has not yet been wired into planner quota-window construction.
- Structured warning, drain, checkpoint, hard-stop, completion-marker reconciliation, and resume execution remain unimplemented for orchestrated attempts.
- Recovery ordering and surplus expiry behavior remain future M5 work.
- The legacy host-local watchdog still owns its independent bucket and resume machinery; this increment does not alter or invoke it.
- Deferral history, workspace metadata, assignments, route reservations, and quota reservations are not yet coordinator-durable; coordinator recovery remains M7.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Implement the second M5 checklist item: close quota-pool admission atomically before warning or draining affected work. Add a small deterministic transition/directive seam that compares prior and derived pool admissions, persists the complete admission transition before any outward directive becomes eligible, binds directives to pool and bucket epochs, and makes replay idempotent. Cover warn-to-constrained, drain/stop closure ordering, repeated epochs, stale transition revisions, multiple pools, and reordered-input determinism. Keep it disconnected from live state and the deployed daemon. Run targeted and race tests, then `go test ./...`, `go vet ./...`, and `git diff --check`. If implementation remains, queue the one successor with `--ungated` as required by the session prompt.
