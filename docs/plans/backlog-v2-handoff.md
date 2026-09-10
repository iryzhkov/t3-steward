# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 through M5.
- Added the final M5 fault-oriented matrix covering drain/completion and hard-stop acknowledgement races, multi-bucket and multi-pool isolation, gradual recovery probes, replay of an in-flight resume, deterministic interactive resume contention, and repeated throttle epochs.
- Fixed repeated-epoch ordering so a late acknowledgement for an older command cannot supersede a newer throttle intent. The latest durable throttle projection is selected by command creation time first, then record update time and directive ID as deterministic tie-breakers.
- Verified explicit `done` wins whether completion or a hard-stop acknowledgement is reconciled first.
- Verified recovering pools admit only the available provider slot, replay the identical pending resume command while full, and admit the next paused attempt only after capacity is released.
- Marked the final M5 checklist item complete.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- `ThrottleCommand.CreatedAt` represents throttle-intent order. `ThrottleAttemptRecord.UpdatedAt` represents projection activity and cannot by itself make an old directive current after a delayed acknowledgement.
- Equal command times fall back to record update time and then directive ID, preserving deterministic reconstruction for legacy or same-transaction records.
- Completion and throttle acknowledgements remain optimistic transitions against the same durable revision; whichever commits first is safe, and an explicit verified completion still wins after a confirmed hard stop.
- Recovery-command replay does not acquire another slot. A pending resume retains its stable command ID and fixed execution identity while the pool is full.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/backlog -run '^TestM5' -count=1`
- `go test -race ./internal/backlog -run 'TestM5|Test(DeriveQuotaPoolAdmissions|PlanQuotaAdmissionTransitions|ThrottleCheckpoint|PlanTurnOutcomes|DeriveQuotaPlanningState|PlanQuotaRecovery|PlanThrottleResumes)' -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Throttle planning and fault behavior remain transport-neutral; coordinator-owned worker delivery and reconciliation are M7 work.
- Command ordering uses durable command timestamps because admission revision is not copied into attempt records. Same-time commands are deterministic, but coordinator persistence must continue to serialize admission revisions before delivery.
- The legacy host-local watchdog still owns its independent throttle machinery; this increment did not alter or invoke it.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Begin M6 by adding versioned schedule definitions and durable trigger history. Reconcile the existing schedule and trigger domain/schema types, define immutable schedule-template version semantics plus overlap, misfire, and after-failure policy validation, add migration and round-trip coverage using temporary databases only, and keep scheduling policy behind a transport-neutral seam. Do not yet implement singleton trigger transactions or live timer integration; leave those for the following M6 increment.
