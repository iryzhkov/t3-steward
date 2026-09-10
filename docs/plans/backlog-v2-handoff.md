# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, M2, M3, M4, and the first two M5 increments.
- Added deterministic admission-transition planning that compares durable per-pool revisions with derived admissions and emits canonical fleet batches independent of input order.
- Added persisted quota-admission records and fleet throttle directives, with a schema-v3 migration and ordered load APIs.
- Admission records for every changed pool and their optional directives commit in one SQLite transaction. A directive is returned to outward callers only after that transaction commits.
- Directives are deterministically identified and bound to quota pool, admission revision, canonical bucket epochs, severity, reason, deadline, and creation time.
- Constrained, draining, and closed admissions map to warn, drain, and stop directives respectively. Repeated state in the same epoch is a no-op; a repeated severity in a new epoch produces a new revision-bound directive.
- Exact transaction replay succeeds without duplicate records or directives. Stale revisions reject and roll back the complete multi-pool batch.
- Admission derivation now carries the earliest accepted bucket drain deadline into transition planning without aliasing caller-owned state.
- Marked the second M5 checklist item complete.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Admission planning and persistence remain behind a small store interface; no worker transport or daemon path is connected yet.
- SQLite is the authority for when a directive becomes eligible: admission rows are written first inside the transaction, directive rows second, and neither is externally visible until commit.
- A persisted admission revision changes only when admission, reason, or canonical bucket epochs change. Merely receiving a fresher equivalent observation does not create directive churn.
- An admission severity is reissued when its bucket epoch set changes, allowing workers to distinguish a new provider reset window while keeping same-epoch replay idempotent.
- Directive identity is a deterministic digest of quota pool, admission revision, severity, and bucket epochs.
- Every directive must match its admission record's pool, revision, epoch set, and allowed admission/severity mapping.
- The earliest drain deadline across reconciled buckets is used for the pool directive.
- Existing databases migrate forward transactionally to schema version 3; tests continue to use temporary databases only.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/backlog -run 'Test(Reconcile|Plan)QuotaAdmissionTransitions' -count=1`
- `go test ./internal/store/sqlite -run 'Test(CommitQuotaAdmissionTransitions|MigrationFromVersionOne)' -count=1`
- `go test ./internal/backlog ./internal/store/sqlite ./internal/domain -count=1`
- `go test -race ./internal/backlog ./internal/store/sqlite -run 'Test(DeriveQuotaPoolAdmissions|ReconcileQuotaAdmissionTransitions|PlanQuotaAdmissionTransitions|CommitQuotaAdmissionTransitions|MigrationFromVersionOne)' -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Persisted directives are not yet delivered, acknowledged, or reconciled with affected attempts; that is the next M5 increment.
- Structured warning, drain, checkpoint capture, hard-stop execution, and resume execution remain unimplemented for orchestrated attempts.
- Completion-marker precedence against active throttle intent is not yet implemented.
- The new resume-reservation seam is scheduler-owned and is not yet populated from durable attempts or assignments.
- Paused required cost is derived but has not yet been wired into planner quota-window construction.
- Runtime-slot release, recovery ordering, user-interaction handling, and surplus expiry behavior remain future M5 work.
- The legacy host-local watchdog still owns its independent bucket, warning, drain, stop, and resume machinery; this increment does not alter or invoke it.
- Deferral history, workspace metadata, assignments, route reservations, and quota reservations are not yet coordinator-durable; broader coordinator recovery remains M7.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Implement the third M5 checklist item: structured warn, drain, checkpoint, hard-stop, and resume handling. Build a transport-neutral worker directive/acknowledgement state machine around the committed directives, record affected attempts deterministically, make delivery and acknowledgement replay-safe, and ensure drain deadlines lead to hard-stop intent only after checkpoint opportunity. Persist checkpoint metadata and the control transitions needed to resume the same attempt/thread/workspace/worker/route, while keeping all worker communication behind fakes and disconnected from the deployed daemon. Cover delivery loss, duplicate acknowledgement, partial worker response, checkpoint success/failure, deadline expiry, multiple buckets, and reordered-input determinism. Run targeted and race tests, then `go test ./...`, `go vet ./...`, and `git diff --check`. If implementation remains, queue the one successor with `--ungated` as required by the session prompt.
