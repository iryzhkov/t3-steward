# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 through M5 and the first three M6 checklist items.
- Added a transport-neutral schedule-trigger request/result contract with deterministic scheduled and manual occurrence keys.
- Added `CommitScheduleTrigger`, which acquires the SQLite write reservation before reading schedule state, deduplicates occurrences, and atomically persists trigger history, accepted workflow runs, and the schedule's active-run projection.
- Enforced one open workflow run per schedule across concurrent coordinator connections.
- Implemented disabled-schedule, skip-misfire, overlap, and after-failure hold suppression; `next-cycle` accepts the next firing after a failed run.
- Implemented manual schedule runs with durable replay, open-run refusal, and failure-hold refusal.
- Added simultaneous-trigger, persistent catch-up, restart replay, failure-policy, and manual-run tests using temporary databases only.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Trigger and workflow-run IDs are caller supplied so transport retries and coordinator restarts can reuse deterministic identities.
- Scheduled occurrence keys are `<schedule-id>/<nominal-UTC-RFC3339Nano>`; manual keys are `<schedule-id>/manual/<trigger-id>`.
- Occurrence replay returns the original immutable trigger and accepted run even if a retry supplies replacement IDs.
- Schedule decisions use the immutable current template, and every trigger remains pinned to that version.
- A no-op schedule update obtains SQLite's write reservation before decision reads, serializing separate coordinator connections without adding a schema-specific mutex.
- Suppressed scheduled firings remain append-only trigger history. Refused manual runs write no trigger or workflow run.
- A failed active run remains the schedule projection: `hold` suppresses scheduled firings and refuses manual runs, while `next-cycle` replaces it only when a new firing is accepted.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/domain ./internal/store/sqlite -count=20`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Deterministic T3 thread IDs and dispatch tokens exist in the domain model, but dispatch persistence/reconciliation is not implemented.
- Lost T3 responses, coordinator restart during dispatch, unavailable workers, and ambiguous worker loss still need fault tests.
- Schedule projection/version advancement outside the trigger operation is not yet an optimistic coordinator transaction.
- Failure-hold acknowledgement/retry/skip/cancel commands remain part of the later admin module.
- The legacy host-local watchdog still owns its independent scheduling machinery; this increment did not alter or invoke it.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Complete M6 by adding an idempotent dispatch state machine around persisted deterministic T3 thread IDs and dispatch tokens. Reconcile lost create responses by querying the same thread ID, retain unknown assignments without reassignment until execution is proven stopped, and add restart, lost-response, and ambiguous-worker-loss tests. Do not contact live workers or T3.
