# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 through M5 and the first M6 checklist item.
- Added immutable `ScheduleTemplate` versions while retaining `Schedule` as the mutable current-state projection.
- Added deterministic schedule-template preparation with defaults (`overlap: forbid`, `misfire: skip`, `after_failure: next-cycle`), timezone validation, supported-policy validation, and strict first/next version sequencing.
- Pinned every durable trigger to the schedule-template version it observed and made trigger records append-only with idempotent identical replay.
- Added schema migration v6, including migration of existing schedule snapshots into immutable templates and backfilling existing trigger versions.
- Added round-trip, validation, immutable-history, idempotent replay, and v5-to-v6 migration tests using temporary databases only.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- `Schedule.Version` continues to identify the current template so existing schedule projections remain compatible; historical definition payloads live in `coordinator_schedule_templates` under `(schedule_id, version)`.
- A template version is immutable after insertion. Identical persistence replay succeeds; different content at the same schedule/version is rejected.
- A trigger is immutable after insertion and stores `ScheduleVersion`, so later schedule edits affect only future triggers and workflow runs.
- Only the policies documented for this release are accepted: overlap `forbid`, misfire `skip`, and after-failure `next-cycle` or `hold`.
- Migration can reconstruct only the current template from a pre-v6 schedule snapshot; it does not invent unavailable older versions.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/domain ./internal/store/sqlite -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- One-open-run enforcement, trigger acceptance/suppression, misfire execution, failure holds, and manual runs are not implemented yet.
- Schedule projection/version advancement is not yet an optimistic coordinator transaction; this increment establishes immutable inputs for that transaction.
- Dispatch idempotency and ambiguous worker-response reconciliation remain later M6 work.
- The legacy host-local watchdog still owns its independent scheduling machinery; this increment did not alter or invoke it.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Continue M6 by implementing a transport-neutral, transactional schedule-trigger operation. Atomically deduplicate occurrence keys, enforce one open workflow run per schedule, persist accepted or suppressed trigger history pinned to the current template, and implement the documented skip-misfire and after-failure hold decisions plus manual-run refusal. Add simultaneous-trigger and persistent-catch-up/restart tests against temporary databases. Do not add live timers or T3 dispatch yet.
