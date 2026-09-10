# Backlog orchestrator handoff

Updated: 2026-09-09

## Completed checkpoint

- Added provider-neutral domain records for workflows, workflow runs, tasks, attempts, assignments, schedules, triggers, provider routes, quota pools, artifacts, and audited admin commands.
- Added typed scheduling, admission, assignment, trigger, artifact, and command states needed by later planner and persistence work.
- Separated durable progress from execution control. An active attempt can be draining, paused, or resuming without being marked complete; paused attempts release provider concurrency while retaining their workflow progress.
- Added JSON round-trip coverage for every new root domain record and table-driven progress lifecycle coverage.

## Decisions

- Domain records use string identifiers to stay compatible with the existing SQLite and T3 boundaries.
- Provider options remain `map[string]string`, matching the existing backlog submission format while keeping domain types independent of T3 protocol details.
- The only schedule overlap policy in this release is `forbid`, matching the one-open-run invariant in the approved plan.
- Progress terminality is limited to succeeded, failed, cancelled, and skipped. Needs-input and paused execution remain open work.

## Verification

- `go test ./internal/domain`
- `go test ./...`
- `make test lint` (`go test`, race suite, `go vet`, Staticcheck, and gofmt check)

All passed.

## Remaining risks

- The new records are not persisted yet; the current database still contains only the version 1 backlog state.
- State transition validation and coordinator-owned revisions are not implemented.
- Legacy Markdown tasks are not yet adapted into one-task workflow records.
- No development code has opened the live state database or dispatched work.

## Exact next increment

Complete M1's schema migration item: add versioned coordinator tables for the new domain records, implement transactional save/load round trips against temporary SQLite databases, and test migration from the current schema without opening live state.
