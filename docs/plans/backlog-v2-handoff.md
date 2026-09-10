# Backlog orchestrator handoff

Updated: 2026-09-09

## Completed checkpoint

- Added schema version 2 with coordinator tables for workflows, workflow runs, tasks, attempts, assignments, schedules, triggers, quota pools, artifacts, and audited admin commands.
- Added indexed projection columns for relationships, revisions, progress, trigger occurrence keys, assignment dispatch tokens, and artifact lookup while retaining lossless JSON records.
- Added transactional coordinator batch save and consistent load operations. A constraint failure rolls back the entire batch.
- Added migration coverage from the current version 1 schema and verified that existing state survives migration.
- Added round-trip coverage for every coordinator root record, including embedded provider routes.

## Decisions

- Version 1 remains the existing host-local steward schema. Version 2 is appended transactionally and migration history is read with `MAX(schema_version.version)`.
- Coordinator records retain their canonical domain JSON alongside selected indexed columns. This keeps round trips lossless while leaving stable query seams for later scheduler work.
- Provider routes remain embedded in tasks and assignments because they have no independent identity in the current domain model.
- Batch saves are upserts: records omitted from a batch remain intact. Lifecycle-driven deletion will be added with the coordinator operations that own those transitions.
- Database uniqueness now protects workflow task names, attempt numbers, assignment dispatch tokens, and schedule occurrence keys. The one-open-run transaction remains scheduled for M6.

## Verification

- `go test ./internal/store/sqlite`
- `go test ./...`
- `make test lint` (`go test`, race suite, `go vet`, Staticcheck, and gofmt check)

All passed.

## Remaining risks

- State transition validation and coordinator-owned revisions are not implemented.
- Legacy Markdown tasks are not yet adapted into one-task workflow records.
- Coordinator record retention and deletion semantics are not yet implemented.
- Schedule singleton enforcement and idempotent dispatch remain later milestones.
- No development code has opened the live state database or dispatched work.

## Exact next increment

Complete M1's legacy adapter item: parse every existing Markdown backlog task into a one-task workflow, workflow run, task, and initial attempt without changing current submission behavior. Add compatibility fixtures for existing task parsing and ordering as part of that increment where needed.
