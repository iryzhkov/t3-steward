# Backlog orchestrator handoff

Updated: 2026-09-09

## Completed checkpoint

- Completed M1, the domain and compatibility foundation.
- Added a legacy adapter that packages every visible Markdown backlog task as a version 1 one-task workflow, workflow run, task, and initial attempt while preserving the existing runner-facing task.
- Made legacy workflow identities deterministic from the file name and content digest. Editing a Markdown task creates a new immutable workflow identity; unchanged scans retain the same IDs.
- Preserved legacy project, placement, provider/model/options, importance, difficulty, estimated cost, turn limit, timing, gate, and enabled semantics in the coordinator representation.
- Routed the existing directory loader through the adapter without changing runner behavior.
- Added exact fixtures for default and all-field `t3-backlog` Markdown output, identity/linkage tests, disabled-task mapping coverage, and a frozen ordering test.
- Extended coordinator workflow/task records with the legacy project and explicit estimated-cost fields and covered their JSON and SQLite round trips.

## Decisions

- Legacy `gate: true` (the default) maps to the `surplus` class; `gate: false` maps to `required`. Hard quota admission remains a later planner concern.
- A disabled legacy task is represented as a skipped workflow run and stopped initial attempt. Enabling it changes the source content and therefore creates a new ready workflow identity.
- Legacy workflows use version 1. Version 2 remains reserved for validated workflow manifests.
- The legacy prompt receives a deterministic artifact ID, but copying prompt content into immutable coordinator-owned storage remains part of M2 atomic bundle ingestion.
- The compatibility runner continues to persist and execute its existing task state. Coordinator persistence is not wired into dispatch yet.

## Verification

- `go test ./internal/backlog/...`
- `go test ./internal/domain ./internal/store/sqlite`
- `go test ./...`
- `go vet ./...`
- `make test lint` (includes the full race suite, vet, Staticcheck, and gofmt check)

All passed.

## Remaining risks

- Prompt artifact content is not yet copied into immutable storage; only its future artifact identity is present.
- Workflow manifest parsing, graph validation, and atomic bundle ingestion are not implemented.
- The coordinator records are not yet the live runner's source of truth.
- State transition validation and coordinator-owned revisions remain unimplemented.
- No development code has opened the live state database or dispatched work.

## Exact next increment

Start M2 by defining and validating the version 2 workflow manifest. Cover defaults and every declared field, reject cycles, missing dependencies, duplicate or invalid task names, invalid artifact references, path escapes, unsafe symlinks, and impossible static placement before implementing bundle copying.
