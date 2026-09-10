# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, the domain and compatibility foundation.
- Completed the first two M2 increments: strict version 2 manifest validation, followed by atomic bundle ingestion and immutable static inputs.
- Added a `BundleIngester` that captures the exact parsed manifest bytes, reopens every referenced file beneath an `os.Root`, expands and deduplicates static input globs, and copies manifest, prompt, and input content into coordinator-owned storage.
- Published bundles under `workflows/<workflow-id>/files/` with read-only files and directories. Every retained file has size, SHA-256, media type, storage path, submission provenance, and UTC creation time.
- Converted a submission into workflow, queued workflow-run, task, initial attempt, and input-artifact domain records, then committed the complete metadata snapshot through the existing SQLite transaction.
- Initial attempts are `ready` when dependency-free and `blocked` otherwise. Workflow environment type, scope, ref, and project are retained in the immutable definition.
- Added rollback coverage for invalid bundles and persistence failures. A failed persistence call removes the published files; staging directories are removed on every earlier failure.

## Decisions

- Each version 2 submission creates a new manual workflow run at revision 1 and one initial attempt per task.
- `workflow.yaml` and expanded static inputs are workflow/run input artifacts. Each task has its own prompt artifact and references the shared static-input artifact IDs.
- Input globs are expanded to a sorted unique file list. Bundle-relative names are retained even when a safe internal symlink is materialized as regular copied content.
- Manifest bytes are read once for parsing and storage. Subsequent source opens use resolved bundle-relative paths through `os.Root`, preventing path or symlink replacement from escaping the submitted tree.
- Files are published by a same-filesystem directory rename before metadata is committed. Ordinary validation, copy, rename, and persistence errors leave no committed partial submission.
- Workflow IDs, run IDs, task IDs, attempt IDs, and artifact IDs are independently generated UUID-based identifiers.
- Legacy version 1 workflows and runner behavior remain unchanged.

## Verification

- `go test ./internal/backlog/... -run 'Manifest|BundleIngester' -count=1`
- `go test ./internal/backlog/... -count=1`
- `go test ./internal/domain ./internal/store/sqlite -count=1`
- `go test ./...`
- `go vet ./...`

All passed.

## Remaining risks

- A process or host crash in the narrow interval after filesystem publication and before the SQLite commit can leave an unreferenced bundle directory. It cannot expose partial coordinator metadata, but startup reconciliation or garbage collection should remove such orphans before production.
- Dependency execution state transitions, strict completion, failure propagation, retry, cancellation, output capture, verification, and dependency artifact transfer remain unimplemented.
- The ingester is not yet wired into a submission command or the live runner; integration must continue only against temporary development state.
- Coordinator persistence is still a general snapshot upsert API. Dedicated create/update command semantics and revision enforcement remain later admin/coordinator work.
- No development code has opened the live state database or dispatched work.

## Exact next increment

Implement deterministic DAG execution state transitions for version 2 workflow runs: dependency readiness, explicit verified completion, descendant blocking on failure, cancellation propagation, and retry as a new attempt without rerunning successful ancestors. Add chain and diamond tests plus failure, cancellation, and retry coverage, without wiring development code to the live runner.
