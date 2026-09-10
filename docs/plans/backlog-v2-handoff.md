# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, the domain and compatibility foundation.
- Completed M2, workflow bundles and DAG execution.
- Version 2 manifests validate the complete graph, placement, declared output references, bundle paths, and immutable inputs before atomic ingestion.
- Bundle ingestion retains exact manifest, prompt, and static-input bytes with checksums and coordinator metadata under read-only coordinator storage.
- The deterministic `DAGExecution` engine implements dependency readiness, explicit verified completion, failure blocking, cancellation propagation, and retry without rerunning successful ancestors.
- Added `AttemptFinalizer` to run declared verification commands only after explicit agent success, capture each command/output/exit status as a checksummed JSON artifact, capture declared output files, and return the strict completion result consumed by the DAG engine.
- Missing declared outputs and nonzero verification exits are task failures, not infrastructure errors. Verification stops at the first failed command while retaining completed reports and available declared outputs.
- Added dependency materialization beneath `.t3/dependencies/<producer>/`. It selects only declared dependency outputs, verifies run identity, size, and SHA-256 before atomic publication, and rejects unsafe paths or an agent-controlled `.t3` symlink.
- Added end-to-end unit coverage proving that finalized output verification releases a dependent task, plus missing-output, checksum-mismatch, selected-transfer, verification-failure, command-order, and symlink-safety tests.
- Legacy version 1 workflows and runner behavior remain unchanged.

## Decisions

- Artifact finalization is a persistence-independent coordinator seam. It publishes a read-only artifact tree and returns metadata for a later atomic coordinator update; it does not touch the live runner or database.
- Artifact paths are coordinator-relative and partitioned by workflow run, task, and attempt: `runs/<run>/<task>/<attempt>/artifacts/`.
- Declared task outputs retain their manifest-relative names. Verification reports use deterministic names such as `verification/001.json`.
- Verification commands execute sequentially in the prepared task workspace through `/bin/sh -c`; a nonzero exit stops later commands.
- Missing explicit success runs neither verification nor output capture. Explicit success with no declared verification commands is vacuously verified if all declared outputs exist.
- Dependency callers must supply artifacts already selected for the successful producer attempt. Duplicate task/output metadata is rejected rather than guessed across retries.
- Published artifact trees and dependency trees are staged on the destination filesystem, made read-only, checksum-checked where applicable, and renamed into place.
- The narrow publish-before-metadata window will be reconciled by later coordinator recovery/garbage collection, matching bundle ingestion.

## Verification

- `go test ./internal/backlog/... -run 'AttemptFinalizer|FinalizedVerification|MaterializeDependencies' -count=1`
- `go test ./internal/backlog/... -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Finalized artifact metadata is not yet persisted through dedicated revision-checked coordinator commands; the existing store remains a general snapshot upsert API.
- A process or host crash after filesystem publication and before SQLite commit can leave an unreferenced bundle or attempt artifact directory. Startup reconciliation or garbage collection must remove such orphans before production.
- Verification command output is currently buffered in memory before being encoded in its report; later resource-containment work should impose a configured capture limit while retaining truncation metadata.
- Retry after ambiguous external side effects still requires later idempotency/manual-verification policy.
- Workflow ingestion, DAG transitions, finalization, and dependency materialization remain unwired from the live runner.
- No development code has opened the live state database or dispatched work.

## Exact next increment

Begin M3 by adding a deterministic worker inventory and capability matcher. Define worker health/eligibility and project/provider inventory records, match task host and capability constraints independently from provider routing, explain every exclusion, and add table-driven tests for alternate workers, GPU-only placement, disabled backlog acceptance, and offline or stale workers. Keep the planner seam pure and do not connect it to live fleet state.
