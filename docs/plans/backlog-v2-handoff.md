# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 through M7.
- Finished M7 with a transport-neutral artifact publication and fetch protocol plus coordinator-owned, content-addressed storage.
- Worker publications stream through temporary storage, verify declared size and SHA-256, become read-only objects, and expose metadata only after an atomic SQLite fence succeeds.
- Publication is fenced to the current coordinator epoch, claimed assignment and assignment epoch, worker process epoch, attempt identity, and attempt revision.
- Exact publication replay returns the immutable record; conflicting metadata is rejected, incomplete streams publish nothing, and failed stale publications remove newly created unreferenced objects.
- Artifact metadata is immutable through both the dedicated publication transaction and the general coordinator snapshot API.
- Cross-worker fetch loads metadata from the coordinator and atomically materializes declared outputs under `.t3/dependencies/<task>/`, with size, checksum, workflow-run, path, and symlink validation.
- Coordinator restart and an offline producer do not affect retained artifact reads. Retention deletes expired metadata first and removes a blob only after confirming that no retained artifact still references it.
- The generic artifact kinds cover immutable inputs, declared outputs, checkpoints, preparation and execution logs, final summaries, Git state/diffs/commits, and verification reports.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Artifact bytes travel separately from transport control messages so large payloads do not enter JSON command envelopes.
- Workers declare artifact identity, provenance, size, checksum, media type, and logical name, but only the coordinator chooses the content-addressed storage path.
- Metadata publication follows durable-bytes-first ordering. A crash may leave an unreferenced object, but never metadata pointing to incomplete or absent content.
- Replay is checked before the current-assignment fence so a worker can recover a lost successful response after the assignment has advanced.
- New publication requires a currently claimed assignment. Unknown, released, completed, reassigned, or revision-changed work cannot add artifacts.
- Retention protects explicitly named workflow runs and never deletes zero-timestamp legacy records automatically.
- Dependency materialization continues to use only explicitly declared predecessor outputs; arbitrary retained artifacts are not injected into a workspace.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/domain ./internal/store/sqlite ./internal/backlog -run 'TestArtifactProtocol|TestCoordinatorArtifact|TestCoordinatorRecordsRoundTrip|TestScheduleTemplatesAndTriggersAreImmutable' -count=1 -v`
- `go test ./internal/domain ./internal/store/sqlite ./internal/backlog -count=20`
- `go test ./...`
- `go build ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- The top-level coordinator/worker service loop still needs to bind a concrete streaming transport to the artifact protocol; this increment establishes and integration-tests the transport-neutral contract and durable implementation only.
- Artifact pruning is metadata-driven and deliberately favors orphaned bytes over dangling metadata after a crash; a later maintenance command may add orphan-object garbage collection.
- Snapshot ingestion remains a separate protocol call; the eventual top-level service loop must preserve snapshot persistence before reconciliation and command delivery.
- Assignment reconciliation updates current projections but does not yet append the full audit-event stream required by the observability milestone.
- Schedule projection/version advancement outside the trigger operation is not yet an optimistic coordinator transaction.
- Failure-hold acknowledgement/retry/skip/cancel commands remain part of M8.
- The legacy host-local watchdog still owns its independent scheduling and dispatch machinery.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Begin M8 by defining the versioned, transport-neutral BacklogAdmin DTO and authorization seams, then implement read-only coordinator queries for status, filtered workflow lists, workflow/task detail, DAG graph, explanations, events, artifacts, worker health, quota admission, reservations, locks, progress, and stable T3 links. Add JSON golden tests and temporary-state query tests before adding revision-checked mutation commands or rewiring the CLI.
