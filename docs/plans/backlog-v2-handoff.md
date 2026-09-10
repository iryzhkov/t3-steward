# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 through M7.
- Earlier M8 increments completed the authorized `backlog.admin/v1` read projections plus immutable, revision-fenced command submission, durable audit events, replay safety, and asynchronous outcomes.
- Continued M8 through a read-only CLI adapter over the versioned `BacklogAdmin` query contract.
- Added coordinator-backed `backlog status`, filtered workflow `list`, workflow `show`, DAG `graph`, task `show`, `events`, `explain`, artifact list/show, and command-visibility commands.
- Added the top-level `schedules list|show|history` adapter, including schedule selection and trigger-history rendering.
- Every new coordinator CLI read constructs a versioned, locally authorized admin query and calls `BacklogAdmin.Query`; parsing and rendering are independent of configuration and storage.
- Added stable indented `--json` output using the existing `backlog.admin/v1` response envelope and deterministic human tables/details for every exposed read view.
- Added project, schedule, progress, class, worker, and quota-pool workflow filters. Progress accepts a comma-separated set.
- Removed the old task-file implementation behind `backlog show`; that command now identifies a workflow run and reads its coordinator projection.
- Preserved the legacy `path`, `check`, `receive`, and `new` helpers. `list --all` remains the explicitly legacy local/remote report path, while bare `list` is the coordinator workflow view.
- Retained legacy `retry` and `cancel` temporarily; they remain for the next command-adapter increment so existing callers are not broken before replacement controls exist.
- Added CLI tests for every command shape, all workflow filters, invalid arguments, version/principal injection, service error propagation, JSON selection, human renderers, schedule selection/history, local authorization, and legacy/admin routing.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Human and JSON rendering consume only safe admin DTOs. Artifact output exposes the admin download locator, never the coordinator storage path.
- `backlog artifacts <task>` follows the planned task-oriented form; `<workflow-run>/<task>` is also accepted to resolve a logical task within one run.
- Workflow and task identifiers use the existing `<workflow-run>/<task>` form consistently for task detail, explanations, scoped artifacts, and command visibility.
- Schedule show/history selection happens after the single authorized schedule query. This keeps the transport-neutral query contract unchanged while giving the CLI focused views.
- The local executable supplies an explicit `local-admin` principal derived from the Unix uid, and its authorizer fails closed on a missing identity or role. Future remote transports still need their own authenticated authorizer.
- The adapter itself owns no store. The executable wiring opens the configured coordinator store and injects the service, while tests inject a fake service and therefore cannot open live state.
- Control commands, schedule mutations, artifact retrieval, and execution of pending commands remain deferred as required by this increment.

## Verification

- Baseline: `go test ./...`
- `go test ./cmd/t3-steward -count=1 -v`
- `go test ./cmd/t3-steward ./internal/backlogadmin -count=1 -v`
- `go test ./cmd/t3-steward -count=20`
- `go test ./...`
- `go build ./...`
- `go vet ./...`
- `git diff --check`
- Agent99 project check: `go build ./...` and `go vet ./...`

All passed.

## Remaining risks

- Pending commands are durable intent, not yet an executor: start/delay/pause/resume/cancel/retry/skip and schedule-specific state transitions still need coordinator handlers.
- Legacy `retry` and `cancel` still mutate the legacy task-state projection directly; the next increment must replace their CLI path with revision-fenced admin commands.
- `artifact get` is not implemented. The safe admin DTO has a download locator, but the CLI still needs a coordinator-owned content retrieval seam with checksum validation.
- Non-admin operations do not yet append native audit events. Their admin event views remain deterministic reconstructions of current projections rather than full historical streams.
- Schedule `run` must continue to use the existing transactional trigger path, and all executor handlers must recheck current state and hard quota constraints before applying pending intent.
- The top-level coordinator/worker service loop still needs concrete transport binding. No development code has contacted workers or dispatched work.
- No development code has opened live state, installed or restarted a service, pushed, or deployed.

## Exact next increment

Continue M8 with revision-fenced CLI mutations and coordinator command execution. Replace the legacy direct `retry`/`cancel` state writes and add `backlog start|delay|pause|resume|cancel|retry|skip <workflow-run>/<task>`, requiring an audit reason and using the latest projected attempt revision. Add `schedules run|delay-next|enable|disable <schedule>`, preserving manual `run` through the existing transactional trigger/idempotency path. Implement scheduler-specific pending-command handlers with atomic state/revision checks, durable outcomes and audit events, and hard quota-health rechecks; do not add an ordinary quota bypass. Keep parsing/execution testable with temporary state and add restart, stale-command, replay, invalid-transition, and quota-closure tests. Defer artifact content retrieval if it does not fit the coherent command increment.
