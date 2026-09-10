# Backlog orchestrator handoff

Updated: 2026-09-10

## Authority and chain invariant

- Repository: `/home/igor/Work/t3-steward`
- Branch: `feature/backlog-orchestrator`
- S10 starting commit: `4f83df0`
- No install, deployment, live-service restart, live configuration/state mutation by development code, worker contact, push, or pull request is authorized.
- The remaining chain follows the named S10–S13 checklist in `docs/plans/backlog-v2.md`.
- A stage may use `BACKLOG STATUS: continue` to stay in its current T3 thread, but then queues no successor. A completed stage may queue exactly one successor and must end `BACKLOG STATUS: done`. These paths are mutually exclusive.

## Chain audit and correction

- A read-only live T3 shell inspection found exactly two running threads: backlog-v2 thread `92557fb5-884d-4b06-89fd-06f0f43050b7` and unrelated Huyang thread `88a93b83-965c-482c-b18f-b78254447d30`. No second backlog-v2 thread is currently executing.
- Historical duplication came from the old successor prompt allowing `BACKLOG STATUS: continue` after also queueing a successor. The steward continued the same thread while the queued task could create another.
- Replaced open-ended “next coherent increment” selection with four authoritative, substantial stages: S10 mutation CLI, S11 command execution/artifact retrieval, S12 end-to-end hardening, and S13 release readiness.
- Successors now use the title `Backlog-v2 serial implementation successor`, `--max-turns 12`, and `--ungated`.
- The prompt now forbids queueing on `continue` or `needs-input`, forbids `continue` after queueing, and permits exactly one successor only after a complete committed stage.
- Old stopped/needs-input backlog records were inspected but not modified; none can dispatch automatically.

## Completed checkpoint

- Completed M1 through M7 and the query/persistence portions of M8.
- Completed S10 — Revision-fenced admin mutation CLI.
- Replaced the legacy direct `retry`/`cancel` task-state writes with coordinator admin mutations. No `SaveTaskState` mutation remains in the backlog CLI.
- Added `backlog start|delay|pause|resume|cancel|retry|skip <workflow-run>/<task>`.
- Added `schedules run|delay-next|enable|disable <schedule>` command submission.
- Every mutation requires `--reason`, accepts optional `--command-id` for exact replay, and supports `--json`.
- Delay commands require an RFC 3339 `--until` value normalized to UTC. Pause records explicit `{"now":false}` or `{"now":true}` payload intent.
- Task commands query the authorized task projection and use the latest attempt revision. Schedule commands query the authorized schedule projection and use its current revision. The durable store fences the revision again when inserting the command.
- Added cryptographically random 96-bit `admin-` command IDs when the operator does not supply a replay ID.
- Added human rendering for pending, rejected/stale, and failed command decisions, including the durable event and current target revision.
- Added tests for every task/schedule command, invalid or command-specific flags, required audit reasons, timezone normalization, current-revision lookup, replay IDs, generated-ID failures, missing targets/attempts, stale decisions, human/JSON output, routing, and removal of the legacy mutation path.
- Added a temporary-SQLite integration test proving CLI submission persists through coordinator restart and exact replay returns the original timestamped command with one command and one audit event.
- No development binary was installed or run against the live coordinator state database.

## Decisions

- S10 submits schedule `run` as durable intent. S11 must execute it through `CommitScheduleTrigger`, using command-derived deterministic trigger/run IDs so manual-run open-run checks and replay guarantees remain authoritative.
- CLI revision lookup is intentionally separate from the atomic store fence. A race between lookup and submission becomes a durable rejected command with the current target snapshot.
- Operator-provided command IDs are the recovery mechanism after a lost response. Auto-generated IDs are unique for ordinary one-shot invocations.
- The local CLI continues to use the explicit fail-closed `local-admin` authorization seam.
- Artifact retrieval remains S11 scope because it requires a distinct coordinator-owned content interface and checksum validation.

## Verification

- Baseline: `go test ./...`
- `go test ./cmd/t3-steward -count=1 -v`
- `go test ./cmd/t3-steward ./internal/backlogadmin ./internal/store/sqlite -count=1 -v`
- `go test ./cmd/t3-steward -count=20`
- `go test ./...`
- `go build ./...`
- `go vet ./...`
- `git diff --check`

All final commands passed. One mistyped exploratory command used an invalid `-countlf` test flag; it changed no files or state and was immediately replaced by the successful `-count=20` run above.

## Remaining risks

- Admin commands are durable intent but are not yet executed. S11 must implement deterministic policy, atomic state transitions, terminal outcomes, and audit events.
- Start/resume still need hard quota-admission, dependency, resource-lock, and worker-health rechecks at execution time. CLI submission grants no bypass.
- Schedule run/delay-next/enable/disable need executor semantics; manual run must retain existing occurrence and one-open-run idempotency.
- `artifact get` is not implemented.
- Non-admin operations do not yet append native audit events; some read views remain deterministic reconstructions of current projections.
- The coordinator/worker service loop still needs concrete transport binding. No development code has contacted workers or dispatched work.

## Exact next stage

S11 — Admin command execution and artifact retrieval. Implement deterministic pending-command policy and atomic SQLite application for task and schedule commands, including valid transition checks, target-revision fencing, hard quota/dependency/lock/worker rechecks, command-derived idempotent manual schedule triggers, terminal command outcomes/audit events, restart/replay behavior, and coordinator-owned checksum-verified artifact retrieval with safe CLI text/download handling. Complete the remaining M8 checklist items and run all focused and M8 full gates.
