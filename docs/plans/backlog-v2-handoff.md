# Backlog orchestrator handoff

Updated: 2026-09-10

## Authority and chain invariant

- Repository: `/home/igor/Work/t3-steward`
- Branch: `feature/backlog-orchestrator`
- S12 starting commit: `8162756`
- No install, deployment, live-service restart, live configuration/state mutation by development code, worker contact, push, or pull request is authorized.
- The remaining chain follows the named S10–S13 checklist in `docs/plans/backlog-v2.md`.
- A stage may use `BACKLOG STATUS: continue` to stay in its current T3 thread, but then queues no successor. A completed stage may queue exactly one successor and must end `BACKLOG STATUS: done`. These paths are mutually exclusive.

## Completed stage

- Completed stage: S12 — End-to-end orchestration hardening.
- Starting commit: `8162756`.
- Exit gates: complete temporary/local workflow coverage for dependencies, artifacts, verification, pause, resume, retry, and recurring-trigger suppression; extended integration, migration, fault, no-duplicate-dispatch, and helper-format compatibility coverage; clean race suite without weakening fail-closed behavior; first three M9 items checked.
- Required verification: focused S12 tests; repeated backlog, SQLite, and CLI package suites; `go test ./...`; `go build ./...`; `go vet ./...`; `go test -race ./...`; and `git diff --check`.

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

- `go test ./internal/backlog ./internal/store/sqlite -run 'TestBacklogV2EndToEndLocalWorkflowHardening|TestT3(Job|Backlog)|TestMigrationFromVersion(Nine|Five|One|Six|Eight)' -count=1 -v`
- `go test ./internal/backlog ./internal/store/sqlite ./cmd/t3-steward -count=10`
- `go test ./internal/backlog ./internal/store/sqlite -run 'Test(M5|FleetCoordinator.*(Lost|Replays)|ReconcileAssignmentDispatch|BundleIngesterRollsBack|CoordinatorArtifactPublicationReplayFencingAndPartialUpload|MigrationFromVersionNine|BacklogV2EndToEnd)' -count=20`
- `go test ./...`
- `go build ./...`
- `go vet ./...`
- `go test -race ./...`
- `git diff --check`

All commands passed. Tests opened only temporary databases and temporary artifact, bundle, and workspace roots.

## S11 final checkpoint

- S11 is complete in the stage commit containing this handoff. All code, tests, documentation, milestone checks, and the named-stage check are committed together; no deployment or live-state action was performed.
- Added deterministic pending-command execution policy for task start/delay/pause/resume/cancel/retry/skip and schedule run/delay-next/enable/disable, with terminal applied/rejected outcomes.
- Added store-side atomic attempt/schedule/retry/run mutation plus command-outcome/audit-event persistence. Manual schedule trigger/run creation shares the same SQLite transaction as its admin outcome, with rollback coverage and command-derived IDs. Apply-time open-run or failure-hold races now terminate the command as an atomic durable rejection instead of leaving pending intent behind.
- Added post-execution replay handling so an explicit command ID returns its immutable original decision after retry creates a newer attempt. The closure audit confirmed that CLI recovery may re-read a newer current target revision; replay identity therefore uses the stable selector/kind/actor/reason/payload while SQLite returns the original durable command, including its immutable expected revision and outcome.
- Added mutable attempt start/delay intent and schedule next-trigger delay projections; forced start bypasses timing and ordinary admission but never hard draining/closed quota.
- Start safety now evaluates worker health and quota admission as route-coupled alternatives, accepts an open fallback without requiring all pools to be open, and rejects cross-route worker/quota combinations. The final audit corrected this path to reuse canonical provider routing, including worker provider/model compatibility and quota-pool derivation from worker inventory when a task route omits `quotaPoolId`. Resume is pinned to its assigned worker and quota pool. Released/completed assignments no longer retain resource locks.
- Start/resume/pause applications now carry an order-independent fingerprint of every durable dependency, lock, worker, route-pool, quota-admission, and pause-binding input. The SQLite apply transaction recomputes and requires the exact fingerprint before changing the target; a mismatch rolls back with the command still pending, and the executor reloads/replans it. Applied start/resume plans additionally carry the earliest validity deadline across the fresh worker observations used by policy, and the transaction's own clock rejects the application after that deadline even when the stored fingerprint is unchanged. Regressions prove both quota closure and elapsed worker freshness between planning and apply mutate nothing before replan. The store also refuses any applied start/resume/pause application without its required fence.
- Admin soft/hard pause now resolves an exact worker-reported assignment observation, including workspace path, and atomically persists a replay-safe drain/hard-stop throttle intent with the draining attempt transition, command outcome, and audit event. Submission alone never claims the work is paused. Pending intent replays after restart without reconstructing the directive; checkpoint/hard-stop acknowledgements project `paused`/`paused-uncheckpointed` into the canonical attempt while retaining assignment, thread, workspace, worker epoch, and provider route. The final audit strengthened the SQLite transaction to independently verify the complete route plus worker epoch, assignment epoch, thread, and workspace observation, with tampering/rollback coverage. Missing or changed execution identity is rejected fail closed.
- Cancel now uses the workflow DAG and atomically cancels unfinished descendants while projecting the workflow run. Skip uses the DAG projection without releasing dependencies, including terminal skipped run state when no branch can proceed.
- Added checksum-verified coordinator artifact opening and CLI `artifact get`, with separate authorization, rune-aware ASCII/C1 terminal-control escaping, malformed UTF-8 replacement, an inline media allowlist and size limit, and atomic owner-only downloads. Downloads reject symlinks at every output-directory level, verify the directory identity again after opening it, and perform staging plus no-overwrite hard-link commit relative to that open directory handle so an ancestor swap cannot redirect output.
- Added operator documentation in `docs/backlog-admin.md` and exposed `artifact get` in command usage.
- Expanded coverage for restart/replay, invalid transitions, hard quota closure and fallback routes, worker-observation expiry at apply, assigned-worker resume, released locks, DAG cancel/skip projections, schedule failure hold/open-run refusal and apply-time races, artifact authorization/media/size/path/terminal safety, and concurrent command application. A second pause while an attempt is already draining is rejected so two independent directives cannot race their acknowledgement projections. The closure audit also added C1 control, malformed UTF-8, nested symlink-ancestor and directory-swap regressions, exact owner-only download mode, and staging cleanup after copy failure.
- Verification passing at this checkpoint:
  - `go test ./cmd/t3-steward ./internal/backlogadmin ./internal/store/sqlite ./internal/backlog -count=1`
  - `go test ./cmd/t3-steward ./internal/backlogadmin ./internal/store/sqlite ./internal/backlog -count=10`
  - `go test ./internal/backlog ./internal/backlogadmin`
  - `go test ./...`
  - `go build ./...`
  - `go vet ./...`
  - `go test -race ./...`
  - `git diff --check`
- S11 exit gates are satisfied and M8 lines 424, 426, and 428 are complete. The complete-diff, runtime-wiring, persistence-race, artifact-path/stream, replay-recovery, cross-record revision, elapsed-freshness, and artifact-directory-race audits are finished; their seven findings are resolved with regressions. Replay recovery is verified against the CLI integration test, the focused S11 and artifact suites passed ten consecutive runs, and all full/static/race gates pass from the final code.

## S12 final checkpoint

- S12 is complete in the stage commit containing this handoff. The first three M9 items and the named S12 checklist entry are checked; S13 documentation and release-candidate work remain intentionally untouched.
- Added a complete temporary/local workflow hardening test backed by a real temporary SQLite coordinator store. It ingests a version 2 bundle, enforces dependency readiness, captures and checksum-verifies a declared output, materializes it for a successor, exercises a verification failure and retry, persists pause/checkpoint state, restarts the coordinator store, resumes only under recovering quota admission, completes verification, and records an accepted then overlap-suppressed recurring trigger.
- Added an exact fixture for the currently installed `t3-job enqueue` Markdown format alongside the existing `t3-backlog` fixtures, including route options, host, timing, and one-task workflow adaptation.
- The migration audit found that v10 created the audit-event table without backfilling admin commands persisted by older schemas. Exact command replay and terminal outcome replay therefore failed after migration because their immutable audit events were absent.
- Migration v10 now backfills deterministic submission events for all pre-existing admin commands and outcome events for terminal commands, including attempt workflow/task context. A version-9 migration regression proves both pending-command replay and terminal-outcome replay after restart.
- The end-to-end pause assertion found that SQLite projected checkpoint acknowledgement control but omitted the canonical attempt's checkpoint artifact ID. Throttle transition synchronization now projects the acknowledged checkpoint ID atomically and retains it through resume.
- Focused fault-injection and idempotency coverage includes ingestion rollback, partial artifact publication, throttle races, lost worker-command responses, lost dispatch acknowledgements, deterministic thread identity recovery, migration replay, and duplicate schedule suppression. Repeated focused suites and the full race suite pass.
- No development binary was installed, no daemon or live configuration was changed, no live state database was opened, and no worker or remote host was contacted.

## Remaining risks

- S12 validates orchestration through transport-neutral coordinator seams and local temporary storage. The coordinator/worker service loop still needs concrete production transport binding; no development code contacted workers or dispatched work.
- Native non-admin audit-event coverage remains incomplete; some read views remain deterministic reconstructions of current projections.
- Configuration, operator recovery, backup/rollback, migration point-of-no-return, deployment order, release-candidate evidence, and the deployment-readiness report remain S13 scope.
- No development binary has been installed or run against the live coordinator state database.

## Exact next stage

S13 — Release candidate and deployment readiness. At successor start, use the S12 stage commit at `HEAD` as the starting commit and record S13's exit gates: document configuration, manifests, operator recovery, backup, rollback, migration point-of-no-return, and deployment order; produce the release-candidate commit and deployment-readiness report with exact verification evidence and remaining operational risks; run all release gates, mark M9 and S13 complete, do not deploy, queue no successor, and leave host-wide deployment for explicit user approval.
