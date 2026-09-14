# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.11.0-rc.38] - 2026-09-14

### Fixed

- Preflight evidence is captured with the attempt's declared outputs instead of
  being copied in after the capture tree is sealed. Appending it afterwards hit
  a read-only directory, so every attempt retried `permission denied` in the
  collecting phase and no task could settle.
- A bundle archived by an ordinary `tar` of its directory is accepted. Directory
  entries carry a trailing slash, which the archive path rules rejected, and the
  refusal named neither the entry nor the reason. Traversal, absolute paths and
  escapes are still refused, and refusals now name the offending entry.
- Explaining a task no longer reports `eligible: false` together with zero
  blockers. That case means the attempt is already under a control decision, and
  it now says so.

## [0.11.0-rc.37] - 2026-09-14

### Fixed

- Worker journal inspection reads a retained catalog this build cannot activate,
  instead of failing with a digest mismatch. An updater asks for the journal
  precisely when it is about to replace the binary, and replacing the binary is
  what changes how a catalog revision is derived, so the previous behaviour
  refused the caller at the one moment the answer mattered and left a fleet
  upgrade stuck with the new binary installed and the old one still serving.
  Activation still refuses a projection it cannot reproduce; inspection reports
  it as `catalogActivatable: false` and keeps the journal's attempt counts.

## [0.11.0-rc.36] - 2026-09-14

### Added

- Workflow manifests declare resource demand and preflight steps. Resource
  demand carries a minimum CPU class as a hard floor, a preferred class as a
  preference, and CPU units, memory and scratch to reserve, with `build` and
  `light` presets an explicit field always overrides. Preflight declares ordered
  typed steps, each a check or a context probe with a failure policy, an output
  byte limit, a timeout and whether its result belongs in the prompt.
- Placement evaluates CPU class and capacity, and workers carry independent
  executor capacity. CPU class, allocatable capacity and observed pressure are
  three separate facts; none is derived from another. Executor capacity is
  independent of provider-session concurrency and an attempt holds both. A
  reservation is acquired atomically and released exactly once on settlement,
  cancellation, failed preparation and lease recovery. Planning accounts for
  capacity within a pass, so a batch is spread rather than over-assigned, and
  chooses the highest-scoring eligible worker rather than the first.
- Preflight runs on the worker after the workspace is prepared and strictly
  before a provider session is created. A `require-pass` failure or an unrunnable
  step means no session is created at all; a `record` failure launches with the
  failing baseline in the prompt. Evidence is redacted before truncation,
  custodied as artifacts through the existing result path, and reused only on an
  exact identity match inside its freshness window.
- Initial prompts are assembled as a bounded, versioned envelope carrying the
  objective, constraints, mounted input digests, required outputs and compact
  preflight results. A task declaring no preflight keeps its previous prompt
  byte for byte.
- Workers report the package capabilities their build implements, so the
  coordinator excludes an incapable worker before assignment instead of
  discovering the gap mid-attempt.

### Fixed

- The lint gate is pinned to an exact analyzer and toolchain. It previously ran
  `staticcheck@latest`, so its result depended on the host's Go version and on
  the day.

## [0.11.0-rc.29] - 2026-09-13

### Added

- Worker thread operations attach to an execution-specific T3 control for
  directory-bound packages, with no fallback to host T3 after an attachment
  error. Dispatch maps its project and worktree to the contained workspace.
  Preparation remains disabled until provider setup, contained verification
  and custody are integrated.

## [0.11.0-rc.28] - 2026-09-13

### Added

- Dedicated contained T3 provisioning creates an execution-local one-hour token,
  refreshes it without relaunching provider work, and disables implicit startup
  projects/threads. Host token reads stay inside control storage and reject
  provider-created symlink escapes or blocking FIFOs. Normal directory worker
  routing and provider credentials remain gated.

## [0.11.0-rc.27] - 2026-09-13

### Added

- Contained executions can expose their namespace-local API through a separate
  owned Unix socket. The scoped T3 client cannot use host proxies or follow
  redirects to shared services. Socket replacement and overlapping control
  storage are refused. Worker routing, dedicated T3 authentication and live
  provider qualification remain gated.

## [0.11.0-rc.26] - 2026-09-13

### Added

- Durable containment supervisor identities and separate user services survive
  caller cancellation and worker reconstruction. Launch requests are never
  automatically repeated after uncertainty; explicit confirmed control-group
  stops persist custody receipts. Lease expiry has no stop behavior. Scoped T3
  and normal directory worker integration remain gated.

## [0.11.0-rc.25] - 2026-09-13

### Fixed

- Use a short private Unix-socket path for the containment bridge cancellation
  test on macOS. rc.24 release artifacts were built, but its macOS CI failed
  before this test-path correction; rc.24 was not deployed.

## [0.11.0-rc.24] - 2026-09-13

### Added

- A dedicated provider-process containment launcher with descriptor-pinned data
  mounts, separate owned home/output storage, private process/network namespaces
  and a provider hostname gateway that refuses host/private destinations.
  Operator CLI and Linux kernel tests qualify the boundary. Normal directory
  execution remains disabled until durable supervisor, scoped T3 control and
  live provider recovery integration are complete.

## [0.11.0-rc.23] - 2026-09-13

### Added

- Operator project directory catalogs and capsule resource requests resolve exact
  approved identities at submission, default to read-only and reject host paths,
  stale revisions, wrong placement and write escalation. Worker catalog revisions
  fence only local resource changes. Accepted requests persist across SQLite
  restart and idempotent replay. Provider containment remains a required runtime
  gate; existing-directory execution is still refused before effects.

## [0.11.0-rc.22] - 2026-09-13

### Added

- Directory bindings now persist on task records, require exact catalog identity
  authorization, and participate in execution-package hashes and host checks.
  The production planner reconstructs reader/writer ownership after restart,
  retains cancelled or lease-expired unsettled owners, and reserves conflicting
  accesses within each scheduling pass. Bound directory execution remains
  refused before effects until the provider containment backend is integrated.

## [0.11.0-rc.21] - 2026-09-13

### Added

- Read-only `worker inspect-directory` identity diagnostics for S5a: descriptor
  traversal rejects symlinks and missing paths; revalidation fences inode birth
  identity, mounts, ancestors and operator registration revisions. Internal access
  checks default to read-only and detect overlapping writers. Existing-directory
  scheduling and provider containment remain disabled pending integration.

## [0.11.0-rc.20] - 2026-09-13

### Fixed

- Worker preparation removes the execution package's static-input namespace
  before materializing inputs, so capsule files appear at `.t3/inputs/<name>`
  instead of `.t3/inputs/inputs/<name>`. Nested artifact paths are preserved.
  Regression coverage exercises the coordinator's prefixed paths for both Git
  and fresh workspaces.

## [0.11.0-rc.13] - 2026-09-13

### Fixed

- Explicit worker stops durably record successful provider settlement and report
  released assignments, allowing cancelled workflow sinks and resources to settle.
  An accepted stop with an unproven effect still holds ownership. Restart repairs
  older stopped records by confirming the stop; retained workspaces expire after
  the existing retention period. Natural completion and quota pauses keep their
  existing collection and ownership rules.

## [0.11.0-rc.12] - 2026-09-13

### Fixed

- Caller discovery parses prefixed T3 provider events, including Codex native
  payload thread IDs. It matches identity fields rather than quoted tool output.
  Live Codex qualification exposed the missing native-event mapping.

## [0.11.0-rc.11] - 2026-09-13

### Added

- Wait caller discovery accepts OpenCode session IDs alongside Claude and Codex.
  A bundled OpenCode shell hook exports the current session per invocation.
  Explicit thread IDs take precedence; ambiguous caller identities are rejected.

## [0.11.0-rc.10] - 2026-09-13

### Fixed

- Graph task additions reject missing or invalid verification before publication.
  Repeatable `--verify` flags on task add/set supply or replace verification commands,
  allowing unassigned legacy tasks to be repaired with the existing revision fence.
  This resolves the S5 case where an accepted addition could never be dispatched.

## [0.11.0-rc.9] - 2026-09-12

### Fixed

- Native wait CLI commands now supply the required artifact byte limit to the
  local admin client, allowing requests to reach the coordinator. A live S5
  registration exposed the failure; a real admin-transport regression covers it.

## [0.11.0-rc.2] - 2026-09-12

### Fixed

- Worker protocol replay now uses bounded worker-local SQLite rows instead of
  decoding and rewriting the entire JSON history for every exchange. Signed
  replay and pending-request recovery are retained, with byte and age limits.
- The first worker exchange migrates the old JSON transactionally and retains
  it as evidence. After migration, a JSON-only binary cannot safely resume from
  that stale file; see [the replay ADR](docs/architecture/adr-s0-replay-store.md).

### Added

- Citadel S0 architecture review and ADRs for sink tasks, node waits, graph
  amendments, worker enrollment and the quota rework. These are future-stage
  designs; only the replay repair is implemented here. DAG amendment mechanism
  remains a user decision.

## [0.11.0-rc.1] - 2026-09-12

First packaged build of the backlog-v2 runtime that normandy has been running
from source. Published as a prerelease so the fleet's daily `t3-update` check
converges every host on the same binary.

### Added

- The backlog-v2 coordinator and worker runtime (`backlog_v2` configuration),
  the revision-fenced admin commands under `t3-steward backlog`, and the
  reliability fixes that let both sides tolerate single bad records and
  restarts.

- A backlog-v2 system model covering state, ownership, boundaries, contracts,
  primitives, first-class concepts, seams, invariants, transactions, failure,
  evidence, and the production-binding sequence.
- Backlog-v2 release-candidate operations, manifest, recovery, coherent backup,
  rollback, migration point-of-no-return, and deployment-order documentation.
- A checked version 2 example workflow bundle and deployment-readiness report.

### Changed

- Clarify that the fleet coordinator remains a deployment NO-GO until production
  configuration and coordinator/worker transport bindings are implemented.
- Rollback guidance now preserves coordinator database and artifact consistency
  instead of treating state deletion as a safe recovery path.

### Fixed

- The binary builds again for darwin: peer-credential authentication of the
  local admin socket is Linux-only, and other platforms refuse that transport
  instead of failing to compile.

### Removed

- Windows builds. The backlog runtime relies on `flock` and unix `stat`, and
  no host in the fleet runs the steward there.
- A preparation command that hits its timeout no longer keeps the attempt
  waiting until its orphaned children exit.

## [0.10.1] - 2026-09-08

### Fixed

- Archive only takes threads that are settled or archived in T3; an idle
  thread still on the active shelf, or pinned active, is left alone.

## [0.10.0] - 2026-09-08

### Added

- `archive`: daily cold storage of threads idle for `archive.after`: full
  T3 export, provider logs and transcript bundled to a directory or an
  SSH destination, verified by checksum, then removed locally and deleted
  from T3. Transcripts stay on disk for `keep_transcripts`.

## [0.9.3] - 2026-09-08

### Changed

- `policy.windows` relabels and rescopes provider windows. By default
  Claude's `seven_day_overage_included` is treated as the 7-day Fable
  limit (label "Claude 7-day (Fable)", model selector `fable`) and acts
  on Fable threads; it is no longer ignored.
- `policy.ignore_windows` entries are globs, not substrings.

## [0.9.2] - 2026-09-08

### Changed

- The projection ladder only applies at or above `warn_percent`; below it
  the burn rate never warns, drains or stops. A bucket left in the warned
  phase drops back to normal when usage is below the threshold.

## [0.9.0] - 2026-09-08

### Added

- `wait`: an agent registers a check and ends its turn; the steward polls
  with exponential backoff (30s to 10m) and wakes the thread with the
  outcome when the check succeeds, gives up or times out. Groups can wake
  once when all their waits settle. The check is verified at registration.

### Changed

- Projection-based escalation needs the projection on two consecutive
  readings, so a single burst does not warn.
- The systemd unit no longer sets PrivateTmp: wait checks must see the
  agents' /tmp.

## [0.8.4] - 2026-09-08

### Fixed

- Resume after a reset nobody observed: one stopped thread per provider is
  resumed as a probe once the reset time has passed by
  `resume.probe_after_reset`, since readings only come from running turns.
- A bucket whose window has passed no longer blocks resumes of other
  threads.

## [0.8.3] - 2026-09-08

### Fixed

- Threads that start while a bucket is already warned or draining now
  receive the warn or drain message on the next poll instead of nothing
  until the stop.

## [0.8.1] - 2026-09-08

### Changed

- Runway rule: with a known burn rate, no action fires while the projected
  time to exhaustion covers `runway_margin` times the time to the reset,
  whatever the percentage. The fixed reset exemption remains for readings
  too sparse to give a rate.

## [0.8.0] - 2026-09-08

### Changed

- Renamed to t3-steward: module path, binary, config and state directories
  (`~/.config/t3-steward`, `~/.local/state/t3-steward`), systemd unit and
  the `T3_STEWARD_` environment prefix. The old directories are adopted
  automatically on first run.

## [0.7.0] - 2026-09-08

### Added

- Projected-exhaustion ladder: the burn rate over the last `rate_window`
  of readings gives a time to 100%, and warn, drain and stop fire when it
  drops below `warn_eta`, `drain_eta`, `stop_eta`, unless the window resets
  first.
- `reset_exemption`: no action when the window resets within it.

## [0.6.0] - 2026-09-08

### Added

- `backlog check`: validate a task's project, provider instance, model and
  options against the host that will run it; the runner applies the same
  check before dispatching.

## [0.5.0] - 2026-09-08

### Added

- Backlog tasks may name the machine that runs them (`host:`), with
  `backlog.default_host` for tasks that name none; tasks for another host
  are forwarded into its backlog over SSH (`backlog receive`), and
  `backlog list --all` shows every host's queue.

## [0.4.0] - 2026-09-08

### Added

- `forecast`: interactive-demand map by weekday and hour learned from
  history, and the headroom available for unattended work right now.
- Backlog runner: markdown tasks run as T3 threads when no interactive
  session has run for a while and the forecast leaves room before the next
  reset; ordered by deadline, importance and cost; multi-turn with a
  `BACKLOG STATUS` protocol; `backlog` command to manage them.
- Threads the watchdog dispatches are registered so they never count as
  interactive use.

## [0.3.0] - 2026-09-08

### Changed

- Attribution splits each rise across the calls of every thread active in
  the interval, weighted by a per-token cost fitted from the data;
  subagent usage (Claude per-turn totals) is spread over the turn so it
  is no longer "outside T3".

### Added

- `export` command and `report.remotes` / `--remotes`: merge readings and
  token samples from other machines that share the provider account.
- Per-call Claude samples from `message_delta` events; `kind` and
  `cumulative_tokens` columns (migrated automatically).

## [0.2.0] - 2026-09-08

### Added

- `report` command: quota consumption by peak/off-peak schedule, hour of
  day, model and thread, normalized per active hour and per million fresh
  tokens, with `--from-logs` backfill from the provider logs.
- The daemon records every accepted quota reading and token usage sample
  (`policy.history_retention`, default 90 days).

### Fixed

- `install-service` no longer claims dry-run is on when it is off.
- Rotation-by-rename test skipped on Windows, where it cannot pass.

## [0.1.0] - 2026-09-08

First prerelease.

### Added

- Quota ingestion from T3's provider event logs for Codex (`primary`,
  `secondary`, spending) and Claude (`five_hour`, `seven_day`, model-specific
  weekly windows), with rotation-safe tailing and persisted read positions.
- Policy state machine per bucket and reset epoch: warn at 85%, drain at 90%
  with a grace period, stop at 95%, rearm on a confirmed reset.
- T3 control over the orchestration HTTP API: warn and drain messages,
  interrupt or session stop with verification and escalation, opt-in
  automatic resume with per-provider staggering and cancellation on manual
  interaction.
- Commands: `init`, `check`, `run`, `status`, `replay`, `install-service`,
  `uninstall-service`, `version`.
- Linux systemd user service installer. macOS and Windows binaries run in the
  foreground.
- SQLite audit log and state, dry-run by default, no telemetry.
