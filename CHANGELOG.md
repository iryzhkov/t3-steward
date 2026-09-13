# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

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
