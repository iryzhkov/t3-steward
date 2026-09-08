# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

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
