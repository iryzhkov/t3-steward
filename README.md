# t3-quota-watchdog

A small daemon that watches the provider quota windows reported to
[T3 Code](https://github.com/pingdotgg/t3code) by Codex and Claude, warns the
running agent sessions as usage approaches a limit, asks them to wind down
their subagents and checkpoint, interrupts them cleanly before the quota is
exhausted, and (optionally) resumes the sessions it stopped once the quota
has recovered.

It exists because a coordinator session that hits a hard quota cut-off
mid-task loses its in-flight subagent work. Stopping ten minutes earlier at
a checkpoint is cheaper.

> **Compatibility warning.** T3's control protocol and log formats are
> internal and undocumented. This tool is written against specific T3
> versions (see the table below) and refuses to warn, stop or resume when
> the server version is outside that range unless you override it. Expect
> to update the watchdog when you update T3.

## Designed for one account per provider

The watchdog works well when the T3 server uses **one Codex account** and
**one Claude account**: the setup of a single developer machine, or one T3
server per person. That is the case it was built and tested for.

The reason is in the data. Claude's rate-limit events carry no account
identifier at all, and Codex's carry only a `limitId`, so every quota window
is attributed to the *T3 provider instance* that reported it. With one
account behind each provider instance that attribution is exact: the
`five_hour` bucket of `claudeAgent` is your Claude five-hour window, full
stop.

If a T3 server drives several accounts of the same provider, configure them
as separate T3 provider instances. Each instance then gets its own buckets
and the watchdog tracks them independently. Several instances that share
**one** account are not supported: their events would be tracked as separate
buckets although they draw on one quota.

## What it can and cannot do

Can:

- Track every window each provider reports, independently: Codex `primary`
  (5 hours), `secondary` (7 days) and spend controls; Claude `five_hour`,
  `seven_day` and model-specific weekly windows such as `seven_day_opus`.
- Send a warning message into every running thread of the affected provider
  at 85%, a "stop spawning subagents, checkpoint, then stop" message at 90%,
  and interrupt the threads at 95% or when the 90% grace period expires.
- Stop threads that start while a bucket is already in the stopped phase.
- Resume the threads it stopped once the provider confirms the reset, one
  per provider at a time, with staggering, and never a thread that a human
  touched in between.
- Survive its own restarts, T3 restarts and provider-log rotation without
  repeating a warning or a stop.
- Run entirely locally: no telemetry, no network access beyond the local T3
  server.

Cannot:

- Stop one subagent. T3 exposes no per-subagent command; subagents live
  inside a thread. The drain message asks the coordinating agent to wind
  them down; the hard stop interrupts the whole thread, which ends them.
- Predict usage. It acts only on percentages the provider reported.
- See a quota that the provider did not report yet. Claude and Codex emit
  rate-limit events during turns; a completely idle server produces none.
- Distinguish accounts (see above), or control anything but T3.
- Modify T3, its database or its files. It reads logs and calls the API.

## Supported platforms, T3 versions and providers

| | |
| --- | --- |
| Operating systems | Linux (systemd user service, fully supported); macOS and Windows (foreground process, binaries provided, installer not yet) |
| Architectures | linux/amd64, linux/arm64, darwin/amd64, darwin/arm64, windows/amd64 (experimental) |
| Providers | Codex (`codex` app-server), Claude (`claudeAgent`, Claude Agent SDK) |
| Quota sources | T3 provider event logs (`userdata/logs/provider/events.*.log`) |

| Watchdog | Tested T3 Code versions |
| --- | --- |
| 0.1.x | 0.0.38 |

`t3-quota-watchdog version` prints the range the binary was built with.
Newer T3 versions run in monitoring-only mode until either a release adds
them to the table or you set `t3.allow_unsupported_version: true`.

## Prerequisites

- A T3 Code server running on the same machine as the watchdog (it reads the
  server's log files), started by a user with a working `t3` CLI on `PATH`
  (Node.js included, since `t3` is a Node script).
- At least one provider CLI authenticated: `codex login` and/or `claude`
  signed in. The watchdog only sees quotas the providers report.
- Go 1.24+ only when building from source.

## Install

### Option A: download a release

Set the version and target, download the archive and the checksum file,
verify, and put the binary on your `PATH`:

```sh
VERSION=0.1.0
TARGET=linux_arm64          # linux_amd64, darwin_arm64, darwin_amd64, windows_amd64
BASE=https://github.com/iryzhkov/t3-quota-watchdog/releases/download/v${VERSION}

curl -fsSLO "${BASE}/t3-quota-watchdog_${VERSION}_${TARGET}.tar.gz"
curl -fsSLO "${BASE}/checksums.txt"
sha256sum -c --ignore-missing checksums.txt

tar -xzf "t3-quota-watchdog_${VERSION}_${TARGET}.tar.gz"
install -d "$HOME/.local/bin"
install -m 0755 t3-quota-watchdog "$HOME/.local/bin/t3-quota-watchdog"
```

On macOS use `shasum -a 256 -c --ignore-missing checksums.txt`. On Windows
the archive is a `.zip`; verify with `Get-FileHash` and compare against
`checksums.txt`.

### Option B: build from source

```sh
git clone https://github.com/iryzhkov/t3-quota-watchdog
cd t3-quota-watchdog
git checkout v0.1.0          # or a newer tag
make build                   # writes bin/t3-quota-watchdog
make install                 # copies it to ~/.local/bin
```

Plain `go build ./cmd/t3-quota-watchdog` works too. No CGO is needed.

### Option C: go install

```sh
go install github.com/iryzhkov/t3-quota-watchdog/cmd/t3-quota-watchdog@latest
```

The binary lands in `$(go env GOPATH)/bin`.

## First run

```sh
t3-quota-watchdog init      # writes a commented config, dry-run on, resume off
t3-quota-watchdog check     # server, version, token, logs, thread list
t3-quota-watchdog run       # foreground, dry-run: logs what it would do
```

`init` discovers the T3 data directory (`$T3CODE_HOME` or `~/.t3`) and the
server URL from `userdata/server-runtime.json`. If discovery fails, pass
`--t3-url http://127.0.0.1:7391` or edit `t3.url` in the configuration.

Authentication is automatic: the watchdog runs
`t3 auth session issue --token-only --ttl 60m` to mint a short-lived bearer
token from T3's own auth store and refreshes it before expiry. Nothing
long-lived is written to disk. For a server on another machine set
`t3.token` to a token you minted there.

Leave it in dry-run for a while. `t3-quota-watchdog status` shows the
buckets it tracks and every action it would have taken:

```text
Buckets (5):
  claudeAgent/claude/five_hour       43%  normal   resets 2026-09-08 16:40 (in 4h28m)  observed 5m ago
  codex/codex/primary                75%  normal   resets 2026-09-08 13:22 (in 1h11m)  observed 47m ago
  ...
Recent actions (2):
  09-08 12:14  warn [dry-run] codex/codex/primary 946f04f0-... "codex primary window" at 86%
```

When the decisions look right, set `policy.dry_run: false` (and, if you
want it, `resume.enabled: true`) and restart.

Paths follow XDG conventions:

```text
Config:  ${XDG_CONFIG_HOME:-~/.config}/t3-quota-watchdog/config.yaml
State:   ${XDG_STATE_HOME:-~/.local/state}/t3-quota-watchdog/state.db
```

Both are created with user-only permissions.

## Linux: run it as a systemd user service

```sh
t3-quota-watchdog install-service          # writes ~/.config/systemd/user/t3-quota-watchdog.service
systemctl --user daemon-reload
systemctl --user enable --now t3-quota-watchdog.service
loginctl enable-linger "$USER"             # keep it running while logged out
```

Then:

```sh
systemctl --user status t3-quota-watchdog.service
journalctl --user -u t3-quota-watchdog.service -f      # logs
systemctl --user restart t3-quota-watchdog.service     # after editing the config
systemctl --user disable --now t3-quota-watchdog.service
t3-quota-watchdog uninstall-service                    # removes the unit, keeps config and state
```

The generated unit uses absolute paths, starts after `t3code.service` when
that user unit exists (and stops with it), restarts on failure, logs to
journald, and bakes in the `PATH` of the shell that ran `install-service`
so the `t3` CLI and Node resolve the same way. It never enables destructive
behaviour by itself: dry-run stays on until you change the configuration.
`install-service --force` regenerates a unit you edited; `--enable` also
enables and starts it.

## macOS and Windows: foreground operation

The daemon and CLI compile and run on both. There is no launchd or Windows
service installer yet, so run it in a terminal, a tmux session, or your own
process manager:

```sh
t3-quota-watchdog init && t3-quota-watchdog check
t3-quota-watchdog run
```

On Windows the log tailer detects rotation only by truncation, so the tail
of a rotated file can be missed; treat Windows as experimental.

## Configuration

The full commented file that `init` writes is
[config.example.yaml](config.example.yaml). Precedence, highest first:
command-line flags, `T3_QUOTA_WATCHDOG_*` environment variables (for example
`T3_QUOTA_WATCHDOG_DRY_RUN=false`, `T3_QUOTA_WATCHDOG_T3_URL`), the
configuration file, then discovery and defaults.

```yaml
t3:
  url: ""                    # discovered from server-runtime.json
  data_dir: ""               # $T3CODE_HOME or ~/.t3
  token: ""                  # leave empty: minted via `t3 auth session issue`
  token_ttl: 1h
  allow_unsupported_version: false

policy:
  warn_percent: 85
  drain_percent: 90
  stop_percent: 95
  grace_period: 60s          # hard stop this long after the drain message
  rearm_percent: 50
  dry_run: true
  stop_mode: interrupt       # or session-stop
  escalate_to_session_stop: true
  stop_new_sessions: true
  ignore_windows: ["overage"]

resume:
  enabled: false
  below_percent: 50
  reset_confirmation_required: true
  reset_settle_delay: 2m
  interval_between_threads: 45s
  max_concurrent_per_provider: 1

polling:
  snapshot_interval: 15s

overrides:
  - match: { provider: claudeAgent, window: "seven_day*" }
    stop_percent: 98
```

`warn < drain < stop <= 100` is enforced at startup, for the base policy and
for every override.

## How limits are modelled

Every quota is a **bucket** keyed by provider instance, limit id and window.
Buckets are independent: the Codex five-hour window can be exhausted while
the weekly window is fine, and a thread is affected by *every* bucket that
applies to it.

- **Account-wide limits** (Codex `primary`/`secondary`, Claude `five_hour`
  and `seven_day`) apply to every thread whose model selection uses that
  provider instance.
- **Rolling and weekly windows** are just buckets with different reset
  times. The watchdog uses the duration and reset time the provider
  reported; it never assumes "primary means five hours".
- **Model-specific limits** (Claude `seven_day_opus`, `seven_day_sonnet`)
  apply only to threads whose selected model id contains that name.
- **Spending limits** (Codex `individualLimit`, reported as remaining
  percent) become a `spending` bucket with `100 - remaining`.
- **Overage windows** (Claude `seven_day_overage_included`) are recorded but
  never act, because they are not a hard limit; adjust
  `policy.ignore_windows` if your plan differs.

## Warning, drain, stop, reset, resume

Per bucket and reset window the state machine is

```text
normal -> warned -> draining -> stopped -> (reset confirmed) -> normal
```

- **85% (warn)**: one message per affected running thread asking it not to
  start new subagents and to have active ones checkpoint.
- **90% (drain)**: one message per thread asking it to stop spawning,
  finish or cancel subagents, collect results, write a checkpoint and stop.
  A grace timer starts.
- **95% (stop)** or grace timer expired: `thread.turn.interrupt` for every
  affected running thread, verification that it left the running state,
  bounded retries, escalation to `thread.session.stop`, and a desktop
  notification if a thread would not stop.
- A jump straight from 60% to 97% performs only the highest action.
- Each notice is sent once per thread per reset window. A daemon restart
  never repeats one.
- Threads that start running while the bucket is stopped are stopped too
  (`policy.stop_new_sessions`).
- **Reset**: the bucket rearms when the provider's reset time has passed
  *and* a fresh snapshot shows usage below `rearm_percent`. Buckets without
  a reset time rearm after two consecutive low observations. The wall clock
  alone never rearms anything; usage merely decreasing does not either.

For Claude threads the warn and drain messages are steered into the running
turn. For Codex threads T3 forwards them as a new turn start; in testing the
Codex agent picked the message up mid-turn and stopped at a checkpoint on
its own.

### Automatic resume

Off by default. When enabled, **only threads that the watchdog stopped can
resume automatically**: a thread gets a resume intent when the watchdog
sent it the drain request or interrupted it. A thread resumes when

- every bucket that caused the stop has rearmed since the stop and is below
  `resume.below_percent`, and every other bucket that applies to the thread
  is healthy (normal phase, below the warn threshold);
- `reset_settle_delay` has passed since the reset was confirmed;
- nobody sent the thread a message, started a new turn, archived or deleted
  it since the stop, and it is not waiting on an approval or a question.

Resumes go out one per provider instance at a time, spaced by
`interval_between_threads`. If any applicable bucket reaches the warning
level again, the rest of the batch is cancelled. The resume prompt tells
the agent to inspect the thread and repository state, not to assume old
subagents are alive, and to continue only the unfinished work.

Intents are cancelled, not resumed, when a human interacted with the thread
after the stop, when the thread was archived or deleted, or after
`resume.max_intent_age`.

## Consumption report

`report` shows where the quota went: peak versus off-peak hours, hour of day
on weekdays and weekends, model and thread, plus the latest window. It reads
the observations the daemon stores and, with `--from-logs`, the full
provider logs (rotated files included), so it works on day one:

```sh
t3-quota-watchdog report --from-logs --days 14 --peak "Mon-Fri 09:00-17:00"
t3-quota-watchdog report --bucket five_hour --json
```

```text
== claudeAgent/claude/five_hour: 511% consumed over 873 observations
              consumed active-h %/active-h fresh-tokens   %/1M-fresh      %/USD
   peak           208%       19       10.9        59.4M         3.50       0.08
   off-peak       303%       57        5.3        67.1M         4.51       0.10
   Per fresh token, peak hours cost 0.78x off-peak hours.
   Per active hour, peak hours consume 2.06x off-peak hours.
```

Consumption is the rise of the reported percentage between consecutive
readings of one reset window, attributed to the thread and model whose turn
produced the reading. Totals are exact; the split between threads running
at the same time is approximate. Two normalizations separate "I use it more
at that time" from "it costs more at that time": consumption per active
hour, and consumption per million fresh tokens (input, cache writes and
output; cache reads are listed separately because their quota weight is
unknown). Claude reports per-model token counts and a cost figure per turn;
Codex reports per-call counts for the thread's model.

`--import` stores scanned log data in the state database, which keeps
history for `policy.history_retention` (90 days by default) after the logs
themselves rotate away.

## Commands

```text
t3-quota-watchdog init [--force] [--t3-url URL] [--t3-data-dir DIR]
t3-quota-watchdog check
t3-quota-watchdog run [--dry-run | --no-dry-run] [--log-level debug]
t3-quota-watchdog status [--json] [--all] [--limit N]
t3-quota-watchdog replay FILE [--resume] [--speed 0.1]
t3-quota-watchdog report [--days 14] [--peak "Mon-Fri 09:00-17:00"] [--bucket TEXT] [--from-logs] [--import] [--json]
t3-quota-watchdog install-service [--force] [--enable]
t3-quota-watchdog uninstall-service
t3-quota-watchdog version
```

`replay` feeds a copied provider log (or a JSONL of canonical events)
through the policy engine against a fake T3 with one running thread per
provider, printing every message and action. It needs no server:

```sh
t3-quota-watchdog replay testdata/codex-rate-limits.log --resume
```

## Troubleshooting

**`no T3 server URL configured and .../server-runtime.json does not exist`**
The server is not running, or it uses another data directory. Start it, or
set `t3.data_dir` / `t3.url`.

**`token command ... failed`** The `t3` CLI is missing from `PATH` or
cannot find its database. Set `t3.t3_binary` to the full path. When T3 runs
with `--base-dir`, set `t3.data_dir` to the same directory: the watchdog
passes it to `t3 auth session issue --base-dir`. Inside the systemd unit the
`PATH` is the one captured at `install-service` time; rerun it with
`--force` after moving Node.

**`T3 API 401`** The token was revoked or the auth store was reset (a T3
upgrade that migrates auth does this). The watchdog re-mints once; if that
fails, check `t3 auth session list`.

**`control actions disabled, monitoring only`** The T3 version is outside
the tested range. Read `docs/t3-protocol.md`, run the verification
checklist against a disposable thread, then set
`t3.allow_unsupported_version: true` or upgrade the watchdog.

**Stale snapshots.** `status` shows when each bucket was last observed.
Providers only report during turns; a bucket observed hours ago is normal
on an idle server. Snapshots older than `policy.max_snapshot_age` are
ignored at startup, and a snapshot whose reset time has already passed
never triggers an action.

**Log permissions.** The watchdog must run as the user who runs T3, or as a
user who can read `userdata/logs/provider/`. It never needs root.

**Nothing happens at 90%.** Check that the thread's `modelSelection.instanceId`
matches the bucket's provider instance (`status --json` shows both), that
the window is not in `ignore_windows`, and that dry-run is off.

## Privacy and security

- All processing is local. The only network peer is the T3 server you
  configure, normally on loopback. No telemetry, no update checks.
- Tokens are minted on demand and held in memory. Logs never contain
  tokens, message bodies, account identifiers or full provider events.
- The state database records bucket percentages, thread ids and titles,
  and the audit log of actions. It is created with mode 0600.
- The systemd unit runs unprivileged with `NoNewPrivileges`, `PrivateTmp`
  and `ProtectSystem=full`. `ProtectHome` is deliberately not set because
  the `t3` CLI writes T3's own database under `$HOME`.
- Report vulnerabilities as described in [SECURITY.md](SECURITY.md).

## Upgrade and rollback

1. Download or build the new version and replace the binary at the same
   path (`~/.local/bin/t3-quota-watchdog`).
2. `t3-quota-watchdog check`, then `systemctl --user restart
   t3-quota-watchdog.service` (or restart your foreground process).
3. The state database migrates forward automatically.

To roll back, put the previous binary back and restart. State written by a
newer version is readable by older ones within the same minor series; if
in doubt, stop the service, delete
`~/.local/state/t3-quota-watchdog/state.db`, and start again. You lose the
audit log and pending resume intents, nothing else.

After a T3 upgrade, run `check`: it reports whether the new server version
is in the tested range.

## Project

- Issues: <https://github.com/iryzhkov/t3-quota-watchdog/issues>
- Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)
- Security: [SECURITY.md](SECURITY.md)
- Protocol notes: [docs/t3-protocol.md](docs/t3-protocol.md)
- License: [MIT](LICENSE)
