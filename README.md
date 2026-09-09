# t3-steward

A steward for [T3 Code](https://github.com/pingdotgg/t3code) sessions and
their Codex and Claude quotas. It watches every quota window the providers
report, warns running agent sessions as usage or the burn rate approaches a
limit, asks them to wind down their subagents and checkpoint, interrupts
them cleanly before the quota is exhausted, and (optionally) resumes the
sessions it stopped once the quota has recovered. It also reports where the
quota went, learns when you work, and runs a backlog of unattended tasks
in the quiet hours on whichever machine should do them.

Formerly `t3-quota-watchdog`; the binary migrates that name's config and
state directories on first run.

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
| 0.1.x to 0.9.x | 0.0.38 |

`t3-steward version` prints the range the binary was built with.
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
BASE=https://github.com/iryzhkov/t3-steward/releases/download/v${VERSION}

curl -fsSLO "${BASE}/t3-steward_${VERSION}_${TARGET}.tar.gz"
curl -fsSLO "${BASE}/checksums.txt"
sha256sum -c --ignore-missing checksums.txt

tar -xzf "t3-steward_${VERSION}_${TARGET}.tar.gz"
install -d "$HOME/.local/bin"
install -m 0755 t3-steward "$HOME/.local/bin/t3-steward"
```

On macOS use `shasum -a 256 -c --ignore-missing checksums.txt`. On Windows
the archive is a `.zip`; verify with `Get-FileHash` and compare against
`checksums.txt`.

### Option B: build from source

```sh
git clone https://github.com/iryzhkov/t3-steward
cd t3-steward
git checkout v0.1.0          # or a newer tag
make build                   # writes bin/t3-steward
make install                 # copies it to ~/.local/bin
```

Plain `go build ./cmd/t3-steward` works too. No CGO is needed.

### Option C: go install

```sh
go install github.com/iryzhkov/t3-steward/cmd/t3-steward@latest
```

The binary lands in `$(go env GOPATH)/bin`.

## First run

```sh
t3-steward init      # writes a commented config, dry-run on, resume off
t3-steward check     # server, version, token, logs, thread list
t3-steward run       # foreground, dry-run: logs what it would do
```

`init` discovers the T3 data directory (`$T3CODE_HOME` or `~/.t3`) and the
server URL from `userdata/server-runtime.json`. If discovery fails, pass
`--t3-url http://127.0.0.1:7391` or edit `t3.url` in the configuration.

Authentication is automatic: the watchdog runs
`t3 auth session issue --token-only --ttl 60m` to mint a short-lived bearer
token from T3's own auth store and refreshes it before expiry. Nothing
long-lived is written to disk. For a server on another machine set
`t3.token` to a token you minted there.

Leave it in dry-run for a while. `t3-steward status` shows the
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
Config:  ${XDG_CONFIG_HOME:-~/.config}/t3-steward/config.yaml
State:   ${XDG_STATE_HOME:-~/.local/state}/t3-steward/state.db
```

Both are created with user-only permissions.

## Linux: run it as a systemd user service

```sh
t3-steward install-service          # writes ~/.config/systemd/user/t3-steward.service
systemctl --user daemon-reload
systemctl --user enable --now t3-steward.service
loginctl enable-linger "$USER"             # keep it running while logged out
```

Then:

```sh
systemctl --user status t3-steward.service
journalctl --user -u t3-steward.service -f      # logs
systemctl --user restart t3-steward.service     # after editing the config
systemctl --user disable --now t3-steward.service
t3-steward uninstall-service                    # removes the unit, keeps config and state
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
t3-steward init && t3-steward check
t3-steward run
```

On Windows the log tailer detects rotation only by truncation, so the tail
of a rotated file can be missed; treat Windows as experimental.

## Configuration

The full commented file that `init` writes is
[config.example.yaml](config.example.yaml). Precedence, highest first:
command-line flags, `T3_STEWARD_*` environment variables (for example
`T3_STEWARD_DRY_RUN=false`, `T3_STEWARD_T3_URL`), the
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
- **Projected exhaustion** works alongside the percentages, once the
  projection has held on two consecutive readings. The burn rate
  over the last ten minutes of readings gives a time to 100%; the same
  warn, drain and stop actions fire when that falls below 30, 15 and 5
  minutes (`policy.warn_eta` etc.), but only if the window does not reset
  first. A session burning 2.3% a minute at 63% gets the drain request at
  about 16 minutes to exhaustion instead of waiting for 90%.
- **Runway**: with a known burn rate, nothing fires while the projected
  time to 100% covers `policy.runway_margin` (1.5) times the time to the
  reset, whatever the percentage: at 96% burning 0.05%/min with the reset
  30 minutes away, the session is left alone. A burst too short to give a
  rate falls back to the percentage ladder.
- **Reset exemption**: when the window resets within `policy.reset_exemption`
  (10 minutes), nothing fires, not even at 96%, and an expired grace
  timer does not stop. Stopping then would save nothing.
- **95% (stop)** or grace timer expired: `thread.turn.interrupt` for every
  affected running thread, verification that it left the running state,
  bounded retries, escalation to `thread.session.stop`, and a desktop
  notification if a thread would not stop.
- A jump straight from 60% to 97% performs only the highest action.
- Each notice is sent once per thread per reset window. A daemon restart
  never repeats one.
- Threads that start running after a threshold was crossed are not left
  out: while the bucket is warned or draining they get the current notice
  on the next poll, and while it is stopped they are stopped too
  (`policy.stop_new_sessions`). Each notice goes once per thread and
  window.
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

Readings only come from running turns, so a reset that nobody is around to
observe would never be confirmed. When the reset time has passed by
`resume.probe_after_reset` (5 minutes) with no reading, one stopped thread
per provider is resumed as a probe: its first call yields the reading that
rearms the bucket and releases the others, or gets it stopped again at
once if the window has not actually reset.

## Consumption report

`report` shows where the quota went: peak versus off-peak hours, hour of day
on weekdays and weekends, model and thread, plus the latest window. It reads
the observations the daemon stores and, with `--from-logs`, the full
provider logs (rotated files included), so it works on day one:

```sh
t3-steward report --from-logs --days 14 --peak "Mon-Fri 09:00-17:00"
t3-steward report --bucket five_hour --json
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
readings of one reset window. Each rise is split across the API calls made
during that interval, by every thread, in proportion to their estimated
quota cost, so concurrent threads share a rise instead of the last reporter
taking all of it. Rises with no calls at all are reported as "outside T3 or
no token data" (the phone, a bare CLI, another machine).

The cost per token type is fitted from your own data: a non-negative least
squares fit of hourly rises against hourly token counts (input, cache
write, cache read, output). The report prints the fitted weights and the
R²; when the data is too thin it falls back to list-price ratios and says
so. Claude reports per-call usage only for the parent agent and per-turn
totals per model (subagents included); the part of a turn that the parent's
calls do not explain is spread over the turn's duration, which is what
keeps subagent-heavy sessions attributable. Codex reports per-call counts
for the thread's model.

`actual/est` compares consumption with the fitted cost of the tokens in
each band; a value well above 1 in one band means that band costs more per
token than the fit expects.

### Several machines, one account

When the same provider account is used from several machines, each
machine sees only its own threads, and a rise caused elsewhere shows up as
"outside T3". List the other hosts and the report merges their data over
SSH (each host runs `t3-steward export`):

```yaml
report:
  peak: "Mon-Fri 09:00-17:00"
  remotes: [gaming-pc, normandy, homelab]
```

```sh
t3-steward report --from-logs            # merges configured remotes
t3-steward report --remotes a,b --local  # override, or local only
```

`--import` stores scanned log data in the state database, which keeps
history for `policy.history_retention` (90 days by default) after the logs
themselves rotate away.

## Forecast and backlog: run queued work when you are not using the quota

The watchdog learns from history when *you* use the quota and can run a
backlog of unattended tasks in the gaps.

### Forecast

```sh
t3-steward forecast --from-logs
```

prints, per weekday and hour, how much of the window interactive threads
consumed (80th percentile over past weeks, so a heavy week is covered), plus
the headroom right now: usage, forecast demand until the next reset, and
what is left for backlog work after the safety margin. Threads the watchdog
dispatched itself are excluded from "interactive", so backlog and scheduled
runs do not teach it that you work at 3 a.m. While history is thin a slot
borrows from the same hour on similar days; `?` marks slots with too few
occurrences, `.` slots never observed.

### Backlog

Enable the runner and drop markdown tasks into the backlog directory:

```yaml
backlog:
  enabled: true
  quiet_for: 30m              # no interactive thread for this long
  safety_margin_percent: 10   # always left unused
  long_window_cap_percent: 80 # weekly windows are never pushed past this
```

```sh
t3-steward backlog new refactor-auth     # writes <config>/backlog/refactor-auth.md
t3-steward backlog list
```

```markdown
---
project: laptop home     # T3 project: the workspace the agent works in
importance: 4            # 1-5, higher runs first
difficulty: 3            # 1-5, seeds the cost (5/10/20/35/50% of the window) and duration
model: claude-opus-5     # optional with instance; else the project's default model
instance: claudeAgent
not_before: 2026-09-09T00:00:00-07:00   # optional
deadline: 2026-09-12T00:00:00-07:00     # optional; within 24 h the gate is bypassed
max_turns: 3
gate: true               # false: run at not_before whenever quota is healthy
host: normandy           # optional: run on that machine's T3 (see below)
---
The prompt, written for an agent that gets no input from you.
```

A task starts when all of these hold:

- no interactive thread has run for `quiet_for`;
- every bucket of the task's provider is healthy;
- for windows that reset within a day: `usage + task cost landing before the
  reset + forecast interactive demand until the reset ≤ 100 − safety margin`;
- for longer windows: `usage + task cost ≤ long_window_cap_percent`;
- no other backlog task is running on that provider.

Order: tasks with a deadline inside 24 hours first, then importance, then
the cheaper estimate. The estimate is seeded by difficulty and replaced by
the measured consumption after the first turn, so it converges per task.

Each task runs as a new T3 thread in the project, `full-access` runtime
mode, with a preamble that tells the agent to work without asking, do
everything that does not depend on a decision, leave a handoff, and end
with one line: `BACKLOG STATUS: done`, `continue`, or `needs-input`.
`continue` re-dispatches the same thread up to `max_turns`; `needs-input`,
or the agent asking a question through T3, parks the task with the thread
link so you can answer it in the app. Edit the file to re-queue a finished
task, or use `backlog retry`; `backlog cancel` stops a pending one. A
running task is an ordinary thread to the watchdog: the warn, drain and
stop ladder applies, and a task interrupted for quota resumes with the
others.

### Checking a task

`t3-steward backlog check <file>` validates a task against the host
that would run it (over SSH when the task names another host): the project
exists, the provider instance is enabled and signed in, the model is one it
offers, the options are ones the model knows. It reads T3's provider caches
(`<data_dir>/caches/<instance>.json`), so it works for every provider T3
knows, Codex, Claude and OpenCode alike. The runner runs the same check
before a dispatch and parks an invalid task as `failed: invalid: ...`.

### Running a task on another machine

A task may name the machine whose T3 server should run it (`host:`, an SSH
alias), and `backlog.default_host` sets the host for tasks that name none.
A task for another host is forwarded: the local runner copies the file into
that host's backlog directory over SSH (`t3-steward backlog receive`
on the remote side, so the binary must be on the login shell's PATH there)
and marks its own copy `forwarded`. The remote runner owns it from then on,
with its projects, its quota view and its quiet-hours gate.
`backlog list --all` shows every host's queue (the hosts in
`report.remotes`). `local` and `localhost` always mean this machine;
`backlog.host_name` sets what tasks call it (default: the OS host name).

The gate cannot see the phone or a bare CLI session start; it sees them
as rises without T3 tokens after the fact. A backlog task may therefore
occasionally start just before you do, and the ladder drains it at 90%
like anything else.

## Waiting for something external

An agent that would otherwise poll in a loop (PR review, CI, a long job)
registers the check with the steward and ends its turn:

```sh
t3-steward wait add --name "PR 123 reviewed" -- gh pr view 123 --json reviewDecision --jq 'select(.reviewDecision != "") | .reviewDecision'
```

The thread is resolved from the calling agent's `CLAUDE_CODE_SESSION_ID`
(or `--thread`). The steward runs the check every 30 seconds, doubling the
interval after every "not yet" up to 10 minutes (`--every`, `--max-every`),
for up to `--timeout` (24 h). Exit 0 means the condition is met, exit 2
means give up, anything else means keep polling. When the wait settles the
steward starts the thread's next turn with the outcome and the check's
last output; if the provider quota is unhealthy at that moment the wake
waits for it. `--group NAME --wake all` wakes once when every wait in the
group has settled. The check is run once at registration and refused if it
cannot run, already succeeds, or gives up. Parked threads cost nothing.

## Commands

```text
t3-steward init [--force] [--t3-url URL] [--t3-data-dir DIR]
t3-steward check
t3-steward run [--dry-run | --no-dry-run] [--log-level debug]
t3-steward status [--json] [--all] [--limit N]
t3-steward replay FILE [--resume] [--speed 0.1]
t3-steward report [--days 14] [--peak "Mon-Fri 09:00-17:00"] [--bucket TEXT] [--from-logs] [--import] [--remotes a,b] [--local] [--json]
t3-steward export [--days 14] [--from-logs]
t3-steward forecast [--days 56] [--bucket TEXT] [--from-logs] [--remotes a,b] [--json]
t3-steward backlog list [--all]|new ID|check FILE|show ID|retry ID|cancel ID|receive ID|path
t3-steward wait add [--name TEXT] [--every 30s] [--max-every 10m] [--timeout 24h] [--thread ID] [--group G --wake all] -- CMD...
t3-steward wait list [--all]|cancel ID|run-now ID
t3-steward install-service [--force] [--enable]
t3-steward uninstall-service
t3-steward version
```

`replay` feeds a copied provider log (or a JSONL of canonical events)
through the policy engine against a fake T3 with one running thread per
provider, printing every message and action. It needs no server:

```sh
t3-steward replay testdata/codex-rate-limits.log --resume
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
- The systemd unit runs unprivileged with `NoNewPrivileges` and
  `ProtectSystem=full`. `ProtectHome` and `PrivateTmp` are deliberately not
  set: the `t3` CLI writes T3's own database under `$HOME`, and wait checks
  written by agents must see the same `/tmp` the agents use.
- Report vulnerabilities as described in [SECURITY.md](SECURITY.md).

## Upgrade and rollback

1. Download or build the new version and replace the binary at the same
   path (`~/.local/bin/t3-steward`).
2. `t3-steward check`, then `systemctl --user restart
   t3-steward.service` (or restart your foreground process).
3. The state database migrates forward automatically.

To roll back, put the previous binary back and restart. State written by a
newer version is readable by older ones within the same minor series; if
in doubt, stop the service, delete
`~/.local/state/t3-steward/state.db`, and start again. You lose the
audit log and pending resume intents, nothing else.

After a T3 upgrade, run `check`: it reports whether the new server version
is in the tested range.

## Project

- Issues: <https://github.com/iryzhkov/t3-steward/issues>
- Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)
- Security: [SECURITY.md](SECURITY.md)
- Protocol notes: [docs/t3-protocol.md](docs/t3-protocol.md)
- License: [MIT](LICENSE)
