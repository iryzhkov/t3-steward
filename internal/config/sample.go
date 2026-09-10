package config

// sampleConfig is written by `init` and shipped as config.example.yaml.
// Keep it in sync with Default().
const sampleConfig = `# t3-steward configuration.
#
# Every value here is the shipped default. Delete what you do not change.
# Environment variables prefixed with T3_STEWARD_ override this file,
# command-line flags override both.

t3:
  # Base URL of the local T3 server. Leave empty to discover it from
  # <data_dir>/userdata/server-runtime.json, which the server writes on start.
  url: ""
  # T3 base directory (the parent of "userdata"). Leave empty to use
  # $T3CODE_HOME or ~/.t3.
  data_dir: ""
  # Bearer token. Prefer leaving this empty: the watchdog then mints a
  # short-lived token by running "t3 auth session issue --token-only" and
  # refreshes it before it expires. Nothing long-lived is stored on disk.
  token: ""
  # Alternative token source: an argv (no shell) that prints a token.
  # token_command: ["/usr/local/bin/my-token-helper"]
  # Lifetime requested for minted tokens and the refresh interval.
  token_ttl: 1h
  # Explicit path to the t3 CLI when it is not on PATH.
  t3_binary: ""
  # T3's control protocol is internal. Control actions are refused when the
  # server version is outside the tested range unless this is true.
  allow_unsupported_version: false
  request_timeout: 30s

policy:
  # Usage thresholds in percent. warn < drain < stop <= 100 is enforced.
  warn_percent: 85    # one warning per affected running thread
  drain_percent: 90   # ask threads to stop subagents and checkpoint
  stop_percent: 95    # interrupt affected threads
  # After the drain message, interrupt even without a newer quota event.
  grace_period: 60s
  # Usage below which a bucket may rearm (see README, "Reset and rearm").
  rearm_percent: 50
  rearm_observations: 2
  # Dry run: log every decision, send nothing, interrupt nothing.
  # Set to false only after "t3-steward check" and a dry-run soak.
  dry_run: true
  # "interrupt" ends the running turn (the thread keeps its session).
  # "session-stop" ends the provider session as well.
  stop_mode: interrupt
  # Retry a stop that did not take effect with a session stop.
  escalate_to_session_stop: true
  stop_verify_timeout: 20s
  stop_retries: 3
  # Also stop threads that start running while an applicable bucket is
  # already in the stopped phase.
  stop_new_sessions: true
  # Window names (globs) that are recorded but never act. Claude's bare
  # "overage" window is not a hard limit.
  ignore_windows: ["overage"]
  # Relabel or rescope provider windows. Claude reports the Fable weekly
  # limit under the key below; scoping it to "fable" makes it act only on
  # threads whose model id contains fable.
  windows:
    seven_day_overage_included:
      label: "Claude 7-day (Fable)"
      model: fable
  # Observations older than this are ignored at startup.
  max_snapshot_age: 12h
  # Projected exhaustion. The burn rate over the last rate_window of
  # readings gives a time to 100%; warn, drain or stop when it falls below
  # these, unless the window resets first. reset_exemption suppresses every
  # action when the reset is that close, because stopping saves nothing.
  rate_window: 10m
  warn_eta: 30m
  drain_eta: 15m
  stop_eta: 5m
  reset_exemption: 10m
  # With a known burn rate, nothing fires while the projected runway covers
  # this many times the time to the reset, whatever the percentage.
  runway_margin: 1.5
  # Quota readings and token samples kept for "t3-steward report".
  history_retention: 2160h

resume:
  # Automatic resume is opt-in and only ever touches threads that the
  # watchdog itself stopped.
  enabled: false
  # Every bucket that caused the stop must be below this after its reset.
  below_percent: 50
  # Never resume on the wall clock alone; wait for a fresh provider snapshot.
  reset_confirmation_required: true
  reset_settle_delay: 2m
  interval_between_threads: 45s
  max_concurrent_per_provider: 1
  coordinator_threads_only: true
  require_checkpoint: false
  # Intents older than this are cancelled.
  max_intent_age: 336h
  # Readings only come from running turns. When the reset time has passed
  # by this long with no reading, one stopped thread per provider is resumed
  # as a probe; its first call confirms the reset (or gets it stopped again).
  probe_after_reset: 5m
  # prompt: |
  #   The provider quota has recovered. Resume from the latest checkpoint...

polling:
  # How often the T3 thread list is read.
  snapshot_interval: 15s
  # Cap on the backoff between failed T3 requests.
  reconnect_max_delay: 30s
  # Fallback scan of the provider log directory when file notifications
  # are missed.
  log_scan_interval: 10s

# Per-bucket threshold overrides. Match fields are globs; the first match
# wins. Window names: Codex "primary"/"secondary"; Claude "five_hour",
# "seven_day", "seven_day_opus", "seven_day_sonnet", ...
overrides:
  # Weekly windows reset slowly and are shared by everything; let them run
  # closer to the edge than the five-hour windows.
  - match:
      provider: claudeAgent
      window: "seven_day*"
    warn_percent: 95
    drain_percent: 97
    stop_percent: 99
  - match:
      provider: codex
      window: secondary
    warn_percent: 95
    drain_percent: 97
    stop_percent: 99

# Message templates. Fields: .LimitName .UsedPercent .ResetsAt .GracePeriod
# .Window .Provider
messages:
  warn: |
    Provider quota warning from the T3 quota watchdog: "{{.LimitName}}" is at {{.UsedPercent}}% and resets at {{.ResetsAt}}. Do not start new subagents. Ask active subagents to checkpoint and return their results, consolidate the current work, then stop at a clean point.
  drain: |
    Provider quota is nearly exhausted (T3 quota watchdog): "{{.LimitName}}" is at {{.UsedPercent}}% and resets at {{.ResetsAt}}. Stop spawning subagents now. Cancel or finish active subagents, collect their results, write a short checkpoint of the current state and remaining work, then stop. The session will be interrupted in {{.GracePeriod}} if it is still running.

report:
  # Hours treated as "peak" by "t3-steward report" (local time).
  peak: "Mon-Fri 09:00-17:00"
  # SSH hosts that run the watchdog against the same provider accounts;
  # the report merges their readings and token samples with this host's.
  remotes: []

backlog:
  # Quota-gated task runner: markdown tasks in "dir" run as T3 threads
  # when no interactive session has run for quiet_for and the forecast
  # of your own usage leaves room before the next reset.
  # See "t3-steward backlog --help".
  enabled: false
  dir: ""                       # default: <config dir>/backlog
  quiet_for: 30m
  long_window_cap_percent: 80   # weekly windows are never pushed past this
  history_days: 56
  # Tasks name the host that runs them (an SSH alias); default_host runs
  # the ones that name none. Empty means this machine. host_name is what
  # tasks call this machine (default: the OS host name).
  default_host: ""
  host_name: ""
  # Forecast of your own (interactive) usage, used to decide how much of a
  # window backlog tasks may spend. See "t3-steward forecast".
  safety_margin_percent: 10     # always left unused
  fallback_per_hour_percent: 10 # assumed demand for hours with little history
  quantile: 0.8                 # cover a heavy week, not the average one
  min_samples: 3                # past occurrences before a slot's history counts

backlog_v2:
  # Disabled by default. "coordinator" is mutually exclusive with backlog.enabled.
  mode: disabled
  coordinator:
    id: ""
  workers: {}
  projects: {}
  setup_profiles: {}
  quota_pools: {}
  storage:
    bundles: ""
    artifacts: ""
    workspaces: ""
  transport:
    kind: ssh
    request_timeout: 30s
  message_limits:
    max_bytes: 4194304
    max_artifact_bytes: 1073741824
  freshness:
    worker_max_age: 1m
    quota_max_age: 1m
  leases:
    duration: 2m
    renew_interval: 30s
  scheduling:
    interval: 10s
    catch_up_max: 100
  # Startup is always closed; no worker or T3 contact occurs in this state.
  startup_admission: closed

archive:
  # Cold storage for finished threads. Once a day a thread that has not
  # been updated for "after" is bundled (full T3 export, provider logs,
  # the provider's transcript) into <destination>/<host>/<yyyy-mm>/<id>.tar.gz,
  # verified by checksum on the far side, and only then removed locally and
  # deleted from T3. Threads with pending resumes, backlog tasks or waits
  # are left alone. "t3-steward archive candidates" shows what would go.
  enabled: false
  after: 48h
  destination: ""               # a directory, or host:/path over SSH
  at: "03:30"                   # local time of the daily run
  delete_from_t3: true
  remove_local: true
  keep_transcripts: 336h        # bundled transcripts stay on disk this long (toolfeedback reads them)
  max_per_run: 50

notifications:
  # Desktop notification (notify-send on Linux) when a stop fails or when
  # a thread is stopped or resumed.
  desktop: true

# SQLite state database. Empty means $XDG_STATE_HOME/t3-steward/state.db
state_path: ""
# debug, info, warn, error
log_level: info
`
