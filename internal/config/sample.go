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
  # A private file (mode 0600, owner only) holding the bearer token, read
  # when token is empty. Lets a host whose t3 CLI has gone away keep
  # reaching its T3 server. T3_STEWARD_T3_TOKEN overrides both.
  token_file: ""
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
  # The worker applies the same duration to a bucket stopped below
  # stop_percent: once the stored reading is that old and nothing on the
  # host is running to refresh it, one paused attempt per bucket epoch is
  # resumed as a probe. Zero disables both probes.
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
      min_window_duration: 168h # weekly, whether primary or secondary
    warn_percent: 95
    drain_percent: 97
    stop_percent: 99

# Message templates. Fields: .LimitName .UsedPercent .ResetsAt .GracePeriod
# .Window .Provider
messages:
  warn: |
    T3 steward quota advisory: "{{.LimitName}}" is at {{.UsedPercent}}% and resets at {{.ResetsAt}}. Continue the current user task and keep work focused. This advisory does not ask you to stop, checkpoint, or cancel active subagents. A separate drain notice will explicitly request a checkpoint and stop if quota becomes critically low.
  drain: |
    T3 steward quota drain: checkpoint and pause. "{{.LimitName}}" is at {{.UsedPercent}}% and resets at {{.ResetsAt}}. Do not start new work or subagents. Ask active subagents to checkpoint and return partial results promptly. Preserve completed work and write a brief checkpoint covering current state, partial results, and remaining work. Then end your turn. T3 steward will interrupt any still-running turn in {{.GracePeriod}}.

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
  # The limit on one runner verification command a task declares (1s-6h).
  verification:
    command_timeout: 30m
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

project_cleanup:
  # Removal of the T3 projects the steward itself created. A backlog task and
  # a supervision activation each open their thread in a project whose
  # workspace root is a directory this host's worker owns, and nothing else
  # removed them, so they accumulated in the T3 sidebar. A project is removed
  # when it holds no thread and its own record has been untouched for "after".
  # Only projects under this host's worker workspaces root are candidates, so
  # a project someone opened is never one; threads belong to archive: and
  # workspaces to the worker, and no directory is ever removed here.
  enabled: true
  after: 24h
  every: 1h
  max_per_pass: 20
  dry_run: false                # log the candidates, delete nothing
  # roots: []                   # extra directories, for a host that moved its worker storage

notifications:
  # Desktop notification (notify-send on Linux) when a stop fails or when
  # a thread is stopped or resumed.
  desktop: true
  # Owner channels for campaign events, delivered by the coordinator only and
  # in addition to the thread wake a submission registers. Each is off until
  # declared. Events default to run-succeeded, run-failed, run-cancelled,
  # run-skipped, needs-input and supervision-escalated; gate-review is opt-in.
  # Events that already exist when an event kind is first enabled are not
  # sent. See "t3-steward campaign help notify".
  # discord:
  #   # A private file (chmod 600) holding the webhook URL and nothing else.
  #   # The URL is a credential and is never written in this file. Only a
  #   # coordinator reads it.
  #   webhook_url_file: ~/.config/t3-steward/discord-webhook
  #   events: [run-failed, run-cancelled, needs-input, supervision-escalated]
  #   # Runs a schedule created send run-succeeded and run-skipped only when
  #   # this is true; their failures are sent either way.
  #   scheduled_success: false
  # command:
  #   # Run with no shell; the event arrives as JSON on standard input and
  #   # exit status 0 means delivered. The same event may arrive twice.
  #   argv: [/usr/local/bin/notify-owner]
  #   events: []
  #   # The program gets PATH, HOME and LANG only, plus the names listed here.
  #   env: []

# SQLite state database. Empty means $XDG_STATE_HOME/t3-steward/state.db
state_path: ""
# debug, info, warn, error
log_level: info
`
