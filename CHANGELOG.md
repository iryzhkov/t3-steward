# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed

- Finished steward projects no longer stay in the T3 sidebar for days. A
  project can only be removed once it holds no thread, and a finished task's
  thread waited the full `archive.after` retention (48 hours) and then the next
  03:30 daily run before leaving T3, after which the project sweep still waited
  24 hours from the project's own creation. A new `archive.managed_after`
  (default 6h, `0` disables) applies to threads in projects under the worker
  workspaces root, and those threads are checked every hour. The default
  `project_cleanup.after` drops from 24h to 1h: the worker provisions a project
  immediately before opening its thread in it and recreates a removed one, so
  the longer wait protected nothing. A finished task now leaves the sidebar
  roughly seven to eight hours after its last thread settled.

### Added

- Owner-channel notifications. A top-level `notifications` section sends
  campaign events to Discord (`discord.webhook_url_file`, a 0600 file owned by
  the coordinator's user that holds the webhook URL and is never logged) or to
  a command (`command.argv`, event JSON on standard input, a minimal
  environment, its own process group). The events are `run-succeeded`,
  `run-failed`, `run-cancelled`, `run-skipped`, `needs-input`,
  `supervision-escalated` (one message per episode) and `gate-review`, and
  every one except `gate-review` is on by default. A scheduled run reports a
  success or skip only with `scheduled_success: true`. Delivery runs from an
  outbox in the coordinator store, at least once, off the scheduling loop,
  with backoff up to 30 minutes over 8 attempts, honouring Discord's
  Retry-After. A per-event watermark means that enabling a sink or event
  never replays history. Settled rows are pruned after 30 days, and the
  section can be changed by a reload. Schema 30 adds the outbox: release
  0.11.0-rc.95 cannot open a schema-30 store, so rolling back needs the backup
  taken before the upgrade. See `campaign help notify` and the operations
  guide.
- `backlog list` and `campaign list` take `--limit N` and `--since DURATION`.
  Text output shows the newest 50 runs by default and says how many were left
  out; `--json` stays unbounded unless `--limit` is given.
- A coordinator that rejects and discards a worker checkpoint records a
  `checkpoint-import-rejected` audit event, which `backlog events` and
  `diagnose` show. If the event cannot be written, the upload stays retryable.

- The steward removes the T3 projects it created itself, once they are empty.
  Every backlog task and every supervision activation opens its thread in a
  project whose workspace root is a directory the worker owns, and nothing
  removed them, so a host accumulated one project per catalog project, one per
  supervision activation and one for every identity a renamed project or a moved
  coordinator left behind, all of them in the T3 sidebar next to the projects a
  person actually opens. A new `project_cleanup` section (enabled, `after: 24h`,
  `every: 1h`, `max_per_pass: 20`, `dry_run`, `roots`) removes a project that
  holds no thread at all -- counted from the full read model, so an archived
  thread still occupies its project -- and whose own record has been untouched
  for `after`.
  Ownership is the whole of the rule: a candidate's workspace root must be
  inside this host's worker workspaces root, so a project someone opened is
  never one. It removes nothing else -- threads belong to `archive` and
  workspaces to the worker's retention, and no directory is deleted -- and it
  never forces a deletion, so a project that gained a thread since the pass read
  its snapshot is refused by T3. Each removal is one `project-cleanup` row in
  `t3-steward status`.

  A managed project is now identified by its owned workspace root rather than by
  the ID derived from its key, which is what makes removing one safe: T3 keeps a
  deleted project's record and refuses to create the same ID twice, so a project
  identified only by that ID could never be provisioned again once anything
  removed it. A project at the owned root is adopted whatever ID it carries, and
  a spent identity is replaced by a fresh one at the same root.
- Every invocation of `t3-steward` appends one record to the tool-feedback
  spool the fleet's MCP servers already write, at
  `~/.local/share/toolfeedback/t3-steward/<host>-<date>.jsonl`. The record
  holds the verb path, the flag keys, an outcome class and the elapsed time,
  and it is written from `main`, where the verb, the flags and the exit code
  are known without parsing anything. It carries no argument values: the verb
  path is built by matching words against the help-page registry and the flag
  keys against the options those pages document, so only names this program
  already publishes are ever written, an undeclared option records as `?`, and
  everything after a bare `--` is not scanned at all. `outcome` maps the exit
  codes the help pages document onto the five classes an agent experiences:
  `ok`, `usage` (3, 4 and a recognised refusal at 1), `state` (2 and 8),
  `transport` (5, 6 and 7) and `internal` (an unclassified 1). The spool is off
  unless the `enabled` marker exists in the spool directory or
  `T3_STEWARD_FRICTION=1` or `TOOLFEEDBACK=1` is set, and a spool that cannot
  be written is skipped in silence: it never changes the exit code, standard
  output or standard error.
- `t3-steward task run` starts one task on the fleet from a checkout, with the
  project, the ref, the route, the idempotency key and the wake derived and
  every derived value printed: the project from `--project` or the checkout's
  `origin` remote matched against the new `projects` query, the ref from
  `--ref` or the current branch when it is pushed (a detached HEAD or an
  unpushed branch is refused with "push first or pass --ref"; a dirty tree is a
  warning), the route from `--model [INSTANCE/]MODEL` against what the
  project's eligible workers advertise, with the quota pool the instance
  advertises and never an invented one, or from the new optional
  `backlog_v2.coordinator_client.defaults.model`. The calling thread is
  notified by default and a start is refused when none resolves unless
  `--no-notify`. `--fan-out GLOB` starts one run with one task per file. A
  repeat replays the same run and prints `replayed: true`. It composes the
  existing campaign path: what it submits is what `campaign submit` would
  submit. The verb is under `task`, beside `task env`; `t3-steward run` is
  still the watchdog's foreground command and is unchanged. `--project NAME`
  with `--model INSTANCE/MODEL` decides what will run without the catalog, so
  it starts a task against a coordinator older than the `projects` query: that
  query is then sent only for the route's quota pool, and a coordinator that
  refuses it leaves the pool empty for the coordinator to resolve from the
  worker's inventory. Both ways of naming a route read the pool the same way,
  so both submit the same archive: the idempotency key covers the instance and
  the model and not the pool, so a pool present on one path and absent on the
  other would give one key two archives and the second start would be refused.
  The pool is outside the key either way, so the identical command run once
  while that query is refused and once while it is answered still submits two
  archives under one key; the second start is refused, and the refusal now
  names the pool and the catalog beside `--worker` and `--name`.
  A start that does need the catalog and meets a coordinator without the query
  is refused with that coordinator's release, the release the query needs and
  those two flags.
- `t3-steward task result <run>[/<task>] [--output DIR] [--json]` collects a
  finished task in one call: `final-message.md` and every declared output,
  written under `./.t3/results/<run>/<task>/`. It exits 0 for a succeeded task,
  2 for a failed or cancelled one with whatever exists still written, and 1
  for one that is not terminal, with its progress printed. `--json` inlines the
  final message.
- `t3-steward models [--project NAME] [--json]` lists every provider route the
  fleet can run now, one row per `instance/model`, joining three facts that
  fail separately: authorisation from the fleet catalog's quota pools,
  advertisement from the workers' inventories, and the pool's admission state
  with the phase and used percent of its worst bucket. An instance that is
  authorised and advertised by nobody, and one advertised with no authorised
  pool (`missingBinding`), are listed with that as their status rather than
  omitted.
- A `projects` query kind, rendered by `t3-steward backlog projects
  [--project NAME] [--json]`: per project the repository, default ref, type and
  setup profile, and per eligible worker whether it is configured for the
  project, whether its inventory advertises it, whether it is enrolled and
  ready, and the instance/model/pool routes it advertises. Every verb that asks
  for the catalog -- `backlog projects`, `models --project` and a `task run`
  that derives its project or its route -- explains a coordinator that does not
  have the query with that coordinator's release, the release the query needs
  and what the verb can do without it, rather than passing on the bare
  `invalid query: kind "projects"`.
- `t3-steward campaign cancel <run> --reason TEXT` cancels every non-terminal
  task of a run with one command, one application and one revision fence per
  attempt. The `<run>/<task>` form is unchanged and works against every
  release. The run form needs a coordinator at this release or newer, because
  an older one accepts the request and cannot apply it; the client reads the
  release the coordinator reports for itself and refuses the run form against
  one that cannot apply it, naming the per-task form. The `--json` document
  names the tasks the command covers under `willCancel`, not `tasks`: the
  command is queued and the coordinator applies it on its next tick, so that
  list is an intention computed from a read and not the applied outcome, which
  is in `t3-steward backlog commands <run>` and the audit event.

- Every wait has a kind, and every wake message begins with one parseable
  line, `t3-steward-wait kind=<kind> outcome=<outcome> wait=<id> ...`, with
  kind-specific pairs after it (contract 3 of the agent-experience campaign).
  The local kinds, settled by the registering host's wait runner: `shell`
  (today's `-- <command>`), `time` (`--at RFC3339`, `--for DURATION`; the poll
  interval follows the remaining time so the last poll lands within 30 s of
  the instant; `at=`), and `github` (`--github run <id> | pr <n> [--state
  completed|merged|reviewed|checks-passed] [--repo owner/name]`; a built-in
  check runs `gh` with fixed arguments and applies a fixed mapping, three
  consecutive `gh` errors give up with the last error; `target= state=
  conclusion= url=`). The coordinator kinds, settled from the coordinator's
  own records with no local check on any host: `node` (`--node <run>[/<task>]
  --state terminal|succeeded|paused|waiting-external|active`; `run= task=
  attempt= revision= progress=` and, for a terminal run, `failed=` and
  `result="t3-steward task result <run>"`) and `quota` (`--quota <pool> --below N |
  --phase normal | --reset`, from the merged bucket observations; `pool=
  phase= percent=`). Every kind works interactively and with `--task current`,
  where a coordinator kind is a task wait with a structured condition. The
  campaign notification and task wakes carry the same first line;
  `wait list --json` and the task wake context carry `kind` and `outcome`.
  Against a coordinator from an earlier release a plain shell `--task
  current` wait keeps working (its registration carries no new field);
  `time`, `github`, `node`, `quota` and `--or-timeout` are refused by that
  coordinator with an `unknown field` error until it is upgraded, so deploy
  the coordinator first.
- `--or-timeout` makes the deadline a normal outcome for every kind: the
  wake says `outcome=timed-out or-timeout=true`, the result reads as exit 0,
  and the coordinator's expiry records no contradiction.
- `--group NAME --wake all` works for the coordinator kinds too (one message
  when every member settled). A group, and a task's `--wake all` set, is all
  local kinds or all coordinator kinds; a registration that would mix the two
  is refused, naming both members.
- Cancelling a task whose attempt is parked on a live task wait settles the
  wait as `cancelled` in the same command application, and the worker cancels
  its check row on its next reconcile (U-4). The coordinator's settlement
  pass also settles any live task wait whose attempt is already terminal as
  `cancelled` on the next boundary tick, so a crash between the cancellation
  and its wait settlement, or any other path that ends an attempt, leaves no
  wait live until its deadline.

- The coordinator fleet projection may carry, per worker, `quota_bindings`: a
  map from a provider instance to the one quota pool its work may be charged
  to, authored by `upkeeper provider add`. The coordinator uses it where
  `backlog_v2.workers.<w>.providers.<instance>` has no `quota_pool`, including
  where it has no entry for that instance at all, so registering a provider is
  one release edit and one pull instead of a hand edit of the coordinator's
  `config.yaml` beside it. An explicit `quota_pool` still wins. A binding
  naming an instance the worker does not run, or a pool it does not join, is
  refused when the projection is decoded; a binding to a pool
  `backlog_v2.quota_pools` does not define on this coordinator is dropped with
  a warning naming it, because the pool's provider and concurrency are still
  operator configuration. The field is optional: a projection rendered before
  it existed decodes and applies exactly as it did. See
  `docs/backlog-v2-operations.md`, "Registering a provider".
  **Upgrading:** Upgrade every coordinator host to this release before any
  UpKeeper release authors a `quota_bindings` entry: an older coordinator
  refuses the whole projection as an unknown field, so its reload is rejected,
  the `steward-fleet-configuration` component fails. UpKeeper v0.1.11 and newer
  roll a refused projection back when the coordinator answered with a receipt;
  a coordinator too old to write one leaves the refused projection on disk,
  where it will also stop that coordinator from starting the next time it is
  restarted. Author the binding in a release that
  also pins this steward version, and do not converge it with `upkeeper pull
  --components steward-fleet-configuration`, which writes the projection
  without installing the binary that can read it.

- `t3-steward models` reports, per instance and per worker, `authorized`,
  `advertised` and, when that instance and worker route cannot run, the
  `reason`: `missing binding` (the coordinator dropped the instance at load),
  `no models` (the fleet authorises it for no model), `not installed` (the
  worker has no such instance) or `unavailable` (installed, not signed in or
  not enabled).
  The text form gains one line per instance and worker under the table. The
  reason field, added with the verb, was empty until now because the
  coordinator did not report why it dropped an instance; it does now, and the
  workers query carries the per-worker provider authorization the answer needs.
  A worker row carries `authorized` only when the coordinator reported
  per-worker authorization at all: an older coordinator's answer leaves the
  field out rather than printing `false` for a route it said nothing about.
  An instance the coordinator dropped is listed at all for the first time: it
  is in no quota pool and in no worker inventory, which is why "the release
  authorises opencode and nothing runs it" was invisible from every command.

### Changed

- A run's supervision activations share one T3 project instead of taking one
  each. The project was keyed by the activation's thread, so a single run put
  five "Steward supervision: run-..." projects in the T3 project picker, which a
  person has to read past when choosing where their own session runs. It is now
  keyed by the run, and addressed by an owned metadata directory rather than by
  the activation's prepared workspace, because a project is identified by its
  workspace root and every activation prepares a different one. The activation
  thread still opens in its own prepared workspace.
- `t3-steward backlog projects` answers the question it was asked. Unfiltered,
  it is now one row per project with the eligible worker count and the
  advertised route count, followed by the totals and the two ways to get the
  detail. It was 28 KB of text and 90 KB of JSON on a thirteen-project fleet,
  because every eligible worker of every project was printed with all
  thirty-six routes spelled out. `--project NAME` is unchanged and still
  answers with one project in full, as does a catalog that holds one project;
  the new `--verbose` prints the whole catalog in the old detailed form.
  `--json` follows the same rule rather than an exception to it: its default
  document carries one entry per project with `workerCount` and `routeCount`,
  the document-level `totalProjects`, `totalWorkerRows` and `totalRoutes`, a
  `summarised` flag and the same pointer to the detail, and `--verbose --json`
  is the old document byte for byte. Nothing machine-parses that document --
  `task run` issues the projects query in process and never reads this
  command's output -- so no schema version changes. The scope is applied
  before the summary, by the coordinator: a scoped answer is the whole of a
  smaller question rather than a window onto a larger one.
- `t3-steward models` gains `--instance ID` and `--available`, which narrow
  what is read, and both are applied to the document before it is rendered or
  encoded. A narrowed table says how many routes and instances the unnarrowed
  answer holds and which filter is in force, so it cannot be mistaken for the
  fleet; a filter that matches nothing says so and gives those totals rather
  than printing the empty table an empty fleet would print. The `--json`
  document gains `instance`, `available`, `totalInstances` and `totalRoutes`.
- `t3-steward backlog task show` and `t3-steward diagnose` print the attempt's
  own timeline: when it started, how long it has been going, and, while it is
  not finished, when its lease expires and how much of it is left. "It started
  an hour ago, why is it not finished" needed a second call in `--json`
  before, because the text path carried the coordinator's generation time, the
  worker's observation time and the waits' deadlines, and no clock of the
  attempt's own. An attempt that has not started prints no timeline at all --
  attempts exist from planning and acquire their assignment at dispatch, so a
  queued, unassigned attempt has no clock yet. A terminal attempt claims no
  lease, and a timestamp missing from the record of an attempt that did start
  is printed as unknown naming the absent field rather than as a zero time
  rendered as a date.
- The session line of `backlog task show` is labelled as the provider
  session's own state, and says what it means when it would contradict the
  attempt above it. `session: thread stopped, control stopped, phase completed`
  meant "the provider thread was not generating at that instant" and was
  printed four lines under the same attempt's `progress: active` and `control:
  running`; an agent diagnosing a slow task read it as "the work stopped". It
  now reads `provider session: idle when the worker last looked (...); the
  attempt is progress active, control running, so that is the provider thread
  between turns and not the task stopping`, and a session the attempt agrees
  with is printed plainly under the same `provider session:` label.
- The three enrollment refusals name the worker, the fact that failed with
  what was observed against what is required, and the command to run next. The
  provider-route refusal was `configured provider route is unavailable on
  worker`, which named neither the route nor the remedy in a binary whose
  `task run` refusals list every acceptable value, and `worker enroll --all`
  prints one refusal per worker.
- `t3-steward campaign --help` offers `--no-notify` and says the calling
  thread is notified by default. The family page still described the submit
  that existed before notification became the default, so the flag an
  unattended caller needs was absent from it.
- A provider instance the fleet projection authorises with desired models and
  no quota binding no longer fails the coordinator's whole configuration. It is
  dropped for that worker, the coordinator logs one warning at startup naming
  the instance, the worker and the remedy, `t3-steward models` reports it as
  `missing binding`, and the rest of the catalog loads. This is the rule a
  project without a local binding already follows, for the same reason:
  refusing the whole configuration took every coordinator admin query down.
  An explicit binding to a pool the projection does not authorise for that
  worker still fails the load, because that is the operator's own file
  contradicting the fleet rather than a missing binding.
  **Upgrading:** a coordinator that was relying on the refusal to notice an
  unbound instance now starts with that route dropped; the startup warning and
  `t3-steward models` name it.

- The `WORKERS` column of `t3-steward models` counts the workers advertising
  the route, not every worker it is authorised for. The rows now include the
  workers a route is authorised for and unavailable on, and a worker that does
  not advertise a route cannot run it however ready it is.

- A version 2 task that declares no provider route at all is refused as
  permanent `no-route`, at `campaign check` and at intake, with the
  instance/model pairs its project's eligible workers advertise. The
  coordinator never chooses a route; before this, such a task was accepted and
  then made every eligible worker a candidate with a nil route, which failed
  the assignment-planning report for the whole fleet on every tick until the
  run was cancelled. The legacy single-task adapter refuses a submission with
  no instance and model the same way, as a content conflict, so its source
  quarantines the file once instead of reporting it on every cycle.
  **Upgrading: check the drop directories first.** The legacy adapter defaults
  neither `instance` nor `model`, so every drop file already sitting in a
  `backlog.dir` without both of them is quarantined on the first cycle after
  this release starts, not only new ones. Nothing is lost and nothing retries
  itself: `t3-steward backlog quarantine` lists them with the reason, and each
  file has to be given an `instance` and a `model`. That edit changes the
  file's content, so its digest changes, the marker is released and the next
  cycle submits it again -- no `quarantine release` is needed for this one.
  `t3-steward models` lists the instance/model pairs the fleet can run.
- Waits are named for the family they hold in every document, and a settled
  task wait is no longer dropped. A run document (`backlog show`, `campaign
  show`) gains `taskWaits`, every task-bound wait of the run with its `kind`
  and, once it has one, its `outcome`, `exitCode`, `reason` and `settledAt`; a
  diagnosis gains `nodeWaits`, the interactive node waits it used to report
  under `waits`. Both documents still carry `waits` with exactly what it
  carried before, the live task waits in a run document and the node waits in
  a diagnosis, in the previous release's shape: the fields this release adds to
  a wait travel under `taskWaits` only, so `waits` is what rc.69 declares and a
  reader that decodes it strictly still reads it. **Deprecated: `waits` is kept
  for one release** so that a client of the previous release keeps working;
  read `taskWaits` and `nodeWaits`, whose names mean the same thing in both
  documents. The compatibility runs both ways for that release, in the text
  form and in `--json` alike: a document from a coordinator of the previous
  release, which sends only `waits`, has its renamed keys filled where it is
  decoded, so `campaign show`, `backlog show` and `diagnose` during a
  mixed-version window say what a parked task is waiting for and print the
  content under `taskWaits` and `nodeWaits` rather than beside them. The
  deprecated key keeps what it carried either way, and the fallback goes away
  with it. The text
  form of a run prints a settled wait with its outcome instead of dropping it.
- `backlog workers` prints, under the worker table, what each worker can
  actually take: the projects it advertises and its instance/model@pool
  routes. They decide where work can run and were visible only in `--json`.
- A shell check that exits 2 settles as `gave-up` rather than `failed`,
  locally and on the coordinator; `failed` now means the condition decided
  against the waiter (a run that concluded with a failure, a pull request
  closed unmerged).
- `--state terminal`, the default of a node wait and what a campaign
  notification waits for, is `met` on any terminal progress except cancelled
  (`cancelled`), with `progress=` and `failed=` saying what happened;
  `--state succeeded` is `failed` on a failed run. `ResolveNode` and success
  dependencies keep their exit codes.

- The coordinator writes a receipt for every SIGHUP at
  `<state dir>/coordinator/reload-receipt.json` (atomically, before the log
  line that reports the outcome, never older than the previous one) with
  `requestedAt`, `completedAt`, `outcome` (`accepted`, `rejected`,
  `unchanged`), `error`, `configurationDigest` (effective after the request),
  `previousDigest`, `release` and, on a rejection, `blockers`. An unchanged
  file no longer restarts the configuration services. The status query's
  runtime block carries the receipt under the new key `lastReloadReceipt`;
  `lastReload` stays the activation time it has been since rc.56, so an older
  admin client still decodes the status document. `t3-steward coordinator
  identity` prints the receipt as `lastReloadReceipt` with `--json` and as
  reload lines in text. The coordinator also writes `coordinator.pid` beside
  the receipt (F-3, contract 2).
- `t3-steward coordinator reload [--json] [--wait DURATION]` sends SIGHUP to
  the coordinator on this host, waits (default 10s) for a receipt requested at
  or after the signal and prints it: exit 0 for `accepted` and `unchanged`, 8
  for `rejected` (class `rejected`, the receipt printed first), 6 when no
  receipt arrives in time, 5 when no coordinator pid file exists (F-3).
- A refused catalog change names every retained assignment on every worker
  whose execution catalog would change, with the attempt's progress and
  control, the phase the worker last reported for it and the action that
  unblocks it (`t3-steward backlog cancel <run>/<task> --reason TEXT`, after
  waiting for a pause to lift or a task wait to settle), in the WARN line and
  in the receipt. The rule itself is unchanged: a paused or parked attempt
  still blocks, because the worker refuses a republished catalog while its
  journal owns a non-terminal attempt and the coordinator refuses a session to
  a worker that does not accept the current revision (F-2, step A).
- Every resolver that reads a credential from `T3_STEWARD_CREDENTIAL_<REF>`
  (project credential checks, the worker protocol credential and the
  coordinator admin credential) also accepts
  `T3_STEWARD_CREDENTIAL_<REF>_FILE=<path>`: the inline variable wins when
  both are set, the file is read at use with one trailing newline trimmed, and
  a missing, symlinked or world-readable file is refused with an error that
  names the variable and the path and never the content.
  `t3-steward install-service --credential-file REF=PATH` (repeatable)
  renders `Environment=T3_STEWARD_CREDENTIAL_<REF>_FILE=<path>` lines into
  the generated unit, with the home directory as `%h`, so a hand-written
  wrapper script that exported the value can be retired with
  `install-service --force --credential-file F02_PROTOCOL=...` (F-10).
- `t3-steward bucket list [--json]` prints every quota bucket in the host's
  state database with its phase, used percent, observation, reset, recovery,
  stop and probe times, the thresholds it was derived under and its last rearm
  with the reason. `t3-steward bucket rearm <key> --reason TEXT [--force]
  [--json]` sets the phase to `normal` with the recovery time now, clears the
  stop and drain bookkeeping, records a `rearm` action carrying `user@host`,
  the reason and the phase before and after, and prints both states. An
  unknown key is refused with the known keys listed; a stored percentage at or
  above `stop_percent` is refused without `--force`. The worker treats the
  rearm as a confirmed recovery, under the same percentage rules as a real
  reset: a paused owned attempt resumes after `resume.reset_settle_delay`
  without a reading when the stored percentage is below
  `resume.below_percent` and below `policy.warn_percent`; a rearm above either
  only reopens the bucket for the next reading. The verb prints which case
  applies, and `--json` carries `resumeEligible` and `resumeBlockedBy` with
  the two thresholds (F-1).
- The worker probes a stopped bucket once per epoch: when the bucket is
  `stopped` below the current `stop_percent`, its stored reading is older than
  `resume.probe_after_reset`, and no running thread on the host matches it,
  one paused owned attempt is resumed to obtain the reading nothing else would
  produce. The probe is recorded on the bucket (`probedAt`) and as a `resume`
  action; the probing thread is not paused again while its reading is
  outstanding, and the reading rearms or re-stops the bucket. A worker whose
  thread list is unknown never probes.
- `t3-steward worker enroll <worker> --current-catalog` reads the catalog
  digest the coordinator requires and the worker's current enrollment revision
  from the coordinator's own workers view and submits the enrollment with them,
  so neither value has to be copied out of `backlog workers --json` or a
  readiness detail string. `--all --current-catalog` re-enrolls every configured
  worker whose accepted digest is stale and prints one line per worker
  (enrolled, already current, or refused with the reason). `--request-id`
  defaults to `enroll-<worker>-<digest12>-rev<N>` in that form; it replays a
  refused attempt (same `--reason`) and, after a success, the next run enrolls
  again at the advanced revision. The fenced
  `--catalog-revision` and `--expected-revision` remain and cannot be combined
  with `--current-catalog`. Enrollment still runs on the coordinator host and
  is still refused to the remote-admin role.

- `t3-steward thread stop <thread-id> [--session]` dispatches
  `thread.turn.interrupt` and, with `--session`, `thread.session.stop` for a
  thread on the local T3 server through the existing control client.
- `t3-steward backlog rewake <run>/<task> --reason TEXT` resumes an attempt
  parked in `waiting-external` with no live task-bound wait, the state a
  thread-side `wait cancel` used to strand it in. It is refused while a wait is
  live, naming the wait.
- `--expected-revision N` on backlog mutations. Without it, a `stale revision`
  rejection is resubmitted once by the CLI after re-reading the target, under a
  new command id, and the response says which revision was used.
- Workers advertise `quota-observations-v1` and report their host watchdog's
  bucket observations on the snapshot exchange when the coordinator asks; the
  coordinator's quota admission merges them with its own by bucket key, keeping
  the freshest, so a pool closes at its stop threshold before dispatch even
  when the coordinator's own host has no fresh reading.
- `backlog task show --json` carries the worker's last report on the attempt
  as `evidence`: thread id, worker, observed control and session state, and
  the quota pause reason. The text renderers of `backlog task show`,
  `campaign show` and `diagnose` do not print it yet. Session state and pause
  reason come from a worker that advertises `quota-observations-v1` and was
  asked for it on the exchange; an older worker reports neither (S-17).

### Fixed

- `wait add --github issue 5` is refused as `unknown --github kind "issue";
  use run or pr`. The id left behind as a positional argument used to be
  reported as a command after `--` that was never given.
- A `--github` target with a `#` that is not `owner/name#<number>`, such as
  `pr owner/name#abc`, is refused as malformed with the accepted forms instead
  of being passed to gh as a branch name. `owner/name#<n>` with no kind is read
  as a pull request.
- `campaign submit`, `campaign rerun` and `backlog submit` text output starts
  with `run <id>`, as `task run` does. The `next:` lines of `campaign submit`
  include `t3-steward task result <run>`. The campaign
  help says that `task result` collects the outcome, and that a caller with no
  thread polls `campaign show <run>`, because `task result` exits 1 until the
  run ends. It also lists `supervision` among the help topics and points at
  `campaign recovery retry`.
- `backlog usage` labels the run's progress as `backlog show` does
  (`Run: <id> (progress: ready)`). Both read the same field. The run moves
  from queued to ready between the two reads, so an unlabelled `(ready)` looked
  like it contradicted `show`. When more than one model appears in `By model`,
  a note says that a model the task was not routed to may be the provider's
  own auxiliary call in the same session. For Claude this is Haiku, which the
  provider reports in the turn's per-model usage. Totals are unchanged.
- `campaign plan` labels a task's importance, difficulty and max turns as
  `scheduling`, noting that importance sets dispatch order and difficulty seeds
  the admission estimate. The old label, `effort`, is also the name of a route
  option.
- The `campaign help fresh` UpKeeper example names placeholder workers rather
  than homelab, which cannot prepare a fresh workspace.
- `campaign list --since` and `backlog list --since` accept a whole number of
  days (`1d`, `7d`) as well as a Go duration. The help says that `--limit 0`
  lists every run.
- `explain` names a dependency that has not succeeded by its manifest name and
  gives its progress, and a cross-run blocker names its source node.
  `campaign explain <run>` without a task lists the run's
  tasks and their states instead of failing with a bare format error.
- `campaign validate --json` carries the validation document's `schemaVersion`
  in its error envelope. The error message on stderr is kept, as for every
  other `--json` failure. An unknown manifest field now names the manifest
  object (`a task`, `the workflow`) instead of the Go type
  `backlog.ManifestTask`.
- A missing prompt file is reported as `prompt_file prompts/review.md does not
  exist (paths are relative to the campaign directory)` instead of the raw
  `lstat` error.
- A misspelt top-level command such as `t3-steward campagin` suggests the
  closest family and is refused before its flags are parsed.
- `check` and `explain` no longer attach the `project-binding-defaulted`
  detail to a fresh project, because a fresh project has no credentials or
  setup profile to bind. Git projects still get the detail.
- The coordinator releases the unclaimed offer of an overseer activation that
  has closed, spent or been revoked, or whose epoch the run's supervision has
  moved past. Such an offer was re-offered and withheld on every boundary, and
  it blocked every catalog reload touching its worker. If the sweep fails,
  activation dispatch waits for the next pass. A row from an older activation
  epoch no longer counts as another valid activation, so it cannot block a
  replacement; the old overseer's decisions are still refused by the epoch
  check.
- Releasing a dead overseer offer, and superseding a failed one on operator
  reassessment, now also cancels the offer's never-started attempt
  (cancelled/stopped, one `attempt-cancelled` audit event) in the same
  transaction. rc.96 released the offer and left the attempt ready/unassigned,
  so coordinator planning and quota planning warned about a nonterminal
  attempt on a settled assignment on every boundary. The sweep also repairs
  that state: an already released activation assignment whose unassigned
  attempt belongs to an ended activation has the attempt cancelled on the
  first pass after upgrade. A claimed attempt, or one whose activation is
  still live, is never touched. An offered overseer assignment whose attempt
  row no longer exists is released too, since it still blocked catalog
  reloads; an offered assignment without an attempt that is not identifiable
  as an overseer's is left offered and counted in a warning.
- Graph amendments (`campaign rerun --prompt`, task edits) are judged by the
  same worker matcher as `campaign check` and submit, so validation and
  readiness can no longer disagree. Amendments that submit already refused
  are now refused too: `accept_backlog: false` without a named host, and an
  unmet CPU-class minimum.
- A worker no longer holds the lock that every coordinator exchange needs while
  it waits for a collection to finish, so exchanges, lease renewal and offers
  go through during a long verification. Two passes that race on the same
  attempt now collect it once. Throttle commands are refused while an attempt
  is being collected. A result whose attempt was superseded meanwhile is
  discarded, not completed.
- One malformed usage sample no longer holds back the worker's whole usage
  batch. It is rejected on its own, and one warning per batch names the
  sample and the reason. A storage error still fails the batch. Workers no
  longer send readings that have no event id.
- `backlog usage <run>` judges coverage from the run. Unbound samples make a
  run partial only if they could be its own: any provider, on a worker the
  run was dispatched to, inside the run's window. The fleet's unbound samples
  in that window are shown as context (`unscopedUnattributedCount` now counts
  only those). A coordinator host's local copy of a forwarded sample is no
  longer counted twice.
- `wait add --github` accepts `owner/name#N` and GitHub URLs for both `pr`
  and `run`, deriving `--repo`, and refuses an unparseable target with the
  forms it accepts. A branch name still works for `pr`.
- `campaign submit` and `check` say plainly that an `accepted_waiting`
  campaign was accepted and what it waits for. A note that every worker of a
  task shares is printed once, and workers the project does not use are
  summarised on one line instead of being shown as impossible.
- Quota planning no longer warns that a woken parked attempt is "paused
  without a durable throttle record"; the warning returns if such an attempt
  stays resuming for more than 75 minutes. `backlog list --all` combined with
  other arguments is refused with a pointer to `--limit`.
- `campaign validate` refuses `commits` on a task whose environment is
  `type: fresh`. A fresh workspace has no Git repository, so the declaration
  was accepted and failed only when the task finished, after its quota was
  spent. `campaign help fresh` says so, and its example verify checks the
  output's content instead of restating that the output exists.
- A manifest field within a typo of a known field of the same object is
  refused with `did you mean <field>?` ahead of the advice that a newer
  release may be required; a field that is not close to any known one gets
  only that advice. An unknown `campaign` subcommand likewise names the nearby
  command, and every such refusal points at `t3-steward campaign help` rather
  than claiming that recovery lives under `backlog`.
- The complete example in `t3-steward campaign help` runs as written: it
  declares a route and uses the fresh `scratch` project. The `wait` usage names
  the `claude-main` pool rather than a pool that does not exist.
- Cold storage measures a thread's retention from its settlement rather than
  from T3's `updatedAt`. Hiding a settled session updates the thread, so every
  session the UI archive hid started its retention again from the act of hiding
  it: a session settled yesterday read as "idle for 0m" and waited another two
  days to be bundled. A thread that was used again after settling is still
  measured from that use.
- A finished steward session is hidden from the T3 session list after the two
  hours a background session gets, rather than the day a person's own session
  gets. The classification comes from the worker journal, and the journal was
  looked for only under a configured `backlog_v2.storage.workspaces` -- which a
  worker running from the private UpKeeper bootstrap does not have in any
  configuration file. Every task and supervision thread therefore counted as a
  person's session: 27 finished runs were sitting in one host's list, none of
  them eligible to be hidden. The journals under this host's own worker storage
  are now read as well, whatever worker identities have run here.
- A settled task-bound check no longer holds its thread forever. The outcome of
  such a check belongs to the coordinator's wait record, which resumes the
  attempt and delivers the wake, so the local row is never woken and stayed
  "met" for the life of the database -- and while it did, the thread counted as
  custody, so the UI archive never hid it and cold storage never bundled it. A
  check that is still waiting, and an interactive outcome whose wake this host
  still owes, remain custody.
- Cold storage reads a thread that is archived in T3 by unarchiving it for the
  export and archiving it again unless it goes on to delete it. T3 omits an
  archived thread from thread detail exactly as it does from the shell
  snapshot, so simply making such threads candidates was not enough: every
  export answered `404 thread_not_found`, and a pass over 114 of them bundled
  none. A bundle that fails anywhere leaves the T3 UI as it found it; a process
  that dies inside the window leaves the thread visible, which the next pass or
  the UI archive corrects.
- Cold storage now sees the threads archived in T3, which are most of the
  threads it exists for. `archive` read the shell snapshot, which leaves an
  archived thread out entirely, so a session hidden by `ui_archive` or archived
  by hand was never bundled and never deleted: the fleet had accumulated 950 of
  them (719 on one host), every one still in T3's database and none in cold
  storage. It now reads the full thread index, so such a thread is bundled,
  verified and deleted from T3 once it has been idle for `archive.after`, at
  most `archive.max_per_run` per host per night. `t3-steward archive candidates`
  shows them before the next run, and `archive restore` brings one back.

  The project cleanup pass counts threads from the same index, because T3 counts
  an archived thread when it decides whether a project may be deleted. Its first
  release asked the shell snapshot, saw an occupied project as empty, and had
  every deletion refused with "is not empty and cannot be deleted without
  force=true"; it now leaves such a project alone and removes it after cold
  storage has emptied it. A thread list that cannot be read fences the pass.
- A `--github` wait registered without `--repo` is now polled where it can be
  read. `gh` resolves the repository from its working directory, and the poll
  runs in the steward daemon, whose working directory under systemd is the
  filesystem root -- so such a wait read its target once at registration, in the
  caller's checkout, and then answered "fatal: not a git repository" on every
  poll until it gave up after three of them. The poll now runs in the directory
  the wait was registered in, which also repairs the waits already stored, and a
  registration that names no repository resolves and records the one its
  directory is a checkout of, so the stored wait says which repository it is
  about.
- `wait add --node` and `wait add --quota` now register the wake for the
  calling host. Recording the calling host was added for the registration path
  of the superseded `--run` and `--task <run>/<task>` spellings, but the
  documented kind flags are parsed by a second path that never stated it, so on
  every host that is not the coordinator such a wait was recorded for the
  coordinator's hostname, its wake was sent into the coordinator's own T3 where
  the waiting thread does not exist, and the row stayed `delivery=pending`
  forever -- while the command still printed "end this turn now". Both paths now
  state the host, decide whether the coordinator accepts the field and report
  the outcome through the same three functions, so they cannot disagree about it
  again, and a registration whose wake cannot be shown to arrive here says so
  instead of promising a wake. A wait registered before this fix is still
  recorded for the wrong host: cancel it with `t3-steward wait cancel <id>` and
  register it again.
- A node wake now reaches a caller that is not on the coordinator. A wake is
  sent by the wait runner whose host matches the wait's, into that host's own
  T3, and a thread exists only on the host that opened it -- but a registration
  recorded the coordinator's hostname, so every `t3-steward task run`,
  `campaign submit --notify-thread` and `wait add --node` from another host
  registered a wait whose wake was sent into the coordinator's T3, where the
  waiting thread does not exist, and the record stayed `delivery=pending`
  forever. The client now states the calling host, the coordinator records it,
  and the steward of that host reads and claims the coordinator's rows over the
  admin transport and sends the wake locally, as it already does for task
  wakes. **Restart the steward daemon on every host, not only replace the
  binary**: the wake is delivered by the daemon and not by the command that
  registered the wait. After the upgrade a `delivery=pending` row is a question
  about the *named* host's steward rather than about the coordinator's, and a
  wait whose host never claims it is claimed by nobody: the coordinator's own
  runner now skips a wait addressed elsewhere.
- A command that registers a node wait promises a wake only when this host's
  steward daemon has recorded that it delivers them. The wait runner rewrites
  `node-wake-delivery.json` beside the state database on every tick on which it
  read this host's node waits and could send their wakes, naming the host, the
  release and its own interval; `task run`, `campaign submit --notify-thread`
  and `wait add --node` print `End this turn now` only when that receipt is
  present, names this host and is younger than four of the daemon's ticks or
  two minutes, whichever is longer -- two minutes at the default 15 s snapshot
  interval -- and otherwise say which of those is false and what to check on
  the host, because a receipt can also go stale under a daemon that is running
  but cannot reach the coordinator, or one running with wait dry run on. During an upgrade the daemon is the one thing that has not been
  replaced, and comparing hostnames could not see it.
- The calling host is stated only to a coordinator whose release is known to be
  `v0.11.0-rc.71` or newer, because an older one decodes the registration with
  unknown fields disallowed and would refuse it whole, failing a command that
  had already submitted its run. A release this client cannot parse is treated
  as an old one, so a coordinator built without the release ldflags -- it
  reports `dev` -- keeps the previous behaviour and says so instead of
  delivering cross-host wakes. Confirm `t3-steward backlog status` reports a
  parseable release on the coordinator after upgrading it.
- The node-wait list a wait runner reads from the coordinator is bounded to its
  own host's undelivered waits and is no longer replay-protected. The
  coordinator's node-wait table is append-only, so the unfiltered list was its
  entire history; and because the carrier classified every `node-wait`
  operation as a mutation, each list took the coordinator's exclusive
  admin-replay lock and wrote its whole answer into a 32 MiB / 4096-row store,
  shortening the 24-hour window for recovering a lost submission answer to
  hours, and to minutes as the table grew. A coordinator that cannot apply the
  narrowing refuses it, and the runner then asks for the whole list as before,
  so delivery still works against `v0.11.0-rc.70`. **Upgrade the coordinator
  before the workers.** That fallback list is unfiltered and, on an rc.70
  coordinator, still classified a mutation by operation word, so every worker's
  tick keeps taking the admin-replay lock and writing its whole answer into the
  replay store: none of the relief above applies until the coordinator itself
  is on this release, and the shortened replay window stays in effect for the
  whole mixed-version period.
- A refused node-wake delivery transition is reported. The claim that fences
  one send now crosses the network on every host that is not the coordinator,
  where a refusal means a rolled-back coordinator, an expired credential or a
  transport fault; it was swallowed silently, leaving a wake that never arrives
  and a record stuck at `delivery=pending`. A lost race stays silent, because
  that is the fence working.
- Every documented `backlog` verb reaches the dispatcher that implements it.
  `backlog projects` and `backlog rewake` were documented, parsed and
  implemented, but missing from the router's list, so both fell through to the
  legacy dispatcher and were refused as `unknown backlog command`. The router
  now derives the revision-fenced controls from the same predicate the parser
  uses, and a test derives its table from the help text, so a documented verb
  that reaches the wrong dispatcher fails the build.
- An answer served from the coordinator's replay cache says that it is a
  replay. The flag was set by the submission service, which a cached answer
  never reaches, so a repeat over the remote carrier printed `replayed: false`
  while replaying: `task run`, `campaign submit`, `schedules put`, a graph
  amendment, a supervision decision and `backlog recover` were all affected.
  The `[Unreleased]` promise above that a repeated `task run` prints
  `replayed: true` is true on the remote path for the first time.
- The JSON record of `task run` names the route's worker in both forms. The
  field was omitted when the route was unpinned, so a reader that decoded the
  document could not tell an unpinned route from a worker whose name it failed
  to read; both forms now print `any` for an unpinned route.
- A stored bucket phase now follows the loaded thresholds at start (F-1). The
  engine records the ladder it evaluated under on the bucket state, and the
  watchdog re-derives every stored phase of a current epoch from the stored
  percentage when it starts: a phase the loaded `warn_percent`,
  `drain_percent` and `stop_percent` would not produce is lowered, with
  `StoppedAt` and the drain deadline cleared and the recovery time set when
  the result is `normal`, and one `rearm` action names the stored phase, the
  new phase and both threshold sets. A phase is never raised at load. Before,
  a bucket stopped at 44 % under tightened thresholds stayed stopped after the
  thresholds were restored and the daemon restarted, because only a reading
  ever set the recovery time and a host whose threads were all paused attempts
  produced none (homelab, 2026-09-17 19:06).
- The interactive override covers every stop and drain path (S-18 remainder).
  A thread whose latest user message is newer than the bucket's stop, beyond
  the ten-second tolerance that covers the watchdog's own messages, is
  recorded in the bucket's thread notices as `user-resumed` with that time,
  and for the rest of the epoch it is stopped by no path (the stopped-phase
  poll, a reading that raises the phase, the drain grace timer,
  `stop_new_sessions`), never drained, warned at most once, and given no
  resume intent. rc.68 exempted it only in the stopped-phase poll and the
  resume intent (omarchy-pc, 2026-09-17 10:11 to 10:13). A rearm clears the
  record with the epoch, and the load-time re-derivation, which keeps the
  epoch, keeps the record and the warn notices; owned threads are excluded
  before the rule.
- A `SIGHUP` reload that brings in a fleet projection whose new project has no
  local binding is accepted as a catalog change. The list of defaulted projects
  the load records had leaked into the lifecycle comparison, so the first such
  reload was refused with `host lifecycle settings require restart` and the
  project stayed unknown to the coordinator until a restart.
- `t3-steward wait cancel <id>` on a local check bound to a task-bound wait
  now cancels the coordinator's wait first, through the configured transport,
  and marks the local check only once the coordinator has settled it. The
  attempt resumes on the coordinator's next tick with the cancellation as its
  wait outcome. Before, only the local row was marked and the attempt stayed
  parked in `waiting-external` on a live coordinator wait that nothing would
  settle until its 24 h deadline. If the coordinator cannot be reached nothing
  changes and the command fails with the transport exit code. `wait cancel
  <tw-id>` by the coordinator id also marks the local check on this host.
- `diagnose` lists the run's task-bound waits (`taskWaits`: id, task, attempt,
  name, condition, registration, deadline, outcome) beside its node waits, so a
  parked attempt is no longer reported with `"waits": null`.
- `wait add` reports the registration probe's exit code and first output line
  in every mode, including `--json` (`firstExit`, `firstOutputLine`), and warns
  on stderr when the first exit is neither 0, 1 nor 2: the protocol treats it
  as "not yet", and a check that fails the same way forever polls until its
  timeout. The registration is not refused, because an exit such as 7 can be a
  legitimate not-yet.
- An interactive `wait add` on a host whose `t3` CLI has disappeared now says
  that resolving the caller's thread needs the T3 API token and how to supply
  one (`t3.token` or the new `t3.token_file` in the configuration,
  `T3_STEWARD_T3_TOKEN` in the environment, or the CLI), instead of the bare
  "t3 CLI not found on PATH". `t3.token_file` names a private (mode 0600) file
  holding the token and is read at load when `t3.token` is empty.
- A fleet project with no `backlog_v2.projects` entry no longer fails the whole
  coordinator configuration load, which took every admin query down with
  `fleet project "home-assistant-config" needs an explicit local execution
  binding` when a project was published before it was bound. Such a project is
  loaded with an empty local binding (no credentials, resource locks or
  directory resources), the coordinator logs one warning naming it at startup,
  `Config.DefaultedFleetProjects()` lists it, and both `campaign check` (on
  each candidate's `unchecked` list) and `explain` (in a new `details` list)
  carry the informational `project-binding-defaulted` detail without changing
  the outcome. Workers and providers keep failing closed.
- A workflow manifest that declares a field this release does not know is
  refused with the field, its line and the running release, and the advice
  that a newer t3-steward release may be required, ahead of the decoder's own
  text. An older binary reading a supervised manifest previously printed only
  `field supervision not found in type backlog.Manifest`.
- `campaign list --progress` refuses an unknown state with the list of valid
  states, and `backlog command <id>`, `backlog task <target>` and
  `backlog artifact <id>` say `usage: backlog command show <command>` and
  suggest the show form with the identifier the caller gave.
- A coordinator boundary tick that fails because the coordinator is shutting
  down is logged at INFO as `shutting down: ...` rather than as a burst of
  ERROR lines reading `context canceled`. Every other tick failure keeps its
  severity.
- `campaign show` and `backlog show` list, under a parked task, the live
  task-bound waits parking it (id, name, condition, deadline), and for a
  supervised run list each
  gate with its state and, while it is pending, the observed tasks that have
  not produced evidence yet. Both appear in the JSON document as `waits` and
  `gates` on the workflow detail.
- `campaign help dag-semantics` documents both mounts a task reads files from:
  static inputs at `.t3/inputs/<declared path>` and dependency artifacts at
  `.t3/dependencies/<producer task id>/<artifact>`, with the note that the
  producer id is assigned at ingestion and must be listed rather than
  hard-coded.
- Tests now assert that `campaign check`, `validate` and `plan` with `--json`
  and `diagnose --json` write exactly one JSON document to stdout.
- The quota watchdog no longer stops or resumes threads a live steward attempt
  owns. Ownership is read from the worker's journal on the same host on every
  tick; a resume intent for an owned thread is cancelled with
  `thread owned by steward attempt <id>`, including intents recorded before
  this release; an earlier intent for a thread whose attempt has since
  settled is cancelled with `thread belonged to a settled steward attempt
  <id>`. Ownership lasts while the assignment lease in the journal is
  unexpired, so a crashed worker's threads return to the watchdog once its
  leases lapse. A cancelled campaign's threads are no longer resumed after the
  reset (S-16).
- A quota stop of an attempt's thread is a pause, not a failure. The worker
  drains or stops its own thread through the throttle path when the host
  bucket for the route is draining or stopped (a stopped bucket sends the
  drain notice first and stops the thread only when it is still working
  `policy.stop_verify_timeout` later), reports the attempt as `paused`
  with the bucket and percent as the reason, collects nothing meanwhile, and
  resumes it when the bucket has recovered and the attempt is still live. A
  session that is still not ready when collected is refused as
  `paused by quota watchdog: <bucket> at <percent>; ...` instead of an
  unexplained session failure (S-3).
- A rejected attempt command names the commands its state admits, for example
  `resume is invalid from waiting-external/waiting-external; allowed: rewake,
  cancel, skip` (S-5).
- The burn-rate projection asks for a drain at most while usage is below
  `stop_percent`; a hard stop needs the percentage threshold or an exhaustion
  under two minutes; the drain's grace timer escalates on the same terms, so
  a below-threshold drain stands when the grace expires and the next reading
  decides. The runway margin defaults to 1, so an exhaustion
  projected after the reset is no reason to act. A turn the user starts after
  a watchdog stop is left running while the bucket is stopped, and the drain
  notice says how long the session has at the current rate (S-18).
- An overseer activation now carries its supervisor identity in the activation
  workspace, as an owner-only `.t3-steward/supervisor.env` the worker writes
  before the thread starts, and the supervision commands discover it by walking
  up from the working directory. The identity previously reached the overseer
  only through T3's thread environment, which is sent only when
  `t3.send_thread_environment` is on; that setting is off by default and off on
  the fleet, so the CLI fell back to the host's own coordinator client and every
  gate the overseer accepted was recorded with actor kind operator. The
  environment remains as an additional channel, an explicit
  `--supervisor-credential` still wins over both, and the activation prompt now
  states that flag in full. The discovered identity reaches the supervision
  verbs and no other command.
- A gate decided by an operator while an overseer activation is live is still
  accepted, and the activation now records that the decision was not its own.
  Its outcome is the new `decided-by-operator` rather than `no-decision`, which
  read as a review that produced nothing, and `campaign supervision show`
  displays which principal decided each gate.
- An activation is closed when its run settles, clearing its lease. The dispatch
  pass skipped a terminal run entirely, so the last activation of every
  supervised run stayed `active` with a live lease and no turns after the run
  finished.
- Overseer activations are admitted and forecast by quota planning through the
  same predicate as every other route, with a durable remaining-cost estimate
  derived from the overseer route and `max_turns_per_activation`. Quota planning
  previously logged two warnings about an inconsistent attempt for every
  activation on every tick, and left the pool slot the activation held
  uncounted.

- Generated systemd user services preserve root-owned SSH configuration ownership.
  `ProtectSystem=false` avoids the user mount namespace that OpenSSH rejects;
  `NoNewPrivileges=true` remains enabled. Existing generated units need migration.
- Task-bound wait registration and settlement use the configured coordinator
  transport from worker hosts. Wake delivery follows the durable assignment owner,
  including recovery when a local poll record is absent. Printed task-wait IDs
  support list and cancellation through the same coordinator endpoint.
- Campaign readiness recognizes full pinned Git object IDs in advertised refs.
  A commit SHA is no longer mistaken for a ref name; an unadvertised object
  remains unverified instead of being falsely reported missing.

### Added

- `wait list --json` for the local checks; the human list names the
  coordinator task wait each task-bound check settles.
- `wait add --json` for interactive waits.

### Changed

- The worker journal decoder tolerates fields it does not know within journal
  version 1, so a worker binary rolled back onto a journal written by this
  release opens it instead of refusing every attempt with
  `worker journal: decode: ... unknown field`. Rollback note: this release adds
  `localThrottle`, `lastLocalThrottle` and `observedThreadState` to attempt
  records; an older binary drops them on its next write, so a locally paused
  attempt is then seen as an ordinary stopped one and collected, which fails
  its session as before this release. Drain or finish paused attempts before
  rolling the worker back.
- The coordinator schema is version 18. The migration adds the nine campaign
  supervision tables and rewrites nothing: an existing run gets no supervision
  record, because the absence of one is the unsupervised case. Migration is
  forward-only and there is no reverse migration; an older binary refuses a
  migrated database rather than degrading it. Rolling back after migrating means
  restoring the coherent stopped backup taken before it, which loses everything
  the coordinator recorded in between. See the rollback section of
  [Backlog-v2 operations](docs/backlog-v2-operations.md) for the loss window and
  the reconciliation it requires. Backup and restore need no new step: the
  snapshot copies the whole stopped database, so the new tables, the decision
  history and the idempotency receipts are captured by construction.
- The `campaign plan --json` document is schema version 2. It gained optional
  `supervision` and `gates` sections, and `totals` gained the `gates` and
  `heldTasks` counters, which are zero for a campaign that declares no
  supervision. The human-readable `campaign plan` output is unchanged byte for
  byte for an unsupervised campaign, including its content digest.

### Added

- A campaign may declare an optional overseer. Two new top-level keys in
  `workflow.yaml`, `supervision` and `gates`, put a separately routed agent
  session in front of named tasks: a gate observes the producers named in `after`
  and withholds the tasks named in `before` until an authorized acceptance is
  recorded, and a final gate guards run settlement instead of a downstream task.
  Decisions are structured and revision-fenced rather than inferred from prose,
  a rejection holds that branch while unrelated branches continue, and an
  unresolved review incident keeps a supervised run from settling. Operators read
  and decide the same state with `t3-steward campaign supervision`. Both keys are
  optional and a campaign that omits them is unchanged.
- Workers advertise the `campaign-supervision-v1` capability, which says that
  this build can run an overseer activation as ordinary assigned work. A worker
  that does not advertise it is never offered one: placement excludes it,
  `campaign check` reports `impossible` when no eligible worker advertises it,
  and the worker exchange withholds the offer as a last boundary. Upgrade workers
  before submitting a supervised campaign; unsupervised campaigns need no
  capability.
- Restricted coordinator-admin SSH transport supports campaign checks, submission,
  diagnostics, recovery and notifications from non-coordinator hosts. Live readiness
  checks include bounded repository probes and reject permanent failures before
  creating a workflow.
- UpKeeper-authored fleet projections now govern the coordinator's worker and
  project membership and desired model authorization. Catalog changes require
  explicit enrollment; desired configuration alone does not prove availability.
- Task-bound waits preserve the attempt and workspace while releasing capacity,
  then resume the same thread. Grouped `all` waits deliver one complete wake;
  collection waits for a causally acknowledged provider turn. Downgrades require
  drained waits and fresh worker capability observations.

- A task may declare Git commits it produces with `commits`, and a successor may
  consume one by name through `inputs_from`. The commit is kept reachable under
  the durable ref `refs/campaigns/<workflow-run>/<task>/<name>`, which pruning
  the repository cache does not touch, and its retained artifact is a provenance
  record naming the producing task, the base commit, the repository and the ref.
  The successor resolves the commit through that reference instead of finding it
  in a shared cache that the next `git remote update --prune` may empty.
  `campaign plan` reports the declarations in its text, JSON and DOT renderings,
  and `campaign help commits` carries the field's contract.

- `t3-steward backlog quarantine [--json]` lists the intake submissions the
  coordinator refused permanently: the key, the digest the marker was recorded
  for, when it was quarantined, the reason, and the fact that changed content is
  tried again. The quarantine audit event names no workflow run, so the
  run-scoped `backlog events <run>` view could not show it and the single log
  line was the only report. The view is read-only and, like every other read,
  works from a non-coordinator host.

- `t3-steward backlog quarantine release <key> --reason TEXT` clears an intake
  quarantine deliberately. The automatic release is bound to the file's content
  digest, so a submission refused for a reason outside the file, such as a
  project no alias mapped, stayed quarantined after the configuration was fixed
  because no byte of the file changed. The release is audited with the operator
  and the reason, and releasing a key that holds no marker says so instead of
  failing.

### Fixed

- A resumed task keeps the identity record it needs to name itself, so it can
  register a second task-bound wait. The coordinator reported an assignment
  parked only while its wait was still undecided, but a settled wait does not
  release the attempt: between the settlement and the moment the wake message
  reaches the thread the parked turn has ended and the resumed turn has not
  started. In that window the worker was told nothing was parked, saw a stopped
  thread, and collected: it deleted `.t3-steward/task.env`, published a result
  for outputs the task had not written, and the turn that resumed a moment later
  could no longer name itself. The statement now stays true until the wake is
  delivered or abandoned. Both the removal of an identity record and a deferred
  collection are logged, because a workspace found without a record used to be
  unexplained by any line in the journal.

- `t3-steward wait add --task current` can park a task again. The coordinator
  stamps the attempt revision into the execution package and then advances the
  attempt itself, when the worker claims the assignment and again when it
  reports the thread running, so the revision the task was handed was always
  behind by the time its turn started and every registration was refused as
  stale. The registration now resolves the live revision itself and is fenced on
  the attempt's own turn: the attempt must still have one, the registering
  thread must be the attempt's thread, and the commit is a compare-and-set
  against the revision read in the same transaction, so a registration racing a
  turn completion is still refused. The injected revision is kept as evidence of
  what the task was told, under its honest name `issuedRevision`.

- A collection that defers no longer destroys the task identity record. The
  worker removed `.t3-steward/` on entry to collection, before it had
  established that the T3 turn was terminal, so a deferred collection deleted
  the record permanently and the turn that resumed after a wake could not name
  itself. The record is now removed only when a collection proceeds, which is
  still before anything is captured from the workspace.

- A rerun of a rerun is no longer refused. The second rerun's subtree root
  carries what the first rerun's reused ancestors produced, and those carried
  inputs named the first run's artifacts until they were referenced into the new
  run, so the commonest recovery path there is — the fix did not work, run it
  again — died with an internal ownership message.

- A declared commit is no longer published for an attempt that has already
  failed. The ref cannot be redefined, so the retry's different commit was
  refused and the task became permanently unrunnable, with a complaint about the
  ref in place of the failure that actually happened.

- A preparation attempt is counted before it runs. A worker that died between
  the driver failing and the journal write came back believing no attempt had
  been made, so the retry budget never terminated and the preparation logs took
  ordinals until retention failed. An uncertain contained preparation still
  spends nothing, and a first cause lost to a crash is reported as not retained
  rather than replaced by a later error.

- Releasing a run's campaign refs reads the list it acts on under the lock a
  publication takes. A publication that landed in between had its record
  destroyed with the directory and its ref left behind forever.

- The coordinator releases the campaign refs of a run it no longer has records
  for, converging on what its store holds exactly as its workers do.

- Per-attempt preparation logs are retained read-only, as the workflow path
  already wrote them, and a copy that fails mid-write no longer leaves a
  truncated file occupying the ordinal. A campaign ref store that cannot answer
  is an error rather than "the ref is absent", which had let a republication
  redefine a ref a successor already resolved.

- Each preparation attempt now retains its own evidence file,
  `<attempt>.preparation.<ordinal>.log`. The retained log was named from the
  attempt ID alone, and the attempt ID does not change across retries, so the
  second attempt could not create the file: the real Git error was wrapped in a
  "file exists" complaint and the log pointer was dropped. The first causal
  failure is also kept durably and quoted in the terminal reason as
  `preparation failed N times; first error: <first>; last error: <last>`.

- A legacy drop file that can never be accepted is now quarantined after one
  durable report instead of producing the same coordinator error on every
  cycle for as long as the coordinator runs. The marker records the digest of
  the file it refused, so changed content is attempted, and reported, again.

- The campaign refs of a run are released when retention removes the provenance
  records that name its declared commits, so a finished campaign stops pinning
  commits. Nothing called the release before, and the store grew without bound.
  The lifetime is the record's rather than the run's: a rerun may only be created
  from a run that has already finished, and a rerun pins its source against
  retention, so a commit a rerun carries is kept for as long as the new run
  needs it.

- A retention pass that meets a pinned run skips it and prunes everything else,
  and reports which runs it skipped and which owners are holding them. A pin is
  enforced by a trigger that aborts the delete, and one abort rolled back the
  whole transaction, so a single rerun pin meant no artifact anywhere was ever
  pruned. No production path prunes coordinator artifacts yet; this is what makes
  the first one that does behave.

- A worker on another host releases its own campaign refs. The coordinator
  states, on the snapshot exchange of every reconciliation pass, the complete
  list of runs whose commits that worker must keep, and the worker releases the
  rest. The statement carries an explicit flag, so a coordinator that does not
  send one is not read as "release everything", it is bounded and validated, and
  it is refused whole rather than applied in part. Releasing stays idempotent and
  a failure never fails the exchange.

## [0.11.0-rc.48] - 2026-09-14

### Fixed

- A task may define success by its declared outputs alone. Requiring a
  verification command made such a package unbuildable, so the campaign
  validated and submitted but every offer was withheld and retried, with the
  reason visible only in a coordinator warning.

## [0.11.0-rc.47] - 2026-09-14

### Fixed

- The schedule lifecycle test no longer races the minute boundary. It read a
  schedule's revision and submitted a command fenced on it; a trigger landing in
  between moved the revision and the command was correctly rejected, failing CI
  for a refusal that was the fence working.

- An attempt whose result is rejected now settles with the rejection as its
  terminal failure. Discarding the result kept one bad result from blocking a
  worker, but left the attempt in verifying, where it could neither settle nor be
  retried and had to be cancelled by hand.

## [0.11.0-rc.45] - 2026-09-14

### Fixed

- `campaign plan` no longer reports a dependency mount path. Artifacts are
  mounted in a directory named for the producing task's ID, which is assigned at
  ingestion, so the projection was rendering a path built from the manifest name
  that looked authoritative and never existed. A static plan may report only what
  the manifest knows.
- Explaining a task also resolves its declared provider routes, so a task that no
  enrolled worker can serve is reported as blocked rather than eligible.

## [0.11.0-rc.44] - 2026-09-14

### Added

- A `campaign` namespace for authoring a workflow as a directory rather than a
  hand-assembled archive. `campaign validate` and `campaign plan` read the
  directory and change nothing; `campaign submit` requires an idempotency key and
  creates exactly one workflow and one run. `list`, `show`, `graph`, `explain`
  and `cancel` delegate to the existing backlog operations unchanged, so their
  JSON and exit codes are identical.
- Packing is deterministic: two directories with identical content produce
  identical bytes and the same digest the coordinator records, which is what
  makes the idempotency key meaningful.
- `campaign plan` renders the statically knowable execution plan as text, JSON or
  DOT: waves, roots, leaves, edges, inherited versus task-level settings,
  artifact bindings with their mount paths, and timing constraints. It does not
  claim a worker, route or capacity will be available; that remains
  `campaign explain` after submission.
- Checked-in single-lead and three-node example campaigns under
  `docs/examples/campaign/`, covered by tests.

## [0.11.0-rc.43] - 2026-09-14

### Fixed

- Preflight evidence is identified by attempt and step rather than by a prefix
  of its own content hash. Two tasks running the same probe against the same
  repository produced identical output and so claimed one identity, and the
  second publication was refused as conflicting with immutable metadata
  belonging to a different task.
- A result that claims an artifact identity belonging to another attempt is
  rejected and discarded once instead of retried forever, which previously
  blocked every other result the worker held.

### Changed

- The submission digest has one exported implementation, so a caller predicting
  the digest before submitting cannot drift from the one the coordinator
  records.

## [0.11.0-rc.42] - 2026-09-14

### Fixed

- Preflight logs are counted apart from the thread archive, so a task declaring
  preflight steps no longer fails the evidence-count contract.
- The session transcript is read from the thread archive rather than from
  whichever log artifact came last, which previously parsed a preflight log as
  JSON and failed the import.

## [0.11.0-rc.41] - 2026-09-14

### Fixed

- A result that can never be imported is discarded once instead of retried
  forever. The coordinator reconciles a worker in a single pass, so an import
  error aborted that pass and blocked every other result the worker held: one
  malformed result stalled an entire host. Superseded and rejected results stay
  distinguishable, because they are discarded for different reasons.

## [0.11.0-rc.40] - 2026-09-14

### Fixed

- Capture preserves an artifact identity the producer already established.
  Preflight evidence was given a fresh identity during capture, so it arrived at
  the coordinator as a generic artifact and failed the import contract, which
  refused the whole result. Outputs and verification reports, which have no
  identity before capture, are unchanged.

## [0.11.0-rc.39] - 2026-09-14

### Fixed

- Preflight evidence imports as its own kind of log. The import contract treated
  a log artifact as the thread archive and nothing else, so every preflight log
  failed an identity check it could not pass and the coordinator refused whole
  results in a retry loop. Both identities stay strict: a preflight log must
  carry a preflight identity and live under the preflight tree.
- A blocked task is explained through the planner's own placement matcher, so it
  reports which worker lacked which capability or fell below which CPU class
  instead of only that no worker was suitable. The admin view's separate copy of
  the placement rules had drifted and knew nothing about class or capacity.

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
