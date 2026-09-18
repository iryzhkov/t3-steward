# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

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
  with `--model INSTANCE/MODEL` derives nothing from the catalog and sends no
  `projects` query, so it starts a task against a coordinator older than that
  query; the route then carries no quota pool and the coordinator resolves it
  from the worker's inventory. A start that does need the catalog and meets a
  coordinator without the query is refused with that coordinator's release, the
  release the query needs and those two flags.
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
  ready, and the instance/model/pool routes it advertises.
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

### Changed

- A version 2 task that declares no provider route at all is refused as
  permanent `no-route`, at `campaign check` and at intake, with the
  instance/model pairs its project's eligible workers advertise. The
  coordinator never chooses a route; before this, such a task was accepted and
  then made every eligible worker a candidate with a nil route, which failed
  the assignment-planning report for the whole fleet on every tick until the
  run was cancelled. The legacy single-task adapter refuses a submission with
  no instance and model the same way, as a content conflict, so its source
  quarantines the file once instead of reporting it on every cycle.
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
  documents. The text
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
