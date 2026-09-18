# Backlog-v2 operations and recovery

This runbook describes the backlog-v2 release-candidate interfaces on branch
`feature/backlog-orchestrator`. It is an operator reference, not deployment
authorization.

## Release-candidate boundary

The release candidate includes the version 2 workflow model and manifest
validation, immutable bundle ingestion, DAG readiness, scheduling and quota
policy, workspace preparation, artifact transfer, coordinator persistence,
worker protocol records, deterministic dispatch identities, throttle
pause/resume, schedule idempotency, the transport-neutral admin service and
CLI, the versioned authenticated exchange, and the S16 restart-safe worker
runtime.

Completed S17 code adds a schema-11 submission journal and bounded
idempotent directory, tar, and legacy single-task submission services. It also
adds revision-fenced audited schedule-definition administration and a
persistent five-field-cron timer whose occurrence cursor and identities survive
restart. It also adds a quota bridge that deduplicates stored provider-bucket
evidence into fleet pools and closes admission for missing, stale, future, or
epoch-conflicting readings. Accepted submissions, quota admission revisions,
and accepted or suppressed schedule triggers publish native audit events in the
same transaction as their state changes; exact replay does not duplicate them.
Archive extraction rejects links, special files, traversal, duplicate paths,
excess entries, and excess bytes. Schedule matching
walks UTC minutes in the configured IANA timezone, so DST gaps produce no
occurrence and folds produce two distinct nominal UTC occurrences.

Completed S18 code adds native transaction-bound audit events for coordinator
authority, worker/assignment/dispatch/lease reconciliation, worker commands,
throttle delivery, artifact publication/pruning, and terminal imports. The
allowlisted detail binds available epochs and revisions, a stable idempotency
identity, actor/reason, and outcome without capability tokens, credentials,
filesystem paths, or arbitrary worker reports. It also adds stopped coherent
SQLite-plus-artifact snapshots, evidence-bound unknown-assignment recovery,
runtime incident status, owner-only production directories and immutable
objects, local-admin request deadlines and concurrency backpressure, and fuzz
seeds for strict protocol, archive, manifest, schedule, and local-frame parsers.

The executable now exposes only three fixed worker operations: control,
artifact receive, and artifact send. They bind strict local configuration,
credential principals, durable epochs, replay state, journals, artifact
custody, isolated workspaces, containment, and the worker-side T3 adapter.
Every worker declaration includes the worker epoch; worker mode rejects a local
epoch that differs from that declaration. The coordinator-side construction
boundary resolves the named credential, binds both epochs and principals to a
fresh authenticated SSH session, and pairs it with the worker-scoped execution
package catalog. Scheduled post-startup cycles initiate fresh sessions for each
configured worker; the startup cycle remains local-only and failed quota
reconstruction supplies an empty new-work admission policy.
The production coordinator now owns a bounded local admin socket for query,
durable mutation submission, artifact retrieval, revision-fenced schedule-
definition administration, and evidence-bound unknown-assignment recovery. It
authenticates the Unix
peer UID and does not trust client-supplied identity; coordinator-mode CLI
commands no longer open SQLite. While closed, the coordinator also ingests the
unchanged owner-controlled `t3-backlog`/`t3-job` Markdown drop through
aggregate byte/file bounds, project alias mapping, and the immutable submission
journal; this does not dispatch work. The same authenticated socket accepts
bounded native tar-bundle streams and returns immutable submission, workflow,
and run identities; exact idempotency-key replay returns the original result.
The bounded local cycle immediately and periodically fires restart-derived
schedule occurrences, executes durable revision-fenced admin commands, and
reconciles legacy submissions. Component errors are logged without preventing
other local boundaries from making progress. The cycle first reconstructs
active slots, paused fixed-route remainder, offered-work reservations, and
numeric per-bucket capacity/forecast windows, then derives and persists quota
admission from stored provider evidence. A failure defers both planning and admin
execution. A successful pass reloads DAG/worker state, applies hard quota
admission, and atomically persists offered assignments without contacting a
worker or creating a dispatch command. A transport-neutral importer now
validates completed-assignment custody, raw object hashes, declared outputs,
ordered verification reports, and structured provider completion before coordinator
artifact publication and replay-safe success/failure projection. Successful turns
need no final status marker: the archived thread must match the assignment, show
a completed turn with valid timestamps and a ready session without errors, active
turns, pending input/approvals or working background activity. Declared outputs
and verification must still pass. Legacy done markers remain accepted but cannot
override failed execution; failed, continue and needs-input markers prevent
implicit success. Tasks without outputs or verification establish normal execution
completion, not independent proof of work quality. Existing terminal history is
not reclassified. Scheduled
sessions compose bounded worker transport, lease expiry/renewal, lifecycle
delivery, durable throttle replay/delivery, one-at-a-time result outbox
discovery, bounded raw fetch, import, and post-import acknowledgement for both
results and checkpoints. Checkpoint publication additionally requires an exact
acknowledged throttle projection. The existing Markdown
`t3-backlog` runner remains the deployed production path. The disposable S19
process-topology, fault, full, and race gates pass. An explicitly authorized
Normandy-to-homelab SSH qualification also passed using an ephemeral no-effects
worker: an authenticated snapshot and empty-offer canary were repeated across
fresh sessions and worker processes. This evidence supports a GO readiness
decision, but it is not deployment approval. The detailed decision is in
[the deployment-readiness report](plans/backlog-v2-deployment-readiness.md);
the wire and worker contract is in
[the worker protocol](backlog-v2-worker-protocol.md).

## Configuration inventory

The shipped YAML schema is documented in
[config.example.yaml](../config.example.yaml). Decoding is strict at every
level: unknown keys are startup errors. Existing valid legacy configurations
remain compatible, and backlog-v2 is disabled by default.

- `backlog.enabled`, `dir`, `quiet_for`, and forecast fields control the
  legacy Markdown runner.
- `backlog.host_name` and `default_host` control legacy SSH forwarding; they
  are not backlog-v2 worker registration.
- `backlog_v2.mode` is `disabled`, `coordinator`, or `worker`.
  Coordinator mode and `backlog.enabled` are mutually exclusive. Worker mode
  exposes only the fixed exchange endpoint and never acquires coordinator
  authority.
- `backlog_v2.coordinator.id` is the durable coordinator identity.
- Worker mode requires `backlog_v2.local_worker.id`, `epoch`, and a positive
  `coordinator_epoch`. The ID must name an entry in `workers`. Rotate these
  durable values only while coordinator admission is closed and reconcile the
  old epoch before accepting work under the new one.
- `workers` declare SSH address, credential reference, capabilities, accepted
  provider instances/models, and quota-pool references.
- `projects` declare repository, default ref, T3 project, setup profile,
  eligible workers, credential references, and resource locks.
- `setup_profiles` contain nonempty command lists and positive timeouts.
- `quota_pools` map fleet admission to providers and require a positive
  `max_concurrent`. Every pool must own at least one configured provider
  instance, and one provider-instance identity cannot span pools.
- `storage` declares absolute, non-root, non-overlapping bundle, artifact, and
  workspace roots. Worker journals, replay state, workspaces, and custody live
  beneath the configured worker-scoped roots and must be restored coherently.
- `transport`, `message_limits`, `freshness`, `leases`, and `scheduling`
  set bounded exchange and lifecycle controls. `message_limits.max_files`
  bounds one legacy-drop scan and bundle/archive expansion. The worker caps
  accepted lease extension at its configured duration. The configured transport
  request timeout also bounds every local-admin connection; the coordinator
  rejects connections above its fixed handler limit with a backpressure error.
- `startup_admission` must be `closed` in coordinator mode. Coordinator
  startup acquires exclusive ownership and advances its epoch without
  contacting a worker or T3.
- `state_path` selects coordinator SQLite. Plain opens never create or migrate
  it; coordinator startup uses the explicit migration path.

The forced worker command must be installed with an absolute, operator-owned
configuration path, and it names no operation:

```text
restrict,command="/usr/local/bin/t3-steward worker-exchange --config /etc/t3-steward/worker.yaml" ssh-ed25519 AAAA... normandy-coordinator
```

Install one line per worker, not one per operation. A worker carries three
operations: `control`, which delivers commands and collects observations, and
`artifact-receive` and `artifact-send`, which move an assignment's inputs and
results. The coordinator reaches all three at the one `address` that worker
declares, and OpenSSH selects the `authorized_keys` line by the key presented,
so a line that pinned an operation would confine that worker to one of the
three. A worker pinned to `control` accepts no delivery of the inputs its
assignments need, so no work runs on it at all.

The operation is taken from the signed request envelope. Its message type is
covered by the same signature as the rest of the envelope, so only a coordinator
holding this worker's credential can choose which of the three it asks for, and
what the worker will accept is fixed by its own allowlist regardless of how the
endpoint was invoked. Pinning an operation as a last word still works and still
refuses a request of any other kind; it is useful only for a worker that
genuinely performs one, which an executing worker never is.

The flag follows the command word. Global flags are parsed after the command, so
a line written as `t3-steward --config PATH worker-exchange` exits with
`unknown command "--config"`.

Never forward `SSH_ORIGINAL_COMMAND` to a shell; the endpoint never reads it.
The endpoint ignores general configuration environment overrides and
command-line dry-run/log-level overrides. Envelope and project secrets remain
worker-managed named credentials and never enter workflow bundles, execution
packages, journals, or error text.

Coordinator-mode admin clients connect to `<resolved-state-path>.admin.sock`,
which is created mode 0600 and authenticates the kernel peer UID. They query or
submit durable intent only, never open or migrate SQLite, and never execute
pending coordinator commands.

## Version 2 workflow bundles

A bundle is a directory rooted at `workflow.yaml`. Referenced prompts and
static inputs are copied into coordinator-owned immutable storage at submission;
workers never depend on the submitting host remaining online. See the
[checked example bundle](examples/backlog-v2/workflow.yaml).

Submit a regular tar file through the coordinator without opening SQLite:

```sh
t3-steward backlog submit bundle.tar --idempotency-key REQUEST_ID
```

The archive byte length is declared and enforced at both ends of the local
transport, while archive entries and expanded bytes remain subject to the
configured `max_files` and `max_bytes` limits. Reuse the exact idempotency key
only for identical content; changed content fails closed. Add `--json` for the
immutable machine-readable result.

The manifest decoder is strict and accepts one YAML document. The top-level
fields are `version`, `name`, `class`, `placement`, `environment`,
`inputs`, `routes`, and `tasks`.

- `version` must be `2`; names use lowercase letters, digits, hyphens, and
  underscores and begin with a letter.
- `class` is `required` or `surplus` and defaults to `surplus`.
- `placement.hosts` limits eligible workers; `placement.requires` names
  capabilities. Task placement intersects host sets and adds capabilities.
- `environment.type` is `git`. `scope` defaults to `task`; `workflow`
  pins tasks to one shared, serialized checkout. `project` must resolve in the
  coordinator catalog, and `ref` defaults from that catalog.
- Routes are ordered candidates with `host`, `instance`, `model`, string
  `options`, and `quota_pool`. Task routes replace inherited workflow
  routes.
- Each task declares `prompt_file`; optional fields include `needs`,
  `inputs_from`, `outputs`, `commits`, `verify`, placement, routes,
  `resource_locks`, class, `importance`, `difficulty`,
  `estimated_cost`, `max_turns`, `not_before`, `deadline`, and
  `expires_at`.
- `commits` declares Git commits a task produces for its successors. Each entry
  has a `name`, which must be one safe path component, and an optional
  `revision` resolved in the producing workspace, defaulting to `HEAD`. A
  successor consumes a commit by that name through `inputs_from`, exactly as it
  consumes a declared output.
- Defaults are importance 3, difficulty 3, and max turns 3. Times are RFC 3339.
- `inputs_from` may name only declared output paths from dependency ancestors.
  A dependency releases only after explicit success and successful verification.
- All paths are relative to the bundle or workspace as appropriate. Absolute
  paths, traversal, unsafe globs, missing files, escaping symlinks, cycles,
  duplicate names, impossible placement, and unknown fields are rejected.

### Campaign-scoped commits

A commit a downstream task needs is represented explicitly. A task declares it,
the coordinator keeps it reachable for the campaign's lifetime under the durable
ref `refs/campaigns/<workflow-run>/<task>/<name>`, and the successor resolves it
by that reference. Nothing searches the repository cache for it: the cache is
refreshed with `git remote update --prune`, which deletes any ref the origin
does not have, so a commit parked there survives only until the next task
refreshes the cache.

Declare the commit on the producing task and consume it by name:

```yaml
tasks:
  implement:
    prompt_file: prompts/implement.md
    commits:
      - name: implementation
        revision: HEAD
  review:
    prompt_file: prompts/review.md
    needs: [implement]
    inputs_from:
      implement: [implementation]
```

The retained artifact of a declared commit is its provenance record, a JSON
document naming the producing task, the base commit the workspace was pinned to,
the repository, the commit and its campaign ref. The successor receives it at
`.t3/dependencies/implement/implementation` and starts with the commit already
fetched into its own checkout under the same ref, so
`git rev-parse refs/campaigns/<workflow-run>/implement/implementation` resolves
there. Preparation records the pin it started from at `.t3/base-commit`.

The refs live in a worker-owned store under `storage.workspaces/campaign-refs`,
which is a sibling of the repository cache and is never pruned. Publishing the
same commit again is idempotent; publishing a different commit under a ref that
already exists is refused, because a successor has already been told what that
ref means. A task that promised a commit it did not produce fails with
`declared commit "<name>": <cause>`, in the same way a missing declared output
fails.

The campaign lifetime of a commit is the lifetime of the provenance record that
names it. While that record is retained the commit must resolve, because the
record is the only thing that ever asks for it; once retention has removed the
record, nothing can ask again and the ref is released. Settlement is not the
boundary: a rerun may only be created from a run that has already finished, so
releasing at settlement would release exactly the commits a rerun is about to
carry.

A rerun needs no special case. It pins its source run against retention, a
pinned run's artifacts cannot be pruned, and so the provenance record — and the
commit it names — survive for as long as the new run does. A rerun authored
after the record has been pruned is refused by the rerun itself, which reads the
artifact before it creates anything, rather than failing hours later in
preparation.

On every coordinator boundary, after the projection has advanced the sinks, the
refs of every run whose provenance records retention has removed are released
together. A run whose sink is not yet terminal is never released, which covers
the window between a worker publishing a commit and the coordinator recording
the artifact that names it. Releasing is idempotent, a run that declared no
commit costs nothing, and a release that fails is logged as `campaign commit
release failed` and retried on the next boundary: the run has already finished
and nothing about its outcome depends on a ref being deleted.

Workers on other hosts keep their own stores. The coordinator states, on the
snapshot exchange of every reconciliation pass, the complete list of runs whose
commits that worker must keep; the worker releases every run it holds that the
list does not name. The statement carries an explicit flag, so a coordinator
that says nothing is not read as "release everything", and it is refused whole
rather than applied in part.

**Known limitation: nothing prunes coordinator artifacts yet.** Artifact
retention exists as a function and is what campaign refs now follow, but no
production path calls it: there is no scheduled retention pass, and no
configured retention window. Campaign refs therefore persist for as long as
their provenance records do, which today is indefinitely. What has changed is
that they are no longer a separate store with a lifetime of their own — on the
coordinator and on every worker they are released the moment the records they
name are gone — so the bound arrives with a retention pass and needs no further
work on the campaign side.

Choosing a retention window is a policy decision about your data, which is why
it is not shipped with a default. Whoever configures one must account for pinned
runs: a rerun, a node wait, a cross-run edge and a clone each hold their source
run against retention, and a pass reports those runs as skipped, with the owners
holding them, instead of pruning them. A skipped run is not a failed pass; the
rest of the fleet is pruned normally, and the run becomes prunable when the last
thing referring to it is gone. Note also that a rerun's carried inputs keep the
creation time of the artifacts they reference, so a window chosen by age alone
will treat them as old on the new run's first pass.

### Legacy intake quarantine

The drop directory is read-only to the coordinator: the coordinator re-reads it
on every cycle and never drains it. A file that can never be accepted, such as
one naming a project no alias maps or one whose content changed after its key
was accepted, is therefore recorded as quarantined in the submission journal,
reported once with its reason, and skipped silently on every later cycle.

The quarantine marker is a `quarantined` submission record under the key
`quarantine:<idempotency key>`. It carries the digest of the exact file bytes it
was recorded for and the reason it was refused; it never carries workflow or run
identities, because nothing was accepted. The single report is the coordinator
log line carrying the reason, and it is durable as one `submission-quarantined`
audit event per key and digest.

That audit event has no workflow run, so the run-scoped `backlog events
<workflow-run>` view cannot list it. The quarantine is read instead with

```
t3-steward backlog quarantine [--json]
```

which reports every marker with its intake key, the namespaced key its record is
stored under in `coordinator_submissions`, the content digest it was recorded
for, when it was quarantined, the reason, and the fact that changed content is
tried again. It is a read of the durable record: it releases nothing and
resubmits nothing, and it is an ordinary admin query, so it works from a
non-coordinator host over the same transport as every other read.

Recovery is to change the file. When the content of a quarantined file changes,
its digest changes, the marker is released, and the submission is attempted
again and reported again. Removing the file also ends the reports, and the
marker then stays in the journal as the record of why the intake refused it.

A quarantine the file cannot fix is cleared deliberately:

```
t3-steward backlog quarantine release <key> --reason TEXT [--json]
```

This is the way out of a refusal that was never about the content — a project
no alias mapped is the ordinary one — because adding the alias changes no byte
of the file, so its digest is unchanged and intake stays silent. The release is
audited with the operator and the reason as `submission-quarantine-released`,
and releasing a key that holds no marker reports exactly that instead of
failing, so an ambiguous response is safe to retry. It creates nothing: the next
cycle reads the file again and the coordinator refuses it again if it is still
impossible.

Dependency artifacts appear in the successor workspace at
`.t3/dependencies/<task>/<output>`. Declared verification commands, output
capture, checksums, final messages, preparation logs, checkpoints, and other
retained records belong to the coordinator recovery unit.

A failed preparation retains its log next to the attempt directory as
`<attempt>.preparation.<ordinal>.log`, with the ordinal counted from 1 in the
order the preparation attempts ran. Every attempt keeps its own immutable file,
so reading the first one shows why the preparation started failing rather than
what the last retry tripped over. The terminal failure of an attempt that
exhausted its preparation budget reads `preparation failed N times; first error:
<first>; last error: <last>`; the first error is the causal one. When the log
itself could not be retained, the retention failure is reported after the
failure that caused the preparation to fail, never in place of it.

## Routine administration

Use the commands in [Backlog administration](backlog-admin.md) while the selected
backlog-v2 coordinator is running. The CLI connects to the admin socket derived
from the selected state path; it does not open the database. Read views include
status, workflow/task/DAG detail, explanations, events, artifacts, schedules,
workers, quota, reservations, locks, command outcomes, and quarantined intake.

Create or revise a definition through the same socket:

```text
t3-steward schedules put <schedule> --name TEXT --workflow ID \
  --cron "EXPR" --timezone IANA --reason TEXT \
  [--after-failure next-cycle|hold] [--disabled] \
  [--expected-revision N] [--request-id ID] [--json]
```

New definitions use expected revision zero; updates must supply the current
revision. Reuse the same request ID after an ambiguous response. The coordinator
returns the immutable original result for exact replay and rejects changed
content under that ID. The authenticated peer UID, not client-supplied identity,
becomes the audit actor.

Every mutation requires an audit reason. Supply `--command-id` for any
operation whose response might be lost, and reuse exactly that ID to recover
the immutable original decision. Never retry an ambiguous mutation with a new
ID.

Production coordinator, worker-custody, bundle, workspace, artifact, snapshot,
and admin-socket roots and files are owner-only. Treat a group/world-accessible
replacement, symlink, or special file as a security incident; do not weaken
modes to make an operation proceed.

A forced start bypasses ordinary ordering and timing only. Dependencies, live
locks, fresh worker identity, route compatibility, and hard draining/closed
quota admission remain authoritative. There is no administrative quota bypass.

### Starting one task from a session

An agent in a session starts one task on the fleet with one call, from the
checkout the work is about:

```sh
t3-steward task run --model claude-haiku-4-5 -- "summarise the open PRs"
t3-steward task run --model t3-primary/opus --prompt-file plan.md --json
t3-steward task run --model opus --fan-out prompts/*.md      # one run, one task per file
```

The CLI derives, the coordinator validates, and the coordinator never chooses a
route. Nothing new crosses the wire: `task run` composes the `projects` query,
`campaign check`, `campaign submit --notify-thread current` and the artifact
verbs, and what it submits is exactly what `campaign submit` would submit from a
directory written by hand.

Derived in this order, each printed in the record:

| Value | Derived from |
| --- | --- |
| project | `--project`, else this checkout's `origin` remote normalised and matched against `t3-steward backlog projects`; exactly one match |
| ref | `--ref`, else the current branch when it has an upstream and is not ahead of it; `--fresh` takes a scratch workspace instead |
| route | `--model INSTANCE/MODEL` exactly, or `--model MODEL` when exactly one advertised instance offers it, else `backlog_v2.coordinator_client.defaults.model`; the quota pool is the one the instance advertises |
| worker | `--worker` pins one; otherwise any eligible worker |
| idempotency key | `--idempotency-key`, else `run-` plus sixteen hex characters of a digest over project, ref, instance, model, the prompts, outputs, verify commands, class and max turns |
| name | `--name`, else the prompt's first line, as a manifest-legal slug |
| notification | the calling thread, as `campaign submit --notify-thread current` resolves it |

Refused, each naming what to pass instead: a remote zero or several projects
match; a model several instances offer; a detached HEAD or a branch ahead of
its upstream ("push first or pass --ref"); more than one prompt source; no
route and no `defaults.model`. A dirty working tree is a warning and not a
refusal: uncommitted changes are not sent, the worker fetches the ref.

A start is refused when no thread resolves, unless `--no-notify` says that a
run nobody will hear about is intended. `check` reports `ready` or
`accepted_waiting` and both are success: the run exists either way.

What the fleet can run right now:

```sh
t3-steward models [--project NAME] [--json]
```

One row per `instance/model`, in the form `--model` takes, with the pool, the
pool's admission state, the phase and used percent of its worst bucket, and how
many of the workers that advertise it are ready. An instance the fleet catalog
authorises that nobody advertises, and one a worker advertises that the catalog
authorises in no pool (`missingBinding`), are listed with that as their status
rather than omitted: a route that cannot run is what the caller most needs to
see.

On the wake, whose first line is the structured trailer
(`t3-steward-wait kind=node outcome=... result="t3-steward task result <run>"`):

```sh
t3-steward task result <run>[/<task>] [--output DIR] [--json]
```

It writes `final-message.md` and every declared output under
`./.t3/results/<run>/<task>/`, each under the name the task declared, and
collects nothing else; the thread archive and the verification records stay
behind `backlog artifacts`. The exit code is the task's own verdict: 0
succeeded, 2 failed or cancelled with whatever exists still written, 1 not
terminal with the progress printed. `--json` inlines the final message.

To stop a run:

```sh
t3-steward campaign cancel <run> --reason TEXT [--json]     # every non-terminal task
t3-steward campaign cancel <run>/<task> --reason TEXT       # one task and its dependents
```

The run form is one command with one revision fence per attempt, which is what
a fan-out run needs: its tasks depend on each other for nothing, so cancelling
one of them cascades to nothing.

A task that declares no route at all is refused as permanent `no-route`, at
`check` and at intake, with the instance/model pairs its project's eligible
workers advertise. The legacy single-task adapter refuses a submission with no
instance and model the same way, which quarantines the file once instead of
reporting it on every cycle.

### Where the logs are

Every role runs in the same per-user unit, `t3-steward.service`: the quota
watchdog, the backlog-v2 coordinator and the worker are all started by
`t3-steward run --config ...` from that unit, and which roles a host plays is
decided by its configuration, not by a separate unit per role. There is no
log file; everything goes to the user journal on the host that runs the role,
so a coordinator question is answered on the coordinator host and a worker
question on the worker host.

```text
journalctl --user -u t3-steward.service -n 200        # last 200 lines
journalctl --user -u t3-steward.service -f            # follow
journalctl --user -u t3-steward.service --since -1h   # last hour
```

The unit generated by `t3-steward install-service` pins
`SyslogIdentifier=t3-steward`, so `journalctl --user -t t3-steward` is
equivalent on such a host. A host whose unit runs a hand-written wrapper
script (one that reads a credential file into the environment and then
`exec`s `t3-steward run`) has no such line: its `SYSLOG_IDENTIFIER` is the
wrapper's name, and `-u t3-steward.service` still works because the unit name
is unchanged. When only the process is known, or the unit name differs, match
on the executable name instead, which is `t3-steward` for every role:

```text
journalctl --user _COMM=t3-steward -n 200
```

Such a wrapper can be retired: the generated unit reads the credential from
the same file itself when it is installed with `--credential-file`, and then
carries `SyslogIdentifier=t3-steward` like every other generated unit, after
which `-t t3-steward` holds on every host. See "Credential files" below for
the exact command and the rollback.

Coordinator startup acquires authority and completes one local reconciliation
pass without contacting workers. Configured SSH worker sessions begin only on a
later scheduled pass. Every pass creates a fresh authenticated session per
worker and reapplies current quota admission immediately before offers and
prepare/dispatch delivery. A failed quota reconstruction is equivalent to no
open pools; observation, stop, and collection remain available.

### Credential files

Every resolver that reads a credential from `T3_STEWARD_CREDENTIAL_<REF>`
(the worker's project credential check, the worker protocol credential and
the coordinator admin credential) also accepts
`T3_STEWARD_CREDENTIAL_<REF>_FILE=<path>`. The inline variable wins when both
are set. The file is read at use, one trailing newline is trimmed, and it is
refused when it is missing, a symbolic link, or readable by others; the
refusal names the variable and the path, never the content. `<REF>` is the
reference with letters and digits upper-cased and everything else replaced by
an underscore, so `secretref:f03-admin/normandy` reads
`T3_STEWARD_CREDENTIAL_SECRETREF_F03_ADMIN_NORMANDY` or its `_FILE` form.

`t3-steward install-service --credential-file REF=PATH` (repeatable) renders
one `Environment=T3_STEWARD_CREDENTIAL_<REF>_FILE=<path>` line per file into
the generated unit, with the home directory written as `%h`. The path is
checked at install time the way the runtime checks it at use, so a unit that
could never resolve its credential is refused before it is written. A
hand-written wrapper that exported the value from a file is retired by
regenerating the unit with the file named instead, for example on a
coordinator host whose wrapper read
`$HOME/.config/t3-steward/f02/protocol-credential.json`. Write the path with
`$HOME`, not `~`: `install-service` does not expand a tilde, and neither does
systemd or `sh` when the command runs over `ssh`. Before restarting, confirm
that the `--config` path the wrapper passed to `t3-steward run` is the one
`install-service` bakes into `ExecStart` (the default is
`$XDG_CONFIG_HOME/t3-steward/config.yaml`), or pass the same `--config` to
`install-service`; and confirm the credential file is a regular 0600 file and
not a symlink, which `install-service` and the resolver both refuse.

```text
cp ~/.config/systemd/user/t3-steward.service \
   ~/.config/systemd/user/t3-steward.service.rollback-<date>
t3-steward install-service --force \
   --credential-file F02_PROTOCOL=$HOME/.config/t3-steward/f02/protocol-credential.json
systemctl --user daemon-reload
systemctl --user restart t3-steward.service
t3-steward coordinator identity        # the coordinator answers under the new unit
```

The wrapper script itself is left in place until the restarted service has
been seen serving; rolling back is copying the retained unit file back over
the generated one, `daemon-reload` and a restart. The retirement is a live
change on the host and is done with approval, not by a pull.

### Reloading the coordinator

The coordinator re-reads its configuration file on SIGHUP and replaces its
configuration-bound services under the same acquired authority. Only the
`backlog_v2` catalog and policy are reloadable: projects, workers, setup
profiles, quota pools, leases, freshness, message limits and scheduling. The
coordinator identity, epochs, storage roots and the host watchdog and T3
settings are lifecycle operations and need a restart; a file that changes one
of them is rejected whole. A worker epoch change is rejected too, because it
is a custody recovery, not a reload.

The coordinator owns the verdict, and it writes it down. For every signal it
writes `<state dir>/coordinator/reload-receipt.json`, which is
`~/.local/state/t3-steward/coordinator/reload-receipt.json` under the default
state path, atomically (a temporary file renamed into place) and before the
log line that reports the same outcome. The receipt carries `requestedAt`,
`completedAt`, `outcome`, `error`, `configurationDigest` (the digest in
effect after the request), `previousDigest`, `release` and, on a rejection,
`blockers`. The outcomes are:

- `accepted`: the new configuration is active. The receipt is written once
  the new services are ready, so a reader that finds it knows the reload is
  done, not merely under way. If the activation fails, the prior
  configuration is restored and the receipt says `rejected` with the
  activation error.
- `unchanged`: the file's digest equals the effective one. Nothing is
  replaced and no service restarts.
- `rejected`: the effective configuration and its digest are exactly what
  they were, and `configurationDigest` equals `previousDigest` to say so.

A receipt is never older than the one before it. The same record is carried
in the status query's runtime block as `lastReloadReceipt` (`lastReload`
stays the activation time, as every earlier release reports it), so
`t3-steward coordinator identity --json` shows the last verdict from any
admin host, and the text form prints it as `reload <outcome> at <time>
(<digest>)` with the error and blockers of a rejection. On the coordinator
host, `t3-steward coordinator reload [--json] [--wait DURATION]` sends the
signal (through the pid file the coordinator writes beside the receipt),
waits up to `--wait` (default 10s) for a receipt requested at or after the
signal and prints it; it exits 0 for `accepted` and `unchanged`, 8 for
`rejected` and 6 when no receipt arrived in time. UpKeeper's
`steward-fleet-configuration` component reads the same receipt after it
signals a changed catalog, and fails its convergence on a rejection or on no
receipt.

What blocks a reload is a catalog change on a worker that still holds a
retained assignment, meaning one that is neither `completed` nor `released`,
at any phase, including a paused attempt and a parked one. A worker whose
catalog revision does not change never blocks. Only `scheduling`, the quota
pool definitions outside a worker's entry, a drain (`accept_backlog: false`)
and a change to another worker's entry or projects reload with work in
flight. The `leases`, `freshness`, `message_limits` and `transport` settings
are part of every connected worker's catalog revision, so a change to any of
them is refused, exactly like a project change, while any connected worker
holds a retained assignment; the receipt's `blockers` name the assignments
that kept it. The refusal names every blocking assignment with its
worker, its attempt, the attempt's progress and control, the phase the worker
last reported and the action that unblocks it:

- a running attempt: let it settle, or cancel its task with
  `t3-steward backlog cancel <run>/<task> --reason TEXT`;
- a paused attempt: wait for the pause to lift and the attempt to settle, or
  cancel it;
- a parked attempt (`waiting-external`): wait for its task wait to settle and
  the attempt to finish, or cancel it;
- otherwise, drain the worker and let the assignment settle.

After the blockers are gone, signal again with `coordinator reload`; the
receipt for the new signal is the one that counts. A touched worker is
offered new work only after it re-enrolls at the new revision
(`t3-steward worker enroll <worker> --current-catalog --reason TEXT`);
enrollment stays an explicit operator action.

### Campaign supervision

A supervised campaign is decided through two operations on this same transport.
`supervision-show` is read-only and returns the run's supervision record, its
gates, its holds and its open review incidents. `supervision-decision` carries
the five mutations: accepting or rejecting a gate, placing a hold, releasing a
hold, escalating an incident and resolving one. They are two operation words
rather than one so that a key may be pinned to reading supervision without also
granting the authority to decide it.

Every decision is structured and nothing is inferred from prose. A gate
decision names the gate, the outcome, the evidence snapshot it reviewed, the
graph revision and the gate revision it expects, the activation epoch it acts
under, an idempotency request key and a reason. The reason is recorded as audit
evidence and grants nothing: a request missing any of the structured fields is
refused however it is worded, and a supervisor process that merely exits
successfully has accepted nothing.

An ambiguous response is recovered by repeating the request with the same
request key. The same key carrying the same decision replays the original
answer; the same key carrying a different decision is refused rather than
applied. Version 1 resolves one named incident at a time, and a request naming
several is refused.

Refusals carry a class, so an agent branches on it instead of reading prose:
`stale-evidence` (a lost race against a newer snapshot, revision or epoch),
`unauthorized-scope` (outside the capability's run, epoch or action list),
`unmet-prerequisite` (the state machine has the transition but a precondition
is not satisfied), `temporarily-unavailable` (retry this one) and
`malformed-request`.

#### Deciding a supervised run from the command line

The operator-facing surface is the `campaign supervision` family, which reaches
the coordinator over the same transport as every other campaign command. Its
full flag contract is in `t3-steward campaign supervision --help`; what matters
operationally is the shape.

```sh
t3-steward campaign supervision show <run> --json
t3-steward campaign supervision decide <run> --gate ID --accept \
    --evidence SNAPSHOT --expected-revision N --graph-revision N \
    --request-id KEY --reason TEXT
t3-steward campaign supervision hold <run> --scope run \
    --expected-revision N --request-id KEY --reason TEXT
t3-steward campaign supervision release <run> --hold ID \
    --expected-revision N --request-id KEY --reason TEXT
t3-steward campaign supervision escalate <run> --incident ID \
    --expected-revision N --request-id KEY --reason TEXT
t3-steward campaign supervision resolve <run> --incident ID \
    --outcome conclude-failure --expected-revision N --request-id KEY --reason TEXT
t3-steward campaign supervision reassess <run> \
    --expected-revision N --request-id KEY --reason TEXT
```

Read every revision from `show --json` before acting on it. `--expected-revision`
is the revision of the record the verb targets: the gate's revision for `decide`,
the supervision record's revision for `hold` and `release`, and the incident's
revision for `escalate` and `resolve`. `--request-id` is the idempotency key, so
an ambiguous answer is recovered by repeating the identical command rather than
by composing a new one. `--reason` is recorded as evidence and grants nothing.

An operator may always act. A supervisor additionally passes `--activation EPOCH`
to name the epoch it holds authority under; a decision naming a stale epoch, or
one whose activation lease has expired, is refused rather than applied late.

#### The worker capability

A worker executes an overseer activation only if its durable inventory
advertises `campaign-supervision-v1`. The capability describes the build running
on that host, so only the worker can report it, and the coordinator reads it from
the worker's published snapshot inventory rather than from the handshake.

Three boundaries enforce it, in this order. Placement does not consider a worker
that lacks the capability a candidate for an activation. `campaign check` reports
`impossible` when no eligible worker advertises it, so a supervised campaign is
refused at submission rather than stalling after admission. The worker exchange
withholds the offer as a last boundary before the package crosses the wire, which
covers a worker downgraded after the plan was committed; withholding is not
cancelling, and the same activation is offered again once the worker advertises
the capability.

To find out whether the fleet can run a supervised campaign before submitting
one, ask the coordinator:

```sh
t3-steward campaign check docs/examples/campaign/supervised-three-node --json
```

The check is read-only and creates nothing. A fleet one release behind
advertises `git`, `huyang`, `preflight` and `task-wait-collection-fence-v1` and
no `campaign-supervision-v1`, so the check reports `impossible` and `submit`
refuses. Upgrade the workers first. Ordinary unsupervised campaigns are
unaffected and require no capability.

#### The supervisor admin client, and what happens without one

A supervised campaign needs exactly one deployment-wide admin client declared as
the supervisor. Declare it on the coordinator, in
`backlog_v2.coordinator.admin_clients`, as a single entry with `supervisor: true`
and a `secretref:f03-admin/<name>` credential:

```yaml
backlog_v2:
  coordinator:
    admin_clients:
      supervisor:normandy:
        credential: secretref:f03-admin/supervisor-normandy
        supervisor: true
```

Exactly one, because the coordinator records that principal on every activation
it dispatches and authorizes supervision against those activations. Two
supervisor entries would make the identity an overseer authenticates as
ambiguous, so the coordinator names the ambiguity and dispatches nothing rather
than silently choosing one.

One supervisor client serves any number of concurrent runs. The capability is
resolved per activation, not per principal: on each request the coordinator asks
whether this principal holds a live activation on the run the request names, and
a live activation on some other run is neither authority here nor a reason to
refuse here. So two supervised campaigns reviewed at the same time each get their
own overseer under the same admin client, and each decides only its own run.

The secret that reference resolves to must exist, as an owner-only 0600 file
under `~/.config/upkeeper/secrets/f03-admin/<name>`, on the coordinator host
**and on every worker host that may run an overseer**. The coordinator needs it
to verify the frames the overseer signs; the worker host needs it because the
overseer's CLI presents that client rather than the host's own admin client. The
admin client name and the `clientPrincipal` inside the secret bundle must be the
same string, because the coordinator selects the configured credential by the
sender name on the frame.

The overseer does not have to be told any of this. The coordinator records the
credential reference on the activation it dispatches, the worker starts the
overseer's thread with `T3_STEWARD_SUPERVISOR_CREDENTIAL` and
`T3_STEWARD_SUPERVISOR_CLIENT` set from it, and `t3-steward campaign supervision`
picks them up. An operator deciding a gate by hand from a host that is not the
coordinator passes `--supervisor-credential secretref:f03-admin/<name>` instead.
The override applies to the `campaign supervision` verbs and to no other command,
and it is inert on the coordinator's own socket, which already authorizes its
peer by UID.

**When there is no supervisor entry**, nothing silently waits. The coordinator
warns at startup and once per affected run; the supervisor route is reported as
unavailable, so the planner withholds every gate-protected task with a
`supervision-route-unavailable` blocker rather than one that reads as a gate
merely pending; `t3-steward campaign explain <run>/<task>` names that blocker and
the missing configuration; `t3-steward campaign supervision show <run>` prints
`no overseer can be dispatched: no supervisor client configured; operator
decision required`; the run's open review incident is escalated with the same
reason; and `t3-steward campaign check` reports a supervised campaign as
`impossible` with the permanent reason `supervisor-client-missing`, so one is
refused at submission instead of stalling after admission.

A run already accepted in that state is not stuck. Every gate can still be
decided by an operator, through the same `campaign supervision` verbs, without
`--activation`.

#### When an overseer ends its turn without deciding

An activation whose turn finishes with no decision recorded is not an
acceptance and not a failure; it is a review that produced nothing. The
coordinator deliberately starts no replacement on the same evidence, because
repeating a review that already declined to decide is a spin that spends the
run's bounded activations without anything new to decide about.

What it does instead is escalate, once: the run's open review incident moves to
`escalated` with the reason `overseer activation <id> ended without a decision`,
and, when the campaign asked for a notification and named a destination, one
delivery intent is queued for the notify thread. `t3-steward campaign
supervision show <run>` and `t3-steward campaign explain <run>/<task>` both say
that the run waits for an operator, and name the command that re-arms it.

That command is:

```sh
t3-steward campaign supervision reassess <run> \
  --expected-revision N --request-id KEY --reason TEXT
```

`--expected-revision` is the supervision record's revision, which `campaign
supervision show <run> --json` prints. Reassessment records the
operator-reassessment trigger, and the next coordinator boundary raises the
epoch and dispatches a fresh activation there, spending one of the run's
declared activations. Only an operator may ask for it: an overseer re-arming
itself is exactly the automatic spin above. Once the run's activation budget is
exhausted, reassess is refused and the run escalates instead; raising the budget
is a separate decision.

#### What the supervisor capability is, and what it is not

A campaign overseer authenticates as an ordinary admin client and is then
granted the `supervisor` role instead of `remote-admin`, by naming it in the
coordinator's own configuration. The role is bound, server-side, to one
activation, which is a pair of a run and an activation epoch: on every request
the coordinator looks up the live activation it dispatched to this principal on
the run the request names, and compares the epoch the request names against that
activation's. A request that names no run is refused outright, and a principal
holding activations on several runs at once is authorized separately on each. A supervisor may read that run's workflow, graph,
tasks, explanations, events and artifacts, and may act on that run's gates and
holds. It may not issue any coordinator command kind at all, which means it
cannot skip a task, mark one successful, retry, pause, cancel, or start work
around quota admission; it cannot modify verification, amend a graph, enroll a
worker, alter routes, clear an operator's hold, or read another run's artifacts
through the ordinary admin APIs, because an artifact read is resolved to its
run before it is authorized. An operator may take over, which revokes the old
activation so that a decision formed under it can no longer be applied late.

**What is enforced, and what is not.** The run-and-epoch scope above is
enforced by the coordinator, on every request, on both carriers. Credential
placement is not a boundary. The admin credential is an owner-only 0600 file
under `~/.config/upkeeper/secrets/f03-admin/<client>`, and campaign tasks run
as full-access agent sessions under the same user on the same host, so any
campaign task on a host that also holds an admin credential can read that file
and present itself as that client. This is a known limitation of the current
deployment and it is stated here rather than worked around: supervision
credentials are **not** isolated from worker-task environments.

What that exposure amounts to in practice is bounded by the server-side scope.
A task that reads a supervisor credential gains the authority to decide its own
run's gates while an activation of that run is live, which is wrong but is not
a cross-run compromise; between activations the epoch fence makes the
credential useless, and it never confers any authority over another run.

Two deployments remove the exposure rather than bounding it: run supervision on
a host that executes no campaign tasks, or run campaign tasks under contained
execution, which passes only the task identity environment into the process and
drops every credential reference. Contained execution is currently an operator
qualification path and not what ordinary tasks take. Until one of those holds,
do not describe supervision credentials as isolated.

### Wait kinds

Every wait has a kind, and the kind decides which side settles it.

| Kind | Registration | Settled by | Trailer pairs |
| --- | --- | --- | --- |
| `shell` | `-- <command>` | the registering host's wait runner, from the command's exit: 0 met, 2 gave up, else not yet | `exit=` |
| `time` | `--at RFC3339` or `--for DURATION` | the registering host's wait runner, from the clock; the poll interval follows the remaining time, never under 30 s | `at=` |
| `github` | `--github run <id> \| pr <n> [--state completed\|merged\|reviewed\|checks-passed] [--repo owner/name]` | the registering host's wait runner, from `gh run view <id> --json status,conclusion,url` or `gh pr view <n> --json state,mergedAt,reviewDecision,statusCheckRollup,url`; three consecutive `gh` errors give up with the last error, a target that is gone gives up at once | `target=run:<id>\|pr:<n> state= conclusion= url=` |
| `node` | `--node <run>[/<task>] [--state terminal\|succeeded\|paused\|waiting-external\|active]` | the coordinator's node settlement pass (`SettleNodeWaits`), from its own records and the workers' last reports; no local check anywhere | `run= task= attempt= revision= progress=` plus `control=`, `pauseReason=` and, for a terminal run, `failed=<comma list>` and `result="t3-steward task result <run>"` |
| `quota` | `--quota <pool> --below N \| --phase normal \| --reset` | the same pass, from the merged bucket observations (the coordinator's own and every worker's, freshest per bucket), which is what admission is derived from | `pool= phase= percent=` |

The local kinds work as task-bound waits through the existing registration:
the coordinator holds the kind, name, condition text and deadline and parks
the attempt; the worker keeps the local check row. A task-bound coordinator
kind is a task wait with a structured condition (`node` or `quota` on the
record) and no local row on any host; registration is refused when the target
or pool is unknown, when the condition already holds, and when a task names
its own run's sink (the run cannot settle while the attempt is parked).

Outcomes are `met`, `failed`, `gave-up`, `cancelled` and `timed-out`.
`--state terminal` (the default, and what a campaign notification waits for)
is `met` on any terminal progress except cancelled, which is `cancelled`; read
`progress=` and `failed=` for what happened. `--state succeeded` is `failed`
on a failed run. `--or-timeout` makes the deadline a normal outcome for every
kind: the outcome is still `timed-out`, the trailer adds `or-timeout=true`,
the result reads as exit 0, and the coordinator records no expiry
contradiction.

The first line of every wake message, every kind, interactive and task-bound,
is the trailer:

```text
t3-steward-wait kind=<kind> outcome=<outcome> wait=<id> <key>=<value> ...
```

The three leading pairs come first; the rest are in no promised order. A value
with a space is quoted; unknown keys are to be ignored; a wake that carries
several waits (a `--wake all` group, a task's all set) names the earliest one
and adds `count=`. A blank line and the prose follow. `wait list --json` and
the task wake context carry `kind` and `outcome` too.

Composition: `--group NAME --wake all` wakes once when every member has
settled, for the local kinds through the local runner and for the coordinator
kinds through the node runner (one message, the earliest member sends). A
group, and a task's `--wake all` set, is all local kinds or all coordinator
kinds: a registration that would mix the two is refused, naming both members.

Cancelling a task whose attempt is parked on a live task wait settles the
wait as `cancelled` in the same command application; the worker cancels its
check row on its next reconcile (it lists the coordinator's waits at most
once a minute while it holds a live bound row). The settlement pass also
settles any live task wait whose attempt is already terminal as `cancelled`
("the attempt ended (<progress>) while the wait was live"), so an attempt
ended by any path leaves no wait live past the next boundary tick.

Version skew: deploy the coordinator before the hosts that register waits.
A plain shell `--task current` wait from a newer host keeps working against
an older coordinator, because its registration carries no new field. A
`time`, `github`, `node` or `quota` wait, and `--or-timeout` on any kind,
is refused by an older coordinator with `invalid operation envelope: json:
unknown field ...`; nothing is parked on a wait the coordinator cannot
settle. An older worker against this coordinator keeps working; the wakes
it composes carry no trailer and it does not reconcile cancelled waits.

### Administering the coordinator from another host

A host that is not the coordinator reaches it through the restricted
`coordinator-exchange` command over SSH, not through a shell. Do not document,
script or teach `ssh <coordinator> t3-steward ...`: that makes the coordinator's
owner account a remote shell for anyone holding the key, which is the authority
story this transport exists to remove.

On the client host, configure the coordinator client in either the configuration
file, which always wins, or the UpKeeper-owned
`~/.config/t3-steward/coordinator-client.json` (mode 0600, `schema_version` 1):

```yaml
backlog_v2:
  coordinator_client:
    coordinator_id: normandy-coordinator
    address: normandy              # ssh destination or alias
    connection: ssh
    remote_command: t3-steward
    credential: secretref:f03-admin/omarchy-pc
    request_timeout: 30s
    message_limits: {max_bytes: 4194304, max_artifact_bytes: 1073741824}
```

On the coordinator host, list the client and the credential reference the
coordinator verifies it against:

```yaml
backlog_v2:
  coordinator:
    id: normandy-coordinator
    admin_clients:
      admin:omarchy-pc:
        credential: secretref:f03-admin/omarchy-pc
```

The credential value itself lives in the UpKeeper secret store on both hosts and
is resolved at use. Never place it in configuration, a manifest, an artifact or a
diagnosis bundle. Admin references (`secretref:f03-admin/...`) and worker
references (`secretref:f02-protocol/...`) are refused in each other's place.

The operator installs the `authorized_keys` entries. They are documented here and
never generated: writing another account's `authorized_keys` is an operator
decision.

Install one line per client. The forced command names no operation, and the
coordinator takes the operation from the signed request frame:

```text
restrict,command="/home/igor/.local/bin/t3-steward coordinator-exchange --config /home/igor/.config/t3-steward/config.yaml" ssh-ed25519 AAAA... omarchy-pc-admin
```

The flag follows the command word. Global flags are parsed after the command, so
a line written as `t3-steward --config PATH coordinator-exchange` exits with
`unknown command "--config"` and the client sees an unusable endpoint.

One line per client is what an ordinary agent workflow needs. `campaign submit`
performs two operations, the readiness query it runs first and the submission
itself, and a coordinator client declares one ssh destination and one identity,
so it presents the same key for both. A line that pinned an operation would
confine that client to one of them.

The operation is authenticated either way. It travels inside the signed frame,
covered by the payload digest, so the coordinator reads it from evidence the
client's own credential vouched for rather than from a word on a command line.
What authority the client then has is decided by the `remote-admin` role, which
refuses worker enrollment and any command kind outside its allowlist, on this
carrier and on the local one alike.

To narrow a key further, pin the operation as the last word. The key then serves
that operation only and the coordinator refuses a frame naming another:

```text
restrict,command="/home/igor/.local/bin/t3-steward coordinator-exchange --config /home/igor/.config/t3-steward/config.yaml query" ssh-ed25519 AAAA... monitor-admin-query
```

This is the tighter arrangement for a client with one job, such as a monitor that
only ever reads. A client that needs several operations needs one key and one
`~/.ssh/config` alias per operation, and its `address` can name only one of them,
so pin an operation only when the client genuinely performs exactly that one.

The operations are `query`, `mutation`, `artifact`, `submission`,
`schedule-definition`, `unknown-recovery`, `node-wait`, `graph-amendment`,
`worker-enrollment`, `quarantine-release`, `supervision-show` and
`supervision-decision`. Pinning `worker-enrollment` achieves nothing: the
`remote-admin` role is refused that operation whatever the key allows, because
enrollment binds a worker to the coordinator's own identity and epoch and stays
an operator action performed on the coordinator itself.

`restrict` disables port forwarding, agent forwarding, PTY allocation and X11.
The `--config` path in the command is the operator's choice and is what supplies
the coordinator identity and the accepted clients; the command never reads
`SSH_ORIGINAL_COMMAND`.

Check the result before submitting anything:

```text
t3-steward coordinator identity --json
```

It reports the coordinator id, owner, release, configuration digest, epoch,
health and the carrier that answered. Exit codes for every coordinator command
are 0 answered, 3 client configuration, 4 authentication, 5 unavailable, 6
timeout, 7 protocol, 8 refused by the coordinator, and 1 for anything else.

A submission whose response is lost is retried with the same `--idempotency-key`.
Exactly one run results. The guarantee comes from the submission service, which
holds the key, the content digest and the result durably and refuses the same key
carrying different content. The transport keeps its own short-lived copy of the
answer so that the common retry costs nothing; that copy is a shield rather than
the guarantee, and a coordinator-exchange process killed between the effect and
its cache write will re-execute the operation, which the service then recognises
as the same submission.

The coordinator's CLI and its daemon must be the same build. The admin socket
refuses a frame carrying fields it does not know, so an older CLI talking to a
newer coordinator fails to decode the coordinator's error responses and reports a
decode failure in place of the real refusal. The symptom is a `protocol` class
and exit 7 with a message about an unknown field, on a command that ought to have
reported something specific. Upgrade both together; UpKeeper already converges
them as one unit.

## Recovery procedures

### Coordinator restart or lost response

1. Stop new admission.
2. Reopen the coordinator store and reload nonterminal assignments, worker
   snapshots, quota admissions, pending commands, leases, and throttle intents.
3. Reconcile every assignment with its recorded worker epoch, assignment epoch,
   deterministic T3 thread ID, dispatch token, and workspace before planning.
4. Redeliver only durable pending commands with the same identity.
5. Leave unproven execution `unknown`; never reassign it until the prior
   execution is proven stopped.
6. Query a submitted admin command by its original `--command-id` rather than
   constructing a replacement.

### Paused or drained work

A checkpoint acknowledgement moves an attempt to `paused` and records the
checkpoint artifact. A forced stop without one becomes
`paused-uncheckpointed`. Preserve assignment, thread, workspace, worker
epoch, provider route, and the assignment's durable remaining-cost estimate.
The coordinator reconstructs that reservation before each admission transition;
missing or contradictory estimates fail reconciliation closed. Resume only the
same thread and route after admission is recovering/open and every apply-time fence
still matches. A cancel command suppresses automatic resume.

### Cancelling a task-bound wait

A task-bound wait is two records: the coordinator's `TaskWait`, which parks the
attempt in `waiting-external`, and the local check on the worker that runs the
shell condition and settles it. `t3-steward wait list` on the worker shows the
local check with the coordinator id it is bound to; `diagnose --json` on any
admin host lists the run's task waits under `taskWaits`.

Cancel through either id: `t3-steward wait cancel w-tw-<id>` (the local check)
or `t3-steward wait cancel tw-<id>` (the coordinator wait). Both settle the
coordinator wait as cancelled first and mark the local check afterwards; the
attempt resumes on the coordinator's next tick with the cancellation as its
wait outcome, and the resumed turn is told so. A wait that already had an
outcome keeps it and the command says which outcome stands. If the coordinator
cannot be reached the local check is left untouched and the command fails with
the transport exit code; retry rather than assuming the wait is gone.

Answering in the task's thread does not cancel the wait; the attempt stays
parked until the condition settles, the wait is cancelled or its deadline
passes.

### Quota pause on the worker host

A thread the worker created for an attempt belongs to that attempt until the
attempt is terminal on the worker or its assignment is released. The quota
watchdog on the same host reads that ownership from the worker's journal on
every tick and leaves owned threads alone: no warn, drain, stop or resume, and
any resume intent for an owned thread is cancelled with
`thread owned by steward attempt <id>`, and an intent recorded before this
release for a thread whose attempt has since settled is cancelled with
`thread belonged to a settled steward attempt <id>`. Ownership lasts only while the
assignment lease in the journal is unexpired (a lease-less record, one hour
since its last update): a crashed or stopped worker stops renewing, and its
threads are the watchdog's again once the leases lapse. The watchdog logs
`thread ownership ignored for stale attempt records` once when that happens.
See [ADR H6](architecture/adr-h6-attempt-owned-threads.md).

When the watchdog's bucket for the attempt's route is draining or stopped, the
worker itself drains or stops the thread through the throttle path and reports
the attempt as `paused` with the bucket as the reason. A stopped bucket sends
the drain notice first and stops the thread only when it is still working
`policy.stop_verify_timeout` later, so a session that checkpoints on request
is never interrupted. The reason reads, for example,
`claudeAgent/claude/seven_day at 97%` in `backlog task show` evidence. Nothing
is collected while the pause is in force, so the attempt does not fail with
"provider session is not ready". The worker resumes the thread when the bucket
has recovered under the watchdog's resume rules and the attempt is still
claimed under a live lease with no stop command; a cancelled attempt is never
resumed, its stop command ends the session. A worker host without a watchdog
state database applies no local pause.

Workers that advertise `quota-observations-v1` report their host's bucket
observations on the snapshot exchange, and admission closes at the pool's
stop threshold on the freshest reading from any host.

To stop a session nothing owns any more, on the host that runs it:

```text
t3-steward thread stop <thread-id>            # thread.turn.interrupt
t3-steward thread stop <thread-id> --session  # also thread.session.stop
```

### Recovering a stopped bucket

A bucket's phase (`normal`, `warned`, `draining`, `stopped`) for one window
epoch lives in the host's state database and belongs to the host watchdog's
policy engine. Readings only come from running turns, so on a host whose
threads are all paused attempts nothing produces one, and before this release
a stopped bucket stayed stopped until the reset however the policy changed.
There are now exactly three writers of a phase, and each leaves an action
record with its evidence, visible in `t3-steward status` and
`t3-steward bucket list`:

1. The engine, from a fresh provider reading or a drain grace timer, as
   before. Only a reading raises a phase.
2. Load-time re-derivation. When the watchdog starts it recomputes, for every
   stored bucket whose window has not passed, the phase the loaded
   `warn_percent`, `drain_percent` and `stop_percent` (with overrides) would
   produce from the stored percentage. A stored phase above that is lowered,
   with `StoppedAt` and the drain deadline cleared and the recovery time set
   when the result is `normal`, and one `rearm` action is recorded naming the
   stored phase, the new phase and both threshold sets, for example
   `re-derived at load: codex/codex/primary at 44% is normal under thresholds
   warn/drain/stop 85/90/95, not stopped as stored under 40/42/44`. A phase is
   never raised at load, no reading is invented, and if the save fails the
   stored phase is kept and an ERROR is logged. So the recovery for a policy
   that was tightened and then restored is a restart of the watchdog.
3. An operator rearm, for the cases the load rule does not cover, such as a
   provider that extended the quota mid-window:

   ```text
   t3-steward bucket list [--json]
   t3-steward bucket rearm <key> --reason TEXT [--force] [--json]
   ```

   `bucket list` prints every bucket with its phase, used percent, when it
   was observed, when it resets, when it recovered, when it was stopped, when
   it was last probed, the thresholds it was derived under and its last rearm
   with the reason. `bucket rearm` sets the phase to `normal` with the
   recovery time now, clears the stop and drain bookkeeping, keeps the stored
   percentage, and records a `rearm` action carrying `user@host`, the reason
   and the phase before and after; it prints the state before and after. The
   key is what `bucket list` and `status` print, for example
   `claudeAgent/claude/five_hour`; an unknown key is refused with the known
   keys listed. A rearm is refused when the stored percentage is at or above
   `stop_percent`, because the next reading would stop the bucket again at
   once; `--force` overrides that. A store failure leaves the stored phase and
   is reported.

The worker's resume rule treats an operator rearm exactly as a confirmed
recovery, and its percentage rules apply to a rearm exactly as they apply
after a real reset: the recovery time is newer than the pause, so a paused
owned attempt resumes on the first reconcile after `resume.reset_settle_delay`,
without a reading, once the stored percentage is below `resume.below_percent`
and below `policy.warn_percent`. A rearm at or above either value only reopens
the bucket for the next reading; nothing resumes until a reading below both
lands. `bucket rearm` computes that condition from the loaded configuration and
prints one line after the before and after state, either `paused attempts on
this bucket may resume after <settle delay>` or `paused attempts will not
resume until a reading below <N>% (resume.below_percent) and <M>%
(policy.warn_percent) lands; the rearm still reopens the bucket`; the `--json`
document carries `resumeEligible` and, when blocked, `resumeBlockedBy` with the
two thresholds. Neither writer guarantees that the provider accepts new turns;
the next reading rearms or re-stops the bucket honestly, and a re-stop is not a
defect. A bucket with no reset time keeps the consecutive-low-readings rearm.
Known limitation: a rearm or probe racing a reading in the same millisecond
can be overwritten by that reading; the action record still shows the rearm.

The probe rule. When a bucket is `stopped` below the current `stop_percent`
(a burn-rate stop, for instance), its stored reading is older than
`resume.probe_after_reset`, the worker can list the host's threads and none
that is running matches the bucket, the worker resumes one paused attempt per
bucket epoch as a probe to obtain the reading nothing else would produce. The
probe is recorded on the bucket (`probedAt` in `bucket list`) and as a
`resume` action naming it; the phase does not change. While the probe's
reading is outstanding the worker does not pause that thread again; the
reading that arrives rearms or re-stops the bucket, and no second probe is
made in the same epoch. A worker whose thread list is unknown never probes.

The interactive override. A thread the user resumed or started by hand after a
watchdog stop, that is, whose latest user message is newer than the bucket's
stop by more than the ten-second tolerance that covers the watchdog's own
messages, is recorded in the bucket's thread notices as `user-resumed` with
that message time. For the rest of that window epoch the watchdog neither
stops nor drains it by any path: not the stopped-phase poll, not a reading
that raises the phase, not the drain grace timer, not `stop_new_sessions`. It
receives the advisory warning at most once and gets no resume intent. The
record is evidence, not a change of phase: a rearm clears the notices with
the epoch, and a stop in the next window holds the thread again until the
user acts. The load-time re-derivation keeps the epoch, so it keeps the
record and the warn notices too. Threads a live steward attempt owns are excluded before this rule
applies; they are the worker's.

### Parked attempt with no live wait

An attempt stays `waiting-external` when its task-bound wait was cancelled
from the thread or settled without a wake reaching it. `resume` and `retry`
are refused from that state, and the refusal names what is allowed:

```text
t3-steward backlog rewake <run>/<task> --reason TEXT [--json]
```

`rewake` applies the transition a settled wait's wake applies (active,
resuming, same assignment and thread). It is refused while a wait is still
live for the attempt, naming the wait; cancel that wait or let it settle. It
is also refused when the execution that parked the attempt is gone; cancel
and retry then.

A recovery command issued right after another one can meet a revision the
coordinator's own tick has moved. Without `--expected-revision` the CLI
re-reads the target once, resubmits under a new command id and says which
revision it used. With `--expected-revision N`, or an explicit `--command-id`,
the stale-revision rejection is returned as it is.

### Schedule overlap or failure hold

A schedule has at most one open workflow run. Duplicate occurrences are
retained as suppressed triggers, not new runs. For `after_failure: hold`, inspect
the failed run and explicitly retry, skip, cancel, or otherwise resolve it
before expecting another run. Do not delete `active_run_id` or trigger rows by
hand.

### Artifact failure

Treat a missing, wrong-size, or checksum-mismatched coordinator artifact as a
recovery fault. Do not release dependencies or substitute worker-local content.
Restore the matching database and artifact snapshot together, then verify the
artifact through `backlog artifact get`.

### Unknown assignment

An expired lease or ambiguous dispatch remains `unknown`; it is not permission
to retry. First close admission and reconcile the recorded worker, worker epoch,
assignment epoch, deterministic thread ID, workspace, and provider. Preserve the
reviewed observation outside coordinator state and calculate its SHA-256. After
the evidence proves either that the old execution stopped or that it must be
terminally failed, use the authenticated local admin socket:

```text
t3-steward backlog recover <assignment> --outcome stopped|failed \
  --coordinator-epoch N --assignment-epoch N --attempt-revision N \
  --evidence-id ID --evidence-sha256 HEX --reason TEXT \
  [--recovery-id ID] [--json]
```

Record and reuse `--recovery-id` if the response can be lost. The coordinator
binds the kernel-authenticated operator identity and current time, verifies all
three revision/epoch fences, and returns the immutable original decision for an
exact replay. Changed replay is rejected. `stopped` releases the assignment and
returns the attempt to ready/unassigned; `failed` releases it and makes the
attempt terminal. Neither outcome opens quota, starts, resumes, claims, or
dispatches work. Admission remains closed until the ordinary quota bridge and
fresh worker reconciliation permit work again.

## Backup

The SQLite database and coordinator-owned artifacts are one consistency unit.
Before any candidate migration:

1. Close admission and let running work checkpoint or finish.
2. Stop the old coordinator cleanly; confirm no process can write the database
   or artifact root.
3. Record the old binary version and checksum, configuration, service unit,
   schema version, active assignments, deterministic thread IDs, worker epochs,
   and quota admission states.
4. Ensure SQLite has checkpointed cleanly. A nonempty `-wal` or `-shm` sibling
   is refused rather than copied as an ambiguous recovery point.
5. Create and verify an owner-only snapshot with the configured coordinator
   roots:

   ```sh
   t3-steward backlog backup create /absolute/new/snapshot-directory
   t3-steward backlog backup verify /absolute/snapshot-directory
   ```

6. Keep the snapshot immutable and retain its manifest with the old binary and
   configuration. The manifest binds its format and schema versions, exact file
   set, sizes, and SHA-256 values. Verification rejects missing, added,
   corrupted, symlinked, special, mismatched-schema, and newer-format content.

Snapshot create and restore compare canonical paths through symlinks and refuse
any database, artifact, snapshot, or restore-target overlap, including aliases.

The command takes the same nonblocking coordinator ownership lock as the daemon
and fails if a coordinator holds it. Copying only `state.db`, or backing up the
database and artifacts at different logical times, is not a valid recovery
point.

Test restoration into absent disposable targets before relying on a snapshot:

```sh
t3-steward backlog backup restore /absolute/snapshot-directory
```

Restore refuses existing state or artifact destinations. It verifies and stages
the whole recovery unit before publishing either target, then opens the restored
database read-only for integrity and exact schema checks. Point the disposable
test configuration at absent destinations; never test restore over live roots.

Campaign supervision needs no change to this procedure and no additional step.
The snapshot copies the whole stopped database file, so the nine tables schema
18 adds are captured by construction: the supervision record, its activations,
gates, holds, incidents, the append-only decision history, the wake outbox, the
event inbox and the idempotency receipts. There is no table enumeration to keep
in step with the schema, and a supervised run restored from a snapshot comes
back with its accepted gates still accepted, its holds still held, its open
incidents still withholding settlement and its request keys still answering
replays with the original answer.

## Rollback and point of no return

Before the new coordinator dispatches or resumes any assignment, rollback is:

1. stop the candidate;
2. restore the old binary and configuration, then use the verified snapshot to
   restore the matching database and artifact tree into absent targets;
3. start the old service with admission closed;
4. verify schema/version compatibility and coordinator views before reopening
   admission.

The operational point of no return is the first candidate-created or
candidate-resumed external execution. After that point, restoring the old
snapshot alone can duplicate work or lose externally produced artifacts.
Rollback then requires reconciliation: close admission, inventory all workers
and deterministic thread IDs, prove every execution stopped or completed,
capture artifacts and Git state, classify ambiguous side effects, and only then
restore or construct a forward repair. Tasks with external side effects require
task-specific idempotency or manual verification.

Never use deletion of the state database as backlog-v2 rollback; it discards
assignment identity, schedule singleton state, reservations, audit events, and
resume intent needed to prevent duplicate execution.

The worker journal (`journal.json` under the worker state root) is versioned by
its `version` header, and within a version a binary ignores record fields it
does not know, so a rolled-back worker binary opens a journal a newer one
wrote. It drops the unknown fields on its next write: a local quota pause
(`localThrottle`) recorded by this release is then lost, and the older binary
collects the stopped thread as finished. Let paused attempts resume or drain,
or cancel them, before rolling a worker back.

### Rolling back the campaign supervision migration (schema 18)

The coordinator schema is forward-only. There is no reverse migration from 18 to
17, and none is planned: the store has no reverse-migration mechanism at all,
and `Migrate` refuses to open a database whose recorded version is newer than
the binary rather than degrading it. An older binary started against a migrated
database therefore fails to start with
`state database schema version 18 is newer than supported version 17`, which is
the intended behaviour and not a fault to work around.

Rolling back a coordinator that has already migrated to 18 means restoring the
coherent stopped backup taken before the migration, with the old binary and its
configuration, using the procedure above. The consequences have to be stated
plainly rather than discovered during the rollback.

**The loss window is everything the coordinator recorded between the backup and
the moment the candidate was stopped.** That is not limited to supervision
state. It includes every workflow run, task attempt, assignment, artifact
metadata record, admin command, schedule trigger and quota observation written
in that interval, because the restore replaces the whole database and the whole
artifact tree as one consistency unit. Supervision state written in that window
is lost along with the rest: a gate accepted after the backup is unaccepted
again, a hold placed after it is gone, an open review incident disappears, and
an idempotency receipt written after it no longer suppresses a repeated request.

Because of that last point, a supervision request whose answer was lost with the
receipt may be applied a second time if the client repeats it after the
rollback. Treat every supervision request key issued after the backup as
unanswered, and reconcile the decisions actually recorded in the restored
database against what the overseer and the operators believe they decided,
before reopening admission.

The operational point of no return above applies unchanged, and one supervision
specific case is worth naming: once a gate acceptance has released a protected
task and that task has dispatched, restoring a pre-acceptance backup does not
un-dispatch the work. Inventory the workers, prove what executed, and repair
forward rather than assuming the restored gate state describes the world.

If a rollback to 17 is needed but the backup is older than is acceptable, the
supported answer is to stay on the newer binary and fix forward. Downgrading the
binary against a migrated database is never the answer, and copying a schema 18
database to a 17 host produces the refusal above rather than a working
coordinator.

## Deployment order after a future GO decision

Host-wide deployment requires explicit user approval and all blockers in the
readiness report resolved.

The completed S19 no-effects qualification is not permission to perform steps
3–10. It installed nothing, opened no live state, and dispatched no T3 thread.
The operator must obtain explicit user approval for host-wide deployment.

1. Freeze submissions and close coordinator admission, including ordinary admin
   starts.
2. Capture and verify the coherent backup described above.
3. Install the coordinator candidate without starting its planner or worker
   delivery.
4. Migrate a disposable copy first, then the real coordinator database exactly
   once; verify schema 11 and record counts.
5. Load and validate project catalog, setup profiles, quota-pool mappings,
   artifact root, and worker identities without dispatch.
6. Upgrade/register workers one at a time. Verify epochs, protocol-version and
   limit negotiation, SSH host/login identity, envelope signing identity,
   restricted remote command, capabilities, provider/model inventory, T3 URL,
   credential names, and clock freshness.
7. Reconcile all pre-existing/nonterminal work while admission remains closed.
8. Enable transport in observe-only mode and prove lease, command replay,
   deterministic dispatch lookup, throttle acknowledgement, and artifact
   transfer.
9. Open one quota pool and one non-side-effecting canary route. Exercise submit,
   dependency release, verification, pause/checkpoint/resume, and schedule
   suppression.
10. Reopen pools gradually. Keep the previous binary and coherent backup until
    the rollback window closes.

Coordinator schema migration precedes worker execution. Worker contact and
dispatch are deliberately excluded from this release-candidate session.
