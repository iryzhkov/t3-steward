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

Coordinator startup acquires authority and completes one local reconciliation
pass without contacting workers. Configured SSH worker sessions begin only on a
later scheduled pass. Every pass creates a fresh authenticated session per
worker and reapplies current quota admission immediately before offers and
prepare/dispatch delivery. A failed quota reconstruction is equivalent to no
open pools; observation, stop, and collection remain available.

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

#### What the supervisor capability is, and what it is not

A campaign overseer authenticates as an ordinary admin client and is then
granted the `supervisor` role instead of `remote-admin`, by naming it in the
coordinator's own configuration. The role is bound, server-side, to one run and
one activation epoch: on every request the coordinator compares the run and
epoch the request names against the activation its own supervision record
currently considers valid. A supervisor may read that run's workflow, graph,
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
