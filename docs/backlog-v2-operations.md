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
durable mutation submission, artifact retrieval, and revision-fenced schedule-
definition administration. It authenticates the Unix
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
ordered verification reports, and the final done marker before coordinator
artifact publication and replay-safe success/failure projection. Scheduled
sessions compose bounded worker transport, lease expiry/renewal, lifecycle
delivery, durable throttle replay/delivery, one-at-a-time result outbox
discovery, bounded raw fetch, import, and post-import acknowledgement for both
results and checkpoints. Checkpoint publication additionally requires an exact
acknowledged throttle projection. The existing Markdown
`t3-backlog` runner remains the deployed production path. Do not deploy
backlog-v2 as a fleet coordinator until the remaining
stages and qualification gates are complete. The detailed decision is in
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
  accepted lease extension at its configured duration.
- `startup_admission` must be `closed` in coordinator mode. Coordinator
  startup acquires exclusive ownership and advances its epoch without
  contacting a worker or T3.
- `state_path` selects coordinator SQLite. Plain opens never create or migrate
  it; coordinator startup uses the explicit migration path.

The forced worker command must be installed with an absolute, operator-owned
configuration path, for example
`t3-steward worker-exchange --config /etc/t3-steward/worker.yaml control`.
Configure a separate forced command for each artifact operation; never forward
`SSH_ORIGINAL_COMMAND` to a shell. The endpoint ignores general configuration
environment overrides and command-line dry-run/log-level overrides. Envelope
and project secrets remain worker-managed named credentials and never enter
workflow bundles, execution packages, journals, or error text.

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
  `inputs_from`, `outputs`, `verify`, placement, routes,
  `resource_locks`, class, `importance`, `difficulty`,
  `estimated_cost`, `max_turns`, `not_before`, `deadline`, and
  `expires_at`.
- Defaults are importance 3, difficulty 3, and max turns 3. Times are RFC 3339.
- `inputs_from` may name only declared output paths from dependency ancestors.
  A dependency releases only after explicit success and successful verification.
- All paths are relative to the bundle or workspace as appropriate. Absolute
  paths, traversal, unsafe globs, missing files, escaping symlinks, cycles,
  duplicate names, impossible placement, and unknown fields are rejected.

Dependency artifacts appear in the successor workspace at
`.t3/dependencies/<task>/<output>`. Declared verification commands, output
capture, checksums, final messages, preparation logs, checkpoints, and other
retained records belong to the coordinator recovery unit.

## Routine administration

Use the commands in [Backlog administration](backlog-admin.md) while the selected
backlog-v2 coordinator is running. The CLI connects to the admin socket derived
from the selected state path; it does not open the database. Read views include
status, workflow/task/DAG detail, explanations, events, artifacts, schedules,
workers, quota, reservations, locks, and command outcomes.

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

A forced start bypasses ordinary ordering and timing only. Dependencies, live
locks, fresh worker identity, route compatibility, and hard draining/closed
quota admission remain authoritative. There is no administrative quota bypass.

Coordinator startup acquires authority and completes one local reconciliation
pass without contacting workers. Configured SSH worker sessions begin only on a
later scheduled pass. Every pass creates a fresh authenticated session per
worker and reapplies current quota admission immediately before offers and
prepare/dispatch delivery. A failed quota reconstruction is equivalent to no
open pools; observation, stop, and collection remain available.

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

## Backup

The SQLite database and coordinator-owned artifacts are one consistency unit.
Before any candidate migration:

1. Close admission and let running work checkpoint or finish.
2. Stop the old coordinator cleanly; confirm no process can write the database
   or artifact root.
3. Record the old binary version and checksum, configuration, service unit,
   schema version, active assignments, deterministic thread IDs, worker epochs,
   and quota admission states.
4. Copy the resolved `state_path` database together with any `-wal` and
   `-shm` siblings, plus the complete coordinator artifact/input/checkpoint
   root, into one timestamped owner-only snapshot.
5. Verify file counts and cryptographic checksums, test-open a copy with the old
   binary or a read-only SQLite tool, and keep the original snapshot immutable.

Copying only `state.db`, or backing up the database and artifacts at different
logical times, is not a valid recovery point.

## Rollback and point of no return

Before the new coordinator dispatches or resumes any assignment, rollback is:

1. stop the candidate;
2. restore the old binary, configuration, database snapshot (including WAL
   state), and matching artifact tree;
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
