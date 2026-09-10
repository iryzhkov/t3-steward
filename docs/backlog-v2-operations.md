# Backlog-v2 operations and recovery

This runbook describes the backlog-v2 release-candidate interfaces on branch
`feature/backlog-orchestrator`. It is an operator reference, not deployment
authorization.

## Release-candidate boundary

The release candidate includes the version 2 workflow model and manifest
validation, immutable bundle ingestion, DAG readiness, scheduling and quota
policy, workspace preparation, artifact transfer, coordinator persistence,
worker protocol records, deterministic dispatch identities, throttle
pause/resume, schedule idempotency, and the transport-neutral admin service and
CLI.

The current executable does **not** bind a fleet project catalog, worker
inventory transport, coordinator planning loop, bundle-submission command, or
worker command delivery transport into the production daemon. The existing
Markdown `t3-backlog` runner remains the production path. Do not deploy
backlog-v2 as a fleet coordinator until those bindings exist and the release
gates have been rerun. The detailed decision is in
[the deployment-readiness report](plans/backlog-v2-deployment-readiness.md).

## Configuration inventory

The shipped YAML schema remains the host-local configuration documented in
[config.example.yaml](../config.example.yaml). The current loader ignores unknown
YAML fields; backlog-v2 production configuration must define and test an explicit
strictness/compatibility contract before rollout. Existing backlog settings
control only the Markdown runner:

- `backlog.enabled`, `dir`, `quiet_for`, and forecast fields control local
  task discovery and admission.
- `backlog.host_name` and `default_host` control legacy SSH forwarding; they
  are not backlog-v2 worker registration.
- `policy.*` and `resume.*` remain the active watchdog safety controls.
- `state_path` selects the SQLite database. Opening it automatically migrates
  its schema to version 10.
- `t3.data_dir` resolves the data root used by the admin CLI for retained
  artifact content under `artifacts/`.

Backlog-v2 additionally requires coordinator-owned configuration that is
currently represented by internal typed seams and is not accepted in the
shipped YAML:

- a project catalog: logical name, canonical HTTPS/SSH Git repository, safe
  default ref, T3 project template, setup profile, resource locks, and required
  credential names;
- setup profiles: nonempty command lists and positive timeouts;
- workers: stable worker ID and epoch, accept-backlog policy, capabilities, T3
  web base URL, project/provider/model inventory, and freshness timestamps;
- provider-instance to quota-pool mappings, admission observations, concurrency
  limits, reservation policy, and coordinator artifact/workspace roots;
- authenticated coordinator/worker transport and lease/heartbeat intervals.

Credential names may appear in the catalog, but credential values must remain in
worker-managed secret stores. Repository URLs must not embed credentials.

## Version 2 workflow bundles

A bundle is a directory rooted at `workflow.yaml`. Referenced prompts and
static inputs are copied into coordinator-owned immutable storage at submission;
workers never depend on the submitting host remaining online. See the
[checked example bundle](examples/backlog-v2/workflow.yaml).

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

Use the commands in [Backlog administration](backlog-admin.md) against a copied
or explicitly selected coordinator database. Read views include status,
workflow/task/DAG detail, explanations, events, artifacts, schedules, workers,
quota, reservations, locks, and command outcomes.

Every mutation requires an audit reason. Supply `--command-id` for any
operation whose response might be lost, and reuse exactly that ID to recover
the immutable original decision. Never retry an ambiguous mutation with a new
ID.

A forced start bypasses ordinary ordering and timing only. Dependencies, live
locks, fresh worker identity, route compatibility, and hard draining/closed
quota admission remain authoritative. There is no administrative quota bypass.

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
epoch, provider route, and remaining-cost reservation. Resume only the same
thread and route after admission is recovering/open and every apply-time fence
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
   once; verify schema 10 and record counts.
5. Load and validate project catalog, setup profiles, quota-pool mappings,
   artifact root, and worker identities without dispatch.
6. Upgrade/register workers one at a time. Verify epochs, capabilities,
   provider/model inventory, T3 URL, credential names, and clock freshness.
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
