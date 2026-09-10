# Backlog orchestrator development plan

Status: approved for implementation and testing on Normandy. Fleet deployment requires separate user approval.

## Goal

Turn the existing host-local Markdown backlog into a quota-aware workflow scheduler for unattended T3 work. It must support DAG workflows, recurring schedules without duplicate runs, alternative workers and model providers, isolated workspaces, durable inputs and artifacts, graceful quota pause and resume, and a complete CLI administration interface.

An unchanged `t3-backlog --project ... --title ... < prompt` command must remain the easiest way to submit one task.

## Scope boundary

This development effort includes implementation, migrations, tests, documentation, and local integration testing on Normandy.

It does not include:

- Installing the development binary as the Normandy system service.
- Changing the live backlog directory or state database format in place.
- Restarting any deployed `t3-steward` daemon.
- Copying binaries or configuration to other hosts.
- Enabling a fleet coordinator in production.
- Building the web dashboard. This release must leave a stable admin interface and data model for it.
- Adding an ordinary admin command that bypasses a closed quota pool.

Stop after producing a tested release candidate and deployment notes. Wait for the user's explicit approval before any host-wide rollout.

## Compatibility promises

1. Existing single Markdown tasks remain valid and behave as one-task workflows.
2. Existing task fields retain their meaning during migration. Deprecated fields produce warnings before removal in a later release.
3. Existing live databases are never opened by development builds during tests. Tests use temporary state and backlog directories.
4. Existing `t3-backlog` callers and gated `t3-job` schedules continue to submit successfully.
5. A migration can be rolled back before a new daemon has dispatched work. Document the point after which rollback needs reconciliation.

## Domain model

Canonical terminology lives in `CONTEXT.md`.

The persistent model consists of:

- `Schedule`: recurring workflow definition and overlap policy.
- `Trigger`: one nominal schedule firing, whether accepted or suppressed.
- `Workflow`: immutable DAG definition and input bundle.
- `WorkflowRun`: one manual or scheduled execution.
- `Task`: one DAG node.
- `Attempt`: one try to finish a task.
- `Assignment`: committed worker and provider route with a lease.
- `Artifact`: immutable input or output with checksum and provenance.
- `AdminCommand`: version-checked and audited mutation request.

Workflow progress and execution control are separate. A task may remain active while its attempt is paused by quota control.

## Submission formats

### Single task

The current command remains unchanged:

```sh
t3-backlog --project "t3-steward development" --title "Review scheduler" < prompt.md
```

The helper packages this as a one-task workflow internally.

### Workflow bundle

A workflow is submitted as a directory or archive:

```text
workflow/
├── workflow.yaml
├── prompts/
│   ├── inspect.md
│   └── implement.md
└── inputs/
    ├── overall-plan.md
    └── findings/
        └── scheduler.md
```

Example manifest:

```yaml
version: 2
name: agent99-improvements
class: required

placement:
  hosts: [homelab, normandy]
  requires: [internet]

environment:
  project: agent99
  type: git
  scope: task
  ref: main

inputs:
  - inputs/overall-plan.md
  - inputs/findings/*.md

routes:
  - instance: codex
    model: gpt-5.6-sol
    options: {effort: high}
  - instance: claudeAgent
    model: claude-opus-5

tasks:
  inspect:
    prompt_file: prompts/inspect.md
    outputs: [findings.md]

  implement:
    needs: [inspect]
    prompt_file: prompts/implement.md
    inputs_from:
      inspect: [findings.md]
    verify:
      - go test ./...
```

Submission must resolve referenced paths relative to the manifest, reject escapes and unsafe symlinks, copy declared inputs, calculate checksums, validate the complete graph, and commit the complete bundle atomically. A worker must never depend on the submitting host remaining online.

## DAG semantics

- A dependency releases only after explicit task success and successful verification.
- Missing status output never releases dependencies for version 2 workflows.
- `continue` starts another turn of the same attempt.
- Retry creates another attempt without rerunning successful ancestors.
- Failure blocks descendants by default.
- Cancellation cancels pending descendants but preserves completed work and artifacts.
- Graph validation rejects cycles, missing nodes, duplicate task names, invalid artifact references, and impossible placement.
- Ready branches may run concurrently only when provider, worker, and resource constraints allow it.
- Project or explicit resource locks prevent concurrent mutation of shared external systems.

## Inputs and artifacts

Static files named by a workflow are copied into its immutable input bundle at submission.

Outputs cross tasks or hosts only when declared as artifacts. Each artifact records its producer, checksum, size, media type, creation time, and storage location. Successors receive dependency artifacts under a predictable `.t3/dependencies/<task>/` path. The scheduler also supplies predecessor status, final summary, and T3 thread link.

Coordinator-owned artifact storage must remain readable when a worker is offline. It retains input documents, declared outputs, checkpoints, preparation logs, final messages, Git state, diffs, commits or bundles, and verification reports according to configurable retention rules.

## Worker placement

Fleet configuration declares worker eligibility and capabilities:

```yaml
workers:
  homelab:
    accept_backlog: true
    capabilities: [always-on, internet, docker]
  normandy:
    accept_backlog: true
    capabilities: [always-on, internet, docker]
  gaming-pc:
    accept_backlog: true
    capabilities: [gpu, internet]
  laptop:
    accept_backlog: false
    capabilities: [mobile]
```

Tasks normally declare capability requirements and an optional eligible-host set. The scheduler chooses a healthy worker. GPU work selects `gaming-pc` through capability matching. The laptop is excluded by fleet policy unless a task explicitly opts in.

Placement and provider choice are independent. The planner constructs eligible provider routes from the worker, installed provider instances, available models, task requirements, and quota pools.

## Project catalog and workspace preparation

A fleet project catalog maps a logical project name to its canonical Git repository, default ref, T3 project template, setup profile, resource locks, and named credential requirements. Secrets stay in host-managed stores and are never copied into workflow bundles.

The default execution environment is a clean clone per task:

```text
~/.local/share/t3-steward/runs/<workflow-run>/<task>/<attempt>/
├── workspace/
├── inputs/
├── dependencies/
└── preparation.log
```

Workers may keep bare repository caches, but every task clone must have an independent object store and Git index. Pin the source commit before execution. Run preparation with a timeout and capture its logs. A preparation failure consumes no model quota and does not release dependencies.

`environment.scope: task` is the default. It allows tasks to move between workers and communicate through artifacts or commits. `scope: workflow` pins all tasks sharing a mutable checkout to one worker and serializes access unless explicitly configured otherwise.

Prototype T3 execution against a steward-created checkout. The T3 `thread.create` protocol already exposes `branch` and `worktreePath`; verify that `worktreePath` accepts the prepared clone before designing a fallback.

Run preparation and task child processes in a user systemd scope or cgroup. A hard stop must terminate leftover builds, tests, tunnels, and helpers. Label externally created resources, including containers, with workflow and attempt identifiers when possible.

## Scheduling classes and quota accounting

There are two task classes:

- `required`: run in the requested window, but never bypass a hard quota closure.
- `surplus`: run only from quota predicted to remain after interactive demand, active work, required reservations, and the safety margin.

Retain compatibility parsing for the old `gate` field. New manifests use `class`.

For each quota pool and window:

```text
available = capacity
          - current usage
          - forecast interactive usage
          - active consumption
          - paused required-work remainder
          - committed reservations
          - safety margin
```

Surplus work uses only the remainder and normally becomes eligible near the applicable weekly reset. It may expire without becoming urgent. Required work gains urgency as its deadline approaches but remains subject to admission safety.

Use route-specific cost and duration estimates. Historical measurements are keyed by task signature and provider route; difficulty remains the cold-start estimate. Do not launch work whose expected runtime plus checkpoint margin exceeds the projected time to drain.

## Provider routing

A task may name ordered provider candidates. Ordered preference is the default. Select the first route that meets capability, worker, quota, duration, and reservation constraints. Add `best-headroom` only after ordered routing is reliable.

Map host-local provider instance identifiers to fleet-wide quota pools. Several workers using one provider account share admission state, concurrency accounting, reservations, and throttle actions. Do not sum duplicate percentage observations from the same account. Reconcile them conservatively using freshness, epoch, and the highest credible severity.

An attempt's route is fixed once its T3 thread starts. A retry may select another route. Cross-provider continuation from an existing thread is outside this release.

## Coordinator and workers

Use one authoritative coordinator rather than peer consensus. The coordinator owns workflows, schedules, planning, assignments, leases, reservations, artifacts, and admin commands. Workers report health, project inventory, provider capabilities, quota observations, active threads, workspaces, and locks.

For each planning cycle, the coordinator:

1. Reconciles worker and T3 state.
2. Computes quota-pool admission states.
3. Finds dependency-ready tasks.
4. Applies time, expiry, placement, route, lock, and quota constraints.
5. Selects a batch and reserves its resources.
6. Writes the complete plan atomically.
7. Lets workers claim assignments with epoch-bound leases.
8. Reconciles preparation, dispatch, pause, completion, and artifacts.

Workers may monitor and stop local work autonomously for safety, but cannot start unleased work. A disconnected worker starts nothing new. Existing work continues or pauses according to its local throttle controller.

Persist the deterministic T3 thread ID and dispatch token before sending `thread.create`. If the response is lost, query T3 for that ID. Never retry with a new thread ID. An unavailable worker leaves its assignment `unknown`; do not reassign until the old execution is proven stopped. External-side-effect tasks require manual takeover when proof is unavailable.

## Throttling integration

Scheduling and throttling share one state model. Each quota pool has a scheduler admission state:

| State | Scheduler behavior |
| --- | --- |
| `open` | Start and resume work after normal planning. |
| `constrained` | Start no surplus work and reject work unlikely to checkpoint before drain. |
| `draining` | Close admission and ask affected tasks to checkpoint. |
| `closed` | Start nothing and hard-stop tasks that missed the drain deadline. |
| `recovering` | Resume paused work gradually before admitting surplus work. |

The coordinator must close admission atomically before workers send drain messages. A fleet throttle directive carries quota pool, bucket epoch, severity, reason, and deadline. Workers acknowledge it and report affected attempts.

At warning, backlog agents avoid beginning long operations. At drain, they leave the workspace consistent, write `.t3/checkpoint.md`, and finish with:

```text
BACKLOG STATUS: paused
CHECKPOINT: .t3/checkpoint.md
```

A compliant attempt becomes `paused` with a confirmed checkpoint. A forced stop becomes `paused-uncheckpointed`. Both retain their workspace, assignment ownership, dependency block, and remaining-cost reservation while releasing their running provider slot.

On recovery, resume the same T3 thread, workspace, worker, and provider route. The resume prompt supplies the checkpoint and throttle reason. Interactive resumptions receive priority over new backlog work. Required paused work normally precedes new required work, except when another deadline is at immediate risk. Valid paused surplus work resumes only when surplus remains; expired surplus occurrences become `skipped` with their artifacts retained.

When reconciling a completed turn, explicit `done` wins and cancels its resume intent. Otherwise an active throttle intent wins over ordinary `continue` or missing status output. This prevents a drained task from being redispatched as a new turn while its quota pool is closed.

## Recurring schedules and duplicate prevention

Every schedule has at most one open workflow run. Open includes queued, blocked, assigned, preparing, running, draining, paused, resuming, and needs-input.

Defaults:

```yaml
overlap: forbid
misfire: skip
```

On every trigger, the coordinator transactionally checks `schedule.active_run_id`. If that run remains open, record a suppressed trigger and create nothing. When the old run finishes, wait for the next future trigger. Never replay missed cycles by default.

Each trigger also has a unique idempotency key composed of schedule ID and nominal fire time. Enforce both occurrence uniqueness and the one-open-run rule in the database. This protects against persistent timer catch-up, process retries, coordinator restarts, and duplicate forwarding.

Manual schedule execution refuses while an open run exists. Replacing or cancelling it requires a separate explicit command. Version schedule templates; an edit affects only future workflow runs.

Support `after_failure: next-cycle` for ordinary reviews and `after_failure: hold` for risky maintenance. A held schedule creates no new run until an operator acknowledges, retries, skips, or cancels the failed run.

During migration, systemd timers may keep invoking `t3-job enqueue`, but submissions carry `schedule_id`, nominal fire time, and coalescing key. Later the coordinator may own recurring triggers directly.

## Verification and resource safety

An explicit agent success marker expresses intent. Declared verification commands determine whether the attempt succeeds. Capture output and exit status as artifacts. Only verified success releases dependencies.

Resource locks cover shared branches, deployment targets, databases, GPUs, and other external systems. Display lock owners and waiters in administration output. Acquire locks in deterministic order and release them on terminal state or confirmed pause policy.

Retries after ambiguous external side effects require task-specific idempotency or manual verification. The scheduler promises at most one active assignment and deterministic T3 dispatch, not universal exactly-once side effects.

## Administration interface

Create a transport-neutral `BacklogAdmin` module. The coordinator implementation owns all reads and mutations. The CLI and future web dashboard are adapters. Neither may edit SQLite or task files directly.

The interface provides workflow, task, schedule, trigger, worker, quota, lock, event, command, and artifact queries. Admin commands use optimistic revisions and an audit reason. A stale command is rejected with current state.

CLI coverage for this release:

```text
t3-steward backlog status
t3-steward backlog list [filters] [--json]
t3-steward backlog show <workflow-run>
t3-steward backlog graph <workflow-run>
t3-steward backlog task show <workflow-run>/<task>
t3-steward backlog events <workflow-run>
t3-steward backlog explain <workflow-run>/<task>
t3-steward backlog artifacts <task>
t3-steward backlog artifact show|get <artifact>
t3-steward backlog start|delay|pause|resume|cancel|retry|skip <task>
t3-steward schedules list|show|history|run|delay-next|enable|disable <schedule>
```

`start` bypasses quiet time, surplus horizon, and ordinary ordering, but not dependencies, resource locks, worker health, or closed quota admission. `pause` requests a checkpoint. `pause --now` hard-interrupts while keeping the attempt resumable. `cancel` prevents automatic resume. `retry` creates a new attempt and may choose another route.

`explain` lists every blocker and the earliest credible start estimate. Status views include workflow DAG progress, task and attempt state, current host and provider, reservations, throttle state, checkpoints, locks, recent events, artifacts, and the T3 thread URL.

The future dashboard uses the same data transfer objects and command semantics. Plan for an HTTP JSON adapter and server-sent events, but do not build the UI in this release. Store a T3 web base URL per worker so thread links are stable. Serve text, Markdown, JSON, and diffs safely; download other artifacts rather than rendering agent-produced HTML.

## Progress and observability

Workflow progress comes from DAG state. Task progress comes from attempts, turns, checkpoints, verification, and optional `.t3/status.json` updates. Missing fine-grained progress must not be treated as failure.

Append an audit event for submission, validation, trigger suppression, planning decision, assignment, lease, preparation, dispatch, throttle transition, checkpoint, resume, verification, artifact capture, admin command, and terminal state. Keep current-state projections for fast CLI and future dashboard queries.

Alert on deadline risk, stale worker or quota state, repeated preparation or verification failure, suppressed schedule triggers, long-lived needs-input, and a paused task that cannot resume before expiry.

Add dry-run planning commands that explain placement, routes, reservations, locks, expected throttle behavior, and duplicate suppression without dispatching anything.

## Persistence and recovery

Use coordinator SQLite transactions for plans, leases, schedule singleton enforcement, and admin commands. Store large artifacts outside SQLite with checksums and transactional metadata. Back up database and artifact storage as one recovery unit.

After coordinator recovery, reconcile every nonterminal assignment with workers and T3 before dispatching new work. Fail closed when worker, quota, or coordinator state is stale. Store times in UTC while preserving each schedule's timezone and defined daylight-saving behavior.

## Implementation milestones

Each milestone should fit one unattended development session where practical. A session may split a milestone when tests or design discoveries demand it, but must leave a committed, tested checkpoint and queue exactly one successor.

### M1. Domain and compatibility foundation

- [x] Add workflow, task, attempt, assignment, schedule, trigger, route, quota-pool, artifact, and admin-command domain types.
- [x] Separate workflow progress from execution control state.
- [x] Add schema migrations and round-trip tests.
- [x] Parse every existing Markdown task as a one-task workflow.
- [x] Freeze compatibility tests for current task parsing, ordering, and CLI submission.

### M2. Workflow bundles and DAG execution

- [x] Define and validate the version 2 manifest.
- [x] Implement atomic bundle ingestion and immutable static inputs.
- [x] Implement dependency readiness, strict completion, failure propagation, retry, cancellation, and graph validation.
- [x] Add declared outputs, dependency artifact transfer, verification commands, and checksums.
- [x] Cover chains, diamonds, cycles, missing artifacts, retry, cancellation, and verification failure.

### M3. Project catalog, placement, and isolated environments

- [x] Add worker inventory and capability matching.
- [x] Add the logical project catalog and named setup profiles.
- [x] Implement clean per-attempt Git environments with pinned revisions and repository caching.
- [x] Prototype and verify T3 `worktreePath` behavior.
- [ ] Add workflow-scoped environments, resource locks, preparation logs, retention, and process containment.
  - [x] Add atomic workflow checkout ownership, worker pinning, deterministic resource reservations, and explicit pause/terminal release policies.
  - [x] Materialize shared workflow checkouts with setup-once preparation, per-attempt dependency views, and explicit terminal retain/remove decisions.
  - [x] Run setup and verification child processes in transient user systemd scopes with whole-cgroup hard stops.
  - [ ] Add interprocess cache locking and crash-orphan retention reconciliation.
- [x] Test alternate workers, GPU-only placement, offline workers, setup failure, and workspace cleanup.

### M4. Planner, task classes, and provider routes

- [ ] Extract a deterministic planner whose input is fleet state plus queued workflows and whose output is a plan with explanations.
- [ ] Implement required and surplus policies, expiry, forecast reservations, remaining-cost accounting, and runtime-versus-drain checks.
- [ ] Add ordered alternative routes, fleet quota pools, route-specific estimates, and concurrency limits.
- [ ] Add fairness and starvation protection without weakening deadlines.
- [ ] Build table-driven simulations for low quota, late-week surplus, competing providers, and stale observations.

### M5. Throttle, checkpoint, and resume integration

- [ ] Derive quota-pool admission states from bucket state and resume intents.
- [ ] Close admission before warning or draining affected work.
- [ ] Implement structured warn, drain, checkpoint, hard-stop, and resume handling.
- [ ] Reconcile completion markers against throttle intents in the documented order.
- [ ] Keep paused work visible to reservations while releasing runtime slots.
- [ ] Add recovery priority and expiry behavior for required and surplus tasks.
- [ ] Test drain races, hard stops, multiple buckets, recovery probes, user interaction, and repeated throttle epochs.

### M6. Schedules and idempotency

- [ ] Add versioned schedule definitions and trigger history.
- [ ] Enforce one open run per schedule transactionally.
- [ ] Add unique occurrence keys, suppressed triggers, misfire behavior, failure holds, and manual schedule runs.
- [ ] Make T3 dispatch idempotent with persisted thread IDs and dispatch tokens.
- [ ] Test simultaneous triggers, persistent timer catch-up, lost responses, restarts, and ambiguous worker loss.

### M7. Fleet coordinator and worker protocol

- [ ] Move authoritative planning and mutation to the coordinator.
- [ ] Add worker snapshots, commands, acknowledgements, leases, epochs, and reconciliation.
- [ ] Prevent dispatch during partitions or stale state.
- [ ] Support centrally retained artifacts and cross-worker dependency transfer.
- [ ] Test concurrent worker claims, coordinator restart, worker reconnect, lease expiry, and no-duplicate guarantees.

### M8. Admin module and CLI

- [ ] Implement transport-neutral admin queries and revision-checked commands.
- [ ] Refactor existing backlog CLI code away from direct database mutation.
- [ ] Implement status, filters, DAG view, task detail, explanations, events, artifacts, controls, and schedule commands.
- [ ] Include worker health, quota admission, reservations, locks, progress, and T3 links.
- [ ] Define versioned JSON data transfer objects suitable for the future dashboard.
- [ ] Test stale commands, asynchronous command outcomes, authorization seam, and JSON stability.

### M9. End-to-end hardening and release candidate

- [ ] Run full unit, integration, race, migration, and fault-injection suites.
- [ ] Exercise a complete local workflow with dependencies, artifacts, verification, pause, resume, retry, and a suppressed recurring trigger.
- [ ] Test compatibility with existing `t3-backlog` and `t3-job` submission formats.
- [ ] Document configuration, manifests, operator recovery, backup, rollback, and deployment order.
- [ ] Produce a release-candidate commit and a deployment-readiness report.
- [ ] Do not deploy. Stop and wait for explicit user approval.

## Session execution contract

Development runs only in `/home/igor/Work/t3-steward` on Normandy, on branch `feature/backlog-orchestrator`. Do not use or modify the laptop checkout or laptop backlog.

At the beginning of every session:

1. Read this plan and `CONTEXT.md` completely.
2. Inspect Git status, recent commits, and the current milestone checklist.
3. Confirm the checkout is on `feature/backlog-orchestrator` and contains no unexplained changes.
4. Select the first incomplete milestone item that forms a coherent tested increment.
5. Reconcile current code before editing. Do not assume the prior final message is complete.

During every session:

- Use the repository's documented development tools and standards.
- Keep interfaces small and put planning policy behind a deterministic planner seam.
- Add tests before or with each behavior change.
- Run targeted tests during development and `go test ./...` before committing.
- Run `go vet ./...` for each completed milestone and the race suite during final hardening.
- Never install the development binary, restart the service, or open the live state database.
- Never push, create a pull request, or deploy unless the user separately authorizes it.

At the end of every nonfinal session:

1. Update this milestone checklist accurately.
2. Write or update `docs/plans/backlog-v2-handoff.md` with completed work, decisions, test results, remaining risks, and the exact next increment.
3. Commit code, tests, plan progress, and handoff together.
4. Confirm the working tree is clean.
5. Queue exactly one successor using `docs/plans/backlog-v2-session-prompt.md` and model `gpt-5.6-sol` on the `codex` instance.
6. End with `BACKLOG STATUS: done`. Do not continue editing after queueing the successor.

At the end of M9, do not queue another session. Commit the readiness report, leave the tree clean, and end with `BACKLOG STATUS: done`, clearly stating that host-wide deployment awaits user approval.

## Definition of done

Implementation is complete when every milestone is checked, all documented test gates pass, compatibility fixtures pass, fault tests demonstrate no duplicate workflow or T3 dispatch, throttle pause and resume preserve work, the CLI explains and controls every supported state, and deployment plus rollback instructions are ready.

No daemon or host has been upgraded as part of satisfying this definition.
