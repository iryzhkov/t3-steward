# Backlog-v2 system model

Status: architecture baseline for completing production bindings  
Framework: *State, Ownership, Contracts and Failure: A Systems Reasoning Framework*  
Revision baseline: `5836493` on `feature/backlog-orchestrator`

## Purpose and scope

Backlog-v2 schedules unattended T3 work without spending quota reserved for
interactive use. It accepts immutable workflows, chooses a safe worker and
provider route, prepares isolated workspaces, dispatches deterministic T3
threads, verifies results, transfers artifacts, pauses for quota safety, resumes
without losing identity, and exposes audited administration.

This document describes the implemented model and the missing production
bindings. It does not describe the legacy Markdown runner as if it were a fleet
coordinator, and it does not treat an internal interface as evidence that a
production boundary is wired.

## Representative scenarios

### Successful workflow

1. An operator submits a version 2 bundle rooted at `workflow.yaml`.
2. The coordinator validates every reference, copies immutable inputs, publishes
   the bundle, and commits workflow/run/task/attempt/artifact metadata.
3. Workers report epoch-bound, expiring inventory snapshots.
4. Quota observations are reconciled into coordinator admission projections.
5. The pure planner evaluates DAG readiness, timing, placement, routes, quota,
   reservations, locks, and fairness.
6. The coordinator atomically commits an offered assignment against the exact
   attempt revision and worker snapshot.
7. The selected worker claims the assignment with its worker/coordinator epochs
   and lease token.
8. Durable prepare and dispatch commands cross the worker boundary. Preparation
   creates a pinned isolated checkout, materializes inputs/dependencies, and
   records its log.
9. Dispatch first observes the deterministic T3 thread ID. It creates the thread
   only when it is provably absent.
10. Explicit agent success plus successful verification completes the attempt.
    Declared outputs and evidence are checksum-verified and published before
    descendants become ready.
11. The coordinator projects terminal workflow state and releases leases, quota
    slots, locks, and workspace retention policy.

Steps 1–10 exist as tested primitives and a local end-to-end test. They are not
yet composed by the production executable.

### Lost dispatch response

1. The coordinator has already committed the assignment and dispatch identity.
2. The worker marks dispatch `creating` before calling T3.
3. T3 creates the thread, but its response is lost.
4. The worker observes the same deterministic thread ID.
5. If active or stopped, it records that state without creating another thread.
   If observation is unavailable or invalid, it records `unknown` and does not
   reassign the work.

The safety property is at-most-one active assignment with deterministic
dispatch and fail-closed uncertainty. It is not universal exactly-once external
effects.

### Quota drain during execution

1. Provider bucket observations produce a draining/closed quota-pool admission
   transition in coordinator state.
2. Admission closes before a drain command is exposed.
3. The worker receives a durable, epoch-bound throttle command.
4. A checkpoint acknowledgement records its artifact and moves the attempt to
   `paused`; a forced stop without one becomes
   `paused-uncheckpointed`.
5. Resume stays pinned to the same worker, workspace, T3 thread, provider route,
   and quota pool, and occurs only after recovery plus fresh apply-time fences.

## System context

```text
Operator / schedules
        |
        | workflow bundles, admin commands
        v
Authoritative coordinator
  |-- SQLite state and audit projections
  |-- immutable bundle/artifact storage
  |-- quota admission derivation
  |-- deterministic planner
  |-- assignment, lease and command reconciliation
        |
        | versioned authenticated worker exchange
        v
Worker runtime (one per host)
  |-- inventory and execution observations
  |-- project catalog resolution
  |-- Git cache and isolated workspaces
  |-- systemd process scopes
  |-- local T3 adapter
        |
        v
T3 server and provider sessions

Provider observations ---> quota watchdog ---> coordinator admission
```

The coordinator owns scheduling truth. Workers own host-local execution and
report observations. T3 owns actual thread/session state. Provider events own
raw quota observations. Filesystem artifacts hold bytes; SQLite holds their
authoritative identity and checksum metadata.

## Current executable wiring

`cmd/t3-steward/main.go:cmdRun` selects mutually exclusive legacy or
backlog-v2 coordinator composition. Backlog-v2 remains disabled by default.
Coordinator mode explicitly migrates its database, acquires exclusive
file-backed ownership, advances the durable epoch, and waits with admission
closed. This startup path does not construct a worker transport or T3 client.

Legacy mode retains the existing quota watchdog and optional Markdown
`backlog.Runner`. The admin CLI opens existing SQLite without migration and
submits or queries durable commands; it no longer executes coordinator
transitions.

The production coordinator still does not construct `BundleIngester`,
`ProjectCatalog`, `FleetCoordinator`, `WorkspacePreparer`, or worker
exchange. Those bindings remain the next architectural deployment gap.

## State and ownership

| State | Authoritative representation | Owner / mutator | Copies and freshness | Recovery |
| --- | --- | --- | --- | --- |
| Shipped configuration | Strict YAML plus environment/flags | Operator; `config.Load` validates values and references | Process-local immutable config; no revision identity | Reload on restart; unknown fields fail startup |
| Workflow definition | `domain.Workflow` plus immutable bundle files | Coordinator ingestion | Worker receives only materialized inputs | Restore SQLite and bundle storage together |
| Workflow-run progress | `domain.WorkflowRun` in coordinator SQLite | Coordinator transactions | Admin views are derived projections | Reload and reconcile nonterminal attempts |
| Task definition | `domain.Task` in coordinator SQLite | Coordinator ingestion; immutable thereafter | Planner and worker execution package | Rebuild only from retained immutable bundle |
| Attempt progress/control | Revisioned `domain.Attempt` | Coordinator transaction methods | Workers report observations, never authoritative transitions | Optimistic replan/reconcile after stale revisions |
| Assignment and lease | `domain.Assignment` | Coordinator; worker may request an epoch-bound claim | Worker observation is expiring evidence | Expiry becomes `unknown`, not automatic reassignment |
| Worker inventory | Last accepted `domain.WorkerSnapshot` | Worker authors; coordinator validates/persists | `ValidUntil`, worker epoch, coordinator epoch, sequence | New epoch invalidates delayed claims/commands |
| T3 thread state | T3 server | Worker-side T3 adapter | Coordinator stores deterministic ID; worker reports observations | Observe before create; uncertainty is explicit |
| Quota bucket reading | Provider event/log observation | Provider source/watchdog | Timestamp and bucket epoch | Stale or absent readings close/degrade admission |
| Quota admission | Revisioned `QuotaAdmissionRecord` | Coordinator derivation/commit | Planner consumes a snapshot | Re-derive, compare epochs, commit atomically |
| Resource/workspace lock | Coordinator reservation records | Coordinator | Worker may hold OS/filesystem resources underneath | Release only on proven terminal/pause policy |
| Worker workspace | Host filesystem and Git checkout | Assigned worker | Coordinator stores path and execution binding | Retain for pause/unknown; reconcile or clean by policy |
| Artifact metadata | `domain.Artifact` in SQLite | Coordinator artifact catalog | Includes producer, size, media type, path and SHA-256 | Verify bytes before use |
| Artifact bytes | Coordinator-owned filesystem root | Coordinator artifact store | Worker may stage/upload a candidate | Publish by checked staging; restore with SQLite |
| Admin intent/outcome | Revisioned `domain.AdminCommand` plus audit events | Coordinator admin service/store | CLI renders immutable decision | Replay by the same command ID |
| Schedule/template/trigger | Versioned records in SQLite | Coordinator | Timer is a trigger source, not authority | Unique occurrence plus one-open-run reconciliation |
| Legacy Markdown task | File definition plus `backlog_tasks` projection | Legacy runner | T3 thread state is observed directly | Existing local runner rules; separate from v2 |

## Ownership boundaries and contracts

### Submission boundary

Input is a directory with one strict version 2 manifest and referenced files.
`LoadManifest` rejects graph errors, missing paths, traversal, and escaping
symlinks. `BundleIngester.Ingest` copies content, calculates checksums, makes
the staged tree immutable, renames it into coordinator storage, then commits
metadata. A failed metadata commit removes the published tree.

It does not guarantee a single atomic commit across SQLite and the filesystem.
The guarantee is prepare/publish plus compensation before returning failure.

Missing production contract: authenticated caller identity, request size and
file-count limits, duplicate submission/idempotency key, archive ingestion, and
a CLI/API adapter.

### Coordinator persistence boundary

SQLite is the sole authority for coordinator transitions. Store methods fence
coordinator epochs, worker epochs/sequences, attempt revisions, assignment
epochs, dispatch revisions, command identities, and apply-time safety
fingerprints. Callers may plan from snapshots but may not declare their plan
committed.

A successful store commit makes the complete local transition visible. It does
not commit a worker, T3, Git, or artifact side effect atomically.

### Coordinator/worker boundary

Workers must not mutate coordinator SQLite. Messages must be versioned,
authenticated, bounded, replay-safe, and tied to worker/coordinator epochs.
Commands must be durable before delivery; command IDs are idempotency keys.
Acknowledgements are evidence of acceptance, not proof of the final external
effect. Subsequent worker snapshots reconcile actual state.

Existing DTOs define snapshots, claims, lease renewals, commands,
acknowledgements, and observations. The execution package transported with an
offered assignment is not yet defined. A `WorkerCommand` contains identity and
kind but not the workflow/task/environment/prompt/artifact material needed to
execute `prepare`, `dispatch`, or `collect`. Offer discovery and artifact
upload/download envelopes are also missing.

### Worker/T3 boundary

The worker owns calls to its local T3 server. Dispatch uses a deterministic
thread ID and token, observes before creating, and observes again after an
ambiguous create response. A missing response never authorizes a new identity.
Stop, resume, final-message reading, and thread URLs must preserve the bound
worker and provider route.

The existing legacy T3 control adapter can create/resume threads, but no
production adapter implements the v2 `AssignmentDispatchWorker` contract.

### Worker/filesystem/process boundary

The project catalog resolves logical projects to credential-free repository
metadata and named setup profiles. Workspace preparation pins the source
revision, uses independent Git state, materializes immutable inputs and
dependency artifacts, and runs preparation/verification through
`ProcessRunner`. `SystemdScopeRunner` makes cancellation kill the complete
process cgroup.

Worker-managed credentials may enter Git or setup subprocess environments but
must not enter workflow bundles, coordinator logs, snapshots, or artifacts.

### Quota boundary

Raw bucket state comes from the existing watchdog. Backlog-v2 derives shared
quota-pool admission and commits it before generating throttle directives.
Neither required work nor an admin start may bypass draining or closed
admission.

Missing production contract: the runtime adapter that maps host/provider bucket
observations into fleet quota pools, deduplicates shared-account observations,
and feeds admission/throttle reconciliation.

### Admin boundary

The versioned `BacklogAdmin` service owns query semantics and revision-fenced
mutations. The CLI is an adapter. Every mutation requires a reason; replay uses
the same command ID. A stale command produces a durable rejection. Start/resume
revalidate dependencies, locks, worker freshness, route compatibility, and hard
quota admission inside the apply transaction.

Remote authentication/authorization and streaming events are outside the
current executable.

### Artifact boundary

SQLite metadata and coordinator-owned bytes are one recovery unit. Publication
and opening verify size and SHA-256. Inline terminal rendering is restricted;
downloads are owner-only, no-overwrite, and symlink resistant. A corrupt or
missing artifact blocks dependency release.

The worker-to-coordinator transfer protocol and upload authorization remain
undefined.

## Stable primitives

| Primitive | Coherent guarantee |
| --- | --- |
| `ParseManifest` / `LoadManifest` | Strict structural, graph, placement, and path validation |
| `BundleIngester.Ingest` | Immutable copied inputs plus compensated metadata publication |
| `NewProjectCatalog.Resolve` | Validated logical project to execution environment mapping |
| `NewDAGExecution` and transitions | Dependency, retry, cancellation, skip, and run projection semantics |
| `BuildPlan` | Pure deterministic planning with explanations and private reservation sessions |
| `FleetCoordinator.PlanAndCommit` | Convert proposals into one revision/worker-fenced assignment transaction |
| `Store.ClaimAssignment` | Epoch/lease-bound exclusive claim plus attempt transition |
| `PlanWorkerCommands` | Deterministic next command from durable and observed state |
| `ReconcileWorkerCommands` | Persist-before-deliver and idempotent acknowledgement reconciliation |
| `WorkspacePreparer.Prepare` | Isolated pinned checkout with inputs, dependencies, setup log, and containment |
| `ReconcileAssignmentDispatch` | Observe-before-create deterministic T3 dispatch |
| `AttemptFinalizer.Finalize` | Verification and checksum-captured outputs before success |
| `CoordinatorArtifactStore.Publish/Open` | Coordinator custody with size/hash validation |
| `ReconcileQuotaAdmissionTransitions` | Atomic admission projection before throttle intent exposure |
| `ReconcileThrottleDeliveries` | Durable checkpoint/stop delivery and acknowledgement projection |
| `ReconcileTurnOutcomes` | Explicit outcome ordering against active throttle intent |
| `Store.CommitScheduleTrigger` | Occurrence idempotency and one-open-run transaction |
| `BacklogAdmin.Mutate/ExecutePendingCommands` | Audited revision-fenced intent and apply-time safety |

A production runtime should compose these primitives. It should not reimplement
their policy in transport handlers or CLI commands.

## First-class concepts

Already first-class: workflow, workflow run, task, attempt, assignment, worker
snapshot/epoch, provider route, quota pool/admission, reservation, resource
lock, workspace reservation, artifact, schedule template, trigger, throttle
directive/command/acknowledgement, worker command/acknowledgement, turn outcome,
admin command, audit event, and verification report.

Concepts still implicit or incomplete:

- **Coordinator runtime identity:** S14 composes file-backed exclusive ownership,
  durable epoch advancement, and a mandatory closed startup state. It remains a
  single-host authority primitive, not distributed consensus.
- **Worker execution package:** no versioned object gives a worker the exact
  immutable task, environment, prompt, dependency artifacts, verification, and
  retention contract for an assignment.
- **Transport session/exchange:** no request identity, protocol version,
  authentication principal, size limits, or compatibility negotiation.
- **Submission request:** no caller idempotency key or durable submission
  outcome.
- **Artifact transfer:** publication metadata exists, but upload/download
  custody and retry state across hosts do not.
- **Schedule firing source:** schedule records exist, but no production timer
  loop owns nominal-fire calculation and durable trigger submission.
- **Coordinator mode/admission latch:** deployment needs an explicit
  closed/observe/open lifecycle rather than inferring safety from process
  startup.
- **Recovery incident:** `unknown` is represented, but operator reconciliation
  decisions and evidence are not yet a dedicated durable record.

## Intentional seams

- Store interfaces isolate every planning/reconciliation policy from SQLite.
- Clocks and ID generators make time and identity deterministic in tests.
- `PlanningConstraint` isolates policy and per-plan reservation state.
- `WorkerCommandTransport`, `ThrottleWorkerTransport`, and
  `AssignmentDispatchWorker` isolate dangerous remote/T3 effects.
- `RepositoryCache`, `ProcessRunner`, and artifact catalog interfaces isolate
  Git, process containment, and storage.
- `BacklogAdmin.Authorizer` isolates caller policy.
- T3 control interfaces isolate the internal T3 protocol.

These seams are justified. The missing work is to supply production
implementations and one runtime composition root, not to add parallel seams.

## Invariants

### Locally enforced

- A workflow manifest has one supported version, a valid acyclic graph, safe
  references, and possible placement.
- Attempt mutations use optimistic revisions.
- Assignment claims match coordinator, worker, and assignment epochs plus the
  lease token.
- One attempt has at most one assignment record.
- Schedule occurrence keys are unique, and overlap checks share the trigger
  transaction.
- Worker commands are durable before delivery and unique per
  assignment-epoch/kind.
- Admin commands are immutable by ID and revision fenced.
- Artifact reads verify recorded size and checksum.
- Hard quota admission cannot be bypassed by an ordinary admin command.

### Cross-component

- At most one active execution is intended for an assignment identity.
- A worker executes only commands for its current epoch and assignment.
- A dependency releases only after explicit success, verification, and required
  artifact publication.
- Admission closes before workers receive a drain directive.
- Resume preserves worker, route, thread, workspace, and checkpoint identity.

These require the future transport/runtime to preserve the existing fences;
types alone do not enforce them across a network.

### Eventual

- Worker observations converge coordinator projections through reconciliation.
- Lost command and dispatch responses converge through idempotent replay and
  deterministic observation.
- Expired leases become explicit uncertainty.
- Filesystem staging debris and retained workspaces converge through cleanup and
  retention reconciliation.

### Not yet enforceable in production

- No unleased worker execution: there is no production worker runtime.
- Authenticated coordinator-only mutation across hosts: there is no transport.
- Complete native audit history for every non-admin transition.
- Fleet-wide quota deduplication/freshness: derivation exists, runtime feed does
  not.
- Atomic coordinator database/artifact backup: documented operator procedure,
  not an implemented snapshot primitive.

## Transactions and failure semantics

SQLite transactions are local commit points. Important ones include assignment
plan, assignment claim/lease, worker command persistence and acknowledgement,
quota admission/throttle transition, schedule trigger, admin application,
turn-outcome projection, and artifact metadata publication.

Filesystem publication uses staging and rename, with cleanup compensation when
metadata commit fails. Across coordinator, worker, T3, Git, and artifact
storage, backlog-v2 is a durable saga:

```text
record coordinator intent
  -> commit assignment/command
  -> perform worker-local effect
  -> report observation/acknowledgement
  -> reconcile coordinator projection
  -> verify and publish evidence
  -> release or retain resources
```

Cancellation after an external effect is compensation, not rollback. An
unobservable effect becomes `unknown` or recovery-required; it must never be
silently guessed away.

## Evidence and limitations

Strong evidence currently includes schema migration tests, compatibility
fixtures, deterministic planner simulations, optimistic concurrency tests,
lost-response and replay tests, throttle fault tests, artifact path/hash tests,
admin JSON goldens, a temporary SQLite end-to-end workflow, repeated suites,
and the race suite.

Evidence limits:

- The complete workflow runs in one test process with temporary storage.
- No real coordinator/worker transport, authentication, process restart pair, or
  remote artifact transfer has been exercised.
- The production `cmdRun` path constructs only the closed-admission authority
  skeleton; planning and worker exchange are not yet composed.
- No candidate has touched live state or dispatched a real worker.
- Some admin views reconstruct events from current projections because native
  non-admin audit emission is incomplete.

## Production-binding sequence

Each slice must update this model when its contract changes.

### P1. Configuration and runtime identity

Complete in S14. The shipped strict YAML model includes disabled-by-default
mode, coordinator identity, workers, projects, setup profiles, quota pools,
safe storage roots, SSH transport, message limits, freshness, leases,
scheduling, and a mandatory closed startup admission state. Existing legacy
configuration remains compatible and cannot be enabled with coordinator mode.

SQLite opening no longer implies migration. Coordinator startup explicitly
migrates, takes exclusive process ownership, and advances the durable epoch;
read/status/admin opens cannot create or migrate schema. A refused second owner
does not advance authority. Admin compatibility clients submit/query only.
Focused tests cover strictness, references, migration fixtures, restart/refusal,
disabled mode, and closed startup without worker or T3 contact.

### P2. Versioned worker exchange and execution package

Define a versioned authenticated exchange envelope containing worker snapshot,
assignment offers/claims, lease renewals, durable commands/acknowledgements, and
a content-addressed execution package. Define request limits, timeout, retry,
ordering, compatibility, and error semantics. Define artifact transfer and
custody records.

For this small SSH-addressable fleet, first evaluate a coordinator-initiated SSH
exchange adapter: it reuses host authentication, requires no listener, and keeps
workers from opening coordinator SQLite. Reject it if bounded streaming,
cancellation, identity, or artifact custody cannot meet the contract; then use
an authenticated HTTP transport.

Gate: protocol JSON goldens, incompatible-version tests, replay/reorder/drop
fault tests, authentication failure tests, and a bounded transport spike.

### P3. Worker runtime

Implement worker inventory, offer acceptance/claim, lease renewal, command
execution, workspace/catalog resolution, T3 dispatch observation, throttle
handling, turn outcome collection, verification, artifact upload, and cleanup.
Persist enough worker-local execution state to survive restart without
duplicating T3 or subprocess effects.

Gate: worker restart at every command boundary, containment cancellation, lost
T3 response, stale epoch, corrupt artifact, and unknown-execution tests.

### P4. Coordinator runtime and submission/schedules

Compose ingestion, schedule firing, quota derivation, planning, assignment,
worker exchange, lease expiry, command reconciliation, turn outcomes, admin
execution, artifact custody, and audit emission into one bounded loop. Add
bundle submission and schedule-definition CLI/API adapters with idempotency.
Keep the legacy runner selectable and mutually exclusive with coordinator mode.

Gate: multi-process temporary-state workflow, simultaneous submission,
coordinator restart at every commit boundary, schedule catch-up/overlap,
closed-admission start refusal, and no duplicate dispatch.

### P5. Observability, backup, and recovery

Emit native audit events for all state-changing primitives, expose runtime mode
and reconciliation incidents, implement or script a coherent stopped
SQLite/artifact snapshot with verification, and add explicit recovery commands
for unknown assignments. Never provide a quota bypass.

Gate: audit completeness assertions, backup/restore drill, corrupt/incomplete
snapshot refusal, recovery authorization and revision tests.

### P6. Deployment qualification

Run observe-only multi-host integration on disposable roots, then a
non-side-effecting canary. Repeat unit, integration, migration, fault, race,
compatibility, security, and rollback gates. Update the deployment-readiness
decision separately from deployment approval.

Gate: a new GO report with exact evidence and remaining risks. Host-wide
deployment still requires explicit user approval.

## Review checklist for future changes

- Does one owner remain authoritative for every changed state?
- Does transport carry observations and intent without becoming a second policy
  owner?
- Is every new external effect preceded by durable identity and followed by
  reconcilable evidence?
- Are retries tied to the same idempotency key and epochs?
- Does closed/stale/unknown state fail closed?
- Are database and artifact commit points stated honestly?
- Can both the happy path and one partial-failure path be tested end to end?
- Did the change update the contract, invariant, recovery, and evidence sections
  that it affects?
