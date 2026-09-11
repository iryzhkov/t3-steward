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

`cmd/t3-steward/main.go:cmdRun` selects mutually exclusive legacy,
backlog-v2 coordinator, or backlog-v2 worker configuration. Backlog-v2 remains
disabled by default. Coordinator mode explicitly migrates its database,
acquires exclusive file-backed ownership, advances the durable epoch, and
serves bounded local administration with admission closed. The owner-only Unix
socket authenticates the kernel peer UID and replaces any claimed principal
before query, mutation, artifact access, native archive submission, or
revision-fenced schedule-definition administration. A bounded local cycle
immediately and periodically reconciles persistent schedule
occurrences, executes pending revision-fenced admin commands, and scans the
owner-controlled legacy Markdown drop with aggregate byte/file bounds and
immutable idempotency. Accepted `t3-backlog`, `t3-job`, scheduled, and native
bundle work remains queued without dispatch because this startup path does not
construct a worker transport or T3 client. Worker mode does not acquire
coordinator authority.

The fixed `worker-exchange` command requires an explicit operator-controlled
configuration path and accepts exactly one of `control`, `artifact-receive`,
or `artifact-send`. It loads strict local YAML without general environment
overrides, binds the configured worker and coordinator epochs, resolves only
named credential references, and constructs the S16 restart-safe worker
service. Control exchanges persist session sequence, pending request identity,
and exact signed responses under an interprocess lock; a crash resumes the same
request through the idempotent worker journal. Artifact streams use separate
signed metadata plus raw size/checksum-bounded bytes and durable custody.

Legacy mode retains the existing quota watchdog and optional Markdown
`backlog.Runner`. Coordinator-mode admin CLI commands connect to the
authenticated local socket and never open SQLite. They submit or query durable
commands and create or revise schedule definitions; only the coordinator owns
state and artifact access.

The production coordinator now exposes authenticated native bundle/archive
submission and revision-fenced schedule-definition administration, and composes
restart-derived schedule firing, durable admin command execution, quota
reconciliation from stored provider observations, and deterministic planning.
Each quota pass returns numeric per-bucket planning windows with conservative
interactive forecast, safety, active, paused, and committed accounting. Planning
reloads DAGs and worker snapshots, derives difficulty-based cold route estimates,
applies hard quota admission, reconstructs resource/checkout ownership, and
atomically persists offered assignments with deterministic future T3 thread
identities. Replay-stable execution-package assembly now resolves the exact
durable assignment/attempt/task/run/workflow, worker-scoped catalog, prompt,
static inputs, dependency outputs, route, limits, and artifact metadata.
Coordinator worker declarations now carry the worker epoch, and the executable
can construct a fresh mutually authenticated SSH protocol session plus its
worker-scoped package builder. New-work offers and pending prepare/dispatch
commands pass a final fail-closed quota check immediately before transport;
stop and collection commands remain available while admission is closed. The
immediate startup cycle remains local-only; later timer cycles schedule fresh
worker sessions and may derive durable prepare/dispatch commands.
Each scheduled worker exchange now expires elapsed leases before observation,
renews only live durably claimed assignments that the same worker still reports,
and refuses to revive an expired unknown execution from a running observation.
Durable quota/admin throttle intent is replayed per worker, new directives are
delivered, checkpoint deadlines escalate, and eligible paused attempts resume
through the authenticated session. A transport-neutral coordinator importer now
validates completed-assignment custody, ordered verification evidence, declared
outputs, final status, and immutable payload bytes before publishing artifacts
and projecting a replay-safe success or failure outcome. Upload discovery and
raw-stream fetching for completed results now run through distinct fixed
`control` and `artifact-send` SSH operations; the worker retains acknowledged
custody metadata and stops rediscovering an import only after the coordinator
commits it. The same bounded sequence imports checkpoint bytes only when they
match an already acknowledged throttle projection before allowing the worker to
retire discovery.

## State and ownership

| State | Authoritative representation | Owner / mutator | Copies and freshness | Recovery |
| --- | --- | --- | --- | --- |
| Shipped configuration | Strict YAML plus environment/flags; worker endpoint uses local YAML only | Operator; `config.Load` serves general commands and `config.LoadFile` isolates worker authority | Process-local immutable config; worker ID and epochs are explicit | Reload on restart; unknown fields or durable epoch mismatch fail startup |
| Workflow definition | `domain.Workflow` plus immutable bundle files | Coordinator ingestion | Worker receives only materialized inputs | Restore SQLite and bundle storage together |
| Workflow-run progress | `domain.WorkflowRun` in coordinator SQLite | Coordinator transactions | Admin views are derived projections | Reload and reconcile nonterminal attempts |
| Task definition | `domain.Task` in coordinator SQLite | Coordinator ingestion; immutable thereafter | Planner and worker execution package | Rebuild only from retained immutable bundle |
| Attempt progress/control | Revisioned `domain.Attempt` | Coordinator transaction methods | Workers report observations, never authoritative transitions | Optimistic replan/reconcile after stale revisions |
| Assignment and lease | `domain.Assignment` | Coordinator; worker may request an epoch-bound claim | Worker observation is expiring evidence | Expiry becomes `unknown`, not automatic reassignment |
| Worker inventory | Last accepted `domain.WorkerSnapshot` | Worker authors; coordinator validates/persists | `ValidUntil`, worker epoch, coordinator epoch, sequence | New epoch invalidates delayed claims/commands |
| Worker execution/replay | fsync-backed local journal, protocol replay state, custody, and workspace | Fixed worker endpoint | Bound to worker/coordinator/assignment epochs and immutable request IDs | Exact replay returns the signed response; pending requests reconcile idempotently; uncertainty becomes `unknown` |
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

Implemented S17 services add bounded directory and safe-tar ingestion, a durable
idempotency reservation/completion journal, immutable accepted results, legacy
single-task adaptation, authenticated local submission transport, and
composition into the production coordinator.

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

S15 defines protocol version 1 envelopes and typed snapshots, offers, claims,
lease renewals, commands, acknowledgements, observations, structured errors,
and capability negotiation. Each exchange binds sender, recipient, coordinator
epoch, authenticated principal/key, session sequence, request identity,
deadline, payload checksum, and HMAC signature. Principal allowlists authorize
message kinds. Exact completed duplicates return cached signed responses;
changed replay, reordering, stale epochs, excess concurrency, and expired or
oversized messages fail closed.

A content-addressed execution package now carries the immutable assignment,
task, prompt, inputs, route, resolved catalog/environment references,
verification, outputs, deadlines, limits, and idempotency identities required
by `prepare`, `dispatch`, and `collect`. It carries credential names, never
credential values. The restart-safe worker runtime composes the package and
protocol. A coordinator protocol client now serializes typed exchanges, retries
only an identical signed envelope, and abandons an ambiguous session rather than
risking a sequence gap. `FleetCoordinator.ReconcileWorker` persists a fresh
snapshot, delivers only durable offers, validates and commits every claim,
refreshes observed state, then uses persist-before-deliver lifecycle commands.
`CoordinatorOfferBuilder` assembles and validates the content-addressed package
from authoritative records and a matching worker catalog revision; retry time
does not alter package identity. The production coordinator's immediate startup
pass remains local-only. Later scheduled passes construct fresh authenticated
client/builder pairs for configured SSH workers and reconcile them independently.
A failed quota pass supplies an empty final admission policy: observation and
stop/collection remain possible, but offers and prepare/dispatch cannot cross
the transport.

### Worker/T3 boundary

The worker owns calls to its local T3 server. Dispatch uses a deterministic
thread ID and token, observes before creating, and observes again after an
ambiguous create response. A missing response never authorizes a new identity.
Stop, resume, final-message reading, and thread URLs must preserve the bound
worker and provider route.

The S16 `LocalDriver` binds the existing T3 control client directly to the v2
execution package. It observes the deterministic thread identity before create,
re-observes ambiguous creation, and implements stop, warning, checkpoint,
resume, final-message, and thread-archive effects behind the durable worker
journal.

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

The S17 quota bridge maps stored host/provider observations into configured
fleet pools, deduplicates bucket identities, and fails closed for missing,
stale, future, or epoch-conflicting evidence. Quota-first composition into the
coordinator lifecycle is present. Scheduled worker sessions now replay and
deliver persisted warning, drain, hard-stop, and resume commands; new-work
offers and prepare/dispatch commands remain separately fenced by current open
admission.

### Admin boundary

The versioned `BacklogAdmin` service owns query semantics and revision-fenced
mutations. The CLI is an adapter. Every mutation requires a reason; replay uses
the same command ID. A stale command produces a durable rejection. Start/resume
revalidate dependencies, locks, worker freshness, route compatibility, and hard
quota admission inside the apply transaction.

Local query, mutation, artifact access, bundle submission, and schedule-definition
administration use an owner-only Unix socket plus kernel peer-UID authentication;
the server replaces caller-supplied identity. Definition updates are request-
idempotent and revision fenced, and use the authenticated principal as audit actor.
Remote administration and streaming events remain outside the executable.

### Artifact boundary

SQLite metadata and coordinator-owned bytes are one recovery unit. Publication
and opening verify size and SHA-256. Inline terminal rendering is restricted;
downloads are owner-only, no-overwrite, and symlink resistant. A corrupt or
missing artifact blocks dependency release.

S15 defines versioned upload/download manifests and checksum-linked custody
records. Transfer objects bind safe relative paths, size, SHA-256, media type,
assignment epochs, direction, and expiry. Bounded tar validation rejects
traversal, links, devices, duplicates, truncation, and expansion excess.
Coordinator publication remains the authoritative custody transition. Workers
publish restart-safe immutable result/checkpoint outbox manifests; the
coordinator importer verifies a complete result into coordinator ownership and
then commits its terminal outcome. Configured sessions poll one result at a
time, fetch the exact announced object sequence over a separately bounded raw
stream, and acknowledge only after import. Checkpoint outbox import uses the
same sequence and is fenced to the exact paused (or later terminal) attempt plus
its acknowledged throttle metadata.

## Stable primitives

| Primitive | Coherent guarantee |
| --- | --- |
| `ParseManifest` / `LoadManifest` | Strict structural, graph, placement, and path validation |
| `BundleIngester.Ingest` | Immutable copied inputs plus compensated metadata publication |
| `SubmissionService` / schema-11 journal | Bounded directory, safe-tar, and legacy single-task idempotency across restart |
| `ScheduleDefinitionService.Put` | Revision-fenced immutable template history with audited replay |
| `ScheduleTimer.Tick` | Durable-cursor five-field cron, DST, catch-up, and nominal occurrence identity |
| `NewProjectCatalog.Resolve` | Validated logical project to execution environment mapping |
| `NewDAGExecution` and transitions | Dependency, retry, cancellation, skip, and run projection semantics |
| `BuildPlan` | Pure deterministic planning with explanations and private reservation sessions |
| `FleetCoordinator.PlanAndCommit` | Convert proposals into one revision/worker-fenced assignment transaction with deterministic thread identity |
| `workerproto.Client` | Serialized typed requests, identical-envelope retry, and ambiguous-session abandonment |
| `CoordinatorOfferBuilder` | Replay-stable package assembly from exact durable execution and artifact identity |
| `FleetCoordinator.ReconcileWorker` | Snapshot, durable offer, validated claim, refreshed observation, and command reconciliation |
| `Store.ClaimAssignment` | Epoch/lease-bound exclusive claim plus attempt transition |
| `PlanWorkerCommands` | Deterministic next command from durable and observed state |
| `ReconcileWorkerCommands` | Persist-before-deliver and idempotent acknowledgement reconciliation |
| `WorkspacePreparer.Prepare` | Isolated pinned checkout with inputs, dependencies, setup log, and containment |
| `ReconcileAssignmentDispatch` | Observe-before-create deterministic T3 dispatch |
| `AttemptFinalizer.Finalize` | Verification and checksum-captured outputs before success |
| `CoordinatorArtifactStore.Publish/Open` | Coordinator custody with size/hash validation |
| `QuotaBridge.Reconcile` | Deduplicated persistent bucket evidence mapped fail-closed into configured fleet pools |
| `ReconcileQuotaAdmissionTransitions` | Atomic admission projection before throttle intent exposure |
| `ReconcileThrottleDeliveries` | Durable checkpoint/stop delivery and acknowledgement projection |
| `ReconcileTurnOutcomes` | Explicit outcome ordering against active throttle intent |
| `Store.CommitScheduleTrigger` | Occurrence idempotency, one-open-run transaction, and atomic accepted/suppressed audit event |
| submission/quota commit points | Atomic accepted-submission and revisioned quota-admission audit events with replay backfill |
| `BacklogAdmin.Mutate/ExecutePendingCommands` | Audited revision-fenced intent and apply-time safety |
| `backlogadmin.LocalServer/LocalClient` | Owner-UID-authenticated, bounded query/mutation/artifact, streamed bundle submission, and schedule-definition transport without client SQLite access |

The S17 production runtime composes these primitives. Transport handlers and CLI
commands do not reimplement their policy.

## First-class concepts

Already first-class: workflow, workflow run, task, attempt, assignment, worker
snapshot/epoch, provider route, quota pool/admission, reservation, resource
lock, workspace reservation, artifact, schedule template, trigger, throttle
directive/command/acknowledgement, worker command/acknowledgement, turn outcome,
admin command, audit event, submission request/result, verification report,
protocol exchange/session, local admin exchange, execution package, artifact
transfer manifest, and custody record.

Concepts still implicit or incomplete:

- **Coordinator runtime identity:** S14 composes file-backed exclusive ownership,
  durable epoch advancement, and a mandatory closed startup state. It remains a
  single-host authority primitive, not distributed consensus.
- **Production worker protocol binding:** S15 defines versioned exchange,
  execution-package, authentication, replay, limit, artifact-transfer, and
  custody contracts plus a bounded SSH foundation. The restart-safe worker and
  the S17 coordinator lifecycle now bind them.
- **Schedule firing source:** the persistent timer implements nominal-fire
  calculation and durable trigger submission and is now composed into the
  coordinator lifecycle.
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
- Protocol requests authenticate complete envelope and payload identity; exact
  duplicate requests cannot execute their handler twice.
- Execution packages and artifact transfers verify safe paths, sizes, SHA-256,
  epochs, expiry, and aggregate limits before custody changes.
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

- No unleased worker execution: the restart-safe worker candidate enforces
  leases, but it is not installed or connected to a production coordinator.
- Authenticated coordinator-only mutation across hosts is composed, but has not
  been qualified against a fleet host.
- Complete native audit history for every non-admin transition.
- Fleet-wide quota directive delivery is composed behind persisted admission,
  but has not been qualified against a fleet host.
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
admin and worker-protocol JSON goldens, a bounded local multi-process SSH
transport harness, a temporary SQLite end-to-end workflow, repeated suites, and
the race suite.

Evidence limits:

- The complete workflow and worker exchange run only in disposable local child
  processes and temporary storage. No fleet connection or remote artifact
  transfer has been exercised.
- The production `cmdRun` path composes quota-authoritative planning, worker
  exchange and delivery, result/checkpoint import, and terminal projection, but
  has not passed S19 observe-only qualification.
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

Complete in S15. Protocol version 1 defines authenticated, authorized, sequenced,
deadline- and size-bounded envelopes for snapshots, offers, claims, lease
renewals, commands, acknowledgements, observations, capabilities, artifacts,
and structured errors. The content-addressed execution package and artifact
transfer/custody manifests preserve assignment identity and validate paths,
archives, sizes, hashes, and expiry.

The coordinator-initiated SSH foundation is retained: it invokes a restricted
remote command without a local shell, uses strict host authentication, bounds
stdin/stdout/stderr and connect/request time, propagates cancellation, and
retries the same immutable request. The local multi-process spike proves these
transport mechanics without contacting a fleet host. S16 binds the SSH
principal and restricted command to the restart-safe worker runtime; failure to
preserve the contract in deployment requires mutually authenticated HTTP.

Focused gates cover JSON goldens, version/epoch/authentication/authorization,
replay/reorder/duplicate/drop, timeout/cancellation/backpressure/limits,
malformed archives/checksums, and repeated local multi-process transport.

### P3. Worker runtime

Implement worker inventory, offer acceptance/claim, lease renewal, command
execution, workspace/catalog resolution, T3 dispatch observation, throttle
handling, turn outcome collection, verification, artifact upload, and cleanup.
Persist enough worker-local execution state to survive restart without
duplicating T3 or subprocess effects.

Gate: worker restart at every command boundary, containment cancellation, lost
T3 response, stale epoch, corrupt artifact, and unknown-execution tests.

### P4. Coordinator runtime and submission/schedules

Completed in S17: compose ingestion, schedule firing, quota derivation, planning, assignment,
worker exchange, lease expiry, command reconciliation, turn outcomes, admin
execution, artifact custody, and the native audit transitions introduced by the
stage into one bounded loop. Bundle submission and schedule-definition CLI/API
adapters are composed with idempotency. The legacy runner remains selectable and
mutually exclusive with coordinator mode.

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
