# T3 Steward system model

Reviewed in Citadel Track S0, 2026-09-12, against main at `0aed0d8`.
Vocabulary: [CONTEXT.md](../../CONTEXT.md) and the systems-reasoning framework.
This replaces the historical production-binding baseline at `5836493`.
Historical S10–S19 plans remain evidence of that implementation sequence;
the Citadel 2026-09-12 Track S checklist controls new work.

## Purpose, current state and scenarios

The steward protects interactive quota and executes unattended workflows across
a small fleet. One coordinator owns scheduling and durable run state. Each worker
owns its local execution, workspaces and T3 effects. UpKeeper owns deployment and
convergence; the steward reports runtime identity and observations at that seam.

The production executable already composes bundle submission, schedules, planning,
SSH worker exchanges, result/checkpoint import, recovery and admin commands.
Normandy is the only enrolled worker. Version 0.11.0-rc.1 was deployed before S0;
coordinator dry run is on and startup admission is closed. Manual start is a
user-authorized quota override, not evidence that automatic admission is safe.

Successful execution:

1. Submit a strict v2 bundle through the owner-authenticated admin socket.
2. Copy immutable inputs and commit workflow/run/task/attempt identity.
3. Accept fresh worker snapshots and derive quota admission.
4. Plan deterministically; commit an assignment and dispatch identity before delivery.
5. Stage inputs, claim with epochs/lease, prepare the workspace and observe T3 before create.
6. Verify explicit success, transfer checksum-verified outputs to coordinator custody,
   and commit the terminal outcome before releasing descendants/resources.

A lost dispatch response preserves deterministic thread identity. The worker
journal records intent before T3 creation and reconciles observations on retry.
Unavailable evidence is uncertainty, never authority to create a different thread.

A quota error currently has excessive scope: `QuotaBridge.ReconcileState` can
fail on one inconsistent attempt record, and `coordinatorBoundaryCycle.tick`
then defers all planning and pending admin execution. Worker observation and
some stop/collection paths remain available. S1 sink projection runs independently
of quota reconciliation. S6 will isolate buckets and attempts;
that isolation is not yet a current production guarantee.

## State and ownership

| State | Authority and mutator | Copies, freshness and recovery |
| --- | --- | --- |
| Workflow/bundle/task definition | Coordinator ingestion; immutable files and SQLite metadata | Worker execution packages carry pinned inputs; restore metadata and bytes coherently |
| Graph revision | Coordinator amendment transaction | Immutable run-local snapshots and request results; templates and sibling runs are unchanged |
| Run/attempt progress | Coordinator transactions | Revisioned projections; reconcile current worker/T3 evidence |
| Assignment/lease/dispatch identity | Coordinator | Epoch-bound worker claim and expiring observations; expiry alone does not prove stopped execution |
| Worker snapshot | Worker observation accepted by coordinator | Worker/coordinator epoch, sequence, validity; planner and status should use the same accepted rows |
| Worker execution and effects | Worker journal plus T3 read model | Persist-before-effect, deterministic identity, reconciliation after restart |
| Protocol replay | Worker-local SQLite after S0 | Individual signed response rows, byte/age retention, pending identity and session fencing |
| Artifact identity/checksum | Coordinator SQLite | Bytes are in coordinator-owned roots; publish/check before dependency release |
| Artifact staging/custody | Assigned worker until accepted import | Checksums, bounded streams, durable outbox and acknowledgement |
| Quota observations | Provider source/watchdog, persisted in steward state | Account/window/reset identity and observation time; no summing host copies |
| Admission/throttle projection | Coordinator currently derives pool/attempt state | S6 replaces reconstruction with bucket-owned decisions and reservations |
| Catalog/policy | Coordinator effective configuration for persistent workers; legacy YAML remains | Signed, revision-fenced worker projections; validated SIGHUP reload and drain without changing coordinator epoch |
| Admin command/audit | Authenticated coordinator admin service | Immutable request ID, expected revision, actor/reason and durable result |
| Schedule/template/trigger | Coordinator SQLite | Versioned templates, nominal occurrence key, one-open-run policy |
| Wait/check/wake | Coordinator native node rows and legacy shell wait subsystem | S2 native settlement/intent is atomic; observed message identity proves delivery, ambiguous sends retain recovery state |
| Legacy Markdown intake | Existing legacy adapter/runner | Still present; S8 deletes it and its migration/forwarding paths |
| Release/configuration identity | Running process and validated configuration | S4 exposes digests; UpKeeper owns deployment comparison and convergence |

## Boundaries and intentional seams

### Admin versus worker

The owner-only Unix admin socket authenticates kernel peer UID and replaces
caller-supplied identity. CLI clients query and submit audited commands; they do
not open coordinator SQLite. Mutation applies revision and effect-safety fences.

The worker protocol carries authenticated, versioned, sequenced, deadline-bounded
envelopes, immutable execution packages and separately bounded artifact streams.
Worker credentials authorize observations and execution of durable assignments,
not graph mutations, enrollment or arbitrary admin commands.

S4 uses a separate owner-only worker socket carrying signed frames. Enrollment
remains on the coordinator admin socket. Persistent SSH forwards frames to the
existing worker process. Remote persistence is transport, not a new policy owner.

### Persistence and external effects

SQLite transactions locally commit assignment offers/claims, commands,
acknowledgements, schedule triggers, admin outcomes and artifact metadata. Filesystem
publication uses staging/rename and compensation. A transaction does not atomically
commit T3, Git, process containment and coordinator state together.

The execution path is a durable saga:

```text
record coordinator intent -> commit assignment/command
  -> record worker intent -> perform local/T3 effect
  -> report evidence -> verify and publish outputs
  -> commit terminal outcome -> release or retain resources
```

Cancellation compensates an effect; it does not erase it. Main already uses
deterministic T3 settlement tokens and verifies the projection before forgetting
settlement work. Unknown execution and settlement failures retain recovery evidence.

### Quota/watchdog

The watchdog collects provider observations and performs configured interactive
safety behavior. The coordinator reads stored observations to derive backlog
admission, reservations and durable throttle intent. The existing global dry-run
switch crosses quota, backlog, wait-wake and archive effects; S2/S5/S6 separate
these components while preserving current deployment safety settings.

S6's bucket table is the shared authority for raw observations and backlog
decisions. Workers act on directives; they do not reconstruct that table from
per-attempt throttle history. Only affected routes should block under that design.

### Files, workspaces and deployment

Project catalog resolution supplies credential-free repository/setup metadata.
Workers pin source revisions, materialize inputs and run preparation/verification
through the containment seam. Credentials remain host-managed references.
Coordinator artifact metadata/bytes are one backup and recovery unit.

RepositoryCache, ProcessRunner, T3Control, worker transports, store interfaces,
clocks and ID generators are useful existing seams. They isolate real effects,
policy from persistence, and deterministic testing. No second scheduler or generic
plugin abstraction is required for the Track S primitives.

UpKeeper changes binaries and host environment. Catalog reload and reporting of
effective runtime digests belong to the steward in S4. Removing self-update,
manifests, laptop convergence and dev-fleet/omarchy-setup changes remain Track U.

## Stable implemented primitives

| Primitive | Guarantee and limit |
| --- | --- |
| Manifest validation / bundle ingestion | Valid DAG and contained immutable inputs; filesystem/SQL publication uses compensation |
| SubmissionService | Durable submission identity and replay; accepted-record replay currently does not revalidate missing bundle custody |
| DAGExecution | Deterministic task progress, retry, skip/cancel and dependency projection; final sink prevents reopening |
| BindRunSink / ProjectRunSink / CommitWorkflowProjection | Run-local sink, no attempt; aggregate after terminal predecessors and execution containment, fenced transaction and one final audit event |
| BuildPlan / PlanAndCommit | Pure plan plus fenced assignment commit; current quota reconstruction can prevent the plan |
| ClaimAssignment / worker reconciliation | Epoch/lease binding and reconciliation; current recovery code can reclaim an observed live expired assignment |
| Worker command journal / T3 adapter | Durable effect identity and observe-before-create; no universal exactly-once external effect guarantee |
| ProtocolReplayStore | Pending/response SQL commits around the handler, bounded history and exact signed replay |
| Artifact publication/import | Size/checksum custody before success/dependency release |
| Schedule timer/trigger transaction | Nominal occurrence idempotency and one-open-run policy |
| BacklogAdmin / local transport | Owner-authenticated, bounded, revision-fenced control and projections |
| Backup/recovery primitives | Coherent stopped snapshot and evidence-fenced recovery; live restoration/soak evidence remains limited |

The main recovery path differs from the old model's claim that expired leases
can never be revived. `worker_reconcile.go` can accept the same observed live
execution after expiry. S4/S5 must prove identity and absence of conflicting ownership
around that path. It must not become authority to create a replacement execution.

## Track S design contracts (not implemented unless stated)

| Primitive | Decision and stage |
| --- | --- |
| Terminal sink | [Sink ADR](adr-s0-sink.md); coordinator-only aggregation, no attempt; implemented in S1 |
| Node waits / cross-run edges | [Node-wait ADR](adr-s0-node-wait.md); implemented S2: retained targets, atomic outcome/intent, observable delivery and cross-run ordering dependencies |
| Graph revisions/amendment | [Amendment ADR](adr-s0-amendment.md); implemented S3: immutable run snapshots, owner-only amendments, assignment fences, clone provenance and joined diagnostics |
| Worker enrollment/catalog/transport | [Worker ADR](adr-s0-worker-enrollment.md); opt-in persistent workers, signed catalog projection and audited enrollment implemented; S4 live fleet qualification outstanding |
| Bucket quota / bounded forecast / measured costs | [Quota ADR](adr-s0-quota.md); S5 fixtures, S6 implementation and requalification |
| Bounded replay persistence | [Replay ADR](adr-s0-replay-store.md); implemented and tested in S0 |

Submission bytes, prior graph revisions, artifacts and dispatched execution
packages remain immutable. A proposed graph revision changes unassigned definitions
under coordinator authority; it is not an attempt revision. Agents can propose
tasks, but only authorized coordinator amendments create them. Worker credentials
never acquire this authority implicitly. Final sinks do not silently reopen.

## Invariants, failure and evidence

Locally enforced: valid acyclic submission, revisioned attempt changes, fenced
claims, unique schedule occurrence, durable command identity, checked artifact
reads, authenticated envelopes and immutable dispatch/package identities.

Cross-component obligations: no duplicate active execution; preserve thread,
worker, workspace and route on resume; do not release success dependencies before
verification/custody; close admission before exposing throttle effects.
These require recovery evidence and fault testing, not just type definitions.

Eventual properties: worker observations converge coordinator projections, pending
effects settle through replay, and staged/retained resources converge through cleanup.
Unknown or recovery-required states may remain intentionally unresolved.

S0 source review confirms global quota deferral, repeated successful tick logging,
duplicated catalog ownership and the provider tailer's partial-line offset defect.
The reported full-file retailing at every restart is not established by source:
bootstrap resumes matching stored inode/offset and otherwise reads a bounded tail.
S5 must capture positions, inode changes and bootstrap work before calling all
513 MB a proven full replay. Observed startup CPU is still a valid investigation input.

Source at `0aed0d8` had green CI. Existing tests cover planner, migration,
replay, lost responses, admin authentication, artifacts, containment and disposable
multi-process qualification. Foundation live recovery evidence is recorded in
Citadel's foundation-hardening decision, whose missing soak gates were waived.
No claim here upgrades that waiver to 30 clean workflows, a 24-hour soak,
multi-worker production dispatch, accurate quota forecasts or enforced safety.

S0 adds replay migration/retention/crash evidence. S5 owns the reliability campaign;
S6 must rerun quota expectations that S5 cannot certify before rework exists.
S7 owns enforcement and release qualification, including the no-quota-failure log
window. S8 deletes the legacy intake outright; there is no compatibility obligation.

## Operator-facing shape and review rule

Keep workflow administration under `backlog`; add focused top-level `quota`,
`worker`, `wait`, `schedule` and `diagnose` commands. S3 owns DAG inspection
and a joined diagnostic view of workers, quotas, waits, schedules, assignments,
leases and journal excerpts. Redact credentials and bound exported evidence.
Existing `schedules` can be an alias during the CLI change. S8 removes legacy
file helpers; it does not prolong their compatibility.

For each later change state its owner, commit point, retry identity, uncertainty
behavior, resource bound, evidence scope and affected ADR. Keep implemented
contracts separate from the design and from claims still owed by the campaign.
