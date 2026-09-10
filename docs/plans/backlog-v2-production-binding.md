# Backlog-v2 production-binding plan

Updated: 2026-09-10
Repository: `/home/igor/Work/t3-steward`
Branch: `feature/backlog-orchestrator`

## Goal

Turn the tested backlog-v2 domain and persistence release candidate into a
production-bound, locally qualified coordinator/worker system while preserving
the legacy backlog path and every hard quota-admission invariant.

This plan resolves the blockers in
`docs/plans/backlog-v2-deployment-readiness.md` and follows the ownership,
boundary, primitive, invariant, and production-binding model in
`docs/architecture/system-model.md`.

Completion of this plan does not authorize installation or deployment. A new
GO readiness report is necessary but still does not replace explicit user
approval for host-wide deployment.

## Fixed decisions

These decisions are part of the implementation contract so unattended stages
do not need to stop for routine design choices.

1. The coordinator process is the sole policy and mutation authority. Worker
   processes report observations and execute durable intent. Admin clients
   submit commands and queries; they never execute coordinator transitions.
2. Backlog-v2 is disabled by default. Legacy and coordinator modes are mutually
   exclusive in one process. Startup begins with admission closed and does not
   contact T3 or a worker until configuration, storage ownership, epoch, quota,
   and freshness checks succeed.
3. Database migration is explicit and coordinator-owned. Read-only/status/admin
   clients must not migrate a database merely by opening it. No development
   command may open the live database.
4. Configuration is strict for shipped fields. Unknown keys, invalid
   combinations, unresolved catalog references, unsafe roots, and incomplete
   credentials fail startup. Existing valid legacy configuration remains
   compatible.
5. Worker exchanges use a versioned envelope and stable idempotency identities.
   Unsupported versions, stale epochs, invalid authentication, oversized
   messages, and incomplete evidence fail closed.
6. Evaluate coordinator-initiated SSH first because this fleet already has host
   identity and does not need worker listeners. Keep it only if the bounded
   transport spike proves streaming limits, cancellation, identity, and
   artifact custody; otherwise implement mutually authenticated HTTP.
7. Execution packages and artifacts are content addressed with SHA-256,
   immutable after publication, size bounded, and verified at every custody
   change.
8. Schedule expressions use a documented five-field cron grammar in an IANA
   timezone. Nominal occurrence time, not observation time, is the idempotency
   identity. DST folds/gaps, catch-up, overlap, and misfire behavior must be
   deterministic and tested.
9. Submission has a caller-provided or generated idempotency key, bounded
   request/archive sizes, safe archive extraction, and an immutable accepted
   result.
10. Unknown execution is a recovery incident, never evidence that work is safe
    to duplicate. No admin or recovery command bypasses closed quota admission.

## Milestones

- [x] R1 — Authority, configuration, and storage lifecycle (S14)
- [x] R2 — Versioned worker exchange and execution package (S15)
- [x] R3 — Restart-safe worker runtime (S16)
- [ ] R4 — Coordinator runtime, submissions, schedules, and quota bridge (S17)
- [ ] R5 — Audit, backup, recovery, and security hardening (S18)
- [ ] R6 — Deployment qualification and new readiness decision (S19)

## Remaining serial stage checklist

This checklist is authoritative. Each unattended thread selects exactly the
first incomplete named stage. It completes the whole stage as one substantial
unit, using additional turns in the same thread while safe in-stage work
remains. It queues exactly one successor only after committing a completed
stage.

### S14 — Authority, configuration, and storage lifecycle

- [x] Add disabled-by-default backlog-v2 runtime configuration for mode,
  coordinator identity, workers, project catalog, setup profiles, quota pools,
  storage roots, transport, message limits, freshness, leases, scheduling, and
  startup admission.
- [x] Reject unknown fields and invalid cross-references while retaining
  fixtures proving current legacy configurations still load.
- [x] Split SQLite opening from migration. Make schema migration an explicit
  coordinator startup/maintenance operation, and prove read/status/admin opens
  cannot alter schema.
- [x] Establish exclusive coordinator ownership and durable epoch advancement.
  A second coordinator must fail closed without advancing authority.
- [x] Remove admin CLI command execution. Until the authenticated admin adapter
  exists, any local compatibility adapter may submit/query only and must neither
  migrate schema nor perform target state transitions.
- [x] Add the runtime mode skeleton to the production composition root, with
  legacy/coordinator mutual exclusion and closed admission. Disabled or closed
  startup must make no worker or T3 contact.
- [x] Update configuration, operations, system-model, and migration-boundary
  documentation.

Exit gates:

- Focused config compatibility/rejection tests.
- SQLite no-implicit-migration and explicit-migration tests from every supported
  schema fixture.
- Coordinator ownership, epoch restart, second-owner refusal, admin
  submission-only, disabled-mode, and closed-startup tests.
- `go test ./...`, `go build ./...`, `go vet ./...`, and
  `git diff --check`.

### S15 — Versioned worker exchange and execution package

- [x] Define stable versioned DTOs for exchange envelopes, snapshots, offers,
  claims, lease renewals, commands, acknowledgements, observations, structured
  errors, and capability negotiation.
- [x] Define the immutable execution package carrying task/prompt data,
  provider route, environment/catalog references, dependency inputs,
  verification, artifact declarations, deadlines, limits, and idempotency
  identities.
- [x] Define authenticated identity, authorization, sequencing, replay,
  retry/backoff, timeout, cancellation, message-size, compatibility, and
  backpressure semantics.
- [x] Define content-addressed artifact upload/download manifests and custody
  records, including safe path and archive rules.
- [x] Implement a bounded local transport spike for coordinator-initiated SSH.
  Retain SSH only if it meets the fixed contract; otherwise implement the
  authenticated HTTP foundation and document the evidence-based decision.
- [x] Keep all spike and integration traffic confined to local disposable
  processes; do not contact fleet workers.

Exit gates:

- Protocol and execution-package JSON goldens.
- Unsupported-version, stale-epoch, authentication, authorization, replay,
  reordering, duplicate, drop, timeout, cancellation, limit, malformed archive,
  and checksum tests.
- A bounded local multi-process transport test.
- `go test ./...`, `go build ./...`, `go vet ./...`, and
  `git diff --check`.

### S16 — Restart-safe worker runtime

- [x] Implement worker inventory and snapshot publication, offer validation,
  assignment claim, lease renewal, durable command receipt, and idempotent
  acknowledgement.
- [x] Resolve catalog/project/setup profiles and prepare isolated attempt or
  workflow workspaces through existing containment primitives.
- [x] Implement the v2 T3 dispatch adapter with observe-before-create recovery,
  deterministic dispatch identity, structured throttle/checkpoint handling,
  verification, turn outcome collection, artifact upload, and cleanup.
- [x] Persist a worker-local journal sufficient to resume after restart without
  duplicating T3 or subprocess effects.
- [x] Fail stale epochs, corrupt packages/artifacts, missing custody evidence,
  lease uncertainty, and unproven execution to explicit closed/unknown states.
- [x] Provide a no-external-effects worker test mode.

Exit gates:

- Worker restart tests at every durable command/effect boundary.
- Lost and ambiguous T3 response, stale epoch, lease loss, corrupt artifact,
  cancellation/whole-cgroup containment, throttling, checkpoint/resume, and
  unknown-execution tests.
- Local coordinator-stub/worker multi-process test on disposable roots.
- `go test ./...`, `go build ./...`, `go vet ./...`, and
  `git diff --check`.

### S17 — Coordinator runtime, submissions, schedules, and quota bridge

- [ ] Compose bundle ingestion, worker exchange, quota derivation, schedule
  firing, planning, atomic assignment, lease expiry, durable command delivery,
  acknowledgement/reconciliation, outcomes, artifact custody, admin execution,
  and recovery into one bounded coordinator lifecycle.
- [ ] Implement bounded, idempotent single-task and bundle submission adapters,
  including safe archive extraction and immutable accepted results.
- [ ] Implement schedule definition administration and a persistent timer source
  using the fixed cron/DST/nominal-occurrence contract.
- [ ] Bridge real quota observations into shared fleet quota-pool state,
  deduplicating observations by pool and closing admission for stale,
  inconsistent, or unavailable evidence.
- [ ] Add authenticated admin query/command transport. CLI clients must stop
  opening coordinator SQLite and remain submission/query clients only.
- [ ] Preserve legacy `t3-backlog` and `t3-job` input compatibility while
  enforcing legacy/coordinator runtime mutual exclusion.
- [ ] Emit native events at coordinator-owned transitions introduced here.

Exit gates:

- Complete local multi-process workflow with dependencies, artifacts,
  verification, pause/resume/retry, and suppressed recurring trigger.
- Simultaneous/replayed submission; unsafe archive; schedule syntax, DST,
  catch-up, overlap, and restart; quota deduplication/staleness; admin auth; and
  legacy/coordinator exclusion tests.
- Coordinator restart fault injection at every persistence/effect boundary,
  closed-admission start refusal, and no-duplicate T3 dispatch assertions.
- `go test ./...`, `go build ./...`, `go vet ./...`, and
  `git diff --check`.

### S18 — Audit, backup, recovery, and security hardening

- [ ] Emit and assert native audit events for every production state-changing
  primitive, including actor, reason, epoch, revision, idempotency identity, and
  outcome.
- [ ] Expose runtime mode, owner/epoch, health, freshness, transport, quota,
  reconciliation, unknown execution, and custody incidents through admin
  projections.
- [ ] Implement a stopped, coherent SQLite-plus-artifact snapshot and verified
  restore flow. Refuse corrupt, incomplete, mismatched, or newer-version
  snapshots.
- [ ] Add authorization- and revision-fenced recovery commands for unknown
  assignments, with evidence-required outcomes and no quota bypass.
- [ ] Harden credentials, secret redaction, file permissions, request limits,
  extraction, timeouts, backpressure, process containment, and error surfaces.
- [ ] Update operator recovery, backup, rollback, and point-of-no-return
  documentation.

Exit gates:

- Audit completeness and redaction assertions.
- Backup/restore drill plus corrupt/incomplete/version-mismatch refusal.
- Recovery authentication, authorization, revision, evidence, replay, and
  closed-quota tests.
- Security/limits/fuzz or property tests appropriate to each parser and
  boundary.
- `go test ./...`, `go build ./...`, `go vet ./...`, and
  `git diff --check`.

### S19 — Deployment qualification and readiness decision

- [ ] Run the complete system as separate coordinator, worker, and client
  processes using disposable configuration, state, artifact, workspace, and
  credential roots.
- [ ] Exercise transport loss/reorder/duplicate, process restarts, clock skew,
  stale quota, schedule catch-up, artifact corruption, backup/restore, rollback,
  and legacy/v2 exclusion without external side effects.
- [ ] Run an observe-only multi-host test and a non-side-effecting canary only
  after explicit authorization identifies the permitted worker hosts and test
  credentials. Never use live state or dispatch a real T3 thread without
  separate explicit authorization.
- [ ] Repeat unit, integration, migration, compatibility, fault-injection,
  security, race, and rollback gates.
- [ ] Update the deployment-readiness report to GO or NO-GO with exact evidence,
  residual risks, rollback boundary, and any remaining authorization needs.
- [ ] Queue no successor. Do not install, restart the service, change live
  configuration/state, or deploy.

Exit gates:

- All authorized qualification tests pass.
- `go test ./...`, `go build ./...`, `go vet ./...`,
  `go test -race ./...`, and `git diff --check`.
- A committed readiness report states a justified GO or NO-GO. A GO is not
  deployment approval.

## Serial execution contract

Development runs only in `/home/igor/Work/t3-steward` on Normandy, on branch
`feature/backlog-orchestrator`. Preserve unexplained changes and never work in
another checkout.

At the start of every stage:

1. Read `CONTEXT.md`, this entire plan,
   `docs/plans/backlog-v2-production-handoff.md`, and every predecessor
   artifact named there.
2. Reconcile branch, status, recent history, milestone/checklist state, code,
   and tests. Never trust a summary over the repository.
3. Select exactly the first incomplete named stage and write its actual starting
   commit and exact exit gates into the handoff before implementation.

During every stage:

- Finish the selected stage as one substantial unit. If it needs another turn,
  update the exact checkpoint and use `BACKLOG STATUS: continue`; do not queue
  a successor.
- Add tests with every behavior change. Run focused gates during development and
  all listed stage gates before completion.
- Update this plan, the handoff, the system model, operations documentation, and
  readiness report when their claims change.
- Commit all code, tests, and documentation for a completed stage together and
  leave a clean worktree.
- The chain uses the installed version-1 `t3-backlog --ungated` path. Ungated
  bypasses quiet-time/forecast admission only; hard quota-health controls still
  apply.
- Never install or deploy the development binary, restart `t3-steward`, alter
  live configuration, open live state with development code, dispatch real
  work, push, or create a pull request.

End states are exclusive:

1. Safe work remains inside the selected stage: queue nothing, update the
   handoff, and end `BACKLOG STATUS: continue`.
2. The selected stage is committed and complete: if another stage remains,
   queue exactly one successor from
   `docs/plans/backlog-v2-production-session-prompt.md`, make no further
   changes, and end `BACKLOG STATUS: done`.
3. User authorization or a material product decision is genuinely required:
   finish independent work, queue nothing, record the exact question, and end
   `BACKLOG STATUS: needs-input`.

S19 is terminal and never queues a successor.
