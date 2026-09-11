# Backlog-v2 production-binding handoff

Updated: 2026-09-10

## Authority

- Repository: `/home/igor/Work/t3-steward`
- Host: Normandy
- Branch: `feature/backlog-orchestrator`
- Authoritative plan:
  `docs/plans/backlog-v2-production-binding.md`
- Architecture contract: `docs/architecture/system-model.md`
- Predecessor implementation plan: `docs/plans/backlog-v2.md`
- Predecessor implementation handoff: `docs/plans/backlog-v2-handoff.md`
- Current readiness report:
  `docs/plans/backlog-v2-deployment-readiness.md`
- Initial planning baseline: `6c8726f`
- No installation, deployment, live-service restart, live configuration/state
  mutation, real worker/T3 dispatch, push, or pull request is authorized.

## Current selection

- Selected stage: S18 — Audit, backup, recovery, and security hardening.
- Exact full starting commit:
  `149ab40ae38649c875b76ed72adf891fdc866f42`.
- Pre-existing worktree state: clean (`git status --short` produced no
  entries); no unexplained changes were present.
- Exit gates copied exactly from the authoritative plan:
  - Audit completeness and redaction assertions.
  - Backup/restore drill plus corrupt/incomplete/version-mismatch refusal.
  - Recovery authentication, authorization, revision, evidence, replay, and
    closed-quota tests.
  - Security/limits/fuzz or property tests appropriate to each parser and
    boundary.
  - `go test ./...`, `go build ./...`, `go vet ./...`, and
    `git diff --check`.
- Focused tests selected before implementation:
  - `go test ./internal/store/sqlite ./internal/backlog ./internal/backlogadmin ./internal/workerruntime ./cmd/t3-steward -run 'Test(Audit|Backup|Restore|Recovery|RuntimeHealth|Incident|Security|Redact)' -count=1 -v`
  - `go test ./internal/store/sqlite ./internal/backlog ./internal/backlogadmin ./internal/workerruntime ./cmd/t3-steward -count=1`

## S18 completion record

- S18 has no safe in-stage work remaining. Its code, tests, plan, architecture,
  readiness report, operations documentation, and handoff are complete.
- Completed S18 implementation commit:
  `58344eb8167fdd01dd05825327ddec86a6253358` (parent and exact stage start
  `149ab40ae38649c875b76ed72adf891fdc866f42`).
- Added `internal/backupsnapshot`, a bounded stopped-snapshot implementation for
  the coordinator SQLite database and configured artifact root. It takes the
  coordinator ownership lock, refuses nonempty WAL/SHM state, links, special
  files, unstable inputs, overlapping/existing targets, corrupt or incomplete
  manifests, unexpected files, checksum/size failures, and unsupported format
  or schema versions. Create and restore stage and verify the complete unit;
  restored SQLite is opened immutable/read-only for integrity and exact-schema
  checks. Snapshot trees are owner-only and immutable after publication.
- Added `t3-steward backlog backup create|verify|restore` using coordinator-mode
  configuration and explicit file/byte limits. Exported read-only SQLite
  integrity/schema inspection and escaped file URLs so paths containing URL
  metacharacters cannot alter the SQLite DSN.
- Added runtime status projection for mode, owner/epoch, health, transport,
  worker/quota freshness, reconciliation issues, unknown executions, and
  durable artifact-custody metadata incidents. Coordinator composition now
  supplies its actual runtime identity and uses the configured v2 artifact
  root rather than the unrelated legacy data directory.
- Added evidence-required unknown-assignment recovery for `stopped` and
  terminal `failed` outcomes. It is coordinator-epoch, assignment-epoch, and
  attempt-revision fenced; records only an evidence identifier and SHA-256;
  releases rather than redispatches the assignment; never changes closed quota;
  and commits the attempt, assignment, and native actor/reason audit event in
  one SQLite transaction. Exact request replay returns the original decision
  even though the authenticated server supplies a new application time;
  changed replay fails closed.
- Exposed recovery only through the bounded Unix admin transport and the
  `backlog recover` CLI. The transport authenticates `SO_PEERCRED`, replaces
  claimed identity, authorizes the assignment-scoped action before the store
  write, requires every epoch/revision/evidence field, and supports an explicit
  replay ID. Updated the admin and operations runbooks with the exact recovery,
  snapshot, verified-restore, rollback, and point-of-no-return procedures.
- Added native same-transaction audit events for coordinator ownership/epoch,
  worker snapshots, assignment plan/claim/dispatch/lease/reconciliation,
  worker command/acknowledgement, throttle delivery, artifact publication and
  prune, and terminal importer transitions. Allowlisted audit details require
  actor, reason, available epochs/revisions, idempotency identity, and outcome;
  tests assert completeness and exclude capability tokens, credential values,
  filesystem paths, and arbitrary record content. Exact transition replay
  requires the immutable original audit event and never duplicates it.
- Hardened all coordinator-owned production roots and immutable objects to
  owner-only modes, removed capability-bearing assignment dumps from stale
  errors, bounded local-admin connections by the configured request deadline
  and a fixed concurrent-handler limit, and added deterministic timeout and
  backpressure tests. Existing fixed invocation, process containment, bounded
  stream, and safe archive extraction gates remain intact.
- Added fuzz/property coverage for strict bounded worker-protocol decoding,
  bounded safe-tar validation, workflow manifest and schedule parsing, and
  framed local-admin decoding. Snapshot paths now resolve aliases through the
  deepest existing ancestor and refuse canonical database/artifact/snapshot or
  restore-target overlap.
- Passing S18 evidence:
  - `go test ./internal/backupsnapshot -count=1`
  - `go test ./internal/store/sqlite ./internal/backlog ./internal/backlogadmin ./internal/workerruntime ./internal/backupsnapshot ./internal/workerproto ./cmd/t3-steward -count=1`
  - `go test ./internal/backlogadmin -run TestLocalTransportBoundsIdleClientsAndBackpressure -count=25`
  - `go test -race ./cmd/t3-steward ./internal/workerproto -run 'TestRunBacklogV2CoordinatorStartsClosedAndAdvancesEpoch|TestRunBacklogV2CoordinatorReconcilesSchedulesAndAdminCommands|TestSSHTransportDropRetryTimeoutCancellationAndLimits' -count=1`
  - `go test ./internal/workerproto -run '^$' -fuzz FuzzProtocolCodecStrictBoundedDecode -fuzztime=2s`
  - `go test ./internal/workerproto -run '^$' -fuzz FuzzArtifactTarValidationIsBounded -fuzztime=2s`
  - `go test ./internal/backlog -run '^$' -fuzz FuzzManifestAndScheduleParsers -fuzztime=2s`
  - `go test ./internal/backlogadmin -run '^$' -fuzz FuzzLocalAdminFrameStrictBoundedDecode -fuzztime=2s`
  - `go test ./... -count=1`
  - `go build ./...`
  - `go vet ./...`
  - `go test -race ./...`
  - `git diff --check`
- No binary was installed, no service was restarted, no live configuration or
  state was changed or opened by development code, no fleet worker was
  contacted, and no real T3 thread was dispatched. All evidence used temporary
  local roots and test child processes.
- S19 is now the first incomplete named stage. It must not contact a fleet
  worker or run a canary without the explicit authorization required by S19;
  a GO readiness report is still not deployment approval.

## S17 final selection record

- Selected stage completed: S17 — Coordinator runtime, submissions, schedules,
  and quota bridge.
- Exact full starting commit:
  `f51170c42fe520e8ed5cdee453f497f8def5272d`.
- Pre-existing worktree state: clean (`git status --short` produced no
  entries); no unexplained changes were present.
- Completed S17 implementation commit:
  `84fb2305f52aa4205f40dec4eb963a653a6bc409` (parent and exact stage start
  `f51170c42fe520e8ed5cdee453f497f8def5272d`). Code, tests, plan,
  architecture, operations, readiness, protocol, and handoff changes were
  committed together. This handoff-only closure commit follows it; the
  worktree is clean and `git diff --check` passes.
- Exit gates copied exactly from the authoritative plan:
  - Complete local multi-process workflow with dependencies, artifacts,
    verification, pause/resume/retry, and suppressed recurring trigger.
  - Simultaneous/replayed submission; unsafe archive; schedule syntax, DST,
    catch-up, overlap, and restart; quota deduplication/staleness; admin auth; and
    legacy/coordinator exclusion tests.
  - Coordinator restart fault injection at every persistence/effect boundary,
    closed-admission start refusal, and no-duplicate T3 dispatch assertions.
  - `go test ./...`, `go build ./...`, `go vet ./...`, and
    `git diff --check`.
- Focused tests selected before implementation:
  - `go test ./internal/backlog ./internal/backlogadmin ./internal/store/sqlite ./internal/workerproto ./internal/workerruntime ./cmd/t3-steward -count=1`
  - `go test ./internal/backlog ./internal/backlogadmin ./internal/store/sqlite ./cmd/t3-steward -run 'Test(Coordinator|Submission|Schedule|Quota|Admin|Legacy|BacklogV2)' -count=1 -v`
  - `go test ./cmd/t3-steward -run TestCoordinatorLocalMultiProcessWorkflow -count=10`
- In-progress checkpoint:
  - Added `ScheduleTimer`, a persistent timer that reloads its cursor from
    durable trigger records and delegates decisions to
    `Store.CommitScheduleTrigger`. Its strict numeric five-field cron parser
    supports wildcards, lists, ranges, steps, standard day-of-month/day-of-week
    behavior, and Sunday aliases 0/7.
  - Occurrences walk the UTC minute timeline in the configured IANA timezone:
    spring gaps create none and fall folds create two distinct UTC identities.
    Catch-up keeps the newest configured bounded set, while stable
    SHA-256-derived trigger/run IDs survive restart.
  - Added revision-fenced, request-idempotent `ScheduleDefinitionService.Put`.
    It preserves immutable template history and current active-run/delay state,
    validates workflows/cron/timezones, and records native create/update audit
    events.
  - Added schema version 11 and a durable submission reservation/completion
    journal. Exact concurrent/restarted requests recover the immutable accepted
    result; changed content under the same key fails closed.
  - Added `SubmissionService` for bounded directory, validated safe-tar, and
    legacy single-task requests. Publication uses deterministic identities and
    recovers a pending file/metadata boundary without duplicating workflows.
    Tar traversal, links, special files, duplicates, excess entries, and excess
    bytes are rejected; legacy gate/host/provider/model/options fields are
    preserved.
  - Added `QuotaBridge`, which maps stored provider-bucket observations into
    configured fleet pools, deduplicates bucket identities, derives admission,
    persists transitions, and returns throttle directives. Missing, stale,
    future, or epoch-conflicting evidence and multiply mapped provider
    instances fail closed.
  - Accepted submissions, quota-admission revisions, and accepted/suppressed
    schedule triggers now emit native audit events in the same SQLite
    transaction as their coordinator-owned state transition. Deterministic
    event identities make exact replay a no-op and backfill an event if a
    pre-event accepted record is replayed; an immutable event conflict rolls
    back the associated state, directive, trigger, and run changes.
  - Added a bounded local admin protocol for queries, durable mutation
    submission, and raw artifact retrieval. The mode-0600 Unix socket
    authenticates `SO_PEERCRED`, accepts only the coordinator owner's UID,
    replaces claimed principals, bounds JSON and artifact bytes, rejects live
    socket/non-socket collisions, and closes idle connections on shutdown.
  - Coordinator mode now owns the admin service/socket, artifact opener, and
    native tar-bundle submission endpoint. The byte-bounded stream is accepted
    only from the authenticated owner UID, reuses the submission journal and
    safe archive validator, and returns immutable accepted/replay identities.
    `t3-steward backlog submit <bundle.tar>` rejects non-regular or changed
    input files and can supply an explicit idempotency key. Framed JSON rejects
    trailing values before any handler runs. Coordinator-mode CLI
    commands connect to the socket instead of opening SQLite; they remain
    query/submission clients and cannot execute commands.
  - Exposed schedule-definition administration through that production local
    socket. The bounded `schedules put` client supplies request idempotency,
    expected revision, cron/timezone, failure policy, enabled state, and reason.
    The server discards claimed identity, uses the kernel-authenticated peer as
    audit actor, returns immutable exact replay, and rejects changed or stale
    requests through `ScheduleDefinitionService`.
  - Added a coordinator-owned legacy Markdown submission source. It scans only
    an owner-controlled, non-group/world-writable real directory, rejects links,
    bounds aggregate bytes and file count, maps unique configured T3 project
    names to logical v2 projects, and submits deterministic immutable requests.
    Exact scans replay; changed content under the same legacy file ID conflicts.
  - The coordinator composes a bounded local cycle. On startup and each
    configured interval it first reconstructs quota reservations and persists
    derived admission, reconciles durable schedule occurrences, reloads planning
    state and atomically persists offered assignments, executes pending
    revision-fenced admin commands, and scans the legacy source. Quota failure
    defers both planning and admin execution; other component errors do not block
    safe local boundaries. Planning produces no worker command or dispatch.
    Unchanged installed `t3-backlog` and `t3-job` drops remain compatible, and
    runtime-level legacy/coordinator overlap is refused before SQLite opens.
  - Quota reconciliation and planning are now production-bound locally.
    Quota-pool config requires positive concurrency, at least one provider
    instance per pool, and unambiguous provider-instance ownership. Each quota
    pass produces numeric per-bucket planning windows, using stored usage and
    burn rate plus configured fallback forecast, safety margin, long-window cap,
    and surplus horizon. Durable active, paused-required, and offered/committed
    assignment costs are overlaid before planning; missing or contradictory
    estimates fail reconciliation without planning or executing queued admin
    commands.
  - Planning reloads canonical workflows, runs, tasks, attempts, assignments, and
    current-epoch worker snapshots; validates every DAG; reconstructs resource
    and workflow-checkout ownership; and derives immutable difficulty cold-start
    estimates for exact worker/provider/model/options routes. The hard
    `QuotaAdmissionPolicy` remains authoritative, including for admin-forced
    starts. `FleetCoordinator.PlanAndCommit` atomically records offered
    assignments and their remaining-cost/runtime/checkpoint estimates plus a
    deterministic future T3 thread identity. Reloaded assigned attempts are
    blocked from replanning.
  - Added a serialized coordinator protocol client for snapshots, offers, lease
    renewals, lifecycle commands, and throttle commands. It signs every typed
    request, delegates bounded identical-envelope retry, and permanently
    abandons a session after an unresolved exchange so later requests cannot
    create a sequence gap or changed replay.
  - Added `FleetCoordinator.ReconcileWorker`, a transport-neutral durable
    exchange boundary. It persists a fresh epoch-bound snapshot, loads only
    already-committed offers for that worker epoch, applies bounded lease and
    offer expiry, validates the immutable execution package and exact assignment
    identity, requires one unique claim per offer, commits claims through
    coordinator SQLite, refreshes the snapshot, then derives, persists, delivers,
    and acknowledges lifecycle commands.
  - Added the final worker-transport admission fence. Only quota pools explicitly
    derived as open may cross the transport with offers or prepare/dispatch
    commands; closed, constrained, draining, recovering, missing, or failed
    quota state withholds them. Previously durable pending new-work commands are
    checked again immediately before delivery, while observation, stop, and
    result collection remain available.
  - Coordinator worker configuration now includes the worker epoch and worker
    mode requires its local epoch to match that declaration. The executable can
    construct a fresh serialized protocol client and worker-scoped package
    builder from the configured address, epoch, message limits, strict SSH
    transport, and environment-resolved mutual credentials. Session IDs contain
    a cryptographically random nonce so an abandoned ambiguous session is never
    reused. The immediate startup cycle remains local-only; subsequent scheduled
    cycles construct fresh sessions for configured workers and reconcile each
    independently. A failed quota pass supplies an empty final admission policy,
    preserving observation/stop/collection while withholding new work. No
    worker or T3 process was contacted during development or verification.
  - Added `CoordinatorOfferBuilder`. It reloads authoritative records and
    requires the ephemeral leased offer to match the durable offered assignment,
    then resolves the exact attempt, task, run, workflow, worker-scoped catalog,
    prompt ownership, static inputs, named dependency outputs, route, deadlines,
    verification, output declarations, and byte/time/turn limits. Package ID and
    creation time derive from the committed assignment, so rebuilding at a later
    retry time yields the same content address; malformed, cross-run, duplicate,
    missing, or ownership-conflicting records fail closed.
  - Legacy single-task submissions now explicitly carry the minimal `true`
    verification command required by the immutable worker package contract.
  - Scheduled worker reconciliation now expires elapsed leases before contact,
    renews only a live durable claim that the same worker still observes, and
    persists the successful extension against the exact pre-renewal snapshot.
    A lost renewal response leaves coordinator state unextended and therefore
    fail closed; an expired unknown assignment cannot be revived merely by a
    continuing running observation.
  - The same authenticated session now replays worker-scoped pending throttle
    intent, delivers new quota warning/drain/hard-stop commands, escalates
    missed checkpoint deadlines, and resumes eligible paused attempts. Per-
    worker store scoping prevents one unavailable worker from consuming or
    blocking another worker's durable throttle records.
  - Added a transport-neutral `CoordinatorResultImporter`. It accepts only a
    completed assignment and verifying attempt bound to the exact coordinator,
    worker, and assignment epochs; verifies the complete checksum-linked
    custody chain, safe result paths, aggregate/object byte limits, payload
    size/SHA-256, declared output names and media types, ordered strict-JSON
    verification reports, thread archive, and explicit final done marker before
    publishing coordinator-owned artifacts.
  - Result import distinguishes invalid evidence from a valid failed task.
    Nonzero verification or missing declared output retains the available
    evidence and commits a replay-stable failed turn outcome; complete passing
    evidence commits success. Exact replay republishes no mutable metadata and
    creates no duplicate outcome transition. Completed assignments are now an
    explicitly fenced artifact-publication state when their attempt is
    verifying or terminal.
  - Worker result custody now preserves declared output paths instead of
    substituting artifact IDs. Coordinator planning uses the reconciled DAG
    snapshot, so a successful imported producer releases its blocked dependent
    for the next planning pass.
  - Added one-at-a-time, purpose-scoped worker outbox polling and authenticated
    post-import acknowledgement. A worker atomically moves an acknowledged
    manifest into retained custody so exact replay succeeds without
    rediscovery; results and checkpoints cannot starve one another during
    discovery.
  - Added an authenticated bounded raw artifact fetch to the coordinator
    protocol client. It requests the exact announced manifest and complete
    ordered object identity over a distinct `artifact-send` session, verifies
    signed response identity plus aggregate/object size and SHA-256, and exposes
    only exact verified objects to the importer. Retries reuse the immutable
    envelope; ambiguous exhaustion poisons only that fresh artifact session.
  - Configured worker cycles now poll, fetch, import, and acknowledge one result
    after ordinary observation/lease/command/throttle reconciliation. Import or
    fetch failure never acknowledges the outbox, and a lost acknowledgement
    safely replays import on the next fresh cycle. The production SSH command
    now supplies the endpoint's mandatory fixed `control` or `artifact-send`
    operation as a separately validated shell-free argument.
  - Added `CoordinatorCheckpointImporter` and composed a second purpose-scoped
    poll/fetch/import/acknowledge pass. Checkpoint bytes publish only when the
    object exactly matches the worker/assignment epochs and the artifact ID,
    size, SHA-256, path, and capture time already accepted in an acknowledged
    throttle projection. Replay works while paused and after the same assignment
    later completes; invalid or corrupt evidence is never acknowledged.
  - Corrected worker checkpoint objects to their actual `text/markdown` media
    type. Coordinator reports now expose imported checkpoint artifacts
    separately from terminal result imports.
  - Added the exact named `TestCoordinatorLocalMultiProcessWorkflow` acceptance
    gate. It runs the complete disposable dependency/artifact/verification/
    pause/resume/retry/suppressed-trigger workflow in a child test process,
    composes the authenticated coordinator-stub/worker child-process exchange,
    and repeats the closed-admission, coordinator/worker restart, durable
    command/effect replay, lost-response, and no-duplicate-dispatch assertions.
  - Added migration-from-10, simultaneous reservation, changed replay,
    crash-pending recovery, bounds, unsafe input, generated key, legacy
    compatibility, schedule revision/replay/syntax/DST/catch-up/overlap/restart,
    quota deduplication/staleness/conflict, local admin auth/limits/shutdown,
    exact installed legacy fixtures, legacy directory ownership/link/byte/file
    bounds, runtime mutual exclusion, closed legacy ingestion,
    transaction-bound submission/quota/schedule events with replay and
    conflict rollback, CLI archive input checks, authenticated bounded native
    archive streaming, end-to-end runtime ingestion/replay,
    schedule-to-timer/runtime-to-admin-socket tests, and end-to-end bounded-cycle
    schedule/admin convergence with zero assignment dispatch; durable assignment
    estimate/replay identity and restart reconstruction; zero-observation closed
    admission; deterministic pool binding; quota-failure admin deferral; and
    schedule-definition CLI parsing, generated identity, peer authentication,
    local transport replay, live coordinator-socket administration, numeric
    quota planning-window projection, active/paused/committed durable accounting,
    cold route estimate assembly, assigned-attempt replay suppression,
    quota-failure planning deferral, atomic offered-assignment persistence
    with no dispatch, deterministic offered thread identity, serialized signed
    client sequencing/ambiguous-session fencing/structured errors, durable
    snapshot-offer-claim-refresh-prepare-command reconciliation, replay-stable
    execution-package assembly, malformed durable-link rejection, and legacy
    verification compatibility.
  - Passing checkpoint gates:
    - `go test ./internal/config ./internal/backlog ./internal/domain ./internal/store/sqlite ./cmd/t3-steward -run 'Test(BacklogV2CoordinatorConfiguration|DeriveQuotaPlanningState|QuotaBridge|FleetCoordinatorCommits|RunBacklogV2Coordinator|CoordinatorBoundaryCycle|CoordinatorQuotaPoolBindings|CoordinatorQuotaReconciler|OrchestratorDomainJSON)' -count=10`
    - `go test ./cmd/t3-steward -run 'TestRunBacklogV2Coordinator(ReconcilesSchedulesAndAdminCommands|StartsClosedAndAdvancesEpoch|ServesAuthenticatedLocalAdmin|IngestsLegacyDropWithoutDispatch|AcceptsNativeArchiveSubmissionAndReplay)$' -count=10`
    - `go test ./internal/backlogadmin ./cmd/t3-steward -run 'Test(LocalSubmission|LocalTransport|ReadLocalJSON|BacklogSubmission|RunBacklogV2CoordinatorAcceptsNative)' -count=10`
    - `go test ./internal/store/sqlite ./internal/backlog -run 'Test(CompleteSubmission|CommitQuotaAdmission|CommitScheduleTrigger|ScheduleDefinition)' -count=10`
    - `go test ./internal/backlog -run TestQuotaBridge -count=1 -v`
    - `go test ./internal/backlogadmin ./cmd/t3-steward -run 'Test(LocalTransport|ListenLocal|RunBacklogV2)' -count=10`
    - `go test ./internal/backlog ./internal/backlogadmin ./cmd/t3-steward -run 'Test(ScheduleDefinition|LocalTransport|ParseScheduleDefinition|RunScheduleDefinition|RunBacklogV2CoordinatorAcceptsNative)' -count=10`
    - `go test ./internal/backlog ./cmd/t3-steward -run 'Test(LegacySubmissionSource|RunBacklogV2Coordinator|RunBacklogV2Refuses)' -count=10`
    - `go test ./internal/backlog ./internal/backlogadmin ./internal/config ./internal/store/sqlite ./internal/workerproto ./internal/workerruntime ./cmd/t3-steward -count=1`
    - `go test ./...`
    - `go build ./...`
    - `go vet ./...`
    - `git diff --check`
    - `go test ./internal/backlog ./internal/config ./cmd/t3-steward -count=1`
    - `go test ./internal/backlog ./cmd/t3-steward -run 'Test(QuotaBridge|DeriveQuotaPlanningState|BuildCoordinatorPlanInput|CoordinatorPlanner|CoordinatorBoundaryCycle|RunBacklogV2Coordinator)' -count=10`
    - `go test ./internal/backlog ./internal/backlogadmin ./internal/store/sqlite ./internal/workerproto ./internal/workerruntime ./cmd/t3-steward -count=1`
    - `go test ./internal/backlog ./internal/backlogadmin ./internal/store/sqlite ./cmd/t3-steward -run 'Test(Coordinator|Submission|Schedule|Quota|Admin|Legacy|BacklogV2)' -count=1 -v`
    - `go test ./cmd/t3-steward -run TestCoordinatorLocalMultiProcessWorkflow -count=10`
    - `go test ./...`
    - `go build ./...`
    - `go vet ./...`
    - `git diff --check`
    - `go test ./internal/backlog ./internal/store/sqlite ./internal/workerproto -count=1`
    - `go test ./internal/backlog ./internal/backlogadmin ./internal/store/sqlite ./internal/workerproto ./internal/workerruntime ./cmd/t3-steward -count=1`
    - `go test ./internal/backlog ./internal/backlogadmin ./internal/store/sqlite ./cmd/t3-steward -run 'Test(Coordinator|Submission|Schedule|Quota|Admin|Legacy|BacklogV2)' -count=1 -v`
    - `go test ./cmd/t3-steward -run TestCoordinatorLocalMultiProcessWorkflow -count=10`
    - `go test ./...`
    - `go build ./...`
    - `go vet ./...`
    - `git diff --check`
    - `go test ./internal/backlog -run 'Test(CoordinatorOfferBuilder|SubmissionServicePreservesLegacy|FleetCoordinatorReconcilesOffer)' -count=10`
    - `go test ./internal/backlog -run TestFleetCoordinatorWithholdsNewWorkAtFinalQuotaBoundary -count=1 -v`
    - `go test ./internal/backlog -run 'Test(FleetCoordinatorRenewsLeaseAndDeliversDurableThrottle|LeaseRenewalsForWorker|PlanWorkerStateTransitions)' -count=1 -v`
    - `go test ./internal/backlog ./cmd/t3-steward -run 'Test(FleetCoordinatorRenewsLeaseAndDeliversDurableThrottle|LeaseRenewalsForWorker|PlanWorkerStateTransitions|CoordinatorWorkerSessions)' -count=10`
    - `go test ./cmd/t3-steward -run 'Test(CoordinatorBoundaryCycle|CoordinatorWorkerSessions|NewCoordinatorWorkerSession)' -count=1 -v`
    - `go test ./internal/backlog ./internal/backlogadmin ./internal/store/sqlite ./internal/workerproto ./internal/workerruntime ./cmd/t3-steward -count=1`
    - `go test ./internal/backlog ./internal/backlogadmin ./internal/store/sqlite ./cmd/t3-steward -run 'Test(Coordinator|Submission|Schedule|Quota|Admin|Legacy|BacklogV2)' -count=1 -v`
    - `go test ./cmd/t3-steward -run TestCoordinatorLocalMultiProcessWorkflow -count=10`
    - `go test ./...`
    - `go build ./...`
    - `go vet ./...`
    - `git diff --check`
- Current outcome/import checkpoint gates:
    - `go test ./internal/backlog ./internal/store/sqlite ./internal/workerruntime -run 'Test(BuildCoordinatorPlanInputReleasesDependency|CoordinatorResultImporter|CoordinatorArtifact|Custody)' -count=10`
    - `go test ./internal/backlog ./internal/backlogadmin ./internal/store/sqlite ./internal/workerproto ./internal/workerruntime ./cmd/t3-steward -count=1`
    - `go test ./...`
    - `go build ./...`
    - `go vet ./...`
    - `git diff --check`
- Current configured result-transfer focused tests:
    - `go test ./cmd/t3-steward ./internal/workerproto ./internal/workerruntime ./internal/backlog -run 'Test(ImportCoordinatorWorkerResult|NewCoordinatorWorkerSession|ClientFetch|SSHArtifact|CustodyPublishes|WorkerArtifactStreams)' -count=1 -v`
- Current checkpoint-transfer focused tests:
    - `go test ./internal/backlog ./internal/workerruntime ./cmd/t3-steward -run 'Test(CoordinatorCheckpointImporter|CustodyPublishes|ImportCoordinatorWorkerCheckpoint|ImportCoordinatorWorkerResult|NewCoordinatorWorkerSession)' -count=10`
- S17 has no safe in-stage work remaining. Its focused, named acceptance, full
  repository, build, vet, and diff gates pass; the stage and R4 checkboxes are
  complete. S18 is the next incomplete named stage.

## Completed stages

- S17 — Coordinator runtime, submissions, schedules, and quota bridge,
  starting from `f51170c42fe520e8ed5cdee453f497f8def5272d` and completed by
  `84fb2305f52aa4205f40dec4eb963a653a6bc409`.
- Composed bounded submissions, persistent schedules, quota-authoritative
  planning, authenticated administration, worker exchange, lease and durable
  lifecycle/throttle reconciliation, result/checkpoint custody, terminal
  outcomes, and legacy compatibility into the coordinator runtime.
- The exact disposable multi-process workflow gate passes dependencies,
  artifacts, verification failure, retry, pause/restart/resume, success,
  suppressed recurrence, closed admission, durable effect replay, and
  no-duplicate dispatch ten consecutive times.
- No development binary was installed or deployed; no service or live
  configuration/state was changed; no fleet worker or T3 process was contacted;
  no workflow was dispatched.
- S14 — Authority, configuration, and storage lifecycle, starting from
  `ccd1d142e2ae612578682220d2b9cc43d90cc157`.
- Added strict, disabled-by-default backlog-v2 configuration with validated
  workers, projects, setup profiles, quota pools, safe roots, SSH transport,
  limits, freshness, leases, scheduling, credentials, and closed admission.
  Existing legacy and shipped example configurations remain valid.
- Split plain existing-database opens from explicit migration. Status, report,
  wait, archive, replay-with-state, legacy backlog reads, and admin clients do
  not migrate. Coordinator/legacy daemon startup owns explicit migration.
  Existing version 1, 5, 6, 8, and 9 fixtures pass.
- Added exclusive file-backed coordinator ownership with atomic durable identity
  and epoch advancement. Restart advances once; a refused second owner does not.
- Removed admin CLI command execution. Local adapters query and submit durable
  pending intent only.
- Added mutually exclusive legacy/coordinator startup. Coordinator mode acquires
  authority and remains closed without constructing a worker or T3 client;
  disabled mode has no backlog-v2 startup effects.
- Updated shipped configuration, operations, architecture, migration boundary,
  and readiness blockers. No live state or external process was contacted.
- S15 — Versioned worker exchange and execution package, starting from
  `ce1fd4dd45c27aae4ba99151b3a015523b6cce05`.
- Added protocol version 1 envelopes and typed capabilities, snapshots, offers,
  claims, lease renewals, commands, acknowledgements, observations, artifact
  exchange, and structured errors. Strict decoding, sender/recipient identity,
  coordinator and worker epochs, peer and response-signer authentication,
  per-principal authorization, deadlines, bounded sequences, exact duplicate
  caching, changed replay/reorder refusal, bounded concurrency, and retry/backoff
  semantics fail closed.
- Added a content-addressed immutable execution package containing complete
  assignment/dispatch identity, prompt and dependency artifact references,
  provider route, catalog/environment references, credential names,
  verification/output declarations, deadlines, and execution/byte limits.
- Added versioned upload/download manifests, safe relative paths, exact sizes and
  SHA-256, checksum-linked custody records, and bounded tar inspection that
  rejects traversal, duplicates, links, devices, truncation, and expansion
  excess.
- Retained coordinator-initiated SSH after the bounded local multi-process spike
  proved a shell-free fixed invocation, batch mode, strict host checking,
  bounded stdin/stdout/stderr, connect/request deadlines, cancellation, signed
  response verification, and same-identity retry. The test transport spawned
  only child copies of the test executable; no fleet worker was contacted.
- Added protocol and execution-package JSON goldens plus focused faults for
  version, epochs, authn/authz, replay, order, duplicate/drop, timeout,
  cancellation, limits, backpressure, malformed archives, and checksums.
  Updated the protocol contract, system model, operations, and NO-GO readiness
  report.

## S16 completion

- S16 completed from
  `d6a79a074b1f102acee4b07b07c5724fa9e5e7f9`.
- Added `internal/workerruntime` with an fsync-and-rename worker-local journal
  protected by an interprocess lock. It binds worker/coordinator epochs,
  monotonic worker sequence, assignment/package identity, effect phase,
  workspace/thread identity, original commands, immutable acknowledgements,
  pending throttle intent, and explicit unknown failures.
- Added restart reconciliation at preparation, observe-before-create T3
  dispatch, stop, collect/upload, cleanup, expired-lease stopping, and pending
  checkpoint boundaries. Exact command replay returns the original
  acknowledgement; changed replay fails closed; ambiguous effects become
  explicit unknown execution.
- Added deterministic inventory/snapshots, offer validation and claims, bounded
  lease renewal, durable lifecycle commands, and structured throttle
  warn/drain/hard-stop/resume handling. A worker rejects lease extensions beyond
  its configured interval.
- Added a concrete local driver that resolves strict worker-scoped
  project/setup/catalog configuration, downloads verified content-addressed
  inputs, uses existing isolated workspace and whole-cgroup containment
  primitives, performs deterministic T3 create/observe/stop/checkpoint/resume/
  collection, verifies output, publishes result/checkpoint custody, and fences
  cleanup.
- Added persistent no-external-effects mode and a bounded one-envelope control
  entrypoint. Added separate authenticated artifact receive/send streams with
  signed manifests/custody and exact raw size/SHA-256 enforcement, immutable
  outbox replay, regular-file checks, and fsync-backed no-overwrite commits.
- Added named project-credential availability checks and protocol credential
  resolution. Principal identity is fixed to `ssh:<coordinator>` and
  `ssh:<worker>`; secrets never enter packages, journals, or error text.
- Added `WorkerService` composition and fixed production executable operations
  `control`, `artifact-receive`, and `artifact-send`. Worker mode never
  acquires coordinator authority. The endpoint requires an explicit local
  config path, ignores general environment overrides, rejects authority-changing
  flags, and takes worker/coordinator epochs only from strict YAML.
- Added an fsync-backed protocol replay store. It serializes across worker
  processes, persists session order, pending request digest, and exact signed
  response, resumes a crash-pending identical request through the idempotent
  runtime, and blocks changed or intervening requests.
- Focused coverage includes every durable lifecycle/effect boundary; exact and
  changed replay across restart; corrupt journal/package/download/custody
  refusal; stale worker/coordinator/catalog epochs; bounded renewals and lease
  loss; lost/ambiguous T3 creation; checkpoint/resume; containment cancellation;
  local Git preparation/finalization; no-effects execution; credential and
  principal binding; artifact receive/send/restart/tamper/replay; throttle
  round trips; and disposable coordinator-stub/worker multi-process exchange.
- Passing completion gates:
  - `go test ./internal/config ./cmd/t3-steward ./internal/workerruntime ./internal/workerproto -count=1`
  - `go test ./internal/workerruntime -run 'Test(WorkerService|WorkerArtifact|EnvironmentProtocol|Custody|RuntimeRestart|LostAndAmbiguousT3|StaleEpoch|LeaseLoss|Throttle|Cancellation)' -count=1`
  - `go test ./internal/workerruntime -run TestLocalCoordinatorStubWorkerMultiProcess -count=10`
  - `go test ./...`
  - `go build ./...`
  - `go vet ./...`
  - `git diff --check`
- No development binary was installed or deployed; no service or live
  configuration/state was changed; no fleet worker or T3 process was contacted;
  no workflow was dispatched.

## Known blockers carried forward

- The worker runtime and fixed endpoints are implemented but not installed,
  deployed, or contacted by a production coordinator.
- The coordinator runtime composes bundle ingestion, schedules, quota bridging,
  admin execution, and quota-authoritative planning through atomic offered
  assignments. S17 now composes worker exchange, delivery/reconciliation,
  lease expiry/renewal, throttle delivery, bounded result/checkpoint transfer,
  coordinator custody, and terminal outcomes. Its named disposable
  multi-process and restart/effect-boundary gates pass. Hard closed quota
  admission remains authoritative for every path.
- S18 now has an uncommitted stopped coordinator snapshot/verified restore,
  runtime incident projections, and authenticated evidence-bound unknown-state
  recovery checkpoint. Complete native non-admin audit coverage, production
  storage/credential/error hardening, fuzz/property limits, and final stage
  evidence remain before S18 can close.
- Observe-only multi-process qualification, disposable-state canary, race/full
  release gates, and the final GO/NO-GO report remain S19 work. Any GO still
  requires explicit user approval before host-wide installation or deployment.
- Current evidence contacted no live T3 instance or fleet worker and opened no
  live state with development code.

## Successor rule

When a stage is complete, update the plan checkboxes and this handoff, commit
the entire stage, confirm the tree is clean, and queue exactly one successor
using the command in
`docs/plans/backlog-v2-production-session-prompt.md`. Do not queue on
`continue` or `needs-input`. S19 queues nothing.
