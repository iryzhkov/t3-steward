# Backlog-v2 deployment-readiness report

Date: 2026-09-10  
Candidate branch: `feature/backlog-orchestrator`  
Candidate baseline: `57f0b3269d8341e52170675863fe663566e8bfd3`

S19 local qualification checkpoint: `2beb3ecaf90abf58221c5afe1d1c187cd0637a4d`

S19 authorization checkpoint: `664f2a10b46827d3b549200fab3302c1d18152d9`

## Decision

**GO for an explicitly approved host-wide deployment.**

S14 binds strict configuration, storage migration, and exclusive closed
coordinator authority. S15 adds the authenticated worker protocol, immutable
execution package, artifact custody contract, and bounded SSH foundation. S16
now adds the restart-safe worker runtime and fixed executable endpoints:
inventory, claims, bounded leases, durable commands and acknowledgements,
isolated preparation, deterministic observe-before-create T3 dispatch,
verification, throttle/checkpoint/resume, raw artifact transfer, durable
cross-process replay, reconciliation, and no-effects testing.

The candidate is still not a deployable fleet coordinator. S17 has completed
quota-authoritative planning through atomic offered-assignment persistence and
added a transport-neutral snapshot/offer/claim/command reconciliation boundary
plus replay-stable execution-package assembly from authoritative records. It now
constructs configured epoch-bound, mutually authenticated SSH sessions and
applies a final fail-closed quota check before offers or prepare/dispatch
delivery. Scheduled sessions now compose lease expiry/renewal and durable
warning/drain/hard-stop/resume delivery. A bounded, replay-safe coordinator
importer now validates completed-assignment custody, output declarations,
verification reports, final status, and payload hashes before artifact
publication and terminal outcome projection; configured sessions now poll,
fetch, import, and acknowledge completed result outboxes over separately
bounded authenticated control/raw SSH operations. Checkpoint bytes use the
same sequence and require exact acknowledged throttle evidence. Its disposable
multi-process workflow and restart/effect-boundary gates pass repeatedly. S18
now supplies complete production-transition audit, runtime incident projection,
coherent stopped backup/restore, fenced evidence-bound recovery, and security
hardening. S19's disposable three-process topology, fault matrix, full suite,
build, vet, and race gates pass. Following explicit authorization for homelab
and Normandy, a real strict-host-key SSH exchange also passed against an
ephemeral no-effects worker: authenticated observation and an empty-offer
canary were repeated across fresh sessions and worker processes.

No binary was installed, no service or live configuration was changed, and no
live state database was opened by development code. S19 contacted homelab only
through the authorized ephemeral qualification wrapper; it did not invoke the
installed steward or dispatch a workflow. All state and credentials were
disposable, and the temporary local and remote roots were removed afterward.

## Candidate contents

- Immutable version 2 workflow bundles, strict DAG and artifact validation, and
  compatibility adaptation for current `t3-backlog` and `t3-job enqueue`
  Markdown.
- Deterministic planning across task class, deadlines, forecasts, reservations,
  workers, routes, shared quota pools, locks, and freshness.
- Atomic assignment/lease/command persistence, deterministic T3 dispatch
  identity, lost-response recovery, and fail-closed ambiguous worker state.
- Protocol version 1 envelopes for capabilities, snapshots, offers, claims,
  leases, commands, acknowledgements, observations, artifacts, and structured
  errors with epoch, identity, sequencing, replay, authentication,
  authorization, deadline, size, and backpressure controls.
- Content-addressed immutable execution packages plus versioned artifact
  transfer manifests, bounded archive validation, and checksum-linked custody
  records.
- A retained coordinator-initiated SSH foundation with strict host checking,
  fixed invocation, bounded streams/timeouts, cancellation, signed responses,
  and same-request retry, qualified against local child processes and an
  explicitly authorized ephemeral worker process on homelab.
- Checkpoint-aware throttle pause/resume with hard closed-admission enforcement.
- Schedule singleton, occurrence idempotency, suppression, misfire, and failure
  hold semantics.
- Coordinator-owned checksum-verified artifacts and safe admin retrieval.
- Versioned admin DTOs, read projections, revision-fenced mutations, durable
  outcomes, and audit events, now exposed through an owner-only, peer-UID
  authenticated, bounded local Unix socket. The same peer-authenticated boundary
  now accepts idempotent, revision-fenced schedule definitions and uses the
  kernel-authenticated caller as audit actor. Coordinator-mode CLI clients no
  longer open SQLite.
- SQLite schema version 11 with migration coverage from versions 1, 5, 6, 8,
  9, and 10, including legacy admin-command event backfill and the durable
  submission journal.
- Completed S17 boundaries for bounded idempotent directory, safe tar, and
  legacy single-task submission; revision-fenced audited schedule definitions
  and a restart-derived five-field-cron timer; and fail-closed quota-pool
  derivation from deduplicated stored provider observations. Their submission,
  quota-admission, and accepted/suppressed schedule-trigger commit points now
  emit transaction-bound native audit events with replay deduplication. The
  coordinator now composes unchanged owner-controlled legacy Markdown
  submission, peer-UID-authenticated byte-bounded native tar streaming,
  restart-derived schedule firing, and durable admin-command execution in one
  bounded local cycle. Pool configuration now requires positive concurrency and
  unambiguous provider-instance ownership. Assignment commits durably fix the
  planner's remaining-cost/runtime estimate, so each quota pass reconstructs
  active slots and paused reservations before atomically changing admission;
  reconstruction failure defers admin execution. Runtime tests prove scheduled,
  quota, and operator-created state converges without assignment dispatch.
  Scheduled worker exchange, lifecycle/throttle delivery, artifact import, and
  terminal outcome projection are composed behind hard quota admission.
- A complete temporary SQLite workflow exercising dependencies, artifacts,
  verification failure, retry, pause, restart, resume, success, and duplicate
  schedule suppression.
- Completed S18 native same-transaction audits for coordinator, worker,
  assignment, dispatch, lease, worker-command, throttle, artifact, importer,
  submission, quota, schedule, admin, and recovery transitions. Audit detail is
  allowlisted and excludes capability tokens, credential material, paths, and
  arbitrary reports.
- Stopped coherent SQLite-plus-artifact snapshot create/verify/restore with
  owner-only immutable publication, canonical alias-overlap refusal, bounded
  exact manifests, hashes, integrity checks, and exact format/schema matching.
- Authenticated evidence-required unknown-assignment recovery with coordinator,
  assignment, and attempt fences, exact replay, durable actor/reason audit, and
  no quota mutation or bypass.
- Runtime status incidents, private production storage, local-admin connection
  deadlines and concurrency backpressure, secret-safe errors, and fuzz/property
  coverage for protocol, archive, manifest, schedule, and local-frame parsers.

## Deployment blockers

1. Obtain explicit user approval before any host-wide installation or
   deployment. This GO readiness decision is not that approval.

The shipped schema and authority boundary, transport/package contract,
restart-safe worker endpoint, coordinator runtime, and S18 recovery/security
boundaries are implemented, and S19 local plus authorized multi-host
qualification passes. The remaining approval gate must not be inferred from
this technical readiness decision.

## Operational risks retained

- SQLite and coordinator artifact files must be backed up and restored as one
  coherent unit. Worker journals, replay state, custody, and workspaces likewise
  require a coherent per-worker recovery point.
- Schema migration is explicit and coordinator-owned. A candidate must never be
  pointed at live state before its coherent backup is verified.
- Exclusive coordinator ownership and worker replay serialization use host-local
  file locks; cross-host transport must not reinterpret them as distributed
  leadership.
- The first candidate dispatch or resume is the rollback point after which
  worker/T3 reconciliation is mandatory.
- Worker loss remains fail closed as `unknown`; automatic reassignment without
  proof of stop can duplicate external effects.
- Deterministic assignment, dispatch, and protocol replay prevent duplicate
  steward effects but cannot make arbitrary task side effects exactly once.
- Worker and coordinator epoch rotation must occur under closed admission.
  Mismatched durable state fails startup and requires reconciliation, not
  deletion.
- Paused required work retains remaining-cost reservations; incorrect operator
  deletion can over-admit quota.
- Binary or oversized artifacts use bounded raw streams and checksum custody;
  missing or corrupt evidence blocks use.
- Fleet clock skew and stale worker/quota observations close admission.
- SSH host/login authentication and envelope signing remain separate controls.
  Forced commands must use an explicit local config, map credential principals
  to configured identities, and never evaluate `SSH_ORIGINAL_COMMAND`.
- Existing systemd timers may continue producing compatible schedule-marked
  submissions during migration; duplicate occurrence and open-run checks remain
  authoritative.

## Documentation evidence

- [State, ownership, boundaries, contracts, primitives, invariants, transactions,
  evidence, and production-binding sequence](../architecture/system-model.md)
- [Operations, configuration, manifest, recovery, backup, rollback, and
  deployment order](../backlog-v2-operations.md)
- [Version 2 example bundle](../examples/backlog-v2/workflow.yaml)
- [Admin command and artifact safety](../backlog-admin.md)
- [Worker protocol, execution package, artifacts, and SSH decision](../backlog-v2-worker-protocol.md)
- [Approved implementation plan](backlog-v2.md)
- [Serial implementation handoff](backlog-v2-handoff.md)
- [Production-binding remediation plan](backlog-v2-production-binding.md)
- [Production-binding serial handoff](backlog-v2-production-handoff.md)

## Verification record

The following gates passed on Normandy against the candidate worktree:

- `go test ./internal/backlog -run TestDocumentedWorkflowBundleStaysValid -count=1`;
- `go test ./internal/backlog ./internal/store/sqlite -run 'TestBacklogV2EndToEndLocalWorkflowHardening|TestT3(Job|Backlog)|TestMigrationFromVersion(Nine|Five|One|Six|Eight)' -count=1 -v`;
- `go test ./internal/backlog ./internal/backlogadmin ./internal/store/sqlite ./cmd/t3-steward -count=10`;
- `go test ./internal/backlog ./internal/backlogadmin ./internal/store/sqlite -run 'Test(M5|FleetCoordinator.*(Lost|Replays)|ReconcileAssignmentDispatch|BundleIngesterRollsBack|CoordinatorArtifactPublicationReplayFencingAndPartialUpload|MigrationFromVersionNine|BacklogV2EndToEnd|ExecutePendingCommandsSurvivesRestartAndReplaysRetry|ExecutePendingStartReplansWhenHardQuotaClosesBeforeApply)' -count=20`;
- `go test ./...`;
- `go build ./...`;
- `go vet ./...`;
- `go test -race ./...`;
- `git diff --check`.

All tests used temporary state, artifact, bundle, and workspace roots. The
documented workflow bundle is loaded by a repository test so manifest drift
fails CI.

### S19 qualification evidence

The local portion of S19 passed on Normandy. A dedicated topology
test runs the real coordinator boundary, local-admin client, and restricted
worker service in three separate OS processes with one temporary configuration,
state, bundle, artifact, workspace, and worker-journal tree plus test-only
credentials. The worker uses the production no-external-effects mode; the local
test opens no SSH or T3 connection. A second aggregate gate launches child
processes for transport loss/reorder/duplicate and clock skew;
coordinator/worker restart and replay; stale quota and schedule catch-up;
artifact corruption; coherent backup/restore; transaction rollback; recovery;
and legacy/v2 exclusion.

After the user explicitly authorized homelab and Normandy, the opt-in
`TestBacklogV2AuthorizedMultiHostCanary` ran from Normandy through the real SSH
transport with strict host-key checking to a fixed ephemeral wrapper on
homelab. It authenticated a worker snapshot and delivered an empty offer set,
requiring zero claims. The sequence was repeated using two fresh client
sessions and remote worker processes while retaining disposable remote replay
state. This exercised connection, identity, envelope authentication, clock,
sequence, and restart/replay boundaries without assignment creation, workspace
preparation, artifact transfer, or T3 dispatch. The installed homelab
`t3-steward 0.10.1` binary and service were untouched. The temporary homelab
root `/tmp/t3-steward-s19.qWmWm6` and local build root were removed, and absence
was confirmed.

Passing commands:

- `go test ./cmd/t3-steward -run 'TestBacklogV2Production(ProcessTopology|Qualification)$' -count=1 -v`;
- `T3_S19_REMOTE_HOST=homelab T3_S19_REMOTE_COMMAND=/tmp/t3-steward-s19.qWmWm6/worker-exchange go test ./cmd/t3-steward -run '^TestBacklogV2AuthorizedMultiHostCanary$' -count=1 -v`;
- `go test ./cmd/t3-steward -run 'Test(BacklogV2ProductionQualification|CoordinatorLocalMultiProcessWorkflow)' -count=1 -v`;
- `go test ./internal/backlog ./internal/backlogadmin ./internal/backupsnapshot ./internal/store/sqlite ./internal/workerproto ./internal/workerruntime ./cmd/t3-steward -run 'Test(BacklogV2|Coordinator|Worker|Transport|Protocol|Schedule|Quota|Artifact|Backup|Restore|Legacy|Migration|Recovery)' -count=1`;
- `go test ./internal/backlog ./internal/store/sqlite ./internal/workerruntime ./internal/workerproto ./cmd/t3-steward -run 'Test(.*Restart|.*Replay|.*Lost|.*Duplicate|.*Reorder|.*Clock|.*Stale|.*Corrupt|.*Rollback|.*Exclusion)' -count=1`;
- `go test ./... -count=1`;
- `go build ./...`;
- `go vet ./...`;
- `go test -race ./... -count=1`;
- `git diff --check`.

No installation, service restart, live configuration/state access, external T3
dispatch, push, or pull request occurred. The only fleet-host contact was the
explicitly authorized no-effects qualification described above. All S19 gates
pass, so the readiness decision is **GO**; deployment still requires explicit
user approval.

### S18 production-binding evidence

S18 completed on Normandy using only disposable local roots and test child
processes. Focused audit, recovery, snapshot, local-admin, credential,
containment, archive, parser, and runtime package tests pass. Each new fuzz
target completed an explicit short fuzz run, the local-admin timeout/
backpressure test passed 25 repetitions, and the complete `go test ./...`,
`go build ./...`, `go vet ./...`, `go test -race ./...`, and
`git diff --check` gates pass. Snapshot tests include corrupt, incomplete,
format/schema mismatch, unsafe-file, active-owner, existing-target, and
canonical symlink-alias refusal. Recovery tests cover authenticated actor
replacement, authorization-before-write, epoch/revision/evidence validation,
exact and changed replay, and unchanged closed quota admission.

No installation, service restart, live configuration/state access, fleet
worker contact, external T3 dispatch, push, or pull request occurred during
S18. S19 subsequently completed under its explicit authorization boundary.

### S14 production-binding evidence

The following S14 gates passed on Normandy against disposable state:

- `go test ./internal/config -count=1`;
- `go test ./internal/store/sqlite -run 'Test(Open|Migrate|Migration|CoordinatorOwner|CoordinatorEpoch)' -count=1 -v`;
- `go test ./cmd/t3-steward -run 'Test(BacklogMutation|Coordinator|Run|Disabled|Closed|Legacy)' -count=1 -v`;
- `go test ./internal/config ./internal/store/sqlite ./cmd/t3-steward -count=1`;
- `go test ./...`;
- `go build ./...`;
- `go vet ./...`;
- `git diff --check`.

The migration-focused run covers the existing version 1, 5, 6, 8, and 9
fixtures plus no-implicit-migration, second-owner refusal, and epoch restart.

### S15 production-binding evidence

The following S15 gates passed on Normandy against disposable local processes
and in-memory protocol state:

- `go test ./internal/workerproto -count=1`;
- `go test ./internal/workerproto -run 'Test(ProtocolJSONGoldens|ExecutionPackageJSONGolden|Exchange|Artifact|SSH)' -count=1 -v`;
- `go test ./internal/workerproto -run 'TestSSHTransportLocalMultiProcess' -count=10`;
- `go test ./...`;
- `go build ./...`;
- `go vet ./...`;
- `git diff --check`.

The focused run covers protocol and execution-package JSON goldens,
unsupported versions, stale coordinator and worker epochs, authentication,
authorization, exact duplicate replay, changed replay, reordering, dropped
responses, retry/backoff, timeout, cancellation, message and stream limits,
backpressure, strict payload decoding, malformed/unsafe tar archives, artifact
and custody checksums, and the exact hardened SSH invocation. No SSH binary or
fleet address is contacted: the transport factory starts only child copies of
the test executable.

## Approval boundary

S15 completes only the worker protocol and transport-foundation stage. S16-S19
remain authorized development work in the production-binding chain, but no
installation or deployment is authorized. After the blockers above are resolved
and an S19 readiness report is GO, host-wide rollout still requires explicit
user approval.
