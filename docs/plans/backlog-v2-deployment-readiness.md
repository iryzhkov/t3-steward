# Backlog-v2 deployment-readiness report

Date: 2026-09-10  
Candidate branch: `feature/backlog-orchestrator`  
Candidate baseline: `57f0b3269d8341e52170675863fe663566e8bfd3`

## Decision

**NO-GO for host-wide deployment.**

S14 binds strict backlog-v2 fleet/project configuration, explicit storage
migration, exclusive coordinator identity/epoch, and a disabled-by-default
closed-admission runtime skeleton into the production executable. It is not yet
a deployable fleet coordinator because authenticated worker exchange, execution
packages, planning, lease/command delivery, acknowledgement, reconciliation,
submission, schedules, and quota bridging are not composed.

No binary was installed, no service or live configuration was changed, no live
state database was opened by development code, no worker was contacted, and no
workflow was dispatched during S14.

## Candidate contents

- Immutable version 2 workflow bundles, strict DAG and artifact validation, and
  compatibility adaptation for current `t3-backlog` and `t3-job enqueue`
  Markdown.
- Deterministic planning across task class, deadlines, forecasts, reservations,
  workers, routes, shared quota pools, locks, and freshness.
- Atomic assignment/lease/command persistence, deterministic T3 dispatch
  identity, lost-response recovery, and fail-closed ambiguous worker state.
- Checkpoint-aware throttle pause/resume with hard closed-admission enforcement.
- Schedule singleton, occurrence idempotency, suppression, misfire, and failure
  hold semantics.
- Coordinator-owned checksum-verified artifacts and safe admin retrieval.
- Versioned transport-neutral admin DTOs, read projections, revision-fenced
  mutations, durable outcomes, and audit events.
- SQLite schema version 10 with migration coverage from versions 1, 5, 6, 8,
  and 9, including legacy admin-command event backfill.
- A complete temporary SQLite workflow exercising dependencies, artifacts,
  verification failure, retry, pause, restart, resume, success, and duplicate
  schedule suppression.

## Deployment blockers

1. Bind bundle submission and schedule-definition administration to the
   coordinator service.
2. Implement and authenticate the production coordinator/worker transport for
   snapshots, planning, claims, leases, durable command delivery,
   acknowledgements, reconciliation, and artifact transfer.
3. Compose planning, scheduling, quota observation, worker exchange, admin
   execution, and recovery beyond the current closed-admission authority
   skeleton.
4. Run an observe-only multi-process integration test, then a non-side-effecting
   canary on disposable state. Repeat all release gates after those changes.
5. Define native audit-event emission for the remaining non-admin state
   transitions, or explicitly accept the current reconstructed read views.

The shipped schema, explicit migration lifecycle, exclusive coordinator
ownership/epoch, and submission-only admin boundary are now implemented. The
remaining items are implementation blockers, not operator toggles; deployment
approval alone must not bypass them.

## Operational risks retained

- SQLite and artifact files must be backed up and restored as one coherent unit.
- Schema migration is explicit and coordinator-owned. A candidate must never be
  pointed at live state before its coherent backup is verified.
- Exclusive ownership uses a host-local file lock; future cross-host transport
  must not turn it into distributed leadership.
- The first candidate dispatch or resume is the rollback point after which
  worker/T3 reconciliation is mandatory.
- Worker loss remains fail closed as `unknown`; automatic reassignment without
  proof of stop can duplicate external effects.
- Deterministic assignment and dispatch prevent duplicate active scheduling but
  cannot make arbitrary external side effects exactly once.
- Paused required work retains remaining-cost reservations; incorrect operator
  deletion can over-admit quota.
- Inline artifact display is intentionally restricted. Binary or oversized
  content must be downloaded, and checksum failure blocks use.
- Fleet clock skew and stale worker/quota observations close admission.
- Existing systemd timers may continue producing compatible schedule-marked
  submissions during migration; duplicate occurrence and open-run checks must
  remain authoritative.

## Documentation evidence

- [State, ownership, boundaries, contracts, primitives, invariants, transactions,
  evidence, and production-binding sequence](../architecture/system-model.md)
- [Operations, configuration, manifest, recovery, backup, rollback, and
  deployment order](../backlog-v2-operations.md)
- [Version 2 example bundle](../examples/backlog-v2/workflow.yaml)
- [Admin command and artifact safety](../backlog-admin.md)
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

## Approval boundary

S13 completes release-candidate preparation only. Queue no implementation
successor and perform no deployment. After the five blockers above are resolved
in a separately authorized development effort and its updated readiness report
is GO, host-wide rollout still requires explicit user approval.
