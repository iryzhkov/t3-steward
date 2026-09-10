# Backlog-v2 deployment-readiness report

Date: 2026-09-10  
Candidate branch: `feature/backlog-orchestrator`  
Candidate baseline: `57f0b3269d8341e52170675863fe663566e8bfd3`

## Decision

**NO-GO for host-wide deployment.**

The code is a release candidate for the backlog-v2 domain, persistence, planner,
worker protocol, orchestration, artifact, throttle, schedule, and admin seams.
It is not yet a deployable fleet coordinator because the production executable
does not load the backlog-v2 fleet/project configuration or run authenticated
coordinator/worker planning, lease, delivery, acknowledgement, and
reconciliation transports. The legacy Markdown backlog remains the wired
production path.

No binary was installed, no service or live configuration was changed, no live
state database was opened by development code, no worker was contacted, and no
workflow was dispatched during release preparation.

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

1. Bind validated project catalog, setup profiles, worker policy, quota-pool
   mapping, coordinator storage, and transport settings into the shipped
   configuration schema.
2. Bind bundle submission and schedule-definition administration to the
   coordinator service.
3. Implement and authenticate the production coordinator/worker transport for
   snapshots, planning, claims, leases, durable command delivery,
   acknowledgements, reconciliation, and artifact transfer.
4. Run an observe-only multi-process integration test, then a non-side-effecting
   canary on disposable state. Repeat all release gates after those changes.
5. Define native audit-event emission for the remaining non-admin state
   transitions, or explicitly accept the current reconstructed read views.

These are implementation blockers, not operator toggles. Deployment approval
alone must not bypass them.

## Operational risks retained

- SQLite and artifact files must be backed up and restored as one coherent unit.
- Schema migration is automatic on open. A candidate must never be pointed at
  live state before its coherent backup is verified.
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

- [Operations, configuration, manifest, recovery, backup, rollback, and
  deployment order](../backlog-v2-operations.md)
- [Version 2 example bundle](../examples/backlog-v2/workflow.yaml)
- [Admin command and artifact safety](../backlog-admin.md)
- [Approved implementation plan](backlog-v2.md)
- [Serial implementation handoff](backlog-v2-handoff.md)

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

## Approval boundary

S13 completes release-candidate preparation only. Queue no implementation
successor and perform no deployment. After the five blockers above are resolved
in a separately authorized development effort and its updated readiness report
is GO, host-wide rollout still requires explicit user approval.
