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

- Completed stage: S16 — Restart-safe worker runtime.
- Exact full starting commit: `d6a79a074b1f102acee4b07b07c5724fa9e5e7f9`.
- Pre-existing worktree state: clean (`git status --short` produced no
  entries); no unexplained changes were present.
- Exit gates copied exactly from the authoritative plan:
  - Worker restart tests at every durable command/effect boundary.
  - Lost and ambiguous T3 response, stale epoch, lease loss, corrupt artifact,
    cancellation/whole-cgroup containment, throttling, checkpoint/resume, and
    unknown-execution tests.
  - Local coordinator-stub/worker multi-process test on disposable roots.
  - `go test ./...`, `go build ./...`, `go vet ./...`, and
    `git diff --check`.
- Focused tests selected before implementation:
  - `go test ./internal/workerruntime -count=1`
  - `go test ./internal/workerruntime -run 'Test(RuntimeRestart|LostAndAmbiguousT3|StaleEpoch|LeaseLoss|CorruptArtifact|Cancellation|Throttle|CheckpointResume|UnknownExecution)' -count=1 -v`
  - `go test ./internal/workerruntime -run TestLocalCoordinatorStubWorkerMultiProcess -count=10`
- All focused and full S16 gates passed. The first incomplete stage is now S17
  — Coordinator runtime, submissions, schedules, and quota bridge. Its
  successor must inspect the clean post-S16 commit and record its own exact
  starting state before implementation.

## Completed stages

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
- The coordinator runtime stops at closed authority. S17 must compose bundle
  ingestion, submissions, planning, schedules, quota bridging, worker exchange,
  atomic delivery/reconciliation, outcomes, artifact import, and admin
  execution. Hard closed quota admission remains authoritative for every path.
- Native non-admin audit, coherent coordinator/worker backup and restore,
  credential/epoch rotation procedures, and explicit unknown-state recovery
  remain S18 work.
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
