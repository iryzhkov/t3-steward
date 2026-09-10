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

- Completed stage: S15 — Versioned worker exchange and execution package.
- Exact full starting commit: `ce1fd4dd45c27aae4ba99151b3a015523b6cce05`.
- Pre-existing worktree state: clean (`git status --short` produced no
  entries); no unexplained changes were present.
- Exit gates copied exactly from the authoritative plan:
  - Protocol and execution-package JSON goldens.
  - Unsupported-version, stale-epoch, authentication, authorization, replay,
    reordering, duplicate, drop, timeout, cancellation, limit, malformed archive,
    and checksum tests.
  - A bounded local multi-process transport test.
  - `go test ./...`, `go build ./...`, `go vet ./...`, and
    `git diff --check`.
- Focused tests selected before implementation and passed:
  - `go test ./internal/workerproto -count=1`
  - `go test ./internal/workerproto -run 'Test(ProtocolJSONGoldens|ExecutionPackageJSONGolden|Exchange|Artifact|SSH)' -count=1 -v`
  - `go test ./internal/workerproto -run 'TestSSHTransportLocalMultiProcess' -count=10`
- Full gates passed: `go test ./...`, `go build ./...`, `go vet ./...`,
  and `git diff --check`.

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

## Known blockers carried forward

- The S15 worker exchange, execution-package, artifact-transfer, and SSH
  foundation is not yet bound to a production worker or coordinator runtime.
  Restricted-command SSH principal and credential resolution remains S16 work.
- The coordinator runtime stops at closed authority; bundle ingestion, planning,
  scheduling, quota bridging, worker exchange, and admin execution are not yet
  composed.
- Schedule syntax/timer ownership, submission idempotency/limits, and the
  production quota observation bridge are incomplete.
- Native non-admin audit, coherent backup/restore, and explicit unknown-state
  recovery remain incomplete.
- Current complete-workflow evidence is same-process and temporary-state. S15
  adds only a disposable local multi-process transport exchange; no fleet or T3
  process was contacted.

## Successor rule

When a stage is complete, update the plan checkboxes and this handoff, commit
the entire stage, confirm the tree is clean, and queue exactly one successor
using the command in
`docs/plans/backlog-v2-production-session-prompt.md`. Do not queue on
`continue` or `needs-input`. S19 queues nothing.
