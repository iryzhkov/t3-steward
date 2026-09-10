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

- Completed stage: S14 — Authority, configuration, and storage lifecycle.
- Exact full starting commit: `ccd1d142e2ae612578682220d2b9cc43d90cc157`.
- Pre-existing worktree state: clean (`git status --short` produced no
  entries); no unexplained changes were present.
- Exit gates copied from the authoritative plan:
  - Focused config compatibility/rejection tests.
  - SQLite no-implicit-migration and explicit-migration tests from every supported
    schema fixture.
  - Coordinator ownership, epoch restart, second-owner refusal, admin
    submission-only, disabled-mode, and closed-startup tests.
  - `go test ./...`, `go build ./...`, `go vet ./...`, and
    `git diff --check`.
- Focused tests passed:
  - `go test ./internal/config -count=1`
  - `go test ./internal/store/sqlite -run 'Test(Open|Migrate|Migration|CoordinatorOwner|CoordinatorEpoch)' -count=1 -v`
  - `go test ./cmd/t3-steward -run 'Test(BacklogMutation|Coordinator|Run|Disabled|Closed|Legacy)' -count=1 -v`
  - `go test ./internal/config ./internal/store/sqlite ./cmd/t3-steward -count=1`
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

## Known blockers carried forward

- Worker exchange cannot yet carry a versioned execution package or artifacts.
- No authenticated coordinator/worker transport or production worker runtime
  exists.
- The coordinator runtime stops at closed authority; bundle ingestion, planning,
  scheduling, quota bridging, worker exchange, and admin execution are not yet
  composed.
- Schedule syntax/timer ownership, submission idempotency/limits, and the
  production quota observation bridge are incomplete.
- Native non-admin audit, coherent backup/restore, and explicit unknown-state
  recovery remain incomplete.
- Current end-to-end evidence is same-process and temporary-state only.

## Successor rule

When a stage is complete, update the plan checkboxes and this handoff, commit
the entire stage, confirm the tree is clean, and queue exactly one successor
using the command in
`docs/plans/backlog-v2-production-session-prompt.md`. Do not queue on
`continue` or `needs-input`. S19 queues nothing.
