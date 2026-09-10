# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 through M7.
- Began M8 with the versioned `backlog.admin/v1` read contract in `internal/backlogadmin`.
- Added one transport-neutral query envelope and response DTO family for status, filtered workflow lists, workflow detail, DAG graphs, task detail, explanations, projected events, artifact metadata, schedules, workers, quota admissions, reservations, and resource locks.
- Added an explicit authorization seam. Every query is authorized before coordinator state is read, and constructing a service without an authorizer fails closed.
- Read queries compose one transactionally consistent coordinator snapshot with durable worker snapshots and quota-admission projections from temporary SQLite state.
- Workflow and task projections include DAG progress, the latest attempt, current assignment, quota reservation, resource-lock ownership/waiters, and a stable T3 thread link derived from the assigned worker's configured web base URL.
- Explanations deterministically expose dependency, timing, expiry, worker-placement, quota-admission, control-state, needs-input, and resource-lock blockers.
- Admin assignment DTOs omit lease and dispatch tokens. Artifact DTOs omit coordinator storage paths and expose only a relative download route.
- Added a frozen JSON golden fixture and temporary-state integration coverage for every read query kind, filters, authorization failure, version/target validation, missing records, and sensitive-field exclusion.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- `backlog.admin/v1` versions the envelope rather than relying on Go package versions, so CLI and future HTTP adapters can negotiate the same contract.
- The coordinator-side service accepts a reader interface and authorizer; adapters supply principals but never read SQLite or task files directly.
- DTOs deliberately project internal records instead of embedding assignment or artifact persistence records when those records contain execution capabilities or coordinator-only paths.
- Read results are deterministically ordered. JSON map encoding is covered by a golden response fixture.
- Current event results are deterministic events reconstructed from durable projections. They are explicitly not a substitute for the append-only audit stream required by the observability milestone.
- A paused nonterminal attempt remains a quota reservation while reporting that it no longer holds a provider concurrency slot.
- Stable T3 links use `<worker web base URL>/thread/<escaped thread ID>`; workers without a configured base URL expose no guessed link.

## Verification

- Baseline: `go test ./...`
- `go test ./internal/backlogadmin -count=1 -v`
- `go test ./internal/backlogadmin ./internal/store/sqlite -count=1`
- `go build ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Revision-checked start, delay, pause, resume, cancel, retry, skip, and schedule mutation commands are not implemented yet.
- Admin command persistence currently exists only as the generic coordinator projection; it needs a dedicated optimistic transaction, immutable request semantics, authorization action, durable audit event, and asynchronous outcome transition.
- The append-only audit event schema required by M8/M9 does not exist. The read endpoint currently reconstructs a useful but incomplete event timeline from current durable projections.
- Explanations reflect durable current state but do not persist the planner's full route-by-route decision or predict quota recovery beyond an explicit `not_before` time.
- The relative artifact download route is a DTO contract only; a future transport adapter must bind it to the coordinator's checksum-verifying artifact fetch path.
- The existing CLI still has host-local backlog paths and has not been rewired through `BacklogAdmin`.
- The top-level coordinator/worker service loop still needs concrete transport binding. No development code has contacted workers or dispatched work.
- No development code has opened live state, installed or restarted a service, pushed, or deployed.

## Exact next increment

Continue M8 by adding a dedicated coordinator transaction for immutable, authorized admin-command submission with optimistic target revisions and idempotent replay. Persist append-only audit events in the same transaction, model asynchronous pending/applied/rejected/failed outcomes, and expose those durable events and command outcomes through `BacklogAdmin`. Cover stale revisions, replay conflicts, authorization denial before mutation, transaction rollback, and restart persistence. Leave CLI rewiring for the following coherent increment.
