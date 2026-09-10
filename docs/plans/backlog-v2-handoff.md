# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, the domain and compatibility foundation.
- Completed the first three M2 increments: strict version 2 manifest validation, atomic bundle ingestion with immutable static inputs, and deterministic DAG execution transitions.
- Added a persistence-independent `DAGExecution` state engine over coordinator workflow-run, task, and attempt records.
- Dependency-free attempts reconcile to `ready`; dependent attempts release only after an ancestor attempt has explicit success and successful verification.
- Missing explicit success and failed verification terminate the attempt as failed and never release descendants. Independent ready branches remain runnable after a failure.
- Failure leaves unfinished descendants blocked. Retry creates a new numbered attempt, retains failed history, reuses successful ancestors, and releases descendants only after the retry succeeds.
- Cancellation propagates through unfinished descendants while preserving completed tasks and their artifacts.
- Runtime graph validation rejects missing or repeated dependencies, self-dependencies, cycles, duplicate task/attempt identities, invalid attempt numbering, and cross-workflow/run records.
- Added chain and diamond tests plus strict completion, verification failure, independent-branch failure, cancellation, retry, invalid transition, cycle, and missing-dependency coverage.
- Legacy version 1 workflows and runner behavior remain unchanged.

## Decisions

- DAG transitions are deterministic in-memory operations. Callers receive a detached snapshot and will persist it atomically through the coordinator seam in a later integration increment.
- A task counts as successful when any of its attempts succeeded. Retrying a failed task therefore does not rerun or replace successful ancestors.
- Failed dependencies keep descendants in nonterminal `blocked` state so retry can release them. A run becomes failed only after no independent ready or active work remains.
- Cancellation is terminal for unfinished descendants, but never rewrites successful attempts.
- Every accepted transition increments the workflow-run revision once and updates its timestamp. Terminal runs receive a completion timestamp; retry clears it.
- Verification is supplied to the transition engine as a reconciled boolean. Executing commands and retaining reports belongs to the next artifact/verification increment.
- Bundle ingestion and the transition engine remain unwired from the live runner and live state.

## Verification

- `go test ./internal/backlog/... -run DAGExecution -count=1`
- `go test ./internal/backlog/... -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Transition snapshots are not yet persisted through dedicated revision-checked coordinator commands; the existing store remains a general snapshot upsert API.
- Declared outputs, verification execution and report artifacts, dependency artifact transfer, and output checksum validation remain unimplemented.
- A process or host crash in the narrow interval after bundle filesystem publication and before SQLite commit can leave an unreferenced bundle directory. Startup reconciliation or garbage collection should remove such orphans before production.
- Retry after ambiguous external side effects still requires later idempotency/manual-verification policy.
- The ingester and DAG engine are not wired into a submission command or the live runner.
- No development code has opened the live state database or dispatched work.

## Exact next increment

Implement declared output capture and verification for version 2 attempts: validate required outputs, run declared verification commands, retain command output/exit status and declared files as checksummed coordinator artifacts, and materialize selected dependency artifacts beneath the documented predictable task path. Add missing-output, checksum, dependency-transfer, and verification-command failure tests without wiring development code to the live runner.
