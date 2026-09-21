# Consultations pre-feature regression baseline

Baseline source: `f9f81b18195aa05c7cb186c754bf09af8252b76a`.
Captured: 2026-09-21 on the isolated `consultations/regression` worktree.

## Differential fixture

`TestConsultationsRegressionBaseline` uses the migrated SQLite store, fixed request IDs,
the repository fixture clock, and the real task-wait transaction boundary. Its checked-in
JSON receipt freezes these observable facts:

- WakeEach resumes after the first settlement; WakeAll waits for the complete set.
- both paths retain one assignment and create exactly two wait records;
- same-content replay retains one wait identity and one durable record;
- cancellation commits a cancelled outcome and resumes the same attempt once.

The receipt intentionally excludes no sessions, blockers, resource counts, outcomes, or
revisions. It compares semantic fields rather than generated wait IDs; those IDs are
already deterministic hashes of the fixed request IDs. Runtime state lives under
`t.TempDir()`.

Run it with:

```sh
go test ./internal/store/sqlite -run '^TestConsultationsRegressionBaseline$' -count=1
```

## Numeric comparison budgets frozen before feature work

Feature-absent and supported-but-unconfigured results must meet all of these budgets:

| Measure | Budget |
| --- | --- |
| Differential JSON receipt | byte-for-byte identical; zero added effects, waits, assignments, or outcomes |
| Extra model/advisor sessions | exactly zero |
| Extra consultation/context records | exactly zero |
| Ordinary scheduling decisions and authoritative receipts | exactly zero semantic differences |
| SQLite write transactions per no-feature fixture | no increase |
| Median ordinary scheduling throughput, at least 5 isolated samples | no more than 10% regression |
| p95 scheduling/reconciliation latency, at least 30 observations | no more than 15% or 100 ms regression, whichever allowance is larger |
| Peak retained no-feature state | no more than 5% growth after additive schema pages are excluded |
| WakeEach/WakeAll, replay, cancellation outcomes | zero failures or identity changes in 100 repeated fixture runs |
| Race detector | zero races |

A breach must be investigated. New sessions, changed blockers/resource counts, changed
outcomes, and altered wake identity are never normalized as noise.

For advising-enabled mixed loads, unrelated ordinary and supervised runs retain the exact
semantic budgets above. The performance budgets permit at most 15% p95 latency regression
and 10% throughput regression while advisor work is queued, active, timed out, or
cancelled. A one-slot worker must still admit ordinary work after a parked task releases
capacity.

## Existing section 16 coverage inventory

The baseline already has focused coverage for:

- ordinary DAG projection, artifacts, sinks, rerun/clone/amend and notification behavior;
- supervision compatibility, gate/hold replay, restart, race, identity, workspace and
  operator decisions;
- real SQLite WakeEach/WakeAll, mixed wait modes, settlement delivery, timeout,
  cancellation, rewind/rewake, terminal sweep and task identity fences;
- quota admission/recovery, throttle delivery/faults, worker admission and reconciliation;
- submission reservation/replay, schedule triggers, recovery, archive retention, backups,
  and pre-supervision schema migration;
- CLI help/JSON purity and worker enrollment/refusal contracts.

Representative tests include `task_wait_test.go`, `task_wait_runner_test.go`,
`supervision_compat_test.go`, `supervision_race_test.go`,
`supervision_restart_test.go`, `submission_test.go`, `schedule_trigger_test.go`,
`artifact_retention_test.go`, and `worker_reconcile_compat_test.go`.

## Gaps that remain for feature integration

This C0 fixture cannot cover code that does not exist at the baseline. Integration must
add the supported-but-unconfigured and enabled mixed matrices, consultation fault
injection at intent/effect/receipt boundaries, old-worker advisor-package refusal,
bursty-question fairness, one-slot and shared/different quota pools, and live pilot
evidence. Ordinary DAG external transport is covered across existing package tests rather
than by one end-to-end differential receipt. Supervision has strong independent store and
coordinator coverage but is not yet composed in this compact fixture. Retention coverage
exists for ordinary records; consultation context-pin ownership and rollback
reconciliation require the feature schema.

## Baseline command receipts

The repository-prescribed gates passed in this isolated worktree:

| Command | Result | Wall time |
| --- | --- | --- |
| `make test` (unit, full race suite, vet) | pass | 2m15.550s |
| `make lint` (pinned staticcheck and gofmt gate) | pass | 6.754s |
| `make build` | pass | 2.450s |
| `go test -race ./internal/store/sqlite -run '^TestConsultationsRegressionBaseline$' -count=100` | pass | 104.214s |

Local command logs are under `/tmp/steward-regression-make-*.log` and are not repository
artifacts.
