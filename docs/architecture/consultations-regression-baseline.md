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

The receipt records attempt revisions plus every wait's settlement, wake, outcome,
delivery state, delivery identity, wake revision, and resumption flag. It also records the
existing assignment-row count. That count is a narrow store invariant, not evidence about
model-session creation. Session counts, scheduler blockers, unrelated effects, and broader
resource accounting remain integration-gate measurements. Runtime state lives under
`t.TempDir()`.

Run it with:

```sh
go test ./internal/store/sqlite -run '^TestConsultationsRegressionBaseline$' -count=1
```

## Numeric comparison budgets frozen before feature work

Feature-absent and supported-but-unconfigured results must meet all of these budgets:

| Measure | Budget |
| --- | --- |
| Differential JSON receipt | byte-for-byte identical for the recorded wait transitions, revisions, delivery identities, assignment count and outcomes |
| Extra model/advisor sessions | exactly zero |
| Extra consultation/context records | exactly zero |
| Ordinary scheduling decisions and authoritative receipts | exactly zero semantic differences |
| SQLite write transactions per no-feature fixture | no increase |
| Median ordinary scheduling throughput, at least 5 isolated samples | no more than 2% regression |
| p95 scheduling/reconciliation latency, at least 30 observations | no more than 5% regression and no more than 25 ms absolute |
| Idle coordinator memory, at least 5 isolated samples | no more than 8 MiB or 2% growth, whichever allowance is smaller |
| WakeEach/WakeAll, replay, cancellation outcomes | zero failures or identity changes in 100 repeated fixture runs |
| Race detector | zero races |

A breach must be investigated. New sessions, changed blockers/resource counts, changed
outcomes, and altered wake identity are never normalized as noise.

For advising-enabled mixed loads, unrelated ordinary and supervised runs retain the exact
semantic budgets above. The performance budgets permit at most 5% p95 latency regression
and 2% throughput regression while advisor work is queued, active, timed out, or
cancelled. A one-slot worker must still admit ordinary work after a parked task releases
capacity.

## Measured pre-feature reconciliation baseline

`BenchmarkConsultationsBaselineReconciliation` runs the real SQLite no-work wake
reconciliation seam used on each coordinator boundary. Five 200-operation samples on the
baseline host measured 84,154–116,217 operations/second (median 101,678) and per-sample
p95 latency of 11.57–19.03 microseconds (median 14.54 microseconds). Compare on the same
host with the same command:

```sh
go test ./internal/store/sqlite -run '^$' -bench '^BenchmarkConsultationsBaselineReconciliation$' -benchtime=200x -count=5
```

This measures the reconciliation transaction only. It does not establish end-to-end
worker scheduling, model-session, or transport latency; those remain integration gates.

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

## One-slot fair-service qualification

The source order does not provide a wake-service bound. The coordinator process runs
ordinary assignment planning in coordinatorBoundaryCycle; task wakes are requested by
the separately scheduled worker wait runner. TestSettledParkedWakeGetsServiceUnderContinuousOrdinaryLoad
freezes a valid interleaving using the production coordinator boundary, planner, SQLite
assignment commit, and wake transaction. The fixture has one ready worker snapshot with
one executor slot, an open provider pool with maxConcurrent 1, one settled parked wait,
and replenished ordinary work. On each of eight passes the planner commits an ordinary
offer, that offer excludes the settled wake from the only slot, the ordinary assignment
completes, and the next ready attempt is added. The wake remains waiting-external; its
revision changes only through wait registration, not resumption.

The desired-contract test therefore fails at the current source revision with: settled
parked wake received no slot in 8 coordinator passes; control=waiting-external revision=3.

Eight is a finite regression bound, not a claim that starvation ends on pass nine. The
same legal ordering can repeat while ordinary work is replenished, so the current design
has no finite service bound. This is orchestrator evidence for one deterministic
interleaving; it does not claim that every deployment starves a wake.

The first repair stage adds one arbitration step inside the existing coordinator planner.
Before committing ordinary work, it builds the existing pure ordinary plan, compares each
settled wake with the oldest actually placeable ordinary proposal sharing its worker or
pool, admits older eligible wakes, then reloads and runs the existing ordinary commit.
A settled parked wake uses the earliest settlement time in its committed wake set and the
stable attempt ID. An ordinary proposal uses its attempt UpdatedAt value from the durable
ready state and attempt ID. Supervision activations are not included in this bounded stage;
their three-class fairness remains an integration requirement.

Compare contenders by ready time, then stable ID; kind is only a final deterministic
tie-breaker and must not grant a permanent class priority. For each worker snapshot, walk
that order once and commit at most the snapshot's available sized capacity. The commit
transaction must revalidate coordinator and worker epoch/sequence, snapshot freshness,
live attempt and assignment revision, wait settlement, supervision gates, route/provider
admission, pool concurrency and sized executor demand. A loser keeps its original durable
ready key for the next boundary. No slot is reserved between boundaries and no second
scheduler is introduced.

Feature-absent compatibility is narrow: with no settled wake or ready activation, ordinary
planning produces the same candidates and assignment semantics. With mixed contenders,
assignment order can change because an older wake or activation can win. Replay identities,
quota and worker fail-closed behavior, and capacity ownership states remain unchanged.
A repaired version of the fixture should pass with a stated finite bound and should also
assert that continuous wakes cannot permanently exclude ordinary attempts.

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
