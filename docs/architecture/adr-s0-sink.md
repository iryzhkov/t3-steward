# S0 ADR: a coordinator-owned terminal sink

Status: implemented in S1 (2026-09-12). Live graph amendments remain S3.

## Scenario and decision

A caller needs one stable node representing completion of a changing run. The
coordinator creates a reserved sink task at submission, with a stable run-scoped
ID, no provider route, worker, assignment or attempt. It depends on every other
task. Each graph revision recomputes that set in the same transaction.

The sink waits for terminal predecessors, unlike ordinary success dependencies.
It succeeds when none failed; otherwise it fails and records the sorted failing
task IDs. Cancelled/skipped IDs and the run cancellation state are also retained.
This follows the Track S rule literally: a cancellation-only run can have a
successful aggregate sink, but must never be described as successful execution.

## Ownership, commit and invariants

The coordinator owns sink creation, dependencies and result. CLI and workers
cannot mutate it directly. Submission, a future graph amendment and sink rebinding
are each single coordinator transactions. Settling the sink and its run completion
projection produces one durable event keyed by run (`sink-settled:<run>`).
The sink is stored inside the run record; it is not an executable task-table row.
The planner excludes the sink; graph/show include it, ordinary progress counts do
not. Reserved IDs and user-authored sink definitions are rejected at ingestion.

The sink cannot settle while a predecessor has an active, paused or recovery-required
attempt, pending retry, or unproven external stop. Descendants made impossible by
an exhausted failed dependency must become terminal skipped with an upstream reason;
placement/quota blocking alone is not terminal. This closes the graph rather than
leaving an unfinishable sink.

A published run completion is final. Commands refuse retries and task mutation
after sink settlement; use a new run for new work. Before settlement, a retry creates
a new attempt and delays the sink. An applied retry is durable work; a merely
queued command can lose the race with settlement and receives a rejected outcome. Successful artifacts
remain immutable. S3 implements cloning; until then resubmit a new bundle.

## Alternatives and failures

Polling the run's task list gives the caller a moving definition and duplicates
aggregation rules. A worker-executed final task wastes quota and can fail merely
because no worker is available. Both are rejected.

Restart reconstructs an unfinished sink from durable graph/progress, with no
external effect. Amendment and completion race on the graph/run revision: only
one commits. Cancellation waits for effect containment evidence before declaring
completion. Sink success alone does not erase explicit run cancellation; node waits
and dependencies use the cancellation rules in the node-wait ADR.

## S1 implementation and evidence

Empty graph, fan-out/fan-in, failed and skipped descendants, cancellation, pending
retry, amendment/rebinding fixture, restart at settlement and concurrent settlement
yield one sink, no sink attempt and one final event in the S1 tests.

Schema 12 backfills graph revision 1 and the stable sink on existing runs without
creating attempts. An unfinished sink is reconciled from durable task, attempt and
assignment records. Earlier attempts retain containment obligations after retries.
Exhausted blocked descendants are skipped only when no branch can still progress,
so a failed task can still be retried while an independent branch is running.
Projection compares the complete run-local read set inside its write transaction;
concurrent retries, task changes, outcomes and assignment changes invalidate it.
Finality is checked again in the admin apply transaction.

`backlog show` and `graph` include `__sink`; `task show <run>/__sink` reports its
result and failing/cancelled/skipped IDs. `status`, `list` and `show` accept
`--include-sink` to opt into counting it. Empty explicit `tasks: {}` bundles settle
without any worker. Omitting `tasks` remains invalid.

The pure rebinding fixture advances the graph revision and preserves the old sink
value. S3 must persist immutable graph snapshots and call the binding primitive in
its amendment transaction; S1 adds no amendment API, node wait or cross-run edge.

Tests: domain/sink_test.go, backlog/sink_projection_test.go,
store/sqlite/workflow_projection_test.go, backlogadmin/sink_test.go and
cmd/t3-steward/backlog_sink_test.go. They use disposable state and no provider quota.
Schema 12 is a forward migration: older binaries reject it. Restore a coherent
pre-migration backup only while stopped, or forward fix; do not resume an older
binary against the migrated state.
