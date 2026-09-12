# S0 ADR: a coordinator-owned terminal sink

Status: accepted design; implementation belongs to S1.

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
projection produces one durable event keyed by run and completion generation.
The planner excludes the sink; graph/show include it, ordinary progress counts do
not. Reserved IDs and user-authored sink definitions are rejected at ingestion.

The sink cannot settle while a predecessor has an active, paused or recovery-required
attempt, pending retry, or unproven external stop. Descendants made impossible by
an exhausted failed dependency must become terminal skipped with an upstream reason;
placement/quota blocking alone is not terminal. This closes the graph rather than
leaving an unfinishable sink.

A published run completion is final. Recommend refusing amendments and retries
after sink settlement; use a clone for new work. Before settlement, a retry creates
a new attempt and delays the sink. This tightens today's ability to retry a settled
run and must be explicit in S1/S2 command errors and tests. Successful artifacts
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

## S1 evidence required

Empty graph, fan-out/fan-in, failed and skipped descendants, cancellation, pending
retry, amendment/rebinding fixture, restart at settlement and concurrent settlement
must yield one sink, no sink attempt and one final event. S0 adds no sink code.
