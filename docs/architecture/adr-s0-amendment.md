# S0 ADR: graph revisions and amendment authority

Status: accepted by the user on 2026-09-12; S3 owns implementation.

## Decision

Use a live coordinator amendment API behind the existing authenticated admin
socket, with immutable graph revision records and revision-fenced commands.
Keep run/task identity stable; keep submission bytes and prior revisions immutable.
A resubmission with supersession remains useful for cloning but is a poor default
for adding a node to a run that another workflow already references.

The user selected live coordinator amendments with immutable graph revisions after
S0. S1 records that decision; S3 implements the API. Supersession is not the
amendment mechanism.

## Proposed contract

A workflow is the immutable submitted definition and input bundle. A run begins
at graph revision 1; a graph revision is an immutable task/edge definition snapshot
plus parent revision, actor, reason, request ID, content digest and audit event.
Attempt revisions fence execution transitions and are distinct from graph revisions.
Run progress remains mutable and derives from its current graph and attempts.

Only the coordinator creates tasks. An operator, or an agent explicitly granted
run-scoped amendment authority, submits intent. A worker may return a proposal as
an artifact; ordinary worker protocol credentials confer no graph/admin authority.

Expose `task add`, `edge add|remove`, `task set` and `run clone --from`.
Each amendment requires expected graph revision, stable request ID and reason.
Validate the complete local/cross-run graph, placement, routes, immutable inputs
and authorization before the transaction. Revalidate against current progress and
assignment state inside it. Commit the revision, new definitions, sink rebinding
and audit event together. Exact retries return the committed result; changed replay
or stale revision is rejected.

Task definitions already used by any offered/claimed assignment are frozen for
that execution. Model/provider/options/timeout changes and incoming-edge changes
are permitted only for unassigned, nonterminal tasks. Refuse terminal task edits,
cycles, removal of required artifact sources and edits that invalidate a running
task's dependency evidence. Adding a successor of a running task is safe if the
new task remains unassigned. No edits after the sink's final completion.

Execution packages retain their original graph/task revision and content digest;
an amendment never changes a package already sent to a worker. Dispatch and
amendment must fence the same task definition to prevent an obsolete queued
package being offered. Retry creates an attempt, not an edit to execution history.

## Alternative: resubmission and supersession

A revised bundle creates a new run and a durable supersedes link. The old run is
not implicitly cancelled; its effects must settle or be explicitly contained.
Reusing outputs requires verified custody and explicit references. Existing waits
and dependencies continue pointing at the old run unless explicitly rebound.
Making them automatically follow supersession introduces a second mutable
identity mechanism and cancellation race, which is why the live API is preferred.

Cloning deliberately uses new run, task, attempt and dispatch identities and
records provenance. It never guesses that earlier external effects are safe to repeat.

## S3 evidence required

Concurrent amendments, stale revision, replay after lost response, cycle rejection,
assignment/amendment races, input publication rollback, terminal edit refusal,
sink rebinding and clone identity tests. `graph --dot` and `diagnose <run>`
must report the graph revision used. The mechanism is approved; implementation remains in S3.
