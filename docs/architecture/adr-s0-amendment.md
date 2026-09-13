# S0 ADR: graph revisions and amendment authority

Status: accepted by the user on 2026-09-12; implemented in S3.

## Decision

Use a live coordinator amendment API behind the existing authenticated admin
socket, with immutable graph revision records and revision-fenced commands.
Keep run/task identity stable; keep submission bytes and prior revisions immutable.
A resubmission with supersession remains useful for cloning but is a poor default
for adding a node to a run that another workflow already references.

The user selected live coordinator amendments with immutable graph revisions after
S0. S1 recorded that decision; S3 implements the owner-authenticated API.
Supersession is not the amendment mechanism.

## Implemented contract

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
Task additions require at least one nonblank, NUL-free verification command.
The CLI accepts repeatable `--verify COMMAND` on `task add` and `task set`;
setting verification replaces the entire ordered list. Existing unassigned tasks
can acquire missing verification through a revision-fenced amendment. Assignment
history and terminal-task fences still apply.
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
report the graph revision used. Tests cover these boundaries with disposable
SQLite, sockets, files and worker/T3 fixtures; no live provider effects are claimed.

## Operational details

Schema 14 retains revision 1 on submission or migration. The current run contains
its amended definition snapshot; the original workflow task catalog remains the
submission template. Scheduled siblings keep their original definitions. Every
changed unassigned attempt advances its execution revision in the graph transaction,
so a stale plan cannot offer it. Assignment records and execution packages pin the
graph revision, task revision and task digest used at offer time.

`task add` takes a bounded inline prompt (at most 256 KiB); prompts are retained
before metadata publication. A failed transaction can leave an unreferenced
content-addressed blob, never a graph pointing at unpublished bytes. External edges
resolve aliases to retained identities. Exact request replay uses the original
request and actor, including after a lost response. Routing/placement validation
uses coordinator configuration; option values remain provider-defined strings.
`task set` route edits require a single route to avoid silently changing alternatives.

`--timeout` is an elapsed assignment budget, including preparation and delivery,
measured from the immutable package creation time. Worker reconciliation enforces
it at exchange boundaries. Ambiguous stop remains stopping; only observed stopped
or missing execution permits a failed outcome. Zero disables the budget. Pauses do
not reset it. A disconnected worker cannot enforce a wall-clock bound until its
runtime reconciles again; S5 must qualify this with live T3.

Clone preserves the submitted workflow identity but gives the new run, all tasks,
attempts, input metadata and future dispatches new identities. It verifies retained
inputs, references their immutable bytes, preserves explicit external dependencies,
and records source run/revision provenance. It does not copy worker outputs,
assignments or success. Normal admission still applies. Source metadata is pinned
for the clone lifetime. No original run is cancelled or superseded.

`diagnose <run>` (also `backlog diagnose`) emits one JSON evidence bundle with the
current graph revision, revision history metadata, status, events, explanations,
assignment/lease state, native waits and bounded worker journal excerpts. Worker
snapshots and wait reads carry their own times; this is not a distributed atomic
snapshot. Missing or older-worker journal evidence is explicitly unavailable.
