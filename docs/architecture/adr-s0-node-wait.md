# S0 ADR: durable node waits and cross-run dependencies

Status: implemented in S2 (2026-09-12); external delivery qualification remains S5.

## Scenario and decision

A waiting thread names a durable task, not a shell command that parses CLI text.
Add a typed node check to the existing wait registration lifecycle:
`wait add --task <run>/<task>`; `--run <run>` resolves to its sink. The local
admin socket resolves identity and observes coordinator state. No SQLite access
or worker credentials are given to the CLI.

Use the same resolver for bundle `needs: <run>/<task>`. Ordinary dependencies
still require verified success and published artifacts; terminal failure is an
outcome, not permission to run. A cancelled source run never releases a success
dependency, even when its cancellation-only sink has no failed predecessors.

## Identity and lifecycle

The coordinator owns a wait ID, authenticated target thread/host, target run/task,
registration revision, terminal observation/result, deadline and wake delivery ID.
States are pending, settled (success/failure/cancel/timeout), wake-pending and
delivered. Settling is a transaction with an outbox entry. Tick/restart never
re-evaluates a settled check, including dry-run checks.

A pending task wait follows the task's current attempt, including retries accepted
before the terminal observation commits. A delivered terminal observation is never
retracted by a later retry. It carries the observed attempt and completion revision;
a caller wanting a subsequent outcome registers a new wait. Run sinks are final
once published (sink ADR).

Exit semantics remain 0 for the requested successful settlement, 2 for terminal
failure/cancellation/timeout, and nonzero/non-2 while pending. Run cancellation is
reported explicitly rather than inferred solely from the sink aggregate status.

## Boundary and failure contract

The waiter is an admin client. A co-located worker transport may share a Unix
listener in S4, but gets only worker-protocol authority, not wait/admin mutation
rights. Reads remain available when an unrelated quota bucket is broken.

Wake delivery uses a stable effect ID and observe-before-retry semantics. The
coordinator can guarantee one durable wake intent. Exactly one externally visible
T3 wake additionally requires a T3 idempotent command or observable delivery token.
S2 must verify that seam; ambiguous delivery remains pending/recovery-required,
never falsely marked delivered. S5 tests the end-to-end property.

Per-component wait dry run records settlement once and exposes a held outbox item,
not repeated "would wake" messages. Turning delivery on releases that same intent.

Cross-run edges bind retained run/task identity and completion evidence. Resolve
all references and reject cycles across runs at submission/amendment under the
coordinator transaction. Pending references pin required metadata/artifacts against
retention. Missing, unauthorized or expired targets fail validation; they never
silently become success. No transport-side polling loop owns dependency truth.

## S2 implementation and evidence

Coordinator restart during wait, retry after registration, run cancellation,
timeout, unavailable target, duplicate registration, lost wake response, quota
failure on another route, retention pins and cross-run cycles. Tests must distinguish
one durable intent from proof of one external delivery.

Implementation: schema 13 stores each native registration, canonical target,
immutable terminal observation and delivery intent in one coordinator-owned row.
The owner-authenticated admin socket provides register/list/cancel/run-now. CLI
native operations never open SQLite or execute a shell check. Registration can
return an already-terminal observation; its exitCode has the same 0/2/1 protocol.
The registering command itself reports RPC success, not the observation exit code.
A stable --request-id makes registration replay safe, including after source
retention is released. Native groups are not supported; each registration has
one independent delivery identity.

The shared domain resolver follows the current attempt while pending and rejects
missing targets. Cross-run needs may be a scalar or list; ingestion resolves names
to IDs in the transaction, validates the combined graph including sink edges, and
pins source metadata/artifacts. These are ordering dependencies; cross-run
inputs_from imports are not added. Existing output custody rules still govern
source success. Pins for graph references are conservative and retained for the
life of the definition, including scheduled reuse. Native-wait pins release only
after delivery or cancellation. SQLite delete guards protect pinned metadata and
artifact catalog rows; no new filesystem collector is introduced.

Projection fences cross-run observations; assignment offer/claim and manual start
recheck source success. A cancelled source sink does not release consumers. Failed
external dependencies skip exhausted consumers so their sinks can settle.

Native settlement runs outside quota reconciliation. wait.dry_run is an optional
native-delivery override; when omitted it inherits policy.dry_run. Held intents
settle once without repeated would-wake logging. Legacy shell checks retain their
existing CLI and delivery policy. Native waits are visible with wait list --native
and keep their target threads busy for archive exclusion.

T3 seam: dispatch carries deterministic commandId and messageId derived from the
persisted delivery token. The adapter reads the most recent 100 turns and accepts
only a matching user message ID as positive delivery evidence. A committed sending
state survives a process death before or after dispatch; it never authorizes another
send. Missing evidence becomes recovery-required, even after a successful HTTP
response, until observation proves delivery. A message outside that bounded window
can remain unresolved. This intentionally sacrifices automatic retry liveness under
ambiguity; S5 must qualify the deployed server's behavior. No claim of universal
exactly-once external effects or live provider-backed wake success is made.

Tests cover retry after registration, restart, timeout, cancellation-only sink,
missing target, changed and exact registration replay, metadata pins, cross-run
sink cycles with transaction rollback, dependency release/skipping, authenticated
socket routing, held delivery, lost response, stable message observation, wrong-host
isolation and settlement while quota reconciliation fails.
