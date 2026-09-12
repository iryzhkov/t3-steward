# S0 ADR: durable node waits and cross-run dependencies

Status: accepted design; implementation belongs to S2.

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

## S2 evidence required

Coordinator restart during wait, retry after registration, run cancellation,
timeout, unavailable target, duplicate registration, lost wake response, quota
failure on another route, retention pins and cross-run cycles. Tests must distinguish
one durable intent from proof of one external delivery. No node-wait code in S0.
