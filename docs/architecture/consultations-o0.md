# Consultations O0 lifecycle evidence

This note records the bounded O0 audit performed before consultation semantics were added. It distinguishes the intentional lifecycle repair from consultation work and from scheduler contracts that remain open.

## Intentional lifecycle delta

Before commit `9f89a2d`, the coordinator gated every overseer activation transition on current worker placement and quota admission. A completed, lost, or expired activation could therefore remain live when its route's quota was closed, quota reconciliation was unhealthy, or its worker was offline. Those transitions reconcile durable work that already happened; they do not start inference.

The coordinator now runs supervision reconciliation after an unhealthy quota pass with an empty, fail-closed admission policy. Within activation reconciliation, placement runs only for `trigger-fired` and `dispatch-undelivered`, the two signals that may create assigned work. Completion, revocation, loss, and spent-to-idle reconciliation proceed without placement. New and retried dispatches still require a current eligible worker and admitted quota before the activation lifecycle advances, so a failed dispatch cannot spend activation budget.

Focused evidence lives in `cmd/t3-steward/coordinator_activation_admission_test.go`:

- a completed activation reaches `spent/no-decision` and emits its operator escalation while admission is closed and no worker is available;
- an unhealthy-quota coordinator boundary still reconciles that completion, then permits spent-to-idle reconciliation while refusing the subsequent dispatch and preserving activation budget;
- a fresh activation remains undispatched under closed admission.

## Confirmed gaps

### Termination final report

`TriggerTerminationFinalReport` is declared and accepted by `SupervisionTriggerKind.Valid`, but the repository has no producer for that event. A supervised campaign therefore has no demonstrated path that requests the declared termination report. O0 does not invent when termination occurs, what evidence the report consumes, or whether report failure affects settlement. That producer and its settlement contract must be defined before implementation.

### Executor capacity repair

O0 makes the durable assignment the authoritative executor reservation. Offered, claimed, and unknown assignments retain one slot plus their frozen CPU, memory, and scratch demand; completed and released assignments release it. `ControlWaitingExternal` releases compute while preserving its assignment, workspace, and locks, and `WakeTaskWaits` changes it to `ControlResuming` only after atomically reacquiring current capacity. A settled wake that cannot reacquire remains parked and undelivered for a later pass.

Ordinary assignment-plan commit freezes `Task.ResourceDemand` onto the assignment while binding its graph revision and digest, and refuses a planner placement whose demand differs. Capacity reconstruction therefore never changes an existing reservation after a graph amendment. Historical ordinary assignments without frozen placement demand fail closed when the worker governs sized capacity or CPU class; slot-only legacy pools remain compatible. Overseer activations are explicitly known slot-only work.

Ordinary assignment-plan commits and activation-assignment commits check occupancy in their existing SQLite transactions. CPU class is a hard floor, and CPU units, memory, and scratch are summed with existing owners. Claim validates the reservation already made by its offer and therefore does not double-count it or revoke a valid commitment after a pool shrink. Parked work must satisfy the current pool when it resumes. Missing or stale worker evidence is fail-closed, including a stale zero-slot snapshot; only fresh, explicit zero configured slots preserves historical ungoverned behavior.

Activation placement filters full candidates before route selection, so a full eligible worker does not mask another eligible worker with capacity. The transactional commit remains authoritative, including across racing activation and ordinary offers.
