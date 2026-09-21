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

### Capacity at activation placement

An activation assignment is included in durable capacity ownership after commit: `capacityOwners` treats its live assignment as a one-slot owner, and ordinary planning sees it on the next pass. `PlaceActivation` itself checks worker freshness, capability, route hosting, and quota admission, but it does not check current executor occupancy before committing the activation assignment. On a full one-slot worker, placement can therefore over-assign before the next planning reconstruction observes the owner.

O0 leaves this unchanged because a correct fix needs one shared placement/commit capacity contract and a race fence, rather than a second approximate slot counter in supervision code. Consultation answering work must not rely on activation placement as proof of available executor capacity until that contract lands.
