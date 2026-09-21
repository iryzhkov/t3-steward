# Consultation transition seams (C0 contract audit)

Status: source-owned C0 contract draft. This document freezes the smallest transaction
interfaces needed to add consultation subscriptions without changing ordinary wait algebra.
It does not define the full consultation schema and does not claim that C0 has passed.

Source baseline for this audit is the O0 branch through `061eea7`. The reviewed design is
`jocasta:288638cf804a0571f75daa2df06e0cfd@5`.

## Existing owners

The reusable parking owner is `sqlite.Store.RegisterTaskWait`
(`internal/store/sqlite/task_wait.go`). In one transaction it loads the authoritative
attempt, verifies run/task identity, a live turn, canonical thread ownership, and that the
package revision is not from the future; then it advances the attempt to
`ProgressWaitingExternal/ControlWaitingExternal` and inserts the wait. The request's
`IssuedRevision` is evidence, not the compare-and-set fence. The transaction's loaded
attempt revision is the fence.

`sqlite.Store.SettleTaskWait` and `settleTaskWaitTx` make the first outcome immutable.
`sqlite.Store.WakeTaskWaits` groups settled waits, checks the authoritative attempt and
assignment, reacquires executor capacity, advances a parked attempt to
`ProgressActive/ControlResuming`, and commits `WokenAt`, `WakeRevision`,
`DeliveryID`, delivery state, and the attempt revision together.
`sqlite.Store.TransitionTaskWake` then owns pending/held/sending/recovery-required/
delivered/abandoned delivery transitions. A durable `sending` state cannot be retried
without positive delivery observation.

Administrative forced resumption is `planAttemptCommand` for
`AdminCommandRewake` in `internal/backlogadmin/execution.go`. It refuses while an
ordinary wait is live, requires the claimed assignment, and applies the same
`ProgressActive/ControlResuming` state change. Attempt cancellation and skipping are
separate terminal transitions in that function. Ordinary wait cancellation is
`sqlite.Store.CancelTaskWait`, which preserves an already committed outcome.

The current overseer batching primitive is `backlog.ActivationInbox` and
`CoalesceSupervisionEvents` in `internal/backlog/supervision_activation.go`.
`HighWaterMark` is a cursor over events; it is not per-subject completion evidence.
Consultation membership therefore cannot be inferred from that cursor.

## Minimal records and APIs

These are logical shapes. Names may be adjusted with the additive migration, but their
fields and transaction boundaries are required.

```go
type ConsultationSubscription struct {
    ConsultationID string
    WaitID string
    RunID string
    TaskID string
    AttemptID string
    AssignmentID string
    WorkerID string
    WorkerEpoch int64
    ThreadID string
    RegisteredAttemptRevision int64
    State string // attached, detached, settled
}

type ConsultationWakeResult struct {
    ConsultationID string
    Outcome string
    AnswerDigest string
    ResponseRevision int64
    RespondentID string
    RespondentEpoch int64
    ContextVersion string
    Reason string
}

type ActivationConsultationMember struct {
    ActivationID string
    ActivationEpoch int64
    ConsultationID string
    QuestionDigest string
    State string // selected, answered, failed, cancelled
}
```

The store needs four narrow transactional entry points:

1. `AcceptConsultationAndMaybeSubscribe`: validate the authenticated caller capability
   against the live assignment, attempt, worker ID/epoch, thread, pinned graph/task
   identity, recipient policy, limits, and canonical payload. Insert the consultation and
   dispatch intent; when synchronous, also insert the exclusive subscription and park the
   attempt in the same transaction. Same-key/same-payload replay returns the original
   receipt; changed replay fails.
2. `AttachConsultationSubscription`: in one transaction either return the immutable
   terminal result or bind the request to the current live attempt/assignment/worker
   epoch/thread and park it. This closes answer-before-await.
3. `CommitConsultationOutcome`: check request and execution epoch, deadline at commit
   time, byte custody, and first-terminal-outcome ownership. Commit the response plus the
   attached wait settlement (or durable settlement intent) atomically.
4. `DetachConsultationSubscription`: compare the consultation, attempt, thread,
   assignment, worker epoch, and registered attempt revision; detach without discarding a
   terminal response. Forced resumption, retry, cancellation, skip, and authority
   revocation must call this in their owning transaction or persist a revocation intent
   that every later response/wake commit checks.

Consultation delivery should reuse `WakeTaskWaits`,
`TaskWaitWakeContext`, and `TransitionTaskWake`. The consultation wait result carries
the compact answer and provenance, while the existing stable `DeliveryID`,
`WakeRevision`, resumption flag, and uncertain-send states remain authoritative.

## Transaction table

| Race or transition | Required single transaction and guards | Durable result |
| --- | --- | --- |
| Ask with default await | Capability use + canonical replay check + request/dispatch insert + exclusive subscription + attempt park. Guard live assignment/attempt, exact worker epoch and thread, pinned task identity, policy and limits. | One consultation receipt and one parked attempt, or no writes. |
| Async ask then await | Read terminal outcome and attach subscription under the same write transaction. Apply the same caller authority guards as ask. | Immediate terminal result, or one attached park. |
| Ordinary wake already committed but undelivered | Before attaching, inspect all attempt waits using `TaskWait.Parking()` and delivery state, not only `Live()`. Refuse when any ordinary wake is committed but pending/held/sending/recovery-required or otherwise still owns the wake window. | No consultation subscription and no second park; response remains explicitly readable or awaitable in a later live turn. |
| Simultaneous answer and await | Both contend on the same consultation row. Answer commits a terminal response; await then returns it. Await commits attachment; answer then settles that exact attachment. | Never a terminal answer plus an unbound sleeping caller. |
| Stale worker authority registers | Verify capability assignment ID, worker ID/epoch, attempt ID, thread ID, purpose, expiry and one-use state against current authoritative rows in the registration transaction. A newer worker snapshot or replacement assignment does not inherit authority. | Refusal with no request or subscription writes. |
| Answer settlement | Verify respondent execution/activation epoch, membership, deadline and content custody, then commit immutable response and wait result. Exact accepted replay returns the receipt; changed or late response fails. | Response completion is distinct from wake delivery. |
| Normal wake | Reuse `WakeTaskWaits`: verify parked identity, assignment worker epoch and capacity; CAS the attempt revision into `ControlResuming`; mark the stable wake pending in the same transaction. | Capacity refusal leaves the settled subscription parked and pending. |
| Forced rewake/resume | In the admin-command transaction, detach the subscription before changing the attempt. Refuse if an ordinary committed wake still owns delivery. The answer remains stored and is never injected into the newly busy turn. | Resumed attempt has no attached consultation; later explicit read/re-await is possible. |
| Attempt cancel/skip/retry or authority revocation | Revoke caller capability and detach/cancel all nonterminal owned consultations before the attempt transition commits. Retry gets no inherited request, wait, delivery or capability identity. | Old responses cannot wake the replacement attempt. |
| Consultation cancellation | Commit terminal cancelled outcome and revoke response authority. Remove only this request from a shared activation membership; do not cancel the activation while another selected consultation or gate obligation survives. | Cancellation is immutable; execution containment remains separately visible. |
| Activation selection | Select at most the bounded batch and insert `ActivationConsultationMember` rows in the same transaction that raises the activation epoch/dispatch intent. Membership is immutable for that activation epoch. | New/unselected requests remain queued regardless of inbox cursor. |
| Activation outcome | Each selected member must become answered, cancelled, or explicitly failed before activation closure. High-water advancement consumes events only and cannot complete consultation membership. | Selected-but-unanswered fails explicitly; unselected stays queued. |
| Wake send uncertainty | Reuse `TransitionTaskWake`; `sending` to delivered/recovery-required requires the stable delivery group and positive observation rules. | No blind second send or second resumed turn. |

## Required ordinary-behavior guards

Consultation await is exclusive for one parking episode. Registration must refuse any
ordinary live wait and any already-settled wake whose delivery window still owns the
attempt. It must not reinterpret `WakeEach` or `WakeAll`, add consultation members to an
ordinary all-set, or turn a later settlement into an unsolicited message to a busy turn.

A stale capability cannot be repaired from request strings. Issuance is worker-only and
must occur for the authenticated current assignment/attempt. Use checks repeat at every
ask, await, inspect-own and cancel-own mutation. The deterministic dispatch token is an
identity component, not a secret.

Selected overseer membership is independent of `ActivationInbox.HighWaterMark`,
`CoalescedTrigger.EventIDs`, and the outbox event list. Those fields remain evidence and
cursor inputs. The membership row is the authority for which consultation IDs this
activation may answer and which IDs require explicit failure at its termination.

## Unresolved safety decision

The reviewed plan says cancellation settles the caller's wait while responder resources
remain held until stop/containment evidence, and also says a run sink cannot settle while
related effects are uncontained. It does not unambiguously decide whether the caller may
resume immediately with `cancelled` while its separately routed answer execution is still
uncontained.

C1 must choose and test one rule before schema implementation:

- allow caller resumption after cancellation, while a separate uncontained-effect blocker
  prevents sink settlement and preserves execution resources; or
- keep the cancelled subscription parked until containment is observed.

The first gives prompt cancellation but permits caller work concurrent with an ambiguous
respondent effect. The second gives the strongest sequencing but can hold the caller
through a worker outage. No implementation should infer the choice from
`TaskWait.Result`, because ordinary settlement and execution containment are currently
separate facts.

## C0 evidence still required

This audit does not prove the new transactions, schema migration, caller capability
adapter, mixed-wait refusal, stale-epoch races, cancellation/containment rule, or selected
batch accounting. C0 remains open until real SQLite race tests cover those boundaries and
the chosen containment rule is frozen.
