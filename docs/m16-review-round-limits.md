# Review round limits and executor end

A task's `review.round_limit` defaults to 2 for routine work and 3 for risky
work. Its maximum equals that default. The first checkpoint freezes the limit
with the review authority. Every allocated checkpoint consumes one round,
including cancelled or timed-out rounds and rounds from earlier attempts.
Replaying the same checkpoint does not allocate a new round.

These are defaults adopted without Igor where the roadmap leaves the contract
open. A retry does not create a new authority and cannot open rounds under a
new attempt. It can succeed only at the latest accepted, current, clean head.
The lead's remedy is `t3-steward campaign rerun <run> --from <task>`, which
creates a new run with a fresh authority and round budget.

## Completion and attention

When completion needs a new review round but every round has been used, the
gate fails as `review-round-limit-exhausted`. It records `roundsUsed` and
`roundLimit`, the latest verdict, and the lead's rerun option, with
`newRoundNeeded` false. Rejected or invalid rounds, a changed workspace HEAD,
and a declared commit elsewhere all use this escalation. A pending latest
round still fails as `review-not-accepted`: the executor finished before the
round did. Dirty-tree failures retain the option to discard changes, and an
accepted, current, clean round at the limit passes.

Explain and task result show the persisted gate. Triage lists an action item
`review-round-limit` for each latest failed task attempt with that code,
including settled runs, with the concrete rerun command. Once any run's
recorded `Graph.RerunOf` names that exact source run and task, triage clears
the source action. The rerun's current state does not reopen the old action;
an exhausted task in the rerun can have its own action. Other tasks in the
source run remain actionable. No automatic retry, ask, or new notification
type is introduced.

## Cancellation and recovery

Before coordinator snapshots, reconciliation closes pending members when
their original executor custody has ended. The recorded reasons are:
`parent run ended`, `parent attempt ended`, `parent attempt superseded`,
`parent assignment ownership lost`, or `review deadline reached`.
Members fail with that reason in `Failure`; the deadline instead times them
out. Child attempts are cancelled with the existing `attempt-cancelled`
audit event, their authority tokens revoked, and unclaimed offers released.
Running workers remain subject to the existing stop/quiescence protocol.

A checkpoint allocated before a crash but never materialized has no child
attempts. The same pass closes all its pending members when the parent ends,
so the existing collector can publish its reply. While custody remains live,
the allocation is untouched for replay. Materialization after the parent ends
is refused.

Coherent draining, paused, paused-uncheckpointed and parked parents are live
for reconciliation and do not block the coordinator or review admission.
Existing review deadlines still apply to materialized rounds.

Writes use the SQLite authority writer lock and parent run row. Callers bind
their known coordinator epoch; replacement coordinators cannot write under an
old epoch. Reopening the store before the first reconciliation needs no
special recovery. Repeated passes do not bump revisions or repeat audit
events. Result import decides the completion gate before cancellation, and
cancellation does not rewrite it. No migration or round-record field is added.
