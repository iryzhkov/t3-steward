# In-task review checkpoint operation

A running campaign task asks the coordinator to open a review round for its
current work with one authenticated coordinator operation, and parks on that
round with a second one. The public command that calls both is
`t3-steward review --task current`, described at the end.

## Request and transport

The operation is the node-wait action `review-checkpoint`, carrying a
`checkpoint` request. It rides the node-wait operation for the reason
task-bound waits do: it is fenced by the same task identity under the same
administrator privilege, and authorization is scoped to the request's run and
task. A coordinator older than this operation refuses the unknown field whole.

The request is the task's own execution identity from
`.t3-steward/task.env` (run, task, attempt, issued revision, assignment and
thread), a checkpoint ID, and an optional head commit. Before calling, the task
pushes its work to `refs/heads/steward/<run>/<task>/<checkpoint-id>` on the
project remote.

The answer carries exactly one of a round or a refusal. A round names the round
ID and number, the checkpoint branch, the trusted base and head, the review
child workflow and run, one reviewer task per declared member, the review
deadline, and whether the answer replayed an earlier call. A refusal carries a
stable code, a reason and a retry flag, so it reaches the task intact across
both admin carriers.

## Fences, all before the first write

- The coordinator's epoch equals the durable epoch (`stale-coordinator-epoch`,
  retryable against the current coordinator).
- The attempt exists for the named run and task, is not finished, has a live
  turn, runs on the named thread under the named assignment, was issued no
  later revision than it holds, and is not superseded by a later attempt of the
  same task. Its assignment is claimed, names the attempt and its thread, and
  names a worker (`attempt-not-current`).
- The task declares `review:` requirements in its manifest
  (`review-not-declared`).
- Declared review admission resolves for the attempt (`admission-refused`).

The epoch check is not only made once. The remote head probe and child staging
take time, and a replacement coordinator can advance the epoch meanwhile, so
the coordinator's epoch is bound to every durable step: the authority freeze,
each checkpoint allocation, child materialization, and the read-only replay
that answers a repeated call each compare it with the durable epoch inside
their own transaction, the writers under the SQLite writer lock. A coordinator
superseded at any point is refused with `stale-coordinator-epoch` and writes
nothing further; the current coordinator completes the same checkpoint from
what was already durable.

## Trusted base and head

The base is the stored immutable workflow base frozen into the review
authority. The head is resolved by the worker assigned to the attempt, through
the non-caching exact-ref resolution of the worker repository probe, using the
project's own repository and credential references. A branch that is not on
the remote is refused (`branch-not-found`, retryable), so a worker-local commit
is never reviewed. A stated head is only compared with the resolved one; a
difference is refused (`head-mismatch`). A worker that does not advertise exact
ref resolution is refused (`worker-unsupported`).

## Order and idempotency

The steps are declared review resolution, freeze, checkpoint allocation, child
staging, a final allocation check, and child materialization. Allocation runs
before staging so that the round's durable creation time fixes the review
deadline: the deadline is the round's creation time plus 24 hours, and every
replay presents the same deadline to the retained stage.

The operation is idempotent per attempt and checkpoint ID. A repeated call with
the same remote head returns the same round, read without staging again. The
same ID whose branch now names a different head is refused
(`checkpoint-head-conflict`) before any write; new work goes to a new
checkpoint ID, which allocates the next round up to the frozen round limit
(`round-limit-exhausted`).

An error or crash between allocation, staging and materialization is answered
as `internal` and retryable. Every step replays, so the next identical call
completes the same checkpoint and creates exactly one child.

## Parking on the round

The node-wait action `review-checkpoint-wait` carries the same request. It is
fenced exactly like opening the round, requires the checkpoint's round to exist
(`checkpoint-not-open` otherwise), and registers the store's review parent wait:
one task-bound node wait per checkpoint, request ID `review-parent:<checkpoint
key>`, on the review child's sink, due at the round deadline. The answer is a
park whose status is `parked` (the attempt is held, the turn must end),
`settled` (this checkpoint's wait already settled, a replay after the task
resumed) or `finished` (the round was collected before anything parked on it,
or the round deadline has passed, when there is nothing left to park for). A
child that ended before the park but whose reviews are not collected still
parks the task: there is no verdict to report yet.

The child's sink can end before its reviews are collected. The wait settles on
the sink as it always has; the steward that delivers the wake holds it until the
round is collected, whether or not that wake is the one that resumed the turn
(another wait of the attempt can resume it first, and the review wake then
arrives mid-turn), or until 10 minutes past the round deadline, and then sends
it with the combined verdict, the blocking finding count, each reviewer's
verdict and blocking titles, and the workspace paths of the documents it placed
at `.t3/reviews/<round>/<reviewer>/review.md` and `verdict.json`. Those files
are written through real directories only, renamed into place, and excluded in
`.git/info/exclude`. If they cannot be written, the wake still carries the
verdict and says so.

## The command

`t3-steward review --task current [--checkpoint ID] [--json]` runs inside the
task workspace:

1. It resolves the task identity from `.t3-steward/task.env` (or the injected
   environment) and refuses outside a task.
2. It requires origin's push URL, which workspace preparation points at the
   project repository (`remote-missing` otherwise), lists this task's
   checkpoint branches there, and chooses the checkpoint ID: the `cp-N` that
   already names HEAD, so a repeated command replays its round after a lost
   answer, or the next unused `cp-N`. Numbering continues across attempts of
   the task, because the review authority and its checkpoints belong to the
   task.
3. It pushes HEAD with `--force-with-lease=<branch>:`, so a checkpoint branch is
   created once and never moved. A branch that already names another commit is
   refused as `checkpoint-head-conflict`; any other push failure is
   `push-refused`.
4. It opens the round, stating HEAD as the cross-check, then parks on it. When
   parked it tells the agent to end the turn. When the round is already over it
   writes the evidence into the workspace, prints the verdict, and says the task
   is not parked.

Reviewer, model, judge, role, risk, swarm, effort, policy, input and
notification flags are refused in task mode: the round's requirements come only
from the manifest `review:` declaration. Refusals print their code, the reason
and the next action. Exit codes: 0 parked or already complete; 1 invalid flags,
not inside a task, `remote-missing` or `push-refused`; 2 a coordinator refusal
that repeating cannot fix; 75 a retryable refusal; 3-8 the transport classes.

## Limits

- The head is not checked to descend from the base; that belongs to the final
  completion gate.
- Exact tag refs are not used; checkpoint branches carry commit semantics.
- The review deadline is a fixed 24 hours, not configuration.
