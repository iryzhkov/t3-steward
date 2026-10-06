# Review completion gate

A task whose manifest declares `review:` does not complete with work its
review never saw. When the worker collects the task's turn, it records the
workspace's physical HEAD, and the coordinator compares that HEAD with the head
of the task's latest review round before the task can succeed. The round
records are the ones the in-task review checkpoint operation writes
(docs/m16-review-checkpoint.md).

## What the worker records

The coordinator's offer builder marks a review-declared task's execution
package with the package capability `workspace-head-v1`. Only a worker that
advertises it is offered the task, so an older worker never runs one and returns
results that cannot be judged.

When the worker collects such a turn, after the task identity record is removed
and before verification commands run, it reads:

- `git rev-parse HEAD` in the workspace, the physical HEAD; and
- `git status --porcelain --untracked-files=no`, the tracked changes.

Declared file outputs, `.t3/` and `.t3-steward/` are not counted as changes,
and untracked files are not either. The report travels with the result as one
artifact of kind `git-state`, identity `workspace-head-<attempt>`, named
`git-state/workspace-head.json`. A workspace without a usable HEAD is reported
as an error, never as a clean empty head. A task without `review:` carries no
such artifact, and the coordinator rejects a result that carries one anyway.

## What the coordinator decides

Result import evaluates the gate for a review-declared task whose result is
otherwise a success: explicit success, verification passed, declared outputs
present. A result that already failed keeps its own reason, and no gate
decision is recorded for it. Otherwise the coordinator reads the task's latest
review round from its durable records at that moment and decides, in this order:

| Code | When |
| --- | --- |
| `review-required` | No review round exists for the task. |
| `review-not-accepted` | The latest round is pending, rejected, or its stored results do not re-validate as accepting. |
| `workspace-head-unknown` | The worker reported no usable HEAD; the gate fails closed. |
| `head-changed-after-review` | The physical HEAD is not the head the latest round accepted, or a declared commit output resolved to another commit. Both heads are named. |
| `dirty-tree-after-review` | HEAD is the accepted head, but tracked files differ from it. |
| `accepted-head` | The latest round accepted the physical HEAD and the tree is clean: the task may succeed. |

The latest round decides, not the best one. Acceptance is re-validated from the
round's retained reviewer results against the frozen review requirements, as
`CheckReviewAuthorityEvidence` does, and never read from a stored combined
verdict alone.

A failing decision fails the attempt with the reason
`review gate <code>: <detail>`. The decision, passing or failing, is stored on
the attempt as `reviewGate`, with the round, its checkpoint and verdict, the
reviewed head, the workspace HEAD, and the dirty paths or declared commit
concerned.

## Head invalidation

A commit after an accepted round moves HEAD away from the accepted head, so
that acceptance no longer completes the task: the decision is
`head-changed-after-review` with `newRoundNeeded`, and its detail says that a
new review round on the new HEAD is needed. A further accepted round on the new
head becomes the latest round, and the task then completes.

## Declared commits

A declared commit output must resolve to the accepted head. Its provenance
record's commit is compared with the head of the latest accepted round, and any
other commit fails the gate as `head-changed-after-review`.

## Where the decision is shown

- `campaign explain <run>/<task>` (and `backlog explain`) prints the decision
  as a detail line and returns it as `reviewGate` in JSON.
- `task result` prints `review gate: ...` under the task, and returns
  `reviewGate` in JSON.

## Restart

The gate reads only durable state: the round records and the worker's result.
A coordinator restart between a round's acceptance and the result's import
changes nothing, and the decision is part of the single transition that
settles the attempt.

## Limits

- The worker publishes a declared commit to its campaign ref while finalizing,
  before the coordinator's gate runs. A gate failure therefore leaves that ref
  naming the unreviewed commit, and a retry that produces a different commit is
  refused by the existing ref-redefinition rule.
- The head is not checked to descend from the frozen base.
- Round limits, escalation and cancelling open rounds when the task ends are
  out of scope (M16-4).
