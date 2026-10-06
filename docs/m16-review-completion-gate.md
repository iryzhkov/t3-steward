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
and after verification commands have run, it reads:

- `git rev-parse HEAD` in the workspace, the physical HEAD;
- `git status --porcelain --untracked-files=no`, the tracked changes the
  workspace's own index reports, including staged ones; and
- the same status against a scratch index read from HEAD, which compares
  every tracked file by content.

The report is taken after verification because a verification command such as
a generator or formatter can rewrite tracked source, and that is work the
review never saw.

The workspace's index and Git configuration belong to the executor, so neither
is trusted to say what changed. Files flagged assume-unchanged or
skip-worktree, forged stat data and a configured file system monitor can all
make the workspace's own status skip a file; the scratch index carries no flags
and no stat cache, and both queries run with `core.fsmonitor` and
`core.sparseCheckout` off and without refreshing the workspace's index. Every
query names the task workspace as its worktree (`--work-tree`), and the
worker's `GIT_DIR`, `GIT_WORK_TREE`, `GIT_INDEX_FILE` and `GIT_COMMON_DIR` are
not passed on, so a `core.worktree` setting cannot point the comparison at a
clean copy elsewhere.

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

The worker cannot know whether the gate will accept its result, and a published
campaign ref is permanent, so a review-declared task's declared commit is only
staged while it finalizes: it is kept reachable under
`refs/campaign-staged/<run>/<task>/<attempt>/<name>`, with its provenance
record under the store's `staged/` directory, and the provenance artifact the
result carries already names the campaign ref it will have. Each attempt stages
under its own ref, so a retry that produces a different commit is not refused
by an earlier attempt's work.

The campaign ref `refs/campaigns/<run>/<task>/<name>` and its record are
created only when a dependent task whose execution package marks the producer
`accepted` consumes the commit. Consumption alone is not acceptance: a review
judge may run after its dependency failed and is given the failed result's
outputs to inspect. The coordinator therefore marks a dependency `accepted`
only for a review-declared producer whose packaged outputs all came from an
attempt that succeeded, and a package with such a mark requires the
`accepted-dependencies-v1` capability, so an older worker is never offered it.
The worker publishes a staged commit only for a commit reference that sits in
the accepted producer's directory of the dependency view and names that
producer as its task. Any other consumer fetches a staged commit from its
staged ref, under the campaign ref name in its own workspace, and the store's
campaign ref stays absent. Producers without `review:` publish directly, and
their consumers' packages are unchanged.

Until an accepted consumer fetches it, `Resolve` finds nothing for the task,
and a commit the gate rejected never becomes the task's output. Releasing a run
drops its staged refs and records with its published ones, and a run that only
staged commits still counts as held.

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

- Every tracked file is hashed at collection, because the scratch index has
  no stat cache. That is a cost in proportion to the repository's size, paid
  only by review-declared tasks.
- A staged commit is promoted on the worker that staged it, which is the only
  store that holds it, exactly as a published commit is fetched only from the
  worker that published it.
- The frozen review authority is bound to the task's first attempt
  (M16-1), so a retried attempt cannot open rounds of its own. A retry
  completes only if its HEAD is exactly the head the latest round accepted;
  otherwise a gate failure is in effect final for the task until M16-4
  decides how rounds carry across attempts.
- The latest round is read in its own snapshot rather than in the transaction
  that settles the attempt. That is safe because opening a round needs a live
  turn, and an attempt whose result is being imported is no longer live. A
  verdict recorded in between can only turn pending into a verdict, which at
  worst fails a task that would have passed a moment later.
- The head is not checked to descend from the frozen base.
- Round limits, escalation and cancelling open rounds when the task ends are
  out of scope (M16-4).
