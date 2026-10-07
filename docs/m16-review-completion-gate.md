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

When the worker collects such a turn, after the task identity record is removed,
after verification commands have run and after every declared commit has been
staged, it reads:

- `git rev-parse HEAD` in the workspace, the physical HEAD;
- the workspace's own index (`git ls-files --stage`), compared entry by entry
  with HEAD's tree, which reads no worktree file: an entry that is missing,
  extra, unmerged or names another mode or object is a change; and
- a status against a fresh index read from HEAD in a scratch repository of
  the worker's own, which compares every tracked file by content, mode and
  type.

The workspace and every checked-out submodule, at any depth, are judged by
the same two comparisons, against the commit their parent's HEAD records. A
change in any repository's index or worktree is therefore a change, whether
or not that repository's HEAD moved; untracked and ignored files that no
index tracks are not counted at any level.

The report is taken after verification because a verification command such as
a generator or formatter can rewrite tracked source, and that is work the
review never saw. It is taken after staging for the same reason: it must
describe the workspace after everything collection runs in it.

Staging itself runs nothing the executor controls. The campaign ref store
fetches the declared commit from the workspace's repository instead of the
workspace pushing it, so the workspace's hooks (`pre-push`,
`reference-transaction` and the rest), its remote and URL configuration and any
receive-pack command it names never run; only an upload-pack, which runs no
hook or repository-configured command, reads the workspace's repository. The
store then reads the ref back, because Git can decline the update, as it does
for a shallow source, and still exit successfully.

No Git command the worker runs in the workspace during collection may reach a
remote. A repository the executor configures as a partial clone would fetch a
missing object from its promisor remote, running that remote's upload-pack
command as the worker. Those commands run with `GIT_NO_LAZY_FETCH=1` (Git 2.45
and later) and `GIT_ALLOW_PROTOCOL=none` (every supported Git), so a missing
object is an error and the capture fails closed.

The workspace's index and Git configuration belong to the executor, so neither
is trusted to say what changed. Files flagged assume-unchanged or
skip-worktree, forged stat data and a configured file system monitor can all
make the workspace's own status skip a file; the scratch index carries no flags
and no stat cache, and both queries run with `core.fsmonitor` and
`core.sparseCheckout` off and without refreshing the workspace's index. Every
query names the task workspace as its worktree (`--work-tree`), and the
worker's `GIT_DIR`, `GIT_WORK_TREE`, `GIT_INDEX_FILE`, `GIT_COMMON_DIR` and
`GIT_REPLACE_REF_BASE` are not passed on, so a `core.worktree` setting cannot
point the comparison at a clean copy elsewhere. Replace refs are ignored
(`--no-replace-objects`), because one could substitute another tree for HEAD's
while HEAD still names the accepted commit, and `core.fileMode` is forced on,
so a mode change is a change.

Conversion rules decide what equal means, and the executor controls those as
well: a clean filter can turn edited bytes back into the committed blob, an
untracked `.gitattributes` can convert line endings back, and
`core.symlinks=false` lets a regular file stand in for a tracked symbolic
link. The content comparison therefore runs in the scratch repository, which
borrows the workspace's objects through an alternate (objects are named by
their content) and has its own fixed configuration: no filter drivers, no
line ending conversion by configuration, symbolic links and file modes
compared. Neither the workspace's configuration and `info/attributes` nor the
system and global configuration and attributes are read, and attributes come
from HEAD's tree (`GIT_ATTR_SOURCE`), so only the reviewed `.gitattributes`
apply. Git older than 2.40 ignores `GIT_ATTR_SOURCE` and reads the worktree's
`.gitattributes`, which can still convert line endings or encodings but cannot
run a filter. The `ident` attribute is cleared in the scratch repository,
because it would let any text between `$Id:` and `$` equal the committed
`$Id$`. A file that a filter driver such as Git LFS produced, or that `ident`
expanded at checkout, differs from its blob there and counts as changed, so
such a repository fails closed.

The object store is the executor's as well, and Git checks the name of a
commit it parses but not of a tree or blob it reads. A tree rewritten under
the reviewed tree's name would make an unreviewed file the reviewed one in
both comparisons. Before either comparison, the worker therefore hashes the
commit, every tree it reaches and every `.gitattributes` blob in them, and a
mismatch is an error. A file's own blob needs no check, because the worktree
file is hashed and compared with the name that the verified tree gives it.
Git would compare a submodule's content inside the submodule, with the
submodule's configuration, so every checked-out submodule is compared the same
way, index and worktree, against the commit HEAD records for it, to a depth of
eight, and its changes are reported under its path. A submodule directory without `.git`,
which Git does not look inside, must be empty.

A filter is code, and a filter the executor planted could put a file back for
the length of the capture and redo the edit afterwards. No query of the
capture therefore reads the worktree under the workspace's or a submodule's
configuration, so none of their filters runs. A process the executor left
running as the worker's user can still change files, or the scratch
repository, while the capture runs; closing that needs process containment
and is not a property of the comparison.

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
created only when a dependent task whose execution package lists the output
under the dependency's `acceptedCommits` consumes the commit. Consumption alone
is not acceptance: a review judge may run after its dependency failed and is
given the failed result's outputs to inspect. The coordinator therefore lists
only the declared commit outputs of a review-declared producer whose packaged
outputs all came from an attempt that succeeded, and a package with such a list
requires the `accepted-dependencies-v1` capability, so an older worker is never
offered it. The worker publishes a staged commit only from the file of a listed
output, in the producer's directory of the dependency view, naming that
producer and that output. Any other file in the producer's outputs is the
executor's content and could hold an earlier, rejected attempt's commit
reference, so it is never a source of publication. Any other consumer fetches a
staged commit from its staged ref, under the campaign ref name in its own
workspace, even after another attempt's commit was published, and the store's
campaign ref is left as it is. A promotion that created the campaign ref but
not its record is completed by the next accepted fetch. Producers without
`review:` publish directly, and their consumers' packages are unchanged.

The staged provenance record names the attempt that staged it
(`stagedAttempt`). A producer offered the `campaign-commit-bundle-v1` capability retains
a bundle of its staging, which names the staged ref rather than the campaign
ref. A consumer on another worker imports that bundle as staged work of the
same attempt, into its own store's staging, so the same rule applies there: an
accepted consumer's fetch publishes it on that worker, and any other consumer
reads it from the staging. When a staging is promoted, its published record no
longer names the attempt. A worker resolves a commit record only from the file
of the declared commit output it names, in its producer's directory of the
dependency view. A copy in any other output is the executor's content and is
not resolved, so it cannot publish a campaign ref, such as one naming the base,
which needs no bundle. A rerun or an external input that carries a
review-declared producer's commit is placed only on a worker with
`accepted-dependencies-v1`, like a consumer in the producer's own run.

`campaign commit export` of a review-declared task's commit exports the staged
work of the task's latest attempt only once the coordinator accepted that
attempt. It uses the retained bundle, or otherwise asks the producing worker,
which exports from that attempt's staging without publishing it. A leaf task,
whose commit no consumer ever promotes, is exported that way too.

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
- A staged commit reaches a consumer on another worker only through its
  retained bundle, exactly as a published commit does. A producer whose bundle
  was left out, for example for its size, records the reason, and its staged
  commit can then be consumed only on the worker that staged it.
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
