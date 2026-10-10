package campaign

// FixLoopsHelp explains the finite expansion and typed review guards.
const FixLoopsHelp = `t3-steward campaign help fix-loops

A successful review process may request changes. needs remains a successful-task
dependency; needs_verdict adds an explicit guard on that producer's structured
review verdict. Accepted values are accept and changes-requested. The named
producer must also appear in needs and must declare review_output. Missing,
invalid or mismatched verdicts never authorize the consumer.

Use a bounded implement/review loop in a version 2 manifest:

  fix_loops:
    patch:
      implement: implement
      review: review
      max_rounds: 3

The review task depends on implement, retains review.md and declares review_output. Round one uses
the original task names; later rounds are implement-round-N and review-round-N.
The compiler expands all rounds into a finite DAG before submission. A
changes-requested review enables the next implementation round, which receives
the prior implementation's declared commit and the review's retained outputs.
An accept skips later rounds. Dependencies retain literal task identity: review
names round one; review-round-N names that explicit later round. There is no
alias for the loop's accepted review. Use needs_verdict for explicit guards. Each task may belong to only one loop; max_rounds is 1 through
20. Expansion that creates a cycle or a generated-name collision is refused.

When the last permitted review requests changes, the run fails with the typed
escalation fix-loop-exhausted. It never creates another round. campaign show and
the terminal wake summary report rounds used, the final verdict and escalation.
Malformed review output fails its producing task and is never treated as a
request for another round. Retrying a loop task in place is refused; use
campaign rerun for a fresh subtree so stale skipped rounds are not reused.
Reused ancestors become carried inputs; their edges and verdict guards are
removed together.
`
