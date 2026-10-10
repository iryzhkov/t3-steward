# Verdict-aware dependencies and bounded fix loops

`needs` requires successful task completion. A review may complete successfully while requesting changes. Add `needs_verdict: {review: accept}` to require its structured verdict before a consumer runs, or use `changes-requested` for a correction task. Each producer must also appear in `needs` and declare `review_output`. Missing, invalid or mismatched verdicts never satisfy a guard.

A version 2 workflow can declare a bounded loop:

```yaml
fix_loops:
  patch:
    implement: implement
    review: review
    max_rounds: 3
```

Loops require `environment: {type: git, scope: task}` so every implementation and review has a fresh workspace and thread. The implementer declares at least one commit, and the reviewer declares no commits. Prompts are reused across rounds: write the implementation prompt to inspect `.t3/dependencies/`, continue from the supplied implementation commit, and address the retained review findings when present. Write the review prompt to inspect that round's supplied commit and produce fresh verdict evidence.

The review task depends on the implementation, retains `review.md`, and declares `review_output`. Each task belongs to at most one loop. The bound is an integer from 1 through 20. Round one retains the original task names; later rounds use `<task>-round-N`. Expansion produces a finite DAG before submission and refuses cycles or generated-name collisions.

A `changes-requested` verdict enables the next implementation round, carrying the prior implementation's declared commit and retained review outputs. An `accept` verdict skips unused rounds. Dependencies retain literal task identity: `review` names round one and `review-round-N` names that explicit later round. There is no alias for the loop's accepted review; use `needs_verdict` to guard an explicit producer. A malformed verdict fails the review task and never enables a correction round.

If the final permitted round still requests changes, the sink fails and records the typed escalation `fix-loop-exhausted`. No additional round is created. `campaign show`, the sink result and terminal wake summaries report the number of rounds used, maximum rounds, final verdict and escalation. Retrying a loop task in place is refused. Use `campaign rerun` to create a fresh subtree; this prevents stale skipped rounds from being reused. Reused ancestors become carried inputs, and their dependency edges and verdict guards are detached together. Read `t3-steward campaign help fix-loops` for the same authoring contract.
