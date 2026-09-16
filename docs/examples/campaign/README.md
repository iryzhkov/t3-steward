# Campaign examples

Three checked-in campaign directories, all submittable as they stand once their operator
configuration is replaced:

- `three-node/` — **the recommended multi-task template.** Two independent analysis tasks
  and one join task, three separate Steward-scheduled T3 sessions, artifacts passed across
  the dependency edges with `inputs_from`. Start here whenever the work is more than one
  unit of judgement.
- `single-lead/` — one task that owns a repository change from start to finish. A
  legitimate choice, not a fallback; see "When one task is the right answer" below.
- `supervised-three-node/` — the same shape with an overseer: two analyses, a review gate
  over both of them, one downstream synthesis task, and a final-settlement gate guarding
  the run's settlement. Use it when a result needs judgement before the next task is
  dispatched. See "Supervised campaigns" below.

## How multi-task campaigns are authored

A campaign is a static version 2 workflow. Every task declared in `workflow.yaml` becomes a
task the Steward schedules, admits against quota, places on a worker and runs in its own T3
session, with its own prompt, its own verification and its own retained artifacts. The
dependency edges are declared with `needs`, and the files that cross those edges are
declared with `inputs_from` and `outputs`. The graph is fixed at submission: the Steward
does not invent tasks, and neither does a running task.

This has one consequence that matters when you write a prompt.

**A task prompt must not use native subagents as a substitute for declared campaign tasks.**
Most agent harnesses can spawn helper agents inside a single session. Work delegated that
way is invisible to the Steward: it has no task record, no dependency edge, no separate
quota admission, no placement decision, no verification, no artifact, no retry boundary and
no separate T3 session. Hidden native delegation is not separately scheduled work, and a
campaign whose lead fans work out that way is a one-task campaign that reports itself as
several. If a task would fan work out, declare that work as tasks instead, and connect them
with `needs` and `inputs_from`.

The example prompts state this rule to the agent that reads them. Keep it in prompts you
write: the schema declares the discipline, and the prompt is where it is actually said. The
Steward cannot technically prevent every harness tool from spawning a helper, so the
instruction is part of the contract rather than an enforced boundary.

What a task may still do inside its own session is ordinary work: reading, searching,
running tests and builds, and calling tools. The rule is about delegating a unit of work
that should have been a declared task, not about how a task does its own job.

## When one task is the right answer

A one-task campaign, like `single-lead/`, is the correct authoring choice when:

- the work is a single unit of judgement that cannot be split without one half waiting on
  the other for context that no artifact can carry;
- splitting it would only produce a handoff document that the next task has to reconstruct
  the reasoning from;
- the change is small enough that one commit, one verification run and one handoff are the
  whole result;
- the tasks would all mutate the same repository, and no isolated worktree or branch and
  integration task exists to make parallel mutation safe.

It is not the right answer merely because the work is large. Large work with separable
parts is what the DAG is for: separate sessions fail independently, are retried
independently, are explained independently and cannot silently consume each other's
context.

## Supervised campaigns

`supervised-three-node/` adds two optional top-level keys to the same version 2 schema.
Omit both and the campaign behaves exactly as an unsupervised one, which is what every
campaign that ran before supervision existed does.

`supervision` declares the overseer: one route, one overseer prompt, and the bounds the run
holds it to. `gates` declares the review boundaries: each gate observes named producers
through `after` and holds named downstream tasks through `before`, and a gate with no
`before` and `final: true` guards the run's settlement instead of a task.

Four rules are worth knowing before you author one:

- `gates` without `supervision` is refused. A gate nothing can release is a campaign that is
  permanently held.
- A gate cannot replace a `needs` edge. Every protected task must already depend on
  everything the gate observes, and the manifest is refused otherwise. A gate constrains
  dispatch; it never carries an artifact.
- A gate holds the tasks it names **and their dependency descendants**. `campaign plan`
  prints that closure, so check it rather than inferring it.
- The overseer accepts, holds or escalates. It cannot retry, rewrite or skip a task, and it
  cannot make failed work successful. A supervisory acceptance and a successful worker
  result are separate facts.

The overseer prompt and every gate rubric are ordinary bundle files: they are validated and
packed like a task prompt, and they are where the review standard is actually written.

One fleet requirement applies only to supervised campaigns. A worker may run an overseer
activation only if its build advertises the `campaign-supervision-v1` capability, so every
worker that might be chosen has to be on a release that has it. `campaign check` reports
`impossible` when no eligible worker advertises it, and `campaign submit` then refuses, so
an out-of-date fleet is a refusal before submission rather than a run that stalls after it.
Unsupervised campaigns require no capability and are unaffected.

## Before submitting any of these directories

`environment.project`, `routes[].instance`, `routes[].model` and `routes[].quota_pool` name
one fleet's registered project, provider instance, model and quota pool. Replace all four
with the names your own Steward is configured with; every such value is marked with a
trailing `# operator configuration` comment.

The same applies to `supervision.route` in the supervised example. It is operator
configuration, not a portable default and not a recommendation of a particular model: what
the design requires is that the overseer route is configured independently of the task
routes, and which route that is, is yours to choose.

```sh
t3-steward campaign validate three-node        # offline
t3-steward campaign plan three-node            # offline; shows the waves and edges
t3-steward campaign check three-node --json    # live, read-only
t3-steward campaign submit three-node --idempotency-key <key> --json
```

Fuller authoring help: `t3-steward campaign help authoring` and
`t3-steward campaign help dag-semantics`.
