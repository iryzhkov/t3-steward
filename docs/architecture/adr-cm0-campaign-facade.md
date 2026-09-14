# CM0 ADR: campaign is a façade, not a new system

Status: accepted. Freezes the contract for the campaign namespace before any of it is built.
Date: 2026-09-14
Authority: [the campaign manager plan](../plans/campaign-manager.md).

## Scenario and decision

Authoring a workflow today means writing `workflow.yaml`, assembling a tar by hand with the right
entries, and submitting the archive. The manifest is already expressive enough for the campaigns we
actually want to run, so the gap is not the model. The gap is that an agent has to know how to build
an archive, has no way to see the graph it is about to create, and finds out about a malformed
bundle only from a submission refusal.

The campaign namespace closes that gap and nothing else. It is a façade over the existing version-2
bundle and `WorkflowRun`. It introduces no domain model, no second DAG engine, no additional
manifest schema, no scheduler and no durable identity of its own.

The temptation this record exists to refuse: a "campaign" sounds like a thing, and a thing wants a
table, an ID, a state machine and a lifecycle. It gets none of them. A campaign is a way of talking
about a workflow run, not a record that exists alongside one.

## Identity mapping

One campaign submission creates exactly one `Workflow` and one `WorkflowRun`. The run ID is the
campaign's identity; there is no campaign ID. Every lifecycle alias resolves a run and delegates.

This is what makes the façade safe to remove or rename later: nothing persists that knows the word
campaign. A run submitted through `campaign submit` and the same bundle submitted through
`backlog submit` are indistinguishable afterwards, by construction, because they travel the same
submission path.

`backlog submit <archive>` remains available and unchanged as the low-level operation.

## Authority and what stays where

| Concern | Owner | Campaign's part |
| --- | --- | --- |
| Manifest schema and defaults | the existing ingestion parser | calls it; never re-parses |
| Path and archive safety | the existing ingestion path rules | calls them before submission |
| DAG semantics after ingestion | the existing DAG engine | none |
| Run, task and attempt state | the coordinator | reads through existing queries |
| Submission and replay | the existing local transport | calls it; adds no protocol |
| Static plan projection | campaign | read-only, creates no records |

Validation must reuse the ingestion parser and path policy. A second validator would be a weaker
validator: it would drift, and the drift would show up as a bundle that validates and then fails to
ingest, which is worse than no validation at all. This is the same failure the placement explanation
had, where a separate copy of the eligibility rules could not explain a decision the real ones made.

## Deterministic packaging

Two campaign directories with identical content produce identical archive bytes and therefore an
identical digest, regardless of the order in which their files were created or the filesystem they
sit on. Entry order is canonical, paths are spelled one way, and modes, ownership and timestamps are
fixed rather than inherited.

This is what makes the idempotency key meaningful. Reusing a key with the same content returns the
same run; reusing it with different content is refused. Neither guarantee survives if the same
directory can produce two different digests, so determinism is a correctness property here, not a
tidiness one.

The archive is not written to a persistent temporary file. It is built in memory within the existing
size limit, or in a securely created temporary file that is always cleaned up if the bounded maximum
ever makes memory inappropriate.

## Static plan versus dynamic explanation

`campaign plan` reports only what is knowable from the manifest: waves, roots, leaves, edges,
inherited versus task-level settings, artifact bindings, timing constraints and the digest that
`submit` will send. It performs no coordinator mutation and consults no live state.

It therefore may not claim that a worker, a provider route or capacity will be available. Placement
and admission are decided later against a capacity snapshot that does not exist yet, and a static
plan that implied otherwise would be lying in the most expensive way: confidently, before the work
is submitted. `campaign explain` remains the authoritative dynamic answer after submission.

## JSON compatibility

Lifecycle aliases return the existing schemas unchanged. `campaign list`, `show`, `graph`, `explain`
and `cancel` are thin delegations to the backlog implementations, with the same JSON and the same
exit codes, so an automation that parses one parses the other.

Only `validate`, `plan` and `submit` introduce new output. Their JSON is versioned from the start,
because an agent-facing surface that is not versioned becomes unchangeable the moment anything
depends on it.

## Alternatives and failures

A `campaign.yaml` schema was rejected. A second authoring format would have to be translated into
the first, and every field would then exist in two places with two sets of defaults; the translation
layer is where the drift would live.

A campaign record with its own identity and lifecycle was rejected. It would duplicate the run's
state machine and immediately raise the question of what happens when the two disagree after a
crash. The plan's own stop condition names this: if the façade cannot be delivered without a durable
campaign identity separate from workflow and run identity, that is a user decision rather than an
implementation choice.

Generating the DAG from a topology template was rejected. The canonical shape in the plan — optional
spikes, one lead implementation, optional qualification, automatic sink settlement — is guidance and
an example. The CLI accepts any valid static version-2 DAG, because hard-coding the shape would make
the façade a workflow type, which is exactly the thing it must not become.

Parallel repository mutation by separate tasks is deliberately absent from the template. It is
allowed only when tasks own isolated worktrees or branches and an explicit integration node exists.
Keeping mutation under one lead is easier to checkpoint, review and recover, and the template should
describe the case that works rather than the case that is possible.

## Explicitly not in this work

No cron or webhook ingress, no dynamic or agent-authored plan mutation, no automatic subagent
scheduling, no checkpoint negotiation, no quota or model-routing expansion, no credential brokerage,
no artifact retention policy, no memory integration and no user interface. Existing revision-fenced
graph amendments remain explicit operator controls under `backlog`.
