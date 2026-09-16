# Campaign manager: bounded authoring and DAG workflow

> **Superseded guidance.** This plan is retained as historical evidence of the campaign
> façade as it was designed and delivered. Its guidance on native subagents inside a lead
> task, and on when a campaign should be authored as one task rather than a DAG, has been
> superseded by `docs/plans/campaign-supervision.md`. Read that document for the current
> authoring discipline: multi-task work is authored as a static version-2 DAG whose tasks
> run as separate Steward-scheduled T3 sessions, and a task prompt does not use native
> subagents as a substitute for declared campaign tasks. Nothing else in this file is
> rewritten.

Status: ready for execution
Date: 2026-09-14
Base: t3-steward `f86c420` (`v0.11.0-rc.38`)
Product boundary: Citadel `docs/reference/active-plan/steward-campaign-execution.md`

## Objective

Make a planned unattended project launchable with one agent-friendly command:

```sh
t3-steward campaign submit ./campaign --idempotency-key my-project-2026-09-14
```

A campaign directory contains the existing version-2 `workflow.yaml` plus its prompt and input
files. The command validates the directory, shows its static DAG and packages it deterministically
before calling the existing coordinator submission transport. Once accepted, the existing
workflow, task, attempt, placement, quota, preflight, artifact and settlement machinery remains the
only runtime authority.

This campaign does not add a second DAG engine, scheduler, durable campaign table or manifest
schema. “Campaign” is the user-facing authoring and lifecycle view of a version-2 workflow run.

## Why this is the next slice

The bounded Track S implementation already supplies the expensive substrate:

- strict version-2 YAML manifests and acyclic dependency validation;
- immutable workflow and run ingestion;
- task dependencies and graph amendments;
- input and output artifacts, including `inputs_from`;
- CPU-class and capacity-aware placement;
- provider routes and quota admission;
- worker-side preflight and bounded prompt envelopes;
- graph, task, event, artifact and explanation queries;
- revision-fenced operator controls;
- schedules that create workflow occurrences.

The remaining gap is operational friction. Today a submitter must assemble an archive itself, invoke
the historical `backlog` namespace and understand which inspection commands belong together. That
is enough for a test harness but not for repeatedly launching planned projects.

## Fixed boundaries

1. `workflow.yaml` version 2 remains the only campaign authoring schema.
2. A campaign submission creates one existing `Workflow` and one existing `WorkflowRun`.
3. The manifest task map and `needs` fields are the DAG definition.
4. The existing DAG engine remains authoritative after ingestion.
5. Existing local submission transport and response schemas are reused unchanged.
6. `backlog submit <archive>` remains available as the low-level compatibility operation.
7. Existing schedule behavior is unchanged; cron and webhooks are later ingress adapters to the
   same submission boundary.
8. No dynamic agent-authored plan mutation is added. Existing revision-fenced graph amendments
   remain explicit operator/authorized-agent controls.
9. No new quota, environment, artifact, wait, user-input or effect machinery enters this campaign.
10. T3-specific session mechanics remain behind the existing worker/runtime boundary.

## Canonical campaign DAG

The recommended project topology is deliberately small:

```text
optional spike/review nodes
        \     /
         \   /
      lead implementation
             |
     independent qualification
             |
       automatic settlement
```

- **Spike/review nodes** are optional, parallel where independent, and artifact-only by default.
  Each emits a short bounded finding or decision input.
- **Lead implementation** consumes the frozen plan and any spike artifacts. It owns repository
  integration, may use a bounded number of subagents inside its T3 session, and emits commits,
  a compact ledger, verification results and a handoff.
- **Independent qualification** is optional. It checks the exact produced revision and reports
  pass/fail; it does not begin an unbounded repair loop.
- The existing workflow sink performs final settlement. A separate agent summarizer is not required.

The campaign CLI accepts any valid static version-2 DAG. This topology is guidance and an example,
not a new hard-coded workflow type.

Parallel repository mutation by separate steward tasks is not part of the initial template. A real
campaign may opt into it only when tasks own isolated worktrees/branches and an explicit integration
node. The simple template keeps mutation under one lead because that is easier to checkpoint,
review and recover.

## Authoring directory

```text
campaign/
├── workflow.yaml
├── inputs/
│   └── plan.md
└── prompts/
    ├── review.md
    ├── implement.md
    └── qualify.md
```

A representative manifest uses only fields already implemented:

```yaml
version: 2
name: project-campaign
class: surplus

environment:
  project: project-name
  type: git
  scope: task
  ref: main

inputs:
  - inputs/plan.md

resources:
  preset: light

preflight:
  steps:
    - id: source
      kind: context
      probe: git_head
      include: summary
    - id: tests
      kind: check
      command: [go, test, ./...]
      failure_policy: record
      include: summary

routes:
  - instance: claudeAgent
    model: claude-opus-5
    quota_pool: claude-main

tasks:
  review:
    prompt_file: prompts/review.md
    outputs: [review.md]
    verify: [test -s review.md]
    resources:
      preset: light

  implement:
    prompt_file: prompts/implement.md
    needs: [review]
    inputs_from:
      review: [review.md]
    outputs: [campaign-ledger.md, verification.txt, final-handoff.md]
    verify: [test -s final-handoff.md]
    resources:
      preset: build
    max_turns: 20

  qualify:
    prompt_file: prompts/qualify.md
    needs: [implement]
    inputs_from:
      implement: [campaign-ledger.md, verification.txt, final-handoff.md]
    outputs: [qualification.md]
    verify: [test -s qualification.md]
    resources:
      preset: build
```

The implementation example must use provider/project names that are clearly marked as operator
configuration, not portable defaults.

## CLI contract

### `campaign validate`

```text
t3-steward campaign validate <directory|workflow.yaml> [--json]
```

Performs no coordinator mutation. It:

- locates the campaign root;
- strictly parses and defaults `workflow.yaml`;
- validates every referenced regular file through the same safe-path rules as ingestion;
- rejects symlink, traversal, duplicate, special-file and size-policy violations;
- reports source-aware diagnostics where the YAML decoder provides them;
- computes the normalized manifest and bundle digest;
- reports a concise success summary: name, tasks, edges, roots, leaves, input count and digest.

Validation must reuse the ingestion parser and path rules. It must not implement a second, weaker
validator.

### `campaign plan`

```text
t3-steward campaign plan <directory|workflow.yaml> [--json|--dot]
```

Performs no coordinator mutation. It renders the statically knowable execution plan:

- topological waves and dependencies;
- root, leaf and sink relationship;
- inherited versus task-level class, placement, resource, route and preflight declarations;
- required input files, expected task outputs and cross-task artifact bindings;
- `not_before`, deadline and expiry constraints;
- the exact bundle digest that submit will send.

It does not claim that a worker, provider quota or dynamic capacity will be available later.
Post-submission `campaign explain` remains the authoritative dynamic explanation.

Text output is optimized for an agent and a human. JSON is stable and versioned. DOT output is a
valid graph with task IDs and dependency edges.

### `campaign submit`

```text
t3-steward campaign submit <directory|workflow.yaml> --idempotency-key KEY [--json]
```

The command:

1. runs the exact validation path;
2. constructs a deterministic uncompressed tar stream internally;
3. sends it through the existing local `SubmitArchive` operation;
4. prints the existing workflow ID, run ID, state, replay flag and digest;
5. prints the next inspection commands in human mode.

The idempotency key is required. Repeating the same key and bytes returns the same run. Reusing the
key with different bytes fails. Archive order, path spelling, modes, ownership fields and timestamps
are canonical so identical campaign directories produce identical bytes and digest.

The implementation must not write a persistent temporary archive. An in-memory buffer is acceptable
within the existing size limit; a securely created temporary file with guaranteed cleanup is
acceptable if the bounded maximum makes memory inappropriate.

### Lifecycle aliases

```text
t3-steward campaign list [existing workflow filters] [--json]
t3-steward campaign show <run> [--json]
t3-steward campaign graph <run> [--json|--dot]
t3-steward campaign explain <run>/<task> [--json]
t3-steward campaign cancel <run>/<task> --reason TEXT [--command-id ID] [--json]
```

These are thin aliases over existing backlog query and control implementations. They return the
same JSON schemas and exit semantics. No copied query service or alternate state is permitted.

Do not add aliases for every administrative command. The campaign namespace covers ordinary
submission and observation; specialized recovery and graph amendment remain under `backlog`.

## Help contract

Top-level and subcommand help are part of the product:

- explain that campaign is a façade over version-2 workflows;
- show the directory layout and smallest valid manifest;
- distinguish static `plan` from dynamic `explain`;
- explain task `needs`, `inputs_from`, outputs and verification;
- explain surplus versus required class;
- explain hostname constraints versus scheduler placement;
- show safe idempotent retry;
- give the exact next commands after submission;
- document exit codes and `--json` stability;
- point to a checked-in single-lead example and a three-node DAG example.

Help must stay concise enough to enter agent context. Detailed examples live in
`docs/examples/campaign/`.

## Implementation stages

### CM0 — Freeze the façade contract

- Add a short ADR recording the “no new domain model or DAG engine” decision.
- Name the campaign-to-workflow/run mapping and JSON compatibility rule.
- Record the deterministic-packaging and static-plan semantics.

Gate: the ADR and this plan agree; no production code changes yet.

### CM1 — Shared campaign loader and deterministic packer

- Extract/reuse bundle loading so validation and ingestion share one parser and path policy.
- Accept a directory or its `workflow.yaml`.
- Produce a canonical file inventory and deterministic tar stream.
- Preserve the coordinator's existing archive size and safety limits.
- Add unit/property tests for order independence, replay digest stability, symlink/special-file
  refusal, traversal, file mutation during packing and cleanup after failure.

Gate: two identical directory trees created in different orders produce identical bytes and digest;
every existing archive-ingestion negative case is rejected before submission.

### CM2 — Static DAG planning

- Build a read-only campaign plan projection from the defaulted manifest.
- Compute deterministic topological waves, roots, leaves and edges.
- Project inherited task settings and artifact bindings without creating domain records.
- Render stable text, JSON and DOT.
- Add tests for parallel roots, joins, disconnected valid components, empty workflow, timing fields,
  inherited overrides and stable ordering.

Gate: plan output round-trips in JSON tests and matches the graph later observed after submission.

### CM3 — Agent-facing CLI and help

- Add the `campaign` namespace and commands above.
- Reuse backlog query/mutation renderers and service clients for lifecycle aliases.
- Make submission require an idempotency key and show next steps.
- Add parser, golden/help and JSON compatibility tests.
- Add checked-in single-lead and three-node example directories.

Gate: an agent can validate, inspect and submit an example without manually invoking `tar` or
reading source code; malformed input identifies the file and field or path.

### CM4 — Local integration and regression

- Run the CLI against a disposable coordinator and worker fixture.
- Submit a three-node directory, verify exactly one workflow/run, compare pre-submit plan with the
  persisted graph, settle it and verify artifacts.
- Replay the same key/bytes and reject same-key/different-bytes.
- Confirm low-level `backlog submit <archive>` and all existing tests remain unchanged.

Gate: full build, unit, race, vet, pinned lint and exact-commit GitHub CI pass.

### CM5 — Publish and two bounded dogfood projects

- Publish the next release candidate and deploy it through UpKeeper with an exact reviewed manifest
  diff.
- Run both projects from checked-in or durably retained campaign directories with distinct
  idempotency keys:
  1. **Single-lead coding project.** Use a disposable small Git repository with one bounded defect or
     feature. Submit a one-lead-task campaign whose required outputs are a verified commit, test
     receipt and concise handoff. This proves the ordinary “give steward a plan and leave” path
     without requiring artifact fan-out.
  2. **Parallel DAG artifact project.** Submit two independent, non-mutating analysis nodes followed
     by one join/qualification node. Each parent emits a different bounded artifact; the child
     declares both through `inputs_from`, verifies their materialization and emits one combined
     result. This proves parallel readiness, dependency release and artifact passing.
- Prefer one project on `gpt-5.6-sol` and the other on `claude-opus-5` when both configured routes
  are healthy. Either approved route may substitute when quota or availability requires it. This is
  campaign qualification, not a model-quality comparison.
- Let normal placement choose among compatible workers. Do not hard-code hosts merely to manufacture
  distribution.
- Record operator commands, intervention, campaign directories and digests, run/task/thread IDs,
  actual model routes, task placement, artifacts, terminal states, total provider usage and any
  steward-attributable failure separately for each project.
- Fix only failures that block the campaign contract. Rerun each project at most once, using a new
  idempotency key and preserving the first result.

Gate: both directory submissions complete unattended: the single-lead project returns a verified
commit and handoff, and the parallel project settles the exact three-node DAG and artifact join.
Total operator work across both is less than fifteen minutes. Broader Huyang-scale batching, cron and
webhook integration remain separate decisions.

## Parallel execution ownership

At most two leaf subagents run concurrently:

- **CLI/packaging lane:** CM1 and the validate/submit portion of CM3.
- **DAG/view lane:** CM2 and examples/help for plan/graph semantics.

The lead owns CM0, shared interfaces, lifecycle aliases, integration, Git history, CI, release,
UpKeeper publication and dogfood. Subagents do not commit or push. All repository reads, searches
and edits use Huyang; shell is reserved for Git, builds and tests.

## Explicit exclusions

- new campaign database tables or coordinator state;
- a second manifest version or a `campaign.yaml` schema;
- dynamic DAG expansion or agent-authored graph policy;
- automatic subagent scheduling by steward;
- Git branch integration orchestration;
- checkpoint negotiation or quota-model expansion;
- OV retrieval, Pensieve or automatic memory writes;
- new cron semantics or webhook ingress;
- general effect sagas, credential brokerage or artifact retention;
- graphical UI;
- broad fleet soak or another large benchmark batch.

## Stop conditions

Stop and request user direction if implementation requires changing the authoritative DAG state
machine, version-2 manifest semantics, coordinator submission protocol, or T3 conversation model.
Also stop if the façade cannot be delivered without a new durable campaign identity separate from
workflow/run identity.

Ordinary defects, test failures and bounded refactors inside the stated seams are not user
decisions.

## Completion report

Report:

- source and release commits;
- exact CLI surface and example path;
- full validation and CI receipts;
- UpKeeper release commit and fleet convergence;
- dogfood run/task/thread IDs and placements;
- artifacts and terminal states;
- operator intervention and provider quota consumed;
- anything deliberately deferred.

Do not claim that cron, webhooks, dynamic campaign planning, checkpoint/resume or autonomous
subagent scheduling were implemented.
