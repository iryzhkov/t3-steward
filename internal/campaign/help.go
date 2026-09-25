package campaign

// The help text lives beside the projection it describes so that the two stay
// consistent: a change to what plan reports is a change to this file. The
// command tree consumes these strings and does not restate them.
//
// Help is part of the product for an agent that has never read this source, so
// it is written to be read once, in full, without following a link. It is kept
// short enough to sit in an agent's context beside the plan itself; the long
// examples live in docs/examples/campaign/.

// PlanHelp is the long help of the plan command.
const PlanHelp = `Render the statically knowable execution plan of a campaign directory.

The plan is computed from workflow.yaml alone. It creates nothing, submits
nothing and reads no coordinator state, so it is safe to run as often as you
like while authoring.

It reports:
  - topological waves, and which tasks may start together in each wave;
  - roots (no dependency at all), leaves (nothing depends on them), every edge,
    and any independent components the manifest declares;
  - for each task, whether its class, placement, resources, routes and preflight
    were inherited from the workflow or declared on the task;
  - declared input files, declared outputs and cross-task artifact bindings;
  - not_before, deadline and expires_at constraints;
  - the bundle digest that submit will send.

It does not report, and cannot know, whether a worker, a provider route or
quota will be available. Placement and admission are decided after submission,
against a capacity snapshot that does not exist while you are planning.

Output:
  (default)  text, for a human or an agent
  --json     versioned document; read schemaVersion before any other field
  --dot      Graphviz digraph; nodes are manifest task names

Worked examples:
  docs/examples/campaign/three-node   two analysis tasks joined by a third; the
                                      recommended multi-task template
  docs/examples/campaign/single-lead  one lead task: commit, test receipt, handoff

How many tasks a campaign should have, and why a task prompt may not replace a
declared task with a native subagent: t3-steward campaign help authoring.
`

// AuthoringHelp is the topic an author reads before writing a manifest: how
// many tasks a campaign should have, and why a task prompt may not replace a
// declared task with a native subagent. It is a help topic rather than part of
// the usage block because the usage has to stay short enough to sit in an
// agent's context, and this is read once, while authoring.
const AuthoringHelp = `How many tasks a campaign should have, and what a task may not delegate.

Multi-task work is authored as a static version 2 DAG. Every task declared in
workflow.yaml becomes a task the Steward schedules: it is admitted against quota
on its own, placed on a worker on its own, and run in its own T3 session with
its own prompt, verification and retained artifacts. Dependencies are declared
with needs, and the files that cross them with outputs and inputs_from. The
graph is fixed at submission. The Steward never invents a task, and neither may
a running one.

A task prompt must not use native subagents as a substitute for declared
campaign tasks. Most harnesses can spawn helper agents inside one session, and
work delegated that way is invisible here: no task record, no dependency edge,
no quota admission of its own, no placement decision, no verification, no
artifact, no retry boundary and no separate T3 session. Hidden native delegation
is not separately scheduled work, however the session reports it, and a campaign
whose lead fans work out that way is a one-task campaign that looks like
several. Declare the work a task would fan out as tasks, joined with needs and
inputs_from.

Say this in the prompt as well as meaning it in the schema. The manifest
declares the discipline, the prompt is where the agent is actually told, and
nothing in the coordinator can prevent a harness tool from spawning a helper.
Where a runtime supports disabling that capability, disable it.

What a task does inside its own session is unaffected: reading, searching,
building, running tests and calling tools are how a task does its own job.

One task is a legitimate authoring choice, not a fallback. Author a single-task
campaign when:

  - the work is one unit of judgement, and splitting it would leave one half
    waiting on context no artifact can carry;
  - the split would only manufacture a handoff the next task has to reconstruct
    the reasoning from;
  - one commit, one verification run and one handoff are the whole result;
  - every part would mutate the same repository and there is no isolated
    worktree or branch per task and no explicit integration task.

Size alone is not a reason. Work with separable parts belongs in a DAG, where
the parts fail, retry and are explained independently.

Templates:
  docs/examples/campaign/three-node   the recommended multi-task template
  docs/examples/campaign/single-lead  one task that owns a repository change
  docs/examples/campaign/README.md    how to choose between them

See also: t3-steward campaign help dag-semantics.
`

// GraphHelp is the long help of the graph command, which reads a submitted run
// rather than a directory.
const GraphHelp = `Show the dependency graph of a submitted run, as the coordinator holds it now.

graph reads live state: it names the durable task IDs, the current progress of
each task and the graph revision. Use it after submission.

Before submission there is no run, so use plan instead. The two agree on shape
by construction, because plan projects the same manifest the coordinator
ingested, with two differences worth expecting:

  - plan names manifest task names; graph names the task IDs assigned at
    ingestion;
  - plan shows the coordinator's terminal sink as a dashed node it will create;
    graph shows the sink that exists, with its progress.

The sink depends on every task in the run, not only on the leaves, and settles
the run once all of them are terminal. It is created by the coordinator and
cannot be authored in a manifest.

Output:
  (default)  text
  --json     the existing backlog graph schema, unchanged
  --dot      Graphviz digraph
`

// DAGSemanticsHelp explains the manifest fields that make a campaign a graph
// rather than a list, and the two mounts through which a task receives files.
// It is referenced from plan, graph and the campaign namespace overview.
const DAGSemanticsHelp = `How a campaign manifest becomes a graph.

needs
  A task starts only after every task it needs has succeeded. Dependencies are
  declared by task name, and the manifest must be acyclic; a cycle is refused at
  parse time. A task with no needs is a root and may be admitted as soon as the
  run starts. Tasks that do not depend on one another may run at the same time,
  on different workers, subject to capacity.

  A dependency written <run>/<task> names a node of another run. It is a real
  dependency, but it is outside this graph: the plan shows it, and cannot say
  anything about its state.

inputs
  The static files a campaign ships, listed under the top-level inputs: field
  as paths relative to the campaign directory (globs are allowed). They are
  retained as artifacts of the run and mounted read-only in every task's
  workspace, at their declared path, under

      .t3/inputs/<declared path>

  The declared path is kept in full, directory and all: a file declared as
  inputs/plan.md is read at .t3/inputs/inputs/plan.md, and one declared as
  fixtures/data.json at .t3/inputs/fixtures/data.json. A prompt can name these
  paths outright, because they are fixed at authoring time.

inputs_from
  Dependencies release a task; inputs_from is how the task receives the work.
  Each entry names a direct dependency and the artifacts to take from it, and
  those artifacts must be declared in that dependency's outputs. The files
  appear in the successor's workspace, read-only, at

      .t3/dependencies/<producer task id>/<artifact>

  The producer task id is assigned at ingestion, when the campaign is
  submitted, so it is not known while the prompt is written and must not be
  hard-coded. A prompt lists .t3/dependencies/ to find the one directory per
  producer, or names the artifact and lets the agent find it there.

  Naming a task in inputs_from without also naming it in needs is refused, so
  an artifact can never be read before it exists.

outputs
  The files a task promises to leave behind, relative to its workspace. Only a
  declared output is captured as an artifact, checksummed and retained; anything
  else the task writes stays in the disposable workspace and is lost. Declare an
  output for every file a later task, or a human, has to read.

commits
  A Git commit a task promises to leave behind, declared by name and consumed
  through inputs_from exactly like an output. What the successor receives is the
  commit's provenance record, and the commit itself is already fetched into its
  checkout, so it never has to search a repository cache for it. See
  "t3-steward campaign help commits".

verify
  Shell commands run in the task's workspace after the agent stops. Every one of
  them must exit zero or the task fails, which means its dependents never start.
  Verification is the difference between a task that claims success and a task
  that demonstrated it, so check the thing that matters: that the output exists
  and is not empty, that the tests pass, that the commit is there.

A useful shape is: each task emits a small, named, verified artifact, and the
task that needs it declares it through inputs_from. Keep repository mutation in
one task unless separate tasks own isolated worktrees or branches and an
explicit integration task joins them.

needs, inputs_from, outputs, commits and verify are the only way work is fanned
out. A task that spawns native
subagents instead of declaring tasks produces no node, no edge, no artifact and
no separate T3 session, so nothing above applies to what it delegated. See
"t3-steward campaign help authoring".
`

// CommitsHelp is the long help of the commits field. It lives here because the
// campaign usage block is capped at a length that fits in an agent's context,
// and because a declared commit is the one output whose contract an author
// cannot guess from its name.
const CommitsHelp = `Hand a Git commit to a later task, by reference rather than by luck.

  tasks:
    implement:
      prompt_file: prompts/implement.md
      commits:
        - name: implementation
          revision: HEAD
    review:
      prompt_file: prompts/review.md
      needs: [implement]
      inputs_from:
        implement: [implementation]

name is one path component, and it is the name a successor consumes. It shares
the namespace of outputs, so one task cannot declare a commit and an output of
the same name. revision is resolved in the producing task's own workspace when
that task finishes, and defaults to HEAD.

revision must be a ref name or a commit id (use the full 40-character id; an
abbreviated one can become ambiguous): HEAD, a branch, a tag or refs/...
Revision expressions are refused by validate ("not a safe Git ref"), for
example HEAD~1, main^, @{u}, a..b, and anything with ~ ^ : ? * [ \ or a space. A
task that hands over several commits commits each to its own branch and
declares one entry per branch:

      commits:
        - {name: schema, revision: refs/heads/task-schema}
        - {name: migration, revision: refs/heads/task-migration}

What the producer promises is that the revision resolves in its workspace when
it finishes. A task that declares a commit it did not produce fails with
"declared commit <name>: <cause>", exactly as a missing declared output fails.

What the successor receives is two things. The retained artifact of that name
is the commit's provenance record: a JSON document naming the producing task,
the base the workspace was pinned to, the repository, the commit and its
campaign ref. It arrives where every dependency artifact arrives, under
.t3/dependencies/<producer>/<name>. The commit itself arrives fetched into the
successor's own checkout under the same ref, so

  git rev-parse refs/campaigns/<run>/<task>/<name>

resolves there and nothing has to search a repository cache. The pin the
workspace started from is recorded at .t3/base-commit.

Why this exists: a successor must receive the exact commit whether or not it
was ever pushed anywhere, and the worker's repository cache is no place to keep
one: each task's preparation refreshes it with --prune, which deletes any ref
the project repository does not have. The campaign ref is in a store beside the
cache that is never pruned, and it is kept for the campaign's lifetime. (Since
0.11.0-rc.91 a task's git push origin reaches the project repository rather
than the cache; a declared commit is still how a successor receives a commit,
and a push is how the owner does.)

plan cannot print the ref. It contains the workflow run and task IDs, which are
assigned at ingestion, so a static plan reports the declaration and the revision
and leaves the ref to the run.

Lifetime: the commit stays reachable for as long as its provenance record is
retained, and the refs of a run are released together when retention removes
them. A rerun pins its source run against retention, so a commit a rerun
carries is kept for as long as the new run needs it. A rerun asked for after the
record has been pruned is refused, naming the artifact it could not read, rather
than starting a task whose input is missing.
`

// StaticVersusDynamicHelp is the one paragraph an agent needs to choose between
// plan and explain. It is short on purpose: it is included in more than one
// help page.
const StaticVersusDynamicHelp = `plan is static, explain is dynamic.

plan answers questions about the manifest: what depends on what, what runs in
which wave, what each task inherited, which artifacts cross which edge, what
bundle digest submit will send. It reads a directory and touches nothing.

explain answers questions about a submitted run: why this task is still
blocked, which worker it was placed on and why, which route and quota pool
admitted it, what its attempts did. It reads coordinator state and needs a run
to exist.

A plan never promises a worker, a route or quota. If the question is whether
something can run now, it is an explain question.
`

// ReadinessHelp is the long help of the check command. It lives here rather
// than in the command usage because the usage has to stay short enough to sit
// in an agent's context, and these tables are what an agent reads once, when a
// check has refused something.
const ReadinessHelp = `Ask the coordinator whether a campaign could run, before submitting it.

check is read-only and live. It creates no record, reserves nothing and sends no
bundle: it sends the projected plan's requirements and asks for a per-task,
per-worker answer about the fleet as it is now.

For every task and every worker the coordinator reports worker freshness,
enrollment and catalog-digest match, project and eligible-worker policy, CPU
class, resource requirements, capabilities, required directories, setup profile,
provider instance, model, quota pool, quota-snapshot freshness,
credential-reference availability, repository and ref reachability, timing
constraints, resource locks, and the coordinator's message limits.

Outcomes:
  ready             at least one worker can take every task now
  accepted_waiting  no worker can now, and the obstruction is temporary, so
                    submission proceeds and the reasons are reported
  impossible        no worker can ever run it as written, so submission is
                    refused and no workflow run is created

Permanent reason codes. Waiting cannot change any of them, so submit refuses:
  unknown-project, workspace-type-mismatch, unknown-setup-profile,
  unknown-provider-instance, unknown-model, unknown-quota-pool,
  worker-not-eligible, capability-missing, cpu-class-impossible,
  resources-impossible, directory-impossible, credential-missing,
  repository-syntax-invalid, repository-authentication-failed,
  repository-not-found, ref-not-found, no-route, no-configured-route,
  supervisor-client-missing, timing-window-closed, message-limit-exceeded.

no-route means a task declares no provider route (instance and model) at all.
The coordinator never chooses one: declare routes in workflow.yaml, or start a
single task with "t3-steward task run --model [INSTANCE/]MODEL", which derives
the route from what the project's eligible workers advertise. The refusal lists
those instance/model pairs; "t3-steward models" shows them with quota state.

Temporary reason codes. Waiting is what fixes them, so submit proceeds:
  quota-closed, worker-at-capacity, worker-offline, worker-stale,
  network-unavailable, dns-failure, probe-timeout, snapshot-stale, lock-held,
  timing-window-not-open, catalog-digest-mismatch.

catalog-digest-mismatch is drift and is never reported as a missing worker. It
carries the catalog digest the coordinator requires, the digest the worker
actually accepted, and the enrollment revision to fence a re-enrolment against.

Repository and ref reachability is observed by a bounded built-in probe
equivalent to "git ls-remote --exit-code -- <repository> <ref>", run with the
execution identity and credential references the real task would use. There is
no shell and a manifest cannot choose the arguments. The result is retained for
ten minutes, keyed on the worker, the catalog digest, the repository, the ref
and the credential references, so rotating a credential or renaming a
repository invalidates it. No credential value ever appears in the result.

Recovery:
  unknown-project            t3-steward backlog projects
  workspace-type-mismatch    match environment.type to the project TYPE (t3-steward campaign help fresh)
  repository-syntax-invalid  fix backlog_v2.projects.<name>.repository
  ref-not-found              fix environment.ref in workflow.yaml
  no-route                   declare routes: [{instance, model}] in workflow.yaml (t3-steward models lists them)
  no-configured-route        add the instance and model to an eligible worker
  catalog-digest-mismatch    t3-steward worker enroll <worker> --current-catalog --reason TEXT   (on the coordinator host)

Exit codes: 0 when the campaign is ready or accepted_waiting, 8 when it is
impossible, and the transport classes 3 to 7 when the coordinator could not be
reached. --json prints the whole matrix; read schemaVersion first.
`

// RerunHelp is the long help of the rerun command. It lives here, and not in
// the campaign usage, because the usage is capped at a length that fits in an
// agent's context and rerun is the verb an agent reads once, after something
// has already failed.
const RerunHelp = `Start a failed campaign again from one task, without touching what happened.

rerun is mutating and live. It creates a new workflow run linked to the source
run and never changes the source run: it stays failed, and it stays readable,
because a run that pretends it did not fail is a run nobody can learn from.

  t3-steward campaign rerun <run> --from <task> --idempotency-key KEY
                                  [--reason TEXT] [--json]

Scope is explicit rather than clever:

  - the named task and every task that depends on it, directly or indirectly,
    are rerun;
  - every other task is reused. Its output artifacts are carried over by
    reference: the new run points at the same stored content, and nothing is
    copied. They arrive in the same place in the workspace they arrived in
    before, so a prompt written against them still reads them;
  - artifacts produced by the failed subtree are not carried over. They stay
    under the source run as evidence of what happened, and they are not
    visible as an input anywhere in the new run.

The new run records its provenance: the source run, the source task, the source
attempt, the idempotency key and the reason. Read it with
"t3-steward campaign graph <new run> --json", under graph.rerunOf.

Refusals, and what each one means:

  the source run has not finished
    Scope is read from which tasks succeeded, and that answer is not stable
    while the run can still change it. Wait, or cancel the run first.
  a task that would be reused did not succeed
    Reusing it would start the rerun without an input that never existed. The
    message names every such task; rerun from a task they all descend from.
  an artifact is no longer retrievable
    An ancestor output has been pruned or lost. The rerun refuses rather than
    starting a task whose declared input is missing. Nothing is created.
  a stale graph revision
    The source run changed between being read and being reran from. Read it
    again with "t3-steward campaign show <run>" and retry.

Idempotency is the same everywhere: the same --idempotency-key with the same
run, task and reason returns the same new run; the same key with different
content is refused. A refused rerun creates nothing, so retrying after fixing
the cause is safe.

A complete example:
  t3-steward campaign show run-42 --json
  t3-steward campaign rerun run-42 --from implement \
    --idempotency-key rerun-run-42-1 --reason 'clone failed on a stale ref' --json
  t3-steward campaign show <new run>

Required configuration: a reachable coordinator, exactly as submit needs one.
No credential of its own: the admin transport's secretref:f03-admin/<client>
reference is resolved at use.

Exit codes: 0 on success, 1 on a usage error, and the transport classes 3 to 8;
a refused rerun is class rejected, exit 8. --json prints a versioned document;
read schemaVersion first.

Safe recovery when a rerun refuses or its outcome is unclear:
  t3-steward campaign show <source run> --json
The source run is unchanged, so reading it is always the next safe step.
`

// NotifyHelp is the long help of submit --notify-thread.
const NotifyHelp = `Be woken when a submitted campaign reaches a terminal outcome.

  t3-steward campaign submit <directory> --idempotency-key KEY \
    --notify-thread <current|id>

submit --notify-thread registers a node wait on the new run's sink, bound to a
T3 thread. When the run settles, the coordinator wakes that thread with the
outcome. There is no SSH helper, no polling loop and no direct reading of
coordinator state: it is the same durable wait "t3-steward wait add --run"
registers, reached through the same admin transport.

current names the calling agent's own canonical T3 thread. It is resolved, not
assumed: inside a task from the injected execution identity, and otherwise from
the caller's provider session, which is an input to the resolution and never a
thread id of its own. A session that resolves to no thread, or to more than
one, is an error; pass --notify-thread <id> with the intended thread.

The thread is resolved before anything is submitted. An unresolvable
--notify-thread therefore leaves no run behind, because a campaign nobody is
listening for is worse than a campaign that was not submitted.

The registration creates and alters no workflow state. It adds one wait record
and holds the source run against retention; the run, its tasks and its attempts
are exactly what they would have been without it.

The registration ID is derived from the submission idempotency key, so
re-running the same submit command registers the same wait rather than a second
one. Registering a wait on a run that has already settled returns the terminal
observation immediately.

If submission succeeds and registration then fails, the error says so and names
the run. The run exists; register the wait separately with
  t3-steward wait add --run <run> --thread <id>

After a successful registration, end the turn. The steward wakes the thread.
`

// FreshHelp tells an author how to run a campaign that needs no repository.
// Research, spike and review campaigns that produce findings rather than
// commits are the common case, and without this topic the only hint was one
// comment in the skill's example manifest.
const FreshHelp = `Campaigns that need no Git repository: environment.type fresh.

Research, spike and review work often produces findings, not commits. Such a
campaign runs in a new, empty directory per task instead of a checkout:

  environment:
    project: scratch      # a catalog project of type fresh
    type: fresh

A fresh campaign names no ref and no repository, and its environment scope is
task (the default). Everything else is the same as a Git campaign: routes, needs,
outputs, inputs_from, verify, placement and check. A task's declared outputs are
the only thing collected from its directory, so declare every file a successor
or the owner needs. A successor finds its inputs_from files under
.t3/dependencies/<producer task id>/, an id assigned at submission, so a prompt
lists .t3/dependencies/ rather than hard-coding it. Campaign inputs are under
.t3/inputs/.

The project must be declared with type fresh in the coordinator catalog.
"t3-steward backlog projects" shows each project's TYPE; check refuses a Git
project with workspace-type-mismatch and names the fresh projects that exist.
When none exists, an operator declares one through UpKeeper:
  upkeeper project add scratch --type fresh --workers homelab,omarchy-pc

One task: t3-steward task run --fresh --model [INSTANCE/]MODEL -- "<prompt>"
works from any directory, with no checkout, and picks the one fresh project.

A minimal two-task example:
  version: 2
  name: findings
  environment: {project: scratch, type: fresh}
  routes: [{instance: claudeAgent, model: claude-sonnet-5}]
  tasks:
    research: {prompt_file: prompts/research.md, outputs: [findings.md]}
    review:
      needs: [research]
      inputs_from: {research: [findings.md]}
      prompt_file: prompts/review.md
      outputs: [review.md]
      verify: ['test -s review.md']
`

// RoutesHelp documents the routes field. It was undocumented anywhere an
// author reads, although validate accepts options and the worker passes them
// to T3's model selection (S4).
const RoutesHelp = `Provider routes: which instance and model run a task.

  routes:
    - instance: claudeAgent        # a T3 provider instance
      model: claude-sonnet-5       # a model that instance offers
      quota_pool: claude-main      # optional; the pool the instance is bound to
      options: {effort: medium}    # optional; passed to T3's model selection

routes may be declared for the whole workflow or per task; a task's own list
replaces the inherited one. The coordinator never invents a route: a task with
none is refused (no-route). "t3-steward models" lists every instance/model the
fleet offers, with its pool and quota state.

quota_pool may be left out: it is the pool the fleet catalog binds the instance
to. Naming a different pool is refused by check (unknown-quota-pool).

options is a map of T3 model-selection options, each sent to T3 as
{id, value} exactly as written. The option T3 honours for Claude and Codex
instances is effort (for example low, medium, high). Steward does not check
option names against the model: an unrecognised option is passed through, and
what happens to it is up to T3. validate refuses an empty option name or value;
check and submit also refuse surrounding spaces, names over 128 bytes and
values over 1024 bytes.

Task context (context:) is accepted by the schema for a pinned project-context
index, but it has not been qualified in the field: do not rely on it. Pass the
same material as input files (inputs, inputs_from) and name them in the prompt.
`

// HelpTopic is one named block of help the command tree can attach wherever it
// wants it.
type HelpTopic struct {
	Name string
	Body string
}

// HelpTopics returns the campaign plan and graph help blocks in a stable order.
func HelpTopics() []HelpTopic {
	return []HelpTopic{
		{Name: "authoring", Body: AuthoringHelp},
		{Name: "fresh", Body: FreshHelp},
		{Name: "routes", Body: RoutesHelp},
		{Name: "plan", Body: PlanHelp},
		{Name: "graph", Body: GraphHelp},
		{Name: "dag-semantics", Body: DAGSemanticsHelp},
		{Name: "commits", Body: CommitsHelp},
		{Name: "static-versus-dynamic", Body: StaticVersusDynamicHelp},
		{Name: "readiness", Body: ReadinessHelp},
		{Name: "rerun", Body: RerunHelp},
		{Name: "notify", Body: NotifyHelp},
	}
}
