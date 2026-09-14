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
  docs/examples/campaign/single-lead  one lead task: commit, test receipt, handoff
  docs/examples/campaign/three-node   two analysis tasks joined by a third
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

// DAGSemanticsHelp explains the four manifest fields that make a campaign a
// graph rather than a list. It is referenced from plan, graph and the campaign
// namespace overview.
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

inputs_from
  Dependencies release a task; inputs_from is how the task receives the work.
  Each entry names a direct dependency and the artifacts to take from it, and
  those artifacts must be declared in that dependency's outputs. The files
  appear in the successor's workspace, read-only, at

      .t3/dependencies/<producer>/<artifact>

  Naming a task in inputs_from without also naming it in needs is refused, so
  an artifact can never be read before it exists.

outputs
  The files a task promises to leave behind, relative to its workspace. Only a
  declared output is captured as an artifact, checksummed and retained; anything
  else the task writes stays in the disposable workspace and is lost. Declare an
  output for every file a later task, or a human, has to read.

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

// HelpTopic is one named block of help the command tree can attach wherever it
// wants it.
type HelpTopic struct {
	Name string
	Body string
}

// HelpTopics returns the campaign plan and graph help blocks in a stable order.
func HelpTopics() []HelpTopic {
	return []HelpTopic{
		{Name: "plan", Body: PlanHelp},
		{Name: "graph", Body: GraphHelp},
		{Name: "dag-semantics", Body: DAGSemanticsHelp},
		{Name: "static-versus-dynamic", Body: StaticVersusDynamicHelp},
	}
}
