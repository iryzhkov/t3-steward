package campaign

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// staticFooter is printed after every text plan. The plan is read before the
// work exists, so the one thing it must never let a reader assume is that a
// worker, a route or quota has been reserved.
const staticFooter = "static plan only: waves say what may start together once dependencies succeed,\n" +
	"not when a worker, provider route or quota will be free. After submission,\n" +
	"`campaign explain <run>/<task>` is the authoritative dynamic answer."

// RenderText renders the plan for a reader who has to hold it in their head, or
// in an agent's context: one block of run-wide facts, then one block per wave.
// Settings a task inherited unchanged are named rather than repeated, because
// the workflow block above already printed them.
func RenderText(plan Plan) string {
	var out strings.Builder
	const width = 12

	fmt.Fprintf(&out, "campaign %s\n", plan.Name)
	if plan.Source != "" {
		header(&out, width, "source", plan.Source)
	}
	if plan.Digest != "" {
		header(&out, width, "digest", plan.Digest)
	}
	header(&out, width, "class", plan.Class)
	header(&out, width, "environment", describeEnvironment(plan.Environment))
	header(&out, width, "graph", describeTotals(plan.Totals))
	header(&out, width, "sink", fmt.Sprintf(
		"%s waits for every task in the run, not only the leaves; the coordinator creates and settles it",
		plan.Sink.Name))
	if placement := describePlacement(plan.Placement); placement != "" {
		header(&out, width, "placement", placement)
	}
	if resources := describeResources(plan.Resources); resources != "" {
		header(&out, width, "resources", resources)
	}
	if len(plan.Routes) == 0 {
		header(&out, width, "routes", "none declared")
	} else {
		header(&out, width, "routes", describeRoute(plan.Routes[0]))
		for _, route := range plan.Routes[1:] {
			header(&out, width, "", describeRoute(route))
		}
	}
	for i, step := range plan.Preflight {
		label := ""
		if i == 0 {
			label = "preflight"
		}
		header(&out, width, label, describeStep(step))
	}
	for i, input := range plan.Inputs {
		label := ""
		if i == 0 {
			label = "inputs"
		}
		header(&out, width, label, describeInput(input))
	}
	if len(plan.Components) > 1 {
		for i, component := range plan.Components {
			label := ""
			if i == 0 {
				label = "components"
			}
			header(&out, width, label, strings.Join(component.Tasks, ", "))
		}
	}

	if len(plan.Tasks) == 0 {
		out.WriteString("\nno tasks: the run settles as soon as it starts.\n")
		out.WriteString("\n" + staticFooter + "\n")
		return out.String()
	}

	byName := make(map[string]Task, len(plan.Tasks))
	for _, task := range plan.Tasks {
		byName[task.Name] = task
	}
	for _, wave := range plan.Waves {
		fmt.Fprintf(&out, "\nwave %d (%s)\n", wave.Index, pluralTasks(len(wave.Tasks)))
		for _, name := range wave.Tasks {
			writeTask(&out, byName[name])
		}
	}
	out.WriteString("\n" + staticFooter + "\n")
	return out.String()
}

func writeTask(out *strings.Builder, task Task) {
	const width = 11
	marks := []string{}
	if task.Root {
		marks = append(marks, "root")
	}
	if task.Leaf {
		marks = append(marks, "leaf")
	}
	if len(marks) == 0 {
		fmt.Fprintf(out, "  %s\n", task.Name)
	} else {
		fmt.Fprintf(out, "  %s [%s]\n", task.Name, strings.Join(marks, " "))
	}
	if task.PromptFile != "" {
		field(out, width, "prompt", task.PromptFile)
	}
	field(out, width, "class", fmt.Sprintf("%s (%s)", task.Class, task.ClassFrom))
	if len(task.Needs) > 0 {
		field(out, width, "needs", strings.Join(task.Needs, ", "))
	}
	for i, dependency := range task.ExternalNeeds {
		label := ""
		if i == 0 {
			label = "needs (run)"
		}
		field(out, width, label, dependency+" -- another run's node, not part of this graph")
	}
	for i, binding := range task.InputsFrom {
		label := ""
		if i == 0 {
			label = "inputs from"
		}
		field(out, width, label, describeBinding(binding))
	}
	if len(task.Outputs) > 0 {
		field(out, width, "outputs", strings.Join(task.Outputs, ", "))
	}
	for i, command := range task.Verify {
		label := ""
		if i == 0 {
			label = "verify"
		}
		field(out, width, label, command)
	}
	if len(task.Dependents) > 0 {
		field(out, width, "unblocks", strings.Join(task.Dependents, ", "))
	}
	if value := describePlacement(task.Placement); value != "" {
		field(out, width, "placement", fmt.Sprintf("%s (%s)", value, task.PlacementFrom))
	}
	if value := describeResources(task.Resources); value != "" {
		field(out, width, "resources", fmt.Sprintf("%s (%s)", value, task.ResourcesFrom))
	}
	switch {
	case task.RoutesFrom == OriginUnset:
		// The workflow block already reported that no route was declared.
	case task.RoutesFrom == OriginInherited:
		field(out, width, "routes", fmt.Sprintf("inherited, %d candidate(s)", len(task.Routes)))
	default:
		for i, route := range task.Routes {
			label := ""
			if i == 0 {
				label = "routes"
			}
			field(out, width, label, describeRoute(route)+" (task)")
		}
	}
	switch {
	case task.PreflightFrom == OriginUnset:
	case task.PreflightFrom == OriginInherited:
		field(out, width, "preflight", fmt.Sprintf("inherited, %d step(s)", len(task.Preflight)))
	default:
		for i, step := range task.Preflight {
			label := ""
			if i == 0 {
				label = "preflight"
			}
			field(out, width, label, describeStep(step)+" (task)")
		}
	}
	if len(task.ResourceLocks) > 0 {
		field(out, width, "locks", strings.Join(task.ResourceLocks, ", "))
	}
	for i, directory := range task.Directories {
		label := ""
		if i == 0 {
			label = "directories"
		}
		value := fmt.Sprintf("%s on %s at revision %s", directory.Resource, directory.Worker, directory.Revision)
		if directory.Access != "" {
			value += ", " + directory.Access
		}
		field(out, width, label, value)
	}
	if task.Timing.Declared() {
		field(out, width, "timing", describeTiming(task.Timing))
	}
	field(out, width, "effort", describeEffort(task))
}

func header(out *strings.Builder, width int, label, value string) {
	fmt.Fprintf(out, "%-*s %s\n", width, label, value)
}

func field(out *strings.Builder, width int, label, value string) {
	fmt.Fprintf(out, "    %-*s %s\n", width, label, value)
}

func pluralTasks(count int) string {
	if count == 1 {
		return "1 task"
	}
	return fmt.Sprintf("%d tasks may start together", count)
}

func describeEnvironment(environment Environment) string {
	value := fmt.Sprintf("project %s, %s, scope %s", environment.Project, environment.Type, environment.Scope)
	if environment.Ref != "" {
		value += ", ref " + environment.Ref
	}
	return value
}

func describeTotals(totals Totals) string {
	parts := []string{
		counted(totals.Tasks, "task", "tasks"),
		counted(totals.Edges, "edge", "edges"),
		counted(totals.Waves, "wave", "waves"),
		counted(totals.Roots, "root", "roots"),
		counted(totals.Leaves, "leaf", "leaves"),
		counted(totals.Components, "component", "components"),
	}
	if totals.ExternalEdges > 0 {
		parts = append(parts, counted(totals.ExternalEdges, "cross-run edge", "cross-run edges"))
	}
	if totals.ArtifactBindings > 0 {
		parts = append(parts, counted(totals.ArtifactBindings, "artifact binding", "artifact bindings"))
	}
	if totals.TimingConstrained > 0 {
		parts = append(parts, counted(totals.TimingConstrained, "timing-constrained task", "timing-constrained tasks"))
	}
	return strings.Join(parts, ", ")
}

func counted(count int, singular, plural string) string {
	if count == 1 {
		return "1 " + singular
	}
	return strconv.Itoa(count) + " " + plural
}

func describePlacement(placement Placement) string {
	parts := []string{}
	if len(placement.Hosts) > 0 {
		parts = append(parts, "hosts "+strings.Join(placement.Hosts, ", "))
	}
	if len(placement.Requires) > 0 {
		parts = append(parts, "requires "+strings.Join(placement.Requires, ", "))
	}
	return strings.Join(parts, "; ")
}

func describeResources(resources Resources) string {
	parts := []string{}
	if resources.Preset != "" {
		parts = append(parts, "preset "+resources.Preset)
	}
	if resources.MinCPUClass != "" {
		parts = append(parts, "min cpu "+resources.MinCPUClass)
	}
	if resources.PreferredCPUClass != "" {
		parts = append(parts, "prefer cpu "+resources.PreferredCPUClass)
	}
	if resources.CPUUnits != nil {
		parts = append(parts, "cpu units "+strconv.FormatFloat(*resources.CPUUnits, 'g', -1, 64))
	}
	if resources.MemoryMB != nil {
		parts = append(parts, fmt.Sprintf("memory %d MB", *resources.MemoryMB))
	}
	if resources.ScratchMB != nil {
		parts = append(parts, fmt.Sprintf("scratch %d MB", *resources.ScratchMB))
	}
	return strings.Join(parts, ", ")
}

func describeRoute(route Route) string {
	value := route.Instance + "/" + route.Model
	if route.Host != "" {
		value += " on " + route.Host
	}
	if route.QuotaPool != "" {
		value += ", pool " + route.QuotaPool
	}
	for _, option := range route.Options {
		value += ", " + option.Name + "=" + option.Value
	}
	return value
}

func describeStep(step Step) string {
	work := "probe " + step.Probe
	if len(step.Command) > 0 {
		work = strings.Join(step.Command, " ")
	}
	value := fmt.Sprintf("%s: %s %s, on failure %s, include %s", step.ID, step.Kind, work, step.FailurePolicy, step.Include)
	if step.Required {
		value += ", required"
	}
	return value
}

func describeInput(input Input) string {
	if len(input.Files) == 0 {
		return input.Pattern
	}
	return input.Pattern + " -> " + strings.Join(input.Files, ", ")
}

func describeBinding(binding Binding) string {
	return binding.Producer + ": " + strings.Join(binding.Artifacts, ", ")
}

func describeTiming(timing Timing) string {
	parts := []string{}
	if timing.NotBefore != nil {
		parts = append(parts, "not before "+timing.NotBefore.Format("2006-01-02T15:04:05Z"))
	}
	if timing.Deadline != nil {
		parts = append(parts, "deadline "+timing.Deadline.Format("2006-01-02T15:04:05Z"))
	}
	if timing.ExpiresAt != nil {
		parts = append(parts, "expires "+timing.ExpiresAt.Format("2006-01-02T15:04:05Z"))
	}
	return strings.Join(parts, ", ")
}

func describeEffort(task Task) string {
	value := fmt.Sprintf("importance %d, difficulty %d, max turns %d", task.Importance, task.Difficulty, task.MaxTurns)
	if task.EstimatedCost != nil {
		value += ", estimated cost " + strconv.FormatFloat(*task.EstimatedCost, 'g', -1, 64)
	}
	return value
}

// RenderJSON renders the versioned plan document. The field order is the struct
// order and every list is already canonically ordered by Project, so the same
// manifest always produces the same bytes.
func RenderJSON(plan Plan) ([]byte, error) {
	encoded, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("render campaign plan: %w", err)
	}
	return append(encoded, '\n'), nil
}

// RenderDOT renders the dependency graph. Nodes are manifest task names: the
// coordinator assigns the durable task IDs at ingestion, so a static plan has
// no honest way to print them. The coordinator's terminal sink and any
// cross-run dependency are drawn dashed, because neither is a task this
// manifest declares.
func RenderDOT(plan Plan) string {
	var out strings.Builder
	out.WriteString("// campaign plan, schema version " + strconv.Itoa(plan.SchemaVersion) + "\n")
	out.WriteString("// Nodes are manifest task names; task IDs are assigned at ingestion.\n")
	fmt.Fprintf(&out, "digraph %s {\n", quoteDOT(plan.Name))
	out.WriteString("  rankdir=LR;\n")
	out.WriteString("  labelloc=\"t\";\n")
	fmt.Fprintf(&out, "  label=%s;\n", quoteDOT(plan.Name))
	out.WriteString("  node [shape=box, style=rounded];\n")

	for _, task := range plan.Tasks {
		fmt.Fprintf(&out, "  %s;\n", quoteDOT(task.Name))
	}
	for _, wave := range plan.Waves {
		if len(wave.Tasks) < 2 {
			continue
		}
		out.WriteString("  { rank=same;")
		for _, name := range wave.Tasks {
			fmt.Fprintf(&out, " %s;", quoteDOT(name))
		}
		out.WriteString(" }\n")
	}

	bindings := make(map[string][]string, len(plan.Tasks))
	for _, task := range plan.Tasks {
		for _, binding := range task.InputsFrom {
			key := binding.Producer + "\x00" + task.Name
			bindings[key] = append(bindings[key], binding.Artifacts...)
		}
	}
	for _, edge := range plan.Edges {
		attributes := []string{}
		if edge.External {
			fmt.Fprintf(&out, "  %s [shape=box, style=dashed];\n", quoteDOT(edge.From))
			attributes = append(attributes, "style=dashed")
		}
		if artifacts := bindings[edge.From+"\x00"+edge.To]; len(artifacts) > 0 {
			attributes = append(attributes, "label="+quoteDOT(strings.Join(artifacts, ", ")))
		}
		if len(attributes) == 0 {
			fmt.Fprintf(&out, "  %s -> %s;\n", quoteDOT(edge.From), quoteDOT(edge.To))
			continue
		}
		fmt.Fprintf(&out, "  %s -> %s [%s];\n", quoteDOT(edge.From), quoteDOT(edge.To), strings.Join(attributes, ", "))
	}

	fmt.Fprintf(&out, "  %s [shape=ellipse, style=dashed, label=\"%s (coordinator settlement)\"];\n",
		quoteDOT(plan.Sink.Name), plan.Sink.Name)
	for _, name := range plan.Sink.Needs {
		fmt.Fprintf(&out, "  %s -> %s [style=dashed];\n", quoteDOT(name), quoteDOT(plan.Sink.Name))
	}
	out.WriteString("}\n")
	return out.String()
}

func quoteDOT(value string) string {
	var out strings.Builder
	out.WriteByte('"')
	for _, character := range value {
		switch character {
		case '"', '\\':
			out.WriteByte('\\')
			out.WriteRune(character)
		case '\n':
			out.WriteString("\\n")
		default:
			out.WriteRune(character)
		}
	}
	out.WriteByte('"')
	return out.String()
}
