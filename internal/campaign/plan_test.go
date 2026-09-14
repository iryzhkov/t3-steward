package campaign

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// parse defaults a manifest through the real ingestion parser, so the tests
// project exactly what submission would ingest rather than a hand-built struct.
func parse(t *testing.T, source string) backlog.Manifest {
	t.Helper()
	manifest, err := backlog.ParseManifest([]byte(source))
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	return manifest
}

func project(t *testing.T, source string) Plan {
	t.Helper()
	plan, err := Project(parse(t, source), Options{})
	if err != nil {
		t.Fatalf("project: %v", err)
	}
	return plan
}

func taskByName(t *testing.T, plan Plan, name string) Task {
	t.Helper()
	for _, task := range plan.Tasks {
		if task.Name == name {
			return task
		}
	}
	t.Fatalf("task %q is missing from the plan", name)
	return Task{}
}

const headerYAML = `version: 2
name: example
environment:
  project: example-project
`

const joinYAML = headerYAML + `tasks:
  beta:
    prompt_file: prompts/beta.md
    outputs: [beta.md]
  alpha:
    prompt_file: prompts/alpha.md
    outputs: [alpha.md, alpha.txt]
  join:
    prompt_file: prompts/join.md
    needs: [alpha, beta]
    inputs_from:
      beta: [beta.md]
      alpha: [alpha.md, alpha.txt]
    outputs: [combined.md]
    verify: [test -s combined.md]
`

func TestProjectParallelRootsAndJoin(t *testing.T) {
	plan := project(t, joinYAML)

	if got := []string{plan.Tasks[0].Name, plan.Tasks[1].Name, plan.Tasks[2].Name}; !reflect.DeepEqual(got, []string{"alpha", "beta", "join"}) {
		t.Fatalf("task order = %v", got)
	}
	want := []Wave{{Index: 0, Tasks: []string{"alpha", "beta"}}, {Index: 1, Tasks: []string{"join"}}}
	if !reflect.DeepEqual(plan.Waves, want) {
		t.Fatalf("waves = %#v, want %#v", plan.Waves, want)
	}
	if !reflect.DeepEqual(plan.Roots, []string{"alpha", "beta"}) {
		t.Fatalf("roots = %v", plan.Roots)
	}
	if !reflect.DeepEqual(plan.Leaves, []string{"join"}) {
		t.Fatalf("leaves = %v", plan.Leaves)
	}
	wantEdges := []Edge{{From: "alpha", To: "join"}, {From: "beta", To: "join"}}
	if !reflect.DeepEqual(plan.Edges, wantEdges) {
		t.Fatalf("edges = %#v", plan.Edges)
	}
	if !reflect.DeepEqual(plan.Components, []Component{{Tasks: []string{"alpha", "beta", "join"}}}) {
		t.Fatalf("components = %#v", plan.Components)
	}

	alpha := taskByName(t, plan, "alpha")
	if !alpha.Root || alpha.Leaf || !reflect.DeepEqual(alpha.Dependents, []string{"join"}) {
		t.Fatalf("alpha = %#v", alpha)
	}
	join := taskByName(t, plan, "join")
	if !join.Leaf || join.Root {
		t.Fatalf("join root/leaf = %v/%v", join.Root, join.Leaf)
	}
	wantBindings := []Binding{
		{Producer: "alpha", Artifacts: []string{"alpha.md", "alpha.txt"}},
		{Producer: "beta", Artifacts: []string{"beta.md"}},
	}
	if !reflect.DeepEqual(join.InputsFrom, wantBindings) {
		t.Fatalf("join bindings = %#v", join.InputsFrom)
	}
	if plan.Sink.Name != domain.SinkTaskName {
		t.Fatalf("sink name = %q", plan.Sink.Name)
	}
	// The coordinator binds its sink to every task, not only to the leaves.
	if !reflect.DeepEqual(plan.Sink.Needs, []string{"alpha", "beta", "join"}) {
		t.Fatalf("sink needs = %v", plan.Sink.Needs)
	}
	wantTotals := Totals{Tasks: 3, Edges: 2, Waves: 2, Roots: 2, Leaves: 1, Components: 1, Outputs: 4, ArtifactBindings: 3}
	if plan.Totals != wantTotals {
		t.Fatalf("totals = %#v, want %#v", plan.Totals, wantTotals)
	}
}

func TestProjectDisconnectedComponents(t *testing.T) {
	plan := project(t, headerYAML+`tasks:
  one:
    prompt_file: prompts/one.md
  two:
    prompt_file: prompts/two.md
    needs: [one]
  lonely:
    prompt_file: prompts/lonely.md
  far:
    prompt_file: prompts/far.md
    needs: [lonely]
`)

	want := []Component{{Tasks: []string{"far", "lonely"}}, {Tasks: []string{"one", "two"}}}
	if !reflect.DeepEqual(plan.Components, want) {
		t.Fatalf("components = %#v, want %#v", plan.Components, want)
	}
	if !reflect.DeepEqual(plan.Roots, []string{"lonely", "one"}) {
		t.Fatalf("roots = %v", plan.Roots)
	}
	if !reflect.DeepEqual(plan.Leaves, []string{"far", "two"}) {
		t.Fatalf("leaves = %v", plan.Leaves)
	}
	// Both components advance together: the wave is a depth, not a queue.
	wantWaves := []Wave{{Index: 0, Tasks: []string{"lonely", "one"}}, {Index: 1, Tasks: []string{"far", "two"}}}
	if !reflect.DeepEqual(plan.Waves, wantWaves) {
		t.Fatalf("waves = %#v", plan.Waves)
	}
}

func TestProjectEmptyWorkflow(t *testing.T) {
	plan := project(t, headerYAML+"tasks: {}\n")

	if len(plan.Tasks) != 0 || len(plan.Waves) != 0 || len(plan.Edges) != 0 {
		t.Fatalf("empty workflow projected %#v", plan)
	}
	if plan.Roots == nil || plan.Leaves == nil || plan.Sink.Needs == nil || plan.Components == nil {
		t.Fatal("empty workflow must project empty lists, not nil, so the JSON stays an array")
	}
	if plan.Totals != (Totals{}) {
		t.Fatalf("totals = %#v", plan.Totals)
	}
	if encoded, err := RenderJSON(plan); err != nil || !strings.Contains(string(encoded), `"tasks": []`) {
		t.Fatalf("render empty plan: %v, %s", err, encoded)
	}
	if text := RenderText(plan); !strings.Contains(text, "no tasks") {
		t.Fatalf("text = %s", text)
	}
}

func TestProjectTimingFields(t *testing.T) {
	plan := project(t, headerYAML+`tasks:
  early:
    prompt_file: prompts/early.md
    not_before: 2026-09-20T11:00:00+02:00
    deadline: 2026-09-21T09:00:00Z
    expires_at: 2026-09-22T09:00:00Z
  whenever:
    prompt_file: prompts/whenever.md
`)

	early := taskByName(t, plan, "early")
	if !early.Timing.Declared() {
		t.Fatal("early declared a window and the projection dropped it")
	}
	// Declared instants are pinned to UTC so that two manifests naming the same
	// moment in different zones project to the same document.
	wantNotBefore := time.Date(2026, 9, 20, 9, 0, 0, 0, time.UTC)
	if !early.Timing.NotBefore.Equal(wantNotBefore) || early.Timing.NotBefore.Location() != time.UTC {
		t.Fatalf("not before = %s", early.Timing.NotBefore)
	}
	if !early.Timing.Deadline.Equal(time.Date(2026, 9, 21, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("deadline = %s", early.Timing.Deadline)
	}
	if !early.Timing.ExpiresAt.Equal(time.Date(2026, 9, 22, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("expires = %s", early.Timing.ExpiresAt)
	}
	if taskByName(t, plan, "whenever").Timing.Declared() {
		t.Fatal("whenever declared no window")
	}
	if plan.Totals.TimingConstrained != 1 {
		t.Fatalf("timing constrained = %d", plan.Totals.TimingConstrained)
	}
	if text := RenderText(plan); !strings.Contains(text, "not before 2026-09-20T09:00:00Z") {
		t.Fatalf("text = %s", text)
	}
}

const inheritanceYAML = `version: 2
name: inheritance
class: required
placement:
  hosts: [alpha, beta]
  requires: [internet]
environment:
  project: example-project
resources:
  preset: light
preflight:
  steps:
    - id: source
      kind: context
      probe: git_head
routes:
  - instance: workflowInstance
    model: workflow-model
tasks:
  inherits:
    prompt_file: prompts/inherits.md
  overrides:
    prompt_file: prompts/overrides.md
    class: surplus
    placement:
      hosts: [alpha]
    resources:
      preset: build
    preflight:
      steps:
        - id: tests
          kind: check
          command: [go, test, ./...]
    routes:
      - instance: taskInstance
        model: task-model
`

func TestProjectInheritedVersusTaskLevel(t *testing.T) {
	plan := project(t, inheritanceYAML)

	inherits := taskByName(t, plan, "inherits")
	for _, check := range []struct {
		name   string
		actual Origin
	}{
		{"class", inherits.ClassFrom},
		{"placement", inherits.PlacementFrom},
		{"resources", inherits.ResourcesFrom},
		{"routes", inherits.RoutesFrom},
		{"preflight", inherits.PreflightFrom},
	} {
		if check.actual != OriginInherited {
			t.Fatalf("inherits %s = %q, want inherited", check.name, check.actual)
		}
	}
	if inherits.Class != string(domain.TaskClassRequired) {
		t.Fatalf("inherits class = %q", inherits.Class)
	}
	if !reflect.DeepEqual(inherits.Placement.Hosts, []string{"alpha", "beta"}) {
		t.Fatalf("inherits hosts = %v", inherits.Placement.Hosts)
	}
	if inherits.Resources.MinCPUClass != "low" {
		t.Fatalf("inherits resources = %#v", inherits.Resources)
	}

	overrides := taskByName(t, plan, "overrides")
	for _, check := range []struct {
		name   string
		actual Origin
	}{
		{"class", overrides.ClassFrom},
		{"placement", overrides.PlacementFrom},
		{"resources", overrides.ResourcesFrom},
		{"routes", overrides.RoutesFrom},
		{"preflight", overrides.PreflightFrom},
	} {
		if check.actual != OriginTask {
			t.Fatalf("overrides %s = %q, want task", check.name, check.actual)
		}
	}
	if overrides.Class != string(domain.TaskClassSurplus) {
		t.Fatalf("overrides class = %q", overrides.Class)
	}
	// Placement intersects and capabilities merge, so the effective value is
	// narrower than either declaration on its own.
	if !reflect.DeepEqual(overrides.Placement.Hosts, []string{"alpha"}) ||
		!reflect.DeepEqual(overrides.Placement.Requires, []string{"internet"}) {
		t.Fatalf("overrides placement = %#v", overrides.Placement)
	}
	if overrides.Resources.MinCPUClass != "medium" || overrides.Resources.PreferredCPUClass != "high" {
		t.Fatalf("overrides resources = %#v", overrides.Resources)
	}
	if len(overrides.Routes) != 1 || overrides.Routes[0].Instance != "taskInstance" {
		t.Fatalf("overrides routes = %#v", overrides.Routes)
	}
	if len(overrides.Preflight) != 1 || overrides.Preflight[0].ID != "tests" {
		t.Fatalf("overrides preflight = %#v", overrides.Preflight)
	}
	// Defaults the parser applied must be reported as the worker will see them.
	if overrides.Preflight[0].FailurePolicy != backlog.PreflightPolicyRecord ||
		overrides.Preflight[0].Include != backlog.PreflightIncludeSummary ||
		overrides.Preflight[0].Timeout != backlog.DefaultPreflightTimeout.String() {
		t.Fatalf("overrides preflight defaults = %#v", overrides.Preflight[0])
	}
}

func TestProjectUnsetSettingsAreNotReportedAsInherited(t *testing.T) {
	plan := project(t, headerYAML+"tasks:\n  solo:\n    prompt_file: prompts/solo.md\n")

	solo := taskByName(t, plan, "solo")
	if solo.PlacementFrom != OriginUnset || solo.ResourcesFrom != OriginUnset ||
		solo.RoutesFrom != OriginUnset || solo.PreflightFrom != OriginUnset {
		t.Fatalf("unset settings = %#v", solo)
	}
	// Class always has a value after defaulting, so it is always attributable.
	if solo.ClassFrom != OriginInherited || solo.Class != string(domain.TaskClassSurplus) {
		t.Fatalf("class = %q from %q", solo.Class, solo.ClassFrom)
	}
}

func TestProjectExternalDependency(t *testing.T) {
	plan := project(t, headerYAML+`tasks:
  waits:
    prompt_file: prompts/waits.md
    needs: [run-7/build]
`)

	waits := taskByName(t, plan, "waits")
	if !reflect.DeepEqual(waits.ExternalNeeds, []string{"run-7/build"}) || len(waits.Needs) != 0 {
		t.Fatalf("waits = %#v", waits)
	}
	// A cross-run dependency is real, so the task is not a root, but the node is
	// outside this graph and therefore adds no wave depth.
	if waits.Root || waits.Wave != 0 {
		t.Fatalf("root = %v, wave = %d", waits.Root, waits.Wave)
	}
	wantEdges := []Edge{{From: "run-7/build", To: "waits", External: true}}
	if !reflect.DeepEqual(plan.Edges, wantEdges) {
		t.Fatalf("edges = %#v", plan.Edges)
	}
	if plan.Totals.ExternalEdges != 1 || plan.Totals.Roots != 0 {
		t.Fatalf("totals = %#v", plan.Totals)
	}
}

func TestProjectInputsAndOptions(t *testing.T) {
	manifest := parse(t, headerYAML+`inputs:
  - inputs/*.md
  - fixtures/data.json
tasks:
  solo:
    prompt_file: prompts/solo.md
`)
	options := Options{
		Source:     "./campaign",
		Digest:     "sha256:0123",
		InputFiles: []string{"inputs/second.md", "fixtures/data.json", "inputs/first.md"},
	}
	plan, err := Project(manifest, options)
	if err != nil {
		t.Fatalf("project: %v", err)
	}

	want := []Input{
		{Pattern: "inputs/*.md", Files: []string{"inputs/first.md", "inputs/second.md"}},
		{Pattern: "fixtures/data.json", Files: []string{"fixtures/data.json"}},
	}
	if !reflect.DeepEqual(plan.Inputs, want) {
		t.Fatalf("inputs = %#v, want %#v", plan.Inputs, want)
	}
	if plan.Source != "./campaign" || plan.Digest != "sha256:0123" {
		t.Fatalf("source/digest = %q/%q", plan.Source, plan.Digest)
	}
	if plan.Totals.InputPatterns != 2 || plan.Totals.InputFiles != 3 {
		t.Fatalf("totals = %#v", plan.Totals)
	}
}

func TestProjectStableAcrossRepeatedParseAndProjection(t *testing.T) {
	// Go randomizes map iteration, and both the task map and inputs_from are
	// maps, so a projection that leaked map order would fail here rather than in
	// somebody's diff.
	first, err := RenderJSON(project(t, joinYAML))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for i := 0; i < 64; i++ {
		next, err := RenderJSON(project(t, joinYAML))
		if err != nil {
			t.Fatalf("render: %v", err)
		}
		if string(next) != string(first) {
			t.Fatalf("projection %d differs:\n%s\n%s", i, first, next)
		}
	}
	plan := project(t, joinYAML)
	if text := RenderText(plan); text != RenderText(plan) {
		t.Fatal("text rendering is not stable")
	}
	if dot := RenderDOT(plan); dot != RenderDOT(plan) {
		t.Fatal("DOT rendering is not stable")
	}
}

func TestProjectRejectsCycleAndMissingDependency(t *testing.T) {
	// The ingestion parser refuses both of these, so they can only be built by
	// hand. The projection still refuses them rather than looping or panicking:
	// it is a library, and its caller is not always the parser.
	cyclic := backlog.Manifest{
		Version: backlog.ManifestVersion,
		Name:    "cyclic",
		Tasks: map[string]backlog.ManifestTask{
			"one": {Needs: backlog.ManifestNeeds{"two"}},
			"two": {Needs: backlog.ManifestNeeds{"one"}},
		},
	}
	if _, err := Project(cyclic, Options{}); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cyclic manifest error = %v", err)
	}

	dangling := backlog.Manifest{
		Version: backlog.ManifestVersion,
		Name:    "dangling",
		Tasks:   map[string]backlog.ManifestTask{"one": {Needs: backlog.ManifestNeeds{"absent"}}},
	}
	if _, err := Project(dangling, Options{}); err == nil || !strings.Contains(err.Error(), "absent") {
		t.Fatalf("dangling manifest error = %v", err)
	}
}

func TestProjectDeepWaves(t *testing.T) {
	// A task waits for its slowest dependency, so its wave is one past the
	// deepest of them rather than one past the first one found.
	plan := project(t, headerYAML+`tasks:
  a:
    prompt_file: prompts/a.md
  b:
    prompt_file: prompts/b.md
    needs: [a]
  c:
    prompt_file: prompts/c.md
    needs: [a, b]
  d:
    prompt_file: prompts/d.md
    needs: [a, c]
`)
	want := []Wave{
		{Index: 0, Tasks: []string{"a"}},
		{Index: 1, Tasks: []string{"b"}},
		{Index: 2, Tasks: []string{"c"}},
		{Index: 3, Tasks: []string{"d"}},
	}
	if !reflect.DeepEqual(plan.Waves, want) {
		t.Fatalf("waves = %#v", plan.Waves)
	}
}
