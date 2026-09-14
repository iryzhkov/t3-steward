package campaign

import (
	"errors"
	"fmt"
	"path"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// PlanSchemaVersion is the version of the JSON plan document. It is part of the
// campaign contract from the first release: an agent that parses a plan reads
// this number before it trusts any other field, and a breaking change to the
// document raises it.
const PlanSchemaVersion = 1

// DependencyMountPrefix is the workspace-relative directory under which the
// coordinator materializes a dependency's declared outputs for a successor.
// The projection reports the resulting path because it is knowable from the
// manifest alone, and a prompt author needs it to read the artifact.
const DependencyMountPrefix = ".t3/dependencies"

// Origin says where an effective task setting came from.
//
// The projection is built from an already-defaulted manifest, in which the
// workflow-level settings have been merged into every task, so the raw
// declaration is no longer present. Origin is therefore derived by comparing a
// task's effective value with the workflow-level declaration: a task that
// restates the workflow value byte for byte is reported as inherited, because
// the two are indistinguishable after defaulting and the effective value is the
// same either way. Origin never changes the value that is reported; it only
// says whether the task narrowed it.
type Origin string

const (
	// OriginUnset means neither the workflow nor the task declared the setting.
	OriginUnset Origin = "unset"
	// OriginInherited means the effective value equals the workflow declaration.
	OriginInherited Origin = "inherited"
	// OriginTask means a task-level declaration changed the effective value.
	OriginTask Origin = "task"
)

// Options carries the facts a static projection cannot derive from the manifest
// itself. Every field is optional: a plan built without them is still complete
// and still correct, it simply omits what it was not told.
type Options struct {
	// Source is the campaign directory as the operator named it.
	Source string
	// Digest is the bundle digest that submission will send, as computed by the
	// campaign packer. The projection never computes a digest of its own.
	Digest string
	// InputFiles are the bundle-relative regular files the declared input
	// patterns matched, as resolved by the campaign loader.
	InputFiles []string
}

// Plan is the read-only static projection of a defaulted version 2 manifest.
//
// It reports only what the manifest knows. It creates no domain records, reads
// no coordinator state, and deliberately says nothing about whether a worker, a
// provider route or quota will be available later: placement and admission are
// decided after submission against a capacity snapshot that does not exist yet.
// The authoritative dynamic answer is the post-submission explanation.
type Plan struct {
	SchemaVersion int         `json:"schemaVersion"`
	Name          string      `json:"name"`
	Source        string      `json:"source,omitempty"`
	Digest        string      `json:"digest,omitempty"`
	Class         string      `json:"class"`
	Environment   Environment `json:"environment"`
	Placement     Placement   `json:"placement"`
	Resources     Resources   `json:"resources"`
	Routes        []Route     `json:"routes,omitempty"`
	Preflight     []Step      `json:"preflight,omitempty"`
	Inputs        []Input     `json:"inputs,omitempty"`
	Tasks         []Task      `json:"tasks"`
	Waves         []Wave      `json:"waves"`
	Edges         []Edge      `json:"edges"`
	Roots         []string    `json:"roots"`
	Leaves        []string    `json:"leaves"`
	Components    []Component `json:"components"`
	Sink          Sink        `json:"sink"`
	Totals        Totals      `json:"totals"`
}

// Environment is the project workspace every task of the run is prepared in.
type Environment struct {
	Project string `json:"project"`
	Type    string `json:"type"`
	Scope   string `json:"scope"`
	Ref     string `json:"ref,omitempty"`
}

// Placement is the effective host and capability constraint.
type Placement struct {
	Hosts    []string `json:"hosts,omitempty"`
	Requires []string `json:"requires,omitempty"`
}

// Resources is the effective resource demand, with any preset already expanded.
type Resources struct {
	Preset            string   `json:"preset,omitempty"`
	MinCPUClass       string   `json:"minCpuClass,omitempty"`
	PreferredCPUClass string   `json:"preferredCpuClass,omitempty"`
	CPUUnits          *float64 `json:"cpuUnits,omitempty"`
	MemoryMB          *int     `json:"memoryMb,omitempty"`
	ScratchMB         *int     `json:"scratchMb,omitempty"`
}

// Route is one ordered provider candidate. Listing a route is not a claim that
// the route will be healthy or that its quota pool will have room.
type Route struct {
	Host      string        `json:"host,omitempty"`
	Instance  string        `json:"instance"`
	Model     string        `json:"model"`
	QuotaPool string        `json:"quotaPool,omitempty"`
	Options   []RouteOption `json:"options,omitempty"`
}

// RouteOption is one provider option, projected as a sorted list rather than a
// map so that the document has one canonical order.
type RouteOption struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Step is one preflight step as the worker will run it, with the manifest
// defaults already applied.
type Step struct {
	ID             string   `json:"id"`
	Kind           string   `json:"kind"`
	Command        []string `json:"command,omitempty"`
	Probe          string   `json:"probe,omitempty"`
	FailurePolicy  string   `json:"failurePolicy"`
	Include        string   `json:"include"`
	MaxOutputBytes int      `json:"maxOutputBytes"`
	Timeout        string   `json:"timeout"`
	Required       bool     `json:"required,omitempty"`
}

// Input is one declared input pattern and, when the loader resolved them, the
// bundle-relative files it matched.
type Input struct {
	Pattern string   `json:"pattern"`
	Files   []string `json:"files,omitempty"`
}

// Directory is one attached operator directory resource.
type Directory struct {
	Worker   string `json:"worker"`
	Resource string `json:"resource"`
	Revision string `json:"revision"`
	Access   string `json:"access,omitempty"`
}

// Timing is the schedulable window a task declared. These bound when a task may
// start or stop being worth starting; they do not promise when it will run.
type Timing struct {
	NotBefore *time.Time `json:"notBefore,omitempty"`
	Deadline  *time.Time `json:"deadline,omitempty"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
}

// Declared reports whether the task constrained its schedulable window at all.
func (t Timing) Declared() bool {
	return t.NotBefore != nil || t.Deadline != nil || t.ExpiresAt != nil
}

// Binding is one cross-task artifact binding declared through inputs_from.
type Binding struct {
	Producer  string   `json:"producer"`
	Artifacts []string `json:"artifacts"`
	// MountDir is absent on purpose. Dependency artifacts are materialized under
	// DependencyMountPrefix in a directory named for the producing task's ID, and
	// that ID is assigned at ingestion. A static plan cannot know it, and
	// fabricating one from the manifest name produced a path that looked
	// authoritative and did not exist, which is precisely what this projection is
	// forbidden to do: report only what the manifest knows.
	//
	// The running task is told the real locations through its prompt envelope.
	// Verification commands should therefore assert the task's own outputs rather
	// than a dependency path the author would have to guess.
}

// Commit is one Git commit a task declares it will produce. It is an output
// like any other as far as the graph is concerned, and it is reported apart
// from Outputs because what a successor receives is not a file the task wrote:
// it is the commit's provenance record, and the commit itself arrives fetched
// into the successor's checkout under a campaign-scoped ref.
type Commit struct {
	Name string `json:"name"`
	// Revision is the revision the manifest declared, resolved in the producing
	// workspace when the task finishes. It is absent when the manifest declared
	// none, which the worker resolves as HEAD; the document reports the
	// declaration and the text rendering names the default.
	Revision string `json:"revision,omitempty"`
}

// Task is one projected node of the graph.
type Task struct {
	Name       string `json:"name"`
	Wave       int    `json:"wave"`
	PromptFile string `json:"promptFile,omitempty"`
	Class      string `json:"class"`
	ClassFrom  Origin `json:"classFrom"`
	// Needs are the dependencies inside this workflow, sorted by name.
	Needs []string `json:"needs,omitempty"`
	// ExternalNeeds are dependencies on nodes of another run, written
	// <run>/<task>. They are real dependencies, but they sit outside this
	// graph, so they add no wave depth and their state is not statically
	// knowable.
	ExternalNeeds []string    `json:"externalNeeds,omitempty"`
	Dependents    []string    `json:"dependents,omitempty"`
	InputsFrom    []Binding   `json:"inputsFrom,omitempty"`
	Outputs       []string    `json:"outputs,omitempty"`
	Commits       []Commit    `json:"commits,omitempty"`
	Verify        []string    `json:"verify,omitempty"`
	Placement     Placement   `json:"placement"`
	PlacementFrom Origin      `json:"placementFrom"`
	Resources     Resources   `json:"resources"`
	ResourcesFrom Origin      `json:"resourcesFrom"`
	Routes        []Route     `json:"routes,omitempty"`
	RoutesFrom    Origin      `json:"routesFrom"`
	Preflight     []Step      `json:"preflight,omitempty"`
	PreflightFrom Origin      `json:"preflightFrom"`
	Directories   []Directory `json:"directories,omitempty"`
	ResourceLocks []string    `json:"resourceLocks,omitempty"`
	Importance    int         `json:"importance"`
	Difficulty    int         `json:"difficulty"`
	MaxTurns      int         `json:"maxTurns"`
	EstimatedCost *float64    `json:"estimatedCost,omitempty"`
	Timing        Timing      `json:"timing"`
	// Root is true when the task declares no dependency of any kind and may be
	// admitted as soon as the run starts.
	Root bool `json:"root"`
	// Leaf is true when no other task in this workflow depends on the task.
	Leaf bool `json:"leaf"`
}

// Wave is a set of tasks whose in-workflow dependencies all sit in earlier
// waves. Tasks in one wave may run together; whether they actually do is a
// capacity decision made after submission.
type Wave struct {
	Index int      `json:"index"`
	Tasks []string `json:"tasks"`
}

// Edge is one dependency: To depends on From.
type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
	// External is true when From names a node of another run rather than a task
	// of this workflow.
	External bool `json:"external,omitempty"`
}

// Component is one weakly connected group of tasks. Several components in one
// manifest are valid: they are independent subgraphs that share a run, a
// settlement and nothing else.
type Component struct {
	Tasks []string `json:"tasks"`
}

// Sink describes the terminal task the coordinator creates for the run. It is
// not declared in the manifest and cannot be authored: the coordinator binds it
// to every task in the run, not only to the leaves, and settles the run when
// they are all terminal.
type Sink struct {
	Name string `json:"name"`
	// Needs lists every task of the workflow, which is what the coordinator
	// binds the sink to.
	Needs []string `json:"needs"`
}

// Totals are the counts a reader wants before reading the graph itself.
type Totals struct {
	Tasks             int `json:"tasks"`
	Edges             int `json:"edges"`
	ExternalEdges     int `json:"externalEdges"`
	Waves             int `json:"waves"`
	Roots             int `json:"roots"`
	Leaves            int `json:"leaves"`
	Components        int `json:"components"`
	InputPatterns     int `json:"inputPatterns"`
	InputFiles        int `json:"inputFiles"`
	Outputs           int `json:"outputs"`
	Commits           int `json:"commits"`
	ArtifactBindings  int `json:"artifactBindings"`
	TimingConstrained int `json:"timingConstrained"`
}

// Project builds the static plan of an already-defaulted manifest, as returned
// by the ingestion parser. It is pure: the same manifest always produces the
// same waves, the same order inside each wave, and the same document bytes.
//
// Project takes the manifest rather than a loaded bundle on purpose. The
// projection has no business reading the filesystem, and taking the parsed
// manifest keeps it from growing a second loader beside the ingestion one.
func Project(manifest backlog.Manifest, opts Options) (Plan, error) {
	names := make([]string, 0, len(manifest.Tasks))
	for name := range manifest.Tasks {
		names = append(names, name)
	}
	sort.Strings(names)

	needs := make(map[string][]string, len(names))
	external := make(map[string][]string, len(names))
	for _, name := range names {
		for _, dependency := range manifest.Tasks[name].Needs {
			if strings.Contains(dependency, "/") {
				external[name] = append(external[name], dependency)
				continue
			}
			if _, ok := manifest.Tasks[dependency]; !ok {
				return Plan{}, fmt.Errorf("task %s needs missing task %q", name, dependency)
			}
			needs[name] = append(needs[name], dependency)
		}
		sort.Strings(needs[name])
		sort.Strings(external[name])
	}

	depth, err := waveDepths(names, needs)
	if err != nil {
		return Plan{}, err
	}

	dependents := make(map[string][]string, len(names))
	for _, name := range names {
		for _, dependency := range needs[name] {
			dependents[dependency] = append(dependents[dependency], name)
		}
	}
	for name := range dependents {
		sort.Strings(dependents[name])
	}

	plan := Plan{
		SchemaVersion: PlanSchemaVersion,
		Name:          manifest.Name,
		Source:        opts.Source,
		Digest:        opts.Digest,
		Class:         string(manifest.Class),
		Environment: Environment{
			Project: manifest.Environment.Project,
			Type:    manifest.Environment.Type,
			Scope:   manifest.Environment.Scope,
			Ref:     manifest.Environment.Ref,
		},
		Placement: projectPlacement(manifest.Placement),
		Resources: projectResources(manifest.Resources),
		Routes:    projectRoutes(manifest.Routes),
		Preflight: projectPreflight(manifest.Preflight),
		Inputs:    projectInputs(manifest.Inputs, opts.InputFiles),
		Tasks:     make([]Task, 0, len(names)),
		Sink:      Sink{Name: domain.SinkTaskName, Needs: append([]string(nil), names...)},
	}
	if plan.Sink.Needs == nil {
		plan.Sink.Needs = []string{}
	}

	workflowPlacement := projectPlacement(manifest.Placement)
	workflowResources := projectResources(manifest.Resources)
	workflowRoutes := projectRoutes(manifest.Routes)
	workflowPreflight := projectPreflight(manifest.Preflight)

	for _, name := range names {
		source := manifest.Tasks[name]
		task := Task{
			Name:          name,
			Wave:          depth[name],
			PromptFile:    source.PromptFile,
			Class:         string(source.Class),
			ClassFrom:     originOf(source.Class == manifest.Class, string(source.Class) != ""),
			Needs:         needs[name],
			ExternalNeeds: external[name],
			Dependents:    dependents[name],
			InputsFrom:    projectBindings(source.InputsFrom),
			Outputs:       cloneStrings(source.Outputs),
			Commits:       projectCommits(source.Commits),
			Verify:        cloneStrings(source.Verify),
			Placement:     projectPlacement(source.Placement),
			Resources:     projectResources(source.Resources),
			Routes:        projectRoutes(source.Routes),
			Preflight:     projectPreflight(source.Preflight),
			Directories:   projectDirectories(source.Directories),
			ResourceLocks: cloneStrings(source.ResourceLocks),
			Importance:    source.Importance,
			Difficulty:    source.Difficulty,
			MaxTurns:      source.MaxTurns,
			EstimatedCost: cloneFloat(source.EstimatedCost),
			Timing: Timing{
				NotBefore: normalizeTime(source.NotBefore),
				Deadline:  normalizeTime(source.Deadline),
				ExpiresAt: normalizeTime(source.ExpiresAt),
			},
			Root: len(needs[name]) == 0 && len(external[name]) == 0,
			Leaf: len(dependents[name]) == 0,
		}
		task.PlacementFrom = originOf(
			reflect.DeepEqual(task.Placement, workflowPlacement),
			len(task.Placement.Hosts) != 0 || len(task.Placement.Requires) != 0,
		)
		task.ResourcesFrom = originOf(
			reflect.DeepEqual(task.Resources, workflowResources),
			task.Resources != (Resources{}),
		)
		task.RoutesFrom = originOf(reflect.DeepEqual(task.Routes, workflowRoutes), len(task.Routes) != 0)
		task.PreflightFrom = originOf(reflect.DeepEqual(task.Preflight, workflowPreflight), len(task.Preflight) != 0)
		plan.Tasks = append(plan.Tasks, task)
	}
	sort.SliceStable(plan.Tasks, func(i, j int) bool {
		if plan.Tasks[i].Wave != plan.Tasks[j].Wave {
			return plan.Tasks[i].Wave < plan.Tasks[j].Wave
		}
		return plan.Tasks[i].Name < plan.Tasks[j].Name
	})

	plan.Waves = buildWaves(plan.Tasks)
	plan.Edges = buildEdges(names, needs, external)
	plan.Components = buildComponents(names, needs)
	for _, task := range plan.Tasks {
		if task.Root {
			plan.Roots = append(plan.Roots, task.Name)
		}
		if task.Leaf {
			plan.Leaves = append(plan.Leaves, task.Name)
		}
	}
	sort.Strings(plan.Roots)
	sort.Strings(plan.Leaves)
	if plan.Roots == nil {
		plan.Roots = []string{}
	}
	if plan.Leaves == nil {
		plan.Leaves = []string{}
	}
	plan.Totals = buildTotals(plan)
	return plan, nil
}

// waveDepths assigns every task the earliest wave its in-workflow dependencies
// allow. A dependency on another run's node adds no depth: that node is not in
// this graph and its progress is not statically knowable.
func waveDepths(names []string, needs map[string][]string) (map[string]int, error) {
	depth := make(map[string]int, len(names))
	remaining := make(map[string]int, len(names))
	ready := make([]string, 0, len(names))
	for _, name := range names {
		remaining[name] = len(needs[name])
		if remaining[name] == 0 {
			ready = append(ready, name)
			depth[name] = 0
		}
	}
	dependents := make(map[string][]string, len(names))
	for _, name := range names {
		for _, dependency := range needs[name] {
			dependents[dependency] = append(dependents[dependency], name)
		}
	}
	settled := 0
	for len(ready) > 0 {
		sort.Strings(ready)
		next := make([]string, 0, len(ready))
		for _, name := range ready {
			settled++
			for _, dependent := range dependents[name] {
				if depth[name]+1 > depth[dependent] {
					depth[dependent] = depth[name] + 1
				}
				remaining[dependent]--
				if remaining[dependent] == 0 {
					next = append(next, dependent)
				}
			}
		}
		ready = next
	}
	if settled != len(names) {
		return nil, errors.New("workflow contains a dependency cycle")
	}
	return depth, nil
}

func buildWaves(tasks []Task) []Wave {
	waves := []Wave{}
	for _, task := range tasks {
		if len(waves) == 0 || waves[len(waves)-1].Index != task.Wave {
			waves = append(waves, Wave{Index: task.Wave, Tasks: []string{}})
		}
		current := &waves[len(waves)-1]
		current.Tasks = append(current.Tasks, task.Name)
	}
	return waves
}

func buildEdges(names []string, needs, external map[string][]string) []Edge {
	edges := []Edge{}
	for _, name := range names {
		for _, dependency := range needs[name] {
			edges = append(edges, Edge{From: dependency, To: name})
		}
		for _, dependency := range external[name] {
			edges = append(edges, Edge{From: dependency, To: name, External: true})
		}
	}
	sort.SliceStable(edges, func(i, j int) bool {
		if edges[i].From != edges[j].From {
			return edges[i].From < edges[j].From
		}
		return edges[i].To < edges[j].To
	})
	return edges
}

// buildComponents groups tasks that are reachable from one another when the
// dependency edges are read as undirected.
func buildComponents(names []string, needs map[string][]string) []Component {
	parent := make(map[string]string, len(names))
	for _, name := range names {
		parent[name] = name
	}
	var find func(string) string
	find = func(name string) string {
		if parent[name] != name {
			parent[name] = find(parent[name])
		}
		return parent[name]
	}
	for _, name := range names {
		for _, dependency := range needs[name] {
			left, right := find(name), find(dependency)
			if left != right {
				parent[left] = right
			}
		}
	}
	groups := make(map[string][]string, len(names))
	for _, name := range names {
		root := find(name)
		groups[root] = append(groups[root], name)
	}
	components := make([]Component, 0, len(groups))
	for _, members := range groups {
		sort.Strings(members)
		components = append(components, Component{Tasks: members})
	}
	sort.SliceStable(components, func(i, j int) bool {
		return components[i].Tasks[0] < components[j].Tasks[0]
	})
	return components
}

func buildTotals(plan Plan) Totals {
	totals := Totals{
		Tasks:         len(plan.Tasks),
		Edges:         len(plan.Edges),
		Waves:         len(plan.Waves),
		Roots:         len(plan.Roots),
		Leaves:        len(plan.Leaves),
		Components:    len(plan.Components),
		InputPatterns: len(plan.Inputs),
	}
	for _, edge := range plan.Edges {
		if edge.External {
			totals.ExternalEdges++
		}
	}
	for _, input := range plan.Inputs {
		totals.InputFiles += len(input.Files)
	}
	for _, task := range plan.Tasks {
		totals.Outputs += len(task.Outputs)
		totals.Commits += len(task.Commits)
		for _, binding := range task.InputsFrom {
			totals.ArtifactBindings += len(binding.Artifacts)
		}
		if task.Timing.Declared() {
			totals.TimingConstrained++
		}
	}
	return totals
}

func originOf(equalsWorkflow, declared bool) Origin {
	switch {
	case !declared:
		return OriginUnset
	case equalsWorkflow:
		return OriginInherited
	default:
		return OriginTask
	}
}

func projectPlacement(placement backlog.ManifestPlacement) Placement {
	return Placement{
		Hosts:    sortedUnique(placement.Hosts),
		Requires: sortedUnique(placement.Requires),
	}
}

func projectResources(resources backlog.ManifestResources) Resources {
	return Resources{
		Preset:            resources.Preset,
		MinCPUClass:       string(resources.MinCPUClass),
		PreferredCPUClass: string(resources.PreferredCPUClass),
		CPUUnits:          cloneFloat(resources.CPUUnits),
		MemoryMB:          cloneInt(resources.MemoryMB),
		ScratchMB:         cloneInt(resources.ScratchMB),
	}
}

func projectRoutes(routes []backlog.ManifestRoute) []Route {
	if len(routes) == 0 {
		return nil
	}
	result := make([]Route, 0, len(routes))
	for _, route := range routes {
		projected := Route{
			Host:      route.Host,
			Instance:  route.Instance,
			Model:     route.Model,
			QuotaPool: route.QuotaPool,
		}
		for name, value := range route.Options {
			projected.Options = append(projected.Options, RouteOption{Name: name, Value: value})
		}
		sort.SliceStable(projected.Options, func(i, j int) bool {
			return projected.Options[i].Name < projected.Options[j].Name
		})
		result = append(result, projected)
	}
	return result
}

func projectPreflight(preflight backlog.ManifestPreflight) []Step {
	if len(preflight.Steps) == 0 {
		return nil
	}
	result := make([]Step, 0, len(preflight.Steps))
	for _, step := range preflight.Steps {
		result = append(result, Step{
			ID:             step.ID,
			Kind:           step.Kind,
			Command:        cloneStrings(step.Command),
			Probe:          step.Probe,
			FailurePolicy:  step.FailurePolicy,
			Include:        step.Include,
			MaxOutputBytes: step.MaxOutputBytes,
			Timeout:        step.Timeout.String(),
			Required:       step.Required,
		})
	}
	return result
}

func projectInputs(patterns []string, files []string) []Input {
	if len(patterns) == 0 {
		return nil
	}
	resolved := append([]string(nil), files...)
	sort.Strings(resolved)
	result := make([]Input, 0, len(patterns))
	for _, pattern := range patterns {
		input := Input{Pattern: pattern}
		for _, file := range resolved {
			if matched, err := path.Match(pattern, file); err == nil && matched {
				input.Files = append(input.Files, file)
			}
		}
		result = append(result, input)
	}
	return result
}

func projectBindings(inputsFrom map[string][]string) []Binding {
	if len(inputsFrom) == 0 {
		return nil
	}
	producers := make([]string, 0, len(inputsFrom))
	for producer := range inputsFrom {
		producers = append(producers, producer)
	}
	sort.Strings(producers)
	result := make([]Binding, 0, len(producers))
	for _, producer := range producers {
		binding := Binding{Producer: producer, Artifacts: cloneStrings(inputsFrom[producer])}
		for _, artifact := range binding.Artifacts {
			_ = artifact
		}
		result = append(result, binding)
	}
	return result
}

// projectCommits reports the commits a task declared, in manifest order, which
// is the order the author wrote and the order the finalizer publishes in.
func projectCommits(commits []backlog.ManifestCommit) []Commit {
	if len(commits) == 0 {
		return nil
	}
	result := make([]Commit, 0, len(commits))
	for _, commit := range commits {
		result = append(result, Commit{Name: commit.Name, Revision: commit.Revision})
	}
	return result
}

func projectDirectories(requests []directoryresource.Request) []Directory {
	if len(requests) == 0 {
		return nil
	}
	result := make([]Directory, 0, len(requests))
	for _, request := range requests {
		result = append(result, Directory{
			Worker:   request.WorkerID,
			Resource: request.ResourceID,
			Revision: request.Revision,
			Access:   string(request.Access),
		})
	}
	return result
}

func sortedUnique(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func cloneStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return append([]string(nil), values...)
}

func cloneFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

// normalizeTime pins a declared instant to UTC so that two manifests naming the
// same moment in different zones project to the same document.
func normalizeTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.UTC()
	return &normalized
}
