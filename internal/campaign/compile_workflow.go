package campaign

import (
	"bytes"
	"path"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"gopkg.in/yaml.v3"
)

// The shapes below are the workflow.yaml a compiled unit carries. They mirror
// the version 2 manifest field for field, but every field the manifest would
// default is omitted, and the fields are declared in the order a person reads
// a workflow: what it is, where it runs, what it carries, then its tasks in
// the order they run. Marshalling backlog.Manifest itself would write every
// default field, about sixty lines a task, none of which the plan said.

type compiledWorkflow struct {
	Version int    `yaml:"version"`
	Name    string `yaml:"name"`
	// Class is omitted for surplus, the manifest default.
	Class       domain.TaskClass    `yaml:"class,omitempty"`
	Environment compiledEnvironment `yaml:"environment"`
	Placement   *compiledPlacement  `yaml:"placement,omitempty"`
	Ledger      *compiledLedger     `yaml:"ledger,omitempty"`
	Inputs      []string            `yaml:"inputs"`
	Tasks       compiledTasks       `yaml:"tasks"`
}

// compiledEnvironment omits type and scope: a compiled unit is always a git
// workspace per task, which is what the manifest defaults to.
type compiledEnvironment struct {
	Project string `yaml:"project"`
	Ref     string `yaml:"ref"`
}

type compiledPlacement struct {
	Hosts    []string `yaml:"hosts,omitempty,flow"`
	Requires []string `yaml:"requires,omitempty,flow"`
}

type compiledLedger struct {
	JocastaProject string   `yaml:"jocasta_project"`
	Plan           string   `yaml:"plan,omitempty"`
	Risk           string   `yaml:"risk,omitempty"`
	Acceptance     []string `yaml:"acceptance,omitempty"`
}

// compiledTasks is a struct rather than a map so that the tasks are written
// in the order they run, not alphabetically.
type compiledTasks struct {
	Implement compiledTask `yaml:"implement"`
	Review    compiledTask `yaml:"review"`
}

type compiledTask struct {
	// Role and Options select the route through the coordinator's route
	// policy; Routes pins it instead. Exactly one of Role and Routes is set.
	Role         string               `yaml:"role,omitempty"`
	Options      map[string]string    `yaml:"options,omitempty,flow"`
	Routes       []compiledRoute      `yaml:"routes,omitempty"`
	PromptFile   string               `yaml:"prompt_file"`
	Needs        []string             `yaml:"needs,omitempty,flow"`
	InputsFrom   map[string][]string  `yaml:"inputs_from,omitempty"`
	Outputs      []string             `yaml:"outputs,flow"`
	Commits      []compiledCommit     `yaml:"commits,omitempty"`
	ReviewOutput *domain.ReviewOutput `yaml:"review_output,omitempty"`
	Verify       []string             `yaml:"verify,omitempty"`
	Resources    *compiledResources   `yaml:"resources,omitempty"`
	// MaxTurns is omitted for 0, which the manifest defaults.
	MaxTurns int `yaml:"max_turns,omitempty"`
}

type compiledRoute struct {
	Instance  string            `yaml:"instance"`
	Model     string            `yaml:"model"`
	QuotaPool string            `yaml:"quota_pool,omitempty"`
	Options   map[string]string `yaml:"options,omitempty,flow"`
}

type compiledCommit struct {
	Name     string `yaml:"name"`
	Revision string `yaml:"revision"`
}

type compiledResources struct {
	Preset            string           `yaml:"preset,omitempty"`
	MinCPUClass       backlog.CPUClass `yaml:"min_cpu_class,omitempty"`
	PreferredCPUClass backlog.CPUClass `yaml:"preferred_cpu_class,omitempty"`
	CPUUnits          *float64         `yaml:"cpu_units,omitempty"`
	MemoryMB          *int             `yaml:"memory_mb,omitempty"`
	ScratchMB         *int             `yaml:"scratch_mb,omitempty"`
}

// compileRouting is how one template task is routed: by the route the plan
// pins, or else by its role.
type compileRouting struct {
	// name is the key under roles and routes, which is also the role name;
	// task is the template task it routes.
	name  string
	task  string
	route *compileRoute
	role  *compileRole
}

func compileTemplateRouting(header compileFrontMatter) []compileRouting {
	return []compileRouting{
		{name: "execute", task: "implement", route: header.Routes.Execute, role: header.Roles.Execute},
		{name: "review", task: "review", route: header.Routes.Review, role: header.Roles.Review},
	}
}

// compiled is the task's routes, or its role and role options.
func (r compileRouting) compiled() ([]compiledRoute, string, map[string]string) {
	if r.route != nil {
		route := compiledRoute{Instance: r.route.Instance, Model: r.route.Model, QuotaPool: r.route.QuotaPool}
		if r.route.Effort != "" {
			route.Options = map[string]string{"effort": r.route.Effort}
		}
		return []compiledRoute{route}, "", nil
	}
	var options map[string]string
	if r.role != nil && r.role.Effort != "" {
		options = map[string]string{"effort": r.role.Effort}
	}
	return nil, r.name, options
}

// checkCompileRouting refuses a template task that both roles and routes
// route, a pinned route without an instance or model, and a role effort the
// workflow format refuses. A plan that declares routes at all must pin every
// task that roles does not route, which keeps the compile v1 refusal of a
// forgotten route.
func checkCompileRouting(header compileFrontMatter, root *yaml.Node, lineOf func(*yaml.Node, ...string) int, refuse func(int, string, ...any) error) error {
	routesDeclared := mappingKey(root, "routes") != nil
	for _, routing := range compileTemplateRouting(header) {
		switch {
		case routing.route != nil && routing.role != nil:
			return refuse(lineOf(root, "roles", routing.name),
				"roles.%s and routes.%s both route the %s task; keep roles.%s to route it by role through the coordinator's route policy, or routes.%s to pin one route, not both",
				routing.name, routing.name, routing.task, routing.name, routing.name)
		case routing.route != nil:
			line := lineOf(root, "routes", routing.name)
			if strings.TrimSpace(routing.route.Instance) == "" {
				return refuse(line, "routes.%s needs an instance", routing.name)
			}
			if strings.TrimSpace(routing.route.Model) == "" {
				return refuse(line, "routes.%s needs a model", routing.name)
			}
		case routesDeclared && routing.role == nil:
			return refuse(lineOf(root, "routes"),
				"routes.%s is required when routes is declared, unless roles.%s routes the %s task by role: {instance, model, quota_pool, effort}",
				routing.name, routing.name, routing.task)
		case routing.role != nil && routing.role.Effort != "" && !backlog.ValidPolicyEffort(routing.role.Effort):
			return refuse(lineOf(root, "roles", routing.name), "roles.%s effort %q is not low, medium or high", routing.name, routing.role.Effort)
		}
	}
	return nil
}

// readCompileInputs checks and reads the extra inputs the front matter names.
// Each is bundled into every unit at inputs/<path> and mounted at
// .t3/inputs/inputs/<path>.
func readCompileInputs(paths []string, node *yaml.Node, fallbackLine int, options CompileOptions, refuse func(int, string, ...any) error) ([]CompiledFile, error) {
	seen := map[string]bool{}
	var files []CompiledFile
	for index, name := range paths {
		line := fallbackLine
		if node != nil && index < len(node.Content) {
			line = node.Content[index].Line
		}
		switch {
		case strings.TrimSpace(name) == "":
			return nil, refuse(line, "input %d is empty", index+1)
		case strings.ContainsAny(name, "\\*?[\x00"):
			return nil, refuse(line, "input %q is not a plain slash-separated path: glob characters and backslashes are refused", name)
		case path.IsAbs(name) || path.Clean(name) != name || !filepath.IsLocal(filepath.FromSlash(name)):
			return nil, refuse(line, "input %q is not a clean relative path inside the plan's directory", name)
		case "inputs/"+name == compiledPlanInput || "inputs/"+name == compiledUnitInput:
			return nil, refuse(line, "input %q would replace inputs/%s, which compile writes for every unit; rename it or move it into a directory", name, name)
		case seen[name]:
			return nil, refuse(line, "input %q is named twice", name)
		case options.ReadInput == nil:
			return nil, refuse(line, "input %q cannot be read: the plan was given without the directory its inputs are relative to", name)
		}
		seen[name] = true
		content, err := options.ReadInput(name)
		if err != nil {
			return nil, refuse(line, "input %q: %v", name, err)
		}
		files = append(files, CompiledFile{Path: "inputs/" + name, Content: content})
	}
	return files, nil
}

func newCompiledPlacement(placement *backlog.ManifestPlacement) *compiledPlacement {
	if placement == nil || (len(placement.Hosts) == 0 && len(placement.Requires) == 0) {
		return nil
	}
	return &compiledPlacement{
		Hosts:    append([]string(nil), placement.Hosts...),
		Requires: append([]string(nil), placement.Requires...),
	}
}

func newCompiledLedger(ledger *backlog.ManifestLedger) *compiledLedger {
	if ledger == nil {
		return nil
	}
	return &compiledLedger{
		JocastaProject: ledger.JocastaProject, Plan: ledger.Plan, Risk: ledger.Risk,
		Acceptance: append([]string(nil), ledger.Acceptance...),
	}
}

func newCompiledResources(resources *backlog.ManifestResources) *compiledResources {
	if resources == nil {
		return nil
	}
	converted := compiledResources{
		Preset: resources.Preset, MinCPUClass: resources.MinCPUClass, PreferredCPUClass: resources.PreferredCPUClass,
		CPUUnits: resources.CPUUnits, MemoryMB: resources.MemoryMB, ScratchMB: resources.ScratchMB,
	}
	if converted == (compiledResources{}) {
		return nil
	}
	return &converted
}

// marshalCompiledWorkflow writes a workflow with two-space indentation, the
// way workflow.yaml files are written by hand.
func marshalCompiledWorkflow(workflow compiledWorkflow) ([]byte, error) {
	var node yaml.Node
	if err := node.Encode(workflow); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	encoder := yaml.NewEncoder(&out)
	encoder.SetIndent(2)
	if err := encoder.Encode(&node); err != nil {
		return nil, err
	}
	if err := encoder.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
