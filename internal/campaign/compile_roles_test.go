package campaign

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"gopkg.in/yaml.v3"
)

const compileRolesPlanFixture = "testdata/compile/roles/plan.md"

// compileFixtureReader reads extra inputs relative to a fixture plan, the way
// the CLI reads them relative to the plan it is given.
func compileFixtureReader(planPath string) CompileOptions {
	dir := filepath.Dir(planPath)
	return CompileOptions{ReadInput: func(name string) ([]byte, error) {
		return os.ReadFile(filepath.Join(dir, filepath.FromSlash(name)))
	}}
}

func parseCompileFixture(t *testing.T, planPath string) CompilePlan {
	t.Helper()
	raw, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := ParseCompilePlanWith("plan.md", raw, compileFixtureReader(planPath))
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func compiledManifest(t *testing.T, unit CompiledUnit) backlog.Manifest {
	t.Helper()
	manifest, err := backlog.ParseManifest([]byte(compiledFile(t, unit, ManifestFileName)))
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

// The workflow a compile v1 plan compiled to before the minimal emission,
// every default field written out, is kept under testdata/compile/legacy. The
// minimal workflow the same plan compiles to now must mean exactly the same
// thing once the workflow format has applied its defaults. Both are compared
// as the workflow format writes them back, where an absent list and an empty
// one are the same thing, as they are to the coordinator.
func TestCompiledV1PlanIsEquivalentToTheLegacyWorkflow(t *testing.T) {
	plan := parseCompileFixture(t, compilePlanFixture)
	for _, unit := range plan.Units {
		legacyRaw, err := os.ReadFile(filepath.Join("testdata", "compile", "legacy", unit.ID+".workflow.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		legacy, err := backlog.ParseManifest(legacyRaw)
		if err != nil {
			t.Fatal(err)
		}
		got, err := yaml.Marshal(compiledManifest(t, unit))
		if err != nil {
			t.Fatal(err)
		}
		want, err := yaml.Marshal(legacy)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("%s: the minimal workflow is not equivalent to the legacy one:\ngot:\n%s\nwant:\n%s", unit.ID, got, want)
		}
	}
}

// The emitted workflow writes no default: no empty value, no null, and none
// of the fields the plan never said.
func TestCompiledWorkflowIsMinimal(t *testing.T) {
	for _, fixture := range []string{compilePlanFixture, compileRolesPlanFixture} {
		for _, unit := range parseCompileFixture(t, fixture).Units {
			workflow := compiledFile(t, unit, ManifestFileName)
			for _, unwanted := range []string{"null", `""`, "[]", "{}", "importance", "difficulty", "preflight", "scope:", "type:", "host:"} {
				if strings.Contains(workflow, unwanted) {
					t.Fatalf("%s %s: workflow.yaml writes %q:\n%s", fixture, unit.ID, unwanted, workflow)
				}
			}
			if lines := strings.Count(workflow, "\n"); lines > 50 {
				t.Fatalf("%s %s: workflow.yaml is %d lines:\n%s", fixture, unit.ID, lines, workflow)
			}
			if implement, review := strings.Index(workflow, "  implement:"), strings.Index(workflow, "  review:"); implement < 0 || review < implement {
				t.Fatalf("%s %s: the tasks are not written in the order they run:\n%s", fixture, unit.ID, workflow)
			}
		}
	}
}

func TestCompiledRolePlanCarriesEveryDeclaredField(t *testing.T) {
	plan := parseCompileFixture(t, compileRolesPlanFixture)
	unit := plan.Units[0]
	manifest := compiledManifest(t, unit)
	implement, review := manifest.Tasks["implement"], manifest.Tasks["review"]
	if implement.Role != "execute" || implement.Options["effort"] != "medium" || len(implement.Routes) != 0 {
		t.Fatalf("implement = role %q options %v routes %v", implement.Role, implement.Options, implement.Routes)
	}
	if review.Role != "review" || review.Options["effort"] != "high" || len(review.Routes) != 0 || review.ReviewOutput == nil {
		t.Fatalf("review = role %q options %v routes %v review_output %v", review.Role, review.Options, review.Routes, review.ReviewOutput)
	}
	if manifest.Ledger == nil || manifest.Ledger.JocastaProject != "t3-steward" || manifest.Ledger.Risk != "medium" ||
		manifest.Ledger.Plan != "jocasta:31a0e0feedfe0d77f39ef05be31085e5@4" || len(manifest.Ledger.Acceptance) != 1 {
		t.Fatalf("ledger = %+v", manifest.Ledger)
	}
	if strings.Join(manifest.Placement.Requires, ",") != "go" || strings.Join(implement.Placement.Requires, ",") != "go" {
		t.Fatalf("placement = %+v, implement placement = %+v", manifest.Placement, implement.Placement)
	}
	if implement.Resources.Preset != "build" || review.Resources.Preset != "light" {
		t.Fatalf("resources = %+v / %+v", implement.Resources, review.Resources)
	}
	if implement.MaxTurns != 5 || review.MaxTurns != 5 {
		t.Fatalf("max_turns = %d / %d", implement.MaxTurns, review.MaxTurns)
	}
	if strings.Join(manifest.Inputs, ",") != "inputs/plan.md,inputs/unit.md,inputs/design/notes.md" {
		t.Fatalf("inputs = %v", manifest.Inputs)
	}
	notes, err := os.ReadFile("testdata/compile/roles/design/notes.md")
	if err != nil {
		t.Fatal(err)
	}
	if got := compiledFile(t, unit, "inputs/design/notes.md"); got != string(notes) {
		t.Fatalf("inputs/design/notes.md is not the input byte for byte: %q", got)
	}
	for _, prompt := range []string{"prompts/implement.md", "prompts/review.md"} {
		if !strings.Contains(compiledFile(t, unit, prompt), "`.t3/inputs/inputs/design/notes.md`") {
			t.Fatalf("%s does not name the extra input:\n%s", prompt, compiledFile(t, unit, prompt))
		}
	}
}

const compileRoutingHead = "---\ncompile: v1\nproject: p\nref: d2e827681336e76413fdab34392f029a3e131cf4\n"
const compileRoutingUnits = "units:\n  - {id: a, section: A}\n---\n## A\n\ntext\n"

// Omitting both roles and routes routes both tasks by role with the policy's
// own effort, and a plan may pin one task while the other is routed by role.
func TestCompilePlanRoutingDefaultsAndMixes(t *testing.T) {
	for _, test := range []struct {
		name                 string
		front                string
		implement, review    string
		implementEffort      string
		reviewRoute          string
		reviewEffortOnRoute  string
		wantRoleOptionsEmpty bool
	}{
		{name: "neither", front: "", implement: "execute", review: "review", wantRoleOptionsEmpty: true},
		{name: "roles without effort", front: "roles:\n  execute: {}\n", implement: "execute", review: "review", wantRoleOptionsEmpty: true},
		{name: "role and a pin", front: "roles:\n  execute: {effort: low}\nroutes:\n  review: {instance: codex, model: m, quota_pool: q, effort: high}\n",
			implement: "execute", implementEffort: "low", reviewRoute: "codex", reviewEffortOnRoute: "high"},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan, err := ParseCompilePlan("plan.md", []byte(compileRoutingHead+test.front+compileRoutingUnits))
			if err != nil {
				t.Fatal(err)
			}
			manifest := compiledManifest(t, plan.Units[0])
			implement, review := manifest.Tasks["implement"], manifest.Tasks["review"]
			if implement.Role != test.implement || implement.Options["effort"] != test.implementEffort || len(implement.Routes) != 0 {
				t.Fatalf("implement = role %q options %v routes %v", implement.Role, implement.Options, implement.Routes)
			}
			if test.wantRoleOptionsEmpty && (implement.Options != nil || review.Options != nil) {
				t.Fatalf("role options = %v / %v, want none", implement.Options, review.Options)
			}
			if test.reviewRoute == "" {
				if review.Role != test.review || len(review.Routes) != 0 {
					t.Fatalf("review = role %q routes %v", review.Role, review.Routes)
				}
				return
			}
			if review.Role != "" || len(review.Routes) != 1 || review.Routes[0].Instance != test.reviewRoute || review.Routes[0].Options["effort"] != test.reviewEffortOnRoute {
				t.Fatalf("review = role %q routes %+v, want the pinned route", review.Role, review.Routes)
			}
		})
	}
}

// A conflicting or ill-formed roles, max_turns, resources or inputs key is
// refused at its plan line before anything is rendered.
func TestCompilePlanRoleAndFieldRefusals(t *testing.T) {
	const route = "{instance: claudeAgent, model: m, quota_pool: q}"
	working := func(name string) ([]byte, error) { return []byte("x"), nil }
	for _, test := range []struct {
		name   string
		front  string
		reader func(string) ([]byte, error)
		want   []string
	}{
		{"roles and routes on execute", "roles:\n  execute: {effort: medium}\nroutes:\n  execute: " + route + "\n  review: " + route + "\n", nil,
			[]string{"plan.md:6:", "roles.execute and routes.execute both route the implement task", "not both"}},
		{"roles and routes on review", "roles:\n  review: {}\nroutes:\n  execute: " + route + "\n  review: " + route + "\n", nil,
			[]string{"plan.md:6:", "roles.review and routes.review both route the review task"}},
		{"routes missing a task roles does not route", "routes:\n  execute: " + route + "\n", nil,
			[]string{"plan.md:5:", "routes.review is required when routes is declared, unless roles.review"}},
		{"role effort max", "roles:\n  execute: {effort: max}\n", nil, []string{"plan.md:6:", `roles.execute effort "max"`}},
		{"unknown role", "roles:\n  plan: {effort: high}\n", nil, []string{"plan.md:6:", "plan", "in roles"}},
		{"model on a role", "roles:\n  execute: {model: m}\n", nil, []string{"plan.md:6:", "model", "in a role"}},
		{"role not a mapping", "roles:\n  execute: medium\n", nil, []string{"plan.md:6:", "a role, which must be a mapping"}},
		{"zero max_turns", "max_turns: 0\n", nil, []string{"plan.md:5:", "max_turns 0"}},
		{"max_turns mapping", "max_turns: {implement: 4}\n", nil, []string{"plan.md:5:"}},
		{"resources for an unknown task", "resources:\n  fix: {preset: build}\n", nil, []string{"plan.md:6:", "fix", "implement and review"}},
		{"unknown resources field", "resources:\n  implement: {gpus: 1}\n", nil, []string{"plan.md:6:", "gpus", "in a task's resources"}},
		{"unknown placement field", "placement:\n  zone: eu\n", nil, []string{"plan.md:6:", "zone", "in placement"}},
		{"unknown ledger field", "ledger:\n  jocasta_project: p\n  owner: me\n", nil, []string{"plan.md:7:", "owner", "in ledger"}},
		{"ledger the workflow refuses", "ledger:\n  jocasta_project: p\n  risk: extreme\n", nil, []string{"plan.md:", "ledger.risk", "compiles to a workflow this release refuses"}},
		{"absolute input", "inputs: [/etc/passwd]\n", working, []string{"plan.md:5:", `"/etc/passwd"`, "inside the plan's directory"}},
		{"escaping input", "inputs: [../secret.md]\n", working, []string{"plan.md:5:", "inside the plan's directory"}},
		{"unclean input", "inputs: [docs/./a.md]\n", working, []string{"plan.md:5:", "clean relative path"}},
		{"glob input", "inputs: [\"docs/*.md\"]\n", working, []string{"plan.md:5:", "glob characters"}},
		{"input shadowing the plan", "inputs: [plan.md]\n", working, []string{"plan.md:5:", "would replace inputs/plan.md"}},
		{"input shadowing the unit", "inputs:\n  - a.md\n  - unit.md\n", working, []string{"plan.md:7:", "would replace inputs/unit.md"}},
		{"duplicate input", "inputs:\n  - a.md\n  - a.md\n", working, []string{"plan.md:7:", "named twice"}},
		{"empty input", "inputs: [\"\"]\n", working, []string{"plan.md:5:", "input 1 is empty"}},
		{"input without a reader", "inputs: [a.md]\n", nil, []string{"plan.md:5:", "cannot be read"}},
		{"unreadable input", "inputs: [a.md]\n", func(string) ([]byte, error) { return nil, errors.New("no such file") }, []string{"plan.md:5:", `"a.md": no such file`}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseCompilePlanWith("plan.md", []byte(compileRoutingHead+test.front+compileRoutingUnits), CompileOptions{ReadInput: test.reader})
			var refusal *CompilePlanError
			if !errors.As(err, &refusal) {
				t.Fatalf("err = %T %v, want a CompilePlanError", err, err)
			}
			for _, want := range test.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("refusal %q does not contain %q", err, want)
				}
			}
			if strings.Contains(err.Error(), "campaign.compile") || strings.Contains(err.Error(), "backlog.Manifest") {
				t.Fatalf("refusal %q names a Go type the author never wrote", err)
			}
		})
	}
}
