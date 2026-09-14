package campaign

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
)

// exampleRoot is the checked-in campaign directory an operator or an agent is
// pointed at by the help text.
func exampleRoot(name string) string {
	return filepath.Join("..", "..", "docs", "examples", "campaign", name)
}

// loadExample validates a checked-in example the way ingestion will: the real
// parser, the real path rules, the real files on disk. A broken example fails
// the build here rather than an operator's first submission.
func loadExample(t *testing.T, name string) Plan {
	t.Helper()
	manifest, err := backlog.LoadManifest(exampleRoot(name))
	if err != nil {
		t.Fatalf("load example %s: %v", name, err)
	}
	plan, err := Project(manifest, Options{Source: exampleRoot(name)})
	if err != nil {
		t.Fatalf("project example %s: %v", name, err)
	}
	return plan
}

func TestSingleLeadExampleProjects(t *testing.T) {
	plan := loadExample(t, "single-lead")

	if plan.Name != "single-lead-example" {
		t.Fatalf("name = %q", plan.Name)
	}
	if !reflect.DeepEqual(plan.Waves, []Wave{{Index: 0, Tasks: []string{"implement"}}}) {
		t.Fatalf("waves = %#v", plan.Waves)
	}
	if !reflect.DeepEqual(plan.Roots, []string{"implement"}) || !reflect.DeepEqual(plan.Leaves, []string{"implement"}) {
		t.Fatalf("the single task is both the root and the leaf: %v / %v", plan.Roots, plan.Leaves)
	}
	implement := taskByName(t, plan, "implement")
	// The plan's single-lead project requires a verified commit, a test receipt
	// and a concise handoff, and each one has to be a declared output or it is
	// not retained.
	if !reflect.DeepEqual(implement.Outputs, []string{"commit.txt", "verification.txt", "handoff.md"}) {
		t.Fatalf("outputs = %v", implement.Outputs)
	}
	for _, output := range implement.Outputs {
		if !strings.Contains(strings.Join(implement.Verify, "\n"), output) {
			t.Fatalf("output %q is declared but never verified", output)
		}
	}
	if implement.PreflightFrom != OriginInherited || len(implement.Preflight) != 2 {
		t.Fatalf("preflight = %q %#v", implement.PreflightFrom, implement.Preflight)
	}
}

func TestThreeNodeExampleProjects(t *testing.T) {
	plan := loadExample(t, "three-node")

	wantWaves := []Wave{
		{Index: 0, Tasks: []string{"interfaces", "tests"}},
		{Index: 1, Tasks: []string{"join"}},
	}
	if !reflect.DeepEqual(plan.Waves, wantWaves) {
		t.Fatalf("waves = %#v", plan.Waves)
	}
	if !reflect.DeepEqual(plan.Roots, []string{"interfaces", "tests"}) {
		t.Fatalf("roots = %v", plan.Roots)
	}
	if !reflect.DeepEqual(plan.Leaves, []string{"join"}) {
		t.Fatalf("leaves = %v", plan.Leaves)
	}
	join := taskByName(t, plan, "join")
	wantBindings := []Binding{
		{Producer: "interfaces", Artifacts: []string{"interfaces.md"}},
		{Producer: "tests", Artifacts: []string{"tests.md"}},
	}
	if !reflect.DeepEqual(join.InputsFrom, wantBindings) {
		t.Fatalf("join bindings = %#v", join.InputsFrom)
	}
	// A verification command must not name a dependency mount path. The directory
	// is named for the producing task's ID, assigned at ingestion, so any path an
	// author writes in advance is a guess that fails at run time.
	for _, command := range join.Verify {
		if strings.Contains(command, DependencyMountPrefix) {
			t.Fatalf("verification %q names a dependency path the author cannot know", command)
		}
	}
	if join.ResourcesFrom != OriginTask || join.Resources.Preset != backlog.ResourcePresetBuild {
		t.Fatalf("join resources = %q %#v", join.ResourcesFrom, join.Resources)
	}
	if interfaces := taskByName(t, plan, "interfaces"); interfaces.ResourcesFrom != OriginInherited {
		t.Fatalf("interfaces resources = %q", interfaces.ResourcesFrom)
	}
}

func TestExamplesMarkOperatorConfiguration(t *testing.T) {
	// Provider instance, model, quota pool and project are one fleet's
	// configuration. An example that presented them as defaults would be copied
	// verbatim and would fail on submission somewhere else.
	for _, name := range []string{"single-lead", "three-node"} {
		manifestPath := filepath.Join(exampleRoot(name), "workflow.yaml")
		content, err := os.ReadFile(manifestPath)
		if err != nil {
			t.Fatalf("read %s: %v", manifestPath, err)
		}
		raw := string(content)
		if !strings.Contains(raw, "OPERATOR CONFIGURATION, NOT PORTABLE DEFAULTS") {
			t.Fatalf("%s does not warn that its provider and project names are local", manifestPath)
		}
		for _, line := range strings.Split(raw, "\n") {
			for _, field := range []string{"project:", "instance:", "model:", "quota_pool:"} {
				if strings.HasPrefix(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "- ")), field) &&
					!strings.Contains(line, "# operator configuration") {
					t.Fatalf("%s: %q is operator configuration and is not marked as such", manifestPath, line)
				}
			}
		}
	}
}

func TestExamplesRenderInEveryFormat(t *testing.T) {
	for _, name := range []string{"single-lead", "three-node"} {
		t.Run(name, func(t *testing.T) {
			plan := loadExample(t, name)
			if text := RenderText(plan); !strings.Contains(text, "static plan only") {
				t.Fatalf("text = %s", text)
			}
			encoded, err := RenderJSON(plan)
			if err != nil {
				t.Fatalf("render JSON: %v", err)
			}
			var decoded Plan
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !reflect.DeepEqual(decoded, plan) {
				t.Fatal("example plan does not round trip")
			}
			if dot := RenderDOT(plan); !strings.HasSuffix(dot, "}\n") || !strings.Contains(dot, "digraph") {
				t.Fatalf("DOT = %s", dot)
			}
		})
	}
}
