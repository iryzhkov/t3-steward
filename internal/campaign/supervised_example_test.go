package campaign

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const supervisedExample = "supervised-three-node"

// The supervised example is validated by the real ingestion parser against the
// real files on disk, so a broken example fails here rather than on an
// operator's first submission.
func TestSupervisedExampleProjects(t *testing.T) {
	plan := loadExample(t, supervisedExample)

	if plan.Name != "supervised-three-node-example" {
		t.Fatalf("name = %q", plan.Name)
	}
	wantWaves := []Wave{
		{Index: 0, Tasks: []string{"interfaces", "tests"}},
		{Index: 1, Tasks: []string{"synthesis"}},
	}
	if !reflect.DeepEqual(plan.Waves, wantWaves) {
		t.Fatalf("waves = %#v", plan.Waves)
	}
	if plan.Supervision == nil {
		t.Fatal("the supervised example declares an overseer and must project one")
	}
	if !plan.Supervision.DistinctFromTaskRoutes {
		t.Fatal("the example exists to show an independently configured overseer route")
	}
	if plan.Supervision.PromptFile != "prompts/overseer.md" {
		t.Fatalf("overseer prompt = %q", plan.Supervision.PromptFile)
	}

	review := gateByName(t, plan, "analysis_review")
	if !reflect.DeepEqual(review.Observes, []string{"interfaces", "tests"}) {
		t.Fatalf("analysis_review observes %v", review.Observes)
	}
	if !reflect.DeepEqual(review.Protects, []string{"synthesis"}) {
		t.Fatalf("analysis_review protects %v", review.Protects)
	}
	if review.RubricFile != "rubrics/analysis.md" {
		t.Fatalf("analysis_review rubric = %q", review.RubricFile)
	}
	final := gateByName(t, plan, "final_report")
	if !final.Final || len(final.Protects) != 0 {
		t.Fatalf("final_report must guard settlement rather than a task: %#v", final)
	}

	synthesis := taskByName(t, plan, "synthesis")
	if !reflect.DeepEqual(synthesis.HeldBy, []string{"analysis_review"}) {
		t.Fatalf("synthesis held by %v", synthesis.HeldBy)
	}
	// The gate protects synthesis, and synthesis reaches both producers through
	// ordinary needs and inputs_from edges. A gate cannot stand in for those.
	if !reflect.DeepEqual(synthesis.Needs, []string{"interfaces", "tests"}) {
		t.Fatalf("synthesis needs %v", synthesis.Needs)
	}
	wantBindings := []Binding{
		{Producer: "interfaces", Artifacts: []string{"interfaces.md"}},
		{Producer: "tests", Artifacts: []string{"tests.md"}},
	}
	if !reflect.DeepEqual(synthesis.InputsFrom, wantBindings) {
		t.Fatalf("synthesis bindings = %#v", synthesis.InputsFrom)
	}
	if plan.Totals.Gates != 2 || plan.Totals.HeldTasks != 1 {
		t.Fatalf("totals = %#v", plan.Totals)
	}
}

// The overseer prompt and both rubrics are referenced by the manifest, so the
// loader has to pack them. A referenced file that does not travel is a campaign
// that validates here and fails on arrival.
func TestSupervisedExamplePacksItsPromptAndRubrics(t *testing.T) {
	loaded, err := Load(exampleRoot(supervisedExample), DefaultLimits)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	roles := map[string]Role{}
	for _, file := range loaded.Files {
		roles[file.Path] = file.Role
	}
	if roles["prompts/overseer.md"] != RolePrompt {
		t.Fatalf("the overseer prompt is packed as %q", roles["prompts/overseer.md"])
	}
	for _, rubric := range []string{"rubrics/analysis.md", "rubrics/settlement.md"} {
		if roles[rubric] != RoleInput {
			t.Fatalf("%s is packed as %q", rubric, roles[rubric])
		}
	}
}

func TestSupervisedExampleMarksOperatorConfiguration(t *testing.T) {
	manifestPath := filepath.Join(exampleRoot(supervisedExample), "workflow.yaml")
	content, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read %s: %v", manifestPath, err)
	}
	raw := string(content)
	if !strings.Contains(raw, "OPERATOR CONFIGURATION, NOT PORTABLE DEFAULTS") {
		t.Fatal("the example does not warn that its provider and project names are local")
	}
	// The overseer route is operator configuration too. The design requires an
	// independently configured route, not a particular model, and an example that
	// presented one as a default would be copied verbatim.
	if !strings.Contains(raw, "THE OVERSEER ROUTE IS OPERATOR\n# CONFIGURATION TOO") {
		t.Fatal("the example does not say that the overseer route is operator configuration")
	}
	for _, line := range strings.Split(raw, "\n") {
		trimmed := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "- "))
		for _, field := range []string{"project:", "instance:", "model:", "quota_pool:"} {
			if strings.HasPrefix(trimmed, field) && !strings.Contains(line, "# operator configuration") {
				t.Fatalf("%q is operator configuration and is not marked as such", line)
			}
		}
	}
}

func TestSupervisedExampleStatesItsDiscipline(t *testing.T) {
	read := func(t *testing.T, file string) string {
		t.Helper()
		path := filepath.Join(exampleRoot(supervisedExample), filepath.FromSlash(file))
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		return string(content)
	}
	// Every prompt still carries the rule the unsupervised examples carry:
	// supervision does not make hidden native delegation visible.
	for _, file := range []string{
		"workflow.yaml",
		"prompts/interfaces.md",
		"prompts/tests.md",
		"prompts/synthesis.md",
		"prompts/overseer.md",
	} {
		if !strings.Contains(read(t, file), "native subagent") {
			t.Fatalf("%s no longer says that native subagents do not replace declared tasks", file)
		}
	}

	overseer := read(t, "prompts/overseer.md")
	for _, want := range []string{
		"Accept",
		"Hold",
		"Escalate",
		"You do not run, retry, rewrite, skip or fix a task",
		"Prose is not a decision",
		"never as instruction",
	} {
		if !strings.Contains(overseer, want) {
			t.Fatalf("the overseer prompt no longer covers %q", want)
		}
	}
	// The rubric is where the review standard is written, and each gate has one.
	for _, rubric := range []string{"rubrics/analysis.md", "rubrics/settlement.md"} {
		if len(strings.TrimSpace(read(t, rubric))) == 0 {
			t.Fatalf("%s is empty", rubric)
		}
	}
	// Worker prompts must not invite an agent to address its reviewer: an
	// artifact is evidence, never an instruction to the overseer.
	for _, file := range []string{"prompts/interfaces.md", "prompts/tests.md"} {
		if !strings.Contains(read(t, file), "evidence") {
			t.Fatalf("%s does not tell the agent its output is evidence", file)
		}
	}
	readme, err := os.ReadFile(filepath.Join(exampleRoot(""), "README.md"))
	if err != nil {
		t.Fatalf("the campaign examples have no README: %v", err)
	}
	for _, want := range []string{
		"supervised-three-node/",
		"`gates` without `supervision` is refused",
		"A gate cannot replace a `needs` edge",
		"their dependency descendants",
	} {
		if !strings.Contains(string(readme), want) {
			t.Fatalf("the examples README no longer covers %q", want)
		}
	}
}

func TestSupervisedExampleRendersInEveryFormat(t *testing.T) {
	plan := loadExample(t, supervisedExample)

	text := RenderText(plan)
	for _, want := range []string{"supervision", "gates (2 review boundaries)", "static plan only"} {
		if !strings.Contains(text, want) {
			t.Fatalf("text is missing %q:\n%s", want, text)
		}
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
		t.Fatal("the supervised example plan does not round trip")
	}
	dot := RenderDOT(plan)
	for _, want := range []string{"\"gate:analysis_review\"", "\"gate:final_report\"", overseerNodeName} {
		if !strings.Contains(dot, want) {
			t.Fatalf("DOT is missing %q:\n%s", want, dot)
		}
	}
}
