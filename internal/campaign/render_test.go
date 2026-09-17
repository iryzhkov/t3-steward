package campaign

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestRenderJSONRoundTrips(t *testing.T) {
	sources := map[string]string{
		"join":        joinYAML,
		"inheritance": inheritanceYAML,
		"empty":       headerYAML + "tasks: {}\n",
		"timing": headerYAML + `tasks:
  early:
    prompt_file: prompts/early.md
    not_before: 2026-09-20T11:00:00+02:00
    deadline: 2026-09-21T09:00:00Z
    expires_at: 2026-09-22T09:00:00Z
    estimated_cost: 1.25
`,
	}
	for name, source := range sources {
		t.Run(name, func(t *testing.T) {
			plan := project(t, source)
			encoded, err := RenderJSON(plan)
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			var decoded Plan
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !reflect.DeepEqual(decoded, plan) {
				t.Fatalf("round trip lost information:\n%#v\n%#v", decoded, plan)
			}
			reencoded, err := RenderJSON(decoded)
			if err != nil {
				t.Fatalf("re-render: %v", err)
			}
			if string(reencoded) != string(encoded) {
				t.Fatalf("re-render differs:\n%s\n%s", encoded, reencoded)
			}
			if decoded.SchemaVersion != PlanSchemaVersion {
				t.Fatalf("schema version = %d", decoded.SchemaVersion)
			}
		})
	}
}

func TestRenderJSONNamesTheSchemaVersionFirst(t *testing.T) {
	// An agent that parses the document reads schemaVersion before it trusts
	// anything else, so the field is present and stable from the first release.
	encoded, err := RenderJSON(project(t, joinYAML))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.HasPrefix(string(encoded), "{\n  \"schemaVersion\": 2,\n") {
		t.Fatalf("document starts with %.40q", encoded)
	}
	if !strings.HasSuffix(string(encoded), "}\n") {
		t.Fatal("document must end with a newline")
	}
}

func TestRenderTextReportsTheGraphAndItsLimits(t *testing.T) {
	manifest := parse(t, joinYAML)
	plan, err := Project(manifest, Options{Source: "./campaign", Digest: "sha256:abc"})
	if err != nil {
		t.Fatalf("project: %v", err)
	}
	text := RenderText(plan)

	for _, want := range []string{
		"campaign example",
		"source       ./campaign",
		"digest       sha256:abc",
		"3 tasks, 2 edges, 2 waves, 2 roots, 1 leaf",
		"wave 0 (2 tasks may start together)",
		"wave 1 (1 task)",
		"alpha [root]",
		"join [leaf]",
		// The binding names the producer and its artifacts. It must not name a
		// mount path: that directory is the producing task's ID, assigned at
		// ingestion, so the projection cannot know it.
		"alpha: alpha.md, alpha.txt",
		"unblocks    join",
		"waits for every task in the run, not only the leaves",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("text is missing %q:\n%s", want, text)
		}
	}
	// The plan is read before the work exists. It must never let a reader
	// believe a worker, a route or quota has been reserved.
	for _, want := range []string{"static plan only", "campaign explain"} {
		if !strings.Contains(text, want) {
			t.Fatalf("text is missing the static-versus-dynamic warning %q:\n%s", want, text)
		}
	}
}

func TestRenderTextNamesInheritedSettingsWithoutRepeatingThem(t *testing.T) {
	text := RenderText(project(t, inheritanceYAML))

	for _, want := range []string{
		"class       required (inherited)",
		"routes      inherited, 1 candidate(s)",
		"preflight   inherited, 1 step(s)",
		"class       surplus (task)",
		"hosts alpha; requires internet (task)",
		"taskInstance/task-model (task)",
		"tests: check go test ./..., on failure record, include summary (task)",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("text is missing %q:\n%s", want, text)
		}
	}
}

func TestRenderDOTIsAValidGraph(t *testing.T) {
	dot := RenderDOT(project(t, joinYAML))

	for _, want := range []string{
		"digraph \"example\" {",
		"  \"alpha\";",
		"  { rank=same; \"alpha\"; \"beta\"; }",
		"  \"alpha\" -> \"join\" [label=\"alpha.md, alpha.txt\"];",
		"  \"beta\" -> \"join\" [label=\"beta.md\"];",
		"  \"__sink\" [shape=ellipse, style=dashed",
		"  \"join\" -> \"__sink\" [style=dashed];",
	} {
		if !strings.Contains(dot, want) {
			t.Fatalf("DOT is missing %q:\n%s", want, dot)
		}
	}
	if strings.Count(dot, "{") != strings.Count(dot, "}") {
		t.Fatalf("unbalanced braces:\n%s", dot)
	}
	if !strings.HasSuffix(dot, "}\n") {
		t.Fatalf("DOT must close its graph:\n%s", dot)
	}
}

func TestRenderDOTDrawsACrossRunDependencyDashed(t *testing.T) {
	dot := RenderDOT(project(t, headerYAML+`tasks:
  waits:
    prompt_file: prompts/waits.md
    needs: [run-7/build]
`))

	if !strings.Contains(dot, "\"run-7/build\" [shape=box, style=dashed];") {
		t.Fatalf("cross-run node is not marked:\n%s", dot)
	}
	if !strings.Contains(dot, "\"run-7/build\" -> \"waits\" [style=dashed];") {
		t.Fatalf("cross-run edge is not marked:\n%s", dot)
	}
}

func TestHelpTopicsAreNonEmptyAndDistinct(t *testing.T) {
	topics := HelpTopics()
	if len(topics) == 0 {
		t.Fatal("no help topics")
	}
	seen := map[string]bool{}
	for _, topic := range topics {
		if topic.Name == "" || strings.TrimSpace(topic.Body) == "" {
			t.Fatalf("topic %#v is incomplete", topic)
		}
		if seen[topic.Name] {
			t.Fatalf("topic %q is declared twice", topic.Name)
		}
		seen[topic.Name] = true
	}
	// The help has to carry the facts an author cannot guess from the schema.
	for _, want := range []string{
		"needs", "inputs_from", "outputs", "verify",
		// Both mounts, spelled the way a prompt has to spell them: the static
		// inputs at their declared path, and dependency artifacts under a
		// producer task id that is assigned at ingestion and must be listed,
		// not hard-coded.
		"\ninputs\n",
		".t3/inputs/<declared path>",
		"inputs/plan.md is read at .t3/inputs/inputs/plan.md",
		".t3/dependencies/<producer task id>/<artifact>",
		"assigned at ingestion",
	} {
		if !strings.Contains(DAGSemanticsHelp, want) {
			t.Fatalf("DAG help is missing %q", want)
		}
	}
	if !strings.Contains(PlanHelp, "docs/examples/campaign/single-lead") ||
		!strings.Contains(PlanHelp, "docs/examples/campaign/three-node") {
		t.Fatal("plan help must point at the checked-in examples")
	}
	if !strings.Contains(StaticVersusDynamicHelp, "plan is static, explain is dynamic") {
		t.Fatal("the static-versus-dynamic distinction must be stated outright")
	}
}
