package campaign

import (
	"os"
	"strings"
	"testing"
)

const planningRef = "d2e827681336e76413fdab34392f029a3e131cf4"
const planningRoutes = "routes:\n  execute: {instance: codex, model: gpt-6.1-sol, effort: high}\n  review: {instance: codex, model: gpt-6.1-sol, effort: high}\n"
const planningFixture = "---\ncompile: v1\nproject: p\ntemplate: plan-review\nref: " + planningRef + "\n" + planningRoutes +
	"ledger: {jocasta_project: p, risk: medium}\nplacement: {implement: {hosts: [worker]}, review: {hosts: [worker]}}\n" +
	"resources: {implement: {preset: light}, review: {preset: review}}\nunits: [{id: a, section: A, outputs: [plan.md, risks.md]}]\n---\n## A\nWrite a plan.\n"

func TestCompilePlanningGoldens(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		raw := planningFixture
		if fresh {
			raw = strings.Replace(raw, "ref: "+planningRef, "type: fresh", 1)
		}
		plan, err := ParseCompilePlan("plan.md", []byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		unit := plan.Units[0]
		workflow := compiledFile(t, unit, ManifestFileName)
		if *updateCompileGoldens && !fresh {
			if err := os.WriteFile("testdata/compile/planning/git.yaml", []byte(workflow), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		want, err := os.ReadFile("testdata/compile/planning/git.yaml")
		if err != nil {
			t.Fatal(err)
		}
		if fresh {
			env, err := os.ReadFile("testdata/compile/planning/fresh-environment.yaml")
			if err != nil {
				t.Fatal(err)
			}
			want = []byte(strings.Replace(string(want), "environment:\n  project: p\n  ref: "+planningRef+"\n", string(env), 1))
			want = []byte(strings.ReplaceAll(string(want), strings.ReplaceAll(compileReportCheck(false), "'", "''"), strings.ReplaceAll(compileReportCheck(true), "'", "''")))
			want = []byte(strings.ReplaceAll(string(want), "      - "+compileCleanSourceCheck+"\n", ""))
		}
		if workflow != string(want) {
			t.Fatalf("fresh=%v workflow:\n%s", fresh, workflow)
		}
		m := compiledManifest(t, unit)
		for _, task := range m.Tasks {
			if len(task.Commits) != 0 {
				t.Fatal("planning declared commits")
			}
		}
		for _, name := range []string{"plan", "plan-review"} {
			prompt := compiledFile(t, unit, "prompts/"+name+".md")
			if name == "plan-review" {
				golden := "testdata/compile/planning/git-review.md"
				if fresh {
					golden = "testdata/compile/planning/fresh-review.md"
				}
				if *updateCompileGoldens {
					if err := os.WriteFile(golden, []byte(prompt), 0o644); err != nil {
						t.Fatal(err)
					}
				}
				want, err := os.ReadFile(golden)
				if err != nil || string(want) != prompt {
					t.Fatalf("%s differs: %v", golden, err)
				}
			}
			for _, text := range []string{"plan.md", "risks.md", "artifacts/handoff.md", ".t3/dependencies/", "Do not change source"} {
				if !strings.Contains(prompt, text) {
					t.Fatalf("%s lacks %q", name, text)
				}
			}
			if strings.Contains(prompt, "implementation commit") {
				t.Fatal("planning asks for an implementation commit")
			}
		}
		if !strings.Contains(compiledFile(t, unit, "prompts/plan-review.md"), "VERDICT: ACCEPT") ||
			m.Tasks["review"].ReviewOutput.VerdictLine != "artifacts/review.md" {
			t.Fatal("missing verdict contract")
		}
		for _, roles := range []string{"", "roles: {execute: {effort: medium}, review: {effort: high}}\n"} {
			rolePlan, err := ParseCompilePlan("plan.md", []byte(strings.Replace(raw, planningRoutes, roles, 1)))
			if err != nil {
				t.Fatal(err)
			}
			rm := compiledManifest(t, rolePlan.Units[0])
			p, r := rm.Tasks["plan"], rm.Tasks["review"]
			effort := "high"
			if roles != "" {
				effort = "medium"
			}
			if p.Role != "execute" || p.Options["effort"] != effort || r.Role != "review" || r.Options["effort"] != "high" ||
				rm.Ledger.Risk != m.Ledger.Risk || p.Resources.Preset != m.Tasks["plan"].Resources.Preset ||
				strings.Join(p.Placement.Hosts, ",") != "worker" {
				t.Fatal("role routing lost effort or metadata")
			}
		}
	}
}

func TestCompilePlanningRefusals(t *testing.T) {
	for _, outputs := range []string{"", "[]", "[\"\"]", "[plan.md, plan.md]", "[../x]", "[/x]", "[./x]", "[a//b]", "[a/../b]", "[x*]", "[\"x\\ny\"]",
		"[artifacts/continuation.md]", "[artifacts/handoff.md]", "[review.md]", "[artifacts]", "[artifacts/handoff.md/x]", "[" + strings.Repeat("x", 256) + "]"} {
		raw := strings.Replace(planningFixture, "outputs: [plan.md, risks.md]", "outputs: "+outputs, 1)
		if outputs == "" {
			raw = strings.Replace(raw, ", outputs: ", "", 1)
		}
		if p, err := ParseCompilePlan("plan.md", []byte(raw)); err == nil || len(p.Units) != 0 {
			t.Fatalf("accepted outputs %q", outputs)
		}
	}
	for _, replacement := range []string{"type: fresh\nref: " + planningRef, "type: fresh\nref: \"\"", "type: fresh\nref: null", "type: other", "", "ref: main"} {
		raw := strings.Replace(planningFixture, "ref: "+planningRef, replacement, 1)
		if _, err := ParseCompilePlan("plan.md", []byte(raw)); err == nil {
			t.Fatalf("accepted %q", replacement)
		}
	}
	conflict := strings.Replace(planningFixture, planningRoutes, planningRoutes+"roles: {execute: {effort: high}}\n", 1)
	if _, err := ParseCompilePlan("plan.md", []byte(conflict)); err == nil {
		t.Fatal("accepted role/route conflict")
	}
}
