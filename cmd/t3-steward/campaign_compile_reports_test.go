package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/campaign"
)

// The full canonical attachment inventory, with a supplied-size portable
// control. Exact mounted original bytes are additionally audited at execution.
func TestCampaignCompileReportClosureAndDiagnostics(t *testing.T) {
	names := strings.Fields("approved-prerequisite-brief.md gate-packet-plan.md gate-packet-report.md gate-packet-handoff.md original-plan.md original-report.md common.md t3-step22-design.md report-fix-original-unit.md report-fix-original-plan.md approved-source-brief.md original-source-planning-brief.md active-cost-owner-plan.md w13-bl-build-cache-a2.md report-path-exact-ci.json active-campaign-snapshot.json active-cost-scope.json")
	for _, fresh := range []bool{false, true} {
		for _, jsonOutput := range []bool{false, true} {
			dir := t.TempDir()
			var inputs string
			for _, name := range names {
				inputs += "  - " + name + "\n"
				if err := os.WriteFile(filepath.Join(dir, name), []byte(strings.Repeat(name+"\n", 500)), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			env := "ref: d2e827681336e76413fdab34392f029a3e131cf4"
			if fresh {
				env = "type: fresh"
			}
			raw := "---\ncompile: v1\nproject: t3-steward-github\nclass: required\ntemplate: plan-review\n" + env +
				"\nroutes:\n  execute: {instance: codex, model: gpt-6.1-sol, effort: high}\n  review: {instance: codex, model: gpt-6.1-sol, effort: high}\n" +
				"resources:\n  implement: {cpu_units: 3, memory_mb: 6000, scratch_mb: 8192, min_cpu_class: medium}\n  review: {cpu_units: 3, memory_mb: 6000, scratch_mb: 8192, min_cpu_class: medium}\n" +
				"inputs:\n" + inputs + "units: [{id: a, section: A, outputs: [notes/plan.md]}, {id: b, section: B, outputs: [notes/report.md]}]\n---\n## A\nPlan.\n## B\nPlan.\n"
			path := filepath.Join(dir, "plan.md")
			if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
				t.Fatal(err)
			}
			out := filepath.Join(t.TempDir(), "wave")
			args := []string{"compile", path, "--out", out}
			if jsonOutput {
				args = append(args, "--json")
			}
			var stdout bytes.Buffer
			if err := campaignTestCLI(t, &stdout).run(context.Background(), args); err != nil {
				t.Fatal(err)
			}
			if jsonOutput {
				dec := json.NewDecoder(&stdout)
				var report campaignCompileReport
				if err := dec.Decode(&report); err != nil || len(report.Units) != 2 {
					t.Fatal(report, err)
				}
				var extra any
				if err := dec.Decode(&extra); err != io.EOF {
					t.Fatal("multiple JSON documents", err)
				}
			} else if !strings.HasPrefix(stdout.String(), "compiled 2 units") {
				t.Fatal(stdout.String())
			}
			for _, id := range []string{"a", "b"} {
				unit := filepath.Join(out, id)
				for _, verb := range []string{"validate", "plan"} {
					var output bytes.Buffer
					if err := campaignTestCLI(t, &output).run(context.Background(), []string{verb, unit}); err != nil {
						t.Fatal(verb, err)
					}
				}
				b, err := os.ReadFile(filepath.Join(unit, campaign.ManifestFileName))
				if err != nil {
					t.Fatal(err)
				}
				m, err := backlog.ParseManifest(b)
				if err != nil {
					t.Fatal(err)
				}
				if len(m.Inputs) != len(names)+2 || m.Class != "required" || m.Environment.Project != "t3-steward-github" {
					t.Fatal(m)
				}
				for _, task := range m.Tasks {
					if task.Resources.CPUUnits == nil || *task.Resources.CPUUnits != 3 || task.Resources.MemoryMB == nil || *task.Resources.MemoryMB != 6000 || task.Resources.ScratchMB == nil || *task.Resources.ScratchMB != 8192 || task.Resources.MinCPUClass != "medium" {
						t.Fatal(task.Resources)
					}
					if task.ReviewOutput != nil && task.ReviewOutput.VerdictLine != "artifacts/review.md" {
						t.Fatal(task.ReviewOutput)
					}
					steps := task.Preflight.Steps
					if len(steps) != 1 || steps[0].FailurePolicy != "require-pass" || len(task.Commits) != 0 {
						t.Fatal(task)
					}
					for _, name := range names {
						original, _ := os.ReadFile(filepath.Join(dir, name))
						bundled, e := os.ReadFile(filepath.Join(unit, "inputs", name))
						if e != nil || !bytes.Equal(original, bundled) {
							t.Fatal(name, e)
						}
						prompt, e := os.ReadFile(filepath.Join(unit, task.PromptFile))
						if e != nil || !bytes.Contains(prompt, []byte(".t3/inputs/inputs/"+name)) {
							t.Fatal("missing mount", name, e)
						}
					}
				}
				if !reflect.DeepEqual(m.Tasks["review"].InputsFrom["plan"], []string{"notes/" + map[string]string{"a": "plan.md", "b": "report.md"}[id], "artifacts/handoff.md", "artifacts/verification.log"}) {
					t.Fatal(m.Tasks["review"].InputsFrom)
				}
			}
			// A valid first unit plus a bad second unit must publish nothing.
			for _, broken := range []string{
				strings.Replace(raw, "notes/report.md", "artifacts/verification.log", 1),
				strings.Replace(raw, "active-cost-scope.json", "missing-input.json", 1),
				strings.Replace(raw, "active-cost-scope.json", "../outside", 1),
				strings.Replace(raw, "active-cost-scope.json", "common.md", 1),
			} {
				if err := os.WriteFile(path, []byte(broken), 0o644); err != nil {
					t.Fatal(err)
				}
				refused := filepath.Join(t.TempDir(), "refused")
				var output bytes.Buffer
				err := campaignTestCLI(t, &output).run(context.Background(), []string{"compile", path, "--out", refused, "--json"})
				if err == nil {
					t.Fatal("accepted invalid plan")
				}
				if _, e := os.Stat(refused); !os.IsNotExist(e) {
					t.Fatal("published refused unit", e)
				}
				if strings.Contains(output.String(), strings.Repeat("common.md\n", 500)) {
					t.Fatal("diagnostics exposed body")
				}
				var document map[string]any
				if e := json.Unmarshal(output.Bytes(), &document); e != nil {
					t.Fatal(fmt.Sprintf("diagnostics: %s", output.String()), e)
				}
			}
		}
	}
}
