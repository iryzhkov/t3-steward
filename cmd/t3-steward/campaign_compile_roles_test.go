package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

const campaignCompileRolesFixture = "../../internal/campaign/testdata/compile/roles"

// compileRolesFixturePlan copies the role-routed compile fixture, its extra
// input included, into a temporary directory.
func compileRolesFixturePlan(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"plan.md", filepath.Join("design", "notes.md")} {
		raw, err := os.ReadFile(filepath.Join(campaignCompileRolesFixture, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return filepath.Join(dir, "plan.md")
}

// A unit compiled from a role-routed plan is one "campaign check" resolves by
// role: the readiness request carries each task's role and effort, the review
// task as review-type with the implement task as its producer, and the check
// shows the coordinator's role selection for each.
func TestCampaignCheckShowsTheRoleSelectionOfACompiledRoleUnit(t *testing.T) {
	planPath := compileRolesFixturePlan(t)
	out := filepath.Join(t.TempDir(), "wave")
	var stdout bytes.Buffer
	if err := campaignTestCLI(t, &stdout).run(context.Background(), []string{"compile", planPath, "--out", out}); err != nil {
		t.Fatalf("%v:\n%s", err, stdout.String())
	}
	dir := filepath.Join(out, "r1")
	assertDirectoryEntries(t, filepath.Join(dir, "inputs"), "design", "plan.md", "unit.md")
	selections := map[string]*domain.RoleSelection{
		"implement": {Role: "execute", Route: "claudeAgent/claude-opus-5-5", Effort: "medium", PolicyDigest: "fixture-digest", Reason: "first eligible candidate", ResolvedAt: time.Unix(1, 0).UTC()},
		"review": {Role: "review", Route: "codex/gpt-6.1-sol", Effort: "high", PolicyDigest: "fixture-digest", Reason: "first diverse eligible candidate",
			Diversity: domain.RoleDiversity{ProducerFamilies: []string{"claude"}, CrossProvider: true, Reason: "cross-provider: codex differs from producer providers claude"}, ResolvedAt: time.Unix(1, 0).UTC()},
	}
	var checked bytes.Buffer
	cli, requests := campaignCheckCLI(t, &checked, func(request backlogadmin.ViabilityRequest) backlogadmin.ViabilityMatrix {
		matrix := campaignReadyMatrix(request)
		for i := range matrix.Tasks {
			matrix.Tasks[i].RoleSelection = selections[matrix.Tasks[i].Task]
		}
		return matrix
	})
	if err := cli.run(context.Background(), []string{"check", dir}); err != nil {
		t.Fatalf("%v:\n%s", err, checked.String())
	}
	if len(*requests) != 1 {
		t.Fatalf("viability queries = %d", len(*requests))
	}
	roles := map[string]backlogadmin.ViabilityTask{}
	for _, task := range (*requests)[0].Tasks {
		roles[task.Name] = task
	}
	if task := roles["implement"]; task.Role != "execute" || task.RoleEffort != "medium" || len(task.Routes) != 0 || task.ResourcePreset != "build" {
		t.Fatalf("implement request = %+v", task)
	}
	if task := roles["review"]; task.Role != "review" || task.RoleEffort != "high" || len(task.Routes) != 0 || !task.ReviewType ||
		strings.Join(task.Producers, ",") != "implement" {
		t.Fatalf("review request = %+v", task)
	}
	for _, want := range []string{
		"role: execute", "selected route: claudeAgent/claude-opus-5-5",
		"role: review", "selected route: codex/gpt-6.1-sol", "effort: high",
		"diversity: cross-provider: codex differs from producer providers claude",
	} {
		if !strings.Contains(checked.String(), want) {
			t.Fatalf("campaign check does not show %q:\n%s", want, checked.String())
		}
	}
}

// An extra input that is missing or resolves outside the plan's directory
// refuses the plan at the input's line, and nothing is written.
func TestCampaignCompileRefusesAnUnreadableInput(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(t *testing.T, planDir string)
		want  string
	}{
		{"missing", func(t *testing.T, planDir string) {
			if err := os.Remove(filepath.Join(planDir, "design", "notes.md")); err != nil {
				t.Fatal(err)
			}
		}, "no such file"},
		{"a directory", func(t *testing.T, planDir string) {
			path := filepath.Join(planDir, "design", "notes.md")
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(path, 0o755); err != nil {
				t.Fatal(err)
			}
		}, "not a regular file"},
		{"a link out of the plan's directory", func(t *testing.T, planDir string) {
			outside := filepath.Join(t.TempDir(), "secret.md")
			if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(planDir, "design", "notes.md")
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, path); err != nil {
				t.Skipf("symbolic links are unavailable: %v", err)
			}
		}, "outside the plan's directory"},
	} {
		t.Run(test.name, func(t *testing.T) {
			planPath := compileRolesFixturePlan(t)
			test.setup(t, filepath.Dir(planPath))
			out := filepath.Join(t.TempDir(), "wave")
			var stdout bytes.Buffer
			err := campaignTestCLI(t, &stdout).run(context.Background(), []string{"compile", planPath, "--out", out})
			if err == nil || !strings.Contains(err.Error(), "plan.md:21:") || !strings.Contains(err.Error(), `"design/notes.md"`) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("err = %v", err)
			}
			if code := exitCodeFor(err); code != 1 {
				t.Fatalf("exit = %d, want 1", code)
			}
			if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
				t.Fatalf("a refused plan created %s: %v", out, statErr)
			}
		})
	}
}

func TestCampaignCompileHelpDocumentsRolesAndTheNewKeys(t *testing.T) {
	page, found := helpPageFor("campaign compile")
	if !found {
		t.Fatal("campaign compile has no help page")
	}
	// The page wraps its notes, so it is read as one line of words.
	body := strings.Join(strings.Fields(page.render()), " ")
	for _, want := range []string{
		"roles.execute and roles.review", "routed by role unless routes pins it", "refused as a conflict",
		"ledger", "placement", "{implement: {preset: build}, review: {preset: light}}", "max_turns",
		"inputs/<path>", "campaign check DIR/<unit id>", "only the fields that differ from the workflow defaults",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("campaign compile help does not mention %q:\n%s", want, body)
		}
	}
}

// An extra input counts towards the size of the unit it is bundled into.
func TestCampaignCompileRefusesAnInputLargerThanACampaign(t *testing.T) {
	planPath := compileRolesFixturePlan(t)
	if err := os.WriteFile(filepath.Join(filepath.Dir(planPath), "design", "notes.md"), bytes.Repeat([]byte("x"), 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "wave")
	var stdout bytes.Buffer
	cli := campaignTestCLI(t, &stdout)
	cli.limits.MaxBytes = 2048
	err := cli.run(context.Background(), []string{"compile", planPath, "--out", out})
	if err == nil || !strings.Contains(err.Error(), "plan.md:21:") || !strings.Contains(err.Error(), "larger than the 2048 bytes") {
		t.Fatalf("err = %v", err)
	}
	if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
		t.Fatalf("a refused plan created %s: %v", out, statErr)
	}
}
