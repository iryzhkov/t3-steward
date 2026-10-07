package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

const campaignCompilePlanFixture = "../../internal/campaign/testdata/compile/plan.md"

// compileFixturePlan copies the shared compile fixture into a temporary
// directory, so that a test can edit it without touching the original.
func compileFixturePlan(t *testing.T) (string, []byte) {
	t.Helper()
	raw, err := os.ReadFile(campaignCompilePlanFixture)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "plan.md")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return path, raw
}

func assertDirectoryEntries(t *testing.T, dir string, want ...string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("%s holds %v, want %v", dir, names, want)
	}
}

// Compile writes one directory per unit that the ordinary validate and plan
// accept, and it reaches no coordinator: campaignTestCLI fails the test if any
// coordinator seam is used.
func TestCampaignCompileWritesUnitsThatValidateAndPlan(t *testing.T) {
	planPath, raw := compileFixturePlan(t)
	out := filepath.Join(t.TempDir(), "wave")
	var stdout bytes.Buffer
	cli := campaignTestCLI(t, &stdout)
	if err := cli.run(context.Background(), []string{"compile", planPath, "--out", out}); err != nil {
		t.Fatal(err)
	}
	text := stdout.String()
	if !strings.HasPrefix(text, "compiled 2 units into "+out+"\n") {
		t.Fatalf("output:\n%s", text)
	}
	for _, unit := range []string{"c1", "b1"} {
		line := ""
		for _, candidate := range strings.Split(text, "\n") {
			if strings.HasPrefix(strings.TrimSpace(candidate), unit+" ") {
				line = candidate
			}
		}
		if !strings.Contains(line, filepath.Join(out, unit)) || !strings.Contains(line, "valid") || !strings.Contains(line, "plan ok") || strings.Contains(line, "check") {
			t.Fatalf("unit %s line %q in:\n%s", unit, line, text)
		}
	}
	assertDirectoryEntries(t, out, "b1", "c1")
	for _, unit := range []string{"c1", "b1"} {
		dir := filepath.Join(out, unit)
		assertDirectoryEntries(t, dir, "inputs", "prompts", "workflow.yaml")
		if copied, err := os.ReadFile(filepath.Join(dir, "inputs", "plan.md")); err != nil || !bytes.Equal(copied, raw) {
			t.Fatalf("%s inputs/plan.md is not the plan byte for byte: %v", unit, err)
		}
		for _, verb := range []string{"validate", "plan"} {
			var verbOut bytes.Buffer
			if err := campaignTestCLI(t, &verbOut).run(context.Background(), []string{verb, dir}); err != nil {
				t.Fatalf("campaign %s %s: %v", verb, unit, err)
			}
		}
	}
}

func TestCampaignCompileJSONIsOneDocumentWithOneObjectPerUnit(t *testing.T) {
	planPath, _ := compileFixturePlan(t)
	out := filepath.Join(t.TempDir(), "wave")
	var stdout bytes.Buffer
	if err := campaignTestCLI(t, &stdout).run(context.Background(), []string{"compile", planPath, "--out", out, "--json"}); err != nil {
		t.Fatal(err)
	}
	var document campaignCompileReport
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		t.Fatalf("%v:\n%s", err, stdout.String())
	}
	if document.SchemaVersion != 1 || document.Out != out || len(document.Units) != 2 {
		t.Fatalf("document = %+v", document)
	}
	for index, id := range []string{"c1", "b1"} {
		unit := document.Units[index]
		if unit.ID != id || unit.Directory != filepath.Join(out, id) || !unit.Valid || len(unit.Errors) != 0 {
			t.Fatalf("unit %d = %+v", index, unit)
		}
		if unit.Plan == nil || unit.Plan.Name != id || unit.Plan.Tasks != 2 || unit.Plan.Edges != 1 || unit.Plan.Digest == "" {
			t.Fatalf("unit %s plan = %+v", id, unit.Plan)
		}
		if unit.Check != nil {
			t.Fatalf("unit %s has a check without --check: %+v", id, unit.Check)
		}
	}
}

// A refused plan writes nothing, and its exit code is the campaign usage code.
func TestCampaignCompileRefusedPlanWritesNothing(t *testing.T) {
	planPath, raw := compileFixturePlan(t)
	broken := strings.Replace(string(raw), "section: B-1", "section: Z-9", 1)
	if err := os.WriteFile(planPath, []byte(broken), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "wave")
	var stdout bytes.Buffer
	err := campaignTestCLI(t, &stdout).run(context.Background(), []string{"compile", planPath, "--out", out})
	if err == nil || !strings.Contains(err.Error(), "plan.md:15:") || !strings.Contains(err.Error(), `"Z-9"`) {
		t.Fatalf("err = %v", err)
	}
	if code := exitCodeFor(err); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
		t.Fatalf("a refused plan created %s: %v", out, statErr)
	}
}

func TestCampaignCompileArgumentRefusals(t *testing.T) {
	planPath, _ := compileFixturePlan(t)
	out := filepath.Join(t.TempDir(), "wave")
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"compile"}, "needs a plan"},
		{[]string{"compile", planPath}, "--out"},
		{[]string{"compile", planPath, "--out"}, "--out"},
		{[]string{"compile", planPath, planPath, "--out", out}, "exactly one plan"},
		{[]string{"compile", planPath, "--out", out, "--out", out}, "--out"},
		{[]string{"compile", planPath, "--out", out, "--dot"}, "--dot"},
		{[]string{"compile", planPath, "--out", out, "--idempotency-key", "k"}, "--idempotency-key"},
		{[]string{"compile", planPath, "--out", out, "--unit", "zz"}, `"zz"`},
		{[]string{"compile", planPath, "--out", out, "--unit"}, "--unit"},
		{[]string{"compile", filepath.Join(t.TempDir(), "absent.md"), "--out", out}, "absent.md"},
	} {
		var stdout bytes.Buffer
		err := campaignTestCLI(t, &stdout).run(context.Background(), test.args)
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("%v: err = %v, want %q", test.args, err, test.want)
		}
		if code := exitCodeFor(err); code != 1 {
			t.Fatalf("%v: exit %d, want 1", test.args, code)
		}
		if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
			t.Fatalf("%v created %s", test.args, out)
		}
	}
}

func TestCampaignCompileSelectsUnits(t *testing.T) {
	planPath, _ := compileFixturePlan(t)
	out := filepath.Join(t.TempDir(), "wave")
	var stdout bytes.Buffer
	if err := campaignTestCLI(t, &stdout).run(context.Background(), []string{"compile", planPath, "--out", out, "--unit", "b1"}); err != nil {
		t.Fatal(err)
	}
	assertDirectoryEntries(t, out, "b1")
	if !strings.HasPrefix(stdout.String(), "compiled 1 unit into ") {
		t.Fatalf("output:\n%s", stdout.String())
	}
}

// An existing unit directory is refused before anything is written, and
// --force replaces only the units being compiled.
func TestCampaignCompileRefusesExistingUnitsUnlessForced(t *testing.T) {
	planPath, _ := compileFixturePlan(t)
	out := filepath.Join(t.TempDir(), "wave")
	for _, dir := range []string{"b1", "unrelated"} {
		if err := os.MkdirAll(filepath.Join(out, dir), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(out, dir, "keep.txt"), []byte("mine"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var stdout bytes.Buffer
	err := campaignTestCLI(t, &stdout).run(context.Background(), []string{"compile", planPath, "--out", out})
	if err == nil || !strings.Contains(err.Error(), filepath.Join(out, "b1")) || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("err = %v", err)
	}
	if exitCodeFor(err) != 1 {
		t.Fatalf("exit = %d, want 1", exitCodeFor(err))
	}
	// c1 comes first in the plan and is not written either: the refusal is
	// made before any unit is.
	assertDirectoryEntries(t, out, "b1", "unrelated")

	// --force with --unit c1 leaves b1 alone.
	if err := campaignTestCLI(t, &stdout).run(context.Background(), []string{"compile", planPath, "--out", out, "--unit", "c1", "--force"}); err != nil {
		t.Fatal(err)
	}
	assertDirectoryEntries(t, filepath.Join(out, "b1"), "keep.txt")

	if err := campaignTestCLI(t, &stdout).run(context.Background(), []string{"compile", planPath, "--out", out, "--force"}); err != nil {
		t.Fatal(err)
	}
	assertDirectoryEntries(t, out, "b1", "c1", "unrelated")
	assertDirectoryEntries(t, filepath.Join(out, "b1"), "inputs", "prompts", "workflow.yaml")
	assertDirectoryEntries(t, filepath.Join(out, "unrelated"), "keep.txt")
}

// --check runs the same readiness path as "campaign check" for every unit and
// maps its outcomes onto that verb's exit codes.
func TestCampaignCompileCheckExitCodes(t *testing.T) {
	for _, test := range []struct {
		name   string
		answer func(backlogadmin.ViabilityRequest) backlogadmin.ViabilityMatrix
		state  string
		exit   int
	}{
		{"ready", campaignReadyMatrix, "ready", 0},
		{"accepted_waiting", campaignWaitingOnQuota, "accepted_waiting", 0},
		{"impossible", campaignImpossibleMatrix, "impossible", 8},
	} {
		t.Run(test.name, func(t *testing.T) {
			planPath, _ := compileFixturePlan(t)
			for _, asJSON := range []bool{false, true} {
				out := filepath.Join(t.TempDir(), "wave")
				var stdout bytes.Buffer
				cli, requests := campaignCheckCLI(t, &stdout, test.answer)
				args := []string{"compile", planPath, "--out", out, "--check"}
				if asJSON {
					args = append(args, "--json")
				}
				err := cli.run(context.Background(), args)
				if got := exitCodeFor(err); (err == nil) != (test.exit == 0) || (err != nil && got != test.exit) {
					t.Fatalf("json=%t: err = %v (exit %d), want exit %d", asJSON, err, got, test.exit)
				}
				if len(*requests) != 2 {
					t.Fatalf("viability queries = %d, want one per unit", len(*requests))
				}
				for _, request := range *requests {
					if len(request.Tasks) != 2 || request.Tasks[0].Ref != "d2e827681336e76413fdab34392f029a3e131cf4" {
						t.Fatalf("viability request = %+v", request)
					}
				}
				if !asJSON {
					if !strings.Contains(stdout.String(), "check "+test.state) {
						t.Fatalf("output does not report check %s:\n%s", test.state, stdout.String())
					}
					continue
				}
				var document campaignCompileReport
				if err := json.Unmarshal(stdout.Bytes(), &document); err != nil {
					t.Fatalf("%v:\n%s", err, stdout.String())
				}
				for _, unit := range document.Units {
					if unit.Check == nil || string(unit.Check.Outcome) != test.state {
						t.Fatalf("unit %s check = %+v", unit.ID, unit.Check)
					}
					if test.state != "ready" && len(unit.Check.Reasons) == 0 {
						t.Fatalf("unit %s check has no reasons: %+v", unit.ID, unit.Check)
					}
				}
			}
		})
	}
}

func TestCampaignCompileIsAVerbWithHelpAndATypoHint(t *testing.T) {
	if near := nearestCampaignCommand("compiel"); near != "compile" {
		t.Fatalf("nearest to compiel = %q", near)
	}
	if !strings.Contains(campaignUsage, "compile PLAN --out DIR") {
		t.Fatal("campaign usage does not list compile")
	}
	page, found := helpPageFor("campaign compile")
	if !found {
		t.Fatal("campaign compile has no help page")
	}
	body := page.render()
	for _, want := range []string{"compile: v1", "implement-review", "--force", "--check", "never submits", ".t3/inputs/inputs/unit.md"} {
		if !strings.Contains(body, want) {
			t.Fatalf("campaign compile help does not mention %q:\n%s", want, body)
		}
	}
}
