package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

func repinSnapshot(t *testing.T, root string) string {
	t.Helper()
	var snapshot strings.Builder
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		snapshot.WriteString(path)
		if !entry.IsDir() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			snapshot.Write(data)
		}
		return nil
	})
	repinMust(t, err)
	return snapshot.String()
}

func TestCampaignRepin(t *testing.T) {
	target := strings.Repeat("a", 40)
	for _, scenario := range []string{"ready", "waiting", "impossible", "transport", "prompt", "ref", "unit", "plan", "extra", "basename", "short", "branch", "malformed"} {
		t.Run(scenario, func(t *testing.T) {
			planPath, raw := compileFixturePlan(t)
			old := "d2e827681336e76413fdab34392f029a3e131cf4"
			raw = bytes.Replace(raw, []byte("class: required"), []byte("inputs: [nested/extra.md]\nclass: required"), 1)
			repinMust(t, os.MkdirAll(filepath.Join(filepath.Dir(planPath), "nested"), 0o755))
			repinMust(t, os.WriteFile(filepath.Join(filepath.Dir(planPath), "nested/extra.md"), []byte(old), 0o644))
			repinMust(t, os.WriteFile(planPath, raw, 0o644))
			out := filepath.Join(t.TempDir(), "wave")
			var stdout bytes.Buffer
			cli, _ := campaignCheckCLI(t, &stdout, campaignReadyMatrix)
			repinMust(t, cli.run(context.Background(), []string{"compile", planPath, "--out", out}))
			dir, to, wantExit := filepath.Join(out, "c1"), target, 1
			path, content := "", []byte("changed")
			switch scenario {
			case "prompt":
				path = "prompts/implement.md"
			case "ref":
				path = "workflow.yaml"
				data, _ := os.ReadFile(filepath.Join(dir, path))
				content = bytes.ReplaceAll(data, []byte(old), []byte(target))
			case "unit":
				path = "inputs/plan.md"
				content = bytes.Replace(raw, []byte("id: c1"), []byte("id: other"), 1)
			case "plan":
				path = "inputs/plan.md"
			case "extra":
				path = "inputs/nested/extra.md"
			case "basename":
				renamed := filepath.Join(out, "renamed")
				repinMust(t, os.Rename(dir, renamed))
				dir = renamed
			case "short":
				to = "abc"
			case "branch":
				to = "main"
			case "malformed":
				to = strings.Repeat("g", 40)
			default:
				wantExit = 0
				if scenario == "impossible" {
					wantExit = 8
				}
				if scenario == "transport" {
					wantExit = 1
				}
			}
			if path != "" {
				var err error
				if scenario == "plan" || scenario == "extra" {
					err = os.Remove(filepath.Join(dir, path))
				} else {
					err = os.WriteFile(filepath.Join(dir, path), content, 0o644)
				}
				repinMust(t, err)
			}
			before, sibling := repinSnapshot(t, out), repinSnapshot(t, filepath.Join(out, "b1"))
			cli.viability = func(_ context.Context, request backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error) {
				data, _ := os.ReadFile(filepath.Join(dir, "inputs/plan.md"))
				if !bytes.Contains(data, []byte(target)) || request.Tasks[0].Ref != target {
					t.Fatal("readiness before publication")
				}
				switch scenario {
				case "waiting":
					return campaignWaitingOnQuota(request), nil
				case "impossible":
					return campaignImpossibleMatrix(request), nil
				case "transport":
					return backlogadmin.ViabilityMatrix{}, errors.New("transport")
				}
				return campaignReadyMatrix(request), nil
			}
			stdout.Reset()
			err := cli.run(context.Background(), []string{"repin", dir, "--to", to, "--check", "--json"})
			if (err == nil) != (wantExit == 0) || (err != nil && exitCodeFor(err) != wantExit) {
				t.Fatalf("exit: %v, want %d", err, wantExit)
			}
			if scenario != "ready" && scenario != "waiting" && scenario != "impossible" && scenario != "transport" {
				if repinSnapshot(t, out) != before {
					t.Fatal("refusal wrote files")
				}
				return
			}
			if repinSnapshot(t, filepath.Join(out, "b1")) != sibling {
				t.Fatal("sibling changed")
			}
			extra, _ := os.ReadFile(filepath.Join(dir, "inputs/nested/extra.md"))
			if string(extra) != old {
				t.Fatal("extra changed")
			}
			if scenario == "transport" {
				data, _ := os.ReadFile(filepath.Join(dir, "inputs/plan.md"))
				if !bytes.Contains(data, []byte(target)) {
					t.Fatal("transport removed publication")
				}
				return
			}
			var report struct{ OldRef, NewRef, Digest string }
			if json.Unmarshal(stdout.Bytes(), &report) != nil || report.OldRef != old || report.NewRef != target || report.Digest == "" {
				t.Fatal("invalid JSON report")
			}
			stdout.Reset()
			cli.viability = campaignTestCLI(t, &stdout).viability
			repinMust(t, cli.run(context.Background(), []string{"repin", dir, "--to", target, "--json"}))
			var same struct{ Digest string }
			if json.Unmarshal(stdout.Bytes(), &same) != nil || same.Digest != report.Digest {
				t.Fatal("digest changed")
			}
		})
	}
	if nearestCampaignCommand("repni") != "repin" {
		t.Fatal("typo hint")
	}
	if _, found := helpPageFor("campaign repin"); !found || !strings.Contains(campaignUsage, "repin DIR --to SHA") {
		t.Fatal("help missing")
	}
}

func repinMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
