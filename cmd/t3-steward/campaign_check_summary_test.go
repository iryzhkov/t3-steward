package main

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

func TestCampaignCheckSummaryAndVerbose(t *testing.T) {
	for _, test := range []struct {
		name   string
		answer func(backlogadmin.ViabilityRequest) backlogadmin.ViabilityMatrix
		want   string
		exit   int
	}{
		{"ready", campaignReadyMatrix, "selected worker: ", 0},
		{"waiting", campaignWaitingOnQuota, "temporary  " + backlogadmin.ReasonQuotaClosed, 0},
		{"impossible", campaignImpossibleMatrix, "permanent  " + backlogadmin.ReasonUnknownProject, 8},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := campaignFixture(t)
			warning := "project-binding-defaulted: project has no binding"
			answer := func(request backlogadmin.ViabilityRequest) backlogadmin.ViabilityMatrix {
				matrix := test.answer(request)
				for i := range matrix.Tasks {
					if matrix.Tasks[i].Outcome == backlogadmin.ViabilityReady {
						matrix.Tasks[i].SelectedWorker = "homelab"
					}
					if len(matrix.Tasks[i].Candidates) == 0 {
						matrix.Tasks[i].Candidates = []backlogadmin.ViabilityCandidate{{Worker: "spare"}}
					}
					for j := range matrix.Tasks[i].Candidates {
						matrix.Tasks[i].Candidates[j].Unchecked = []string{warning}
					}
				}
				return matrix
			}
			for _, verbose := range []bool{false, true} {
				var out bytes.Buffer
				cli, requests := campaignCheckCLI(t, &out, answer)
				args := []string{"check", root}
				if verbose {
					args = append(args, "--verbose")
				}
				err := cli.run(context.Background(), args)
				if code := backlogadmin.ExitCodeFor(err); code != test.exit {
					t.Fatalf("verbose=%v: exit %d, want %d: %v", verbose, code, test.exit, err)
				}
				matrix := answer((*requests)[0])
				document := campaignCheck{Name: "example-campaign", Matrix: matrix}
				if verbose {
					var expected bytes.Buffer
					if err := renderCampaignCheck(&expected, document); err != nil {
						t.Fatal(err)
					}
					if out.String() != expected.String() {
						t.Fatalf("verbose rendering changed:\n%s\nwant:\n%s", out.String(), expected.String())
					}
				} else {
					lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
					if len(lines) != len(matrix.Tasks)+2 || lines[0] != campaignOutcomeHeadline(document.Name, matrix) {
						t.Fatalf("summary lines = %q", lines)
					}
					for i, task := range matrix.Tasks {
						if !strings.HasPrefix(lines[i+1], "  "+task.Task+"  "+string(task.Outcome)) ||
							!strings.Contains(lines[i+1], test.want) {
							t.Fatalf("task row = %q, want %q", lines[i+1], test.want)
						}
					}
					if strings.Count(out.String(), warning) != 1 {
						t.Fatalf("shared warning repeated:\n%s", out.String())
					}
				}
			}
			var plain, verbose bytes.Buffer
			for i, out := range []*bytes.Buffer{&plain, &verbose} {
				cli, _ := campaignCheckCLI(t, out, answer)
				args := []string{"check", root, "--json"}
				if i == 1 {
					args = append(args, "--verbose")
				}
				_ = cli.run(context.Background(), args)
			}
			if plain.String() != verbose.String() {
				t.Fatal("--verbose changed JSON")
			}
			var document campaignCheck
			if err := json.Unmarshal(plain.Bytes(), &document); err != nil {
				t.Fatal(err)
			}
			request := backlogadmin.ViabilityRequest{Tasks: []backlogadmin.ViabilityTask{{Name: "review"}, {Name: "implement"}}}
			if !reflect.DeepEqual(document.Matrix, answer(request)) {
				t.Fatalf("JSON matrix changed: %+v", document.Matrix)
			}
		})
	}
}

func TestCampaignCheckSummaryPrioritizesPermanentReasons(t *testing.T) {
	matrix := backlogadmin.ViabilityMatrix{Outcome: backlogadmin.ViabilityImpossible,
		Tasks: []backlogadmin.ViabilityTaskResult{{Task: "build", Outcome: backlogadmin.ViabilityImpossible,
			Reasons: []backlogadmin.ViabilityReason{{Code: "temporary", Detail: "wait"}},
			Candidates: []backlogadmin.ViabilityCandidate{{Reasons: []backlogadmin.ViabilityReason{
				{Code: "first-permanent", Permanent: true, Detail: "fix"},
				{Code: "second-permanent", Permanent: true, Detail: "other"},
			}}},
		}},
	}
	var out bytes.Buffer
	if err := renderCampaignCheckSummary(&out, campaignCheck{Name: "demo", Matrix: matrix}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "permanent  first-permanent: fix") ||
		strings.Contains(out.String(), "second-permanent") || strings.Contains(out.String(), "temporary") {
		t.Fatalf("wrong reason selected: %s", out.String())
	}
}

func TestCampaignVerboseOnlyAcceptedOnceByCheck(t *testing.T) {
	for _, test := range []struct {
		command string
		args    []string
	}{
		{"validate", []string{"source", "--verbose"}},
		{"plan", []string{"source", "--verbose"}},
		{"submit", []string{"source", "--verbose"}},
		{"check", []string{"source", "--verbose", "--verbose"}},
	} {
		if _, err := parseCampaignArgs(test.command, test.args, false, false); err == nil {
			t.Fatalf("accepted %s %v", test.command, test.args)
		}
	}
}
