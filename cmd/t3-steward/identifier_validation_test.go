package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// unsafeIdempotencyKeys are keys the coordinator refuses, each with the escaped
// form a refusal must show instead of the raw bytes.
var unsafeIdempotencyKeys = map[string]string{
	"":                       `""`,
	" padded":                `" padded"`,
	strings.Repeat("k", 257): `"` + strings.Repeat("k", 257) + `"`,
	strings.Repeat("k", 400): `"` + strings.Repeat("k", 400) + `"`,
	"key\x1b[2J":             `"key\x1b[2J"`,
	"ke\ny":                  `"ke\ny"`,
	"key\x00":                `"key\x00"`,
}

func wantKeyRefusal(t *testing.T, err error, escaped string) {
	t.Helper()
	want := "--idempotency-key " + escaped + ": " + domain.IdempotencyKeyRule
	if err == nil || err.Error() != want {
		t.Fatalf("err = %v, want %q", err, want)
	}
}

func TestTaskRunDryRunRefusesUnsafeIdempotencyKeysOffline(t *testing.T) {
	for key, escaped := range unsafeIdempotencyKeys {
		if key == "" {
			// An empty flag value is refused by the flag parser, and an absent
			// key is derived.
			continue
		}
		h := newTaskRunHarness()
		err := h.run("--dry-run", "--model", "opus", "--idempotency-key", key, "--", "Check")
		wantKeyRefusal(t, err, escaped)
		if len(h.queries) != 0 || len(h.viability) != 0 || len(h.requests) != 0 || len(h.notified) != 0 {
			t.Fatalf("key %q: the coordinator was contacted: queries=%v viability=%d requests=%d", key, h.queries, len(h.viability), len(h.requests))
		}
	}
}

func TestTaskRunDryRunAcceptsTheLongestKeyAndPrintsItAsGiven(t *testing.T) {
	key := strings.Repeat("k", 256)
	h := newTaskRunHarness()
	if err := h.run("--dry-run", "--model", "opus", "--idempotency-key", key, "--", "Check"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(h.stdout.String(), "\nkey "+key+"\n") {
		t.Fatalf("output=%s", h.stdout.String())
	}
	h = newTaskRunHarness()
	if err := h.run("--dry-run", "--json", "--model", "opus", "--idempotency-key", "ключ release", "--", "Check"); err != nil {
		t.Fatal(err)
	}
	var dry dryRunTestDocument
	if err := json.Unmarshal(h.stdout.Bytes(), &dry); err != nil || dry.IdempotencyKey != "ключ release" {
		t.Fatalf("dry=%+v err=%v", dry, err)
	}
}

func TestDisplayValueEscapesOnlyUnprintableValues(t *testing.T) {
	for value, want := range map[string]string{
		"plain-key":    "plain-key",
		"ключ":         "ключ",
		"with space":   "with space",
		"esc\x1b[2J":   `"esc\x1b[2J"`,
		"line\nbreak":  `"line\nbreak"`,
		`quote"inside`: `"quote\"inside"`,
	} {
		if got := displayValue(value); got != want {
			t.Errorf("displayValue(%q) = %s, want %s", value, got, want)
		}
	}
}

func TestTaskRunDryRunRefusesUnsafeOutputsOffline(t *testing.T) {
	for output, want := range map[string]string{
		"report\nfake.md":         `--output path "report\nfake.md": path contains a control character`,
		"report\x7f.md":           `--output path "report\x7f.md": path contains a control character`,
		strings.Repeat("o", 5000): "path component is 5000 bytes, longer than 255",
	} {
		h := newTaskRunHarness()
		err := h.run("--dry-run", "--model", "opus", "--output", output, "--", "Check")
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
		if len(h.queries) != 0 || len(h.viability) != 0 || len(h.requests) != 0 {
			t.Fatalf("the coordinator was contacted for %q", output)
		}
	}
}

func TestSubmissionCommandsRefuseUnsafeIdempotencyKeys(t *testing.T) {
	for key, escaped := range unsafeIdempotencyKeys {
		if key == "" {
			continue
		}
		_, err := parseCampaignArgs("submit", []string{"campaign-dir", "--idempotency-key", key}, false, true)
		wantKeyRefusal(t, err, escaped)
		_, _, _, err = parseSubmissionArgs([]string{"bundle.tar", "--idempotency-key", key})
		wantKeyRefusal(t, err, escaped)
		if strings.TrimSpace(key) != "" {
			_, err = parseCampaignFixArgs([]string{"run-1/review", "--idempotency-key", key, "--dry-run", "--out", "fix"})
			wantKeyRefusal(t, err, escaped)
		}
	}
	if _, err := parseCampaignArgs("submit", []string{"campaign-dir", "--idempotency-key", strings.Repeat("k", 256)}, false, true); err != nil {
		t.Fatalf("256-byte key refused: %v", err)
	}
	if _, _, _, err := parseSubmissionArgs([]string{"bundle.tar", "--idempotency-key", "release 2026-10-08"}); err != nil {
		t.Fatalf("key with an inner space refused: %v", err)
	}
}

// crossRunCheckPlan is a one-task plan that needs the given cross-run nodes.
func crossRunCheckPlan(needs ...string) campaign.Plan {
	return campaign.Plan{
		Environment: campaign.Environment{Project: "steward", Type: "git", Ref: "main"},
		Tasks:       []campaign.Task{{Name: "consume", ExternalNeeds: needs}},
	}
}

func readyMatrix() backlogadmin.ViabilityMatrix {
	return backlogadmin.ViabilityMatrix{
		Outcome: backlogadmin.ViabilityReady,
		Tasks:   []backlogadmin.ViabilityTaskResult{{Task: "consume", Outcome: backlogadmin.ViabilityReady}},
	}
}

func crossRunCheckCLI(lookups *[]string, runs map[string]backlogadmin.WorkflowDetail, lookupErr error) campaignCLI {
	return campaignCLI{
		viability: func(context.Context, backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error) {
			return readyMatrix(), nil
		},
		detail: func(_ context.Context, runID string) (backlogadmin.WorkflowDetail, error) {
			*lookups = append(*lookups, runID)
			if lookupErr != nil {
				return backlogadmin.WorkflowDetail{}, lookupErr
			}
			detail, ok := runs[runID]
			if !ok {
				// The remote carrier delivers the coordinator's refusal as text.
				return backlogadmin.WorkflowDetail{}, errors.New(fmt.Errorf("%w: workflow run %q", backlogadmin.ErrNotFound, runID).Error())
			}
			return detail, nil
		},
	}
}

func TestCampaignCheckReportsUnknownCrossRunNodes(t *testing.T) {
	runs := map[string]backlogadmin.WorkflowDetail{
		"run-known": {
			Summary: backlogadmin.WorkflowSummary{Run: domain.WorkflowRun{ID: "run-known", Sink: &domain.SinkTask{ID: "sink-known"}}},
			Tasks:   []backlogadmin.TaskDetail{{Task: domain.Task{ID: "task-1", Name: "probe"}}},
		},
		"run:legacy:7": {
			Tasks: []backlogadmin.TaskDetail{{Task: domain.Task{ID: "task:legacy:1", Name: "probe"}}},
		},
	}
	for name, test := range map[string]struct {
		needs []string
		want  string
	}{
		"unknown run":  {[]string{"run-missing/probe"}, `task consume needs "run-missing/probe": run "run-missing" is unknown to the coordinator`},
		"unknown task": {[]string{"run-known/absent"}, `task consume needs "run-known/absent": task "absent" is unknown in run "run-known"`},
		"second need":  {[]string{"run-known/probe", "run-missing/probe"}, `run "run-missing" is unknown`},
	} {
		t.Run(name, func(t *testing.T) {
			var lookups []string
			matrix, err := crossRunCheckCLI(&lookups, runs, nil).checkViability(context.Background(), crossRunCheckPlan(test.needs...), campaign.Bundle{}, "")
			if err != nil {
				t.Fatal(err)
			}
			if matrix.Outcome != backlogadmin.ViabilityImpossible || matrix.Tasks[0].Outcome != backlogadmin.ViabilityImpossible {
				t.Fatalf("matrix = %+v, want impossible", matrix)
			}
			reasons := matrix.PermanentReasons()
			if len(reasons) != 1 || reasons[0].Code != backlogadmin.ReasonUnknownNode || !strings.Contains(reasons[0].Detail, test.want) {
				t.Fatalf("reasons = %+v, want %q", reasons, test.want)
			}
		})
	}
	for _, needs := range [][]string{{"run-known/probe"}, {"run-known/task-1"}, {"run-known/__sink"}, {"run-known/sink-known"}, {"run:legacy:7/probe"}} {
		var lookups []string
		matrix, err := crossRunCheckCLI(&lookups, runs, nil).checkViability(context.Background(), crossRunCheckPlan(needs...), campaign.Bundle{}, "")
		if err != nil || matrix.Outcome != backlogadmin.ViabilityReady {
			t.Fatalf("needs %v: outcome=%s err=%v", needs, matrix.Outcome, err)
		}
		if len(lookups) != 1 {
			t.Fatalf("needs %v: lookups=%v", needs, lookups)
		}
	}
}

func TestCampaignCheckDoesNotGuessWhenTheNodeLookupFails(t *testing.T) {
	var lookups []string
	_, err := crossRunCheckCLI(&lookups, nil, errors.New("coordinator unreachable")).checkViability(context.Background(), crossRunCheckPlan("run-known/probe"), campaign.Bundle{}, "")
	if err == nil || !strings.Contains(err.Error(), `look up cross-run dependency "run-known/probe": coordinator unreachable`) {
		t.Fatalf("err = %v", err)
	}
}

func TestCampaignCheckWithoutCrossRunNeedsLooksNothingUp(t *testing.T) {
	var lookups []string
	matrix, err := crossRunCheckCLI(&lookups, nil, nil).checkViability(context.Background(), crossRunCheckPlan(), campaign.Bundle{}, "")
	if err != nil || matrix.Outcome != backlogadmin.ViabilityReady || len(lookups) != 0 {
		t.Fatalf("outcome=%s err=%v lookups=%v", matrix.Outcome, err, lookups)
	}
}
