package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCampaignRerunUseCommitRequestsAndReportsFailedCommit(t *testing.T) {
	result := campaignRerunResult()
	raw := []byte(`{"sourceRunId":"run-1","sourceTaskId":"review","sourceAttemptId":"review-attempt","idempotencyKey":"reuse-1","reusedCommits":[{"producer":"implement","name":"implementation","commit":"abc123","sourceAttemptId":"failed-attempt","verificationFailures":["allowlist failed","other check"]}]}`)
	if err := json.Unmarshal(raw, result.Graph.RerunOf); err != nil {
		t.Fatal(err)
	}
	for _, asJSON := range []bool{false, true} {
		var out bytes.Buffer
		var sent domain.GraphAmendment
		cli := campaignRerunCLI(&out, &sent, result, nil)
		cli.release = func(context.Context) (string, error) { return "0.11.0-rc.118", nil }
		args := []string{"rerun", "run-1", "--from", "review", "--use-commit", "--idempotency-key", "reuse-1"}
		if asJSON {
			args = append(args, "--json")
		}
		if err := cli.run(context.Background(), args); err != nil {
			t.Fatal(err)
		}
		request, err := json.Marshal(sent)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(request, &fields); err != nil {
			t.Fatal(err)
		}
		if string(fields["useCommit"]) != "true" {
			t.Fatalf("useCommit request missing: %s", request)
		}
		for _, fragment := range []string{"implement", "implementation", "abc123", "failed-attempt", "allowlist failed"} {
			if !strings.Contains(out.String(), fragment) {
				t.Fatalf("receipt missing %q: %s", fragment, out.String())
			}
		}
	}
}

func TestCampaignRerunUseCommitHelp(t *testing.T) {
	for _, fragment := range []string{"--use-commit", "failed attempt", "verification failure", "0.11.0-rc.118"} {
		if !strings.Contains(campaign.RerunHelp, fragment) {
			t.Fatalf("rerun help missing %q", fragment)
		}
	}
}

func TestCampaignRerunUseCommitDuplicateAndOldCoordinator(t *testing.T) {
	base := []string{"run-1", "--from", "review", "--idempotency-key", "reuse-1", "--use-commit"}
	if _, err := parseCampaignRerunArgs(base); err != nil {
		t.Fatal(err)
	}
	if _, err := parseCampaignRerunArgs(append(base, "--use-commit")); err == nil || !strings.Contains(err.Error(), "only once") {
		t.Fatalf("duplicate: %v", err)
	}
	// rc.117 shipped without A3, so it is refused like any older coordinator.
	for _, release := range []string{"0.11.0-rc.116", "0.11.0-rc.117", "unknown", ""} {
		var sent domain.GraphAmendment
		cli := campaignRerunCLI(&bytes.Buffer{}, &sent, campaignRerunResult(), nil)
		cli.release = func(context.Context) (string, error) { return release, nil }
		if err := cli.run(context.Background(), append([]string{"rerun"}, base...)); err == nil || !strings.Contains(err.Error(), "0.11.0-rc.118") {
			t.Fatalf("old coordinator %q: %v", release, err)
		}
		if sent.ID != "" {
			t.Fatal("unsupported coordinator received amendment")
		}
	}
}
