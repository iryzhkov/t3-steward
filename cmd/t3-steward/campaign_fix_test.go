package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCampaignFixArgumentsRefusedBeforeCoordinator(t *testing.T) {
	for _, args := range [][]string{
		{"run/review"}, {"bad", "--idempotency-key", "key"},
		{"run/review", "--idempotency-key", "key", "--dry-run"},
		{"run/review", "--idempotency-key", "key", "--round-limit", "0"},
		{"run/review", "--idempotency-key", "key", "--round-limit", "9"},
		{"run/review", "--idempotency-key", "key", "--round-limit", "1.5"},
		{"run/review", "--idempotency-key", "key", "--context", "rules.md"},
		{"run/review", "--idempotency-key", "key", "--context", "a/x.md", "--context", "b/x.md"},
		{"run/review", "--idempotency-key", "key", "--gate-timeout", "0"},
		{"run/review", "--idempotency-key", "key", "--gate-timeout", "7h"},
		{"run/review", "--idempotency-key", "key", "--gate", "x", "--gate", "x"},
		{"run/review", "--idempotency-key", "key", "--gate", "x\x00y"},
		{"run/review", "--idempotency-key", "key", "--commit", "a/b"},
		{"run/review", "--idempotency-key", "key", "--no-notify", "--notify-thread", "current"},
	} {
		cli := campaignFixCLI{campaignCLI: campaignCLI{detail: func(context.Context, string) (backlogadmin.WorkflowDetail, error) {
			t.Fatal("coordinator reached")
			return backlogadmin.WorkflowDetail{}, nil
		}}}
		if err := cli.run(context.Background(), args); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}

func fixCommandFixture(t *testing.T) (campaignFixCLI, *bytes.Buffer) {
	t.Helper()
	out := new(bytes.Buffer)
	producer := domain.Task{ID: "p", Name: "implement", PromptArtifactID: "prompt", Outputs: []domain.ArtifactDeclaration{{Name: "implementation", Commit: &domain.CommitOutput{}}, {Name: "handoff.md"}}, Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt-6.1-sol"}}, MaxTurns: 10, Gate: &domain.TaskGate{Commands: []string{"true"}}}
	review := domain.Task{ID: "r", Name: "review", DependencyInputs: map[string][]string{"implement": {"implementation", "handoff.md"}}, Outputs: []domain.ArtifactDeclaration{{Name: "review.md"}}, Routes: []domain.ProviderRoute{{ProviderInstanceID: "claudeAgent", Model: "claude-opus-5-5"}}, MaxTurns: 10}
	detail := backlogadmin.WorkflowDetail{Summary: backlogadmin.WorkflowSummary{Run: domain.WorkflowRun{ID: "run"}, Workflow: domain.Workflow{Name: "source", Project: "project", Class: domain.TaskClassRequired, Environment: domain.ExecutionEnvironment{Type: "git", Scope: "task", Ref: "main"}}}, Tasks: []backlogadmin.TaskDetail{
		{Task: producer, Attempt: &domain.Attempt{ID: "pa", Progress: domain.ProgressSucceeded}, Artifacts: []backlogadmin.Artifact{{Metadata: backlogadmin.ArtifactMetadata{ID: "commit", Name: "implementation", AttemptID: "pa"}}, {Metadata: backlogadmin.ArtifactMetadata{ID: "handoff", Name: "handoff.md", AttemptID: "pa"}}}},
		{Task: review, Attempt: &domain.Attempt{ID: "ra", Progress: domain.ProgressSucceeded, ReviewVerdict: &domain.ReviewVerdict{Verdict: "changes-requested", BlockingFindings: 1}}, Artifacts: []backlogadmin.Artifact{{Metadata: backlogadmin.ArtifactMetadata{ID: "review", Name: "review.md", AttemptID: "ra"}}}},
	}}
	provenance := backlog.CommitProvenance{Version: backlog.CampaignCommitRecordVersion, WorkflowRunID: "run", TaskID: "p", Name: "implementation", Repository: "project", Base: strings.Repeat("a", 40), Commit: strings.Repeat("b", 40), Ref: "refs/campaign/test"}
	raw, _ := json.Marshal(provenance)
	data := map[string][]byte{"prompt": []byte("original brief"), "commit": raw, "handoff": []byte("handoff"), "review": []byte("VERDICT: CHANGES_REQUESTED\nfix it")}
	cli := campaignFixCLI{campaignCLI: campaignCLI{stdout: out, limits: campaign.DefaultLimits, detail: func(_ context.Context, run string) (backlogadmin.WorkflowDetail, error) { return detail, nil }}, open: func(_ context.Context, id string) (backlogadmin.ArtifactContent, error) {
		return backlogadmin.ArtifactContent{Content: io.NopCloser(bytes.NewReader(data[id]))}, nil
	}}
	return cli, out
}

func TestCampaignFixRefusesAcceptedVerdict(t *testing.T) {
	cli, _ := fixCommandFixture(t)
	old := cli.detail
	cli.detail = func(ctx context.Context, s string) (backlogadmin.WorkflowDetail, error) {
		d, e := old(ctx, s)
		d.Tasks[1].Attempt.ReviewVerdict.Verdict = "accept"
		return d, e
	}
	_, err := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}})
	if err == nil || !strings.Contains(err.Error(), "nothing to fix") {
		t.Fatalf("%v", err)
	}
}
func TestCampaignFixRefusesMissingOrUnsucceededVerdict(t *testing.T) {
	for _, kind := range []string{"missing", "failed", "declared-missing"} {
		t.Run(kind, func(t *testing.T) {
			cli, _ := fixCommandFixture(t)
			old := cli.detail
			cli.detail = func(ctx context.Context, s string) (backlogadmin.WorkflowDetail, error) {
				d, e := old(ctx, s)
				switch kind {
				case "missing":
					d.Tasks[1].Attempt.ReviewVerdict = nil
					d.Tasks[1].Task.Outputs = nil
				case "failed":
					d.Tasks[1].Attempt.Progress = domain.ProgressFailed
				case "declared-missing":
					d.Tasks[1].Attempt.ReviewVerdict = nil
					d.Tasks[1].Task.ReviewOutput = &domain.ReviewOutput{Verdict: "verdict.json"}
				}
				return d, e
			}
			if _, err := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}}); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}
func TestCampaignFixReadsReviewFirstLineFallback(t *testing.T) {
	cli, _ := fixCommandFixture(t)
	old := cli.detail
	cli.detail = func(ctx context.Context, s string) (backlogadmin.WorkflowDetail, error) {
		d, e := old(ctx, s)
		d.Tasks[1].Attempt.ReviewVerdict = nil
		return d, e
	}
	result, err := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Verdict.Source != "review.md first line" {
		t.Fatalf("%+v", result)
	}
	for _, line := range []string{"CHANGES_REQUESTED", " VERDICT: CHANGES_REQUESTED", "VERDICT: CHANGES_REQUESTED ", "VERDICT: CHANGES_REQUESTED\r"} {
		cli.open = func(context.Context, string) (backlogadmin.ArtifactContent, error) {
			return backlogadmin.ArtifactContent{Content: io.NopCloser(strings.NewReader(line + "\n"))}, nil
		}
		if _, err := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}}); err == nil {
			t.Fatalf("accepted %q", line)
		}
	}
}
func TestCampaignFixResolvesLocalCarriedAndExplicitCommit(t *testing.T) {
	for _, mode := range []string{"local", "carried", "explicit"} {
		t.Run(mode, func(t *testing.T) {
			cli, _ := fixCommandFixture(t)
			old := cli.detail
			a := campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}}
			if mode == "explicit" {
				a.commit = "run/implement/implementation"
			}
			if mode == "carried" {
				cli.detail = func(ctx context.Context, s string) (backlogadmin.WorkflowDetail, error) {
					d, e := old(ctx, s)
					d.Tasks[1].Task.DependencyInputs = nil
					d.Tasks[1].Task.CarriedInputs = []domain.CarriedInput{{SourceRunID: "run", ProducerTaskID: "p", Name: "implementation"}}
					return d, e
				}
			}
			r, e := cli.resolve(context.Background(), a)
			if e != nil {
				t.Fatal(e)
			}
			if r.Commit.Commit != strings.Repeat("b", 40) || r.Commit.Task != "implement" {
				t.Fatalf("%+v", r.Commit)
			}
		})
	}
}
func TestCampaignFixRefusesUnresolvableOrAmbiguousCommit(t *testing.T) {
	for _, mode := range []string{"none", "legacy", "ambiguous"} {
		t.Run(mode, func(t *testing.T) {
			cli, _ := fixCommandFixture(t)
			old := cli.detail
			cli.detail = func(ctx context.Context, s string) (backlogadmin.WorkflowDetail, error) {
				d, e := old(ctx, s)
				switch mode {
				case "none":
					d.Tasks[1].Task.DependencyInputs = nil
				case "legacy":
					d.Tasks[1].Task.DependencyInputs = nil
					d.Tasks[1].Task.CarriedInputs = []domain.CarriedInput{{ProducerTaskID: "p", Name: "implementation"}}
				case "ambiguous":
					d.Tasks[0].Task.Outputs = append(d.Tasks[0].Task.Outputs, domain.ArtifactDeclaration{Name: "other", Commit: &domain.CommitOutput{}})
					d.Tasks[1].Task.DependencyInputs["implement"] = append(d.Tasks[1].Task.DependencyInputs["implement"], "other")
				}
				return d, e
			}
			if _, e := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}}); e == nil {
				t.Fatal("accepted")
			}
		})
	}
}
func TestCampaignFixGateResolution(t *testing.T) {
	cli, _ := fixCommandFixture(t)
	for _, tc := range []struct {
		a      campaignFixArgs
		source string
	}{{campaignFixArgs{gate: []string{"custom"}}, "flag"}, {campaignFixArgs{noGate: true}, "no-gate"}, {campaignFixArgs{}, "producer"}} {
		tc.a.node = domain.NodeRef{RunID: "run", TaskID: "review"}
		r, e := cli.resolve(context.Background(), tc.a)
		if e != nil {
			t.Fatal(e)
		}
		if r.Gate.Source != tc.source {
			t.Fatalf("%+v", r.Gate)
		}
	}
}
func TestCampaignFixCountsRoundsAndEscalates(t *testing.T) {
	for _, tc := range []struct {
		used, limit, want int
		fail              bool
	}{{0, 4, 2, false}, {2, 4, 2, false}, {3, 4, 1, false}, {4, 4, 0, true}, {4, 6, 2, false}} {
		declared, e := fixRounds(tc.used, tc.limit, &domain.ReviewVerdict{Verdict: "changes-requested", FindingTitles: []string{"repair"}}, "run/review")
		if (e != nil) != tc.fail || declared != tc.want {
			t.Fatalf("%+v got %d %v", tc, declared, e)
		}
		if tc.fail && !strings.Contains(e.Error(), "review-round-limit-exhausted") {
			t.Fatal(e)
		}
	}
}
func TestCampaignFixDryRunWritesOnly(t *testing.T) {
	cli, out := fixCommandFixture(t)
	dir := filepath.Join(t.TempDir(), "generated")
	if e := cli.run(context.Background(), []string{"run/review", "--idempotency-key", "key", "--out", dir, "--dry-run", "--no-notify"}); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out.String(), "dry-run") {
		t.Fatal(out.String())
	}
	if _, e := campaign.Prepare(dir, cli.limits); e != nil {
		t.Fatal(e)
	}
}
