package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func fixLineageFixture(t *testing.T, used int, noOp bool) campaignFixCLI {
	t.Helper()
	cli, _ := fixCommandFixture(t)
	oldDetail, oldOpen := cli.detail, cli.open
	lineage := campaign.FixLineage{Schema: "steward.fix-lineage/v1", RootRun: "root", RootProducingTask: "implement", RootReviewTask: "review", RoundLimit: 4, RoundsUsedBefore: used, RoundsDeclared: 2, Gate: &domain.TaskGate{Commands: []string{"true"}, Timeout: time.Minute}}
	raw, _ := json.Marshal(lineage)
	cli.detail = func(ctx context.Context, s string) (backlogadmin.WorkflowDetail, error) {
		d, e := oldDetail(ctx, s)
		d.Summary.Workflow.InputArtifactIDs = []string{"lineage", "brief"}
		d.Artifacts = []backlogadmin.Artifact{{Metadata: backlogadmin.ArtifactMetadata{ID: "lineage", Name: "inputs/fix/lineage.json"}}, {Metadata: backlogadmin.ArtifactMetadata{ID: "brief", Name: "inputs/fix/brief/prompt.md"}}}
		verdict := "changes-requested"
		if noOp {
			verdict = "accept"
		}
		d.Tasks = append(d.Tasks,
			backlogadmin.TaskDetail{Task: domain.Task{Name: "fix1"}, Attempt: &domain.Attempt{Progress: domain.ProgressSucceeded}},
			backlogadmin.TaskDetail{Task: domain.Task{Name: "fix2"}, Attempt: &domain.Attempt{Progress: domain.ProgressSucceeded}},
			backlogadmin.TaskDetail{Task: domain.Task{Name: "review2"}, Attempt: &domain.Attempt{Progress: domain.ProgressSucceeded, ReviewVerdict: &domain.ReviewVerdict{Verdict: verdict}}},
		)
		return d, e
	}
	cli.open = func(ctx context.Context, id string) (backlogadmin.ArtifactContent, error) {
		switch id {
		case "lineage":
			return backlogadmin.ArtifactContent{Content: io.NopCloser(bytes.NewReader(raw))}, nil
		case "brief":
			return backlogadmin.ArtifactContent{Content: io.NopCloser(strings.NewReader("original carried brief"))}, nil
		}
		return oldOpen(ctx, id)
	}
	return cli
}
func TestCampaignFixLineageRoundsAndNoOp(t *testing.T) {
	for _, tc := range []struct {
		used                   int
		noOp                   bool
		wantUsed, wantDeclared int
		fail                   bool
	}{{0, false, 2, 2, false}, {2, false, 4, 0, true}, {2, true, 3, 1, false}} {
		cli := fixLineageFixture(t, tc.used, tc.noOp)
		r, e := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}})
		if (e != nil) != tc.fail {
			t.Fatalf("%+v: %v", tc, e)
		}
		if tc.fail {
			if !strings.Contains(e.Error(), "review-round-limit-exhausted") {
				t.Fatal(e)
			}
			continue
		}
		if r.Lineage.RoundsUsed != tc.wantUsed || r.Lineage.RoundsDeclared != tc.wantDeclared || r.Gate.Source != "lineage" {
			t.Fatalf("%+v", r)
		}
		if string(r.Options.Brief[0].Content) != "original carried brief" {
			t.Fatalf("%+v", r.Options.Brief)
		}
	}
	cli := fixLineageFixture(t, 2, false)
	r, e := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}, roundLimit: 6})
	if e != nil || r.Options.Lineage.RoundLimit != 6 || r.Lineage.RoundsDeclared != 2 {
		t.Fatalf("%+v %v", r, e)
	}
}
func TestCampaignFixOlderCoordinator(t *testing.T) {
	cli, _ := fixCommandFixture(t)
	old := cli.detail
	cli.detail = func(ctx context.Context, s string) (backlogadmin.WorkflowDetail, error) {
		d, e := old(ctx, s)
		d.Tasks[1].Attempt.ReviewVerdict = nil
		d.Tasks[1].Task.Outputs = nil
		return d, e
	}
	_, e := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}})
	if e == nil || !strings.Contains(e.Error(), "coordinator predates rc.116") {
		t.Fatal(e)
	}
}

type fixReplaySubmission struct{ archives [][]byte }

func (f *fixReplaySubmission) SubmitArchive(_ context.Context, r backlogadmin.LocalSubmissionRequest, body io.Reader, _ int64) (backlogadmin.LocalSubmissionResponse, error) {
	raw, e := io.ReadAll(body)
	if e != nil {
		return backlogadmin.LocalSubmissionResponse{}, e
	}
	f.archives = append(f.archives, raw)
	return backlogadmin.LocalSubmissionResponse{Key: r.IdempotencyKey, RunID: "new-run", WorkflowID: "new-workflow", Replay: len(f.archives) > 1, State: "accepted"}, nil
}
func armFixSubmission(t *testing.T, cli *campaignFixCLI) *fixReplaySubmission {
	t.Helper()
	f := new(fixReplaySubmission)
	cli.submissions = func() (adminSubmissionService, error) { return f, nil }
	cli.viability = func(_ context.Context, r backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error) {
		return campaignReadyMatrix(r), nil
	}
	cli.describe = func(context.Context, string) (backlogadmin.WorkflowSummary, error) {
		return backlogadmin.WorkflowSummary{Run: domain.WorkflowRun{Progress: domain.ProgressSucceeded}}, nil
	}
	return f
}
func TestCampaignFixReplayPrintsSameRun(t *testing.T) {
	cli, out := fixCommandFixture(t)
	f := armFixSubmission(t, &cli)
	args := []string{"run/review", "--idempotency-key", "same-key", "--no-notify"}
	for i := 0; i < 2; i++ {
		out.Reset()
		if e := cli.run(context.Background(), args); e != nil {
			t.Fatal(e)
		}
		if !strings.HasPrefix(out.String(), "run new-run\n") {
			t.Fatal(out.String())
		}
		if i == 1 && !strings.Contains(out.String(), "replay=true") {
			t.Fatal(out.String())
		}
	}
	if len(f.archives) != 2 || !bytes.Equal(f.archives[0], f.archives[1]) {
		t.Fatal("replay changed archive")
	}
}
func TestCampaignFixJSONDocument(t *testing.T) {
	cli, out := fixCommandFixture(t)
	armFixSubmission(t, &cli)
	if e := cli.run(context.Background(), []string{"run/review", "--idempotency-key", "key", "--no-notify", "--json"}); e != nil {
		t.Fatal(e)
	}
	var d campaignFixDocument
	if e := json.Unmarshal(out.Bytes(), &d); e != nil {
		t.Fatal(e)
	}
	if d.SchemaVersion != 1 || d.RunID != "new-run" || d.Lineage.RoundLimit != 4 || d.Verdict.Source != "recorded" || d.Submission.RunID != d.RunID {
		t.Fatalf("%+v", d)
	}
}
func TestCampaignFixArtifactFailuresAndLimits(t *testing.T) {
	cli, _ := fixCommandFixture(t)
	cli.open = func(context.Context, string) (backlogadmin.ArtifactContent, error) {
		return backlogadmin.ArtifactContent{}, errors.New("read failed")
	}
	if _, e := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}}); e == nil || !strings.Contains(e.Error(), "read failed") {
		t.Fatal(e)
	}
	cli, _ = fixCommandFixture(t)
	cli.limits.MaxBytes = 1
	if _, e := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}}); e == nil || !strings.Contains(e.Error(), "exceeds") {
		t.Fatal(e)
	}
}
