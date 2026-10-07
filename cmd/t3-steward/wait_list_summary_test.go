package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

func TestWaitListJSONCarriesTheRecordedNodeSummary(t *testing.T) {
	node := summaryTestNodeWait()
	recorded := wait.WakeSummary{Schema: "t3-steward.wake-summary/v1", Kind: "node", Headline: "recorded headline"}
	sources := waitListSources{coordinator: func(context.Context) ([]domain.NodeWait, []domain.TaskWait, error) {
		return []domain.NodeWait{node}, nil, nil
	}, summary: func(context.Context, string) (*wait.WakeSummary, error) { return &recorded, nil }}
	var out bytes.Buffer
	if err := runWaitList(context.Background(), sources, waitListOptions{all: true, asJSON: true}, &out); err != nil {
		t.Fatal(err)
	}
	var answer waitListAnswer
	if err := json.Unmarshal(out.Bytes(), &answer); err != nil {
		t.Fatal(err)
	}
	if len(answer.Rows) != 1 || answer.Rows[0].Summary == nil || !reflect.DeepEqual(*answer.Rows[0].Summary, recorded) {
		t.Fatal(out.String())
	}
	out.Reset()
	if err := runWaitList(context.Background(), sources, waitListOptions{all: true}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), " summary=\"recorded headline\"\n") {
		t.Fatal(out.String())
	}
	// No record adds no text to the old row.
	sources.summary = nil
	out.Reset()
	if err := runWaitList(context.Background(), sources, waitListOptions{all: true}, &out); err != nil {
		t.Fatal(err)
	}
	want := "nw-5d0c2f1e node \"" + summaryTestRun + "/__sink\" state=settled: met delivery=delivered thread=- registered=- deadline=-\nsources read: coordinator-held waits\n"
	if out.String() != want {
		t.Fatalf("got %q want %q", out.String(), want)
	}
}

func TestWaitSummaryPrefersTheRecordedSummary(t *testing.T) {
	node := summaryTestNodeWait()
	recorded := wait.WakeSummary{Schema: "t3-steward.wake-summary/v1", Kind: "node", Headline: "recorded"}
	sources := waitSummarySources{nodes: func(context.Context) ([]domain.NodeWait, error) { return []domain.NodeWait{node}, nil }, summary: summaryTestSource(t, summaryTestDetail(), nil), recorded: func(context.Context, string) (*wait.WakeSummary, error) { return &recorded, nil }}
	var out bytes.Buffer
	if err := runWaitSummary(context.Background(), sources, node.Request.ID, true, &out); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["source"] != "wake" || got["headline"] != "recorded" {
		t.Fatal(out.String())
	}
	sources.recorded = nil
	out.Reset()
	if err := runWaitSummary(context.Background(), sources, node.Request.ID, true, &out); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["source"] != "live" || got["headline"] == "recorded" {
		t.Fatal(out.String())
	}
	sources.recorded = func(context.Context, string) (*wait.WakeSummary, error) { return nil, errors.New("broken store") }
	out.Reset()
	if err := runWaitSummary(context.Background(), sources, node.Request.ID, true, &out); err == nil {
		t.Fatal("record read failure hidden")
	}
}

func TestRecordedNodeSummaryKVBoundAndOverwrite(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s := wait.WakeSummary{Schema: "t3-steward.wake-summary/v1", Headline: "first"}
	save, load := recordNodeSummary(store), recordedNodeSummary(store)
	if err := save(ctx, "nw-record", s); err != nil {
		t.Fatal(err)
	}
	raw, ok, err := store.GetKV(ctx, "wake-summary/nw-record")
	if err != nil || !ok || !strings.Contains(raw, "first") {
		t.Fatal(raw, ok, err)
	}
	s.Headline = "second"
	if err := save(ctx, "nw-record", s); err != nil {
		t.Fatal(err)
	}
	got, err := load(ctx, "nw-record")
	if err != nil || !reflect.DeepEqual(got, &s) {
		t.Fatal(got, err)
	}
	s.Headline = strings.Repeat("a", nodeSummaryRecordLimit)
	if err := save(ctx, "nw-record", s); err == nil {
		t.Fatal("oversized accepted")
	}
	got, err = load(ctx, "nw-record")
	if err != nil || got.Headline != "second" {
		t.Fatal(got, err)
	}
}

func TestWaitListSendsTheSameCoordinatorRequest(t *testing.T) {
	path := filepath.Join(shortTempDir(t), "state.db")
	fake := serveFakeCoordinator(t, path, func(request map[string]any) any {
		return map[string]any{"version": backlogadmin.LocalTransportVersion, "nodeWait": backlogadmin.NodeWaitResponse{}}
	})
	cfg := config.Default()
	cfg.StatePath = path
	// Exercise the production native wait-list caller with the local carrier.
	// The joined coordinator source is unchanged and uses this same list operation.
	if err := cmdNodeWait(context.Background(), cfg, []string{"list", "--native", "--json"}); err != nil {
		t.Fatal(err)
	}
	if len(fake.requests) != 2 {
		t.Fatalf("requests=%v", fake.requests)
	}
	for i, action := range []string{"list", "list-task"} {
		data, _ := json.Marshal(fake.requests[i])
		var request struct {
			NodeWait backlogadmin.NodeWaitOperation `json:"nodeWait"`
		}
		if err := json.Unmarshal(data, &request); err != nil {
			t.Fatal(err)
		}
		want, _ := json.Marshal(backlogadmin.NodeWaitOperation{Action: action})
		got, _ := json.Marshal(request.NodeWait)
		if !bytes.Equal(got, want) {
			t.Fatalf("got %s want %s envelope %s", got, want, data)
		}
	}
}

func TestWakeSummaryRc117MacosFix2Shape(t *testing.T) {
	runID := "run-" + strings.Repeat("b", 32)
	sha := "2e4fd08d71bb45e0c50a13fb06c489326ee248a4"
	detail := backlogadmin.WorkflowDetail{Summary: backlogadmin.WorkflowSummary{Run: domain.WorkflowRun{ID: runID, Progress: domain.ProgressSucceeded}, Workflow: domain.Workflow{Name: "steward-rc117-macos-fix2", TaskIDs: []string{"fix", "review"}}},
		Tasks: []backlogadmin.TaskDetail{
			{Task: domain.Task{ID: "fix", Name: "fix", Outputs: []domain.ArtifactDeclaration{{Name: "fix.bundle"}}}, Attempt: &domain.Attempt{ID: "fix-attempt", Progress: domain.ProgressSucceeded}},
			{Task: domain.Task{ID: "review", Name: "review", Outputs: []domain.ArtifactDeclaration{{Name: "review.md"}}}, Attempt: &domain.Attempt{ID: "review-attempt", Progress: domain.ProgressSucceeded}}},
		Artifacts: []backlogadmin.Artifact{
			{Metadata: backlogadmin.ArtifactMetadata{ID: "bundle", TaskID: "fix", AttemptID: "fix-attempt", Name: "fix.bundle", Kind: domain.ArtifactOutput, Size: 716077}},
			{Metadata: backlogadmin.ArtifactMetadata{ID: "review-file", TaskID: "review", AttemptID: "review-attempt", Name: "review.md", Kind: domain.ArtifactOutput, Size: 5853}}}}
	source := &coordinatorSummarySource{query: func(context.Context, backlogadmin.Query) (backlogadmin.Response, error) {
		return backlogadmin.Response{Workflow: &detail}, nil
	}, open: func(ctx context.Context, id string) (backlogadmin.ArtifactContent, error) {
		body := "VERDICT: ACCEPT\n"
		if id == "bundle" {
			body = "# v2 git bundle\n" + sha + " refs/heads/rc117\n\n" + strings.Repeat("x", 716000)
		}
		streamCtx, cancel := context.WithCancel(ctx)
		return backlogadmin.ArtifactContent{Content: &summaryAbortFixture{Reader: strings.NewReader(body), ctx: streamCtx, abort: cancel}}, nil
	}}
	node := summaryTestNodeWait()
	node.Request.Target.RunID = runID
	node.Observation.Target.RunID = runID
	node.Observation.Progress = domain.ProgressSucceeded
	summary := wait.BuildNodeSummary(context.Background(), source, node)
	if summary.Headline != "steward-rc117-macos-fix2: SUCCEEDED | review ACCEPT | head "+sha[:7] {
		t.Fatalf("%+v", summary)
	}
	if summary.Tasks[0].Verdict != "no review output" || summary.Tasks[0].Head != sha || summary.Tasks[0].HeadSource != "fix.bundle" || summary.Tasks[1].Verdict != "ACCEPT" || summary.Tasks[1].Head != "no declared commit" {
		t.Fatalf("%+v", summary.Tasks)
	}
}

// Close simulates a streaming transport abort: cancellation must precede wait.
type summaryAbortFixture struct {
	*strings.Reader
	ctx   context.Context
	abort context.CancelFunc
}

func (s *summaryAbortFixture) Close() error {
	if s.Len() > 0 {
		s.abort()
		<-s.ctx.Done()
	} else {
		s.abort()
	}
	return nil
}

var _ io.ReadCloser = (*summaryAbortFixture)(nil)

func TestSummaryRunOfCarriesReviewFields(t *testing.T) {
	detail := summaryTestDetail()
	detail.Tasks[0].Task.ReviewOutput = &domain.ReviewOutput{VerdictLine: "notes/r.md"}
	detail.Tasks[0].Attempt.ReviewVerdict = &domain.ReviewVerdict{Verdict: "changes-requested"}
	row := summaryRunOf(detail).Tasks[0]
	if row.ReviewOutput == nil || row.ReviewOutput.VerdictLine != "notes/r.md" || row.ReviewVerdict == nil || row.ReviewVerdict.Verdict != "changes-requested" {
		t.Fatalf("%+v", row)
	}
}
