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
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

const summaryTestRun = "run-8c2bcc2de8960f8b92f0a68567b9b46e"

// summaryTestDetail is the fixture run as the coordinator's workflow query
// answers it: two tasks, a review and a gate, and the sink.
func summaryTestDetail() backlogadmin.WorkflowDetail {
	output := func(id, task, attempt, name string, size int64) backlogadmin.Artifact {
		return backlogadmin.Artifact{Metadata: backlogadmin.ArtifactMetadata{ID: id, WorkflowRunID: summaryTestRun, TaskID: task, AttemptID: attempt, Kind: domain.ArtifactOutput, Name: name, Size: size}}
	}
	return backlogadmin.WorkflowDetail{
		Summary: backlogadmin.WorkflowSummary{Run: domain.WorkflowRun{ID: summaryTestRun, Progress: domain.ProgressFailed}, Workflow: domain.Workflow{Name: "upkeeper-fresh-host-fixchain3"}},
		Tasks: []backlogadmin.TaskDetail{
			{Task: domain.Task{ID: "task-gate", Name: "gate", Outputs: []domain.ArtifactDeclaration{{Name: "gate.log"}}},
				Attempt: &domain.Attempt{ID: "attempt-gate-2", Progress: domain.ProgressFailed, Failure: "verification command failed (1)"}},
			{Task: domain.Task{ID: "task-review", Name: "review2", Outputs: []domain.ArtifactDeclaration{{Name: "review.md"}}},
				Attempt: &domain.Attempt{ID: "attempt-review", Progress: domain.ProgressSucceeded}},
			{Sink: &domain.SinkTask{ID: "sink:" + summaryTestRun, Name: domain.SinkTaskName, Progress: domain.ProgressFailed}},
		},
		Artifacts: []backlogadmin.Artifact{
			// An earlier attempt's gate.log, which must not answer for the task.
			output("artifact-gate-1", "task-gate", "attempt-gate-1", "gate.log", 14),
			output("artifact-gate-2", "task-gate", "attempt-gate-2", "gate.log", 14),
			output("artifact-review", "task-review", "attempt-review", "review.md", 27),
		},
	}
}

func summaryTestSource(t *testing.T, detail backlogadmin.WorkflowDetail, queryErr error) *coordinatorSummarySource {
	bodies := map[string]string{
		"artifact-gate-1": "RESULT EXIT 0\n",
		"artifact-gate-2": "RESULT EXIT 2\n",
		"artifact-review": "VERDICT: CHANGES_REQUESTED\n",
	}
	return &coordinatorSummarySource{
		query: func(_ context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
			if query.Kind != backlogadmin.QueryWorkflow || query.WorkflowRunID != summaryTestRun {
				t.Errorf("unexpected query %+v", query)
			}
			if queryErr != nil {
				return backlogadmin.Response{}, queryErr
			}
			return backlogadmin.Response{Workflow: &detail}, nil
		},
		open: func(_ context.Context, id string) (backlogadmin.ArtifactContent, error) {
			body, ok := bodies[id]
			if !ok {
				return backlogadmin.ArtifactContent{}, errors.New("no such artifact")
			}
			return backlogadmin.ArtifactContent{Content: io.NopCloser(strings.NewReader(body))}, nil
		},
	}
}

func summaryTestNodeWait() domain.NodeWait {
	now := time.Date(2026, 10, 6, 21, 0, 0, 0, time.UTC)
	return domain.NodeWait{
		Request:   domain.NodeWaitRequest{ID: "nw-5d0c2f1e", Target: domain.NodeRef{RunID: summaryTestRun, TaskID: domain.SinkTaskName}},
		SettledAt: &now, Delivery: "delivered",
		Observation: &domain.NodeObservation{Target: domain.NodeRef{RunID: summaryTestRun, TaskID: "sink:" + summaryTestRun},
			Progress: domain.ProgressFailed, Outcome: domain.TaskWaitMet, ExitCode: 2, Reason: "failed"},
	}
}

func TestWaitSummaryNodeWaitJSON(t *testing.T) {
	sources := waitSummarySources{
		nodes:   func(context.Context) ([]domain.NodeWait, error) { return []domain.NodeWait{summaryTestNodeWait()}, nil },
		summary: summaryTestSource(t, summaryTestDetail(), nil),
	}
	var out bytes.Buffer
	if err := runWaitSummary(context.Background(), sources, "nw-5d0c2f1e", true, &out); err != nil {
		t.Fatal(err)
	}
	var document struct {
		Schema, Kind, Outcome, Headline, Verdict, Run, Workflow, Progress string
		Tasks                                                             []map[string]any
	}
	if err := json.Unmarshal(out.Bytes(), &document); err != nil {
		t.Fatal(err, out.String())
	}
	if document.Schema != "t3-steward.wake-summary/v1" || document.Kind != "node" || document.Outcome != "met" || document.Run != summaryTestRun ||
		document.Headline != "upkeeper-fresh-host-fixchain3: FAILED at gate (RESULT EXIT 2) | review2 CHANGES_REQUESTED" || document.Verdict != "changes-requested" {
		t.Fatalf("document = %s", out.String())
	}
	if len(document.Tasks) != 2 || document.Tasks[0]["gate"] != "RESULT EXIT 2" || document.Tasks[1]["verdict"] != "CHANGES_REQUESTED" {
		t.Fatalf("tasks = %v", document.Tasks)
	}
	out.Reset()
	if err := runWaitSummary(context.Background(), sources, "nw-5d0c2f1e", false, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), document.Headline+"\n") || !strings.Contains(out.String(), "RESULT EXIT 2") {
		t.Fatalf("text = %s", out.String())
	}
}

func TestWaitSummaryNodeWaitUnavailableStillPrints(t *testing.T) {
	sources := waitSummarySources{
		nodes: func(context.Context) ([]domain.NodeWait, error) { return []domain.NodeWait{summaryTestNodeWait()}, nil },
		summary: summaryTestSource(t, summaryTestDetail(), &backlogadmin.TransportError{Class: backlogadmin.ClassTimeout,
			Err: errors.New("PRIVATE remote detail")}),
	}
	var out bytes.Buffer
	err := runWaitSummary(context.Background(), sources, "nw-5d0c2f1e", true, &out)
	var printed documentPrinted
	if err == nil || !errors.As(err, &printed) {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(out.String(), `"unavailable": "timeout"`) || strings.Contains(out.String(), "PRIVATE") {
		t.Fatal(out.String())
	}
}

func TestWaitSummaryLocalGitHubWait(t *testing.T) {
	warnings := 11
	stored := &wait.WakeSummary{Schema: wait.WakeSummarySchema, Kind: "github", Headline: "run 1 success | failures 0 | warnings 11 (11 known noise) | annotations complete",
		State: "complete", Counts: &wait.WakeSummaryCounts{Warning: &warnings}, Noise: []wait.WakeSummaryNoise{{Class: "cache restore", Level: "warning", Count: 11, Checks: 6}}}
	sources := waitSummarySources{local: func(context.Context) ([]wait.Wait, error) {
		return []wait.Wait{{ID: "w-plain", Kind: domain.WaitKindShell}, {ID: "w-ci", Kind: domain.WaitKindGitHub, Status: wait.StatusMet, Summary: stored}}, nil
	}}
	var out bytes.Buffer
	if err := runWaitSummary(context.Background(), sources, "w-ci", true, &out); err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(out.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	counts := document["counts"].(map[string]any)
	if document["schema"] != wait.WakeSummarySchema || document["kind"] != "github" || document["state"] != "complete" ||
		counts["warning"] != float64(11) || counts["failure"] != nil {
		t.Fatal(out.String())
	}
	for _, id := range []string{"w-plain", "w-missing", "tw-1"} {
		if err := runWaitSummary(context.Background(), sources, id, true, io.Discard); err == nil {
			t.Errorf("%s: no error", id)
		}
	}
	if err := runWaitSummary(context.Background(), sources, "nw-1", true, io.Discard); err == nil || !strings.Contains(err.Error(), "no coordinator configured") {
		t.Errorf("node wait without a coordinator: %v", err)
	}
}

func TestParseWaitSummaryArgs(t *testing.T) {
	for _, tc := range []struct {
		args []string
		id   string
		json bool
		ok   bool
	}{
		{[]string{"nw-1"}, "nw-1", false, true},
		{[]string{"nw-1", "--json"}, "nw-1", true, true},
		{[]string{"--json", "w-1"}, "w-1", true, true},
		{nil, "", false, false},
		{[]string{"a", "b"}, "", false, false},
		{[]string{"a", "--bogus"}, "", false, false},
	} {
		id, asJSON, err := parseWaitSummaryArgs(tc.args)
		if (err == nil) != tc.ok || (tc.ok && (id != tc.id || asJSON != tc.json)) {
			t.Errorf("%v: %q %v %v", tc.args, id, asJSON, err)
		}
	}
}

func TestSummaryRunOfKeepsTheLatestAttemptAndOlderCoordinators(t *testing.T) {
	run := summaryRunOf(summaryTestDetail())
	if len(run.Tasks) != 3 || !run.Tasks[2].Sink || run.Tasks[2].ID != "sink:"+summaryTestRun || run.Workflow != "upkeeper-fresh-host-fixchain3" {
		t.Fatalf("run = %+v", run)
	}
	gate := run.Tasks[0]
	if len(gate.Artifacts) != 1 || gate.Artifacts[0].ID != "artifact-gate-2" || gate.Attempt != "attempt-gate-2" || gate.Failure == "" {
		t.Fatalf("gate = %+v", gate)
	}
	// A coordinator that predates artifact listing answers without them; the
	// declarations survive, so the summary shows ? rather than "none".
	older := summaryTestDetail()
	older.Artifacts = nil
	run = summaryRunOf(older)
	if len(run.Tasks[1].Artifacts) != 0 || len(run.Tasks[1].Outputs) != 1 {
		t.Fatalf("older = %+v", run.Tasks[1])
	}
	s := wait.BuildNodeSummary(context.Background(), summaryTestSource(t, older, nil), summaryTestNodeWait())
	if s.Unavailable != "" || s.Tasks[1].Verdict != "?" || s.Verdict != "" {
		t.Fatalf("older coordinator summary = %+v", s)
	}
}

// The workflow detail lists tasks by name; the summary's rows and its "last
// review" follow the manifest instead (self-review lens 3).
func TestSummaryRunOfFollowsTheManifestOrder(t *testing.T) {
	detail := summaryTestDetail()
	detail.Tasks = append(detail.Tasks, backlogadmin.TaskDetail{
		Task:    domain.Task{ID: "task-early-review", Name: "areview", Outputs: []domain.ArtifactDeclaration{{Name: "review.md"}}},
		Attempt: &domain.Attempt{ID: "attempt-early", Progress: domain.ProgressSucceeded},
	})
	detail.Artifacts = append(detail.Artifacts, backlogadmin.Artifact{Metadata: backlogadmin.ArtifactMetadata{ID: "artifact-early",
		TaskID: "task-early-review", AttemptID: "attempt-early", Kind: domain.ArtifactOutput, Name: "review.md", Size: 16}})
	// Manifest: areview, gate, review2. By name the detail already reads
	// areview, gate, review2, so the manifest is reversed to tell them apart.
	detail.Summary.Workflow.TaskIDs = []string{"task-review", "task-gate", "task-early-review"}
	run := summaryRunOf(detail)
	var names []string
	for _, task := range run.Tasks {
		names = append(names, task.Name)
	}
	if strings.Join(names, ",") != "review2,gate,areview,"+domain.SinkTaskName {
		t.Fatalf("order = %v", names)
	}
	source := summaryTestSource(t, detail, nil)
	bodies := source.open
	source.open = func(ctx context.Context, id string) (backlogadmin.ArtifactContent, error) {
		if id == "artifact-early" {
			return backlogadmin.ArtifactContent{Content: io.NopCloser(strings.NewReader("VERDICT: ACCEPT\n"))}, nil
		}
		return bodies(ctx, id)
	}
	s := wait.BuildNodeSummary(context.Background(), source, summaryTestNodeWait())
	if s.Verdict != "accept" || !strings.HasSuffix(s.Headline, "| areview ACCEPT") {
		t.Fatalf("the last review in manifest order is areview: %q %q", s.Headline, s.Verdict)
	}
}

func TestWaitListJSONCarriesTheGitHubSummary(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	stored := &wait.WakeSummary{Schema: wait.WakeSummarySchema, Kind: "github", Headline: "run 1 success | failures 0 | warnings 0 (0 known noise) | annotations complete", State: "complete"}
	sources := waitListSources{host: "here", local: func(context.Context, string) ([]wait.Wait, error) {
		return []wait.Wait{
			{ID: "w-ci", ThreadID: "t", Name: "ci", Kind: domain.WaitKindGitHub, Status: wait.StatusMet, CreatedAt: now, SettledAt: &now, Summary: stored},
			{ID: "w-shell", ThreadID: "t", Name: "sh", Command: []string{"true"}, Status: wait.StatusWaiting, CreatedAt: now},
		}, nil
	}}
	var out bytes.Buffer
	if err := runWaitList(context.Background(), sources, waitListOptions{all: true, asJSON: true}, &out); err != nil {
		t.Fatal(err)
	}
	var answer struct {
		Waits []map[string]any `json:"waits"`
	}
	if err := json.Unmarshal(out.Bytes(), &answer); err != nil {
		t.Fatal(err)
	}
	for _, row := range answer.Waits {
		summary, has := row["summary"].(map[string]any)
		switch row["id"] {
		case "w-ci":
			if !has || summary["headline"] != stored.Headline || summary["schema"] != wait.WakeSummarySchema {
				t.Fatalf("github row = %v", row)
			}
		case "w-shell":
			if _, present := row["summary"]; present {
				t.Fatalf("shell row gained a summary key: %v", row)
			}
		}
	}
	if len(answer.Waits) != 2 {
		t.Fatal(out.String())
	}
}
