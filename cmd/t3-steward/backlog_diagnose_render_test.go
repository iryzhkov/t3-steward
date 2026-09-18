package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// diagnoseFixture gives the run revision and the graph revision different
// values, so the text renderer's revision line is proven to print the graph
// revision the diagnosis was taken at, not the run's own revision.
func diagnoseFixture(now time.Time) *backlogadmin.Diagnosis {
	return &backlogadmin.Diagnosis{
		GraphRevision: 3, GeneratedAt: now,
		Workflow: backlogadmin.WorkflowDetail{
			Summary: backlogadmin.WorkflowSummary{
				Run:      domain.WorkflowRun{ID: "run-1", Progress: domain.ProgressActive, Revision: 7},
				Workflow: domain.Workflow{ID: "workflow-1", Name: "rebuild", Project: "home-assistant"},
			},
			Tasks: []backlogadmin.TaskDetail{
				{Task: domain.Task{ID: "task-a", Name: "analyse"}, Attempt: &domain.Attempt{ID: "attempt-a", Progress: domain.ProgressActive, Control: domain.ControlRunning}},
				{Task: domain.Task{ID: "task-b", Name: "nest"}, Attempt: &domain.Attempt{ID: "attempt-b", Progress: domain.ProgressWaitingExternal, Control: domain.ControlWaitingExternal}},
				{Task: domain.Task{ID: "task-c", Name: "publish"}},
			},
		},
		TaskWaits: []domain.TaskWait{
			{ID: "tw-1", TaskID: "task-b", AttemptID: "attempt-b", Name: "nest answered", Condition: "jocasta exists x.md", RegisteredAt: now, Deadline: now.Add(time.Hour)},
			{ID: "tw-0", TaskID: "task-a", AttemptID: "attempt-a", Name: "old", RegisteredAt: now.Add(-2 * time.Hour), Deadline: now, SettledAt: &now, Result: &domain.TaskWaitResult{Outcome: domain.TaskWaitMet}},
		},
		Waits: []domain.NodeWait{{
			Request: domain.NodeWaitRequest{ID: "nw-1", ThreadID: "thread-x", Name: "until analyse", Target: domain.NodeRef{RunID: "run-1", TaskID: "task-a"}},
			Host:    "omarchy-pc", Deadline: now.Add(2 * time.Hour), Delivery: "pending",
		}},
		Workers: []backlogadmin.DiagnosticWorker{{
			WorkerID: "normandy", WorkerEpoch: "e1", Sequence: 9, ObservedAt: now,
			Assignments: []domain.WorkerAssignmentObservation{{AssignmentID: "asg-a", State: domain.AssignmentClaimed, Control: domain.ControlRunning, ThreadID: "thread-a"}},
		}},
		Unavailable: []string{"worker journal excerpt for asg-b"},
	}
}

// F-18: diagnose without --json printed the JSON document; it now prints a
// short summary, and --json keeps the document exactly as before.
func TestBacklogDiagnoseTextSummary(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	fake := &fakeAdminQueryService{response: backlogadmin.Response{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryDiagnose, GeneratedAt: now, Diagnosis: diagnoseFixture(now),
	}}
	var stdout bytes.Buffer
	cli := backlogAdminCLI{service: fake, principal: backlogadmin.Principal{ID: "operator", Roles: []string{"local-admin"}}, stdout: &stdout}
	if err := cli.runBacklog(context.Background(), []string{"diagnose", "run-1"}); err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(stdout.String(), "{") {
		t.Fatalf("diagnose without --json printed JSON:\n%s", stdout.String())
	}
	for _, want := range []string{
		"run: run-1\n",
		"revision: 3\n",
		"  analyse (task-a): active running attempt=attempt-a\n",
		"  nest (task-b): waiting-external waiting-external attempt=attempt-b\n",
		"  publish (task-c): queued  attempt=-\n",
		"task waits:\n",
		"  tw-1 task=task-b attempt=attempt-b \"nest answered\": jocasta exists x.md (deadline 2026-09-17T13:00:00Z)\n",
		"node waits:\n",
		"  nw-1 \"until analyse\" thread=thread-x host=omarchy-pc delivery=pending (deadline 2026-09-17T14:00:00Z)\n",
		"workers:\n",
		"  normandy epoch=e1 sequence=9 observed=2026-09-17T12:00:00Z\n",
		"    asg-a claimed running thread=thread-a\n",
		"unavailable:\n",
		"  worker journal excerpt for asg-b\n",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("output does not contain %q:\n%s", want, stdout.String())
		}
	}
	// A settled task wait no longer parks anything and is not a live wait.
	if strings.Contains(stdout.String(), "tw-0") {
		t.Errorf("output lists the settled wait tw-0:\n%s", stdout.String())
	}
	// The revision line is the graph revision (3), not the run revision (7).
	if strings.Contains(stdout.String(), "revision: 7\n") {
		t.Errorf("output prints the run revision where the graph revision belongs:\n%s", stdout.String())
	}
}

func TestBacklogDiagnoseJSONIsTheDocument(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	response := backlogadmin.Response{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryDiagnose, GeneratedAt: now, Diagnosis: diagnoseFixture(now),
	}
	fake := &fakeAdminQueryService{response: response}
	var stdout bytes.Buffer
	cli := backlogAdminCLI{service: fake, principal: backlogadmin.Principal{ID: "operator", Roles: []string{"local-admin"}}, stdout: &stdout}
	if err := cli.runBacklog(context.Background(), []string{"diagnose", "run-1", "--json"}); err != nil {
		t.Fatal(err)
	}
	var want bytes.Buffer
	encoder := json.NewEncoder(&want)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(response); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != want.String() {
		t.Fatalf("diagnose --json changed:\n%s\nwant:\n%s", stdout.String(), want.String())
	}
}
