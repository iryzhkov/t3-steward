package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Inside a task, the parent a submission names is the task's own identity, read
// the way `--task current` reads it; outside one there is none, and an
// unusable identity makes the submission root work rather than failing it.
func TestSubmissionParentComesFromTheTaskIdentity(t *testing.T) {
	writeIdentityFile(t, injectedIdentity(), 0o600)
	parent := submissionParentFrom(noEnvironment, "")
	if parent == nil || *parent != (domain.SubmissionParent{RunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1"}) {
		t.Fatalf("parent = %+v, want the task's own run, task and attempt", parent)
	}
	t.Chdir(t.TempDir())
	if parent := submissionParentFrom(noEnvironment, ""); parent != nil {
		t.Fatalf("parent outside a task = %+v, want none", parent)
	}
	writeIdentityFile(t, injectedIdentity(), 0o644)
	if parent := submissionParentFrom(noEnvironment, ""); parent != nil {
		t.Fatalf("parent from a non-private identity file = %+v, want none", parent)
	}
}

// task run claims the task it runs inside as the parent of the run it starts.
func TestTaskRunSubmitsItsParentTask(t *testing.T) {
	parent := domain.SubmissionParent{RunID: "parent-run", TaskID: "parent-task", AttemptID: "parent-attempt"}
	previous := submissionParentLookup
	submissionParentLookup = func() *domain.SubmissionParent { copied := parent; return &copied }
	t.Cleanup(func() { submissionParentLookup = previous })
	h := newTaskRunHarness()
	if err := h.run("--model", "claude-haiku-4-5", "--json", "--", "probe the fleet"); err != nil {
		t.Fatal(err)
	}
	if len(h.requests) != 1 || h.requests[0].Parent == nil || *h.requests[0].Parent != parent {
		t.Fatalf("submission requests = %+v, want the parent task claimed", h.requests)
	}
}

// A coordinator from before lineage refuses the unknown parent field before it
// records anything; the submission is sent again without the claim and is
// accepted as root work. Any other refusal is returned unchanged.
func TestSubmissionFallsBackWithoutParentForAnOlderCoordinator(t *testing.T) {
	parent := &domain.SubmissionParent{RunID: "r", TaskID: "t", AttemptID: "a"}
	var seen []backlogadmin.LocalSubmissionRequest
	older := submissionFunc(func(_ context.Context, request backlogadmin.LocalSubmissionRequest, body io.Reader, size int64) (backlogadmin.LocalSubmissionResponse, error) {
		raw, err := io.ReadAll(body)
		if err != nil || int64(len(raw)) != size || string(raw) != "archive" {
			t.Fatalf("archive = %q (%d) err=%v, want the whole archive on every attempt", raw, size, err)
		}
		seen = append(seen, request)
		if request.Parent != nil {
			return backlogadmin.LocalSubmissionResponse{}, errors.New(`invalid operation envelope: json: unknown field "parent"`)
		}
		return backlogadmin.LocalSubmissionResponse{RunID: "run-1"}, nil
	})
	response, err := submitArchiveClaimingParent(context.Background(), older,
		backlogadmin.LocalSubmissionRequest{IdempotencyKey: "key", Parent: parent}, []byte("archive"))
	if err != nil || response.RunID != "run-1" || len(seen) != 2 || seen[1].Parent != nil || seen[1].IdempotencyKey != "key" {
		t.Fatalf("response = %+v err=%v requests=%+v, want one retry without the parent", response, err, seen)
	}

	seen = nil
	refusing := submissionFunc(func(_ context.Context, request backlogadmin.LocalSubmissionRequest, _ io.Reader, _ int64) (backlogadmin.LocalSubmissionResponse, error) {
		seen = append(seen, request)
		return backlogadmin.LocalSubmissionResponse{}, errors.New("submission lineage: forged")
	})
	if _, err := submitArchiveClaimingParent(context.Background(), refusing,
		backlogadmin.LocalSubmissionRequest{Parent: parent}, []byte("archive")); err == nil || len(seen) != 1 {
		t.Fatalf("err=%v requests=%d, want a refused claim returned without a retry", err, len(seen))
	}
}

// campaign show names a nested run's parent, and under a parked task the
// typed reason its settled wait has not resumed it yet.
func TestCampaignShowRendersLineageAndWakeDeferral(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	settled := now.Add(-time.Minute)
	detail := &backlogadmin.WorkflowDetail{
		Summary: backlogadmin.WorkflowSummary{
			Run: domain.WorkflowRun{ID: "child-run", Progress: domain.ProgressActive, Revision: 2, Lineage: &domain.RunLineage{
				ParentRunID: "parent-run", ParentTaskID: "parent-task", ParentAttemptID: "parent-attempt",
				ParentStartedAt: now.Add(-time.Hour), RecordedAt: now,
			}},
			Workflow: domain.Workflow{ID: "child-workflow", Name: "probe", Project: "project", Class: domain.TaskClassRequired},
		},
		Tasks: []backlogadmin.TaskDetail{{
			Task:    domain.Task{ID: "task-verify", Name: "verify"},
			Attempt: &domain.Attempt{ID: "attempt-verify", Progress: domain.ProgressWaitingExternal, Control: domain.ControlWaitingExternal},
		}},
		TaskWaits: []backlogadmin.TaskWaitDetail{{
			ID: "tw-1", TaskID: "task-verify", AttemptID: "attempt-verify", Name: "probe", Outcome: "met",
			RegisteredAt: now.Add(-time.Hour), Deadline: now.Add(time.Hour), SettledAt: &settled,
			WakeDeferral: &domain.TaskWaitWakeDeferral{
				Code: domain.WakeDeferredExecutorCapacity, Detail: "worker \"omarchy-pc\" has 0 available executor-slots",
				WorkerID: "omarchy-pc", ObservedAt: settled,
			},
		}},
	}
	var out bytes.Buffer
	renderWorkflow(&out, detail)
	for _, want := range []string{
		"parent: run parent-run task parent-task attempt parent-attempt (parent started 2026-10-08T11:00:00Z)\n",
		"      wake deferred wake-executor-capacity: worker \"omarchy-pc\" has 0 available executor-slots (since 2026-10-08T11:59:00Z)\n",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("show output lacks %q:\n%s", want, out.String())
		}
	}
	detail.Summary.Run.Lineage = nil
	detail.TaskWaits[0].WakeDeferral = nil
	out.Reset()
	renderWorkflow(&out, detail)
	if strings.Contains(out.String(), "parent:") || strings.Contains(out.String(), "wake deferred") {
		t.Fatalf("a root run with an applied wake rendered lineage or deferral:\n%s", out.String())
	}
}

// triage lists a capacity deadlock the last planning pass reported, as an
// action with the explain command, and proposes no automatic change.
func TestTriageListsCapacityDeadlocks(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	report := triageReport{Items: []triageItem{}}
	triageCapacityDeadlocks(&report, []backlogadmin.WorkflowSummary{
		{Run: domain.WorkflowRun{ID: "quiet-run", UpdatedAt: now}},
		{Run: domain.WorkflowRun{ID: "child-run", UpdatedAt: now}, CapacityDeadlocks: []backlogadmin.CapacityDeadlock{{
			TaskID: "task-probe", TaskName: "probe", AttemptID: "probe-1", HolderID: "parent-1",
			Detail: "every executor or pool slot this task can use is held by attempts waiting on this run",
		}}},
	})
	if len(report.Items) != 1 {
		t.Fatalf("items = %+v, want one deadlock", report.Items)
	}
	item := report.Items[0]
	if item.Kind != backlog.PlanningBlockerCapacityDeadlock || item.Severity != "action" || item.Run != "child-run" ||
		!strings.Contains(item.Summary, `task "probe" cannot start`) || len(item.Commands) == 0 ||
		item.Commands[0].Run != "t3-steward campaign explain child-run/probe" {
		t.Fatalf("item = %+v", item)
	}
	for _, command := range item.Commands {
		if strings.Contains(command.Run, "cancel") {
			t.Fatalf("triage proposed a destructive command for a deadlock: %q", command.Run)
		}
	}
	// A deadlock outranks every other action except a worker that is down,
	// which may be its cause.
	items := append(report.Items, triageItem{Kind: "run-stalled", Severity: "action", Subject: "a"},
		triageItem{Kind: "worker-down", Severity: "action", Subject: "z"})
	sortTriage(items)
	if items[0].Kind != "worker-down" || items[1].Kind != backlog.PlanningBlockerCapacityDeadlock {
		t.Fatalf("order = %s, %s, %s", items[0].Kind, items[1].Kind, items[2].Kind)
	}
}
