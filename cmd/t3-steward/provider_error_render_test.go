package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

var providerErrorObserved = time.Date(2026, 10, 7, 5, 40, 0, 0, time.UTC)

func resumingProviderError(state domain.ProviderResumeState) *domain.WorkerProviderError {
	resumeAt := providerErrorObserved.Add(time.Minute)
	return &domain.WorkerProviderError{
		Kind: domain.ProviderErrorCapacity, Detail: "Selected model is at capacity", TurnID: "turn-1",
		ObservedAt: providerErrorObserved, State: state, Resumes: 1, Budget: 3, Errors: 1, ResumeAt: &resumeAt,
		WaitReason: "quota-closed: pool codex-main admission is closed",
	}
}

func providerErrorTask() backlogadmin.TaskDetail {
	return backlogadmin.TaskDetail{
		Task:    domain.Task{ID: "task-1", Name: "implement", WorkflowID: "workflow-1"},
		Attempt: &domain.Attempt{ID: "attempt-1", Number: 1, Revision: 3, Progress: domain.ProgressActive, Control: domain.ControlRunning},
		Evidence: &backlogadmin.AttemptEvidence{ThreadID: "thread-1", Phase: "stopped", ThreadState: "stopped",
			ProviderError: resumingProviderError(domain.ProviderResumeScheduled), ObservedAt: providerErrorObserved},
	}
}

// "task show" and "campaign show" print the provider error the worker is
// resuming from, so a held attempt is not read as running work.
func TestRenderTaskAndCampaignShowPrintTheProviderError(t *testing.T) {
	task := providerErrorTask()
	want := "provider error: capacity (infrastructure): Selected model is at capacity; resume 1 of 3 of the same session at 2026-10-07T05:41:00Z"
	var out bytes.Buffer
	renderTask(&out, &task, providerErrorObserved)
	if !strings.Contains(out.String(), want+"\n") {
		t.Fatalf("task show output %q does not show %q", out.String(), want)
	}
	out.Reset()
	renderWorkflow(&out, &backlogadmin.WorkflowDetail{
		Summary: backlogadmin.WorkflowSummary{Run: domain.WorkflowRun{ID: "run-1", Progress: domain.ProgressActive}, Workflow: domain.Workflow{ID: "workflow-1", Name: "fixes"}},
		Tasks:   []backlogadmin.TaskDetail{task},
	})
	if !strings.Contains(out.String(), "    "+want+"\n") {
		t.Fatalf("campaign show output %q does not show %q", out.String(), want)
	}
}

// "task result" carries the provider error in its document and prints it.
func TestTaskResultPrintsTheProviderError(t *testing.T) {
	document := taskResultDocument{Run: "run-1", Outcome: "failed", Directory: t.TempDir(), Tasks: []taskResultTask{{
		Task: "implement", Progress: "failed", Directory: t.TempDir(), Files: []taskResultFile{},
		Failure:       "infrastructure: provider error (capacity) after 3 of 3 in-session resumes: at capacity",
		ProviderError: resumingProviderError(domain.ProviderResumeExhausted),
	}}}
	var out bytes.Buffer
	if err := renderTaskResult(&out, document); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "  provider error: capacity (infrastructure): Selected model is at capacity; every resume was spent (1 of 3), the attempt failed\n") {
		t.Fatalf("task result output %q", out.String())
	}
}

// Triage lists every attempt a worker still holds whose session ended with a
// provider error, with the worker and thread, and leaves out the ones that
// recovered or failed. A resume waiting for quota needs an operator.
func TestTriageReportsRunningAttemptsWhoseSessionEndedWithAnError(t *testing.T) {
	observation := func(id string, providerError *domain.WorkerProviderError) domain.WorkerAssignmentObservation {
		return domain.WorkerAssignmentObservation{AssignmentID: id, AssignmentEpoch: 1, State: domain.AssignmentClaimed,
			Control: domain.ControlRunning, ThreadID: "thread-" + id, ObservedAt: providerErrorObserved,
			Journal: &domain.WorkerJournalExcerpt{Phase: "stopped", ProviderError: providerError}}
	}
	workers := []backlogadmin.Worker{{Snapshot: domain.WorkerSnapshot{WorkerID: "agent-a", Assignments: []domain.WorkerAssignmentObservation{
		observation("scheduled", resumingProviderError(domain.ProviderResumeScheduled)),
		observation("quota", resumingProviderError(domain.ProviderResumeQuotaWait)),
		observation("sent", resumingProviderError(domain.ProviderResumeSent)),
		observation("recovered", resumingProviderError(domain.ProviderResumeRecovered)),
		observation("exhausted", resumingProviderError(domain.ProviderResumeExhausted)),
		observation("clean", nil),
	}}}}
	report := triageReport{Items: []triageItem{}}
	triageProviderErrors(&report, workers)
	sortTriage(report.Items)
	if len(report.Items) != 3 {
		t.Fatalf("items = %+v", report.Items)
	}
	if first := report.Items[0]; first.Subject != "quota" || first.Severity != "action" || first.Kind != "provider-error" {
		t.Fatalf("first item = %+v, want the quota wait as an action", first)
	}
	for _, item := range report.Items {
		if !strings.Contains(item.Summary, "agent-a") || !strings.Contains(item.Summary, "thread-"+item.Subject) ||
			!strings.Contains(item.Summary, "capacity (infrastructure)") || item.Since == nil || len(item.Commands) == 0 {
			t.Fatalf("item = %+v", item)
		}
		if item.Commands[0].Host != "agent-a" || !strings.Contains(item.Commands[0].Run, item.Subject) {
			t.Fatalf("commands = %+v", item.Commands)
		}
	}
	var out bytes.Buffer
	if err := renderTriage(&out, report); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "provider-error") {
		t.Fatalf("rendered triage %q", out.String())
	}
}
