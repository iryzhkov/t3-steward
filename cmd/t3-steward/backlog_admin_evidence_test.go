package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// F-7: the worker's evidence on an attempt (thread, worker, observed session
// state, pause reason) travelled only in --json; the text renderers printed
// worker, provider and thread URL and nothing about why the attempt was not
// moving, although the operations guide says the pause reason reads in
// "backlog task show".
func TestRenderTaskPrintsAttemptEvidence(t *testing.T) {
	observed := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	detail := &backlogadmin.TaskDetail{
		Task:    domain.Task{ID: "task-1", Name: "analyse", WorkflowID: "workflow-1"},
		Attempt: &domain.Attempt{ID: "attempt-1", Number: 2, Revision: 7, Progress: domain.ProgressActive, Control: domain.ControlPaused},
		Assignment: &backlogadmin.Assignment{
			WorkerID: "normandy", Route: domain.ProviderRoute{ProviderInstanceID: "claudeAgent", Model: "claude-opus"},
		},
		Evidence: &backlogadmin.AttemptEvidence{
			ThreadID: "thread-abc", WorkerID: "normandy", Control: domain.ControlPaused, Phase: "running",
			ThreadState: "stopped", PauseReason: "claudeAgent/claude/seven_day at 97%", ObservedAt: observed,
		},
	}
	var out bytes.Buffer
	renderTask(&out, detail, observed)
	for _, want := range []string{
		"thread: thread-abc\n",
		"worker: normandy\n",
		// The session line names the provider session, because the words are
		// the provider's and they are about the thread, not about the task.
		// The attempt here is paused, which the words agree with, so they are
		// printed as the worker reported them.
		"provider session: thread stopped, control paused, phase running, observed 2026-09-17T12:00:00Z\n",
		"paused: claudeAgent/claude/seven_day at 97%\n",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output %q does not contain %q", out.String(), want)
		}
	}
	if strings.Count(out.String(), "worker: normandy\n") != 1 {
		t.Errorf("output %q names the worker more than once", out.String())
	}

	// Without evidence nothing about a session or a pause is printed.
	out.Reset()
	detail.Evidence = nil
	renderTask(&out, detail, observed)
	if strings.Contains(out.String(), "session:") || strings.Contains(out.String(), "paused:") {
		t.Errorf("output %q reports evidence it does not have", out.String())
	}
}

func TestRenderWorkflowMarksPausedAndParkedAttempts(t *testing.T) {
	detail := &backlogadmin.WorkflowDetail{
		Summary: backlogadmin.WorkflowSummary{
			Run:      domain.WorkflowRun{ID: "run-1", Progress: domain.ProgressActive},
			Workflow: domain.Workflow{ID: "workflow-1", Name: "rebuild", Project: "home-assistant"},
		},
		Tasks: []backlogadmin.TaskDetail{
			{
				Task:     domain.Task{ID: "task-paused", Name: "analyse"},
				Attempt:  &domain.Attempt{ID: "attempt-paused", Progress: domain.ProgressActive, Control: domain.ControlPaused},
				Evidence: &backlogadmin.AttemptEvidence{ThreadID: "t1", Control: domain.ControlPaused, PauseReason: "codex/codex/primary at 97%"},
			},
			{
				Task:     domain.Task{ID: "task-parked", Name: "nest"},
				Attempt:  &domain.Attempt{ID: "attempt-parked", Progress: domain.ProgressWaitingExternal, Control: domain.ControlWaitingExternal},
				Evidence: &backlogadmin.AttemptEvidence{ThreadID: "t2", Control: domain.ControlWaitingExternal},
			},
			{
				Task:     domain.Task{ID: "task-running", Name: "publish"},
				Attempt:  &domain.Attempt{ID: "attempt-running", Progress: domain.ProgressActive, Control: domain.ControlRunning},
				Evidence: &backlogadmin.AttemptEvidence{ThreadID: "t3", Control: domain.ControlRunning},
			},
		},
	}
	var out bytes.Buffer
	renderWorkflow(&out, detail)
	for _, want := range []string{
		"  analyse (task-paused): active paused attempt=attempt-paused paused\n",
		"  nest (task-parked): waiting-external waiting-external attempt=attempt-parked parked\n",
		"  publish (task-running): active running attempt=attempt-running\n",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output %q does not contain %q", out.String(), want)
		}
	}
}
