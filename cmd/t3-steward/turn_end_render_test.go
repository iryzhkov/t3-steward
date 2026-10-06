package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// "backlog task show" says what a worker holding an ended turn waits for.
func TestRenderTaskPrintsTurnEndState(t *testing.T) {
	observed := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	note := "waiting for 2 background commands: sh -c make check-review, sleep 300 (nudge 1 of 2)"
	detail := &backlogadmin.TaskDetail{
		Task:     domain.Task{ID: "task-1", Name: "implement", WorkflowID: "workflow-1"},
		Attempt:  &domain.Attempt{ID: "attempt-1", Number: 1, Revision: 3, Progress: domain.ProgressActive, Control: domain.ControlRunning},
		Evidence: &backlogadmin.AttemptEvidence{ThreadID: "thread-1", Phase: "stopped", ThreadState: "stopped", TurnEnd: note, ObservedAt: observed},
	}
	var out bytes.Buffer
	renderTask(&out, detail, observed)
	if !strings.Contains(out.String(), "turn end: "+note+"\n") {
		t.Fatalf("output %q does not show the turn-end state", out.String())
	}
}
