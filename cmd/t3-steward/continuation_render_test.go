package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func testCheckpoint() *backlogadmin.ContinuationCheckpoint {
	return &backlogadmin.ContinuationCheckpoint{
		AttemptID: "attempt-1", ArtifactID: "continuation-attempt-1", Size: 4096,
		CapturedAt: time.Date(2026, 10, 6, 9, 30, 0, 0, time.UTC),
	}
}

func TestExplainShowsTheCheckpointTimeAndSizeOrNone(t *testing.T) {
	var out bytes.Buffer
	renderExplanation(&out, &backlogadmin.Explanation{WorkflowRunID: "run-1", TaskID: "task-1", Summary: "running", Checkpoint: testCheckpoint()})
	if !strings.Contains(out.String(), "checkpoint: continuation.md 4096 bytes captured 2026-10-06T09:30:00Z by attempt-1") {
		t.Fatalf("explain output:\n%s", out.String())
	}
	out.Reset()
	renderExplanation(&out, &backlogadmin.Explanation{WorkflowRunID: "run-1", TaskID: "task-1", Summary: "running"})
	if !strings.Contains(out.String(), "checkpoint: no checkpoint") {
		t.Fatalf("explain output without a checkpoint:\n%s", out.String())
	}
}

func TestTaskShowAndCampaignShowReportTheCheckpoint(t *testing.T) {
	task := backlogadmin.TaskDetail{
		Task:       domain.Task{ID: "task-1", Name: "build", WorkflowID: "workflow-1"},
		Attempt:    &domain.Attempt{ID: "attempt-2", Number: 2, Progress: domain.ProgressActive, Control: domain.ControlRunning},
		Checkpoint: testCheckpoint(),
	}
	var out bytes.Buffer
	renderTask(&out, &task, time.Time{})
	if !strings.Contains(out.String(), "checkpoint: continuation.md 4096 bytes captured 2026-10-06T09:30:00Z by attempt-1") {
		t.Fatalf("task show output:\n%s", out.String())
	}
	out.Reset()
	renderWorkflow(&out, &backlogadmin.WorkflowDetail{Tasks: []backlogadmin.TaskDetail{task}})
	if !strings.Contains(out.String(), "    checkpoint: continuation.md 4096 bytes captured 2026-10-06T09:30:00Z by attempt-1") {
		t.Fatalf("campaign show output:\n%s", out.String())
	}
	task.Checkpoint = nil
	out.Reset()
	renderTask(&out, &task, time.Time{})
	if !strings.Contains(out.String(), "checkpoint: no checkpoint") {
		t.Fatalf("task show output without a checkpoint:\n%s", out.String())
	}
}

func TestTaskResultReportsTheCheckpoint(t *testing.T) {
	document := taskResultDocument{Run: "run-1", Outcome: "failed", Directory: t.TempDir(), Tasks: []taskResultTask{
		{Task: "build", Progress: "failed", Checkpoint: testCheckpoint()},
		{Task: "lint", Progress: "succeeded"},
	}}
	var out bytes.Buffer
	if err := renderTaskResult(&out, document); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if !strings.Contains(text, "  checkpoint: continuation.md 4096 bytes captured 2026-10-06T09:30:00Z by attempt-1") ||
		!strings.Contains(text, "  checkpoint: no checkpoint") {
		t.Fatalf("task result output:\n%s", text)
	}
}
