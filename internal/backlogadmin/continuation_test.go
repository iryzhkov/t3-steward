package backlogadmin

import (
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func continuationViewRecords(now time.Time, snapshots ...domain.Artifact) sqlite.CoordinatorRecords {
	workflow := domain.Workflow{ID: "workflow-1", Name: "workflow", Project: "steward", TaskIDs: []string{"task-1"}, CreatedAt: now}
	run := domain.WorkflowRun{ID: "run-1", WorkflowID: workflow.ID, Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now}
	task := domain.Task{ID: "task-1", WorkflowID: workflow.ID, Name: "build", Class: domain.TaskClassRequired}
	attempt := domain.Attempt{ID: "attempt-2", WorkflowRunID: run.ID, TaskID: task.ID, Number: 2, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 1, UpdatedAt: now}
	return sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{workflow}, WorkflowRuns: []domain.WorkflowRun{run},
		Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Artifacts: snapshots,
	}
}

func continuationSnapshot(attemptID string, size int64, captured time.Time) domain.Artifact {
	return domain.Artifact{
		ID: domain.ContinuationArtifactID(attemptID), WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: attemptID,
		Kind: domain.ArtifactCheckpoint, Name: domain.ContinuationArtifactName, MediaType: "text/markdown",
		Size: size, SHA256: strings.Repeat("c", 64), Producer: "worker:normandy", CreatedAt: captured,
	}
}

// Task show and explain report the latest checkpoint's time and size, never
// its content, and say plainly when there is none.
func TestTaskDetailAndExplanationReportTheLatestCheckpoint(t *testing.T) {
	now := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	newer, older := now.Add(-5*time.Minute), now.Add(-time.Hour)
	v := newView(continuationViewRecords(now, continuationSnapshot("attempt-1", 300, newer), continuationSnapshot("attempt-0", 100, older)), nil, nil, RuntimeInfo{}, now)
	detail, ok := v.taskDetail("run-1", "task-1")
	if !ok || detail.Checkpoint == nil || detail.Checkpoint.AttemptID != "attempt-1" || detail.Checkpoint.Size != 300 || !detail.Checkpoint.CapturedAt.Equal(newer) {
		t.Fatalf("task checkpoint = %+v", detail.Checkpoint)
	}
	explanation, ok := v.explanation("run-1", "task-1")
	if !ok || explanation.Checkpoint == nil || *explanation.Checkpoint != *detail.Checkpoint {
		t.Fatalf("explanation checkpoint = %+v", explanation.Checkpoint)
	}

	empty := newView(continuationViewRecords(now), nil, nil, RuntimeInfo{}, now)
	if detail, _ := empty.taskDetail("run-1", "task-1"); detail.Checkpoint != nil {
		t.Fatalf("a task without a snapshot reported one: %+v", detail.Checkpoint)
	}
	if explanation, _ := empty.explanation("run-1", "task-1"); explanation.Checkpoint != nil {
		t.Fatalf("an explanation without a snapshot reported one: %+v", explanation.Checkpoint)
	}
}
