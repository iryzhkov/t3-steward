package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestReviewRegressionLateArtifactDoesNotWedgeRecoveryReplay(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	run := domain.WorkflowRun{ID: "run", WorkflowID: "workflow", GraphRevision: 1, Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now, Supervision: &domain.SupervisionRecord{RunID: "run", Config: recoveryTestSupervisionConfig()}}
	attempt := domain.Attempt{ID: "attempt", WorkflowRunID: run.ID, TaskID: "task", Number: 1, Progress: domain.ProgressFailed, Control: domain.ControlStopped, Failure: "failed", Revision: 1, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}, Attempts: []domain.Attempt{attempt}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSupervision(ctx, sqlite.SupervisionMaterialization{Record: *run.Supervision}); err != nil {
		t.Fatal(err)
	}
	coordinator := coordinatorSupervision{store: backlog.CoordinatorSupervisionStore{Store: store}}
	if err := coordinator.observeRecoveryFailures(ctx, run, now); err != nil {
		t.Fatal(err)
	}
	late := domain.Artifact{ID: "late-log", WorkflowRunID: run.ID, TaskID: attempt.TaskID, AttemptID: attempt.ID, Kind: domain.ArtifactLog, SHA256: "late", CreatedAt: now.Add(time.Minute)}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Artifacts: []domain.Artifact{late}}); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.observeRecoveryFailures(ctx, run, now.Add(time.Minute)); err != nil {
		t.Fatalf("late retained evidence wedged replay: %v", err)
	}
}
