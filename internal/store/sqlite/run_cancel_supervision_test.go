package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Settlement revokes every supervision capability, release included, so a hold
// still active when the sink settled could never be released by anyone, and
// triage listed four of them on the fleet as "still recorded active on a
// settled run". The settlement transaction now releases them itself.
func TestSinkSettlementReleasesTheRunsActiveHolds(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	ctx := context.Background()
	now := supervisionTestTime
	run := domain.WorkflowRun{
		ID: "run-1", WorkflowID: "workflow-1", GraphRevision: 1,
		Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	tasks := []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "only", Class: domain.TaskClassRequired}}
	bound, err := domain.BindRunSink(run, tasks)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{bound}, Tasks: tasks,
		Attempts: []domain.Attempt{{
			ID: "attempt-1", WorkflowRunID: run.ID, TaskID: "task-1", Number: 1, Revision: 1,
			Progress: domain.ProgressSucceeded, Control: domain.ControlStopped, UpdatedAt: now,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSupervision(ctx, SupervisionMaterialization{
		Record: domain.SupervisionRecord{RunID: run.ID, Config: supervisionTestConfig()},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PlaceHold(ctx, supervisionRunHold("hold-1", "request-hold-1", "look first")); err != nil {
		t.Fatal(err)
	}
	before := loadSupervisionProjectionFixture(t, store, run.ID)
	settled, err := domain.ProjectRunSink(before.Run, before.Tasks, before.Attempts, before.Assignments, now)
	if err != nil {
		t.Fatal(err)
	}
	if !settled.Sink.Progress.Terminal() {
		t.Fatalf("sink = %#v, want it settling", settled.Sink)
	}
	settled.Revision++
	settled.UpdatedAt = now
	if err := store.CommitWorkflowProjection(ctx, before, settled, before.Attempts, now); err != nil {
		t.Fatal(err)
	}
	read, err := store.SupervisionReadSet(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Holds) != 1 || read.Holds[0].State != domain.HoldReleased || read.Holds[0].ReleasedAt == nil {
		t.Fatalf("holds = %#v, want the hold released with the settlement", read.Holds)
	}
}
