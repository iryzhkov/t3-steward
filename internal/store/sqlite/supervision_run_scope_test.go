package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// seedNamedSupervisedRun writes one supervised run whose tasks and gate carry
// the given prefix, so two of them can exist side by side in the same store.
func seedNamedSupervisedRun(t *testing.T, store *Store, runID string, gate domain.Gate) {
	t.Helper()
	ctx := context.Background()
	now := supervisionTestTime
	workflowID := "workflow-" + runID
	producer := runID + "-task-producer"
	protected := runID + "-task-protected"
	run := domain.WorkflowRun{
		ID: runID, WorkflowID: workflowID, GraphRevision: 1,
		Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	tasks := []domain.Task{
		{ID: producer, WorkflowID: workflowID, Name: "producer", Class: domain.TaskClassRequired},
		{ID: protected, WorkflowID: workflowID, Name: "protected", Class: domain.TaskClassRequired, Needs: []string{producer}},
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: workflowID, Version: 1, Name: "workflow", Class: domain.TaskClassRequired,
			TaskIDs: []string{producer, protected}, CreatedAt: now,
		}},
		WorkflowRuns: []domain.WorkflowRun{run},
		Tasks:        tasks,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSupervision(ctx, SupervisionMaterialization{
		Record: domain.SupervisionRecord{RunID: runID, Config: supervisionTestConfig()},
		Gates:  []domain.Gate{gate},
	}); err != nil {
		t.Fatalf("materialize supervision of %q: %v", runID, err)
	}
}

func runScopedGate(runID string) domain.Gate {
	return domain.Gate{
		RunID: runID, State: domain.GatePendingEvidence, GraphRevision: 1,
		Definition: domain.GateDefinition{
			ID: runID + ":review", Name: "review",
			ObservedTaskIDs:  []string{runID + "-task-producer"},
			ProtectedTaskIDs: []string{runID + "-task-protected"},
		},
	}
}

// Two supervised runs declaring a gate of the same authored name must not share
// a gate row. Before the run-scoped gate identity, the second run's insert
// rewrote the first run's row in place, so the first run's protected set named
// tasks of a different run and its accepted decision was lost.
func TestSupervisionGatesOfTwoRunsWithTheSameGateNameStaySeparate(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	ctx := context.Background()
	seedNamedSupervisedRun(t, store, "run-a", runScopedGate("run-a"))
	seedNamedSupervisedRun(t, store, "run-b", runScopedGate("run-b"))

	for _, runID := range []string{"run-a", "run-b"} {
		snapshot, err := store.LoadSupervisionSnapshot(ctx, runID)
		if err != nil {
			t.Fatalf("snapshot of %q: %v", runID, err)
		}
		if !snapshot.Supervised || len(snapshot.Gates) != 1 {
			t.Fatalf("run %q sees %d gates (supervised=%v)", runID, len(snapshot.Gates), snapshot.Supervised)
		}
		gate := snapshot.Gates[0]
		if gate.RunID != runID || gate.Definition.ID != runID+":review" || gate.Definition.Name != "review" {
			t.Fatalf("run %q sees gate %#v", runID, gate.Definition)
		}
		if !gate.Definition.Protects(runID + "-task-protected") {
			t.Fatalf("run %q gate stopped protecting its own task: %#v", runID, gate.Definition)
		}
		other := "run-a"
		if runID == "run-a" {
			other = "run-b"
		}
		if gate.Definition.Protects(other + "-task-protected") {
			t.Fatalf("run %q gate protects a task of %q: %#v", runID, other, gate.Definition)
		}
	}
}

// The writer is the backstop for the same defect: whatever mints a gate ID, a
// row that already belongs to another run is never rewritten in place.
func TestSupervisionGateWriterRefusesAnotherRunsGateRow(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	ctx := context.Background()
	seedNamedSupervisedRun(t, store, "run-a", runScopedGate("run-a"))
	seedNamedSupervisedRun(t, store, "run-b", runScopedGate("run-b"))

	colliding := runScopedGate("run-b")
	colliding.Definition.ID = "run-a:review"
	_, err := store.PutSupervision(ctx, SupervisionMaterialization{
		Record: domain.SupervisionRecord{RunID: "run-b", Config: supervisionTestConfig()},
		Gates:  []domain.Gate{colliding},
	})
	if !errors.Is(err, ErrSupervisionRequestConflict) {
		t.Fatalf("cross-run gate write error = %v, want ErrSupervisionRequestConflict", err)
	}
	snapshot, err := store.LoadSupervisionSnapshot(ctx, "run-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Gates) != 1 || !snapshot.Gates[0].Definition.Protects("run-a-task-protected") {
		t.Fatalf("run-a gate was damaged by the refused write: %#v", snapshot.Gates)
	}
}
