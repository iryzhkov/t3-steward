package sqlite

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A retention pass that meets a pinned run must prune everything else and say
// what it left alone. The pin is enforced by a trigger that aborts the delete,
// and one abort rolls back the whole transaction, so before this a single rerun
// pin meant no artifact anywhere was ever pruned.
func TestPruneArtifactsSkipsPinnedRunsAndPrunesTheRest(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	created := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

	records := CoordinatorRecords{Workflows: []domain.Workflow{{
		ID: "workflow-1", Version: 2, Name: "build", Class: domain.TaskClassRequired,
		TaskIDs: []string{"task-a", "task-b", "task-c"}, CreatedAt: created,
	}}}
	for _, name := range []string{"a", "b", "c"} {
		runID := "run-" + name
		records.WorkflowRuns = append(records.WorkflowRuns, domain.WorkflowRun{
			ID: runID, WorkflowID: "workflow-1", Progress: domain.ProgressSucceeded,
			Revision: 1, CreatedAt: created, UpdatedAt: created,
		})
		records.Tasks = append(records.Tasks, domain.Task{
			ID: "task-" + name, WorkflowID: "workflow-1", RunID: runID, Name: "task-" + name,
			Class: domain.TaskClassRequired, MaxTurns: 1,
		})
		records.Attempts = append(records.Attempts, domain.Attempt{
			ID: "attempt-" + name, WorkflowRunID: runID, TaskID: "task-" + name, Number: 1,
			Progress: domain.ProgressSucceeded, Control: domain.ControlStopped,
			Revision: 1, UpdatedAt: created,
		})
		records.Artifacts = append(records.Artifacts, domain.Artifact{
			ID: "output-" + name, WorkflowRunID: runID, TaskID: "task-" + name,
			AttemptID: "attempt-" + name, Kind: domain.ArtifactOutput, Name: "result.md",
			MediaType: "text/markdown", Size: 3, SHA256: strings.Repeat("a", 64),
			StoragePath: "runs/" + runID + "/result.md", Producer: "worker:test", CreatedAt: created,
		})
	}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	// One of the three is held, as a rerun of it would hold it.
	if _, err := store.db.ExecContext(ctx,
		"INSERT INTO coordinator_retention_pins(owner,run_id,task_id) VALUES(?,?,?)",
		"rerun:run:rerun-1", "run-b", "task-b"); err != nil {
		t.Fatal(err)
	}

	expired, skipped, err := store.PruneArtifacts(ctx, created.Add(24*time.Hour), nil)
	if err != nil {
		t.Fatalf("a pinned run failed the whole pass: %v", err)
	}
	pruned := make([]string, 0, len(expired))
	for _, artifact := range expired {
		pruned = append(pruned, artifact.ID)
	}
	if strings.Join(pruned, ",") != "output-a,output-c" {
		t.Fatalf("pruned %v, want every run but the pinned one", pruned)
	}
	if len(skipped) != 1 || skipped[0].WorkflowRunID != "run-b" || skipped[0].Artifacts != 1 ||
		!strings.Contains(skipped[0].Reason, "rerun:run:rerun-1") {
		t.Fatalf("skipped = %+v, want the pinned run named with its holder", skipped)
	}
	if _, err := store.LoadArtifacts(ctx, []string{"output-b"}); err != nil {
		t.Fatalf("the pinned run's artifact did not survive: %v", err)
	}
	for _, id := range []string{"output-a", "output-c"} {
		if _, err := store.LoadArtifacts(ctx, []string{id}); err == nil {
			t.Fatalf("%s was reported pruned but is still retained", id)
		}
	}

	// Once the pin is gone the same pass prunes what it skipped, and reports
	// nothing skipped.
	if _, err := store.db.ExecContext(ctx, "DELETE FROM coordinator_retention_pins"); err != nil {
		t.Fatal(err)
	}
	expired, skipped, err = store.PruneArtifacts(ctx, created.Add(24*time.Hour), nil)
	if err != nil || len(skipped) != 0 || len(expired) != 1 || expired[0].ID != "output-b" {
		t.Fatalf("after the pin: expired %+v, skipped %+v, err %v", expired, skipped, err)
	}
}

// A run named as protected is not a skip: the caller asked for it to be kept,
// so there is nothing to report about it.
func TestPruneArtifactsReportsNoSkipForAProtectedRun(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	fixture := coordinatorFixture()
	if err := store.SaveCoordinatorRecords(ctx, fixture); err != nil {
		t.Fatal(err)
	}
	later := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	expired, skipped, err := store.PruneArtifacts(ctx, later, []string{"run-1"})
	if err != nil || len(expired) != 0 || len(skipped) != 0 {
		t.Fatalf("protected prune: expired %+v, skipped %+v, err %v", expired, skipped, err)
	}
}
