package backlog

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestSinkProjectionFanOutFailureAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	tasks := []domain.Task{
		{ID: "a", Name: "a", WorkflowID: "w"},
		{ID: "b", Name: "b", WorkflowID: "w", Needs: []string{"a"}},
		{ID: "c", Name: "c", WorkflowID: "w", Needs: []string{"a"}},
		{ID: "d", Name: "d", WorkflowID: "w", Needs: []string{"b", "c"}},
		{ID: "e", Name: "e", WorkflowID: "w"},
	}
	attempts := make([]domain.Attempt, len(tasks))
	for i, task := range tasks {
		attempts[i] = domain.Attempt{ID: task.ID + "1", TaskID: task.ID, WorkflowRunID: "r", Number: 1, Revision: 1, Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, UpdatedAt: now}
	}
	attempts[0].Progress = domain.ProgressFailed
	attempts[0].Control = domain.ControlStopped
	attempts[4].Progress = domain.ProgressActive
	attempts[4].Control = domain.ControlRunning
	attempts[4].AssignmentID = "live"
	run, err := domain.BindRunSink(domain.WorkflowRun{ID: "r", WorkflowID: "w", Revision: 1, Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now}, tasks)
	if err != nil {
		t.Fatal(err)
	}
	records := sqlite.CoordinatorRecords{Workflows: []domain.Workflow{{ID: "w", Version: 2, Name: "w", Class: domain.TaskClassRequired, CreatedAt: now}}, WorkflowRuns: []domain.WorkflowRun{run}, Tasks: tasks, Attempts: attempts, Assignments: []domain.Assignment{{ID: "live", AttemptID: "e1", State: domain.AssignmentClaimed, CreatedAt: now, UpdatedAt: now}}}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	if _, err := ProjectWorkflowRuns(ctx, store, now); err != nil {
		t.Fatal(err)
	}
	got, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.WorkflowRuns[0].Sink.Progress.Terminal() {
		t.Fatal("settled with live sibling")
	}
	for _, a := range got.Attempts {
		if a.TaskID == "b" && a.Progress != domain.ProgressBlocked {
			t.Fatal("lost retry opportunity")
		}
	}
	for i := range got.Attempts {
		if got.Attempts[i].ID == "e1" {
			got.Attempts[i].Progress = domain.ProgressSucceeded
			got.Attempts[i].Control = domain.ControlStopped
			got.Attempts[i].Revision++
		}
	}
	got.Assignments[0].State = domain.AssignmentCompleted
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: got.Attempts, Assignments: got.Assignments}); err != nil {
		t.Fatal(err)
	}
	if _, err := ProjectWorkflowRuns(ctx, store, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	final, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sink := final.WorkflowRuns[0].Sink
	if sink.Progress != domain.ProgressFailed || !reflect.DeepEqual(sink.Result.FailedTaskIDs, []string{"a"}) || !reflect.DeepEqual(sink.Result.SkippedTaskIDs, []string{"b", "c", "d"}) {
		t.Fatalf("sink=%+v result=%+v", sink, sink.Result)
	}
	if len(final.Attempts) != 5 || len(final.Tasks) != 5 || len(final.Assignments) != 1 || len(final.AuditEvents) != 1 {
		t.Fatalf("unexpected executable records: %+v", final)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	report, err := ProjectWorkflowRuns(ctx, store, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	replay, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Runs) != 0 || !reflect.DeepEqual(final, replay) {
		t.Fatal("restart changed settled sink")
	}
	execution, err := NewDAGExecution(DAGState{Run: replay.WorkflowRuns[0], Tasks: replay.Tasks, Attempts: replay.Attempts})
	if err != nil {
		t.Fatal(err)
	}
	if err := execution.RetryTask("a", "a2", now); err == nil {
		t.Fatal("retried final run")
	}
}

func TestEmptyBundleCreatesOnlyCoordinatorSink(t *testing.T) {
	bundle := validBundle(t)
	rewriteBundleManifest(t, bundle, "version: 2\nname: empty\nclass: required\nenvironment: {project: t3-steward}\ntasks: {}\n")
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ingester := BundleIngester{StorageRoot: filepath.Join(t.TempDir(), "artifacts"), Store: store}
	t.Cleanup(func() { _ = removeIngestedTree(ingester.StorageRoot) })
	got, err := ingester.Ingest(context.Background(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Records.Tasks) != 0 || len(got.Records.Attempts) != 0 || got.Records.WorkflowRuns[0].Sink == nil {
		t.Fatalf("records=%+v", got.Records)
	}
	if _, err := ProjectWorkflowRuns(context.Background(), store, time.Now()); err != nil {
		t.Fatal(err)
	}
	final, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if final.WorkflowRuns[0].Sink.Progress != domain.ProgressSucceeded || len(final.Assignments) != 0 {
		t.Fatalf("final=%+v", final)
	}
}
