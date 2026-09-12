package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func sinkStoreFixture(t *testing.T) (*Store, WorkflowProjectionSnapshot, time.Time) {
	t.Helper()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	before := WorkflowProjectionSnapshot{
		Run:      domain.WorkflowRun{ID: "r", WorkflowID: "w", Revision: 1, Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now},
		Tasks:    []domain.Task{{ID: "t", Name: "t", WorkflowID: "w"}},
		Attempts: []domain.Attempt{{ID: "a", TaskID: "t", WorkflowRunID: "r", Number: 1, Revision: 1, Progress: domain.ProgressFailed, Control: domain.ControlStopped, UpdatedAt: now}},
	}
	before.Run, err = domain.BindRunSink(before.Run, before.Tasks)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{before.Run}, Tasks: before.Tasks, Attempts: before.Attempts}); err != nil {
		t.Fatal(err)
	}
	return store, before, now
}

func sinkProjected(t *testing.T, before WorkflowProjectionSnapshot, now time.Time) domain.WorkflowRun {
	t.Helper()
	run, err := domain.ProjectRunSink(before.Run, before.Tasks, before.Attempts, before.Assignments, now)
	if err != nil {
		t.Fatal(err)
	}
	run.Revision++
	run.UpdatedAt = now
	return run
}

func TestSinkProjectionFencesEveryReadSetMutation(t *testing.T) {
	for _, kind := range []string{"run", "task", "attempt", "retry", "assignment"} {
		t.Run(kind, func(t *testing.T) {
			store, before, now := sinkStoreFixture(t)
			run := sinkProjected(t, before, now)
			var change CoordinatorRecords
			switch kind {
			case "run":
				changed := before.Run
				changed.Revision++
				change.WorkflowRuns = []domain.WorkflowRun{changed}
			case "task":
				changed := before.Tasks[0]
				changed.Name = "changed"
				change.Tasks = []domain.Task{changed}
			case "attempt":
				changed := before.Attempts[0]
				changed.Control = domain.ControlRunning
				change.Attempts = []domain.Attempt{changed}
			case "retry":
				changed := before.Attempts[0]
				changed.ID = "retry"
				changed.Number = 2
				changed.Progress = domain.ProgressReady
				change.Attempts = []domain.Attempt{changed}
			case "assignment":
				change.Assignments = []domain.Assignment{{ID: "unknown", AttemptID: "a", State: domain.AssignmentUnknown, CreatedAt: now, UpdatedAt: now}}
			}
			if err := store.SaveCoordinatorRecords(context.Background(), change); err != nil {
				t.Fatal(err)
			}
			if err := store.CommitWorkflowProjection(context.Background(), before, run, before.Attempts, now); !errors.Is(err, ErrStaleWorkflowProjection) {
				t.Fatalf("error=%v", err)
			}
			got, err := store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if got.WorkflowRuns[0].Sink.Progress.Terminal() || len(got.AuditEvents) != 0 {
				t.Fatal("stale sink published")
			}
		})
	}
}

func TestConcurrentSinkSettlementPublishesExactlyOnce(t *testing.T) {
	store, before, now := sinkStoreFixture(t)
	run := sinkProjected(t, before, now)
	const n = 8
	results := make(chan error, n)
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- store.CommitWorkflowProjection(context.Background(), before, run, before.Attempts, now)
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrStaleWorkflowProjection) {
			t.Error(err)
		}
	}
	if successes != 1 {
		t.Fatalf("successes=%d", successes)
	}
	got, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.AuditEvents) != 1 || got.AuditEvents[0].Kind != "sink-settled" || len(got.Attempts) != 1 || !reflect.DeepEqual(got.WorkflowRuns[0].Sink, run.Sink) {
		t.Fatalf("records=%+v", got)
	}
	// A retry planned before settlement is rejected within the apply transaction.
	cmd := domain.AdminCommand{ID: "retry", Kind: domain.AdminCommandRetry, TargetType: domain.AdminTargetAttempt, TargetID: "a", ExpectedRevision: 1, Reason: "retry", RequestedBy: "test", State: domain.AdminCommandPending, CreatedAt: now}
	if _, err := store.SubmitAdminCommand(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
	prior := before.Attempts[0]
	prior.Revision++
	retry := prior
	retry.ID = "a2"
	retry.Number = 2
	retry.Revision = 1
	retry.Progress = domain.ProgressReady
	decision, err := store.ApplyAdminCommand(context.Background(), domain.AdminCommandApplication{CommandID: cmd.ID, ExpectedCommandState: domain.AdminCommandPending, ExpectedTargetRevision: 1, State: domain.AdminCommandApplied, Attempt: &prior, NewAttempt: &retry, AppliedAt: now})
	if err != nil || decision.Command.State != domain.AdminCommandRejected {
		t.Fatalf("retry=%+v err=%v", decision, err)
	}
	got, err = store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Attempts) != 1 || !reflect.DeepEqual(got.WorkflowRuns[0].Sink, run.Sink) {
		t.Fatal("retry changed final run")
	}
}

func TestMigrationV12BackfillsStableSinkWithoutAttempts(t *testing.T) {
	store, before, _ := sinkStoreFixture(t)
	before.Run.Sink = nil
	before.Run.GraphRevision = 0
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{before.Run}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec("DELETE FROM schema_version WHERE version=12"); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(); err != nil {
		t.Fatal(err)
	}
	got, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	run := got.WorkflowRuns[0]
	if run.Revision != 2 || run.GraphRevision != 1 || run.Sink.ID != "sink:r" || !reflect.DeepEqual(run.Sink.Needs, []string{"t"}) || len(got.Attempts) != 1 {
		t.Fatalf("migration=%+v sink=%+v", run, run.Sink)
	}
	if err := store.Migrate(); err != nil {
		t.Fatal(err)
	}
	again, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, again) {
		t.Fatal("migration is not idempotent")
	}
}
