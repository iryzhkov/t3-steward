package sqlite

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func displayDecisionFixture(t *testing.T, s *Store) (domain.Assignment, workerproto.ExecutionIdentity) {
	t.Helper()
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	a := domain.Assignment{ID: "display-assignment", AttemptID: "display-attempt", WorkerID: "normandy", WorkerEpoch: "worker-1", Epoch: 1, State: domain.AssignmentOffered, DispatchToken: "display-dispatch", ThreadID: "display-thread", LeaseToken: "display-lease", LeaseExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now}
	i := workerproto.ExecutionIdentity{WorkflowID: "display-workflow", WorkflowRunID: "display-run", TaskID: "display-task", AttemptID: a.AttemptID, AttemptRevision: 1, AssignmentID: a.ID, AssignmentEpoch: a.Epoch, DispatchToken: a.DispatchToken, ThreadID: a.ThreadID}
	if err := s.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: i.WorkflowRunID, WorkflowID: i.WorkflowID, Progress: domain.ProgressActive, Revision: 1}},
		Attempts:     []domain.Attempt{{ID: a.AttemptID, WorkflowRunID: i.WorkflowRunID, TaskID: i.TaskID, Number: 1, AssignmentID: a.ID, Revision: 1, Control: domain.ControlUnassigned, Progress: domain.ProgressReady}},
		Assignments:  []domain.Assignment{a},
	}); err != nil {
		t.Fatal(err)
	}
	return a, i
}

func TestAssignmentDisplayV33MigrationReplayAndBinding(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.migrateThrough(32); err != nil {
		t.Fatal(err)
	}
	a, i := displayDecisionFixture(t, s)
	var foundationSchema string
	if err := s.db.QueryRow("SELECT group_concat(sql) FROM sqlite_master WHERE name LIKE 'coordinator_review_%' ORDER BY name").Scan(&foundationSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FreezeAssignmentDisplay(ctx, 1, "coordinator", a, i, nil); err == nil {
		t.Fatal("V32 unexpectedly has display allocation")
	}
	if err := s.migrateThrough(33); err != nil {
		t.Fatal(err)
	}
	if got := schemaVersionOf(t, s); got != 33 {
		t.Fatalf("version %d", got)
	}
	var migratedFoundationSchema string
	if err := s.db.QueryRow("SELECT group_concat(sql) FROM sqlite_master WHERE name LIKE 'coordinator_review_%' ORDER BY name").Scan(&migratedFoundationSchema); err != nil {
		t.Fatal(err)
	}
	if migratedFoundationSchema != foundationSchema {
		t.Fatal("V33 changed V32 foundation schema")
	}
	if err := s.Migrate(); err != nil {
		t.Fatal(err)
	}
	proposed := &workerproto.SessionDisplay{WorkflowName: "Campaign", TaskName: "Build"}
	wrongIdentity := i
	wrongIdentity.WorkflowRunID = "wrong-run"
	if _, err := s.FreezeAssignmentDisplay(ctx, 1, "coordinator", a, wrongIdentity, proposed); err == nil {
		t.Fatal("unbound identity accepted")
	}
	var count int
	if err := s.db.QueryRow("SELECT count(*) FROM coordinator_assignment_displays").Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed decision leaked: %d %v", count, err)
	}
	if got, err := s.FreezeAssignmentDisplay(ctx, 1, "coordinator", a, i, nil); err != nil || got != nil {
		t.Fatalf("freeze null: %+v %v", got, err)
	}
	var raw string
	if err := s.db.QueryRow("SELECT display FROM coordinator_assignment_displays").Scan(&raw); err != nil || raw != "null" {
		t.Fatalf("omission not explicit: %q %v", raw, err)
	}
	if got, err := s.FreezeAssignmentDisplay(ctx, 1, "coordinator", a, i, proposed); err != nil || got != nil {
		t.Fatalf("null replay changed: %+v %v", got, err)
	}
	for _, mutate := range []func(*domain.Assignment){
		func(a *domain.Assignment) { a.WorkerID = "other" },
		func(a *domain.Assignment) { a.WorkerEpoch = "other" },
		func(a *domain.Assignment) { a.DispatchToken = "other" },
		func(a *domain.Assignment) { a.ThreadID = "other" },
	} {
		wrong := a
		mutate(&wrong)
		if _, err := s.FreezeAssignmentDisplay(ctx, 1, "coordinator", wrong, i, proposed); err == nil {
			t.Fatal("assignment tamper accepted")
		}
	}
	if _, err := s.FreezeAssignmentDisplay(ctx, 2, "coordinator", a, i, proposed); err == nil {
		t.Fatal("stale coordinator accepted")
	}
	// A different assignment epoch gets its own decision.
	a.Epoch++
	i.AssignmentEpoch++
	if err := s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Assignments: []domain.Assignment{a}}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.FreezeAssignmentDisplay(ctx, 1, "coordinator", a, i, proposed); err != nil || !reflect.DeepEqual(got, proposed) {
		t.Fatalf("new epoch decision: %+v %v", got, err)
	}
	// A different assignment also gets its own decision.
	a.ID = "other-assignment"
	a.AttemptID = "other-attempt"
	a.DispatchToken = "other-dispatch"
	i.AssignmentID = a.ID
	i.AttemptID = a.AttemptID
	i.DispatchToken = a.DispatchToken
	if err := s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Assignments: []domain.Assignment{a}, Attempts: []domain.Attempt{{ID: a.AttemptID, WorkflowRunID: i.WorkflowRunID, TaskID: i.TaskID, Number: 2, AssignmentID: a.ID, Revision: 1}}}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.FreezeAssignmentDisplay(ctx, 1, "coordinator", a, i, nil); err != nil || got != nil {
		t.Fatalf("new assignment inherited display: %+v %v", got, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openMigratedFixture(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.FreezeAssignmentDisplay(ctx, 1, "coordinator", a, i, proposed); err != nil || got != nil {
		t.Fatalf("reopen changed explicit null: %+v %v", got, err)
	}
}
