package backlog

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestDrainIntentClaimsAndResume(t *testing.T) {
	now := coordinatorTestTime.Add(time.Hour)
	for _, intent := range []domain.ControlState{domain.ControlDraining, domain.ControlPaused, domain.ControlPausedUncheckpointed, domain.ControlResuming} {
		for _, observedControl := range []domain.ControlState{domain.ControlPreparing, domain.ControlRunning} {
			t.Run(string(intent)+"/"+string(observedControl), func(t *testing.T) {
				assignment := domain.Assignment{ID: "a", AttemptID: "t", WorkerID: "w", WorkerEpoch: "e", Epoch: 1, State: domain.AssignmentClaimed, LeaseExpiresAt: now.Add(-time.Minute)}
				attempt := domain.Attempt{ID: "t", AssignmentID: "a", Control: intent, Progress: domain.ProgressActive, Revision: 1}
				snapshot := domain.WorkerSnapshot{WorkerID: "w", WorkerEpoch: "e", CoordinatorEpoch: 1, Sequence: 1, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour), Assignments: []domain.WorkerAssignmentObservation{{AssignmentID: "a", AssignmentEpoch: 1, State: domain.AssignmentClaimed, Control: observedControl, ThreadID: "thread", ObservedAt: now}}}
				transitions, err := PlanWorkerStateTransitions(sqlite.CoordinatorRecords{Assignments: []domain.Assignment{assignment}, Attempts: []domain.Attempt{attempt}}, snapshot, nil, now)
				if err != nil || len(transitions) != 1 {
					t.Fatalf("transitions=%#v err=%v", transitions, err)
				}
				got := transitions[0]
				if got.Attempt.Control != intent || got.Assignment.ThreadID != "thread" || got.Attempt.ThreadID != "thread" || !got.Assignment.LeaseExpiresAt.Equal(snapshot.ValidUntil) {
					t.Fatalf("claim lost intent or custody: %#v", got)
				}
			})
		}
	}
}

func TestDrainIntentRejectsReceiptAndOmissionRelease(t *testing.T) {
	now := coordinatorTestTime.Add(time.Hour)
	for _, progress := range []domain.ProgressState{domain.ProgressActive, domain.ProgressCancelled} {
		for _, kind := range []domain.WorkerCommandKind{domain.WorkerCommandPrepare, domain.WorkerCommandDispatch, domain.WorkerCommandStop} {
			t.Run(string(progress)+"/"+string(kind), func(t *testing.T) {
				snapshot := coordinatorSnapshot(2)
				snapshot.ObservedAt, snapshot.ValidUntil = now, now.Add(time.Hour)
				assignment := domain.Assignment{ID: "a", AttemptID: "t", WorkerID: snapshot.WorkerID, WorkerEpoch: "old-session", Epoch: 1, State: domain.AssignmentUnknown}
				attempt := domain.Attempt{ID: "t", AssignmentID: "a", Progress: progress, Control: domain.ControlDraining, Revision: 1}
				record := workerCommandRecord(snapshot, assignment, kind, kind == domain.WorkerCommandStop)
				transitions, err := PlanWorkerStateTransitions(sqlite.CoordinatorRecords{Assignments: []domain.Assignment{assignment}, Attempts: []domain.Attempt{attempt}}, snapshot, []domain.WorkerCommandRecord{record}, now)
				if err != nil || len(transitions) != 0 {
					t.Fatalf("receipt/omission released drain: %#v %v", transitions, err)
				}
			})
		}
	}
}

func TestTerminalDrainUnknownAllowsHealthySibling(t *testing.T) {
	t.Run("unknown", func(t *testing.T) { testTerminalDrainWithHealthySibling(t, true) })
	t.Run("claimed", func(t *testing.T) { testTerminalDrainWithHealthySibling(t, false) })
}

func testTerminalDrainWithHealthySibling(t *testing.T, unknown bool) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.Migrate(); err != nil {
		t.Fatal(err)
	}
	now := coordinatorTestTime.Add(time.Hour)
	snapshot := domain.WorkerSnapshot{WorkerID: "w", WorkerEpoch: "e", CoordinatorEpoch: 1, Sequence: 1, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour), Inventory: domain.WorkerInventory{ID: "w"}}
	records := sqlite.CoordinatorRecords{}
	for _, id := range []string{"terminal", "healthy"} {
		assignment := domain.Assignment{ID: id, AttemptID: id, WorkerID: "w", WorkerEpoch: "e", Epoch: 1, State: domain.AssignmentClaimed, LeaseToken: "lease-" + id, DispatchToken: "dispatch-" + id, LeaseExpiresAt: now.Add(time.Hour)}
		attempt := domain.Attempt{ID: id, WorkflowRunID: "run", TaskID: id, Number: 1, AssignmentID: id, Progress: domain.ProgressActive, Control: domain.ControlPreparing, Revision: 1}
		observation := domain.WorkerAssignmentObservation{AssignmentID: id, AssignmentEpoch: 1, State: domain.AssignmentClaimed, Control: domain.ControlRunning, ThreadID: "thread-" + id, ObservedAt: now}
		if id == "terminal" {
			attempt.Progress = domain.ProgressCancelled
			attempt.Control = domain.ControlDraining
			attempt.Failure = "retained"
			if unknown {
				observation.State = domain.AssignmentUnknown
			}
		}
		records.Assignments = append(records.Assignments, assignment)
		records.Attempts = append(records.Attempts, attempt)
		snapshot.Assignments = append(snapshot.Assignments, observation)
	}
	if err = store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	if err = store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	transitions, err := PlanWorkerStateTransitions(records, snapshot, nil, now)
	if err != nil || len(transitions) != 2 {
		t.Fatalf("transitions=%#v err=%v", transitions, err)
	}
	if _, err = store.CommitWorkerStateTransitions(ctx, transitions); err != nil {
		t.Fatal(err)
	}
	got, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range got.Attempts {
		if attempt.ID == "healthy" && attempt.Control != domain.ControlRunning {
			t.Fatalf("healthy sibling blocked: %#v", attempt)
		}
		wantControl := domain.ControlDraining
		if unknown {
			wantControl = domain.ControlStopped
		}
		if attempt.ID == "terminal" && (attempt.Progress != domain.ProgressCancelled || attempt.Control != wantControl || attempt.Failure != "retained") {
			t.Fatalf("terminal evidence changed: %#v", attempt)
		}
	}
}
