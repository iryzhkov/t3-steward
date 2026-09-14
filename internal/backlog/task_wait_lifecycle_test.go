package backlog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// waitingOutcomeStore is turnOutcomeStoreFake plus the task-bound wait surface.
type waitingOutcomeStore struct {
	attempts  []domain.Attempt
	live      map[string]string
	recorded  []domain.TaskWaitReconciliation
	committed []domain.TurnOutcomeTransition
}

func (s *waitingOutcomeStore) LoadTurnOutcomeState(context.Context) ([]domain.Attempt, []domain.ThrottleAttemptRecord, error) {
	return s.attempts, nil, nil
}

func (s *waitingOutcomeStore) CommitTurnOutcomeTransitions(_ context.Context, transitions []domain.TurnOutcomeTransition) error {
	s.committed = append(s.committed, transitions...)
	return nil
}

func (s *waitingOutcomeStore) LiveTaskWaitAttempts(context.Context) (map[string]string, error) {
	return s.live, nil
}

func (s *waitingOutcomeStore) RecordTaskWaitReconciliations(_ context.Context, events []domain.TaskWaitReconciliation) error {
	s.recorded = append(s.recorded, events...)
	return nil
}

// A done marker observed while a live task-bound wait exists is refused and
// recorded. It must never verify the task: this is the exact step that failed
// a campaign against outputs its thread had not written yet.
func TestDoneMarkerIsRefusedWhileATaskWaitIsLive(t *testing.T) {
	now := time.Date(2026, 9, 14, 17, 30, 29, 0, time.UTC)
	store := &waitingOutcomeStore{
		attempts: []domain.Attempt{{
			ID: "a1", Revision: 4, ThreadID: "thread-1",
			Progress: domain.ProgressWaitingExternal, Control: domain.ControlWaitingExternal,
		}},
		live: map[string]string{"a1": "tw-ci"},
	}
	_, err := ReconcileTurnOutcomes(context.Background(), store, []domain.TurnOutcome{{
		ID: "o1", AttemptID: "a1", Marker: domain.TurnOutcomeDone, VerificationPassed: false,
		Failure: "missing declared output: final-commit.txt", ObservedAt: now,
	}}, now)
	if !errors.Is(err, ErrTurnOutcomeWaiting) {
		t.Fatalf("a done marker was honoured while a wait was live: %v", err)
	}
	if !errors.Is(err, ErrResultImportSuperseded) {
		t.Fatal("the refusal is not recognisable as a discardable result")
	}
	if len(store.committed) != 0 {
		t.Fatalf("the refused marker still committed a transition: %v", store.committed)
	}
	if len(store.recorded) != 1 || store.recorded[0].Kind != domain.TaskWaitReconciliationDoneWhileWaiting ||
		store.recorded[0].WaitID != "tw-ci" || store.recorded[0].ThreadID != "thread-1" {
		t.Fatalf("the contradiction was not recorded: %+v", store.recorded)
	}
}

// With no live wait the same marker settles the attempt exactly as before, so
// the guard costs an ordinary completion nothing.
func TestDoneMarkerStillSettlesWhenNoTaskWaitIsLive(t *testing.T) {
	now := time.Date(2026, 9, 14, 17, 30, 29, 0, time.UTC)
	store := &waitingOutcomeStore{
		attempts: []domain.Attempt{{ID: "a1", Revision: 4, Progress: domain.ProgressVerifying}},
		live:     map[string]string{},
	}
	transitions, err := ReconcileTurnOutcomes(context.Background(), store, []domain.TurnOutcome{{
		ID: "o1", AttemptID: "a1", Marker: domain.TurnOutcomeDone, VerificationPassed: true, ObservedAt: now,
	}}, now)
	if err != nil || len(transitions) != 1 || transitions[0].Attempt.Progress != domain.ProgressSucceeded {
		t.Fatalf("transitions=%v err=%v", transitions, err)
	}
}

// A waiting marker parks the attempt without ending it, and carries no
// terminal result data.
func TestWaitingTurnOutcomeParksTheAttempt(t *testing.T) {
	now := time.Date(2026, 9, 14, 17, 30, 29, 0, time.UTC)
	attempts := []domain.Attempt{{ID: "a1", Revision: 4, Progress: domain.ProgressActive, Control: domain.ControlRunning}}
	transitions, refusals, err := PlanTurnOutcomesWithTaskWaits(attempts, nil, []domain.TurnOutcome{{
		ID: "o1", AttemptID: "a1", Marker: domain.TurnOutcomeWaiting, ObservedAt: now,
	}}, nil, now)
	if err != nil || len(refusals) != 0 || len(transitions) != 1 {
		t.Fatalf("transitions=%v refusals=%v err=%v", transitions, refusals, err)
	}
	parked := transitions[0].Attempt
	if parked.Progress != domain.ProgressWaitingExternal || parked.Control != domain.ControlWaitingExternal {
		t.Fatalf("the waiting marker did not park the attempt: %q/%q", parked.Progress, parked.Control)
	}
	if parked.Progress.Terminal() || parked.CompletedAt != nil {
		t.Fatal("the waiting marker ended the attempt")
	}
	if _, _, err := PlanTurnOutcomesWithTaskWaits(attempts, nil, []domain.TurnOutcome{{
		ID: "o2", AttemptID: "a1", Marker: domain.TurnOutcomeWaiting, VerificationPassed: true, ObservedAt: now,
	}}, nil, now); err == nil {
		t.Fatal("a waiting outcome carrying a verification result was accepted")
	}
}

// parkStore is the narrow view parkedAssignments needs: which attempts are
// parked, and the records that map them to assignments.
type parkStore struct {
	live    map[string]string
	records sqlite.CoordinatorRecords
	err     error
}

func (s parkStore) LiveTaskWaitAttempts(context.Context) (map[string]string, error) {
	return s.live, s.err
}

func (s parkStore) LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error) {
	return s.records, nil
}

// The coordinator tells each worker which of its own assignments are parked,
// with the attempt revision the park is fenced on. The statement is scoped to
// that worker: a worker learns nothing about another worker's executions.
func TestParkedAssignmentsAreScopedToOneWorker(t *testing.T) {
	store := parkStore{
		live: map[string]string{"a1": "tw-ci", "a3": "tw-review"},
		records: sqlite.CoordinatorRecords{
			Attempts: []domain.Attempt{
				{ID: "a1", Revision: 12}, {ID: "a2", Revision: 3}, {ID: "a3", Revision: 7},
			},
			Assignments: []domain.Assignment{
				{ID: "assign-1", AttemptID: "a1", WorkerID: "normandy", Epoch: 2},
				{ID: "assign-2", AttemptID: "a2", WorkerID: "normandy", Epoch: 1},
				{ID: "assign-3", AttemptID: "a3", WorkerID: "tempest", Epoch: 5},
			},
		},
	}
	request, err := parkedAssignmentsFor(context.Background(), store, "normandy")
	if err != nil {
		t.Fatal(err)
	}
	if !request.ParkedReported {
		t.Fatal("the statement does not say it reports parked assignments, so an empty list would read as unknown")
	}
	if len(request.Parked) != 1 {
		t.Fatalf("parked = %+v, want only this worker's parked assignment", request.Parked)
	}
	parked := request.Parked[0]
	if parked.AssignmentID != "assign-1" || parked.AssignmentEpoch != 2 ||
		parked.AttemptID != "a1" || parked.AttemptRevision != 12 || parked.WaitID != "tw-ci" {
		t.Fatalf("parked = %+v", parked)
	}
	// A worker with nothing parked is told so positively, not by silence.
	empty, err := parkedAssignmentsFor(context.Background(), store, "unrelated")
	if err != nil || !empty.ParkedReported || len(empty.Parked) != 0 {
		t.Fatalf("empty statement = %+v err=%v", empty, err)
	}
	// A store with no task-bound waits at all still reports, because it truly
	// has nothing parked.
	none, err := parkedAssignmentsFor(context.Background(), parkStore{live: map[string]string{}}, "normandy")
	if err != nil || !none.ParkedReported || len(none.Parked) != 0 {
		t.Fatalf("statement without waits = %+v err=%v", none, err)
	}
	// A store that cannot answer fails the exchange rather than claiming that
	// nothing is parked.
	if _, err := parkedAssignmentsFor(context.Background(), parkStore{err: errors.New("database is locked")}, "normandy"); err == nil {
		t.Fatal("an unreadable store reported that nothing is parked")
	}
}

// A parked attempt releases the executor slot and its CPU, memory and scratch
// reservation, while keeping the assignment that owns its workspace and locks.
func TestParkedAttemptReleasesExecutorCapacityAndKeepsItsAssignment(t *testing.T) {
	run := domain.WorkflowRun{ID: "r", WorkflowID: "w"}
	tasks := []domain.Task{{ID: "t", WorkflowID: "w", Name: "t",
		ResourceDemand: domain.ResourceDemand{CPUUnits: 2, MemoryMB: 4096, ScratchMB: 1024},
		ResourceLocks:  []string{"repo"}}}
	assignment := domain.Assignment{ID: "assign-1", AttemptID: "a1", WorkerID: "worker", State: domain.AssignmentClaimed}
	running := domain.Attempt{ID: "a1", WorkflowRunID: "r", TaskID: "t", AssignmentID: "assign-1",
		Progress: domain.ProgressActive, Control: domain.ControlRunning}
	if owners := capacityOwners([]domain.Attempt{running}, []domain.Assignment{assignment}, []domain.WorkflowRun{run}, tasks); len(owners) != 1 {
		t.Fatalf("a running attempt did not hold capacity: %v", owners)
	}
	parked := running
	parked.Progress = domain.ProgressWaitingExternal
	parked.Control = domain.ControlWaitingExternal
	if owners := capacityOwners([]domain.Attempt{parked}, []domain.Assignment{assignment}, []domain.WorkflowRun{run}, tasks); len(owners) != 0 {
		t.Fatalf("a parked attempt still holds executor capacity: %v", owners)
	}
	if parked.AssignmentID == "" {
		t.Fatal("a parked attempt lost the assignment that owns its workspace and locks")
	}
	if domain.ControlWaitingExternal.HoldsProviderSlot() {
		t.Fatal("a parked attempt still holds a provider slot")
	}
	// Resource locks and directory bindings are derived from a nonterminal
	// attempt whose assignment is not settled, so a parked attempt keeps them.
	// It has to: the thread can still touch its workspace when it wakes, and
	// releasing the lock would let a conflicting writer in while the first
	// writer is merely parked.
	if parked.Progress.Terminal() || assignment.State == domain.AssignmentReleased ||
		assignment.State == domain.AssignmentCompleted {
		t.Fatal("a parked attempt no longer meets the condition that holds its locks")
	}
	// The sink must not settle while any attempt is parked, whatever the
	// assignment says.
	if domain.RunExecutionsQuiescent("r", []domain.Attempt{parked}, []domain.Assignment{assignment}) {
		t.Fatal("a parked attempt was reported quiescent")
	}
}
