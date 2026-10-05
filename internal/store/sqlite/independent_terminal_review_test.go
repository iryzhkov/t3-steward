package sqlite

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"reflect"
	"testing"
	"time"
)

func TestIndependentRuntimeTerminalFencesPendingAndReplay(t *testing.T) {
	for _, kind := range []domain.WorkerCommandKind{domain.WorkerCommandPrepare, domain.WorkerCommandDispatch} {
		for _, boundary := range []string{"skipped", "control-park", "binding", "completed-wait"} {
			t.Run(string(kind)+"/"+boundary, func(t *testing.T) {
				ctx := context.Background()
				s := openFleetTestStore(t)
				claimFleetAssignment(t, s)
				old := fleetWorkerCommand(kind, "old", 1, fleetTestTime.Add(2*time.Second))
				if got, err := s.CommitWorkerCommands(ctx, []domain.WorkerCommand{old}); err != nil || len(got) != 1 {
					t.Fatalf("initial %+v %v", got, err)
				}
				r, err := s.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				a := r.Attempts[0]
				switch boundary {
				case "skipped":
					a.Progress = domain.ProgressSkipped
					a.Control = domain.ControlRunning
				case "control-park":
					a.Progress = domain.ProgressActive
					a.Control = domain.ControlWaitingExternal
				case "binding":
					a.AssignmentID = "another-assignment"
				case "completed-wait":
					a.Progress = domain.ProgressWaitingExternal
					a.Control = domain.ControlWaitingExternal
					at := fleetTestTime.Add(time.Second)
					a.CompletedAt = &at
				}
				a.Revision++
				saveFleetAttempt(t, s, a)
				cleanup := old
				cleanup.ID = "collect"
				cleanup.Kind = domain.WorkerCommandCollect
				fresh := old
				fresh.ID = "new"
				got, err := s.CommitWorkerCommands(ctx, []domain.WorkerCommand{cleanup, fresh})
				if err != nil || !reflect.DeepEqual(got, []domain.WorkerCommand{cleanup}) {
					t.Fatalf("mixed cleanup/start %+v %v", got, err)
				}
				before := cancellationSnapshot(t, s)
				pending, err := s.LoadPendingWorkerCommands(ctx, old.WorkerID, old.WorkerEpoch, 1, 1, fleetTestTime.Add(3*time.Second))
				if err != nil || !reflect.DeepEqual(pending, []domain.WorkerCommand{cleanup}) {
					t.Fatalf("pending %+v %v", pending, err)
				}
				if before != cancellationSnapshot(t, s) {
					t.Fatal("pending load mutated history")
				}
				// Historical receipts survive replacement of the worker epoch.
				snapshot := fleetSnapshot(1, "replacement", 2, true, fleetTestTime.Add(time.Hour))
				snapshot.ObservedAt = fleetTestTime.Add(4 * time.Second)
				if err = s.SaveWorkerSnapshot(ctx, snapshot); err != nil {
					t.Fatal(err)
				}
				before = cancellationSnapshot(t, s)
				if got, err = s.CommitWorkerCommands(ctx, []domain.WorkerCommand{old, cleanup}); err != nil || !reflect.DeepEqual(got, []domain.WorkerCommand{old, cleanup}) || before != cancellationSnapshot(t, s) {
					t.Fatalf("historical replay %+v %v", got, err)
				}
				pending, err = s.LoadPendingWorkerCommands(ctx, old.WorkerID, "replacement", 1, 2, fleetTestTime.Add(5*time.Second))
				if err != nil || len(pending) != 0 {
					t.Fatalf("epoch fence %+v %v", pending, err)
				}
			})
		}
	}
}
func TestIndependentRuntimeTerminalFencesSkippedGuardAudit(t *testing.T) {
	ctx := context.Background()
	s := openFleetTestStore(t)
	claimFleetAssignment(t, s)
	r, err := s.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a := r.Attempts[0]
	assignment := r.Assignments[0]
	a.Progress = domain.ProgressSkipped
	a.Control = domain.ControlStopped
	a.CompletedAt = nil
	a.Failure = "keep"
	a.Revision++
	saveFleetAttempt(t, s, a)
	now := fleetTestTime.Add(3 * time.Second)
	next := a
	next.Revision++
	next.UpdatedAt = now
	na := assignment
	na.State = domain.AssignmentCompleted
	na.LeaseExpiresAt = time.Time{}
	na.UpdatedAt = now
	valid := domain.WorkerStateTransition{CoordinatorEpoch: 1, WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch, WorkerSequence: 1, TransitionedAt: now, ExpectedAssignment: assignment, ExpectedAttemptRevision: a.Revision, Assignment: na, Attempt: next, Reason: "collect-accepted"}
	forged := valid
	forged.Attempt.Progress = domain.ProgressReady
	before := cancellationSnapshot(t, s)
	if _, err = s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{forged}); !errors.Is(err, ErrStaleWorkerStateTransition) || before != cancellationSnapshot(t, s) {
		t.Fatalf("skipped guard %v", err)
	}
	// An exact projection installed without the native transition audit is not a valid replay.
	if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{next}, Assignments: []domain.Assignment{na}}); err != nil {
		t.Fatal(err)
	}
	before = cancellationSnapshot(t, s)
	if _, err = s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{valid}); err == nil || before != cancellationSnapshot(t, s) {
		t.Fatalf("unaudited replay %v", err)
	}
}
