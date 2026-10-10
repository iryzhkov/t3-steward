package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestWorkerClaimCannotResumeCoordinatorIntent(t *testing.T) {
	for _, intent := range []domain.ControlState{domain.ControlPaused, domain.ControlPausedUncheckpointed, domain.ControlDraining, domain.ControlResuming} {
		for _, projection := range []domain.ControlState{domain.ControlPreparing, domain.ControlRunning, domain.ControlResuming} {
			if intent == domain.ControlResuming && projection == domain.ControlResuming {
				continue
			}
			t.Run(string(intent)+"/"+string(projection), func(t *testing.T) {
				ctx := context.Background()
				store, err := openMigratedFixture(filepath.Join(t.TempDir(), "state.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				now := fleetTestTime.Add(time.Hour)
				snapshot := fleetSnapshot(1, "e", 1, true, now.Add(time.Hour))
				snapshot.ObservedAt = now
				if err = store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
					t.Fatal(err)
				}
				attempt := fleetAttempt("attempt")
				attempt.Control = intent
				attempt.Progress = domain.ProgressActive
				attempt.AssignmentID = "assignment"
				attempt.Revision = 1
				assignment := domain.Assignment{ID: "assignment", AttemptID: attempt.ID, WorkerID: snapshot.WorkerID, WorkerEpoch: "e", Epoch: 1, State: domain.AssignmentClaimed, LeaseToken: "lease", DispatchToken: "dispatch", LeaseExpiresAt: snapshot.ValidUntil}
				if err = store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
					t.Fatal(err)
				}
				next := attempt
				next.Control = projection
				next.Revision++
				next.UpdatedAt = now
				transition := domain.WorkerStateTransition{CoordinatorEpoch: 1, WorkerID: snapshot.WorkerID, WorkerEpoch: "e", WorkerSequence: 1, TransitionedAt: now, ExpectedAssignment: assignment, ExpectedAttemptRevision: 1, Assignment: assignment, Attempt: next, Reason: "worker-observed-present"}
				if _, err = store.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{transition}); !errors.Is(err, ErrStaleWorkerStateTransition) {
					t.Fatalf("projection accepted: %v", err)
				}
				records, err := store.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if records.Attempts[0].Control != intent || records.Attempts[0].Revision != 1 {
					t.Fatalf("guard mutated attempt: %#v", records.Attempts[0])
				}
			})
		}
	}
}

func TestStopReleaseRequiresExactObservation(t *testing.T) {
	for _, test := range []struct {
		name, epoch, thread string
		observed            bool
		reason              string
		ok                  bool
	}{
		{"exact", "e", "thread", true, "worker-observed-stopped", true},
		{"foreign epoch", "other", "thread", true, "worker-observed-stopped", false},
		{"foreign thread", "e", "other", true, "worker-observed-stopped", false},
		{"missing thread", "e", "", true, "worker-observed-stopped", false},
		{"ack alone", "e", "thread", false, "stop-accepted", false},
		{"forged rejection", "e", "thread", false, "worker-command-rejected", false},
		{"forged omission", "e", "thread", false, "worker-observed-absent", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store, err := openMigratedFixture(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			now := fleetTestTime.Add(time.Hour)
			snapshot := fleetSnapshot(1, test.epoch, 1, true, now.Add(time.Hour))
			snapshot.ObservedAt = now
			assignment := domain.Assignment{ID: "assignment", AttemptID: "attempt", WorkerID: snapshot.WorkerID, WorkerEpoch: "e", ThreadID: "thread", Epoch: 1, State: domain.AssignmentClaimed, LeaseToken: "lease", DispatchToken: "dispatch", LeaseExpiresAt: snapshot.ValidUntil}
			attempt := fleetAttempt("attempt")
			attempt.AssignmentID = assignment.ID
			attempt.Control = domain.ControlDraining
			attempt.Progress = domain.ProgressActive
			attempt.Revision = 1
			if test.observed {
				snapshot.Assignments = []domain.WorkerAssignmentObservation{{AssignmentID: assignment.ID, AssignmentEpoch: 1, State: domain.AssignmentReleased, ThreadID: test.thread, ObservedAt: now}}
			}
			if err = store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
				t.Fatal(err)
			}
			if err = store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
				t.Fatal(err)
			}
			nextAssignment := assignment
			nextAssignment.State = domain.AssignmentReleased
			nextAssignment.LeaseExpiresAt = time.Time{}
			nextAssignment.UpdatedAt = now
			nextAttempt := attempt
			nextAttempt.AssignmentID = ""
			nextAttempt.Control = domain.ControlUnassigned
			nextAttempt.Progress = domain.ProgressReady
			nextAttempt.Revision++
			nextAttempt.UpdatedAt = now
			transition := domain.WorkerStateTransition{CoordinatorEpoch: 1, WorkerID: snapshot.WorkerID, WorkerEpoch: test.epoch, WorkerSequence: 1, TransitionedAt: now, ExpectedAssignment: assignment, ExpectedAttemptRevision: 1, Assignment: nextAssignment, Attempt: nextAttempt, Reason: test.reason}
			_, err = store.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{transition})
			if test.ok && err != nil || !test.ok && !errors.Is(err, ErrStaleWorkerStateTransition) {
				t.Fatalf("release ok=%v err=%v", test.ok, err)
			}
		})
	}
}
