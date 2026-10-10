package sqlite

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestIndependentRepair1TerminalOnlyRollbackAndAudit(t *testing.T) {
	for _, progress := range []domain.ProgressState{domain.ProgressCancelled, domain.ProgressFailed, domain.ProgressSucceeded, domain.ProgressSkipped} {
		t.Run(string(progress), func(t *testing.T) {
			ctx := context.Background()
			s := openFleetTestStore(t)
			claimFleetAssignment(t, s)
			records, err := s.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			a := records.Attempts[0]
			as := records.Assignments[0]
			a.Progress = progress
			a.CompletedAt = nil
			a.Control = domain.ControlWaitingExternal
			a.Revision++
			a.Failure = "keep"
			a.CheckpointArtifactID = "cp"
			a.FinalSummaryArtifactID = "final"
			saveFleetAttempt(t, s, a)
			now := fleetTestTime.Add(3 * time.Second)
			next := a
			next.Control = domain.ControlStopped
			next.Revision++
			next.UpdatedAt = now
			na := as
			na.State = domain.AssignmentCompleted
			na.LeaseExpiresAt = time.Time{}
			na.UpdatedAt = now
			valid := domain.WorkerStateTransition{CoordinatorEpoch: 1, WorkerID: as.WorkerID, WorkerEpoch: as.WorkerEpoch, WorkerSequence: 1, TransitionedAt: now, ExpectedAssignment: as, ExpectedAttemptRevision: a.Revision, Assignment: na, Attempt: next, Reason: "collect-accepted"}
			live := a
			live.ID = "live"
			live.TaskID = "live-task"
			live.AssignmentID = "live-assignment"
			live.Progress = domain.ProgressActive
			live.Control = domain.ControlPreparing
			la := as
			la.ID = live.AssignmentID
			la.AttemptID = live.ID
			la.DispatchToken = "live-token"
			la.LeaseToken = "live-lease"
			if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{live}, Assignments: []domain.Assignment{la}}); err != nil {
				t.Fatal(err)
			}
			first := valid
			first.ExpectedAssignment = la
			first.Assignment = la
			first.Attempt = live
			first.Attempt.Control = domain.ControlRunning
			first.Attempt.Revision++
			first.Attempt.UpdatedAt = now
			for _, mutation := range []string{"ready", "park", "completion", "failure", "checkpoint", "summary", "old-epoch"} {
				forged := valid
				switch mutation {
				case "ready":
					forged.Attempt.Progress = domain.ProgressReady
				case "park":
					forged.Attempt.Control = domain.ControlWaitingExternal
				case "completion":
					forged.Attempt.CompletedAt = &now
				case "failure":
					forged.Attempt.Failure = ""
				case "checkpoint":
					forged.Attempt.CheckpointArtifactID = ""
				case "summary":
					forged.Attempt.FinalSummaryArtifactID = ""
				case "old-epoch":
					forged.Assignment.State = domain.AssignmentUnknown
					forged.Assignment.WorkerEpoch = "old"
				}
				before := cancellationSnapshot(t, s)
				if _, err = s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{first, forged}); !errors.Is(err, ErrStaleWorkerStateTransition) {
					t.Fatalf("%s accepted forged projection: %v", mutation, err)
				}
				if cancellationSnapshot(t, s) != before {
					t.Fatalf("%s committed partial writes/audit", mutation)
				}
			}
			s = repair2Reopen(t, s)
			if _, err = s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{first, valid}); err != nil {
				t.Fatal(err)
			}
			before := cancellationSnapshot(t, s)
			if _, err = s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{first, valid}); err != nil || before != cancellationSnapshot(t, s) {
				t.Fatalf("valid native replay: %v", err)
			}
			if _, err = s.db.ExecContext(ctx, "DELETE FROM coordinator_audit_events WHERE id = ?", workerStateAuditID(valid)); err != nil {
				t.Fatal(err)
			}
			before = cancellationSnapshot(t, s)
			if _, err = s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{valid}); err == nil || before != cancellationSnapshot(t, s) {
				t.Fatalf("unaudited replay accepted/mutated: %v", err)
			}
		})
	}
}

func TestIndependentRepair1MixedPendingDelivery(t *testing.T) {
	for _, kind := range []domain.WorkerCommandKind{domain.WorkerCommandPrepare, domain.WorkerCommandDispatch} {
		for _, shape := range []string{"completed-ready", "terminal-park"} {
			t.Run(string(kind)+"/"+shape, func(t *testing.T) {
				ctx := context.Background()
				s := openFleetTestStore(t)
				claimFleetAssignment(t, s)
				old := fleetWorkerCommand(kind, "old-start", 1, fleetTestTime.Add(2*time.Second))
				if _, err := s.CommitWorkerCommands(ctx, []domain.WorkerCommand{old}); err != nil {
					t.Fatal(err)
				}
				records, err := s.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				a := records.Attempts[0]
				a.Revision++
				if shape == "completed-ready" {
					a.Progress = domain.ProgressReady
					a.Control = domain.ControlResuming
					at := fleetTestTime.Add(time.Second)
					a.CompletedAt = &at
				} else {
					a.Progress = domain.ProgressSkipped
					a.Control = domain.ControlWaitingExternal
				}
				saveFleetAttempt(t, s, a)
				live := a
				live.ID = "live"
				live.TaskID = "live-task"
				live.AssignmentID = "live-assignment"
				live.Progress = domain.ProgressActive
				live.Control = domain.ControlPreparing
				live.CompletedAt = nil
				la := records.Assignments[0]
				la.ID = live.AssignmentID
				la.AttemptID = live.ID
				la.DispatchToken = "live-token"
				la.LeaseToken = "live-lease"
				if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{live}, Assignments: []domain.Assignment{la}}); err != nil {
					t.Fatal(err)
				}
				sibling := old
				sibling.ID = "0-live"
				sibling.AssignmentID = la.ID
				fresh := old
				fresh.ID = "fresh-start"
				cleanup := old
				cleanup.ID = "cleanup"
				cleanup.Kind = domain.WorkerCommandCollect
				got, err := s.CommitWorkerCommands(ctx, []domain.WorkerCommand{sibling, fresh, cleanup})
				if err != nil || !reflect.DeepEqual(got, []domain.WorkerCommand{sibling, cleanup}) {
					t.Fatalf("mixed commit: %+v %v", got, err)
				}
				for reopen := 0; reopen < 2; reopen++ {
					before := cancellationSnapshot(t, s)
					pending, err := s.LoadPendingWorkerCommands(ctx, old.WorkerID, old.WorkerEpoch, 1, 1, fleetTestTime.Add(3*time.Second))
					if err != nil || !reflect.DeepEqual(pending, []domain.WorkerCommand{sibling, cleanup}) || before != cancellationSnapshot(t, s) {
						t.Fatalf("pending mutated or delivered ineligible start: %+v %v", pending, err)
					}
					got, err = s.CommitWorkerCommands(ctx, []domain.WorkerCommand{old})
					if err != nil || !reflect.DeepEqual(got, []domain.WorkerCommand{old}) || before != cancellationSnapshot(t, s) {
						t.Fatalf("receipt changed: %+v %v", got, err)
					}
					if reopen == 0 {
						s = repair2Reopen(t, s)
					}
				}
			})
		}
	}
}
