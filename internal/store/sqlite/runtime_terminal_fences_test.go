package sqlite

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestRuntimeTerminalFencesCommands(t *testing.T) {
	for _, kind := range []domain.WorkerCommandKind{domain.WorkerCommandPrepare, domain.WorkerCommandDispatch} {
		for _, progress := range []domain.ProgressState{domain.ProgressCancelled, domain.ProgressFailed, domain.ProgressSucceeded, domain.ProgressActive, domain.ProgressWaitingExternal} {
			t.Run(string(kind)+"/"+string(progress), func(t *testing.T) {
				ctx := context.Background()
				s := openFleetTestStore(t)
				claimFleetAssignment(t, s)
				cmd := fleetWorkerCommand(kind, "old-start", 1, fleetTestTime.Add(2*time.Second))
				if got, err := s.CommitWorkerCommands(ctx, []domain.WorkerCommand{cmd}); err != nil || len(got) != 1 {
					t.Fatalf("live commit %+v %v", got, err)
				}
				records, err := s.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				a := records.Attempts[0]
				a.Progress = progress
				a.Control = domain.ControlStopped
				a.Failure = "retained failure"
				a.CheckpointArtifactID = "checkpoint"
				a.FinalSummaryArtifactID = "summary"
				a.Revision++
				completed := fleetTestTime.Add(3 * time.Second)
				if progress == domain.ProgressActive {
					a.CompletedAt = &completed
				}
				if progress == domain.ProgressWaitingExternal {
					a.Control = domain.ControlWaitingExternal
				}
				saveFleetAttempt(t, s, a)
				// A live sibling survives filtering of the earlier sorted stale command.
				sibling := a
				sibling.ID = "sibling"
				sibling.TaskID = "sibling-task"
				sibling.Progress = domain.ProgressActive
				sibling.Control = domain.ControlPreparing
				sibling.CompletedAt = nil
				sibling.AssignmentID = "sibling-assignment"
				assignment := records.Assignments[0]
				assignment.ID = sibling.AssignmentID
				assignment.AttemptID = sibling.ID
				assignment.DispatchToken = "sibling-token"
				assignment.LeaseToken = "sibling-lease"
				live := cmd
				live.ID = "z-live"
				live.AssignmentID = assignment.ID
				if err := s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{sibling}, Assignments: []domain.Assignment{assignment}}); err != nil {
					t.Fatal(err)
				}
				if _, err := s.CommitWorkerCommands(ctx, []domain.WorkerCommand{live}); err != nil {
					t.Fatal(err)
				}
				before := cancellationSnapshot(t, s)
				// Exact receipt replay is still benign history, with no mutation.
				if got, err := s.CommitWorkerCommands(ctx, []domain.WorkerCommand{cmd}); err != nil || !reflect.DeepEqual(got, []domain.WorkerCommand{cmd}) {
					t.Fatalf("receipt replay %+v %v", got, err)
				}
				if before != cancellationSnapshot(t, s) {
					t.Fatal("receipt replay mutated history")
				}
				// A new start on terminal custody is withheld, without blocking a valid cleanup.
				fresh := cmd
				fresh.ID = "fresh-start"
				cleanup := cmd
				cleanup.ID = "cleanup"
				cleanup.Kind = domain.WorkerCommandStop
				got, err := s.CommitWorkerCommands(ctx, []domain.WorkerCommand{fresh, cleanup})
				if err != nil || !reflect.DeepEqual(got, []domain.WorkerCommand{cleanup}) {
					t.Fatalf("new start/cleanup %+v %v", got, err)
				}
				for reopen := 0; reopen < 2; reopen++ {
					pending, err := s.LoadPendingWorkerCommands(ctx, cmd.WorkerID, cmd.WorkerEpoch, 1, 1, fleetTestTime.Add(4*time.Second))
					if err != nil || len(pending) != 2 {
						t.Fatalf("pending %+v %v", pending, err)
					}
					for _, c := range pending {
						if c.ID != cleanup.ID && c.ID != live.ID {
							t.Fatalf("stale start delivered %+v", c)
						}
					}
					if reopen == 0 {
						s = repair2Reopen(t, s)
					}
				}
				// Cancellation does not revoke a worker's ability to report an
				// already processed effect; acknowledgement facts remain idempotent.
				ack := fleetWorkerAcknowledgement(cmd, 1, fleetTestTime.Add(4*time.Second))
				if _, err := s.AcknowledgeWorkerCommand(ctx, ack); err != nil {
					t.Fatal(err)
				}
				before = cancellationSnapshot(t, s)
				if _, err := s.AcknowledgeWorkerCommand(ctx, ack); err != nil || before != cancellationSnapshot(t, s) {
					t.Fatalf("ack replay %v", err)
				}
			})
		}
	}
}

func TestRuntimeTerminalFencesForgedTransitionRollback(t *testing.T) {
	for _, progress := range []domain.ProgressState{domain.ProgressCancelled, domain.ProgressFailed, domain.ProgressSucceeded, domain.ProgressActive} {
		for _, mutation := range []string{"active", "ready", "verifying", "preparing", "resuming", "running", "completion", "failure", "checkpoint", "summary"} {
			t.Run(string(progress)+"/"+mutation, func(t *testing.T) {
				ctx := context.Background()
				s := openFleetTestStore(t)
				claimFleetAssignment(t, s)
				records, err := s.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				a := records.Attempts[0]
				a.Progress = progress
				a.Control = domain.ControlStopped
				completed := fleetTestTime.Add(2 * time.Second)
				a.CompletedAt = &completed
				a.Failure = "failure"
				a.CheckpointArtifactID = "checkpoint"
				a.FinalSummaryArtifactID = "summary"
				a.Revision++
				saveFleetAttempt(t, s, a)
				assignment := records.Assignments[0]
				now := fleetTestTime.Add(3 * time.Second)
				next := a
				next.Revision++
				next.UpdatedAt = now
				nextAssignment := assignment
				nextAssignment.State = domain.AssignmentCompleted
				nextAssignment.LeaseExpiresAt = time.Time{}
				nextAssignment.UpdatedAt = now
				valid := domain.WorkerStateTransition{CoordinatorEpoch: 1, WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch, WorkerSequence: 1, TransitionedAt: now, ExpectedAssignment: assignment, ExpectedAttemptRevision: a.Revision, Assignment: nextAssignment, Attempt: next, Reason: "collect-accepted"}
				// First write in batch is a valid independent live sibling. The
				// forged current-revision second transition must roll it all back.
				sibling := a
				sibling.ID = "sibling"
				sibling.TaskID = "sibling-task"
				sibling.AssignmentID = "sibling-assignment"
				sibling.Progress = domain.ProgressActive
				sibling.Control = domain.ControlPreparing
				sibling.CompletedAt = nil
				siblingAssignment := assignment
				siblingAssignment.ID = sibling.AssignmentID
				siblingAssignment.AttemptID = sibling.ID
				siblingAssignment.DispatchToken = "sibling-token"
				siblingAssignment.LeaseToken = "sibling-lease"
				if err := s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{sibling}, Assignments: []domain.Assignment{siblingAssignment}}); err != nil {
					t.Fatal(err)
				}
				first := valid
				first.ExpectedAssignment = siblingAssignment
				first.Assignment = siblingAssignment
				first.Attempt = sibling
				first.Attempt.Control = domain.ControlRunning
				first.Attempt.Revision++
				first.Attempt.UpdatedAt = now
				forged := valid
				switch mutation {
				case "active":
					forged.Attempt.Progress = domain.ProgressActive
					forged.Attempt.Control = domain.ControlRunning
				case "ready":
					forged.Attempt.Progress = domain.ProgressReady
				case "verifying":
					forged.Attempt.Progress = domain.ProgressVerifying
				case "preparing":
					forged.Attempt.Control = domain.ControlPreparing
				case "resuming":
					forged.Attempt.Control = domain.ControlResuming
				case "running":
					forged.Attempt.Control = domain.ControlRunning
				case "completion":
					forged.Attempt.CompletedAt = nil
				case "failure":
					forged.Attempt.Failure = ""
				case "checkpoint":
					forged.Attempt.CheckpointArtifactID = ""
				case "summary":
					forged.Attempt.FinalSummaryArtifactID = ""
				}
				before := cancellationSnapshot(t, s)
				if _, err := s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{first, forged}); !errors.Is(err, ErrStaleWorkerStateTransition) {
					t.Fatalf("forged transition error %v", err)
				}
				if before != cancellationSnapshot(t, s) {
					t.Fatal("forged batch partially committed")
				}
				s = repair2Reopen(t, s)
				if before != cancellationSnapshot(t, s) {
					t.Fatal("rollback not durable")
				}
				if _, err := s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{first, valid}); err != nil {
					t.Fatal(err)
				}
				before = cancellationSnapshot(t, s)
				if _, err := s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{first, valid}); err != nil || before != cancellationSnapshot(t, s) {
					t.Fatalf("exact replay %v", err)
				}
			})
		}
	}
}
