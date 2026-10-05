package backlog

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// Simulated transport: no live worker and no implied acknowledgement.
type terminalFenceTransport struct{ commands []domain.WorkerCommand }

func (t *terminalFenceTransport) DeliverWorkerCommands(_ context.Context, _ domain.WorkerSnapshot, commands []domain.WorkerCommand) ([]domain.WorkerAcknowledgement, error) {
	t.commands = append(t.commands, commands...)
	return nil, nil
}

func TestRuntimeTerminalFencesEvidenceAndCleanup(t *testing.T) {
	now := coordinatorTestTime.Add(time.Minute)
	completed := now.Add(-time.Second)
	for _, progress := range []domain.ProgressState{domain.ProgressCancelled, domain.ProgressFailed, domain.ProgressSucceeded, domain.ProgressActive} {
		for _, evidence := range []string{"none", "restart", "running", "unknown", "completed", "stop-ack", "collect-ack", "released"} {
			t.Run(string(progress)+"/"+evidence, func(t *testing.T) {
				snapshot := coordinatorSnapshot(2)
				snapshot.ObservedAt = now
				snapshot.ValidUntil = now.Add(time.Hour)
				assignment := domain.Assignment{ID: "a", AttemptID: "t", WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch, State: domain.AssignmentClaimed, Epoch: 1, LeaseExpiresAt: snapshot.ValidUntil}
				attempt := domain.Attempt{ID: "t", AssignmentID: "a", Revision: 3, Progress: progress, Control: domain.ControlPreparing, Failure: "retained", CheckpointArtifactID: "checkpoint", FinalSummaryArtifactID: "summary"}
				if progress == domain.ProgressActive {
					attempt.CompletedAt = &completed
				}
				commands := []domain.WorkerCommandRecord{workerCommandRecord(snapshot, assignment, domain.WorkerCommandDispatch, true)}
				switch evidence {
				case "restart":
					snapshot.WorkerEpoch = "new-worker-epoch"
				case "running", "unknown", "completed", "released":
					state := domain.AssignmentClaimed
					if evidence == "unknown" {
						state = domain.AssignmentUnknown
					}
					if evidence == "completed" {
						state = domain.AssignmentCompleted
					}
					if evidence == "released" {
						state = domain.AssignmentReleased
					}
					snapshot.Assignments = []domain.WorkerAssignmentObservation{{AssignmentID: "a", AssignmentEpoch: 1, State: state, Control: domain.ControlRunning, ObservedAt: now}}
				case "stop-ack":
					commands = append(commands, workerCommandRecord(snapshot, assignment, domain.WorkerCommandStop, true))
				case "collect-ack":
					commands = append(commands, workerCommandRecord(snapshot, assignment, domain.WorkerCommandCollect, true))
				}
				records := sqlite.CoordinatorRecords{Assignments: []domain.Assignment{assignment}, Attempts: []domain.Attempt{attempt}}
				trs, err := PlanWorkerStateTransitions(records, snapshot, commands, now)
				if err != nil || len(trs) != 1 {
					t.Fatalf("transition %+v %v", trs, err)
				}
				got := trs[0]
				if got.Attempt.Progress != attempt.Progress || got.Attempt.Control != domain.ControlStopped ||
					!reflect.DeepEqual(got.Attempt.CompletedAt, attempt.CompletedAt) || got.Attempt.Failure != attempt.Failure ||
					got.Attempt.CheckpointArtifactID != attempt.CheckpointArtifactID || got.Attempt.FinalSummaryArtifactID != attempt.FinalSummaryArtifactID {
					t.Fatalf("lost protected evidence %+v", got)
				}
				wantState := domain.AssignmentClaimed
				if evidence == "unknown" {
					wantState = domain.AssignmentUnknown
				}
				if evidence == "stop-ack" || evidence == "released" {
					wantState = domain.AssignmentReleased
				}
				if evidence == "collect-ack" {
					wantState = domain.AssignmentCompleted
				}
				if got.Assignment.State != wantState {
					t.Fatalf("custody %s want %s", got.Assignment.State, wantState)
				}
				// Direct command planning also fences finished attempts whose control
				// has not yet been repaired to stopped.
				planned, err := PlanWorkerCommands(records, snapshot, commands, now)
				if err != nil {
					t.Fatal(err)
				}
				if evidence == "restart" || evidence == "stop-ack" {
					return
				}
				want := domain.WorkerCommandStop
				if evidence == "completed" {
					want = domain.WorkerCommandCollect
				}
				if len(planned) != 1 || planned[0].Kind != want {
					t.Fatalf("cleanup %+v", planned)
				}
			})
		}
	}
}

func TestRuntimeTerminalFencesOldDispatch(t *testing.T) {
	now := coordinatorTestTime.Add(time.Minute)
	completed := now.Add(-time.Second)
	snapshot := coordinatorSnapshot(2)
	snapshot.ObservedAt = now
	snapshot.ValidUntil = now.Add(time.Hour)
	for _, progress := range []domain.ProgressState{domain.ProgressCancelled, domain.ProgressFailed, domain.ProgressSucceeded, domain.ProgressActive, domain.ProgressWaitingExternal} {
		for _, custody := range []domain.AssignmentState{domain.AssignmentClaimed, domain.AssignmentUnknown} {
			t.Run(string(progress)+"/"+string(custody), func(t *testing.T) {
				a := domain.Assignment{ID: "a", AttemptID: "t", WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch, State: custody, Epoch: 1, LeaseExpiresAt: snapshot.ValidUntil}
				attempt := domain.Attempt{ID: "t", AssignmentID: a.ID, Revision: 3, Progress: progress, Control: domain.ControlStopped}
				if progress == domain.ProgressActive {
					attempt.CompletedAt = &completed
				}
				if progress == domain.ProgressWaitingExternal {
					attempt.Control = domain.ControlWaitingExternal
				}
				commands := []domain.WorkerCommandRecord{workerCommandRecord(snapshot, a, domain.WorkerCommandDispatch, true)}
				records := sqlite.CoordinatorRecords{Assignments: []domain.Assignment{a}, Attempts: []domain.Attempt{attempt}}
				for tick := 0; tick < 3; tick++ {
					trs, err := PlanWorkerStateTransitions(records, snapshot, commands, now)
					if err != nil {
						t.Fatal(err)
					}
					for _, tr := range trs {
						if tr.Attempt.Progress != attempt.Progress || !reflect.DeepEqual(tr.Attempt.CompletedAt, attempt.CompletedAt) || tr.Attempt.Control != attempt.Control {
							t.Fatalf("old dispatch changed protected attempt: %+v", tr.Attempt)
						}
						if progress != domain.ProgressWaitingExternal && tr.Assignment.State != custody {
							t.Fatal("terminal custody released without boundary evidence")
						}
						records.Attempts[0] = tr.Attempt
						records.Assignments[0] = tr.Assignment
					}
					if progress != domain.ProgressWaitingExternal {
						planned, err := PlanWorkerCommands(records, snapshot, commands, now)
						if err != nil || len(planned) != 1 || planned[0].Kind != domain.WorkerCommandStop {
							t.Fatalf("cleanup: %+v %v", planned, err)
						}
					}
				}
			})
		}
	}
}
