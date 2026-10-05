package backlog

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestIndependentRuntimeTerminalFencesSkippedAndEpoch(t *testing.T) {
	now := coordinatorTestTime.Add(time.Minute)
	for _, evidence := range []string{"none", "restart", "running", "completed", "released", "stop", "collect"} {
		t.Run(evidence, func(t *testing.T) {
			snapshot := coordinatorSnapshot(2)
			snapshot.ObservedAt = now
			snapshot.ValidUntil = now.Add(time.Hour)
			a := domain.Assignment{ID: "a", AttemptID: "t", WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch, Epoch: 1, State: domain.AssignmentUnknown, LeaseExpiresAt: snapshot.ValidUntil}
			attempt := domain.Attempt{ID: "t", AssignmentID: "a", Revision: 3, Progress: domain.ProgressSkipped, Control: domain.ControlRunning, Failure: "keep", CheckpointArtifactID: "cp", FinalSummaryArtifactID: "final"}
			commands := []domain.WorkerCommandRecord{workerCommandRecord(snapshot, a, domain.WorkerCommandDispatch, true)}
			switch evidence {
			case "restart":
				snapshot.WorkerEpoch = "new"
			case "running", "completed", "released":
				state := domain.AssignmentClaimed
				if evidence == "completed" {
					state = domain.AssignmentCompleted
				}
				if evidence == "released" {
					state = domain.AssignmentReleased
				}
				snapshot.Assignments = []domain.WorkerAssignmentObservation{{AssignmentID: a.ID, AssignmentEpoch: 1, State: state, Control: domain.ControlRunning, ObservedAt: now}}
			case "stop":
				commands = append(commands, workerCommandRecord(snapshot, a, domain.WorkerCommandStop, true))
			case "collect":
				commands = append(commands, workerCommandRecord(snapshot, a, domain.WorkerCommandCollect, true))
			}
			trs, err := PlanWorkerStateTransitions(sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{a}}, snapshot, commands, now)
			if evidence == "restart" {
				if err != nil || len(trs) != 0 {
					t.Fatalf("unsafe old-epoch repair was not omitted: %+v %v", trs, err)
				}
				planned, e := PlanWorkerCommands(sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{a}}, snapshot, commands, now)
				if e != nil || len(planned) != 0 {
					t.Fatalf("invented new-session ownership: %+v %v", planned, e)
				}
				return
			}
			if err != nil || len(trs) != 1 {
				t.Fatalf("transition %+v %v", trs, err)
			}
			got := trs[0].Attempt
			if got.Progress != attempt.Progress || got.Control != domain.ControlStopped || got.Failure != attempt.Failure || got.CheckpointArtifactID != attempt.CheckpointArtifactID || got.FinalSummaryArtifactID != attempt.FinalSummaryArtifactID {
				t.Fatalf("protected evidence %+v", got)
			}
		})
	}
}
func TestIndependentRuntimeTerminalFencesControlPark(t *testing.T) {
	now := coordinatorTestTime.Add(time.Minute)
	for _, observed := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "running"}[observed], func(t *testing.T) {
			snapshot := coordinatorSnapshot(2)
			snapshot.ObservedAt = now
			snapshot.ValidUntil = now.Add(time.Hour)
			a := domain.Assignment{ID: "a", AttemptID: "t", WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch, Epoch: 1, State: domain.AssignmentClaimed, LeaseExpiresAt: snapshot.ValidUntil}
			attempt := domain.Attempt{ID: "t", AssignmentID: "a", Revision: 3, Progress: domain.ProgressActive, Control: domain.ControlWaitingExternal}
			if observed {
				snapshot.Assignments = []domain.WorkerAssignmentObservation{{AssignmentID: a.ID, AssignmentEpoch: 1, State: domain.AssignmentClaimed, Control: domain.ControlRunning, ObservedAt: now}}
			}
			records := sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{a}}
			commands := []domain.WorkerCommandRecord{workerCommandRecord(snapshot, a, domain.WorkerCommandDispatch, true)}
			trs, err := PlanWorkerStateTransitions(records, snapshot, commands, now)
			if err != nil {
				t.Fatal(err)
			}
			for _, tr := range trs {
				if tr.Attempt.Progress != attempt.Progress || tr.Attempt.Control != attempt.Control {
					t.Errorf("park lost without wake: %+v", tr.Attempt)
				}
				records.Attempts[0] = tr.Attempt
			}
			planned, err := PlanWorkerCommands(records, snapshot, commands, now)
			if err != nil || len(planned) != 0 {
				t.Fatalf("park commands %+v %v", planned, err)
			}
		})
	}
}
func TestIndependentRuntimeTerminalFencesDurableCleanup(t *testing.T) {
	for _, evidence := range []string{"released", "collect", "restart"} {
		t.Run(evidence, func(t *testing.T) {
			ctx := context.Background()
			s, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			now := coordinatorTestTime.Add(time.Minute)
			completed := now.Add(-time.Second)
			snapshot := coordinatorSnapshot(2)
			snapshot.ObservedAt = now
			snapshot.ValidUntil = now.Add(time.Hour)
			attempt := domain.Attempt{ID: "t", WorkflowRunID: "r", TaskID: "task", Number: 1, AssignmentID: "a", Revision: 3, Progress: domain.ProgressActive, Control: domain.ControlWaitingExternal, CompletedAt: &completed, Failure: "keep", CheckpointArtifactID: "cp", FinalSummaryArtifactID: "final"}
			a := domain.Assignment{ID: "a", AttemptID: "t", WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch, Epoch: 1, State: domain.AssignmentClaimed, LeaseToken: "lease", DispatchToken: "dispatch", LeaseExpiresAt: snapshot.ValidUntil}
			var commands []domain.WorkerCommandRecord
			if evidence == "restart" {
				attempt.Control = domain.ControlPreparing
				snapshot.WorkerEpoch = "new"
			} else {
				state := domain.AssignmentReleased
				if evidence == "collect" {
					state = domain.AssignmentCompleted
					commands = []domain.WorkerCommandRecord{workerCommandRecord(snapshot, a, domain.WorkerCommandCollect, true)}
				}
				snapshot.Assignments = []domain.WorkerAssignmentObservation{{AssignmentID: a.ID, AssignmentEpoch: 1, State: state, Control: domain.ControlStopped, ObservedAt: now}}
			}
			live := attempt
			live.ID = "zz-live"
			live.TaskID = "live-task"
			live.AssignmentID = "0-live"
			live.CompletedAt = nil
			live.Control = domain.ControlPreparing
			la := a
			la.ID = "0-live"
			la.AttemptID = live.ID
			la.WorkerEpoch = snapshot.WorkerEpoch
			la.LeaseToken = "live-lease"
			la.DispatchToken = "live-dispatch"
			snapshot.Assignments = append(snapshot.Assignments, domain.WorkerAssignmentObservation{AssignmentID: la.ID, AssignmentEpoch: 1, State: domain.AssignmentClaimed, Control: domain.ControlRunning, ObservedAt: now})
			records := sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt, live}, Assignments: []domain.Assignment{a, la}}
			if err = s.SaveCoordinatorRecords(ctx, records); err != nil {
				t.Fatal(err)
			}
			if err = s.SaveWorkerSnapshot(ctx, snapshot); err != nil {
				t.Fatal(err)
			}
			if evidence == "collect" {
				cmd := domain.WorkerCommand{ID: "actual-collect", Kind: domain.WorkerCommandCollect, WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch, CoordinatorEpoch: 1, AssignmentID: a.ID, AssignmentEpoch: 1, ExpectedWorkerSequence: 2, CreatedAt: now}
				if _, err = s.CommitWorkerCommands(ctx, []domain.WorkerCommand{cmd}); err != nil {
					t.Fatal(err)
				}
				if _, err = s.AcknowledgeWorkerCommand(ctx, domain.WorkerAcknowledgement{CommandID: cmd.ID, WorkerID: cmd.WorkerID, WorkerEpoch: cmd.WorkerEpoch, CoordinatorEpoch: 1, AssignmentID: a.ID, AssignmentEpoch: 1, WorkerSequence: 2, Accepted: true, AcknowledgedAt: now}); err != nil {
					t.Fatal(err)
				}
				commands, err = s.LoadWorkerCommandRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
			}
			trs, err := PlanWorkerStateTransitions(records, snapshot, commands, now)
			wantTransitions := 2
			if evidence == "restart" {
				wantTransitions = 1
			}
			if err != nil || len(trs) != wantTransitions {
				t.Fatalf("mixed plan %+v %v", trs, err)
			}
			if evidence == "restart" && trs[0].Assignment.ID != la.ID {
				t.Fatal("old-epoch projection emitted")
			}
			before, err := s.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			transport := &terminalFenceTransport{}
			c := FleetCoordinator{Store: s, Now: func() time.Time { return now }}
			if _, err = c.ReconcileWorkerCommands(ctx, snapshot, transport); err != nil {
				after, e := s.LoadCoordinatorRecords(ctx)
				if e != nil {
					t.Fatal(e)
				}
				if !reflect.DeepEqual(before, after) || len(transport.commands) != 0 {
					t.Fatal("invalid cleanup partially committed or sent")
				}
				t.Fatalf("planner cleanup rejects entire mixed coordinator batch, live sibling rolled back: %v", err)
			}
			got, err := s.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if evidence == "restart" {
				if !reflect.DeepEqual(repair1Attempt(t, got, attempt.ID), attempt) || !reflect.DeepEqual(repair1Assignment(t, got, a.ID), a) {
					t.Fatal("unsafe skipped custody/evidence changed")
				}
				if repair1Attempt(t, got, live.ID).Control != domain.ControlRunning || len(transport.commands) != 1 || transport.commands[0].AssignmentID != la.ID {
					t.Fatal("safe sibling did not commit and deliver")
				}
				return
			}
			if got.Attempts[0].Control != domain.ControlStopped || got.Attempts[0].Progress != attempt.Progress || !reflect.DeepEqual(got.Attempts[0].CompletedAt, attempt.CompletedAt) {
				t.Fatalf("completion lost %+v", got.Attempts)
			}
		})
	}
}
