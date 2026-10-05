package backlog

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func repair1Attempt(t *testing.T, records sqlite.CoordinatorRecords, id string) domain.Attempt {
	t.Helper()
	for _, a := range records.Attempts {
		if a.ID == id {
			return a
		}
	}
	t.Fatalf("missing attempt %s", id)
	return domain.Attempt{}
}
func repair1Assignment(t *testing.T, records sqlite.CoordinatorRecords, id string) domain.Assignment {
	t.Helper()
	for _, a := range records.Assignments {
		if a.ID == id {
			return a
		}
	}
	t.Fatalf("missing assignment %s", id)
	return domain.Assignment{}
}
func repair1Records(t *testing.T, s *sqlite.Store) sqlite.CoordinatorRecords {
	t.Helper()
	r, err := s.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func repair1RawCustody(t *testing.T, path string) [2]string {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var raw [2]string
	if err = db.QueryRow("SELECT record FROM coordinator_attempts WHERE id = 't'").Scan(&raw[0]); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("SELECT record FROM coordinator_assignments WHERE id = 'a'").Scan(&raw[1]); err != nil {
		t.Fatal(err)
	}
	return raw
}
func repair1Fixture(now time.Time) (domain.WorkerSnapshot, domain.Assignment, domain.Attempt) {
	snapshot := coordinatorSnapshot(2)
	snapshot.ObservedAt = now
	snapshot.ValidUntil = now.Add(time.Hour)
	a := domain.Assignment{ID: "a", AttemptID: "t", WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch, Epoch: 1, State: domain.AssignmentClaimed, LeaseToken: "lease", DispatchToken: "dispatch", LeaseExpiresAt: snapshot.ValidUntil}
	attempt := domain.Attempt{ID: "t", WorkflowRunID: "r", TaskID: "task", Number: 1, AssignmentID: a.ID, Revision: 3, Progress: domain.ProgressActive, Control: domain.ControlWaitingExternal, Failure: "keep", CheckpointArtifactID: "cp", FinalSummaryArtifactID: "final"}
	return snapshot, a, attempt
}
func repair1Ack(t *testing.T, s *sqlite.Store, cmd domain.WorkerCommand, now time.Time) {
	t.Helper()
	if _, err := s.CommitWorkerCommands(context.Background(), []domain.WorkerCommand{cmd}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcknowledgeWorkerCommand(context.Background(), domain.WorkerAcknowledgement{CommandID: cmd.ID, WorkerID: cmd.WorkerID, WorkerEpoch: cmd.WorkerEpoch, CoordinatorEpoch: cmd.CoordinatorEpoch, AssignmentID: cmd.AssignmentID, AssignmentEpoch: cmd.AssignmentEpoch, WorkerSequence: cmd.ExpectedWorkerSequence, Accepted: true, AcknowledgedAt: now}); err != nil {
		t.Fatal(err)
	}
}
func TestRuntimeTerminalFencesRepair1FinishedParkCleanup(t *testing.T) {
	now := coordinatorTestTime.Add(time.Minute)
	completed := now.Add(-time.Second)
	shapes := []struct {
		name      string
		progress  domain.ProgressState
		control   domain.ControlState
		completed *time.Time
	}{
		{"cancelled-control", domain.ProgressCancelled, domain.ControlWaitingExternal, nil},
		{"failed-control", domain.ProgressFailed, domain.ControlWaitingExternal, nil},
		{"succeeded-control", domain.ProgressSucceeded, domain.ControlWaitingExternal, nil},
		{"skipped-control", domain.ProgressSkipped, domain.ControlWaitingExternal, nil},
		{"completed-control", domain.ProgressActive, domain.ControlWaitingExternal, &completed},
		{"completed-progress", domain.ProgressWaitingExternal, domain.ControlRunning, &completed},
		{"completed-both", domain.ProgressWaitingExternal, domain.ControlWaitingExternal, &completed},
	}
	for _, shape := range shapes {
		for _, evidence := range []string{"released", "stop", "observed-completed", "collect-observed", "collect-absent"} {
			t.Run(shape.name+"/"+evidence, func(t *testing.T) {
				ctx := context.Background()
				s, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				snapshot, a, attempt := repair1Fixture(now)
				attempt.Progress = shape.progress
				attempt.Control = shape.control
				attempt.CompletedAt = shape.completed
				if evidence == "released" {
					snapshot.Assignments = []domain.WorkerAssignmentObservation{{AssignmentID: a.ID, AssignmentEpoch: a.Epoch, State: domain.AssignmentReleased, ObservedAt: now}}
				}
				if evidence == "observed-completed" || evidence == "collect-observed" {
					snapshot.Assignments = []domain.WorkerAssignmentObservation{{AssignmentID: a.ID, AssignmentEpoch: a.Epoch, State: domain.AssignmentCompleted, ObservedAt: now}}
				}
				if err = s.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{a}}); err != nil {
					t.Fatal(err)
				}
				if err = s.SaveWorkerSnapshot(ctx, snapshot); err != nil {
					t.Fatal(err)
				}
				if evidence == "stop" || evidence == "collect-observed" || evidence == "collect-absent" {
					kind := domain.WorkerCommandCollect
					if evidence == "stop" {
						kind = domain.WorkerCommandStop
					}
					repair1Ack(t, s, domain.WorkerCommand{ID: "cleanup", Kind: kind, WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch, CoordinatorEpoch: 1, AssignmentID: a.ID, AssignmentEpoch: 1, ExpectedWorkerSequence: 2, CreatedAt: now}, now)
				}
				c := FleetCoordinator{Store: s, Now: func() time.Time { return now }}
				transport := &terminalFenceTransport{}
				if _, err = c.ReconcileWorkerCommands(ctx, snapshot, transport); err != nil {
					t.Fatal(err)
				}
				got := repair1Records(t, s)
				at := repair1Attempt(t, got, attempt.ID)
				as := repair1Assignment(t, got, a.ID)
				if at.Control != domain.ControlStopped || at.Progress != attempt.Progress || !reflect.DeepEqual(at.CompletedAt, attempt.CompletedAt) || at.Failure != attempt.Failure || at.CheckpointArtifactID != attempt.CheckpointArtifactID || at.FinalSummaryArtifactID != attempt.FinalSummaryArtifactID {
					t.Fatalf("finished evidence changed: %+v", at)
				}
				wantState := domain.AssignmentCompleted
				if evidence == "released" || evidence == "stop" {
					wantState = domain.AssignmentReleased
					if at.AssignmentID != "" {
						t.Fatal("released binding retained")
					}
				}
				if evidence == "observed-completed" {
					wantState = domain.AssignmentClaimed
					if len(transport.commands) != 1 || transport.commands[0].Kind != domain.WorkerCommandCollect {
						t.Fatalf("collection missing: %+v", transport.commands)
					}
				}
				if as.State != wantState {
					t.Fatalf("custody %+v want %s", as, wantState)
				}
				if wantState != domain.AssignmentReleased && at.AssignmentID != a.ID {
					t.Fatal("unreleased binding lost")
				}
			})
		}
	}
}
func TestRuntimeTerminalFencesRepair1ControlParkDurable(t *testing.T) {
	now := coordinatorTestTime.Add(time.Minute)
	for _, evidence := range []string{"absent", "running", "stale-wait-after-wake"} {
		t.Run(evidence, func(t *testing.T) {
			ctx := context.Background()
			s, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			snapshot, a, attempt := repair1Fixture(now)
			if evidence != "absent" {
				control := domain.ControlRunning
				if evidence == "stale-wait-after-wake" {
					control = domain.ControlWaitingExternal
					attempt.Control = domain.ControlResuming
				}
				snapshot.Assignments = []domain.WorkerAssignmentObservation{{AssignmentID: a.ID, AssignmentEpoch: 1, State: domain.AssignmentClaimed, Control: control, ObservedAt: now}}
			}
			initial := attempt
			initial.Control = domain.ControlRunning
			initial.Revision--
			if err = s.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{initial}, Assignments: []domain.Assignment{a}}); err != nil {
				t.Fatal(err)
			}
			if err = s.SaveWorkerSnapshot(ctx, snapshot); err != nil {
				t.Fatal(err)
			}
			repair1Ack(t, s, domain.WorkerCommand{ID: "old-dispatch", Kind: domain.WorkerCommandDispatch, WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch, CoordinatorEpoch: 1, AssignmentID: a.ID, AssignmentEpoch: 1, ExpectedWorkerSequence: 2, CreatedAt: now}, now)
			if err = s.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{a}}); err != nil {
				t.Fatal(err)
			}
			c := FleetCoordinator{Store: s, Now: func() time.Time { return now }}
			transport := &terminalFenceTransport{}
			if _, err = c.ReconcileWorkerCommands(ctx, snapshot, transport); err != nil {
				t.Fatal(err)
			}
			got := repair1Attempt(t, repair1Records(t, s), attempt.ID)
			if got.Progress != attempt.Progress || got.Control != attempt.Control {
				t.Fatalf("coordinator park/wake overwritten: %+v", got)
			}
			if evidence != "stale-wait-after-wake" && len(transport.commands) != 0 {
				t.Fatalf("park delivered commands: %+v", transport.commands)
			}
		})
	}
}
func TestRuntimeTerminalFencesRepair1OldEpochSafeOmission(t *testing.T) {
	now := coordinatorTestTime.Add(time.Minute)
	completed := now.Add(-time.Second)
	for _, custody := range []domain.AssignmentState{domain.AssignmentClaimed, domain.AssignmentUnknown} {
		for _, progress := range []domain.ProgressState{domain.ProgressCancelled, domain.ProgressFailed, domain.ProgressSucceeded, domain.ProgressSkipped, domain.ProgressActive} {
			for _, control := range []domain.ControlState{domain.ControlPreparing, domain.ControlStopped} {
				t.Run(string(custody)+"/"+string(progress)+"/"+string(control), func(t *testing.T) {
					ctx := context.Background()
					path := filepath.Join(t.TempDir(), "state.db")
					s, err := sqlite.OpenMigrated(path)
					if err != nil {
						t.Fatal(err)
					}
					defer s.Close()
					snapshot, a, attempt := repair1Fixture(now)
					a.State = custody
					attempt.Progress = progress
					attempt.Control = control
					if progress == domain.ProgressActive {
						attempt.CompletedAt = &completed
					}
					snapshot.WorkerEpoch = "new"
					live := attempt
					live.ID = "live"
					live.TaskID = "live-task"
					live.AssignmentID = "0-live"
					live.Progress = domain.ProgressActive
					live.Control = domain.ControlPreparing
					live.CompletedAt = nil
					la := a
					la.ID = live.AssignmentID
					la.AttemptID = live.ID
					la.State = domain.AssignmentClaimed
					la.WorkerEpoch = snapshot.WorkerEpoch
					la.LeaseToken = "live-lease"
					la.DispatchToken = "live-dispatch"
					snapshot.Assignments = []domain.WorkerAssignmentObservation{{AssignmentID: la.ID, AssignmentEpoch: 1, State: domain.AssignmentClaimed, Control: domain.ControlRunning, ObservedAt: now}}
					records := sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt, live}, Assignments: []domain.Assignment{a, la}}
					if err = s.SaveCoordinatorRecords(ctx, records); err != nil {
						t.Fatal(err)
					}
					if err = s.SaveWorkerSnapshot(ctx, snapshot); err != nil {
						t.Fatal(err)
					}
					before := repair1RawCustody(t, path)
					trs, err := PlanWorkerStateTransitions(records, snapshot, nil, now)
					if err != nil || len(trs) != 1 || trs[0].Assignment.ID != la.ID {
						t.Fatalf("unsafe projection emitted / sibling missing: %+v %v", trs, err)
					}
					c := FleetCoordinator{Store: s, Now: func() time.Time { return now }}
					transport := &terminalFenceTransport{}
					for tick := 0; tick < 3; tick++ {
						if _, err = c.ReconcileWorkerCommands(ctx, snapshot, transport); err != nil {
							t.Fatal(err)
						}
						if raw := repair1RawCustody(t, path); raw != before {
							t.Fatal("old custody/attempt raw bytes changed")
						}
						got := repair1Records(t, s)
						if !reflect.DeepEqual(repair1Attempt(t, got, attempt.ID), attempt) || !reflect.DeepEqual(repair1Assignment(t, got, a.ID), a) {
							t.Fatal("old evidence/ownership changed")
						}
						if repair1Attempt(t, got, live.ID).Control != domain.ControlRunning {
							t.Fatal("sibling failed to commit")
						}
					}
					if len(transport.commands) == 0 {
						t.Fatal("sibling transport absent")
					}
					for _, cmd := range transport.commands {
						if cmd.AssignmentID != la.ID || cmd.Kind != domain.WorkerCommandPrepare {
							t.Fatalf("fabricated old-session ownership: %+v", cmd)
						}
					}
					if err = s.Close(); err != nil {
						t.Fatal(err)
					}
					s, err = sqlite.OpenMigrated(path)
					if err != nil {
						t.Fatal(err)
					}
					defer s.Close()
					c.Store = s
					if _, err = c.ReconcileWorkerCommands(ctx, snapshot, transport); err != nil {
						t.Fatal(err)
					}
					if repair1RawCustody(t, path) != before {
						t.Fatal("reopen changed skipped raw evidence")
					}
					// Settle the sibling's real pending Prepare receipt before isolating
					// recovered Stop delivery; replay was valid while unacknowledged.
					repair1Ack(t, s, transport.commands[0], now)
					// A real matching observation, unlike omission, qualifies existing recovery.
					snapshot.Sequence++
					snapshot.ObservedAt = now.Add(time.Second)
					snapshot.Assignments = append(snapshot.Assignments, domain.WorkerAssignmentObservation{AssignmentID: a.ID, AssignmentEpoch: a.Epoch, State: domain.AssignmentClaimed, Control: domain.ControlRunning, ObservedAt: snapshot.ObservedAt})
					if err = s.SaveWorkerSnapshot(ctx, snapshot); err != nil {
						t.Fatal(err)
					}
					transport.commands = nil
					if _, err = c.ReconcileWorkerCommands(ctx, snapshot, transport); err != nil {
						t.Fatal(err)
					}
					got := repair1Records(t, s)
					recovered := repair1Assignment(t, got, a.ID)
					at := repair1Attempt(t, got, attempt.ID)
					if recovered.WorkerEpoch != snapshot.WorkerEpoch || recovered.State != domain.AssignmentClaimed || at.Control != domain.ControlStopped || at.Progress != attempt.Progress || !reflect.DeepEqual(at.CompletedAt, attempt.CompletedAt) || at.Failure != attempt.Failure || at.CheckpointArtifactID != attempt.CheckpointArtifactID || at.FinalSummaryArtifactID != attempt.FinalSummaryArtifactID {
						t.Fatal("matching recovery lost evidence or binding")
					}
					if len(transport.commands) != 1 || transport.commands[0].AssignmentID != a.ID || transport.commands[0].Kind != domain.WorkerCommandStop || transport.commands[0].WorkerEpoch != snapshot.WorkerEpoch {
						t.Fatalf("qualified cleanup missing: %+v", transport.commands)
					}
					cmd := transport.commands[0]
					repair1Ack(t, s, cmd, now)
					snapshot.Sequence++
					snapshot.ObservedAt = now.Add(2 * time.Second)
					snapshot.Assignments = nil
					if err = s.SaveWorkerSnapshot(ctx, snapshot); err != nil {
						t.Fatal(err)
					}
					if _, err = c.ReconcileWorkerCommands(ctx, snapshot, transport); err != nil {
						t.Fatal(err)
					}
					got = repair1Records(t, s)
					at = repair1Attempt(t, got, attempt.ID)
					if repair1Assignment(t, got, a.ID).State != domain.AssignmentReleased || at.AssignmentID != "" || at.Control != domain.ControlStopped || at.Progress != attempt.Progress || !reflect.DeepEqual(at.CompletedAt, attempt.CompletedAt) {
						t.Fatal("acknowledged cleanup failed")
					}
				})
			}
		}
	}
}
