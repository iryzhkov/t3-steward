package backlog

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

// Actual durable receipts under the old epoch, followed by a replacement worker
// and a valid sibling. No receipt is synthesized in the planner input.
func TestIndependentRepair1EpochReceiptBoundary(t *testing.T) {
	for _, shape := range []string{"terminal-park", "completed-park"} {
		for _, evidence := range []string{"none", "wrong-observation", "pending-stop", "rejected-stop", "accepted-stop", "accepted-collect"} {
			t.Run(shape+"/"+evidence, func(t *testing.T) {
				ctx := context.Background()
				path := filepath.Join(t.TempDir(), "state.db")
				s, err := sqlitetest.OpenMigrated(path)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { s.Close() }()
				now := coordinatorTestTime.Add(time.Minute)
				snapshot, a, attempt := repair1Fixture(now)
				if shape == "terminal-park" {
					attempt.Progress = domain.ProgressSkipped
				} else {
					at := now.Add(-time.Second)
					attempt.CompletedAt = &at
					attempt.Progress = domain.ProgressWaitingExternal
				}
				a.State = domain.AssignmentUnknown
				if err = s.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{a}}); err != nil {
					t.Fatal(err)
				}
				if err = s.SaveWorkerSnapshot(ctx, snapshot); err != nil {
					t.Fatal(err)
				}
				var old domain.WorkerCommand
				if evidence != "none" && evidence != "wrong-observation" {
					kind := domain.WorkerCommandStop
					if evidence == "accepted-collect" {
						kind = domain.WorkerCommandCollect
					}
					old = domain.WorkerCommand{ID: "old-cleanup", Kind: kind, WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch, CoordinatorEpoch: 1, AssignmentID: a.ID, AssignmentEpoch: 1, ExpectedWorkerSequence: 2, CreatedAt: now}
					if _, err = s.CommitWorkerCommands(ctx, []domain.WorkerCommand{old}); err != nil {
						t.Fatal(err)
					}
					if evidence != "pending-stop" {
						_, err = s.AcknowledgeWorkerCommand(ctx, domain.WorkerAcknowledgement{CommandID: old.ID, WorkerID: old.WorkerID, WorkerEpoch: old.WorkerEpoch, CoordinatorEpoch: 1, AssignmentID: a.ID, AssignmentEpoch: 1, WorkerSequence: 2, Accepted: evidence != "rejected-stop", AcknowledgedAt: now})
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				snapshot.WorkerEpoch = "replacement"
				snapshot.Sequence++
				snapshot.ObservedAt = now.Add(time.Second)
				live := attempt
				live.ID = "zz-live"
				live.TaskID = "live-task"
				live.AssignmentID = "0-live"
				live.Progress = domain.ProgressActive
				live.Control = domain.ControlPreparing
				live.CompletedAt = nil
				la := a
				la.ID = live.AssignmentID
				la.AttemptID = live.ID
				la.WorkerEpoch = snapshot.WorkerEpoch
				la.State = domain.AssignmentClaimed
				la.DispatchToken = "live-token"
				la.LeaseToken = "live-lease"
				snapshot.Assignments = []domain.WorkerAssignmentObservation{{AssignmentID: la.ID, AssignmentEpoch: 1, State: domain.AssignmentClaimed, Control: domain.ControlRunning, ObservedAt: snapshot.ObservedAt}}
				if evidence == "wrong-observation" {
					snapshot.Assignments = append(snapshot.Assignments, domain.WorkerAssignmentObservation{AssignmentID: a.ID, AssignmentEpoch: 2, State: domain.AssignmentCompleted, ObservedAt: snapshot.ObservedAt})
				}
				if err = s.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{live}, Assignments: []domain.Assignment{la}}); err != nil {
					t.Fatal(err)
				}
				if err = s.SaveWorkerSnapshot(ctx, snapshot); err != nil {
					t.Fatal(err)
				}
				before := repair1RawCustody(t, path)
				c := FleetCoordinator{Store: s, Now: func() time.Time { return now.Add(time.Second) }}
				transport := &terminalFenceTransport{}
				settled := evidence == "accepted-collect" // A historical stop receipt alone retains custody.
				for tick := 0; tick < 3; tick++ {
					if _, err = c.ReconcileWorkerCommands(ctx, snapshot, transport); err != nil {
						t.Fatal(err)
					}
					got := repair1Records(t, s)
					at := repair1Attempt(t, got, attempt.ID)
					as := repair1Assignment(t, got, a.ID)
					if !settled {
						if repair1RawCustody(t, path) != before {
							t.Fatal("absence/pending/rejected/wrong-epoch evidence changed old custody")
						}
					} else {
						want := domain.AssignmentReleased
						if evidence == "accepted-collect" {
							want = domain.AssignmentCompleted
						}
						if as.State != want || as.WorkerEpoch != a.WorkerEpoch || at.Control != domain.ControlStopped || at.Progress != attempt.Progress || !reflect.DeepEqual(at.CompletedAt, attempt.CompletedAt) || at.Failure != attempt.Failure || at.CheckpointArtifactID != attempt.CheckpointArtifactID || at.FinalSummaryArtifactID != attempt.FinalSummaryArtifactID {
							t.Fatalf("historical cleanup lost evidence or fabricated transfer: %+v %+v", as, at)
						}
						if want == domain.AssignmentReleased && at.AssignmentID != "" {
							t.Fatal("release kept binding")
						}
					}
					if repair1Attempt(t, got, live.ID).Control != domain.ControlRunning {
						t.Fatal("sibling write lost")
					}
					if tick == 0 {
						if err = s.Close(); err != nil {
							t.Fatal(err)
						}
						s, err = sqlitetest.OpenMigrated(path)
						if err != nil {
							t.Fatal(err)
						}
						c.Store = s
					}
				}
				if len(transport.commands) != 3 {
					t.Fatalf("sibling delivery missing: %+v", transport.commands)
				}
				for _, cmd := range transport.commands {
					if cmd.AssignmentID != la.ID || cmd.Kind != domain.WorkerCommandPrepare {
						t.Fatalf("old ownership fabricated: %+v", cmd)
					}
				}
				if old.ID != "" {
					got, err := s.CommitWorkerCommands(ctx, []domain.WorkerCommand{old})
					if err != nil || !reflect.DeepEqual(got, []domain.WorkerCommand{old}) {
						t.Fatalf("receipt replay lost: %+v %v", got, err)
					}
				}
			})
		}
	}
}

// A live parked attempt retains its park and writer binding while cleanup
// evidence is reconciled. A sibling sorts before it to expose whole-batch failure.
func TestIndependentRepair1ParkCleanupMixedBatch(t *testing.T) {
	for _, shape := range []string{"control-only", "progress-and-control"} {
		for _, evidence := range []string{"released", "accepted-stop", "observed-completed", "accepted-collect"} {
			t.Run(shape+"/"+evidence, func(t *testing.T) {
				ctx := context.Background()
				s, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				now := coordinatorTestTime.Add(time.Minute)
				snapshot, a, attempt := repair1Fixture(now)
				if shape == "progress-and-control" {
					attempt.Progress = domain.ProgressWaitingExternal
				}
				live := attempt
				live.ID = "zz-live"
				live.TaskID = "live-task"
				live.AssignmentID = "0-live"
				live.Progress = domain.ProgressActive
				live.Control = domain.ControlPreparing
				la := a
				la.ID = live.AssignmentID
				la.AttemptID = live.ID
				la.DispatchToken = "live-token"
				la.LeaseToken = "live-lease"
				snapshot.Assignments = []domain.WorkerAssignmentObservation{{AssignmentID: la.ID, AssignmentEpoch: 1, State: domain.AssignmentClaimed, Control: domain.ControlRunning, ObservedAt: now}}
				if evidence == "released" || evidence == "observed-completed" || evidence == "accepted-stop" {
					state := domain.AssignmentReleased
					if evidence == "observed-completed" {
						state = domain.AssignmentCompleted
					}
					snapshot.Assignments = append(snapshot.Assignments, domain.WorkerAssignmentObservation{AssignmentID: a.ID, AssignmentEpoch: 1, State: state, ObservedAt: now})
				}
				if err = s.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt, live}, Assignments: []domain.Assignment{a, la}}); err != nil {
					t.Fatal(err)
				}
				if err = s.SaveWorkerSnapshot(ctx, snapshot); err != nil {
					t.Fatal(err)
				}
				if evidence == "accepted-stop" || evidence == "accepted-collect" {
					kind := domain.WorkerCommandStop
					if evidence == "accepted-collect" {
						kind = domain.WorkerCommandCollect
					}
					repair1Ack(t, s, domain.WorkerCommand{ID: "park-cleanup", Kind: kind, WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch, CoordinatorEpoch: 1, AssignmentID: a.ID, AssignmentEpoch: 1, ExpectedWorkerSequence: 2, CreatedAt: now}, now)
				}
				before := repair1Records(t, s)
				transport := &terminalFenceTransport{}
				c := FleetCoordinator{Store: s, Now: func() time.Time { return now }}
				if _, err = c.ReconcileWorkerCommands(ctx, snapshot, transport); err != nil {
					after := repair1Records(t, s)
					if !reflect.DeepEqual(before, after) || len(transport.commands) != 0 {
						t.Fatal("invalid batch partially committed")
					}
					t.Fatalf("park cleanup blocks valid sibling and transport: %v", err)
				}
				got := repair1Records(t, s)
				at := repair1Attempt(t, got, attempt.ID)
				as := repair1Assignment(t, got, a.ID)
				if at.Progress != attempt.Progress || at.Control != attempt.Control || at.AssignmentID != a.ID || at.CompletedAt != nil {
					t.Fatalf("park/writer binding changed: %+v", at)
				}
				want := domain.AssignmentCompleted
				if evidence == "released" || evidence == "accepted-stop" {
					want = domain.AssignmentReleased
				}
				if evidence == "observed-completed" {
					want = domain.AssignmentClaimed
				}
				if as.State != want {
					t.Fatalf("cleanup evidence missing: %+v", as)
				}
				if repair1Attempt(t, got, live.ID).Control != domain.ControlRunning || len(transport.commands) == 0 || transport.commands[0].AssignmentID != la.ID {
					t.Fatal("valid sibling did not progress")
				}
			})
		}
	}
}
