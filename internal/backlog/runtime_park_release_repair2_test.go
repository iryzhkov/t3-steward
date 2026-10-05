package backlog

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// Registration and the real release planner/store precede the ready wait.
// The wake owner, not the release projection, revokes the lost execution.
func TestRuntimeTerminalFencesRegisteredParkReleaseAbandonment(t *testing.T) {
	for _, evidence := range []string{"released", "accepted-stop"} {
		t.Run(evidence, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "state.db")
			s, err := sqlite.OpenMigrated(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { s.Close() }()
			records, snapshot := parkedReleaseRecords()
			now := parkedReleaseTime
			original := records.Attempts[0]
			original.Progress = domain.ProgressActive
			original.Control = domain.ControlRunning
			records.Attempts[0] = original
			records.WorkflowRuns[0].Progress = domain.ProgressActive
			records.Tasks[0].Class = domain.TaskClassRequired
			records.Tasks[0].DirectoryBindings = parkedTestBindings()
			records.WorkflowRuns[0], err = domain.BindRunSink(records.WorkflowRuns[0], records.Tasks)
			if err != nil {
				t.Fatal(err)
			}
			snapshot.Inventory = domain.WorkerInventory{ID: snapshot.WorkerID, Health: domain.WorkerHealthReady, AcceptBacklog: true}
			if err = s.SaveCoordinatorRecords(ctx, records); err != nil {
				t.Fatal(err)
			}
			if err = s.SaveWorkerSnapshot(ctx, snapshot); err != nil {
				t.Fatal(err)
			}
			wait, err := s.RegisterTaskWait(ctx, domain.TaskWaitRegistration{RequestID: "actual-park", WorkflowRunID: original.WorkflowRunID, TaskID: original.TaskID, AttemptID: original.ID, IssuedRevision: original.Revision, ThreadID: original.ThreadID, Wake: domain.WakeEach, MaxDuration: time.Hour, Name: "ci", Condition: "test -f signal"}, now)
			if err != nil {
				t.Fatal(err)
			}
			parked := repair1Attempt(t, repair1Records(t, s), original.ID)
			if parked.LastTurnOutcomeMarker != domain.TurnOutcomeWaiting {
				t.Fatal("registration did not own park")
			}
			if owners := directoryOwners([]domain.Attempt{parked}, records.Assignments, records.WorkflowRuns, records.Tasks); len(owners) != 1 {
				t.Fatal("registered park lost live directory ownership")
			}
			live := original
			live.ID = "zz-live"
			live.TaskID = "live-task"
			live.AssignmentID = "0-live"
			live.Control = domain.ControlPreparing
			la := records.Assignments[0]
			la.ID = live.AssignmentID
			la.AttemptID = live.ID
			la.LeaseToken = "live-lease"
			la.DispatchToken = "live-token"
			if err = s.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{live}, Assignments: []domain.Assignment{la}}); err != nil {
				t.Fatal(err)
			}
			if evidence == "released" {
				snapshot.Assignments = append(snapshot.Assignments, domain.WorkerAssignmentObservation{AssignmentID: original.AssignmentID, AssignmentEpoch: 2, State: domain.AssignmentReleased, ObservedAt: now})
			} else {
				repair1Ack(t, s, domain.WorkerCommand{ID: "real-stop", Kind: domain.WorkerCommandStop, WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch, CoordinatorEpoch: 1, AssignmentID: original.AssignmentID, AssignmentEpoch: 2, ExpectedWorkerSequence: 1, CreatedAt: now}, now)
			}
			snapshot.Assignments = append(snapshot.Assignments, domain.WorkerAssignmentObservation{AssignmentID: la.ID, AssignmentEpoch: 2, State: domain.AssignmentClaimed, Control: domain.ControlRunning, ObservedAt: now})
			// Save a later snapshot sequence, so both receipt and observed paths are real.
			snapshot.Sequence++
			snapshot.ObservedAt = now.Add(time.Second)
			if err = s.SaveWorkerSnapshot(ctx, snapshot); err != nil {
				t.Fatal(err)
			}
			c := FleetCoordinator{Store: s, Now: func() time.Time { return now }}
			transport := &terminalFenceTransport{}
			if _, err = c.ReconcileWorkerCommands(ctx, snapshot, transport); err != nil {
				t.Fatal(err)
			}
			got := repair1Records(t, s)
			at := repair1Attempt(t, got, original.ID)
			expected := parked
			expected.Revision = at.Revision
			expected.UpdatedAt = at.UpdatedAt
			if !reflect.DeepEqual(at, expected) || repair1Assignment(t, got, original.AssignmentID).State != domain.AssignmentReleased {
				t.Fatal("release lost parked binding/evidence")
			}
			if len(transport.commands) == 0 || repair1Attempt(t, got, live.ID).Control != domain.ControlRunning {
				t.Fatal("sibling blocked")
			}
			for _, cmd := range transport.commands {
				if cmd.AssignmentID == original.AssignmentID {
					t.Fatal("lost execution dispatched")
				}
			}
			if wakes, err := s.WakeTaskWaits(ctx, now); err != nil || len(wakes) != 0 {
				t.Fatalf("unready wake: %+v %v", wakes, err)
			}
			if _, err = s.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet, Reason: "actual ready settlement"}, now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = sqlite.OpenMigrated(path)
			if err != nil {
				t.Fatal(err)
			}
			var final domain.Attempt
			for pass := 0; pass < 2; pass++ {
				if wakes, err := s.WakeTaskWaits(ctx, now.Add(time.Minute)); err != nil || len(wakes) != 0 {
					t.Fatalf("abandoned execution resumed: %+v %v", wakes, err)
				}
				got = repair1Records(t, s)
				at = repair1Attempt(t, got, original.ID)
				if at.Progress != domain.ProgressFailed || at.Control != domain.ControlStopped || at.CompletedAt == nil || at.Failure == "" || repair1Assignment(t, got, original.AssignmentID).DispatchToken != "" {
					t.Fatal("abandonment/revocation missing")
				}
				if pass == 0 {
					final = at
				} else if !reflect.DeepEqual(at, final) {
					t.Fatal("repeat/reopen changed abandoned attempt")
				}
				if owners := directoryOwners([]domain.Attempt{at}, got.Assignments, got.WorkflowRuns, got.Tasks); len(owners) != 0 {
					t.Fatal("abandoned attempt retained directory ownership")
				}
				events, err := s.ListTaskWaitReconciliations(ctx)
				if err != nil || len(events) != 1 || events[0].Kind != domain.TaskWaitReconciliationAuthorityRevoked {
					t.Fatalf("revocation events: %+v %v", events, err)
				}
				pending, err := s.TaskWakesAwaitingDelivery(ctx, now.Add(time.Minute))
				if err != nil || len(pending) != 0 {
					t.Fatal("abandoned wake delivered")
				}
				if pass == 0 {
					if err = s.Close(); err != nil {
						t.Fatal(err)
					}
					s, err = sqlite.OpenMigrated(path)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}
