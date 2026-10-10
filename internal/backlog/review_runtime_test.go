package backlog

import (
	"context"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"reflect"
	"testing"
	"time"
)

func TestReviewRuntimeAutomaticHealthySiblingAndReopen(t *testing.T) {
	for _, healthy := range []bool{false, true} {
		t.Run(fmt.Sprintf("healthy=%v", healthy), func(t *testing.T) {
			ctx := context.Background()
			f := newRetainedChild(t, false)
			prepared, err := f.prepare(ctx)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := f.parent.store.MaterializeReviewChild(ctx, f.frozen, f.checkpoint, prepared)
			if err != nil {
				t.Fatal(err)
			}
			records, err := f.parent.store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var parent domain.Attempt
			for _, a := range records.Attempts {
				if a.ID == f.frozen.Parent.AttemptID {
					parent = a
				}
			}
			parent.Revision++
			if !healthy {
				parent.Progress = domain.ProgressFailed
				parent.Control = domain.ControlStopped
			}
			now := time.Now().UTC()
			sibling := domain.Attempt{ID: "rt-ordinary-sibling", WorkflowRunID: "plain-run", TaskID: "plain-task", Number: 1, Revision: 1, Progress: domain.ProgressActive, Control: domain.ControlPreparing, AssignmentID: "plain-assignment", UpdatedAt: now}
			route := receipt.Graph.Tasks[0].Routes[0]
			assignment := domain.Assignment{ID: sibling.AssignmentID, AttemptID: sibling.ID, WorkerID: "child-worker", WorkerEpoch: "child-session", Epoch: 1, State: domain.AssignmentClaimed, Project: f.frozen.Parent.Repository, Route: route, ThreadID: "plain-thread", LeaseToken: "plain-lease", DispatchToken: "plain-token", LeaseExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now}
			if err = f.parent.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{parent, sibling}, Assignments: []domain.Assignment{assignment}}); err != nil {
				t.Fatal(err)
			}
			snapshot := domain.WorkerSnapshot{WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch, CoordinatorEpoch: 1, Sequence: 1, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour), Inventory: domain.WorkerInventory{ID: assignment.WorkerID, Health: domain.WorkerHealthReady, AcceptBacklog: true}}
			if err = f.parent.store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
				t.Fatal(err)
			}
			transport := &terminalFenceTransport{}
			c := FleetCoordinator{Store: f.parent.store, Now: func() time.Time { return now }}
			assert := func() {
				t.Helper()
				report, err := c.ReconcileWorkerCommands(ctx, snapshot, transport)
				if err != nil {
					t.Fatal(err)
				}
				if len(report.Pending) != 1 || report.Pending[0].AssignmentID != assignment.ID || report.Pending[0].Kind != domain.WorkerCommandPrepare {
					t.Fatalf("healthy sibling delivery %+v", report)
				}
				current, err := f.parent.store.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, a := range current.Attempts {
					if a.ID == parent.ID && !reflect.DeepEqual(a, parent) {
						t.Fatal("automatic pass changed parent")
					}
					if a.WorkflowRunID == receipt.Graph.Run.ID && (a.Progress != domain.ProgressCancelled) != healthy {
						t.Fatalf("review child healthy=%v %+v", healthy, a)
					}
				}
			}
			assert()
			if err = f.parent.store.Close(); err != nil {
				t.Fatal(err)
			}
			f.parent.store.Store, err = sqlitetest.OpenMigrated(f.parent.store.dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer f.parent.store.Close()
			assert()
			assert()
		})
	}
}

func TestReviewRuntimeAutomaticWorkerCustodyIntegration(t *testing.T) {
	for _, completion := range []bool{false, true} {
		for _, state := range []domain.AssignmentState{domain.AssignmentClaimed, domain.AssignmentUnknown} {
			t.Run(string(state)+map[bool]string{false: "/stop", true: "/collect"}[completion], func(t *testing.T) {
				ctx := context.Background()
				f := newRetainedChild(t, false)
				prepared, err := f.prepare(ctx)
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := f.parent.store.MaterializeReviewChild(ctx, f.frozen, f.checkpoint, prepared)
				if err != nil {
					t.Fatal(err)
				}
				// Park the parent through the existing node-wait mechanism before it ends.
				if _, err = f.parent.store.WaitReviewParent(ctx, f.frozen, f.checkpoint); err != nil {
					t.Fatal(err)
				}
				records, err := f.parent.store.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				now := time.Now().UTC()
				a := receipt.Graph.Attempts[0]
				a.Progress = domain.ProgressActive
				a.Control = domain.ControlRunning
				a.Revision++
				a.AssignmentID = "child-assignment"
				a.ThreadID = "child-thread"
				route := receipt.Graph.Tasks[0].Routes[0]
				assignment := domain.Assignment{ID: a.AssignmentID, AttemptID: a.ID, Epoch: 1, State: state, WorkerID: "child-worker", WorkerEpoch: "child-epoch", Project: "t3-steward", Route: route, ThreadID: a.ThreadID, DispatchToken: "child-token", LeaseToken: "child-lease", LeaseExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now}
				if err = f.parent.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: append(records.Attempts, a), Assignments: []domain.Assignment{assignment}}); err != nil {
					t.Fatal(err)
				}
				// Persist and accept Dispatch while the actual child is live. Its
				// historical receipt must survive cancellation without authorizing it.
				initialSnapshot := domain.WorkerSnapshot{WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch, CoordinatorEpoch: 1, Sequence: 1, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour), Inventory: domain.WorkerInventory{ID: assignment.WorkerID, AcceptBacklog: true, Health: domain.WorkerHealthReady}}
				if err = f.parent.store.SaveWorkerSnapshot(ctx, initialSnapshot); err != nil {
					t.Fatal(err)
				}
				claimed := assignment
				claimed.State = domain.AssignmentClaimed
				if err = f.parent.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: []domain.Assignment{claimed}}); err != nil {
					t.Fatal(err)
				}
				dispatch := domain.WorkerCommand{ID: "old-child-dispatch", Kind: domain.WorkerCommandDispatch, WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch, CoordinatorEpoch: 1, AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, ExpectedWorkerSequence: 1, CreatedAt: now}
				if _, err = f.parent.store.CommitWorkerCommands(ctx, []domain.WorkerCommand{dispatch}); err != nil {
					t.Fatal(err)
				}
				if _, err = f.parent.store.AcknowledgeWorkerCommand(ctx, domain.WorkerAcknowledgement{CommandID: dispatch.ID, WorkerID: dispatch.WorkerID, WorkerEpoch: dispatch.WorkerEpoch, CoordinatorEpoch: 1, AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, WorkerSequence: 1, Accepted: true, AcknowledgedAt: now}); err != nil {
					t.Fatal(err)
				}
				if err = f.parent.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: []domain.Assignment{assignment}}); err != nil {
					t.Fatal(err)
				}
				for i := range records.Attempts {
					if records.Attempts[i].ID == f.frozen.Parent.AttemptID {
						records.Attempts[i].Progress = domain.ProgressFailed
						records.Attempts[i].Control = domain.ControlStopped
						records.Attempts[i].Revision++
					}
				}
				for _, parent := range records.Attempts {
					if parent.ID == f.frozen.Parent.AttemptID {
						if err = f.parent.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{parent}}); err != nil {
							t.Fatal(err)
						}
					}
				}
				parentBefore, err := f.parent.store.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				autoTransport := &terminalFenceTransport{}
				autoCoordinator := FleetCoordinator{Store: f.parent.store, Now: func() time.Time { return now }}
				if _, err = autoCoordinator.ReconcileWorkerCommands(ctx, initialSnapshot, autoTransport); err != nil {
					t.Fatal(err)
				}
				autoRecords, err := f.parent.store.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, child := range autoRecords.Attempts {
					if child.ID == a.ID && child.Progress != domain.ProgressCancelled {
						t.Fatalf("automatic pass did not cancel materialized child: %+v; commands %+v", child, autoTransport.commands)
					}
				}
				for _, cmd := range autoTransport.commands {
					if cmd.Kind == domain.WorkerCommandPrepare || cmd.Kind == domain.WorkerCommandDispatch {
						t.Fatalf("automatic pass delivered start: %+v", cmd)
					}
				}
				got, err := f.parent.store.ReconcileReviewChildCancellation(ctx, f.frozen, f.checkpoint)
				if err != nil || got.Status != "stop-requested" || !got.WorkerStopPending {
					t.Fatalf("cancel %+v %v", got, err)
				}
				records, err = f.parent.store.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, x := range parentBefore.Attempts {
					if x.ID == f.frozen.Parent.AttemptID {
						for _, y := range records.Attempts {
							if y.ID == x.ID && !reflect.DeepEqual(x, y) {
								t.Fatal("parent changed")
							}
						}
					}
				}
				if domain.RunExecutionsQuiescent(f.checkpoint.RoundID, records.Attempts, records.Assignments) {
					t.Fatal("premature quiescence")
				}
				childRun := receipt.Graph.Run
				projected, err := domain.ProjectRunSink(childRun, receipt.Graph.Tasks, records.Attempts, records.Assignments, now)
				if err != nil || projected.Sink.Progress.Terminal() {
					t.Fatalf("premature sink %+v %v", projected, err)
				}

				// Exercise the real coordinator after internal cancellation. The
				// transport records delivery but deliberately withholds acknowledgements.
				initialSnapshot.Sequence = 2
				initialSnapshot.ObservedAt = now.Add(time.Millisecond)
				if err = f.parent.store.SaveWorkerSnapshot(ctx, initialSnapshot); err != nil {
					t.Fatal(err)
				}
				transport := &terminalFenceTransport{}
				coordinator := FleetCoordinator{Store: f.parent.store, Now: func() time.Time { return now }}
				for tick := 0; tick < 3; tick++ {
					report, err := coordinator.ReconcileWorkerCommands(ctx, initialSnapshot, transport)
					if err != nil {
						t.Fatal(err)
					}
					for _, cmd := range report.Pending {
						if cmd.Kind != domain.WorkerCommandStop {
							t.Fatalf("cancelled child delivered %s", cmd.Kind)
						}
					}
					current, err := f.parent.store.LoadCoordinatorRecords(ctx)
					if err != nil {
						t.Fatal(err)
					}
					for _, x := range current.Attempts {
						if x.ID == a.ID && (x.Progress != domain.ProgressCancelled || x.Control != domain.ControlStopped || x.CompletedAt == nil) {
							t.Fatalf("resurrected child: %+v", x)
						}
					}
					for _, x := range current.Assignments {
						if x.ID == assignment.ID && x.State != state {
							t.Fatal("cancellation alone released custody")
						}
					}
					if domain.RunExecutionsQuiescent(f.checkpoint.RoundID, current.Attempts, current.Assignments) {
						t.Fatal("cancellation alone became quiescent")
					}
				}
				if len(transport.commands) != 3 {
					t.Fatalf("cleanup deliveries %d", len(transport.commands))
				}
				// A stale completion projection built before cancellation cannot overwrite it.
				nextAssignment := assignment
				nextAssignment.State = domain.AssignmentCompleted
				lateAttempt := a
				lateAttempt.Progress = domain.ProgressVerifying
				lateAttempt.Control = domain.ControlStopped
				lateAttempt.Revision++
				snapshot := domain.WorkerSnapshot{WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch, CoordinatorEpoch: 1, Sequence: 2, Connected: true, ObservedAt: now.Add(time.Millisecond), ValidUntil: now.Add(time.Hour), Inventory: domain.WorkerInventory{ID: assignment.WorkerID, AcceptBacklog: true, Health: domain.WorkerHealthReady}}
				if err = f.parent.store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
					t.Fatal(err)
				}
				_, err = f.parent.store.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{{CoordinatorEpoch: 1, WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch, WorkerSequence: 2, ExpectedAssignment: assignment, ExpectedAttemptRevision: a.Revision, Assignment: nextAssignment, Attempt: lateAttempt, Reason: "late completion", TransitionedAt: now}})
				if err == nil {
					t.Fatal("late completion overwrote cancellation")
				}
				observedState := domain.AssignmentClaimed
				if completion {
					observedState = domain.AssignmentCompleted
				}
				snapshot.Assignments = []domain.WorkerAssignmentObservation{{AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, ObservedAt: now, State: observedState, Control: domain.ControlStopped, ThreadID: assignment.ThreadID}}
				if !completion {
					snapshot.Assignments[0].Control = domain.ControlRunning
				}
				commands, err := PlanWorkerCommands(records, snapshot, nil, now)
				if err != nil || len(commands) != 1 {
					t.Fatalf("commands %+v %v", commands, err)
				}
				want := domain.WorkerCommandStop
				if completion {
					want = domain.WorkerCommandCollect
				}
				if commands[0].Kind != want {
					t.Fatalf("kind %s", commands[0].Kind)
				}
				replay, err := PlanWorkerCommands(records, snapshot, nil, now)
				if err != nil || !reflect.DeepEqual(commands, replay) {
					t.Fatal("unstable command")
				}
				// The automatic pass already issued Stop under sequence 1. Reuse
				// its historical exact bytes; do not fabricate a changed replay.
				if !completion {
					stored, loadErr := f.parent.store.LoadWorkerCommandRecords(ctx)
					if loadErr != nil {
						t.Fatal(loadErr)
					}
					for _, record := range stored {
						if record.Command.ID == commands[0].ID {
							commands[0] = record.Command
						}
					}
				}
				if _, err = f.parent.store.CommitWorkerCommands(ctx, commands); err != nil {
					t.Fatal(err)
				}
				pending, err := f.parent.store.LoadWorkerCommandRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				// Before acknowledgement, both Stop and Collect retain custody.
				preAck, err := PlanWorkerStateTransitions(records, snapshot, pending, now)
				if err != nil {
					t.Fatal(err)
				}
				for _, tr := range preAck {
					if tr.Assignment.State == domain.AssignmentReleased || tr.Assignment.State == domain.AssignmentCompleted {
						t.Fatal("pre-ack transition relinquished custody")
					}
				}
				if domain.RunExecutionsQuiescent(f.checkpoint.RoundID, records.Attempts, records.Assignments) {
					t.Fatal("pre-ack custody became quiescent")
				}
				again, err := f.parent.store.ReconcileReviewChildCancellation(ctx, f.frozen, f.checkpoint)
				if err != nil || again.Status != "stop-requested" || !again.WorkerStopPending || len(again.CancelledAttempts) != 0 {
					t.Fatalf("pre-ack replay %+v %v", again, err)
				}
				// Command persistence alone is not worker acknowledgement.
				pending, err = f.parent.store.LoadWorkerCommandRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				snapshot.Sequence = 3
				snapshot.ObservedAt = now.Add(2 * time.Millisecond)
				if completion {
					// Completed observation remains uncollected until the collect ack.
				} else {
					snapshot.Assignments = nil
				}
				if err = f.parent.store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
					t.Fatal(err)
				}
				transitions, err := PlanWorkerStateTransitions(records, snapshot, pending, now)
				if err != nil {
					t.Fatal(err)
				}
				if completion {
					for _, tr := range transitions {
						if tr.Attempt.Progress != domain.ProgressCancelled {
							t.Fatal("completed observation resurrected attempt")
						}
					}
				}
				// Simulated disposable worker response, committed through the real ack API.
				ack := domain.WorkerAcknowledgement{CommandID: commands[0].ID, WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch, CoordinatorEpoch: 1, AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, WorkerSequence: 3, Accepted: true, AcknowledgedAt: now}
				if _, err = f.parent.store.AcknowledgeWorkerCommand(ctx, ack); err != nil {
					t.Fatal(err)
				}
				pending, err = f.parent.store.LoadWorkerCommandRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if !completion {
					retained, retainErr := PlanWorkerStateTransitions(records, snapshot, pending, now)
					if retainErr != nil || len(retained) != 0 {
						t.Fatalf("stop acknowledgement released without observation: %+v %v", retained, retainErr)
					}
					snapshot.Sequence++
					snapshot.ObservedAt = snapshot.ObservedAt.Add(time.Millisecond)
					snapshot.Assignments = []domain.WorkerAssignmentObservation{{AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, State: domain.AssignmentReleased, ThreadID: assignment.ThreadID, ObservedAt: now}}
					if err = f.parent.store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
						t.Fatal(err)
					}
				}
				transitions, err = PlanWorkerStateTransitions(records, snapshot, pending, now)
				if err != nil || len(transitions) != 1 {
					t.Fatalf("ack transition %+v %v", transitions, err)
				}
				if transitions[0].Attempt.Progress != domain.ProgressCancelled {
					t.Fatal("ack resurrected cancelled attempt")
				}
				if _, err = f.parent.store.CommitWorkerStateTransitions(ctx, transitions); err != nil {
					t.Fatal(err)
				}
				got, err = f.parent.store.ReconcileReviewChildCancellation(ctx, f.frozen, f.checkpoint)
				if err != nil || got.Status != "quiescent" || got.WorkerStopPending {
					t.Fatalf("settled %+v %v", got, err)
				}
				records, err = f.parent.store.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				projected, err = domain.ProjectRunSink(childRun, receipt.Graph.Tasks, records.Attempts, records.Assignments, now)
				if err != nil || !projected.Sink.Progress.Terminal() {
					t.Fatalf("sink %+v %v", projected, err)
				}
				projected.Revision++
				if err = f.parent.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{projected}}); err != nil {
					t.Fatal(err)
				}
				if err = f.parent.store.SettleNodeWaits(ctx, now); err != nil {
					t.Fatal(err)
				}
				waits, err := f.parent.store.ListTaskWaits(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, w := range waits {
					if w.AttemptID == f.frozen.Parent.AttemptID && w.Live() {
						t.Fatal("ended parent wait remained live")
					}
				}
			})
		}
	}
}
