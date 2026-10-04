package backlog

import (
	"context"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestReviewChildCancellationWorkerCustodyIntegration(t *testing.T) {
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
				for i := range records.Attempts {
					if records.Attempts[i].ID == f.frozen.Parent.AttemptID {
						records.Attempts[i].Progress = domain.ProgressFailed
						records.Attempts[i].Control = domain.ControlStopped
						records.Attempts[i].Revision++
					}
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
				parentBefore, err := f.parent.store.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
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
				// A stale completion projection built before cancellation cannot overwrite it.
				nextAssignment := assignment
				nextAssignment.State = domain.AssignmentCompleted
				lateAttempt := a
				lateAttempt.Progress = domain.ProgressVerifying
				lateAttempt.Control = domain.ControlStopped
				lateAttempt.Revision++
				snapshot := domain.WorkerSnapshot{WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch, CoordinatorEpoch: 1, Sequence: 2, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour), Inventory: domain.WorkerInventory{ID: assignment.WorkerID, AcceptBacklog: true, Health: domain.WorkerHealthReady}}
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
				if _, err = f.parent.store.CommitWorkerCommands(ctx, commands); err != nil {
					t.Fatal(err)
				}
				// Command persistence alone is not worker acknowledgement.
				pending, err := f.parent.store.LoadWorkerCommandRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				snapshot.Sequence = 3
				snapshot.ObservedAt = now.Add(time.Millisecond)
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

func TestReviewChildCancellationCollectorPreservesCompletedEvidence(t *testing.T) {
	ctx := context.Background()
	f := newRetainedChild(t, true)
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
	for i := range records.Attempts {
		if records.Attempts[i].ID == f.frozen.Parent.AttemptID {
			records.Attempts[i].Progress = domain.ProgressSucceeded
			records.Attempts[i].Control = domain.ControlStopped
			records.Attempts[i].Revision++
		}
	}
	complete := receipt.Graph.Attempts[0]
	complete.Progress = domain.ProgressSucceeded
	complete.Control = domain.ControlStopped
	complete.Revision++
	payloads := map[string]string{}
	outputs := []domain.Artifact{}
	verdict := review.Verdict{Schema: review.Schema, Verdict: "accept", Findings: []review.Finding{}, InputManifestDigest: f.checkpoint.Checkpoint.InputDigest, ReviewerRoute: f.frozen.Requirements.Members[0].Route}
	raw, err := json.Marshal(verdict)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"review.md", "verdict.json"} {
		id := "completed-" + name
		content := "Completed retained review"
		if name == "verdict.json" {
			content = string(raw)
		}
		payloads[id] = content
		outputs = append(outputs, domain.Artifact{ID: id, WorkflowRunID: complete.WorkflowRunID, TaskID: complete.TaskID, AttemptID: complete.ID, Kind: domain.ArtifactOutput, Name: name, Size: int64(len(content)), SHA256: admissionDigestBytes([]byte(content))})
	}
	if err = f.parent.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: append(records.Attempts, complete), Artifacts: outputs}); err != nil {
		t.Fatal(err)
	}
	got, err := f.parent.store.ReconcileReviewChildCancellation(ctx, f.frozen, f.checkpoint)
	if err != nil || len(got.CancelledAttempts) != len(receipt.Graph.Attempts)-1 {
		t.Fatalf("cancel %+v %v", got, err)
	}
	round, err := f.parent.store.GetReviewRound(ctx, f.checkpoint.RoundID)
	if err != nil {
		t.Fatal(err)
	}
	if round.Reviewers[0].State != "pending" {
		t.Fatal("collector lost completed member")
	}
	collector := ReviewCollector{Store: f.parent.store.Store, Results: t.TempDir(), Open: func(_ context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
		return domain.Artifact{ID: id}, io.NopCloser(strings.NewReader(payloads[id])), nil
	}}
	if err = collector.Tick(ctx, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	round, err = f.parent.store.GetReviewRound(ctx, f.checkpoint.RoundID)
	if err != nil {
		t.Fatal(err)
	}
	if round.Reviewers[0].State != "succeeded" || round.Reviewers[0].Verdict == nil || round.Reviewers[0].Verdict.Verdict != "accept" || round.Combined != "reject" {
		t.Fatalf("collected %+v", round)
	}
	for _, m := range round.Reviewers[1:] {
		if m.State != "failed" {
			t.Fatalf("cancellation accepted %+v", m)
		}
	}
	before := round
	if _, err = f.parent.store.ReconcileReviewChildCancellation(ctx, f.frozen, f.checkpoint); err != nil {
		t.Fatal(err)
	}
	round, err = f.parent.store.GetReviewRound(ctx, f.checkpoint.RoundID)
	if err != nil || !reflect.DeepEqual(before, round) {
		t.Fatal("terminal results rewritten")
	}
}
