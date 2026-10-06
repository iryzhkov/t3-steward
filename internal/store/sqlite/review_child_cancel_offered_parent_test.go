package sqlite

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// These offers reconstruct planner output except the commit case, which uses
// the actual durable commit API against the fixture's existing worker snapshot.
func TestReviewChildCancellationOfferedParentRetryLifecycle(t *testing.T) {
	for _, branch := range []string{"ended", "superseded", "run-ended", "ownership-lost", "deadline"} {
		for _, layout := range []string{"queued", "ready", "blocked", "commit"} {
			t.Run(branch+"/"+layout, func(t *testing.T) {
				t.Parallel()
				ctx := context.Background()
				s, f, cp, receipt := parentWaitFixture(t)
				if _, err := s.WaitReviewParent(ctx, f, cp); err != nil {
					t.Fatal(err)
				}
				waits, err := s.ListTaskWaits(ctx)
				if err != nil || len(waits) == 0 {
					t.Fatalf("missing parked wait: %v", err)
				}
				child := cancellationAssign(t, s, receipt.Graph.Attempts[0], domain.AssignmentClaimed, f.Requirements.Members[0].Route)
				retry := cancellationParentRow(t, s, f.Parent.AttemptID)
				retry.ID = "offered-retry"
				retry.Number++
				retry.Revision = 1
				retry.Progress = domain.ProgressReady
				retry.Control = domain.ControlUnassigned
				retry.ThreadID = ""
				retry.AssignmentID = ""
				retry.CompletedAt = nil
				if layout == "queued" {
					retry.Progress = domain.ProgressQueued
				}
				if layout == "blocked" {
					retry.Progress = domain.ProgressBlocked
				}
				cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{retry}})
				var offer domain.Assignment
				if layout == "commit" {
					snapshots, err := s.LoadWorkerSnapshots(ctx)
					if err != nil {
						t.Fatal(err)
					}
					var snapshot domain.WorkerSnapshot
					for _, candidate := range snapshots {
						if candidate.WorkerID == "worker" {
							snapshot = candidate
						}
					}
					if snapshot.WorkerID == "" {
						t.Fatal("fixture worker absent")
					}
					now := snapshot.ObservedAt
					offer = domain.Assignment{ID: "committed-offer", AttemptID: retry.ID, WorkerID: snapshot.WorkerID, Project: f.Parent.Repository,
						Route: domain.ProviderRoute{ProviderInstanceID: "codex", Model: "sol"}, State: domain.AssignmentOffered, Epoch: int64(retry.Number),
						ThreadID: "reserved-thread", DispatchToken: "reserved-token", LeaseToken: "reserved-lease",
						Estimate: &domain.TaskAdmissionEstimate{RemainingCost: 10, ExpectedRuntime: time.Hour, CheckpointMargin: time.Minute}, ExecutorDemand: &domain.ResourceDemand{}}
					offers, err := s.CommitAssignmentPlan(ctx, domain.AssignmentPlanCommit{CoordinatorEpoch: snapshot.CoordinatorEpoch, CommittedAt: now,
						Items: []domain.AssignmentPlanItem{{ExpectedAttemptRevision: retry.Revision, WorkerEpoch: snapshot.WorkerEpoch, WorkerSnapshotSequence: snapshot.Sequence, Assignment: offer}}})
					if err != nil || len(offers) != 1 {
						t.Fatalf("real commit offer: %+v %v", offers, err)
					}
					offer = offers[0]
				} else {
					offer = cancellationAssign(t, s, retry, domain.AssignmentOffered, f.Parent.ExecutorRoute)
				}
				retry = cancellationParentRow(t, s, retry.ID)
				if retry.ThreadID != "" || retry.AssignmentID != offer.ID || offer.ThreadID == "" || offer.DispatchToken == "" ||
					offer.DispatchState != "" || offer.DispatchRevision != 0 || offer.DispatchConfirmedAt != nil {
					t.Fatalf("not normal reserved offer: %+v %+v", retry, offer)
				}
				switch branch {
				case "ended":
					cancellationEndParent(t, s, f, domain.ProgressFailed)
				case "run-ended":
					for _, run := range cancellationRecords(t, s).WorkflowRuns {
						if run.ID == f.Parent.RunID {
							run.Progress = domain.ProgressSucceeded
							run.Revision++
							cancellationSave(t, s, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}})
						}
					}
				case "ownership-lost":
					original := cancellationParentAssignment(t, s, f.Parent.AssignmentID)
					original.State = domain.AssignmentUnknown
					cancellationSave(t, s, CoordinatorRecords{Assignments: []domain.Assignment{original}})
				case "deadline":
					s.now = func() time.Time { return *receipt.Graph.Tasks[0].Deadline }
				}
				path, clock := s.path, s.now
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				s, err = Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				s.now = clock
				parentBefore := parentWaitSnapshot(t, s, f)
				got, err := s.ReconcileReviewChildCancellation(ctx, f, cp)
				if err != nil || got.Status != "stop-requested" || !got.WorkerStopPending || len(got.CancelledAttempts) != len(receipt.Graph.Attempts) {
					t.Fatalf("cleanup: %+v %v", got, err)
				}
				if parentBefore != parentWaitSnapshot(t, s, f) {
					t.Fatal("parent/waits changed")
				}
				if !reflect.DeepEqual(retry, cancellationParentRow(t, s, retry.ID)) || !reflect.DeepEqual(offer, cancellationParentAssignment(t, s, offer.ID)) {
					t.Fatal("parent retry/offer changed")
				}
				retained := cancellationParentAssignment(t, s, child.ID)
				if retained.State != domain.AssignmentClaimed || retained.Epoch != child.Epoch || retained.ThreadID != child.ThreadID || retained.DispatchToken != "" {
					t.Fatalf("lost claimed custody or token not revoked: %+v", retained)
				}
				round, err := s.GetReviewRound(ctx, cp.RoundID)
				if err != nil {
					t.Fatal(err)
				}
				for _, member := range round.Reviewers {
					want := "failed"
					if branch == "deadline" {
						want = "timed-out"
					}
					if member.State != want {
						t.Fatalf("round result: %+v want %s", member, want)
					}
				}
				for _, attempt := range cancellationRecords(t, s).Attempts {
					if attempt.WorkflowRunID == cp.RoundID && (attempt.Progress != domain.ProgressCancelled || attempt.Control != domain.ControlStopped) {
						t.Fatalf("child not stopped: %+v", attempt)
					}
				}
				after := cancellationSnapshot(t, s)
				// Replay after a second reopen must retain audit/results and pending custody.
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				s, err = Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				s.now = clock
				again, err := s.ReconcileReviewChildCancellation(ctx, f, cp)
				if err != nil || again.Status != "stop-requested" || !again.WorkerStopPending || len(again.CancelledAttempts) != 0 || after != cancellationSnapshot(t, s) {
					t.Fatalf("reopened replay: %+v %v", again, err)
				}
			})
		}
	}
}

// The exact deadline cannot turn an incoherent offer into cleanup authority.
func TestReviewChildCancellationOfferedParentRetryRefusals(t *testing.T) {
	for _, mode := range []string{"foreign-thread", "running", "preparing", "active", "completed-at", "dispatch-revision", "dispatch-state", "dispatch-confirmed",
		"original-offered", "foreign-ref", "missing-ref", "foreign-owner", "indexed-owner", "runtime-owner", "indexed-token", "indexed-state", "indexed-epoch",
		"foreign-project", "foreign-role", "missing-route", "activation", "gate", "zero-epoch", "claimed-empty-thread", "unknown-empty-thread", "completed-empty-thread", "released-empty-thread"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s, f, cp, receipt := parentWaitFixture(t)
			if _, err := s.WaitReviewParent(ctx, f, cp); err != nil {
				t.Fatal(err)
			}
			waits, err := s.ListTaskWaits(ctx)
			if err != nil || len(waits) == 0 {
				t.Fatalf("missing wait: %v", err)
			}
			cancellationAssign(t, s, receipt.Graph.Attempts[0], domain.AssignmentClaimed, f.Requirements.Members[0].Route)
			retry := cancellationParentRow(t, s, f.Parent.AttemptID)
			retry.ID = "offered-boundary"
			retry.Number++
			retry.Revision = 1
			retry.Progress = domain.ProgressReady
			retry.Control = domain.ControlUnassigned
			retry.ThreadID = ""
			retry.AssignmentID = ""
			retry.CompletedAt = nil
			cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{retry}})
			offer := cancellationAssign(t, s, retry, domain.AssignmentOffered, f.Parent.ExecutorRoute)
			retry = cancellationParentRow(t, s, retry.ID)
			switch mode {
			case "foreign-thread":
				retry.ThreadID = "foreign"
			case "running":
				retry.Control = domain.ControlRunning
			case "preparing":
				retry.Control = domain.ControlPreparing
			case "active":
				retry.Progress = domain.ProgressActive
			case "completed-at":
				now := time.Now()
				retry.CompletedAt = &now
			case "dispatch-revision":
				offer.DispatchRevision = 1
			case "dispatch-state":
				offer.DispatchState = domain.DispatchState("accepted")
			case "dispatch-confirmed":
				now := time.Now()
				offer.DispatchConfirmedAt = &now
			case "original-offered":
				original := cancellationParentAssignment(t, s, f.Parent.AssignmentID)
				original.State = domain.AssignmentOffered
				cancellationSave(t, s, CoordinatorRecords{Assignments: []domain.Assignment{original}})
			case "foreign-ref":
				retry.AssignmentID = f.Parent.AssignmentID
			case "missing-ref":
				retry.AssignmentID = ""
			case "foreign-owner":
				offer.AttemptID = "foreign-attempt"
			case "foreign-project":
				offer.Project = "foreign"
			case "foreign-role":
				offer.ExecutionRole = domain.ExecutionRole("foreign")
			case "missing-route":
				offer.Route.Model = ""
			case "activation":
				offer.ActivationID = "foreign"
			case "gate":
				offer.GateID = "foreign"
			case "zero-epoch":
				offer.Epoch = 0
			case "claimed-empty-thread":
				offer.State = domain.AssignmentClaimed
			case "unknown-empty-thread":
				offer.State = domain.AssignmentUnknown
			case "completed-empty-thread":
				offer.State = domain.AssignmentCompleted
			case "released-empty-thread":
				offer.State = domain.AssignmentReleased
			}
			cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{retry}, Assignments: []domain.Assignment{offer}})
			var query string
			switch mode {
			case "indexed-owner":
				query = "UPDATE coordinator_assignments SET attempt_id='foreign' WHERE id=?"
			case "runtime-owner":
				query = "UPDATE coordinator_assignments SET record=json_set(record,'$.attemptId','foreign') WHERE id=?"
			case "indexed-token":
				query = "UPDATE coordinator_assignments SET dispatch_token='foreign' WHERE id=?"
			case "indexed-state":
				query = "UPDATE coordinator_assignments SET assignment_state='claimed' WHERE id=?"
			case "indexed-epoch":
				query = "UPDATE coordinator_assignments SET assignment_epoch=2 WHERE id=?"
			}
			if query != "" {
				if _, err = s.db.Exec(query, offer.ID); err != nil {
					t.Fatal(err)
				}
			}
			clock := func() time.Time { return *receipt.Graph.Tasks[0].Deadline }
			path := s.path
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			s.now = clock
			before := cancellationSnapshot(t, s)
			got, err := s.ReconcileReviewChildCancellation(ctx, f, cp)
			if err == nil {
				t.Fatalf("incoherent offer accepted: %+v", got)
			}
			if before != cancellationSnapshot(t, s) {
				t.Fatal("refusal changed full durable snapshot, parked wait or claimed token")
			}
		})
	}
}
