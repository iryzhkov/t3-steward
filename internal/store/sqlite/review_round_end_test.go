package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestReviewPausedParentDoesNotBlockReconcileOrAdmission(t *testing.T) {
	for _, control := range []domain.ControlState{domain.ControlDraining, domain.ControlPaused, domain.ControlPausedUncheckpointed, domain.ControlWaitingExternal} {
		t.Run(string(control), func(t *testing.T) {
			s, f, cp, receipt := parentWaitFixture(t)
			a := loadAttempt(t, s, f.Parent.AttemptID)
			a.Revision++
			a.Control = control
			if control == domain.ControlWaitingExternal {
				a.Progress = domain.ProgressWaitingExternal
			}
			cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{a}})
			before := cancellationSnapshot(t, s)
			got, err := s.ReconcileReviewChildCancellation(context.Background(), f, cp)
			if err != nil || got.Status != "no-action" {
				t.Fatalf("paused reconcile %+v %v", got, err)
			}
			if err := s.ReconcileMaterializedReviewChildren(context.Background()); err != nil {
				t.Fatal(err)
			}
			tx, err := s.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			reason, err := s.reviewAttemptAdmissionTx(context.Background(), tx, receipt.Graph.Attempts[0])
			tx.Rollback()
			if err != nil || reason != "" {
				t.Fatalf("admission %q %v", reason, err)
			}
			if before != cancellationSnapshot(t, s) {
				t.Fatal("live parent changed")
			}
		})
	}
}

func TestReviewAllocatedOnlySettlesAfterParentEnds(t *testing.T) {
	for _, kind := range []string{"healthy", "ended", "run-ended", "superseded", "released", "completed"} {
		t.Run(kind, func(t *testing.T) {
			s, f, cp, prepared, _ := childFixture(t)
			ctx := context.Background()
			want := ""
			switch kind {
			case "ended":
				cancellationEndParent(t, s, f, domain.ProgressFailed)
				want = "parent attempt ended"
			case "run-ended":
				for _, r := range cancellationRecords(t, s).WorkflowRuns {
					if r.ID == f.Parent.RunID {
						r.Progress = domain.ProgressCancelled
						r.Revision++
						cancellationSave(t, s, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{r}})
					}
				}
				want = "parent run ended"
			case "superseded":
				a := loadAttempt(t, s, f.Parent.AttemptID)
				a.ID = "retry"
				a.Number++
				a.Revision = 1
				a.ThreadID = ""
				a.AssignmentID = ""
				a.Control = domain.ControlUnassigned
				a.Progress = domain.ProgressReady
				cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{a}})
				want = "parent attempt superseded"
			case "released", "completed":
				for _, a := range cancellationRecords(t, s).Assignments {
					if a.ID == f.Parent.AssignmentID {
						a.State = domain.AssignmentReleased
						if kind == "completed" {
							a.State = domain.AssignmentCompleted
						}
						cancellationSave(t, s, CoordinatorRecords{Assignments: []domain.Assignment{a}})
					}
				}
				want = "parent assignment ownership lost"
			}
			s = reviewRuntimeReopen(t, s)
			before := cancellationSnapshot(t, s)
			if err := s.ReconcileMaterializedReviewChildren(ctx); err != nil {
				t.Fatal(err)
			}
			round, err := s.GetReviewRound(ctx, cp.RoundID)
			if err != nil {
				t.Fatal(err)
			}
			if want == "" {
				if round.Terminal() || before != cancellationSnapshot(t, s) {
					t.Fatal("live allocated round changed")
				}
				if _, err := s.MaterializeReviewChild(ctx, f, cp, prepared); err != nil {
					t.Fatal(err)
				}
				return
			}
			if !round.Terminal() {
				t.Fatalf("round still open: %+v", round)
			}
			for _, m := range round.Reviewers {
				if m.State != "failed" || m.Failure != want {
					t.Fatalf("member %+v want %q", m, want)
				}
			}
			if _, err := s.MaterializeReviewChild(ctx, f, cp, prepared); !errors.Is(err, ErrReviewAuthorityIdentity) {
				t.Fatalf("late materialization %v", err)
			}
			settled := cancellationSnapshot(t, s)
			if err := s.ReconcileMaterializedReviewChildren(ctx); err != nil || settled != cancellationSnapshot(t, s) {
				t.Fatalf("replay changed %v", err)
			}
		})
	}
}

func TestReviewAllocatedCancellationEpochFence(t *testing.T) {
	s, f, _, _, _ := childFixture(t)
	cancellationEndParent(t, s, f, domain.ProgressFailed)
	epoch, err := s.AcquireCoordinator(context.Background(), "first")
	if err != nil {
		t.Fatal(err)
	}
	s = reviewRuntimeReopen(t, s)
	if _, err := s.AcquireCoordinator(context.Background(), "second"); err != nil {
		t.Fatal(err)
	}
	before := cancellationSnapshot(t, s)
	err = s.ReconcileMaterializedReviewChildren(WithCoordinatorEpochFence(context.Background(), epoch))
	if !errors.Is(err, ErrStaleCoordinatorEpoch) || before != cancellationSnapshot(t, s) {
		t.Fatalf("stale epoch wrote: %v", err)
	}
}
