package sqlite

import (
	"context"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Exact portable Astra review probe, retained as a regression (gofmt only).
func TestIndependentCancellationParentRefusal(t *testing.T) {
	for _, kind := range []string{"healthy-control", "terminal-control", "offered-live", "offered-terminal", "bad-progress-superseded", "bad-control-terminal", "changed-thread-terminal"} {
		t.Run(kind, func(t *testing.T) {
			s, f, cp, _ := parentWaitFixture(t)
			records := cancellationRecords(t, s)
			var parent domain.Attempt
			for _, a := range records.Attempts {
				if a.ID == f.Parent.AttemptID {
					parent = a
				}
			}
			if kind == "terminal-control" || kind == "offered-terminal" || kind == "bad-control-terminal" || kind == "changed-thread-terminal" {
				parent.Progress = domain.ProgressFailed
				parent.Control = domain.ControlStopped
			}
			if kind == "bad-progress-superseded" {
				retry := parent
				retry.ID = "independent-parent-retry"
				retry.Number++
				retry.Revision = 1
				retry.AssignmentID = ""
				retry.ThreadID = ""
				retry.Progress = domain.ProgressReady
				retry.Control = domain.ControlUnassigned
				cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{retry}})
				parent.Progress = domain.ProgressState("invalid-parent-progress")
			}
			if kind == "bad-control-terminal" {
				parent.Control = domain.ControlState("invalid-parent-control")
			}
			if kind == "changed-thread-terminal" {
				parent.ThreadID = "foreign-thread"
			}
			parent.Revision++
			cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{parent}})
			if kind == "offered-live" || kind == "offered-terminal" {
				for _, a := range records.Assignments {
					if a.ID == f.Parent.AssignmentID {
						a.State = domain.AssignmentOffered
						cancellationSave(t, s, CoordinatorRecords{Assignments: []domain.Assignment{a}})
					}
				}
			}
			before := cancellationSnapshot(t, s)
			got, err := s.ReconcileReviewChildCancellation(context.Background(), f, cp)
			if kind == "healthy-control" {
				if err != nil || got.Status != "no-action" || before != cancellationSnapshot(t, s) {
					t.Fatalf("healthy control: %+v %v", got, err)
				}
				return
			}
			if kind == "terminal-control" {
				if err != nil || len(got.CancelledAttempts) == 0 {
					t.Fatalf("terminal control: %+v %v", got, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("corrupt parent accepted: status=%s reason=%q cancelled=%d changed=%v", got.Status, got.Reason, len(got.CancelledAttempts), before != cancellationSnapshot(t, s))
			}
			if before != cancellationSnapshot(t, s) {
				t.Fatal("refusal mutated durable state")
			}
		})
	}
}
