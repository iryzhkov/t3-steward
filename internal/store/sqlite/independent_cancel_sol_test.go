package sqlite

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
)

// Portable Sol minimal recipe retained with the same BEFORE/AFTER assertions.
func TestIndependentCancellationMalformedNewestRetry(t *testing.T) {
	s, f, cp, _ := parentWaitFixture(t)
	for _, a := range cancellationRecords(t, s).Attempts {
		if a.ID != f.Parent.AttemptID {
			continue
		}
		retry := a
		retry.ID = "independent-parent-retry"
		retry.Number++
		retry.Revision = 1
		retry.Progress = "invalid-progress"
		retry.Control = domain.ControlUnassigned
		retry.AssignmentID = ""
		retry.ThreadID = ""
		cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{retry}})
	}
	before := cancellationSnapshot(t, s)
	got, err := s.ReconcileReviewChildCancellation(context.Background(), f, cp)
	if err == nil {
		t.Fatalf("corrupt parent accepted: %+v; changed=%v", got, before != cancellationSnapshot(t, s))
	}
}
