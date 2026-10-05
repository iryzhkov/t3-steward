package sqlite

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

// Actual Sol repair review overlay, retained with its original assertions.
func TestIndependentCancellationRepairHistoryAndWait(t *testing.T) {
	for _, mode := range []string{"newest-progress", "older-control", "older-completed", "older-thread", "run-progress", "offered-deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s, f, cp, receipt := parentWaitFixture(t)
			if _, err := s.WaitReviewParent(ctx, f, cp); err != nil {
				t.Fatal(err)
			}
			waits, err := s.ListTaskWaits(ctx)
			if err != nil || len(waits) == 0 {
				t.Fatalf("real wait missing: %v", err)
			}
			cancellationAssign(t, s, receipt.Graph.Attempts[0], domain.AssignmentClaimed, f.Requirements.Members[0].Route)
			p := cancellationParentRow(t, s, f.Parent.AttemptID)
			retry := p
			retry.ID = "independent-history"
			retry.Number++
			retry.Revision = 1
			retry.AssignmentID = ""
			retry.ThreadID = ""
			retry.Progress = domain.ProgressReady
			retry.Control = domain.ControlUnassigned
			retry.CompletedAt = nil
			switch mode {
			case "newest-progress":
				retry.Progress = "invalid"
			case "older-control":
				retry.Control = "invalid"
			case "older-completed":
				now := time.Now()
				retry.CompletedAt = &now
			case "older-thread":
				retry.ThreadID = "foreign"
			}
			if mode == "newest-progress" || mode == "older-control" || mode == "older-completed" || mode == "older-thread" {
				cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{retry}})
				if mode != "newest-progress" {
					newest := retry
					newest.ID = "valid-newest"
					newest.Number++
					newest.Progress = domain.ProgressReady
					newest.Control = domain.ControlUnassigned
					newest.CompletedAt = nil
					newest.ThreadID = ""
					cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{newest}})
				}
			}
			if mode == "run-progress" {
				for _, run := range cancellationRecords(t, s).WorkflowRuns {
					if run.ID == f.Parent.RunID {
						run.Progress = "invalid"
						run.Revision++
						cancellationSave(t, s, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}})
					}
				}
			}
			if mode == "offered-deadline" {
				assignment := cancellationParentAssignment(t, s, f.Parent.AssignmentID)
				assignment.State = domain.AssignmentOffered
				cancellationSave(t, s, CoordinatorRecords{Assignments: []domain.Assignment{assignment}})
			}
			// Deadline cannot turn an invalid history into authority.
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
				t.Fatalf("invalid history accepted: %+v", got)
			}
			if before != cancellationSnapshot(t, s) {
				t.Fatal("refusal changed durable tables including parked wait and claimed token")
			}
		})
	}
}
func TestIndependentCancellationRepairUnrelatedRouteCompatibility(t *testing.T) {
	for _, mode := range []string{"omitted", "null"} {
		t.Run(mode, func(t *testing.T) {
			s, f, cp, _ := parentWaitFixture(t)
			unrelated := domain.Assignment{ID: "unrelated", AttemptID: "unrelated-attempt", WorkerID: "unrelated-worker", Epoch: 1, State: domain.AssignmentReleased}
			cancellationSave(t, s, CoordinatorRecords{Assignments: []domain.Assignment{unrelated}})
			query := "UPDATE coordinator_assignments SET record=json_remove(record,'$.route') WHERE id='unrelated'"
			if mode == "null" {
				query = "UPDATE coordinator_assignments SET record=json_set(record,'$.route',NULL) WHERE id='unrelated'"
			}
			if _, err := s.db.Exec(query); err != nil {
				t.Fatal(err)
			}
			before := cancellationSnapshot(t, s)
			got, err := s.ReconcileReviewChildCancellation(context.Background(), f, cp)
			if err != nil || got.Status != "no-action" || before != cancellationSnapshot(t, s) {
				t.Fatalf("runtime route compatibility: %+v %v", got, err)
			}
		})
	}
}
