package sqlite

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

// Replay must not recreate a retained binding after its owner advances or
// revokes it, and a late replay refusal rolls back a fresh earlier sibling.
func TestIndependentRepair2ReplayOwnerBoundary(t *testing.T) {
	for _, boundary := range []string{"missing-audit", "revoked-token", "owner-advanced"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := context.Background()
			s := openFleetTestStore(t)
			defer func() { s.Close() }()
			claimFleetAssignment(t, s)
			records, err := s.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			current := records.Attempts[0]
			custody := records.Assignments[0]
			current.Progress = domain.ProgressWaitingExternal
			current.Control = domain.ControlWaitingExternal
			if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{current}}); err != nil {
				t.Fatal(err)
			}
			now := fleetTestTime.Add(3 * time.Second)
			next := current
			next.Revision++
			next.UpdatedAt = now
			released := custody
			released.State = domain.AssignmentReleased
			released.LeaseExpiresAt = time.Time{}
			released.UpdatedAt = now
			target := domain.WorkerStateTransition{CoordinatorEpoch: 1, WorkerID: custody.WorkerID, WorkerEpoch: custody.WorkerEpoch, WorkerSequence: 1, TransitionedAt: now, ExpectedAssignment: custody, ExpectedAttemptRevision: current.Revision, Assignment: released, Attempt: next, Reason: "observed-released"}
			if _, err = s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{target}); err != nil {
				t.Fatal(err)
			}
			before := cancellationSnapshot(t, s)
			if _, err = s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{target}); err != nil || before != cancellationSnapshot(t, s) {
				t.Fatalf("audited exact replay %v", err)
			}
			live := current
			live.ID = "sibling"
			live.TaskID = "sibling-task"
			live.AssignmentID = "sibling-assignment"
			live.Progress = domain.ProgressActive
			live.Control = domain.ControlPreparing
			la := custody
			la.ID = live.AssignmentID
			la.AttemptID = live.ID
			la.DispatchToken = "sibling-token"
			la.LeaseToken = "sibling-lease"
			if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{live}, Assignments: []domain.Assignment{la}}); err != nil {
				t.Fatal(err)
			}
			first := target
			first.ExpectedAssignment = la
			first.Assignment = la
			first.ExpectedAttemptRevision = live.Revision
			first.Attempt = live
			first.Attempt.Control = domain.ControlRunning
			first.Attempt.Revision++
			first.Attempt.UpdatedAt = now
			switch boundary {
			case "missing-audit":
				if _, err = s.db.ExecContext(ctx, "DELETE FROM coordinator_audit_events WHERE id = ?", workerStateAuditID(target)); err != nil {
					t.Fatal(err)
				}
			case "revoked-token":
				released.DispatchToken = ""
				if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Assignments: []domain.Assignment{released}}); err != nil {
					t.Fatal(err)
				}
			case "owner-advanced":
				next.Progress = domain.ProgressFailed
				next.Control = domain.ControlStopped
				next.CompletedAt = &now
				next.Failure = "execution abandoned"
				next.Revision++
				if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{next}}); err != nil {
					t.Fatal(err)
				}
			}
			before = cancellationSnapshot(t, s)
			if _, err = s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{first, target}); err == nil {
				t.Fatal("replay recreated lost authority")
			}
			if before != cancellationSnapshot(t, s) {
				t.Fatal("late replay refusal left sibling writes/audit")
			}
			s = reopenParkProof(t, s)
			if before != cancellationSnapshot(t, s) {
				t.Fatal("rollback changed across reopen")
			}
			if _, err = s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{first}); err != nil {
				t.Fatalf("valid sibling blocked independently: %v", err)
			}
		})
	}
}
