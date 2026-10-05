package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestIndependentRepair2CurrentAndReplayBoundary(t *testing.T) {
	for _, shape := range []string{"progress", "control", "both"} {
		for _, state := range []domain.AssignmentState{domain.AssignmentClaimed, domain.AssignmentUnknown} {
			t.Run(shape+"/"+string(state), func(t *testing.T) {
				ctx := context.Background()
				s := openFleetTestStore(t)
				defer func() { s.Close() }()
				claimFleetAssignment(t, s)
				records, err := s.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				at, as := records.Attempts[0], records.Assignments[0]
				at.Progress, at.Control = domain.ProgressActive, domain.ControlRunning
				if shape != "control" {
					at.Progress = domain.ProgressWaitingExternal
				}
				if shape != "progress" {
					at.Control = domain.ControlWaitingExternal
				}
				as.State = state
				if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{at}, Assignments: []domain.Assignment{as}}); err != nil {
					t.Fatal(err)
				}
				now := fleetTestTime.Add(3 * time.Second)
				next, released := at, as
				next.Revision++
				next.UpdatedAt = now
				released.State = domain.AssignmentReleased
				released.LeaseExpiresAt = time.Time{}
				released.UpdatedAt = now
				valid := domain.WorkerStateTransition{CoordinatorEpoch: 1, WorkerID: as.WorkerID, WorkerEpoch: as.WorkerEpoch, WorkerSequence: 1, TransitionedAt: now, ExpectedAssignment: as, ExpectedAttemptRevision: at.Revision, Assignment: released, Attempt: next, Reason: "observed-released"}
				live, la := at, as
				live.ID = "independent-live"
				live.TaskID = "independent-task"
				live.AssignmentID = "independent-assignment"
				live.Progress, live.Control = domain.ProgressActive, domain.ControlPreparing
				la.ID, la.AttemptID, la.State = live.AssignmentID, live.ID, domain.AssignmentClaimed
				la.LeaseToken, la.DispatchToken = "independent-lease", "independent-token"
				if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{live}, Assignments: []domain.Assignment{la}}); err != nil {
					t.Fatal(err)
				}
				first := valid
				first.ExpectedAssignment, first.Assignment, first.Attempt = la, la, live
				first.Attempt.Revision++
				first.Attempt.UpdatedAt = now
				first.Attempt.Control = domain.ControlRunning
				refuse := func(label string, tr domain.WorkerStateTransition, stale bool) {
					t.Helper()
					before := cancellationSnapshot(t, s)
					_, e := s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{first, tr})
					if e == nil || stale && !errors.Is(e, ErrStaleWorkerStateTransition) {
						t.Fatalf("%s accepted or wrong refusal: %v", label, e)
					}
					if before != cancellationSnapshot(t, s) {
						t.Fatalf("%s changed full tables/audit/earlier sibling", label)
					}
					s = reopenParkProof(t, s)
					if before != cancellationSnapshot(t, s) {
						t.Fatalf("%s rollback lost after reopen", label)
					}
				}
				// Compound edits evade the static retained-reference test, but CURRENT park
				// must still forbid clearing the binding, unpark, or forged termination.
				for _, terminal := range []bool{false, true} {
					forged := valid
					forged.Attempt.AssignmentID = ""
					forged.Attempt.Progress, forged.Attempt.Control = domain.ProgressReady, domain.ControlUnassigned
					if terminal {
						forged.Attempt.Progress = domain.ProgressFailed
						forged.Attempt.Control = domain.ControlStopped
						forged.Attempt.CompletedAt = &now
					}
					if e := validateWorkerStateTransition(forged); e != nil {
						t.Fatalf("probe did not reach dynamic fence: %v", e)
					}
					refuse("compound-clear", forged, true)
				}
				// Exact projected rows alone are not native commit evidence.
				if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{next}, Assignments: []domain.Assignment{released}}); err != nil {
					t.Fatal(err)
				}
				refuse("imported-equal-without-audit", valid, false)
				if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{at}, Assignments: []domain.Assignment{as}}); err != nil {
					t.Fatal(err)
				}
				// Commit only the park: the earlier live sibling remains fresh during replay.
				if _, err = s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{valid}); err != nil {
					t.Fatal(err)
				}
				before := cancellationSnapshot(t, s)
				if _, err = s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{valid}); err != nil || before != cancellationSnapshot(t, s) {
					t.Fatalf("audited replay %v", err)
				}
				if _, err = s.db.ExecContext(ctx, "DELETE FROM coordinator_audit_events WHERE id = ?", workerStateAuditID(valid)); err != nil {
					t.Fatal(err)
				}
				refuse("missing-native-audit-after-fresh-sibling", valid, false)
			})
		}
	}
}
