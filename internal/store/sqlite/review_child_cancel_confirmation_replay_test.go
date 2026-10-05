package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Confirmation must be validated even when every existing child is terminal.
func TestReviewChildCancellationConfirmationTerminalReplay(t *testing.T) {
	for _, state := range []domain.AssignmentState{domain.AssignmentClaimed, domain.AssignmentUnknown} {
		for _, key := range []string{"dispatchConfirmedAt", "DispatchConfirmedAt", `dispatchConf\u0069rmedAt`, "single-time"} {
			t.Run(string(state)+"/"+key, func(t *testing.T) {
				ctx := context.Background()
				s, f, cp, receipt := parentWaitFixture(t)
				if _, err := s.WaitReviewParent(ctx, f, cp); err != nil {
					t.Fatal(err)
				}
				cancellationAssign(t, s, receipt.Graph.Attempts[0], state, f.Requirements.Members[0].Route)
				_, offer := repair2Offer(t, s, f)
				s.now = func() time.Time { return *receipt.Graph.Tasks[0].Deadline }
				s = repair2Reopen(t, s)
				parent := repair2ParentBytes(t, s, f.Parent.RunID)
				got, err := s.ReconcileReviewChildCancellation(ctx, f, cp)
				if err != nil || len(got.CancelledAttempts) != len(receipt.Graph.Attempts) || !got.WorkerStopPending {
					t.Fatalf("initial cleanup: %+v %v", got, err)
				}
				for _, a := range cancellationRecords(t, s).Attempts {
					if a.WorkflowRunID == cp.RoundID && !a.Progress.Terminal() {
						t.Fatalf("child not terminal: %+v", a)
					}
				}
				if parent != repair2ParentBytes(t, s, f.Parent.RunID) {
					t.Fatal("cleanup changed raw parent/history/assignment/wait bytes")
				}
				raw, err := json.Marshal(offer)
				if err != nil {
					t.Fatal(err)
				}
				suffix := `, "dispatchConfirmedAt":"2026-10-04T00:00:00Z","` + key + `":null}`
				if key == "single-time" {
					suffix = `, "dispatchConfirmedAt":"2026-10-04T00:00:00Z"}`
				}
				raw = append(raw[:len(raw)-1], []byte(suffix)...)
				if _, err = s.db.Exec("UPDATE coordinator_assignments SET record=? WHERE id=?", raw, offer.ID); err != nil {
					t.Fatal(err)
				}
				s = repair2Reopen(t, s)
				before := cancellationSnapshot(t, s)
				rawParent := repair2ParentBytes(t, s, f.Parent.RunID)
				if _, err = s.ReconcileReviewChildCancellation(ctx, f, cp); err == nil {
					t.Fatal("terminal replay accepted corrupt confirmation")
				}
				if before != cancellationSnapshot(t, s) || rawParent != repair2ParentBytes(t, s, f.Parent.RunID) {
					t.Fatal("refusal changed application tables or raw parent/wait bytes")
				}
				s = repair2Reopen(t, s)
				if before != cancellationSnapshot(t, s) {
					t.Fatal("refusal mutation persisted")
				}
				cancellationSave(t, s, CoordinatorRecords{Assignments: []domain.Assignment{offer}})
				restored := cancellationSnapshot(t, s)
				again, err := s.ReconcileReviewChildCancellation(ctx, f, cp)
				if err != nil || !again.WorkerStopPending || len(again.CancelledAttempts) != 0 || restored != cancellationSnapshot(t, s) {
					t.Fatalf("restored replay: %+v %v", again, err)
				}
			})
		}
	}
}
