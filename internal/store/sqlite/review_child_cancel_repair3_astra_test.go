// Retained complete actual astra repair2 review overlay; assertions unchanged.
package sqlite

import (
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Disposable independent repair2 probes. Durable fixture API, not live planning.
func repair2Offer(t *testing.T, s *Store, f review.FrozenAuthority) (domain.Attempt, domain.Assignment) {
	t.Helper()
	p := cancellationParentRow(t, s, f.Parent.AttemptID)
	p.ID = "independent-repair2-parent-retry"
	p.Number++
	p.Revision = 1
	p.Progress = domain.ProgressReady
	p.Control = domain.ControlUnassigned
	p.ThreadID = ""
	p.AssignmentID = ""
	p.CompletedAt = nil
	cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{p}})
	snapshots, err := s.LoadWorkerSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var w domain.WorkerSnapshot
	for _, x := range snapshots {
		if x.WorkerID == "worker" {
			w = x
		}
	}
	if w.WorkerID == "" {
		t.Fatal("missing fixture worker")
	}
	a := domain.Assignment{ID: "independent-repair2-offer", AttemptID: p.ID, WorkerID: w.WorkerID, Project: f.Parent.Repository,
		Route: domain.ProviderRoute{ProviderInstanceID: "codex", Model: "sol"}, State: domain.AssignmentOffered, Epoch: int64(p.Number),
		ThreadID: "independent-reserved-thread", DispatchToken: "independent-reserved-token", LeaseToken: "independent-lease",
		Estimate: &domain.TaskAdmissionEstimate{RemainingCost: 10, ExpectedRuntime: time.Hour, CheckpointMargin: time.Minute}, ExecutorDemand: &domain.ResourceDemand{}}
	offers, err := s.CommitAssignmentPlan(context.Background(), domain.AssignmentPlanCommit{CoordinatorEpoch: w.CoordinatorEpoch, CommittedAt: w.ObservedAt,
		Items: []domain.AssignmentPlanItem{{ExpectedAttemptRevision: p.Revision, WorkerEpoch: w.WorkerEpoch, WorkerSnapshotSequence: w.Sequence, Assignment: a}}})
	if err != nil || len(offers) != 1 {
		t.Fatalf("commit offer: %+v %v", offers, err)
	}
	p = cancellationParentRow(t, s, p.ID)
	a = offers[0]
	if p.ThreadID != "" || p.AssignmentID != a.ID || a.ThreadID == "" || a.DispatchToken == "" || a.DispatchState != "" || a.DispatchRevision != 0 || a.DispatchConfirmedAt != nil {
		t.Fatalf("not reserved: %+v %+v", p, a)
	}
	return p, a
}
func repair2Reopen(t *testing.T, s *Store) *Store {
	t.Helper()
	path, clock := s.path, s.now
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	next.now = clock
	t.Cleanup(func() { next.Close() })
	return next
}
func repair2ParentBytes(t *testing.T, s *Store, run string) string {
	t.Helper()
	var out []string
	for _, q := range []string{
		"SELECT record FROM coordinator_attempts WHERE workflow_run_id='" + run + "' ORDER BY id",
		"SELECT record FROM coordinator_assignments WHERE attempt_id IN (SELECT id FROM coordinator_attempts WHERE workflow_run_id='" + run + "') ORDER BY id",
		"SELECT record FROM coordinator_task_waits ORDER BY id",
	} {
		rows, err := s.db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var raw string
			if err = rows.Scan(&raw); err != nil {
				t.Fatal(err)
			}
			out = append(out, raw)
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}
	return strings.Join(out, "\n")
}
func TestIndependentCancellationRepair2ConfirmationAmbiguity(t *testing.T) {
	for _, branch := range []string{"ended", "superseded", "run-ended", "ownership-lost", "deadline"} {
		for _, mode := range []string{"omitted", "canonical-null", "confirmed", "duplicate-confirmed", "case-confirmed", "escaped-confirmed", "duplicate-state", "duplicate-revision"} {
			t.Run(branch+"/"+mode, func(t *testing.T) {
				t.Parallel()
				ctx := context.Background()
				s, f, cp, receipt := parentWaitFixture(t)
				if _, err := s.WaitReviewParent(ctx, f, cp); err != nil {
					t.Fatal(err)
				}
				child := cancellationAssign(t, s, receipt.Graph.Attempts[0], domain.AssignmentClaimed, f.Requirements.Members[0].Route)
				_, offer := repair2Offer(t, s, f)
				switch branch {
				case "ended":
					cancellationEndParent(t, s, f, domain.ProgressFailed)
				case "run-ended":
					for _, r := range cancellationRecords(t, s).WorkflowRuns {
						if r.ID == f.Parent.RunID {
							r.Progress = domain.ProgressSucceeded
							r.Revision++
							cancellationSave(t, s, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{r}})
						}
					}
				case "ownership-lost":
					a := cancellationParentAssignment(t, s, f.Parent.AssignmentID)
					a.State = domain.AssignmentUnknown
					cancellationSave(t, s, CoordinatorRecords{Assignments: []domain.Assignment{a}})
				case "deadline":
					s.now = func() time.Time { return *receipt.Graph.Tasks[0].Deadline }
				}
				suffix := ""
				switch mode {
				case "canonical-null":
					suffix = `, "dispatchConfirmedAt":null`
				case "confirmed":
					suffix = `, "dispatchConfirmedAt":"2026-10-04T00:00:00Z"`
				case "duplicate-confirmed":
					suffix = `, "dispatchConfirmedAt":"2026-10-04T00:00:00Z","dispatchConfirmedAt":null`
				case "case-confirmed":
					suffix = `, "dispatchConfirmedAt":"2026-10-04T00:00:00Z","DispatchConfirmedAt":null`
				case "escaped-confirmed":
					suffix = `, "dispatchConfirmedAt":"2026-10-04T00:00:00Z","dispatchConf\u0069rmedAt":null`
				case "duplicate-state":
					suffix = `, "dispatchState":"confirmed","dispatchState":""`
				case "duplicate-revision":
					suffix = `, "dispatchRevision":1,"dispatchRevision":0`
				}
				if suffix != "" {
					raw, err := json.Marshal(offer)
					if err != nil {
						t.Fatal(err)
					}
					raw = append(raw[:len(raw)-1], []byte(suffix+"}")...)
					if _, err = s.db.Exec("UPDATE coordinator_assignments SET record=? WHERE id=?", raw, offer.ID); err != nil {
						t.Fatal(err)
					}
				}
				s = repair2Reopen(t, s)
				before := cancellationSnapshot(t, s)
				parent := repair2ParentBytes(t, s, f.Parent.RunID)
				got, err := s.ReconcileReviewChildCancellation(ctx, f, cp)
				if mode != "omitted" && mode != "canonical-null" {
					if err == nil {
						retained := cancellationParentAssignment(t, s, child.ID)
						t.Fatalf("ambiguous/dispatched offer accepted: status=%s reason=%q cancelled=%d pending=%v changed=%v tokenRevoked=%v custodyRetained=%v parentBytesPreserved=%v", got.Status, got.Reason, len(got.CancelledAttempts), got.WorkerStopPending, before != cancellationSnapshot(t, s), retained.DispatchToken == "", retained.State == child.State && retained.Epoch == child.Epoch, parent == repair2ParentBytes(t, s, f.Parent.RunID))
					}
					if before != cancellationSnapshot(t, s) {
						t.Fatal("refusal mutated full snapshot")
					}
				} else {
					if err != nil || got.Status != "stop-requested" || !got.WorkerStopPending || len(got.CancelledAttempts) != len(receipt.Graph.Attempts) {
						t.Fatalf("compatible cleanup: %+v %v", got, err)
					}
					if parent != repair2ParentBytes(t, s, f.Parent.RunID) {
						t.Fatal("parent/wait/offer bytes changed")
					}
					retained := cancellationParentAssignment(t, s, child.ID)
					if retained.DispatchToken != "" || retained.State != child.State || retained.Epoch != child.Epoch {
						t.Fatalf("custody: %+v", retained)
					}
				}
			})
		}
	}
}
func TestIndependentCancellationRepair2AtomicReplay(t *testing.T) {
	for _, state := range []domain.AssignmentState{domain.AssignmentClaimed, domain.AssignmentUnknown} {
		t.Run(string(state), func(t *testing.T) {
			ctx := context.Background()
			s, f, cp, receipt := parentWaitFixture(t)
			if _, err := s.WaitReviewParent(ctx, f, cp); err != nil {
				t.Fatal(err)
			}
			child := cancellationAssign(t, s, receipt.Graph.Attempts[0], state, f.Requirements.Members[0].Route)
			retry, offer := repair2Offer(t, s, f)
			s.now = func() time.Time { return *receipt.Graph.Tasks[0].Deadline }
			s = repair2Reopen(t, s)
			if _, err := s.db.Exec("CREATE TRIGGER repair2_token_order BEFORE UPDATE ON coordinator_attempts WHEN json_extract(NEW.record,'$.progress')='cancelled' AND EXISTS(SELECT 1 FROM coordinator_assignments WHERE attempt_id=NEW.id AND COALESCE(json_extract(record,'$.dispatchToken'),'')!='') BEGIN SELECT RAISE(ABORT,'token still live'); END"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec("CREATE TRIGGER repair2_failure BEFORE UPDATE ON coordinator_review_rounds BEGIN SELECT RAISE(ABORT,'repair2 round failure'); END"); err != nil {
				t.Fatal(err)
			}
			before := cancellationSnapshot(t, s)
			parent := repair2ParentBytes(t, s, f.Parent.RunID)
			if _, err := s.ReconcileReviewChildCancellation(ctx, f, cp); err == nil || !strings.Contains(err.Error(), "repair2 round failure") {
				t.Fatalf("did not reach late failure: %v", err)
			}
			if before != cancellationSnapshot(t, s) {
				t.Fatal("partial transaction leaked")
			}
			s = repair2Reopen(t, s)
			if before != cancellationSnapshot(t, s) {
				t.Fatal("rollback not durable")
			}
			if _, err := s.db.Exec("DROP TRIGGER repair2_failure"); err != nil {
				t.Fatal(err)
			}
			got, err := s.ReconcileReviewChildCancellation(ctx, f, cp)
			if err != nil || got.Status != "stop-requested" || !got.WorkerStopPending || got.Reason != "review deadline reached" {
				t.Fatalf("cleanup: %+v %v", got, err)
			}
			if parent != repair2ParentBytes(t, s, f.Parent.RunID) {
				t.Fatal("parent/wait/retry/offer bytes changed")
			}
			retained := cancellationParentAssignment(t, s, child.ID)
			if retained.State != state || retained.Epoch != child.Epoch || retained.ThreadID != child.ThreadID || retained.DispatchToken != "" {
				t.Fatalf("custody: %+v", retained)
			}
			if !reflect.DeepEqual(retry, cancellationParentRow(t, s, retry.ID)) || !reflect.DeepEqual(offer, cancellationParentAssignment(t, s, offer.ID)) {
				t.Fatal("reserved parent modified")
			}
			after := cancellationSnapshot(t, s)
			s = repair2Reopen(t, s)
			again, err := s.ReconcileReviewChildCancellation(ctx, f, cp)
			if err != nil || !again.WorkerStopPending || len(again.CancelledAttempts) != 0 || after != cancellationSnapshot(t, s) {
				t.Fatalf("replay: %+v %v", again, err)
			}
			corrupt := offer
			corrupt.DispatchRevision = 1
			cancellationSave(t, s, CoordinatorRecords{Assignments: []domain.Assignment{corrupt}})
			corruptBefore := cancellationSnapshot(t, s)
			if _, err = s.ReconcileReviewChildCancellation(ctx, f, cp); err == nil || corruptBefore != cancellationSnapshot(t, s) {
				t.Fatalf("replay skipped corrupted history: %v", err)
			}
			cancellationSave(t, s, CoordinatorRecords{Assignments: []domain.Assignment{offer}})
			fresh := receipt.Graph.Attempts[1]
			fresh.ID = "independent-repair2-child-retry"
			fresh.Number = 2
			fresh.Revision = 1
			cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{fresh}})
			final, err := s.ReconcileReviewChildCancellation(ctx, f, cp)
			if err != nil || !reflect.DeepEqual(final.CancelledAttempts, []string{fresh.ID}) || !final.WorkerStopPending {
				t.Fatalf("new child retry: %+v %v", final, err)
			}
			if parent != repair2ParentBytes(t, s, f.Parent.RunID) {
				t.Fatal("late retry changed parent bytes")
			}
		})
	}
}
