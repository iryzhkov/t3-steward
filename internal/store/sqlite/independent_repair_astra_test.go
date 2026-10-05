package sqlite

import (
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

// Actual Astra repair review overlay, retained with its original assertions.
func TestIndependentCancellationRepairAdditional(t *testing.T) {
	for _, kind := range []string{"unrelated-route-omitted", "unrelated-route-null", "duplicate-route-model", "escaped-route-model", "duplicate-control", "escaped-control", "middle-history-control", "middle-history-thread", "offered-retry", "unassigned-retry", "claimed-retry"} {
		t.Run(kind, func(t *testing.T) {
			s, f, cp, receipt := parentWaitFixture(t)
			if _, err := s.WaitReviewParent(context.Background(), f, cp); err != nil {
				t.Fatal(err)
			}
			cancellationAssign(t, s, receipt.Graph.Attempts[0], domain.AssignmentClaimed, f.Requirements.Members[0].Route)
			p := cancellationParentRow(t, s, f.Parent.AttemptID)
			p.Progress = domain.ProgressFailed
			p.Control = domain.ControlStopped
			p.Revision++
			cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{p}})
			wantRefusal := false
			switch kind {
			case "unrelated-route-omitted", "unrelated-route-null":
				a := domain.Assignment{ID: "unrelated-assignment", AttemptID: "unrelated-attempt", WorkerID: "unrelated-worker", Epoch: 1, State: domain.AssignmentReleased}
				cancellationSave(t, s, CoordinatorRecords{Assignments: []domain.Assignment{a}})
				query := "UPDATE coordinator_assignments SET record=json_remove(record,'$.route') WHERE id=?"
				if kind == "unrelated-route-null" {
					query = "UPDATE coordinator_assignments SET record=json_set(record,'$.route',NULL) WHERE id=?"
				}
				if _, err := s.db.Exec(query, a.ID); err != nil {
					t.Fatal(err)
				}
			case "duplicate-route-model", "escaped-route-model":
				wantRefusal = true
				a := cancellationParentAssignment(t, s, f.Parent.AssignmentID)
				raw, err := json.Marshal(a)
				if err != nil {
					t.Fatal(err)
				}
				var fields map[string]json.RawMessage
				if err = json.Unmarshal(raw, &fields); err != nil {
					t.Fatal(err)
				}
				route := fields["route"]
				key := "model"
				if kind == "escaped-route-model" {
					key = `mo\u0064el`
				}
				route = append(route[:len(route)-1], []byte(",\""+key+"\":\"foreign\"}")...)
				fields["route"] = route
				raw, err = json.Marshal(fields)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = s.db.Exec("UPDATE coordinator_assignments SET record=? WHERE id=?", raw, a.ID); err != nil {
					t.Fatal(err)
				}
			case "duplicate-control", "escaped-control":
				wantRefusal = true
				raw, err := json.Marshal(p)
				if err != nil {
					t.Fatal(err)
				}
				key := "control"
				if kind == "escaped-control" {
					key = `con\u0074rol`
				}
				raw = append(raw[:len(raw)-1], []byte(",\""+key+"\":\"stopped\"}")...)
				if _, err = s.db.Exec("UPDATE coordinator_attempts SET record=? WHERE id=?", raw, p.ID); err != nil {
					t.Fatal(err)
				}
			default:
				retry := p
				retry.ID = "independent-middle"
				retry.Number++
				retry.Revision = 1
				retry.Progress = domain.ProgressReady
				retry.Control = domain.ControlUnassigned
				retry.AssignmentID = ""
				retry.ThreadID = ""
				retry.CompletedAt = nil
				if kind == "middle-history-control" || kind == "middle-history-thread" {
					wantRefusal = true
					latest := retry
					latest.ID = "independent-latest"
					latest.Number++
					cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{latest}})
					if kind == "middle-history-control" {
						retry.Control = "invalid-middle-control"
					} else {
						retry.ThreadID = "foreign-middle-thread"
					}
				}
				cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{retry}})
				if kind == "offered-retry" {
					cancellationAssign(t, s, retry, domain.AssignmentOffered, f.Parent.ExecutorRoute)
				}
				if kind == "claimed-retry" {
					cancellationAssign(t, s, retry, domain.AssignmentClaimed, f.Parent.ExecutorRoute)
				}
			}
			path := s.path
			clock := s.now
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			s.now = clock
			before := cancellationSnapshot(t, s)
			got, err := s.ReconcileReviewChildCancellation(context.Background(), f, cp)
			if wantRefusal {
				if err == nil {
					t.Fatalf("corruption accepted: %+v", got)
				}
				if before != cancellationSnapshot(t, s) {
					t.Fatal("refusal changed full tables")
				}
			} else {
				if err != nil || got.Status != "stop-requested" || !got.WorkerStopPending || len(got.CancelledAttempts) != len(receipt.Graph.Attempts) {
					t.Fatalf("coherent cleanup refused or lost custody: %+v err=%v unchanged=%v", got, err, before == cancellationSnapshot(t, s))
				}
			}
		})
	}
}

func TestIndependentCancellationRepairOfferedRetryDispositions(t *testing.T) {
	for _, branch := range []string{"superseded", "run-ended", "ownership-lost", "deadline"} {
		t.Run(branch, func(t *testing.T) {
			s, f, cp, receipt := parentWaitFixture(t)
			p := cancellationParentRow(t, s, f.Parent.AttemptID)
			retry := p
			retry.ID = "offered-parent-retry"
			retry.Number++
			retry.Revision = 1
			retry.Progress = domain.ProgressReady
			retry.Control = domain.ControlUnassigned
			retry.ThreadID = ""
			retry.AssignmentID = ""
			cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{retry}})
			cancellationAssign(t, s, retry, domain.AssignmentOffered, f.Parent.ExecutorRoute)
			if branch == "run-ended" {
				for _, r := range cancellationRecords(t, s).WorkflowRuns {
					if r.ID == f.Parent.RunID {
						r.Progress = domain.ProgressSucceeded
						r.Revision++
						cancellationSave(t, s, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{r}})
					}
				}
			}
			if branch == "ownership-lost" {
				a := cancellationParentAssignment(t, s, f.Parent.AssignmentID)
				a.State = domain.AssignmentUnknown
				cancellationSave(t, s, CoordinatorRecords{Assignments: []domain.Assignment{a}})
			}
			if branch == "deadline" {
				s.now = func() time.Time { return *receipt.Graph.Tasks[0].Deadline }
			}
			before := cancellationSnapshot(t, s)
			got, err := s.ReconcileReviewChildCancellation(context.Background(), f, cp)
			if err != nil || len(got.CancelledAttempts) != len(receipt.Graph.Attempts) {
				t.Fatalf("valid offered retry prevents cleanup: %+v err=%v unchanged=%v", got, err, before == cancellationSnapshot(t, s))
			}
		})
	}
}
