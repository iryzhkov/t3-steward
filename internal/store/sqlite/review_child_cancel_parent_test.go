package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func cancellationParentRow(t *testing.T, s *Store, id string) domain.Attempt {
	t.Helper()
	for _, a := range cancellationRecords(t, s).Attempts {
		if a.ID == id {
			return a
		}
	}
	t.Fatal("parent absent")
	return domain.Attempt{}
}
func cancellationParentAssignment(t *testing.T, s *Store, id string) domain.Assignment {
	t.Helper()
	for _, a := range cancellationRecords(t, s).Assignments {
		if a.ID == id {
			return a
		}
	}
	t.Fatal("assignment absent")
	return domain.Assignment{}
}

// Every corrupt parent record must refuse even when a separate disposition would
// justify cleanup. Reopen prevents relying on in-memory state; snapshots include
// all durable tables and their indexed columns, not just decoded records.
func TestReviewChildCancellationParentCoherenceCrossProduct(t *testing.T) {
	branches := []string{"healthy", "run-ended", "attempt-ended", "superseded", "ownership-lost", "deadline"}
	corruptions := []string{"original-progress", "original-control", "original-completed", "latest-progress", "latest-control", "latest-completed", "latest-thread", "latest-ref", "latest-ambiguous-control", "offered-original", "foreign-thread", "foreign-ref", "foreign-assignment-thread", "foreign-route", "run-progress", "ambiguous-progress", "ambiguous-control", "ambiguous-thread", "ambiguous-ref", "ambiguous-completed", "ambiguous-run-progress", "ambiguous-route"}
	for _, branch := range branches {
		for _, corruption := range corruptions {
			t.Run(branch+"/"+corruption, func(t *testing.T) {
				s, f, cp, receipt := parentWaitFixture(t)
				p := cancellationParentRow(t, s, f.Parent.AttemptID)
				assignment := cancellationParentAssignment(t, s, f.Parent.AssignmentID)
				if branch == "attempt-ended" {
					p.Progress = domain.ProgressFailed
					p.Control = domain.ControlStopped
				}
				if branch == "run-ended" {
					for _, run := range cancellationRecords(t, s).WorkflowRuns {
						if run.ID == f.Parent.RunID {
							run.Progress = domain.ProgressSucceeded
							run.Revision++
							cancellationSave(t, s, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}})
						}
					}
				}
				if branch == "ownership-lost" {
					assignment.State = domain.AssignmentUnknown
				}
				if branch == "deadline" {
					s.now = func() time.Time { return *receipt.Graph.Tasks[0].Deadline }
				}
				retry := p
				retry.ID = "coherent-parent-retry"
				retry.Number++
				retry.Revision = 1
				retry.Progress = domain.ProgressReady
				retry.Control = domain.ControlUnassigned
				retry.AssignmentID = ""
				retry.ThreadID = ""
				retry.CompletedAt = nil
				latest := corruption == "latest-progress" || corruption == "latest-control" || corruption == "latest-completed" || corruption == "latest-thread" || corruption == "latest-ref" || corruption == "latest-ambiguous-control"
				if corruption == "original-progress" {
					p.Progress = "invalid-progress"
				}
				if corruption == "original-control" {
					p.Control = "invalid-control"
				}
				if corruption == "original-completed" {
					p.Progress = domain.ProgressActive
					now := time.Now()
					p.CompletedAt = &now
				}
				if corruption == "latest-progress" {
					retry.Progress = "invalid-progress"
				}
				if corruption == "latest-control" {
					retry.Control = "invalid-control"
				}
				if corruption == "latest-completed" {
					now := time.Now()
					retry.CompletedAt = &now
				}
				if corruption == "latest-thread" {
					retry.ThreadID = "foreign"
				}
				if corruption == "latest-ref" {
					retry.AssignmentID = p.AssignmentID
				}
				if corruption == "offered-original" {
					assignment.State = domain.AssignmentOffered
				}
				if corruption == "foreign-thread" {
					p.ThreadID = "foreign"
				}
				if corruption == "foreign-ref" {
					p.AssignmentID = "foreign"
				}
				if corruption == "foreign-assignment-thread" {
					assignment.ThreadID = "foreign"
				}
				if corruption == "foreign-route" {
					assignment.Route.Model = "foreign"
				}
				p.Revision++
				cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{p}, Assignments: []domain.Assignment{assignment}})
				if branch == "superseded" || latest {
					cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{retry}})
				}
				if corruption == "latest-ambiguous-control" {
					raw, err := json.Marshal(retry)
					if err != nil {
						t.Fatal(err)
					}
					raw = append(raw[:len(raw)-1], []byte(",\"Control\":\"unassigned\"}")...)
					if _, err = s.db.Exec("UPDATE coordinator_attempts SET record=? WHERE id=?", raw, retry.ID); err != nil {
						t.Fatal(err)
					}
				}
				if corruption == "run-progress" {
					for _, run := range cancellationRecords(t, s).WorkflowRuns {
						if run.ID == f.Parent.RunID {
							run.Progress = "invalid-progress"
							run.Revision++
							cancellationSave(t, s, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}})
						}
					}
				}
				keys := map[string]string{"ambiguous-progress": "Progress", "ambiguous-control": "Control", "ambiguous-thread": "ThreadId", "ambiguous-ref": "AssignmentId", "ambiguous-completed": "CompletedAt"}
				if key, ok := keys[corruption]; ok {
					raw, err := json.Marshal(p)
					if err != nil {
						t.Fatal(err)
					}
					extra := `"invalid"`
					if key == "CompletedAt" {
						extra = "null"
					}
					raw = append(raw[:len(raw)-1], []byte(",\""+key+"\":"+extra+"}")...)
					if _, err = s.db.Exec("UPDATE coordinator_attempts SET record=? WHERE id=?", raw, p.ID); err != nil {
						t.Fatal(err)
					}
				}
				if corruption == "ambiguous-run-progress" {
					if _, err := s.db.Exec(`UPDATE coordinator_workflow_runs SET record=substr(record,1,length(record)-1)||',"Progress":"failed"}' WHERE id=?`, f.Parent.RunID); err != nil {
						t.Fatal(err)
					}
				}
				if corruption == "ambiguous-route" {
					if _, err := s.db.Exec(`UPDATE coordinator_assignments SET record=json_set(record,'$.route.Model','foreign') WHERE id=?`, assignment.ID); err != nil {
						t.Fatal(err)
					}
				}
				path := s.path
				clock := s.now
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				reopened.now = clock
				before := cancellationSnapshot(t, reopened)
				got, err := reopened.ReconcileReviewChildCancellation(context.Background(), f, cp)
				if err == nil {
					t.Fatalf("corrupt parent accepted: %+v", got)
				}
				if before != cancellationSnapshot(t, reopened) {
					t.Fatal("refusal changed full durable snapshot")
				}
			})
		}
	}
}

func TestReviewChildCancellationParentCompatibility(t *testing.T) {
	for _, kind := range []string{"advancement", "released-ref", "completed-ref", "released-both", "terminal-no-time", "terminal-time", "retry-terminal", "replacement-thread"} {
		t.Run(kind, func(t *testing.T) {
			s, f, cp, _ := parentWaitFixture(t)
			p := cancellationParentRow(t, s, f.Parent.AttemptID)
			assignment := cancellationParentAssignment(t, s, f.Parent.AssignmentID)
			switch kind {
			case "advancement":
				p.Progress = domain.ProgressWaitingExternal
				p.Control = domain.ControlWaitingExternal
			case "released-ref":
				assignment.State = domain.AssignmentReleased
				p.AssignmentID = ""
				p.Progress = domain.ProgressReady
				p.Control = domain.ControlUnassigned
			case "completed-ref":
				assignment.State = domain.AssignmentCompleted
				p.AssignmentID = ""
				p.Progress = domain.ProgressVerifying
				p.Control = domain.ControlStopped
			case "released-both":
				assignment.State = domain.AssignmentReleased
				p.AssignmentID = ""
				p.ThreadID = ""
				p.Progress = domain.ProgressReady
				p.Control = domain.ControlUnassigned
			case "terminal-no-time", "terminal-time":
				p.Progress = domain.ProgressSucceeded
				p.Control = domain.ControlStopped
				if kind == "terminal-time" {
					now := time.Now()
					p.CompletedAt = &now
				}
			case "retry-terminal":
				retry := p
				retry.ID = "terminal-retry"
				retry.Number++
				retry.Revision = 1
				retry.AssignmentID = ""
				retry.ThreadID = ""
				retry.Progress = domain.ProgressSucceeded
				retry.Control = domain.ControlStopped
				cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{retry}})
			case "replacement-thread":
				assignment.Epoch++
				assignment.ThreadID = "replacement"
				assignment.Route.Model = "replacement"
				p.ThreadID = assignment.ThreadID
			}
			p.Revision++
			cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{p}, Assignments: []domain.Assignment{assignment}})
			before := cancellationSnapshot(t, s)
			got, err := s.ReconcileReviewChildCancellation(context.Background(), f, cp)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "advancement" {
				if got.Status != "no-action" || before != cancellationSnapshot(t, s) {
					t.Fatalf("advancement: %+v", got)
				}
			} else if got.Status != "quiescent" || len(got.CancelledAttempts) == 0 {
				t.Fatalf("cleanup: %+v", got)
			}
			after := cancellationSnapshot(t, s)
			if _, err = s.ReconcileReviewChildCancellation(context.Background(), f, cp); err != nil || after != cancellationSnapshot(t, s) {
				t.Fatalf("replay: %v", err)
			}
		})
	}
}
