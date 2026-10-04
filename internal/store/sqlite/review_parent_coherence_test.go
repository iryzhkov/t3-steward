package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"strings"
	"testing"
	"time"
)

func parentCoherenceSnapshot(t *testing.T, s *Store) string {
	t.Helper()
	records, err := s.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	waits, err := s.ListTaskWaits(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(struct {
		Records CoordinatorRecords
		Waits   []domain.TaskWait
	}{records, waits})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := s.db.Query("SELECT id,workflow_run_id,task_id,number,revision,record FROM coordinator_attempts ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var indexed []string
	for rows.Next() {
		var id, run, task, record string
		var number int
		var revision int64
		if err := rows.Scan(&id, &run, &task, &number, &revision, &record); err != nil {
			t.Fatal(err)
		}
		indexed = append(indexed, fmt.Sprintf("%q/%q/%q/%d/%d/%q", id, run, task, number, revision, record))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return string(raw) + strings.Join(indexed, "\n")
}

func insertParentRetry(t *testing.T, s *Store, id string) {
	t.Helper()
	_, err := s.db.Exec("INSERT INTO coordinator_attempts(id,workflow_run_id,task_id,number,revision,record) SELECT id||'-retry',workflow_run_id,task_id,number+1,revision,json_set(record,'$.id',id||'-retry','$.number',number+1) FROM coordinator_attempts WHERE id=?", id)
	if err != nil {
		t.Fatal(err)
	}
}

// Every return branch is behind the same parent fence, including reopened waits.
func TestReviewParentWaitCoherentOwnershipBranches(t *testing.T) {
	cases := []struct {
		name, query string
		retry       bool
	}{
		{"hidden-run", "UPDATE coordinator_attempts SET workflow_run_id='foreign' WHERE id=?", true},
		{"hidden-task", "UPDATE coordinator_attempts SET task_id='foreign' WHERE id=?", true},
		{"decoded-only", "UPDATE coordinator_attempts SET workflow_run_id='foreign',task_id='foreign' WHERE id=?", true},
		{"indexed-only", "UPDATE coordinator_attempts SET record=json_set(record,'$.workflowRunId','foreign','$.taskId','foreign') WHERE id=?", true},
		{"retry-id", "UPDATE coordinator_attempts SET record=json_set(record,'$.id','foreign') WHERE id=?", true},
		{"retry-number", "UPDATE coordinator_attempts SET record=json_set(record,'$.number',99) WHERE id=?", true},
		{"retry-revision", "UPDATE coordinator_attempts SET record=json_set(record,'$.revision',99) WHERE id=?", true},
		{"decoded-duplicate-number", "UPDATE coordinator_attempts SET workflow_run_id='foreign',number=number-1,record=json_set(record,'$.number',number-1) WHERE id=?", true},
		{"bound-index-id", "UPDATE coordinator_attempts SET id=id||'-moved',workflow_run_id='foreign',task_id='foreign' WHERE id=?", false},
		{"bound-id", "UPDATE coordinator_attempts SET record=json_set(record,'$.id','foreign') WHERE id=?", false},
		{"bound-run", "UPDATE coordinator_attempts SET workflow_run_id='foreign' WHERE id=?", false},
		{"bound-task", "UPDATE coordinator_attempts SET task_id='foreign' WHERE id=?", false},
		{"bound-number", "UPDATE coordinator_attempts SET record=json_set(record,'$.number',99) WHERE id=?", false},
		{"bound-revision", "UPDATE coordinator_attempts SET revision=revision+1 WHERE id=?", false},
		{"zero-number", "UPDATE coordinator_attempts SET number=0,record=json_set(record,'$.number',0) WHERE id=?", false},
		{"zero-revision", "UPDATE coordinator_attempts SET revision=0,record=json_set(record,'$.revision',0) WHERE id=?", false},
	}
	for _, branch := range []string{"pending", "sink-finished", "collected", "live", "settled", "resumed"} {
		for _, tc := range cases {
			t.Run(branch+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				s, f, cp, receipt := parentWaitFixture(t)
				if branch == "live" || branch == "settled" || branch == "resumed" {
					if _, err := s.WaitReviewParent(ctx, f, cp); err != nil {
						t.Fatal(err)
					}
				}
				if branch == "sink-finished" || branch == "settled" || branch == "resumed" {
					records, err := finishedChildRecords(receipt, time.Now().UTC())
					if err != nil {
						t.Fatal(err)
					}
					if err := s.SaveCoordinatorRecords(ctx, records); err != nil {
						t.Fatal(err)
					}
				}
				if branch == "collected" {
					round, err := s.GetReviewRound(ctx, cp.RoundID)
					if err != nil {
						t.Fatal(err)
					}
					for _, m := range round.Reviewers {
						round, err = s.RecordReviewResult(ctx, round.ID, m.ID, round.Revision, review.Result{State: "failed", Failure: "fixture"})
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				if branch == "settled" || branch == "resumed" {
					if err := s.SettleNodeWaits(ctx, time.Now().UTC()); err != nil {
						t.Fatal(err)
					}
				}
				if branch == "resumed" {
					wakes, err := wakeReviewParent(t, s)
					if err != nil || len(wakes) != 1 {
						t.Fatalf("wake: %v %v", wakes, err)
					}
				}
				id := f.Parent.AttemptID
				if tc.retry {
					insertParentRetry(t, s, id)
					id += "-retry"
				}
				if _, err := s.db.Exec(tc.query, id); err != nil {
					t.Fatal(err)
				}
				// The original binding ID must still select a row whose indexed owner differs.
				other, err := Open(s.path)
				if err != nil {
					t.Fatal(err)
				}
				defer other.Close()
				before := parentCoherenceSnapshot(t, other)
				got, err := other.WaitReviewParent(ctx, f, cp)
				if err == nil {
					t.Fatalf("accepted incoherent parent: %+v", got)
				}
				if before != parentCoherenceSnapshot(t, other) {
					t.Fatal("refusal changed full records, indexes or waits")
				}
			})
		}
	}
}

func TestReviewParentWaitProtectedIdentityKeys(t *testing.T) {
	for _, key := range []string{"id", "workflowRunId", "taskId", "number", "revision"} {
		for _, spelling := range []string{"duplicate", "case", "escaped", "unicode"} {
			t.Run(key+"/"+spelling, func(t *testing.T) {
				s, f, cp, _ := parentWaitFixture(t)
				var raw string
				if err := s.db.QueryRow("SELECT record FROM coordinator_attempts WHERE id=?", f.Parent.AttemptID).Scan(&raw); err != nil {
					t.Fatal(err)
				}
				var fields map[string]json.RawMessage
				if err := json.Unmarshal([]byte(raw), &fields); err != nil {
					t.Fatal(err)
				}
				name := key
				switch spelling {
				case "case":
					name = strings.ToUpper(key)
				case "escaped":
					name = fmt.Sprintf("\\u%04x%s", key[0], key[1:])
				case "unicode": // Go's Unicode folding treats long s as S in taskId.
					if key != "taskId" {
						t.Skip("Unicode fold alias applies to taskId")
					}
					name = "taſkId"
				}
				raw = "{\"" + name + "\":" + string(fields[key]) + "," + raw[1:]
				if _, err := s.db.Exec("UPDATE coordinator_attempts SET record=? WHERE id=?", raw, f.Parent.AttemptID); err != nil {
					t.Fatal(err)
				}
				before := parentCoherenceSnapshot(t, s)
				tx, err := s.reviewAuthorityWriteTx(context.Background(), f.Parent)
				if err != nil {
					t.Fatal(err)
				}
				err = reviewParentCurrentTx(context.Background(), tx, f.Parent, false)
				tx.Rollback()
				if !errors.Is(err, ErrReviewAuthorityIdentity) {
					t.Fatalf("parent ambiguity fence: %v", err)
				}
				if _, err := s.WaitReviewParent(context.Background(), f, cp); err == nil {
					t.Fatal("accepted ambiguous identity")
				}
				if before != parentCoherenceSnapshot(t, s) {
					t.Fatal("ambiguity refusal mutated state")
				}
			})
		}
	}
}

func TestReviewParentWaitCanonicalUnrelatedAndJSONLayout(t *testing.T) {
	s, f, cp, _ := parentWaitFixture(t)
	// A foreign attempt may use the same number. Uniqueness belongs to this parent.
	insertParentRetry(t, s, f.Parent.AttemptID)
	if _, err := s.db.Exec("UPDATE coordinator_attempts SET workflow_run_id='foreign',task_id='foreign',number=1,record=json_set(record,'$.workflowRunId','foreign','$.taskId','foreign','$.number',1) WHERE id=?", f.Parent.AttemptID+"-retry"); err != nil {
		t.Fatal(err)
	}
	a := loadAttempt(t, s, f.Parent.AttemptID)
	raw, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	raw, err = json.MarshalIndent(fields, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE coordinator_attempts SET record=? WHERE id=?", raw, a.ID); err != nil {
		t.Fatal(err)
	}
	got, err := s.WaitReviewParent(context.Background(), f, cp)
	if err != nil || got.Status != "parked" {
		t.Fatalf("valid layout/unrelated rejected: %+v %v", got, err)
	}
}

func TestReviewParentWaitIndependentDeadlineConsumption(t *testing.T) {
	s, f, cp, _ := parentWaitFixture(t)
	first, err := s.WaitReviewParent(context.Background(), f, cp)
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return first.Wait.Deadline }
	results, err := s.ExpireTaskWaits(context.Background(), first.Wait.Deadline)
	if err != nil || len(results) != 1 {
		t.Fatalf("expiry: %v %v", results, err)
	}
	before := parentCoherenceSnapshot(t, s)
	settled, err := s.WaitReviewParent(context.Background(), f, cp)
	if err != nil || settled.Status != "settled" || !settled.CollectionPending || settled.Wait.Result == nil ||
		settled.Wait.Result.ExitCode != 2 || settled.Wait.Result.Outcome != "timed-out" ||
		!settled.Wait.Deadline.Equal(first.Wait.Deadline) || before != parentCoherenceSnapshot(t, s) {
		t.Fatalf("timeout replay: %+v %v", settled, err)
	}
	wakes, err := wakeReviewParent(t, s)
	if err != nil || len(wakes) != 1 || wakes[0].AttemptID != f.Parent.AttemptID || wakes[0].ThreadID != f.Parent.ThreadID {
		t.Fatalf("timeout wake: %v %v", wakes, err)
	}
	before = parentCoherenceSnapshot(t, s)
	settled, err = s.WaitReviewParent(context.Background(), f, cp)
	if err != nil || settled.Status != "settled" || before != parentCoherenceSnapshot(t, s) {
		t.Fatalf("resumed timeout: %+v %v", settled, err)
	}
}

func TestReviewParentWaitIndependentSecondWriteFullRollback(t *testing.T) {
	s, f, cp, receipt := parentWaitFixture(t)
	if _, err := s.db.Exec("CREATE TRIGGER independent_refuse_wait BEFORE INSERT ON coordinator_task_waits BEGIN SELECT RAISE(ABORT,'independent second write failure'); END"); err != nil {
		t.Fatal(err)
	}
	before := parentCoherenceSnapshot(t, s)
	if _, err := s.WaitReviewParent(context.Background(), f, cp); err == nil {
		t.Fatal("accepted second write failure")
	}
	if before != parentCoherenceSnapshot(t, s) {
		t.Fatal("second write failure mutated full records/indexes/waits")
	}
	if _, err := s.db.Exec("DROP TRIGGER independent_refuse_wait"); err != nil {
		t.Fatal(err)
	}
	got, err := s.WaitReviewParent(context.Background(), f, cp)
	if err != nil || got.Status != "parked" || got.Wait.Node.Target.TaskID != receipt.Graph.Run.Sink.ID {
		t.Fatalf("retry: %+v %v", got, err)
	}
}

func TestReviewParentWaitUnrelatedCorruptJSONFailsClosed(t *testing.T) {
	s, f, cp, _ := parentWaitFixture(t)
	insertParentRetry(t, s, f.Parent.AttemptID)
	if _, err := s.db.Exec("UPDATE coordinator_attempts SET workflow_run_id='foreign',task_id='foreign',record='{' WHERE id=?", f.Parent.AttemptID+"-retry"); err != nil {
		t.Fatal(err)
	}
	before := parentWaitSnapshot(t, s, f)
	var revision int64
	if err := s.db.QueryRow("SELECT revision FROM coordinator_attempts WHERE id=?", f.Parent.AttemptID).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WaitReviewParent(context.Background(), f, cp); err == nil {
		t.Fatal("accepted malformed unrelated JSON")
	}
	var after int64
	if err := s.db.QueryRow("SELECT revision FROM coordinator_attempts WHERE id=?", f.Parent.AttemptID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != parentWaitSnapshot(t, s, f) || revision != after {
		t.Fatal("corrupt unrelated refusal changed parent/waits/index")
	}
	var raw string
	if err := s.db.QueryRow("SELECT record FROM coordinator_attempts WHERE id=?", f.Parent.AttemptID+"-retry").Scan(&raw); err != nil || raw != "{" {
		t.Fatalf("corruption repaired: %q %v", raw, err)
	}
}

func TestReviewAuthorityHiddenRetryCallers(t *testing.T) {
	for _, caller := range []string{"freeze", "allocate", "materialize"} {
		t.Run(caller, func(t *testing.T) {
			s, f, cp, p, _ := childFixture(t)
			insertParentRetry(t, s, f.Parent.AttemptID)
			if _, err := s.db.Exec("UPDATE coordinator_attempts SET workflow_run_id='foreign' WHERE id=?", f.Parent.AttemptID+"-retry"); err != nil {
				t.Fatal(err)
			}
			before := parentCoherenceSnapshot(t, s) + childSnapshot(t, s, cp)
			var err error
			switch caller {
			case "freeze":
				_, err = s.FreezeReviewAuthority(context.Background(), f)
			case "allocate":
				_, err = s.AllocateReviewCheckpoint(context.Background(), f, cp.Checkpoint)
			case "materialize":
				_, err = s.MaterializeReviewChild(context.Background(), f, cp, p)
			}
			if !errors.Is(err, ErrReviewAuthorityIdentity) {
				t.Fatalf("wrong refusal: %v", err)
			}
			if before != parentCoherenceSnapshot(t, s)+childSnapshot(t, s, cp) {
				t.Fatal("authority caller mutated records/indexes/receipt")
			}
		})
	}
}

func TestReviewParentWaitCoherentHistoryAndPartialUnrelatedOwnership(t *testing.T) {
	for _, foreign := range []string{"run", "task"} {
		t.Run(foreign, func(t *testing.T) {
			s, f, cp, _ := parentWaitFixture(t)
			if _, err := s.db.Exec("UPDATE coordinator_attempts SET number=2,record=json_set(record,'$.number',2) WHERE id=?", f.Parent.AttemptID); err != nil {
				t.Fatal(err)
			}
			insertParentRetry(t, s, f.Parent.AttemptID)
			if _, err := s.db.Exec("UPDATE coordinator_attempts SET number=1,record=json_set(record,'$.number',1) WHERE id=?", f.Parent.AttemptID+"-retry"); err != nil {
				t.Fatal(err)
			}
			// A second canonical row shares only one parent ownership component.
			a := loadAttempt(t, s, f.Parent.AttemptID)
			a.ID += "-unrelated"
			if foreign == "run" {
				a.WorkflowRunID = "foreign"
			} else {
				a.TaskID = "foreign"
			}
			if err := s.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Attempts: []domain.Attempt{a}}); err != nil {
				t.Fatal(err)
			}
			got, err := s.WaitReviewParent(context.Background(), f, cp)
			if err != nil || got.Status != "parked" {
				t.Fatalf("valid latest/history/partial unrelated: %+v %v", got, err)
			}
		})
	}
}

func TestReviewParentWaitMissingBoundAttempt(t *testing.T) {
	s, f, cp, _ := parentWaitFixture(t)
	if _, err := s.db.Exec("DELETE FROM coordinator_attempts WHERE id=?", f.Parent.AttemptID); err != nil {
		t.Fatal(err)
	}
	before := parentCoherenceSnapshot(t, s)
	if _, err := s.WaitReviewParent(context.Background(), f, cp); err == nil {
		t.Fatal("accepted missing bound attempt")
	}
	if before != parentCoherenceSnapshot(t, s) {
		t.Fatal("missing parent refusal mutated state")
	}
}

func TestReviewParentWaitReviewerHiddenRetry(t *testing.T) {
	for _, hidden := range []bool{false, true} {
		name := "coherent-retry"
		if hidden {
			name = "index-hidden-retry"
		}
		t.Run(name, func(t *testing.T) {
			s, f, cp, _ := parentWaitFixture(t)
			q := "INSERT INTO coordinator_attempts(id,workflow_run_id,task_id,number,revision,record) SELECT id||'-retry',workflow_run_id,task_id,number+1,revision,json_set(record,'$.id',id||'-retry','$.number',number+1) FROM coordinator_attempts WHERE id=?"
			if _, err := s.db.Exec(q, f.Parent.AttemptID); err != nil {
				t.Fatal(err)
			}
			if hidden {
				if _, err := s.db.Exec("UPDATE coordinator_attempts SET workflow_run_id='foreign-index-run' WHERE id=?", f.Parent.AttemptID+"-retry"); err != nil {
					t.Fatal(err)
				}
			}
			before := parentWaitSnapshot(t, s, f)
			got, err := s.WaitReviewParent(context.Background(), f, cp)
			if err == nil {
				t.Errorf("accepted superseded parent: status=%s wait=%v", got.Status, got.Wait != nil)
			}
			if before != parentWaitSnapshot(t, s, f) {
				t.Error("changed parent/waits despite invalid ownership")
			}
		})
	}
}
