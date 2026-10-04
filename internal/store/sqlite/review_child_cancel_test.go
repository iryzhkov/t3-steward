package sqlite

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
)

func cancellationRecords(t *testing.T, s *Store) CoordinatorRecords {
	t.Helper()
	r, err := s.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func cancellationSave(t *testing.T, s *Store, r CoordinatorRecords) {
	t.Helper()
	if err := s.SaveCoordinatorRecords(context.Background(), r); err != nil {
		t.Fatal(err)
	}
}
func cancellationEndParent(t *testing.T, s *Store, f review.FrozenAuthority, progress domain.ProgressState) {
	t.Helper()
	for _, a := range cancellationRecords(t, s).Attempts {
		if a.ID == f.Parent.AttemptID {
			a.Progress = progress
			a.Control = domain.ControlStopped
			a.Revision++
			cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{a}})
			return
		}
	}
	t.Fatal("parent absent")
}
func cancellationSnapshot(t *testing.T, s *Store) string {
	t.Helper()
	var out []string
	for _, table := range []string{"coordinator_attempts", "coordinator_assignments", "coordinator_workflow_runs", "coordinator_tasks", "coordinator_artifacts", "coordinator_review_rounds", "coordinator_review_authorities", "coordinator_review_checkpoints", "coordinator_review_materializations", "coordinator_audit_events", "coordinator_task_waits", "coordinator_task_wait_events"} {
		rows, err := s.db.Query("SELECT * FROM " + table + " ORDER BY 1")
		if err != nil {
			t.Fatal(err)
		}
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err = rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(vals)
			out = append(out, table+string(raw))
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}
	raw, _ := json.Marshal(out)
	return string(raw)
}
func cancellationAssign(t *testing.T, s *Store, a domain.Attempt, state domain.AssignmentState, route string) domain.Assignment {
	t.Helper()
	a.AssignmentID = "assignment-" + a.ID
	thread := "thread-" + a.ID
	assignment := domain.Assignment{ID: a.AssignmentID, AttemptID: a.ID, WorkerID: "child-worker", WorkerEpoch: "child-epoch", Epoch: 1, State: state, DispatchToken: "token-" + a.ID, LeaseToken: "lease-" + a.ID, ThreadID: thread, Project: "repo", ExecutionRole: domain.ExecutionRoleExecutor, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	instance, model := splitCancellationRoute(route)
	assignment.Route = domain.ProviderRoute{ProviderInstanceID: instance, Model: model}
	if state != domain.AssignmentOffered {
		a.Progress = domain.ProgressActive
		a.Control = domain.ControlRunning
		a.ThreadID = thread
	}
	a.Revision++
	cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{a}, Assignments: []domain.Assignment{assignment}})
	return assignment
}
func splitCancellationRoute(s string) (string, string) {
	for i, c := range s {
		if c == '/' {
			return s[:i], s[i+1:]
		}
	}
	return "", s
}

func TestReviewChildCancellationAcceptedRoundStillStopsWork(t *testing.T) {
	ctx := context.Background()
	s, f, cp, receipt := parentWaitFixture(t)
	round, err := s.GetReviewRound(ctx, cp.RoundID)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range round.Reviewers {
		verdict := []byte(`{"schema":"review-verdict/v1","verdict":"accept","findings":[],"inputManifestDigest":"` + cp.Checkpoint.InputDigest + `","reviewerRoute":"` + m.Route + `"}`)
		round, err = s.RecordReviewResult(ctx, cp.RoundID, m.ID, round.Revision, review.Result{State: "succeeded", ReviewMD: "retained", VerdictJSON: verdict})
		if err != nil {
			t.Fatal(err)
		}
	}
	if round.Combined != "accept" {
		t.Fatal("fixture not accepted")
	}
	cancellationEndParent(t, s, f, domain.ProgressSucceeded)
	got, err := s.ReconcileReviewChildCancellation(ctx, f, cp)
	if err != nil || len(got.CancelledAttempts) != len(receipt.Graph.Attempts) {
		t.Fatalf("accepted round stranded work %+v %v", got, err)
	}
	after, err := s.GetReviewRound(ctx, cp.RoundID)
	if err != nil || !reflect.DeepEqual(round, after) {
		t.Fatal("accepted results changed")
	}
}

func TestReviewChildCancellationDisposition(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"healthy", "succeeded", "failed", "cancelled", "terminal-run", "superseded", "released", "completed", "unknown", "reassigned", "deadline-before", "deadline-exact"} {
		t.Run(kind, func(t *testing.T) {
			s, f, cp, receipt := parentWaitFixture(t)
			before := cancellationSnapshot(t, s)
			switch kind {
			case "succeeded":
				cancellationEndParent(t, s, f, domain.ProgressSucceeded)
			case "failed":
				cancellationEndParent(t, s, f, domain.ProgressFailed)
			case "cancelled":
				cancellationEndParent(t, s, f, domain.ProgressCancelled)
			case "terminal-run":
				for _, r := range cancellationRecords(t, s).WorkflowRuns {
					if r.ID == f.Parent.RunID {
						r.Progress = domain.ProgressSucceeded
						r.Revision++
						cancellationSave(t, s, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{r}})
					}
				}
			case "superseded":
				for _, a := range cancellationRecords(t, s).Attempts {
					if a.ID == f.Parent.AttemptID {
						a.ID = "parent-retry"
						a.Number++
						a.Revision = 1
						a.AssignmentID = ""
						a.ThreadID = ""
						a.Progress = domain.ProgressReady
						a.Control = domain.ControlUnassigned
						cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{a}})
					}
				}
			case "released", "completed", "unknown", "reassigned":
				for _, a := range cancellationRecords(t, s).Assignments {
					if a.ID == f.Parent.AssignmentID {
						switch kind {
						case "released":
							a.State = domain.AssignmentReleased
						case "completed":
							a.State = domain.AssignmentCompleted
						case "unknown":
							a.State = domain.AssignmentUnknown
						case "reassigned":
							a.Epoch++
							a.ThreadID = "replacement"
							a.Route.Model = "replacement"
						}
						cancellationSave(t, s, CoordinatorRecords{Assignments: []domain.Assignment{a}})
					}
				}
			case "deadline-before":
				s.now = func() time.Time { return receipt.Graph.Tasks[0].Deadline.Add(-time.Nanosecond) }
			case "deadline-exact":
				s.now = func() time.Time { return *receipt.Graph.Tasks[0].Deadline }
			}
			parentBefore := parentWaitSnapshot(t, s, f)
			got, err := s.ReconcileReviewChildCancellation(ctx, f, cp)
			if err != nil {
				t.Fatal(err)
			}
			noAction := kind == "healthy" || kind == "deadline-before"
			if noAction {
				if got.Status != "no-action" || cancellationSnapshot(t, s) != before {
					t.Fatalf("no-op changed: %+v", got)
				}
				return
			}
			if got.Status != "quiescent" || len(got.CancelledAttempts) != len(receipt.Graph.Attempts) || got.WorkerStopPending {
				t.Fatalf("cancel: %+v", got)
			}
			if parentBefore != parentWaitSnapshot(t, s, f) {
				t.Fatal("parent changed")
			}
			round, err := s.GetReviewRound(ctx, cp.RoundID)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range round.Reviewers {
				want := "failed"
				if kind == "deadline-exact" {
					want = "timed-out"
				}
				if m.State != want {
					t.Fatalf("result %+v", m)
				}
			}
			for _, a := range cancellationRecords(t, s).Attempts {
				if a.WorkflowRunID == cp.RoundID && (a.Progress != domain.ProgressCancelled || a.Control != domain.ControlStopped) {
					t.Fatalf("attempt %+v", a)
				}
			}
			snapshot := cancellationSnapshot(t, s)
			again, err := s.ReconcileReviewChildCancellation(ctx, f, cp)
			if err != nil || again.Status != "quiescent" || len(again.CancelledAttempts) != 0 || snapshot != cancellationSnapshot(t, s) {
				t.Fatalf("replay %+v %v", again, err)
			}
		})
	}
}

func TestReviewChildCancellationCustodyRollbackReopen(t *testing.T) {
	ctx := context.Background()
	for _, state := range []domain.AssignmentState{domain.AssignmentOffered, domain.AssignmentClaimed, domain.AssignmentUnknown} {
		t.Run(string(state), func(t *testing.T) {
			s, f, cp, receipt := parentWaitFixture(t)
			a := receipt.Graph.Attempts[0]
			assignment := cancellationAssign(t, s, a, state, f.Requirements.Members[0].Route)
			if state == domain.AssignmentClaimed {
				records := cancellationRecords(t, s)
				for _, a := range records.Attempts {
					if a.ID == assignment.AttemptID {
						a.Progress = domain.ProgressWaitingExternal
						a.Control = domain.ControlWaitingExternal
						a.Revision++
						cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{a}})
					}
				}
			}
			cancellationEndParent(t, s, f, domain.ProgressFailed)
			before := cancellationSnapshot(t, s)
			if _, err := s.db.Exec("CREATE TRIGGER cancellation_token_order BEFORE UPDATE ON coordinator_attempts WHEN json_extract(NEW.record,'$.progress')='cancelled' AND EXISTS(SELECT 1 FROM coordinator_assignments WHERE attempt_id=NEW.id AND COALESCE(json_extract(record,'$.dispatchToken'),'')!='') BEGIN SELECT RAISE(ABORT,'token still live'); END"); err != nil {
				t.Fatal(err)
			}
			// Fail after token/attempt/audit writes, at member result persistence.
			if _, err := s.db.Exec("CREATE TRIGGER cancellation_fail BEFORE UPDATE ON coordinator_review_rounds BEGIN SELECT RAISE(ABORT,'injected result failure'); END"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ReconcileReviewChildCancellation(ctx, f, cp); err == nil {
				t.Fatal("injected failure committed")
			}
			if before != cancellationSnapshot(t, s) {
				t.Fatal("rollback leaked token/state/result/audit")
			}
			if _, err := s.db.Exec("DROP TRIGGER cancellation_fail"); err != nil {
				t.Fatal(err)
			}
			got, err := s.ReconcileReviewChildCancellation(ctx, f, cp)
			if err != nil {
				t.Fatal(err)
			}
			want := "stop-requested"
			if state == domain.AssignmentOffered {
				want = "quiescent"
			}
			if got.Status != want || got.WorkerStopPending != (state != domain.AssignmentOffered) {
				t.Fatalf("custody %+v", got)
			}
			for _, next := range cancellationRecords(t, s).Assignments {
				if next.ID == assignment.ID {
					if next.DispatchToken != "" || next.Epoch != assignment.Epoch {
						t.Fatal("authority retained or epoch changed")
					}
					if state == domain.AssignmentOffered {
						if next.State != domain.AssignmentReleased {
							t.Fatal("offer retained")
						}
					} else if next.State != state {
						t.Fatal("active ownership released")
					}
				}
			}
			snapshot := cancellationSnapshot(t, s)
			path := s.path
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			got, err = s.ReconcileReviewChildCancellation(ctx, f, cp)
			if err != nil || got.Status != want || len(got.CancelledAttempts) != 0 || snapshot != cancellationSnapshot(t, s) {
				t.Fatalf("reopen %+v %v", got, err)
			}
			// A legitimate later retry is stopped too, without rewriting member results.
			retry := receipt.Graph.Attempts[0]
			retry.ID = "child-retry"
			retry.Number = 2
			retry.Revision = 1
			cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{retry}})
			got, err = s.ReconcileReviewChildCancellation(ctx, f, cp)
			if err != nil || !reflect.DeepEqual(got.CancelledAttempts, []string{retry.ID}) {
				t.Fatalf("retry %+v %v", got, err)
			}
		})
	}
}

func TestReviewChildCancellationRefusalUnchanged(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"missing-parent", "hidden-parent-retry", "hidden-child-retry", "assignment-index", "assignment-runtime", "assignment-epoch", "assignment-case", "child-case", "foreign-authority", "deadline-mutated", "lease-index", "route-mismatch", "round-index", "parent-thread-case", "invalid-progress"} {
		t.Run(kind, func(t *testing.T) {
			s, f, cp, receipt := parentWaitFixture(t)
			assignment := cancellationAssign(t, s, receipt.Graph.Attempts[0], domain.AssignmentClaimed, f.Requirements.Members[0].Route)
			cancellationEndParent(t, s, f, domain.ProgressFailed)
			var err error
			switch kind {
			case "missing-parent":
				_, err = s.db.Exec("DELETE FROM coordinator_attempts WHERE id=?", f.Parent.AttemptID)
			case "hidden-parent-retry", "hidden-child-retry":
				a := receipt.Graph.Attempts[0]
				if kind == "hidden-parent-retry" {
					for _, p := range cancellationRecords(t, s).Attempts {
						if p.ID == f.Parent.AttemptID {
							a = p
						}
					}
				}
				a.ID = "hidden-retry"
				a.Number = 2
				a.Revision = 1
				a.AssignmentID = ""
				a.ThreadID = ""
				cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{a}})
				_, err = s.db.Exec("UPDATE coordinator_attempts SET workflow_run_id='foreign',task_id='foreign' WHERE id=?", a.ID)
			case "assignment-index":
				_, err = s.db.Exec("UPDATE coordinator_assignments SET attempt_id='foreign' WHERE id=?", assignment.ID)
			case "assignment-runtime":
				_, err = s.db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.attemptId','foreign') WHERE id=?", assignment.ID)
			case "assignment-epoch":
				_, err = s.db.Exec("UPDATE coordinator_assignments SET assignment_epoch=2 WHERE id=?", assignment.ID)
			case "assignment-case":
				_, err = s.db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.AttemptId',attempt_id) WHERE id=?", assignment.ID)
			case "child-case":
				_, err = s.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.TaskId',task_id) WHERE id=?", receipt.Graph.Attempts[0].ID)
			case "lease-index":
				_, err = s.db.Exec("UPDATE coordinator_assignments SET lease_expires_at='invalid' WHERE id=?", assignment.ID)
			case "route-mismatch":
				_, err = s.db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.route.model','foreign') WHERE id=?", assignment.ID)
			case "round-index":
				_, err = s.db.Exec("UPDATE coordinator_review_rounds SET revision=revision+1 WHERE id=?", cp.RoundID)
			case "parent-thread-case":
				_, err = s.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.ThreadId','hidden') WHERE id=?", f.Parent.AttemptID)
			case "invalid-progress":
				_, err = s.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.progress','foreign') WHERE id=?", receipt.Graph.Attempts[0].ID)
			case "foreign-authority":
				cp.AuthorityKey = "foreign"
			case "deadline-mutated":
				_, err = s.db.Exec("UPDATE coordinator_review_rounds SET record=json_set(record,'$.deadline',?) WHERE id=?", time.Now().Add(2*time.Hour).UTC().Format(time.RFC3339Nano), cp.RoundID)
			}
			if err != nil {
				t.Fatal(err)
			}
			before := cancellationSnapshot(t, s)
			if _, err = s.ReconcileReviewChildCancellation(ctx, f, cp); err == nil {
				t.Fatal("corruption accepted")
			}
			if before != cancellationSnapshot(t, s) {
				t.Fatal("refusal changed records or indexes")
			}
		})
	}
}

func TestReviewChildCancellationConcurrentAndTerminalPreservation(t *testing.T) {
	ctx := context.Background()
	s, f, cp, receipt := parentWaitFixture(t)
	complete := receipt.Graph.Attempts[0]
	complete.Progress = domain.ProgressSucceeded
	complete.Control = domain.ControlStopped
	complete.Revision++
	output := domain.Artifact{ID: "completed-output", WorkflowRunID: cp.RoundID, TaskID: complete.TaskID, AttemptID: complete.ID, Kind: domain.ArtifactOutput, Name: "review.md", SHA256: "retained"}
	cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{complete}, Artifacts: []domain.Artifact{output}})
	// Stranded older execution cancelled, while a completed latest execution
	// stays pending for the collector.
	older := receipt.Graph.Attempts[1]
	latest := older
	latest.ID = "completed-retry"
	latest.Number = 2
	latest.Revision = 1
	latest.Progress = domain.ProgressFailed
	latest.Control = domain.ControlStopped
	cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{latest}})
	cancellationEndParent(t, s, f, domain.ProgressSucceeded)
	other, err := Open(s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, store := range []*Store{s, other} {
		wg.Add(1)
		go func(s *Store) { defer wg.Done(); _, err := s.ReconcileReviewChildCancellation(ctx, f, cp); errs <- err }(store)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	r := cancellationRecords(t, s)
	for _, a := range r.Attempts {
		if a.ID == complete.ID && !reflect.DeepEqual(a, complete) {
			t.Fatal("completed attempt rewritten")
		}
	}
	found := false
	for _, a := range r.Artifacts {
		if a.ID == output.ID {
			found = reflect.DeepEqual(a, output)
		}
	}
	if !found {
		t.Fatal("completed output lost")
	}
	round, err := s.GetReviewRound(ctx, cp.RoundID)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range round.Reviewers {
		if m.State != "pending" {
			t.Fatal("terminal latest execution stolen from collector")
		}
	}
	var count int
	if err = s.db.QueryRow("SELECT count(*) FROM coordinator_audit_events WHERE id LIKE 'review-child-cancelled:%'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("audits %d %v", count, err)
	}
}
