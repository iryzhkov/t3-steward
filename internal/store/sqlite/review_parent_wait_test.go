package sqlite

import (
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"reflect"
	"sync"
	"testing"
	"time"
)

func parentWaitFixture(t *testing.T) (*Store, review.FrozenAuthority, review.CheckpointAuthority, ReviewMaterialization) {
	t.Helper()
	s, f, cp, p, _ := childFixture(t)
	receipt, err := s.MaterializeReviewChild(context.Background(), f, cp, p)
	if err != nil {
		t.Fatal(err)
	}
	records, err := s.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assignment := records.Assignments[0]
	assignment.WorkerEpoch = "worker-epoch-1"
	assignment.Project = "repo"
	assignment.ExecutorDemand = &domain.ResourceDemand{}
	if err = s.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err = s.SaveWorkerSnapshot(context.Background(), domain.WorkerSnapshot{WorkerID: "worker", WorkerEpoch: "worker-epoch-1", CoordinatorEpoch: 1, Sequence: 2, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour), Inventory: domain.WorkerInventory{ID: "worker", AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now, Providers: []domain.WorkerProviderInventory{{InstanceID: "codex", Available: true, Models: []string{"sol"}}}, Projects: []domain.WorkerProjectInventory{{Name: "repo", Available: true}}}}); err != nil {
		t.Fatal(err)
	}
	return s, f, cp, receipt
}

// Use the runtime sink projection with completed child attempts.
func finishedChildRecords(receipt ReviewMaterialization, now time.Time) (CoordinatorRecords, error) {
	attempts := append([]domain.Attempt(nil), receipt.Graph.Attempts...)
	for i := range attempts {
		attempts[i].Progress = domain.ProgressSucceeded
		attempts[i].Control = domain.ControlStopped
		attempts[i].Revision++
		attempts[i].CompletedAt = &now
		attempts[i].UpdatedAt = now
	}
	run, err := domain.ProjectRunSink(receipt.Graph.Run, receipt.Graph.Tasks, attempts, nil, now)
	return CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}, Attempts: attempts}, err
}
func wakeReviewParent(t *testing.T, s *Store) ([]domain.TaskWaitWakeContext, error) {
	t.Helper()
	snapshots, err := s.LoadWorkerSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := snapshots[0]
	return s.WakeTaskWaitsBefore(context.Background(), time.Now().UTC(), TaskWakeCutoffs{AuthorizedWorkers: map[string]TaskWakeWorkerAuthorization{"worker": {WorkerEpoch: snapshot.WorkerEpoch, SnapshotSequence: snapshot.Sequence, CatalogRevision: snapshot.Inventory.CatalogRevision, ValidUntil: snapshot.ValidUntil, Providers: snapshot.Inventory.Providers, Projects: snapshot.Inventory.Projects}}})
}
func parentWaitSnapshot(t *testing.T, s *Store, f review.FrozenAuthority) string {
	t.Helper()
	var parent string
	if err := s.db.QueryRow("SELECT record FROM coordinator_attempts WHERE id=?", f.Parent.AttemptID).Scan(&parent); err != nil {
		t.Fatal(err)
	}
	waits, err := s.ListTaskWaits(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(waits)
	return parent + string(raw)
}
func TestReviewParentWaitLifecycleReopen(t *testing.T) {
	ctx := context.Background()
	s, f, cp, receipt := parentWaitFixture(t)
	if _, err := s.db.Exec("UPDATE coordinator_attempts SET revision=revision+1,record=json_set(record,'$.revision',revision+1) WHERE id=?", f.Parent.AttemptID); err != nil {
		t.Fatal(err)
	}
	first, err := s.WaitReviewParent(ctx, f, cp)
	if err != nil {
		t.Fatal(err)
	}
	parent := loadAttempt(t, s, f.Parent.AttemptID)
	if first.Status != "parked" || first.Wait == nil || first.Wait.IssuedRevision != f.Parent.IssuedRevision ||
		!first.Wait.Deadline.Equal(*receipt.Graph.Tasks[0].Deadline) || first.Wait.Wake != domain.WakeEach ||
		parent.Progress != domain.ProgressWaitingExternal || parent.Control.HoldsProviderSlot() {
		t.Fatalf("bad park: %+v %+v", first, parent)
	}
	before := parentWaitSnapshot(t, s, f)
	other, err := Open(s.path)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := other.WaitReviewParent(ctx, f, cp)
	if err != nil || !reflect.DeepEqual(first, replay) || before != parentWaitSnapshot(t, s, f) {
		t.Fatalf("live replay: %+v %v", replay, err)
	}
	other.Close()
	records, err := finishedChildRecords(receipt, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	path := s.path
	s.Close()
	s, err = OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.SettleNodeWaits(ctx, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	settled, err := s.WaitReviewParent(ctx, f, cp)
	if err != nil || settled.Status != "settled" || !settled.CollectionPending || settled.Wait.Result == nil {
		t.Fatalf("settled replay: %+v %v", settled, err)
	}
	wakes, err := wakeReviewParent(t, s)
	if err != nil || len(wakes) != 1 {
		t.Fatalf("wake: %+v %v", wakes, err)
	}
	resumed := loadAttempt(t, s, f.Parent.AttemptID)
	if resumed.ID != parent.ID || resumed.ThreadID != parent.ThreadID || resumed.AssignmentID != parent.AssignmentID ||
		resumed.Progress != domain.ProgressActive {
		t.Fatalf("wrong resumption: %+v", resumed)
	}
	before = parentWaitSnapshot(t, s, f)
	s.now = func() time.Time { return first.Wait.Deadline.Add(time.Second) }
	replay, err = s.WaitReviewParent(ctx, f, cp)
	if err != nil || replay.Status != "settled" || before != parentWaitSnapshot(t, s, f) ||
		!replay.Wait.Deadline.Equal(first.Wait.Deadline) {
		t.Fatalf("resumed replay reparked: %+v %v", replay, err)
	}
	if wakes, err = wakeReviewParent(t, s); err != nil || len(wakes) != 0 {
		t.Fatalf("duplicate wake: %v %v", wakes, err)
	}
}
func TestReviewParentWaitEarlyFinished(t *testing.T) {
	for _, collection := range []bool{false, true} {
		t.Run(map[bool]string{false: "sink before collector", true: "collected before park"}[collection], func(t *testing.T) {
			ctx := context.Background()
			s, f, cp, receipt := parentWaitFixture(t)
			if collection {
				round, err := s.GetReviewRound(ctx, cp.RoundID)
				if err != nil {
					t.Fatal(err)
				}
				for _, m := range round.Reviewers {
					round, err = s.RecordReviewResult(ctx, round.ID, m.ID, round.Revision, review.Result{State: "failed", Failure: "review failed"})
					if err != nil {
						t.Fatal(err)
					}
				}
			} else {
				records, err := finishedChildRecords(receipt, time.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}
				if err = s.SaveCoordinatorRecords(ctx, records); err != nil {
					t.Fatal(err)
				}
			}
			before := parentWaitSnapshot(t, s, f)
			got, err := s.WaitReviewParent(ctx, f, cp)
			if err != nil || got.Status != "finished" || got.Wait != nil || got.RoundID != cp.RoundID ||
				got.CollectionPending == collection || before != parentWaitSnapshot(t, s, f) {
				t.Fatalf("early finish: %+v %v", got, err)
			}
		})
	}
}
func TestReviewParentWaitCompletionRace(t *testing.T) {
	for i := 0; i < 8; i++ {
		s, f, cp, receipt := parentWaitFixture(t)
		other, err := Open(s.path)
		if err != nil {
			t.Fatal(err)
		}
		records, err := finishedChildRecords(receipt, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var result ReviewParentWaitResult
		var parkErr, completeErr error
		go func() { defer wg.Done(); <-start; result, parkErr = s.WaitReviewParent(context.Background(), f, cp) }()
		go func() {
			defer wg.Done()
			<-start
			completeErr = other.SaveCoordinatorRecords(context.Background(), records)
		}()
		close(start)
		wg.Wait()
		other.Close()
		if parkErr != nil || completeErr != nil {
			t.Fatalf("race errors: %v %v", parkErr, completeErr)
		}
		if result.Status == "parked" {
			if err = s.SettleNodeWaits(context.Background(), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			wakes, err := wakeReviewParent(t, s)
			if err != nil || len(wakes) != 1 {
				t.Fatalf("lost wake: %+v %v", wakes, err)
			}
		} else if result.Status != "finished" {
			t.Fatalf("race result: %+v", result)
		}
		if parent := loadAttempt(t, s, f.Parent.AttemptID); parent.Progress != domain.ProgressActive {
			t.Fatalf("stranded parent: %+v", parent)
		}
		s.Close()
	}
}
func TestReviewParentWaitRefusalsRollback(t *testing.T) {
	cases := []struct{ name, query string }{
		{"foreign run", "UPDATE coordinator_attempts SET record=json_set(record,'$.workflowRunId','foreign') WHERE id=?"},
		{"assignment thread", "UPDATE coordinator_assignments SET record=json_set(record,'$.threadId','foreign') WHERE attempt_id=?"},
		{"assignment owner", "UPDATE coordinator_assignments SET record=json_set(record,'$.attemptId','foreign') WHERE attempt_id=?"},
		{"assignment route", "UPDATE coordinator_assignments SET record=json_set(record,'$.route.model','foreign') WHERE attempt_id=?"},
		{"run ended", "UPDATE coordinator_workflow_runs SET record=json_set(record,'$.progress','succeeded') WHERE id=(SELECT workflow_run_id FROM coordinator_attempts WHERE id=?)"},
		{"thread", "UPDATE coordinator_attempts SET record=json_set(record,'$.threadId','foreign') WHERE id=?"},
		{"assignment", "UPDATE coordinator_attempts SET record=json_set(record,'$.assignmentId','foreign') WHERE id=?"},
		{"ended", "UPDATE coordinator_attempts SET record=json_set(record,'$.progress','succeeded','$.control','stopped') WHERE id=?"},
		{"epoch", "UPDATE coordinator_assignments SET record=json_set(record,'$.epoch',2) WHERE attempt_id=?"},
		{"reassigned", "UPDATE coordinator_assignments SET record=json_set(record,'$.state','released') WHERE attempt_id=?"},
		{"project", "UPDATE coordinator_workflows SET record=json_set(record,'$.project','foreign') WHERE id=(SELECT workflow_id FROM coordinator_workflow_runs WHERE id=(SELECT workflow_run_id FROM coordinator_attempts WHERE id=?))"},
		{"retry", "INSERT INTO coordinator_attempts(id,workflow_run_id,task_id,number,revision,record) SELECT id||'-retry',workflow_run_id,task_id,number+1,revision,json_set(record,'$.id',id||'-retry','$.number',number+1) FROM coordinator_attempts WHERE id=?"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, f, cp, _ := parentWaitFixture(t)
			if _, err := s.db.Exec(tc.query, f.Parent.AttemptID); err != nil {
				t.Fatal(err)
			}
			before := parentWaitSnapshot(t, s, f)
			if _, err := s.WaitReviewParent(context.Background(), f, cp); err == nil {
				t.Fatal("accepted changed ownership")
			}
			if before != parentWaitSnapshot(t, s, f) {
				t.Fatal("refusal mutated parent/waits")
			}
		})
	}
	for _, kind := range []string{"expired", "graph", "checkpoint", "authority", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			s, f, cp, receipt := parentWaitFixture(t)
			switch kind {
			case "expired":
				s.now = func() time.Time { return receipt.Graph.Tasks[0].Deadline.Add(time.Second) }
			case "graph":
				if _, err := s.db.Exec("UPDATE coordinator_tasks SET record=json_set(record,'$.name','foreign') WHERE id=?", receipt.Graph.Tasks[0].ID); err != nil {
					t.Fatal(err)
				}
			case "checkpoint":
				cp.Number++
			case "authority":
				f.Parent.ThreadID = "foreign"
			case "deadline":
				if _, err := s.db.Exec("UPDATE coordinator_review_rounds SET record=json_set(record,'$.deadline',?) WHERE id=?", time.Now().Add(2*time.Hour).UTC().Format(time.RFC3339Nano), cp.RoundID); err != nil {
					t.Fatal(err)
				}
			}
			before := parentWaitSnapshot(t, s, f)
			if _, err := s.WaitReviewParent(context.Background(), f, cp); err == nil {
				t.Fatal("accepted corrupt/expired material")
			}
			if before != parentWaitSnapshot(t, s, f) {
				t.Fatal("refusal mutated parent/waits")
			}
		})
	}
}
func TestReviewParentWaitReplayRefusals(t *testing.T) {
	for _, kind := range []string{"node", "deadline", "request", "not parked", "ownership", "indexed owner", "revision", "expired"} {
		t.Run(kind, func(t *testing.T) {
			s, f, cp, _ := parentWaitFixture(t)
			got, err := s.WaitReviewParent(context.Background(), f, cp)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "indexed owner":
				_, err = s.db.Exec("UPDATE coordinator_task_waits SET attempt_id='foreign' WHERE id=?", got.Wait.ID)
			case "revision":
				_, err = s.db.Exec("UPDATE coordinator_task_waits SET record=json_set(record,'$.registeredRevision',999) WHERE id=?", got.Wait.ID)
			case "expired":
				s.now = func() time.Time { return got.Wait.Deadline.Add(time.Second) }
			case "node":
				_, err = s.db.Exec("UPDATE coordinator_task_waits SET record=json_set(record,'$.node.target.runId','foreign') WHERE id=?", got.Wait.ID)
			case "deadline":
				_, err = s.db.Exec("UPDATE coordinator_task_waits SET record=json_set(record,'$.maxDuration',1) WHERE id=?", got.Wait.ID)
			case "request":
				_, err = s.db.Exec("UPDATE coordinator_task_waits SET request_id='foreign',record=json_set(record,'$.requestId','foreign') WHERE id=?", got.Wait.ID)
			case "not parked":
				_, err = s.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.progress','active','$.control','running') WHERE id=?", f.Parent.AttemptID)
			case "ownership":
				_, err = s.db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.epoch',2) WHERE attempt_id=?", f.Parent.AttemptID)
			}
			if err != nil {
				t.Fatal(err)
			}
			before := parentWaitSnapshot(t, s, f)
			if _, err = s.WaitReviewParent(context.Background(), f, cp); err == nil {
				t.Fatal("accepted corrupt replay")
			}
			if before != parentWaitSnapshot(t, s, f) {
				t.Fatal("replay refusal mutated parent/waits")
			}
		})
	}
}

// Force the second persistence step to fail: the parent CAS must roll back too.
func TestReviewParentWaitAtomicPersistenceFailure(t *testing.T) {
	s, f, cp, _ := parentWaitFixture(t)
	if _, err := s.db.Exec("CREATE TRIGGER refuse_parent_wait BEFORE INSERT ON coordinator_task_waits BEGIN SELECT RAISE(ABORT,'fixture wait failure'); END;"); err != nil {
		t.Fatal(err)
	}
	before := parentWaitSnapshot(t, s, f)
	if _, err := s.WaitReviewParent(context.Background(), f, cp); err == nil {
		t.Fatal("expected insert failure")
	}
	if before != parentWaitSnapshot(t, s, f) {
		t.Fatal("failed insert left parent parked")
	}
}
func TestReviewParentWaitConcurrentRegistration(t *testing.T) {
	s, f, cp, _ := parentWaitFixture(t)
	other, err := Open(s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	var results [2]ReviewParentWaitResult
	var errs [2]error
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	for i, store := range []*Store{s, other} {
		go func(i int, store *Store) {
			defer wg.Done()
			<-start
			results[i], errs[i] = store.WaitReviewParent(context.Background(), f, cp)
		}(i, store)
	}
	close(start)
	wg.Wait()
	if errs[0] != nil || errs[1] != nil || !reflect.DeepEqual(results[0], results[1]) {
		t.Fatalf("registration race: %+v %v", results, errs)
	}
	waits, err := s.ListTaskWaits(context.Background())
	if err != nil || len(waits) != 1 {
		t.Fatalf("duplicate wait: %+v %v", waits, err)
	}
}
