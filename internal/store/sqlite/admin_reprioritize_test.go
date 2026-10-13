package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func reprioritizeFixture(t *testing.T, states ...domain.ProgressState) (*Store, string, CoordinatorRecords) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	s := openAdminCommandStore(t, path)
	s.SetClock(func() time.Time { return adminCommandTestTime.Add(time.Minute) })
	r := CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "wf", Version: 2, Name: "workflow"}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run", WorkflowID: "wf", Progress: domain.ProgressActive, Revision: 3, CreatedAt: adminCommandTestTime, UpdatedAt: adminCommandTestTime}},
	}
	for i, state := range states {
		id := fmt.Sprintf("task-%02d", i)
		reprioritizeRecord(t, r.Workflows, "wf").TaskIDs = append(reprioritizeRecord(t, r.Workflows, "wf").TaskIDs, id)
		r.Tasks = append(r.Tasks, domain.Task{ID: id, WorkflowID: "wf", Name: "name-" + id, Importance: 2, ResourceLocks: []string{"kept"}})
		r.Attempts = append(r.Attempts, domain.Attempt{ID: "attempt-" + id, WorkflowRunID: "run", TaskID: id, Number: 1, Progress: state, Control: domain.ControlUnassigned, Revision: 7, UpdatedAt: adminCommandTestTime})
	}
	reprioritizeSave(t, s, r)
	return s, path, r
}
func reprioritizeSave(t *testing.T, s *Store, r CoordinatorRecords) {
	t.Helper()
	if err := s.SaveCoordinatorRecords(context.Background(), r); err != nil {
		t.Fatal(err)
	}
}
func reprioritizeLoad(t *testing.T, s *Store) CoordinatorRecords {
	t.Helper()
	r, err := s.LoadCoordinatorRecordsWithAudit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// Index complete records by durable identity, independent of loader order.
func reprioritizeByID[T any](t *testing.T, records []T) map[string]*T {
	t.Helper()
	if records == nil {
		return nil
	}
	byID := make(map[string]*T, len(records))
	for i := range records {
		id := reflect.ValueOf(records[i]).FieldByName("ID").String()
		if id == "" || byID[id] != nil {
			t.Fatalf("missing or duplicate record ID: %q", id)
		}
		byID[id] = &records[i]
	}
	return byID
}
func reprioritizeRecord[T any](t *testing.T, records []T, id string) *T {
	t.Helper()
	record := reprioritizeByID(t, records)[id]
	if record == nil {
		t.Fatalf("missing record %q", id)
	}
	return record
}
func reprioritizeSameRecords(t *testing.T, before, after CoordinatorRecords) bool {
	t.Helper()
	same := reflect.DeepEqual(reprioritizeByID(t, before.Attempts), reprioritizeByID(t, after.Attempts)) &&
		reflect.DeepEqual(reprioritizeByID(t, before.WorkflowRuns), reprioritizeByID(t, after.WorkflowRuns)) &&
		reflect.DeepEqual(reprioritizeByID(t, before.Tasks), reprioritizeByID(t, after.Tasks))
	before.Attempts, after.Attempts = nil, nil
	before.WorkflowRuns, after.WorkflowRuns = nil, nil
	before.Tasks, after.Tasks = nil, nil
	return same && reflect.DeepEqual(before, after)
}

func reprioritizeSubmit(t *testing.T, s *Store, id string, revision int64, importance int) domain.AdminCommandApplication {
	t.Helper()
	c := domain.AdminCommand{ID: id, Kind: domain.AdminCommandReprioritize, TargetType: domain.AdminTargetWorkflowRun, TargetID: "run", ExpectedRevision: revision, RequestedBy: "operator", Reason: "priority only", State: domain.AdminCommandPending, CreatedAt: adminCommandTestTime, Payload: json.RawMessage(fmt.Sprintf("{\"importance\":%d}", importance))}
	d, err := s.SubmitAdminCommand(context.Background(), c)
	if err != nil || d.Command.State != domain.AdminCommandPending {
		t.Fatalf("submit: %#v %v", d, err)
	}
	if d.Event.Actor != c.RequestedBy || d.Event.Reason != c.Reason {
		t.Fatal("submission identity lost")
	}
	return domain.AdminCommandApplication{CommandID: id, ExpectedCommandState: domain.AdminCommandPending, ExpectedTargetRevision: revision, State: domain.AdminCommandApplied, AppliedAt: adminCommandTestTime.Add(time.Minute)}
}
func reprioritizeApply(t *testing.T, s *Store, a domain.AdminCommandApplication) domain.AdminCommandDecision {
	t.Helper()
	d, err := s.ApplyAdminCommand(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

type reprioritizeTestRecord struct {
	TaskID   string `json:"taskId"`
	TaskName string `json:"taskName"`
	Reason   string `json:"reason,omitempty"`
}
type reprioritizeTestReceipt struct {
	Importance int                      `json:"importance"`
	Changed    []reprioritizeTestRecord `json:"changed"`
	Skipped    []reprioritizeTestRecord `json:"skipped"`
}

func reprioritizeReceipt(t *testing.T, d domain.AdminCommandDecision) reprioritizeTestReceipt {
	t.Helper()
	var detail struct {
		Receipt *reprioritizeTestReceipt `json:"reprioritize,omitempty"`
	}
	if err := json.Unmarshal(d.Event.Detail, &detail); err != nil {
		t.Fatal(err)
	}
	if detail.Receipt == nil {
		t.Fatalf("missing receipt: %s", d.Event.Detail)
	}
	return *detail.Receipt
}
func TestRunReprioritizeHistoryAndApplyRace(t *testing.T) {
	s, _, r := reprioritizeFixture(t, domain.ProgressReady, domain.ProgressReady, domain.ProgressReady, domain.ProgressReady, domain.ProgressReady)
	a := reprioritizeSubmit(t, s, "history", 3, 5)
	old := *reprioritizeRecord(t, r.Attempts, "attempt-task-00")
	old.ID = "old"
	old.Number = 0
	old.StartedAt = &adminCommandTestTime
	reprioritizeRecord(t, r.Attempts, "attempt-task-00").Number = 2
	r.Attempts = append(r.Attempts, old)
	reprioritizeRecord(t, r.Attempts, "attempt-task-01").Number = 2
	old = *reprioritizeRecord(t, r.Attempts, "attempt-task-02")
	old.ID = "thread-history"
	old.Number = 0
	old.ThreadID = "cleared-current-thread"
	r.Attempts = append(r.Attempts, old)
	r.Assignments = []domain.Assignment{{ID: "historical-row", AttemptID: reprioritizeRecord(t, r.Attempts, "attempt-task-03").ID, State: domain.AssignmentReleased}}
	// Writer is ordered after intent/submission, before application, with a barrier.
	committed := make(chan struct{})
	go func() {
		defer close(committed)
		if err := s.SaveCoordinatorRecords(context.Background(), r); err != nil {
			t.Error(err)
		}
	}()
	<-committed
	newest := *reprioritizeRecord(t, r.Attempts, "attempt-task-04")
	newest.Number = 2
	newest.ID = "new-current"
	newest.Control = domain.ControlPreparing
	newest.AssignmentID = "fresh-offer"
	newest.Revision = 9
	reprioritizeSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{newest}})
	before := reprioritizeLoad(t, s)
	d := reprioritizeApply(t, s, a)
	receipt := reprioritizeReceipt(t, d)
	if len(receipt.Changed) != 0 || len(receipt.Skipped) != 5 {
		t.Fatalf("receipt: %#v", receipt)
	}
	for i, reason := range []string{"history", "retry", "history", "history", "history"} {
		if receipt.Skipped[i].Reason != reason {
			t.Fatalf("reason: %#v", receipt)
		}
	}
	if !reflect.DeepEqual(reprioritizeByID(t, before.Attempts), reprioritizeByID(t, reprioritizeLoad(t, s).Attempts)) {
		t.Fatal("fresh history overwritten")
	}
}

func TestRunReprioritizeApplicationFencesReplaysAndPersists(t *testing.T) {
	for _, fence := range []string{"current", "submission"} {
		t.Run(fence, func(t *testing.T) {
			s, _, r := reprioritizeFixture(t, domain.ProgressReady)
			a := reprioritizeSubmit(t, s, "fence", 3, 5)
			if fence == "current" {
				reprioritizeRecord(t, r.WorkflowRuns, "run").Revision++
				reprioritizeSave(t, s, r)
			} else {
				a.ExpectedTargetRevision++
			}
			before := reprioritizeLoad(t, s)
			d := reprioritizeApply(t, s, a)
			after := reprioritizeLoad(t, s)
			if d.Command.State != domain.AdminCommandRejected || d.CurrentTarget == nil || !reflect.DeepEqual(reprioritizeByID(t, before.Attempts), reprioritizeByID(t, after.Attempts)) || !reflect.DeepEqual(reprioritizeByID(t, before.WorkflowRuns), reprioritizeByID(t, after.WorkflowRuns)) {
				t.Fatal("stale fence writes")
			}
		})
	}
	s, path, _ := reprioritizeFixture(t, domain.ProgressReady)
	a := reprioritizeSubmit(t, s, "persist", 3, 1)
	first := reprioritizeApply(t, s, a)
	if first.Event.Actor != "coordinator" || first.Event.Reason != "priority only" {
		t.Fatal("applied identity")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openAdminCommandStore(t, path)
	before := reprioritizeLoad(t, s)
	if reprioritizeRecord(t, before.Attempts, "attempt-task-00").AdminImportance == nil || *reprioritizeRecord(t, before.Attempts, "attempt-task-00").AdminImportance != 1 {
		t.Fatal("override not persisted")
	}
	later := *reprioritizeRecord(t, before.Attempts, "attempt-task-00")
	later.ThreadID = "post-application"
	later.Revision++
	reprioritizeSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{later}, Assignments: []domain.Assignment{{ID: "later", AttemptID: later.ID, State: domain.AssignmentReleased}}})
	before = reprioritizeLoad(t, s)
	replay := reprioritizeApply(t, s, a)
	after := reprioritizeLoad(t, s)
	if string(first.Event.Detail) != string(replay.Event.Detail) || !reprioritizeSameRecords(t, before, after) {
		t.Fatal("replay recomputed/wrote receipt")
	}
	var legacy adminEventDetail
	if err := json.Unmarshal([]byte(`{"commandKind":"cancel","state":"applied","expectedRevision":1}`), &legacy); err != nil || legacy.Reprioritize != nil {
		t.Fatal("old event decoding")
	}
	t.Run("corrupt-payload", func(t *testing.T) {
		s, _, _ := reprioritizeFixture(t, domain.ProgressReady)
		a := reprioritizeSubmit(t, s, "corrupt", 3, 5)
		c := *reprioritizeRecord(t, reprioritizeLoad(t, s).AdminCommands, a.CommandID)
		c.Payload = json.RawMessage(`{"importance":0}`)
		raw, _ := json.Marshal(c)
		if _, err := s.db.Exec("UPDATE coordinator_admin_commands SET record=? WHERE id=?", raw, c.ID); err != nil {
			t.Fatal(err)
		}
		before := reprioritizeLoad(t, s)
		d := reprioritizeApply(t, s, a)
		after := reprioritizeLoad(t, s)
		if d.Command.State != domain.AdminCommandRejected || !reflect.DeepEqual(reprioritizeByID(t, before.Attempts), reprioritizeByID(t, after.Attempts)) || !reflect.DeepEqual(reprioritizeByID(t, before.WorkflowRuns), reprioritizeByID(t, after.WorkflowRuns)) {
			t.Fatal("malformed durable payload applied")
		}
	})
}

func TestRunReprioritizeAllSkipped(t *testing.T) {
	for _, mode := range []string{"missing", "zero", "activation", "final", "closed"} {
		t.Run(mode, func(t *testing.T) {
			s, _, r := reprioritizeFixture(t)
			if mode != "zero" {
				r.Tasks = []domain.Task{{ID: "missing", WorkflowID: "wf", Name: "missing-name"}}
			}
			r.Attempts = []domain.Attempt{{ID: "overseer", WorkflowRunID: "run", TaskID: "missing", Number: 1, Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, SupervisionActivationID: "activation"}}
			if mode == "final" {
				reprioritizeRecord(t, r.WorkflowRuns, "run").Sink = &domain.SinkTask{Progress: domain.ProgressSucceeded}
			}
			reprioritizeSave(t, s, r)
			if mode == "closed" {
				_, err := s.PutSupervision(context.Background(), SupervisionMaterialization{Record: domain.SupervisionRecord{RunID: "run", Config: supervisionTestConfig(), ClosedByCancel: &domain.SupervisionClosure{CommandID: "cancel", ClosedAt: adminCommandTestTime}}})
				if err != nil {
					t.Fatal(err)
				}
			}
			before := reprioritizeLoad(t, s)
			a := reprioritizeSubmit(t, s, "all-skipped", 3, 5)
			d := reprioritizeApply(t, s, a)
			after := reprioritizeLoad(t, s)
			if mode == "final" || mode == "closed" {
				if d.Command.State != domain.AdminCommandRejected || !reflect.DeepEqual(reprioritizeByID(t, before.WorkflowRuns), reprioritizeByID(t, after.WorkflowRuns)) {
					t.Fatal("closed/final run modified")
				}
			} else {
				receipt := reprioritizeReceipt(t, d)
				if receipt.Changed == nil || len(receipt.Changed) != 0 || receipt.Skipped == nil {
					t.Fatal("empty arrays")
				}
				want := 1
				if mode == "zero" {
					want = 0
				}
				if len(receipt.Skipped) != want || want == 1 && receipt.Skipped[0].Reason != "missing-attempt" || reprioritizeRecord(t, after.WorkflowRuns, "run").Revision != 4 {
					t.Fatalf("all skipped: %#v", receipt)
				}
			}
			if !reflect.DeepEqual(reprioritizeByID(t, before.Attempts), reprioritizeByID(t, after.Attempts)) || !reflect.DeepEqual(reprioritizeByID(t, before.Assignments), reprioritizeByID(t, after.Assignments)) || !reflect.DeepEqual(reprioritizeByID(t, before.Activations), reprioritizeByID(t, after.Activations)) {
				t.Fatal("activation/lifecycle changed")
			}
		})
	}
}

func TestRunReprioritizePreservesHeldDelayedAndWaitingState(t *testing.T) {
	s, _, r := reprioritizeFixture(t, domain.ProgressBlocked, domain.ProgressActive)
	ctx := context.Background()
	reprioritizeRecord(t, r.WorkflowRuns, "run").GraphRevision = 1
	reprioritizeRecord(t, r.WorkflowRuns, "run").Sink = &domain.SinkTask{ID: "sink", Progress: domain.ProgressBlocked, GraphRevision: 1}
	reprioritizeRecord(t, r.Attempts, "attempt-task-00").AdminNotBefore = &adminCommandTestTime
	reprioritizeRecord(t, r.Attempts, "attempt-task-01").Control = domain.ControlRunning
	reprioritizeRecord(t, r.Attempts, "attempt-task-01").ThreadID = "thread"
	reprioritizeRecord(t, r.Attempts, "attempt-task-01").AssignmentID = "assignment"
	r.Assignments = []domain.Assignment{{ID: "assignment", AttemptID: reprioritizeRecord(t, r.Attempts, "attempt-task-01").ID, WorkerID: "worker", State: domain.AssignmentClaimed, Epoch: 1, ThreadID: "thread", LeaseToken: "lease", DispatchToken: "dispatch"}}
	r.QuotaPools = []domain.QuotaPool{{ID: "closed-pool", Admission: domain.AdmissionClosed}}
	r.Artifacts = []domain.Artifact{{ID: "retained", WorkflowRunID: "run", TaskID: reprioritizeRecord(t, r.Tasks, "task-00").ID, AttemptID: reprioritizeRecord(t, r.Attempts, "attempt-task-00").ID, SHA256: "retained-sha"}}
	reprioritizeSave(t, s, r)
	if err := s.SaveWorkerSnapshot(ctx, domain.WorkerSnapshot{WorkerID: "worker", WorkerEpoch: "epoch", CoordinatorEpoch: 1, Sequence: 1, Connected: true, ObservedAt: adminCommandTestTime, ValidUntil: adminCommandTestTime.Add(time.Hour), Inventory: domain.WorkerInventory{ID: "worker", AcceptBacklog: true, Health: domain.WorkerHealthReady}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterTaskWait(ctx, taskWaitRegistration(*reprioritizeRecord(t, r.Attempts, "attempt-task-01"), "wait", domain.WakeEach), adminCommandTestTime); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSupervision(ctx, SupervisionMaterialization{Record: domain.SupervisionRecord{RunID: "run", Config: supervisionTestConfig()}}); err != nil {
		t.Fatal(err)
	}
	hold := supervisionRunHold("hold", "hold-request", "hold stays")
	hold.RunID = "run"
	if _, err := s.PlaceHold(ctx, hold); err != nil {
		t.Fatal(err)
	}
	before := reprioritizeLoad(t, s)
	supervision, err := s.SupervisionReadSet(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	waits, err := s.LiveTaskWaitAttempts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	d := reprioritizeApply(t, s, reprioritizeSubmit(t, s, "preserve", 3, 5))
	if receipt := reprioritizeReceipt(t, d); len(receipt.Changed) != 1 {
		t.Fatalf("held blocked task not overridden: %#v", receipt)
	}
	after := reprioritizeLoad(t, s)
	if !reflect.DeepEqual(reprioritizeByID(t, before.Artifacts), reprioritizeByID(t, after.Artifacts)) || !reflect.DeepEqual(reprioritizeByID(t, before.Assignments), reprioritizeByID(t, after.Assignments)) || !reflect.DeepEqual(reprioritizeByID(t, before.QuotaPools), reprioritizeByID(t, after.QuotaPools)) || !reflect.DeepEqual(reprioritizeRecord(t, before.Attempts, "attempt-task-01"), reprioritizeRecord(t, after.Attempts, "attempt-task-01")) || !reflect.DeepEqual(reprioritizeRecord(t, before.WorkflowRuns, "run").Sink, reprioritizeRecord(t, after.WorkflowRuns, "run").Sink) {
		t.Fatal("custody/admission/waiting lifecycle changed")
	}
	gotSupervision, err := s.SupervisionReadSet(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	gotWaits, err := s.LiveTaskWaitAttempts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(supervision, gotSupervision) || !reflect.DeepEqual(waits, gotWaits) || !reflect.DeepEqual(reprioritizeRecord(t, before.Attempts, "attempt-task-00").AdminNotBefore, reprioritizeRecord(t, after.Attempts, "attempt-task-00").AdminNotBefore) {
		t.Fatal("hold/wait/delay mutated")
	}
}

func TestRunReprioritizeRunSetAtomicity(t *testing.T) {
	t.Run("membership-writer", func(t *testing.T) {
		s, _, _ := reprioritizeFixture(t, domain.ProgressReady)
		a := reprioritizeSubmit(t, s, "membership", 3, 5)
		start := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			<-start
			tx, err := s.db.BeginTx(context.Background(), nil)
			if err != nil {
				done <- err
				return
			}
			defer tx.Rollback()
			snapshot, err := loadWorkflowProjectionTx(context.Background(), tx, "run")
			if err == nil {
				task := domain.Task{ID: "new-member", WorkflowID: "wf", Name: "new member", RunID: "run", Importance: 2}
				raw, _ := json.Marshal(task)
				_, err = tx.Exec("INSERT INTO coordinator_tasks(id,workflow_id,name,record) VALUES(?,?,?,?)", task.ID, task.WorkflowID, task.Name, raw)
			}
			if err == nil {
				snapshot.Run.Revision++
				err = updateAdminWorkflowRunTx(context.Background(), tx, snapshot.Run)
			}
			if err == nil {
				err = tx.Commit()
			}
			done <- err
		}()
		close(start)
		d, err := s.ApplyAdminCommand(context.Background(), a)
		if writerErr := <-done; writerErr != nil {
			t.Fatal(writerErr)
		}
		if err != nil {
			t.Fatal(err)
		}
		after := reprioritizeLoad(t, s)
		switch d.Command.State {
		case domain.AdminCommandApplied:
			if len(reprioritizeReceipt(t, d).Changed) != 1 || reprioritizeRecord(t, after.Attempts, "attempt-task-00").Revision != 8 || reprioritizeRecord(t, after.WorkflowRuns, "run").Revision != 5 {
				t.Fatal("incoherent apply-first selection")
			}
		case domain.AdminCommandRejected:
			if d.CurrentTarget == nil || reprioritizeRecord(t, after.Attempts, "attempt-task-00").Revision != 7 || reprioritizeRecord(t, after.WorkflowRuns, "run").Revision != 4 {
				t.Fatal("writer-first fence failed")
			}
		default:
			t.Fatal("unexpected outcome")
		}
		if len(after.Tasks) != 2 || reprioritizeRecord(t, after.Tasks, "new-member").Name != "new member" {
			t.Fatal("membership writer lost")
		}
	})
	t.Run("audit-rollback", func(t *testing.T) {
		s, _, _ := reprioritizeFixture(t, domain.ProgressReady, domain.ProgressBlocked)
		a := reprioritizeSubmit(t, s, "rollback", 3, 5)
		if _, err := s.db.Exec(`CREATE TEMP TRIGGER fail_priority_audit BEFORE INSERT ON coordinator_audit_events WHEN NEW.kind='admin-command-applied' BEGIN SELECT RAISE(ABORT,'test outcome failure'); END`); err != nil {
			t.Fatal(err)
		}
		before := reprioritizeLoad(t, s)
		if _, err := s.ApplyAdminCommand(context.Background(), a); err == nil {
			t.Fatal("expected rollback")
		}
		if !reprioritizeSameRecords(t, before, reprioritizeLoad(t, s)) {
			t.Fatal("partial run-set or outcome")
		}
	})
	t.Run("concurrent", func(t *testing.T) {
		s, _, _ := reprioritizeFixture(t, domain.ProgressReady, domain.ProgressBlocked)
		a := reprioritizeSubmit(t, s, "concurrent", 3, 5)
		before := reprioritizeLoad(t, s)
		const callers = 6
		start := make(chan struct{})
		var wg sync.WaitGroup
		decisions := make(chan domain.AdminCommandDecision, callers)
		for range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				d, err := s.ApplyAdminCommand(context.Background(), a)
				if err != nil {
					t.Error(err)
					return
				}
				decisions <- d
			}()
		}
		close(start)
		wg.Wait()
		close(decisions)
		var detail string
		for d := range decisions {
			if detail == "" {
				detail = string(d.Event.Detail)
			}
			if d.Command.State != domain.AdminCommandApplied || string(d.Event.Detail) != detail {
				t.Fatal("incoherent outcomes")
			}
		}
		after := reprioritizeLoad(t, s)
		if len(after.AuditEvents) != len(before.AuditEvents)+1 || len(before.Attempts) != len(after.Attempts) || reprioritizeRecord(t, after.WorkflowRuns, "run").Revision != 4 {
			t.Fatal("multiple outcomes")
		}
		for id := range reprioritizeByID(t, before.Attempts) {
			attempt := reprioritizeRecord(t, after.Attempts, id)
			if attempt.Revision != 8 {
				t.Fatal("multiple transitions")
			}
		}
	})
	for _, field := range []string{"attempt", "related", "new", "run", "schedule", "trigger", "pause", "safety", "validity", "wrong-target"} {
		t.Run(field, func(t *testing.T) {
			s, _, r := reprioritizeFixture(t, domain.ProgressReady)
			a := reprioritizeSubmit(t, s, "inject", 3, 5)
			switch field {
			case "attempt":
				a.Attempt = reprioritizeRecord(t, r.Attempts, "attempt-task-00")
			case "related":
				a.RelatedAttempts = r.Attempts
			case "new":
				a.NewAttempt = reprioritizeRecord(t, r.Attempts, "attempt-task-00")
			case "run":
				a.WorkflowRun = reprioritizeRecord(t, r.WorkflowRuns, "run")
			case "schedule":
				a.Schedule = &domain.Schedule{}
			case "trigger":
				a.ScheduleTrigger = &domain.ScheduleTriggerRequest{}
			case "pause":
				a.PauseIntent = &domain.ThrottleAttemptTransition{}
			case "safety":
				a.SafetyFingerprint = "injected"
			case "validity":
				a.SafetyValidUntil = &adminCommandTestTime
			case "wrong-target":
				c := *reprioritizeRecord(t, reprioritizeLoad(t, s).AdminCommands, a.CommandID)
				c.TargetType = domain.AdminTargetAttempt
				c.TargetID = reprioritizeRecord(t, r.Attempts, "attempt-task-00").ID
				raw, _ := json.Marshal(c)
				if _, err := s.db.Exec("UPDATE coordinator_admin_commands SET record=? WHERE id=?", raw, c.ID); err != nil {
					t.Fatal(err)
				}
			}
			before := reprioritizeLoad(t, s)
			if _, err := s.ApplyAdminCommand(context.Background(), a); err == nil {
				t.Fatal("injected transition accepted")
			}
			if !reprioritizeSameRecords(t, before, reprioritizeLoad(t, s)) {
				t.Fatal("invalid application wrote state")
			}
		})
	}
}

func TestRunReprioritizeSelectsNeverStartedTasks(t *testing.T) {
	s, _, r := reprioritizeFixture(t, domain.ProgressReady, domain.ProgressBlocked, domain.ProgressReady, domain.ProgressReady, domain.ProgressActive, domain.ProgressReady, domain.ProgressReady, domain.ProgressSucceeded)
	reprioritizeRecord(t, r.Attempts, "attempt-task-02").Control = domain.ControlPreparing
	reprioritizeRecord(t, r.Attempts, "attempt-task-03").AssignmentID = "offer"
	reprioritizeRecord(t, r.Attempts, "attempt-task-04").Control = domain.ControlRunning
	reprioritizeRecord(t, r.Attempts, "attempt-task-05").Control = domain.ControlPaused
	reprioritizeRecord(t, r.Attempts, "attempt-task-06").Control = domain.ControlDraining
	other := *reprioritizeRecord(t, r.Attempts, "attempt-task-00")
	other.ID = "other-run"
	other.WorkflowRunID = "other"
	r.Attempts = append(r.Attempts, other)
	r.WorkflowRuns = append(r.WorkflowRuns, domain.WorkflowRun{ID: "other", WorkflowID: "wf", Revision: 1})
	// Scramble persisted input; receipt order must come from task IDs.
	firstTask, lastTask := reprioritizeRecord(t, r.Tasks, "task-00"), reprioritizeRecord(t, r.Tasks, "task-07")
	*firstTask, *lastTask = *lastTask, *firstTask
	reprioritizeSave(t, s, r)
	for round, importance := range []int{5, 5, 1} {
		before := reprioritizeLoad(t, s)
		d := reprioritizeApply(t, s, reprioritizeSubmit(t, s, fmt.Sprintf("priority-%d", round), int64(3+round), importance))
		if d.Command.State != domain.AdminCommandApplied {
			t.Fatalf("not applied: %#v", d)
		}
		receipt := reprioritizeReceipt(t, d)
		if receipt.Importance != importance || len(receipt.Changed) != 2 || len(receipt.Skipped) != 6 {
			t.Fatalf("receipt: %#v", receipt)
		}
		wantChanged := []reprioritizeTestRecord{
			{TaskID: "task-00", TaskName: "name-task-00"},
			{TaskID: "task-01", TaskName: "name-task-01"},
		}
		if !reflect.DeepEqual(receipt.Changed, wantChanged) {
			t.Fatalf("changed identities/order: %#v", receipt.Changed)
		}
		wantReasons := []string{"control", "history", "control", "control", "control", "terminal"}
		for i, record := range receipt.Skipped {
			if record.TaskID != fmt.Sprintf("task-%02d", i+2) || record.TaskName != "name-"+record.TaskID || record.Reason != wantReasons[i] {
				t.Fatalf("skip: %#v", record)
			}
		}
		after := reprioritizeLoad(t, s)
		if len(before.Attempts) != len(after.Attempts) || len(before.WorkflowRuns) != len(after.WorkflowRuns) {
			t.Fatal("record membership changed")
		}
		for id, old := range reprioritizeByID(t, before.Attempts) {
			got := reprioritizeRecord(t, after.Attempts, id)
			if old.WorkflowRunID == "run" && (old.TaskID == "task-00" || old.TaskID == "task-01") {
				var override struct {
					Importance *int `json:"adminImportance"`
				}
				raw, _ := json.Marshal(got)
				json.Unmarshal(raw, &override)
				if override.Importance == nil || *override.Importance != importance || got.Revision != old.Revision+1 || !got.UpdatedAt.Equal(d.Event.CreatedAt) {
					t.Fatalf("override: %#v %s", got, raw)
				}
				var left, right map[string]any
				raw, _ = json.Marshal(old)
				json.Unmarshal(raw, &left)
				raw, _ = json.Marshal(got)
				json.Unmarshal(raw, &right)
				for _, key := range []string{"adminImportance", "revision", "updatedAt"} {
					delete(left, key)
					delete(right, key)
				}
				if !reflect.DeepEqual(left, right) {
					t.Fatal("changed lifecycle fields")
				}
			} else if !reflect.DeepEqual(old, got) {
				t.Fatalf("skip/other-run changed: %s", old.ID)
			}
		}
		for id, original := range reprioritizeByID(t, before.WorkflowRuns) {
			old := *original
			got := *reprioritizeRecord(t, after.WorkflowRuns, id)
			if old.ID == "run" {
				if got.Revision != old.Revision+1 {
					t.Fatal("run revision")
				}
				got.Revision = old.Revision
				got.UpdatedAt = old.UpdatedAt
			}
			if !reflect.DeepEqual(old, got) {
				t.Fatal("run fields changed")
			}
		}
		if !reflect.DeepEqual(reprioritizeByID(t, before.Tasks), reprioritizeByID(t, after.Tasks)) || !reflect.DeepEqual(reprioritizeByID(t, before.Workflows), reprioritizeByID(t, after.Workflows)) || !reflect.DeepEqual(reprioritizeByID(t, before.Assignments), reprioritizeByID(t, after.Assignments)) || !reflect.DeepEqual(reprioritizeByID(t, before.QuotaPools), reprioritizeByID(t, after.QuotaPools)) {
			t.Fatal("definitions/admission changed")
		}
	}
}
