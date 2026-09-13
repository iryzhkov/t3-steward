package sqlite

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestNativeWaitRetryRestartSettlementAndDeliveryFence(t *testing.T) {
	ctx := context.Background()
	store, before, now := sinkStoreFixture(t)
	path := store.path
	a := before.Attempts[0]
	a.Progress = domain.ProgressActive
	a.Control = domain.ControlRunning
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{a}}); err != nil {
		t.Fatal(err)
	}
	req := domain.NodeWaitRequest{ID: "nw-test", ThreadID: "thread", Name: "task", Target: domain.NodeRef{RunID: "r", TaskID: "t"}, Timeout: time.Hour}
	first, err := store.RegisterNodeWait(ctx, req, "operator", "host", now)
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.RegisterNodeWait(ctx, req, "operator", "host", now.Add(time.Minute))
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatalf("replay=%+v err=%v", again, err)
	}
	changed := req
	changed.ThreadID = "other"
	if _, err := store.RegisterNodeWait(ctx, changed, "operator", "host", now); err == nil {
		t.Fatal("changed registration accepted")
	}
	a.Progress = domain.ProgressFailed
	a.Control = domain.ControlStopped
	retry := a
	retry.ID = "a2"
	retry.Number = 2
	retry.Progress = domain.ProgressActive
	retry.Control = domain.ControlRunning
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{a, retry}}); err != nil {
		t.Fatal(err)
	}
	if err := store.SettleNodeWaits(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, err := store.ListNodeWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if waits[0].SettledAt != nil {
		t.Fatal("settled during applied retry")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	retry.Progress = domain.ProgressSucceeded
	retry.Control = domain.ControlStopped
	retry.Revision++
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{retry}}); err != nil {
		t.Fatal(err)
	}
	if err := store.SettleNodeWaits(ctx, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, err = store.ListNodeWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	settled := waits[0]
	if settled.Observation.ExitCode != 0 || settled.Observation.AttemptID != "a2" || settled.DeliveryID != "node-wake:nw-test" {
		t.Fatalf("settled=%+v", settled)
	}
	// A new later attempt cannot retract committed wait evidence.
	retry.ID = "a3"
	retry.Number = 3
	retry.Progress = domain.ProgressActive
	retry.Control = domain.ControlRunning
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{retry}}); err != nil {
		t.Fatal(err)
	}
	if err := store.SettleNodeWaits(ctx, now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, err = store.ListNodeWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(settled, waits[0]) {
		t.Fatal("terminal observation re-evaluated")
	}
	if _, err := store.db.Exec("DELETE FROM coordinator_workflow_runs WHERE id='r'"); err == nil {
		t.Fatal("deleted pinned run")
	}
	if _, err := store.db.Exec("DELETE FROM coordinator_attempts WHERE id='a2'"); err == nil {
		t.Fatal("deleted pinned attempt")
	}
	if ok, err := store.TransitionNodeWake(ctx, req.ID, "pending", "sending", now); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if ok, err := store.TransitionNodeWake(ctx, req.ID, "pending", "sending", now); err != nil || ok {
		t.Fatal("double delivery claim", ok, err)
	}
	if _, err := store.TransitionNodeWake(ctx, req.ID, "sending", "recovery-required", now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionNodeWake(ctx, req.ID, "recovery-required", "sending", now); err == nil {
		t.Fatal("rearmed ambiguous delivery")
	}
	if _, err := store.TransitionNodeWake(ctx, req.ID, "recovery-required", "delivered", now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec("DELETE FROM coordinator_workflow_runs WHERE id='r'"); err != nil {
		t.Fatal("pin not released", err)
	}
	if _, err := store.RegisterNodeWait(ctx, req, "operator", "host", now); err != nil {
		t.Fatal("replay required deleted source", err)
	}
}

func TestNativeWaitCancellationTimeoutAndMissing(t *testing.T) {
	for _, mode := range []string{"cancel", "timeout", "missing"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store, before, now := sinkStoreFixture(t)
			req := domain.NodeWaitRequest{ID: "nw-test", ThreadID: "thread", Target: domain.NodeRef{RunID: "r", TaskID: domain.SinkTaskName}, Timeout: time.Minute}
			if mode == "missing" {
				req.Target.RunID = "absent"
				if _, err := store.RegisterNodeWait(ctx, req, "operator", "host", now); err == nil {
					t.Fatal("missing target accepted")
				}
				return
			}
			if _, err := store.RegisterNodeWait(ctx, req, "operator", "host", now); err != nil {
				t.Fatal(err)
			}
			if mode == "cancel" {
				a := before.Attempts[0]
				a.Progress = domain.ProgressCancelled
				run, err := domain.ProjectRunSink(before.Run, before.Tasks, []domain.Attempt{a}, nil, now)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}, Attempts: []domain.Attempt{a}}); err != nil {
					t.Fatal(err)
				}
			}
			at := now
			if mode == "timeout" {
				at = now.Add(time.Minute)
			}
			if err := store.SettleNodeWaits(ctx, at); err != nil {
				t.Fatal(err)
			}
			waits, err := store.ListNodeWaits(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if waits[0].Observation == nil || waits[0].Observation.ExitCode != 2 {
				t.Fatalf("wait=%+v", waits)
			}
		})
	}
}

func TestCrossRunEdgesBindPinAndRejectCycleAtomically(t *testing.T) {
	ctx := context.Background()
	store, before, now := sinkStoreFixture(t)
	source := domain.NodeRef{RunID: "r", TaskID: "t"}
	task := domain.Task{ID: "consumer", Name: "consumer", WorkflowID: "w2", ExternalNeeds: []domain.NodeRef{source}}
	run, err := domain.BindRunSink(domain.WorkflowRun{ID: "r2", WorkflowID: "w2", Revision: 1, Progress: domain.ProgressBlocked, CreatedAt: now, UpdatedAt: now}, []domain.Task{task})
	if err != nil {
		t.Fatal(err)
	}
	records := CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{task}}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec("DELETE FROM coordinator_tasks WHERE id='t'"); err == nil {
		t.Fatal("deleted pinned source task")
	}
	cycle := before.Tasks[0]
	cycle.ExternalNeeds = []domain.NodeRef{{RunID: "r2", TaskID: domain.SinkTaskName}}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Tasks: []domain.Task{cycle}}); err == nil {
		t.Fatal("cross-run sink cycle accepted")
	}
	got, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range got.Tasks {
		if task.ID == "t" && len(task.ExternalNeeds) != 0 {
			t.Fatal("cycle transaction partially published")
		}
	}
	bad := domain.Task{ID: "bad", Name: "bad", WorkflowID: "w3", ExternalNeeds: []domain.NodeRef{{RunID: "no-run", TaskID: "no-task"}}}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Tasks: []domain.Task{bad}}); err == nil {
		t.Fatal("unavailable source accepted")
	}
}
