package backlogadmin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// U-4: cancelling a task whose attempt is parked on a live task wait settles
// the wait as cancelled in the same command application, so the worker's
// check row has a settled coordinator record to reconcile against and the
// attempt is not left parked on a wait nothing will ever settle.
func TestCancelSettlesTheLiveTaskWait(t *testing.T) {
	ctx := context.Background()
	store := openAdminTestStore(t)
	now := adminTestNow
	records := parkedRecords(now)
	records.Attempts[0].Progress, records.Attempts[0].Control = domain.ProgressActive, domain.ControlRunning
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	wait, err := store.RegisterTaskWait(ctx, domain.TaskWaitRegistration{
		RequestID: "park-1", WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1",
		IssuedRevision: 6, ThreadID: "thread-1", Wake: domain.WakeEach, MaxDuration: time.Hour,
		Name: "ci", Condition: "gh run view",
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now.Add(time.Minute) })
	submitted, err := service.Mutate(ctx, Mutation{
		Version: Version, Principal: Principal{ID: "operator"}, ID: "cancel-parked",
		Kind: domain.AdminCommandCancel, WorkflowRunID: "run-1", TaskID: "nest-model",
		ExpectedRevision: 7, Reason: "the branch was abandoned",
	})
	if err != nil || submitted.Command.State != domain.AdminCommandPending {
		t.Fatalf("submission = %#v, %v", submitted, err)
	}
	report, err := service.ExecutePendingCommands(ctx)
	if err != nil || len(report.Decisions) != 1 || report.Decisions[0].Command.State != domain.AdminCommandApplied {
		t.Fatalf("execution = %#v, %v", report, err)
	}
	loaded, err := store.LoadCoordinatorRecords(ctx)
	if err != nil || loaded.Attempts[0].Progress != domain.ProgressCancelled {
		t.Fatalf("attempt = %#v err=%v", loaded.Attempts, err)
	}
	waits, err := store.ListTaskWaits(ctx)
	if err != nil || len(waits) != 1 {
		t.Fatalf("waits=%v err=%v", waits, err)
	}
	if waits[0].ID != wait.ID || waits[0].Result == nil || waits[0].Result.Outcome != domain.TaskWaitCancelled {
		t.Fatalf("the live wait was not settled by the cancellation: %+v", waits[0])
	}
	if !strings.Contains(waits[0].Result.Reason, "cancelled") || !strings.Contains(waits[0].Result.Reason, "the branch was abandoned") {
		t.Fatalf("the settlement does not say why: %q", waits[0].Result.Reason)
	}
	// The attempt is terminal, so the wake is closed rather than delivered,
	// and nothing is left parked.
	if _, err := store.WakeTaskWaits(ctx, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, _ = store.ListTaskWaits(ctx)
	if !waits[0].Woken() || waits[0].Delivery != "abandoned" {
		t.Fatalf("the settled wait of a cancelled attempt was not closed: %+v", waits[0])
	}
	live, err := store.LiveTaskWaitAttempts(ctx)
	if err != nil || len(live) != 0 {
		t.Fatalf("live waits after cancel: %v err=%v", live, err)
	}
}
