package workerruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestFailedPreparationPublishesWithoutProviderWaitAcknowledgement(t *testing.T) {
	ctx := context.Background()
	driver := &fakeDriver{prepareErrs: []error{errors.New("first setup failure"), errors.New("second setup failure"), errors.New("third setup failure")}}
	runtime := newClaimedRuntime(t, t.TempDir(), driver)
	if err := runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ParkedReported: true}); err != nil {
		t.Fatal(err)
	}
	prepare := testCommand(t, runtime, domain.WorkerCommandPrepare, "prepare")
	if _, err := runtime.DeliverCommands(ctx, workerproto.CommandDelivery{Commands: []domain.WorkerCommand{prepare}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < MaxPrepareAttempts; i++ {
		if err := runtime.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
	}
	state, _ := runtime.journal.snapshot()
	if state.Attempts["assignment-1"].Phase != PhaseFailed {
		t.Fatal("did not exhaust preparation budget")
	}
	collect := testCommand(t, runtime, domain.WorkerCommandCollect, "collect")
	if _, err := runtime.DeliverCommands(ctx, workerproto.CommandDelivery{Commands: []domain.WorkerCommand{collect}}); err != nil {
		t.Fatal(err)
	}
	state, _ = runtime.journal.snapshot()
	if driver.collectFailureCalls != 1 || state.Attempts["assignment-1"].Phase != PhaseCompleted {
		t.Fatalf("failed result not published: calls=%d phase=%s", driver.collectFailureCalls, state.Attempts["assignment-1"].Phase)
	}
	if driver.collectCalls != 0 {
		t.Fatal("collected task outputs after failed preparation")
	}
}
