package workerruntime

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestEntireParkResumeBetweenWorkerObservationsPreservesSecondPark(t *testing.T) {
	f := newWakeWindowFixture(t)
	ctx := context.Background()
	// A coordinator report is constructed before the first registration, then
	// arrives after the task has registered and stopped.
	beforeFirstPark, err := backlog.ParkedAssignmentsFor(ctx, f.store, f.pkg.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	first, err := registerFromInjectedIdentity(t, f.store, f.workspace, "invisible-first", f.now)
	if err != nil {
		t.Fatal(err)
	}
	f.turnEnded()
	if err := f.runtime.ApplyParkedAssignments(beforeFirstPark); err != nil {
		t.Fatal(err)
	}
	stopped, err := f.runtime.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.SaveWorkerSnapshot(ctx, stopped); err != nil {
		t.Fatal(err)
	}
	if !f.identityPresent(t) {
		t.Fatal("first pre-registration report collected")
	}

	// While the worker has no exchange, the coordinator settles the wait,
	// delivers its wake, and the provider resumes. No worker poll sees Running
	// and no positive parked report ever reaches the worker.
	if _, err := f.store.SettleTaskWait(ctx, first.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, f.now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	wakes, err := f.store.WakeTaskWaits(ctx, f.now.Add(time.Minute))
	if err != nil || len(wakes) != 1 {
		t.Fatalf("wake: %+v %v", wakes, err)
	}
	f.deliverWake(t, wakes[0], f.now.Add(2*time.Minute))
	f.turnRunning()
	beforeSecondPark, err := backlog.ParkedAssignmentsFor(ctx, f.store, f.pkg.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeSecondPark.Parked) != 0 || beforeSecondPark.ObservedSequence != stopped.Sequence {
		t.Fatalf("fixture needs an empty statement acknowledging original stop: %+v", beforeSecondPark)
	}
	if _, err := registerFromInjectedIdentity(t, f.store, f.workspace, "invisible-second", f.now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	f.turnEnded()
	f.control.message = "parked again"
	f.control.archive = []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-2","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`)
	if err := f.runtime.ApplyParkedAssignments(beforeSecondPark); err != nil {
		t.Fatal(err)
	}
	if _, err := f.runtime.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	if !f.identityPresent(t) || len(f.publisher.results) != 0 {
		t.Fatal("an unobserved park/resume reused the original stop ack and collected the second parked turn")
	}
}
