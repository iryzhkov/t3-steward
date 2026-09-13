package workerruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type countedT3 struct {
	recordingT3
	reads   int
	readErr error
}

func (c *countedT3) ListThreads(ctx context.Context) ([]domain.Thread, error) {
	c.reads++
	if c.readErr != nil {
		return nil, c.readErr
	}
	return c.recordingT3.ListThreads(ctx)
}

type cachedObservationDriver struct {
	*fakeDriver
	local *LocalDriver
}

func (d *cachedObservationDriver) BeginObservationPass() {
	d.local.BeginObservationPass()
}

func (d *cachedObservationDriver) ObserveThread(ctx context.Context, pkg workerproto.ExecutionPackage) (backlog.DispatchThreadState, error) {
	return d.local.ObserveThread(ctx, pkg)
}

func TestPersistentRuntimeRefreshesCompletedTurn(t *testing.T) {
	ctx := context.Background()
	control := &countedT3{recordingT3: recordingT3{
		thread: &domain.Thread{ID: "thread-1", TurnState: "running", Running: true},
	}}
	cache := NewCachedT3(control)
	driver := &cachedObservationDriver{
		fakeDriver: &fakeDriver{workspace: "workspace", workspaceReady: true},
		local:      &LocalDriver{T3: cache},
	}
	runtime := newClaimedRuntime(t, t.TempDir(), driver.fakeDriver)
	runtime.driver = driver
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", "workspace", "thread-1"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	// Repeated observations in this pass reuse the same snapshot.
	for range 3 {
		if _, err := driver.ObserveThread(ctx, testPackage()); err != nil {
			t.Fatal(err)
		}
	}
	if control.reads != 1 || driver.collectCalls != 0 {
		t.Fatalf("running pass: reads=%d collections=%d", control.reads, driver.collectCalls)
	}
	control.thread.Running = false
	control.thread.TurnState = "completed"
	// Unavailable fresh evidence must not turn a cached observation into a result.
	control.readErr = errors.New("T3 unavailable")
	if err := runtime.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if driver.collectCalls != 0 {
		t.Fatal("collected without fresh terminal evidence")
	}
	control.readErr = nil
	if err := runtime.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if state.Attempts["assignment-1"].Phase != PhaseCompleted || driver.collectCalls != 1 || driver.createCalls != 0 {
		t.Fatalf("terminal pass: phase=%s collections=%d creates=%d",
			state.Attempts["assignment-1"].Phase, driver.collectCalls, driver.createCalls)
	}
}

func TestCachedT3MutationInvalidatesWithinPass(t *testing.T) {
	ctx := context.Background()
	control := &countedT3{recordingT3: recordingT3{
		thread: &domain.Thread{ID: "thread-1", TurnState: "running", Running: true},
	}}
	cache := NewCachedT3(control)
	driver := &LocalDriver{T3: cache}
	if _, err := driver.ObserveThread(ctx, testPackage()); err != nil {
		t.Fatal(err)
	}
	if err := driver.StopThread(ctx, testPackage()); err != nil {
		t.Fatal(err)
	}
	state, err := driver.ObserveThread(ctx, testPackage())
	if err != nil || state != backlog.DispatchThreadStopped || control.reads != 2 {
		t.Fatalf("after stop: state=%s reads=%d err=%v", state, control.reads, err)
	}
}
