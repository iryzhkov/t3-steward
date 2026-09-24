package workerruntime

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// slowVerifyDriver collects the way the local driver does when a declared
// verification command runs longer than a reconcile pass: it honours the
// context it is given, and a context that ends first kills the command.
type slowVerifyDriver struct {
	*fakeDriver
	verify time.Duration

	mu         sync.Mutex
	calls      int
	cancelled  int
	running    int
	maxRunning int
	deadlines  []time.Duration
}

func (d *slowVerifyDriver) Collect(ctx context.Context, _ workerproto.ExecutionPackage, _ string) error {
	d.mu.Lock()
	d.calls++
	d.running++
	d.maxRunning = max(d.maxRunning, d.running)
	if deadline, ok := ctx.Deadline(); ok {
		d.deadlines = append(d.deadlines, time.Until(deadline))
	} else {
		d.deadlines = append(d.deadlines, -1)
	}
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		d.running--
		d.mu.Unlock()
	}()
	select {
	case <-time.After(d.verify):
		return nil
	case <-ctx.Done():
		d.mu.Lock()
		d.cancelled++
		d.mu.Unlock()
		return fmt.Errorf("finalize attempt verification %q: %w", "sh check-build.sh", ctx.Err())
	}
}

func (d *slowVerifyDriver) counts() (calls, cancelled, maxRunning int, deadlines []time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls, d.cancelled, d.maxRunning, append([]time.Duration(nil), d.deadlines...)
}

func newSlowVerifyRuntime(t *testing.T, root string, driver *slowVerifyDriver) *Runtime {
	return newSlowVerifyRuntimeWithLifetime(t, root, driver, nil)
}

func newSlowVerifyRuntimeWithLifetime(t *testing.T, root string, driver *slowVerifyDriver, lifetime context.Context) *Runtime {
	t.Helper()
	journal, err := OpenJournal(root, "normandy", "worker-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	config := testConfig(func() time.Time { return runtimeTestNow })
	config.Lifetime = lifetime
	runtime, err := New(config, journal, driver)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

// A stopped attempt whose verification takes longer than a reconcile pass is
// collected exactly once and completes. The field failure (RC87, 2026-09-24)
// was a stopped-phase collection that ran its multi-minute verification under
// the two-minute context of the worker exchange that reached it: the command
// was killed at the deadline, the collection deferred, and the next pass
// started it again under the same kind of context.
func TestStoppedCollectionOutlivesReconcileDeadline(t *testing.T) {
	root := t.TempDir()
	driver := &slowVerifyDriver{
		fakeDriver: &fakeDriver{
			workspace:      filepath.Join(root, "workspace"),
			workspaceReady: true,
			observations:   []backlog.DispatchThreadState{backlog.DispatchThreadStopped},
		},
		verify: 600 * time.Millisecond,
	}
	runtime := newSlowVerifyRuntime(t, root, driver)
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseStopped, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}

	const tick = 100 * time.Millisecond
	stop := time.Now().Add(5 * time.Second)
	phase := Phase("")
	for passes := 0; time.Now().Before(stop); passes++ {
		ctx, cancel := context.WithTimeout(context.Background(), tick)
		err := runtime.Reconcile(ctx)
		cancel()
		if err != nil {
			t.Fatalf("pass %d: %v", passes, err)
		}
		state, err := runtime.journal.snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if phase = state.Attempts["assignment-1"].Phase; phase == PhaseCompleted {
			break
		}
		if passes == 1 {
			// A persistent worker replaces its runtime when the coordinator
			// epoch moves. The replacement must not start a second
			// verification of an attempt that is still being verified.
			runtime = newSlowVerifyRuntime(t, root, driver)
		}
	}
	calls, cancelled, maxRunning, deadlines := driver.counts()
	if phase != PhaseCompleted || calls != 1 || cancelled != 0 || maxRunning != 1 {
		t.Fatalf("phase=%q collections=%d cancelled=%d concurrent=%d; want one uncancelled collection that completes",
			phase, calls, cancelled, maxRunning)
	}
	// The collection is bounded, and by its own budget rather than the pass's.
	if len(deadlines) != 1 || deadlines[0] < 10*time.Minute || deadlines[0] > DefaultFinalizationTimeout {
		t.Fatalf("collection deadlines = %v; want one bounded by the finalization budget", deadlines)
	}
}

// A collection that outlives its pass still ends with the worker process: the
// daemon's shutdown cancels it, and the attempt stays collecting in the journal
// for the next process to collect again.
func TestRunningCollectionEndsWithWorkerLifetime(t *testing.T) {
	root := t.TempDir()
	driver := &slowVerifyDriver{
		fakeDriver: &fakeDriver{
			workspace:      filepath.Join(root, "workspace"),
			workspaceReady: true,
			observations:   []backlog.DispatchThreadState{backlog.DispatchThreadStopped},
		},
		verify: time.Minute,
	}
	lifetime, shutdown := context.WithCancel(context.Background())
	defer shutdown()
	runtime := newSlowVerifyRuntimeWithLifetime(t, root, driver, lifetime)
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseStopped, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err := runtime.Reconcile(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	shutdown()
	stop := time.Now().Add(5 * time.Second)
	for time.Now().Before(stop) {
		if _, cancelled, _, _ := driver.counts(); cancelled == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	calls, cancelled, _, _ := driver.counts()
	if phase := state.Attempts["assignment-1"].Phase; phase != PhaseCollecting || cancelled != 1 {
		t.Fatalf("phase=%q collections=%d cancelled=%d; want the shutdown to cancel it and leave the attempt collecting", phase, calls, cancelled)
	}
}
