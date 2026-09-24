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
	// The daemon drains on shutdown, so no verification it started is still
	// running when the next process collects the attempt again.
	drainCtx, stopDrain := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopDrain()
	if err := DrainCollections(drainCtx); err != nil {
		t.Fatal(err)
	}
	driver.mu.Lock()
	stillRunning := driver.running
	driver.mu.Unlock()
	if stillRunning != 0 {
		t.Fatalf("%d verifications still running after the drain", stillRunning)
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

// startSlowCollection claims an attempt, stops it, and runs one reconcile pass
// that starts its collection and leaves it running in the background.
func startSlowCollection(t *testing.T, verify time.Duration) (*Runtime, *slowVerifyDriver) {
	t.Helper()
	root := t.TempDir()
	driver := &slowVerifyDriver{
		fakeDriver: &fakeDriver{
			workspace:      filepath.Join(root, "workspace"),
			workspaceReady: true,
			observations:   []backlog.DispatchThreadState{backlog.DispatchThreadStopped},
		},
		verify: verify,
	}
	runtime := newSlowVerifyRuntime(t, root, driver)
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
	if record := attemptRecord(t, runtime); record.Phase != PhaseCollecting || !runtime.collectionRunning(record) {
		t.Fatalf("phase=%q running=%v; want a collection running past its pass", record.Phase, runtime.collectionRunning(record))
	}
	return runtime, driver
}

func attemptRecord(t *testing.T, runtime *Runtime) AttemptRecord {
	t.Helper()
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return state.Attempts["assignment-1"]
}

// waitCollectionFinished waits until the running collection has finished and
// its result is published but not yet taken by any pass.
func waitCollectionFinished(t *testing.T, runtime *Runtime) AttemptRecord {
	t.Helper()
	stop := time.Now().Add(5 * time.Second)
	for time.Now().Before(stop) {
		record := attemptRecord(t, runtime)
		if !runtime.collectionRunning(record) {
			if !runtime.collectionRegistered(record) {
				t.Fatal("the finished collection was taken before the test could act on it")
			}
			return record
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("collection did not finish")
	return AttemptRecord{}
}

func supersedingOffer(t *testing.T) workerproto.AssignmentOffers {
	t.Helper()
	offer := testOffer(t)
	offer.Assignment.Epoch = 3
	offer.Package.Package.Identity.AssignmentEpoch = 3
	manifest, err := workerproto.BuildExecutionPackageManifest(offer.Package.Package)
	if err != nil {
		t.Fatal(err)
	}
	offer.Package = manifest
	return workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{offer}}
}

// A higher-epoch offer does not replace an attempt whose collection is
// running, or finished and not yet taken: the older epoch would otherwise
// keep verifying in the same workspace and publish under the same result
// manifest the new epoch needs. The offer is withheld until a pass has taken
// the result.
func TestSupersedingOfferWaitsForCollection(t *testing.T) {
	runtime, driver := startSlowCollection(t, 300*time.Millisecond)
	for _, moment := range []string{"running", "finished"} {
		if moment == "finished" {
			waitCollectionFinished(t, runtime)
		}
		claims, err := runtime.AcceptOffers(context.Background(), supersedingOffer(t))
		if err != nil {
			t.Fatal(err)
		}
		record := attemptRecord(t, runtime)
		if len(claims.Claims) != 0 || record.Assignment.Epoch == 3 || record.Phase != PhaseCollecting || driver.stopCalls != 0 {
			t.Fatalf("%s: claims=%d epoch=%d phase=%q stops=%d; want the offer withheld and the collection untouched",
				moment, len(claims.Claims), record.Assignment.Epoch, record.Phase, driver.stopCalls)
		}
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if record := attemptRecord(t, runtime); record.Phase != PhaseCompleted {
		t.Fatalf("phase = %q, want the collection's result taken", record.Phase)
	}
	claims, err := runtime.AcceptOffers(context.Background(), supersedingOffer(t))
	if err != nil {
		t.Fatal(err)
	}
	if record := attemptRecord(t, runtime); len(claims.Claims) != 1 || record.Assignment.Epoch != 3 {
		t.Fatalf("claims=%d epoch=%d; want the retried offer claimed once the collection is taken", len(claims.Claims), record.Assignment.Epoch)
	}
	if calls, _, _, _ := driver.counts(); calls != 1 {
		t.Fatalf("collections = %d, want 1", calls)
	}
}

// A stop yields to a collection an earlier pass started. While it runs the
// stop is deferred; once it has finished the stop takes its result, so the
// attempt completes with the published result instead of being stopped over
// it, and nothing is left in the registry.
func TestStopYieldsToCollection(t *testing.T) {
	runtime, driver := startSlowCollection(t, 300*time.Millisecond)
	if err := runtime.stop(context.Background(), "assignment-1"); err == nil {
		t.Fatal("stop during a running collection was not deferred")
	}
	if record := attemptRecord(t, runtime); record.Phase != PhaseCollecting || driver.stopCalls != 0 {
		t.Fatalf("during: phase=%q stops=%d", record.Phase, driver.stopCalls)
	}
	record := waitCollectionFinished(t, runtime)
	if err := runtime.stop(context.Background(), "assignment-1"); err != nil {
		t.Fatal(err)
	}
	after := attemptRecord(t, runtime)
	if after.Phase != PhaseCompleted || after.StopConfirmed || driver.stopCalls != 0 || runtime.collectionRegistered(record) {
		t.Fatalf("after: phase=%q stopConfirmed=%v stops=%d registered=%v; want the result taken and no stop",
			after.Phase, after.StopConfirmed, driver.stopCalls, runtime.collectionRegistered(record))
	}
}

// A workspace that disappears after a collection started does not fail the
// attempt over the collection's own result.
func TestMissingWorkspaceDoesNotSupersedeCollection(t *testing.T) {
	runtime, driver := startSlowCollection(t, 300*time.Millisecond)
	driver.workspaceReady = false
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitCollectionFinished(t, runtime)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if record := attemptRecord(t, runtime); record.Phase != PhaseCompleted || record.Failure != "" || driver.collectFailureCalls != 0 {
		t.Fatalf("phase=%q failure=%q failure-collections=%d; want the collection's result", record.Phase, record.Failure, driver.collectFailureCalls)
	}
}
