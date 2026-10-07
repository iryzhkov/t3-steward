package workerruntime

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// livenessProbeDriver records the journal's liveness timestamp at the moment
// the pass observes its first thread.
type livenessProbeDriver struct {
	*fakeDriver
	journal *Journal
	seen    []time.Time
}

func (d *livenessProbeDriver) ObserveThread(ctx context.Context, pkg workerproto.ExecutionPackage) (backlog.DispatchThreadState, error) {
	state, err := d.journal.snapshot()
	if err != nil {
		return "", err
	}
	d.seen = append(d.seen, state.WorkerLastSeenAt)
	return d.fakeDriver.ObserveThread(ctx, pkg)
}

// The liveness write is a synced journal write. Done before the pass, it
// spent the exchange's reconcile budget before any attempt was looked at, and
// under disk contention a short budget expired before collection began. The
// pass therefore runs first and liveness is recorded after it.
func TestReconcileRecordsLivenessAfterThePass(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, "normandy", "worker-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	driver := &livenessProbeDriver{fakeDriver: &fakeDriver{
		workspace: filepath.Join(root, "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive},
	}, journal: journal}
	runtime, err := New(testConfig(func() time.Time { return runtimeTestNow }), journal, driver)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(driver.seen) != 1 || !driver.seen[0].IsZero() {
		t.Fatalf("liveness during the first pass = %v; want it written only after the pass", driver.seen)
	}
	state, err := journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !state.WorkerLastSeenAt.Equal(runtimeTestNow) {
		t.Fatalf("liveness after the pass = %v, want %v", state.WorkerLastSeenAt, runtimeTestNow)
	}
}

// A pass that fails still proves the worker process is alive.
func TestReconcileRecordsLivenessWhenThePassFails(t *testing.T) {
	root := t.TempDir()
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true}
	runtime := newTestRuntime(t, root, driver)
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runtime.Reconcile(ctx); err == nil {
		t.Fatal("cancelled pass reported success")
	}
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !state.WorkerLastSeenAt.Equal(runtimeTestNow) {
		t.Fatalf("liveness after a failed pass = %v, want %v", state.WorkerLastSeenAt, runtimeTestNow)
	}
}

// Every exchange reconciles, so a synced write per pass is a steady cost on
// every worker. Liveness is refreshed at most once per LivenessRefreshInterval,
// well inside OwnershipLivenessGrace.
func TestReconcileThrottlesLivenessWrites(t *testing.T) {
	root := t.TempDir()
	now := runtimeTestNow
	runtime := newTestRuntimeWithClock(t, root, &fakeDriver{}, func() time.Time { return now })
	journalInode := func() uint64 {
		t.Helper()
		info, err := os.Stat(runtime.journal.path)
		if err != nil {
			t.Fatal(err)
		}
		return info.Sys().(*syscall.Stat_t).Ino
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := journalInode()
	now = runtimeTestNow.Add(10 * time.Second)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if journalInode() != first {
		t.Fatal("a pass ten seconds after the last liveness write rewrote the journal")
	}
	now = runtimeTestNow.Add(LivenessRefreshInterval)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if journalInode() == first {
		t.Fatal("liveness was not refreshed after the refresh interval")
	}
	state, err := runtime.journal.snapshot()
	if err != nil || !state.WorkerLastSeenAt.Equal(now) {
		t.Fatalf("refreshed liveness = %v, %v; want %v", state.WorkerLastSeenAt, err, now)
	}
	if LivenessRefreshInterval*5 > OwnershipLivenessGrace {
		t.Fatalf("refresh interval %s leaves too little of the %s ownership grace", LivenessRefreshInterval, OwnershipLivenessGrace)
	}
}
