package workerruntime

import (
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// startGatedCollection starts the collection of a collecting attempt in a pass
// that does not wait for it, and returns with that collection running.
func startGatedCollection(t *testing.T) (*Runtime, *gatedCollectDriver, *atomic.Int32, func()) {
	t.Helper()
	root := t.TempDir()
	collects := new(atomic.Int32)
	driver := newGatedCollectDriver(filepath.Join(root, "workspace"), collects)
	release := releaseOnce(driver)
	t.Cleanup(release)
	runtime := newGatedRuntime(t, root, driver)
	seedAttempt(t, runtime, collectingRecord(t, driver.workspace))
	pass := &collectionPass{}
	if err := runtime.Reconcile(withCollectionPass(context.Background(), pass)); err != nil {
		t.Fatal(err)
	}
	receiveWithin(t, driver.started, 5*time.Second, "the collection")
	if record := attemptRecord(t, runtime); !runtime.collectionRunning(record) {
		t.Fatal("the collection is not running past the pass that started it")
	}
	return runtime, driver, collects, func() {
		release()
		if !pass.wait(context.Background(), 5*time.Second) {
			t.Fatal("the collection did not finish once released")
		}
	}
}

// A collection whose attempt was superseded while it ran does not decide the
// attempt: its result is discarded, the superseding record is left as it is,
// and the collection is not started again.
func TestSupersededCollectionResultIsDiscarded(t *testing.T) {
	for _, tc := range []struct {
		name      string
		supersede func(*AttemptRecord)
		wantPhase Phase
		wantEpoch int64
	}{
		{
			name: "a higher assignment epoch replaced the attempt",
			supersede: func(record *AttemptRecord) {
				record.Assignment.Epoch = 3
				record.Package.Package.Identity.AssignmentEpoch = 3
				record.Phase = PhaseClaimed
			},
			wantPhase: PhaseClaimed, wantEpoch: 3,
		},
		{
			name:      "the attempt was already finalized",
			supersede: func(record *AttemptRecord) { record.Phase = PhaseFailed; record.Failure = "failed elsewhere" },
			wantPhase: PhaseFailed, wantEpoch: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime, driver, collects, finish := startGatedCollection(t)
			if err := runtime.journal.update(func(state *journalState) error {
				record := state.Attempts["assignment-1"]
				tc.supersede(&record)
				state.Attempts["assignment-1"] = record
				state.Sequence++
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			finish()
			// The pass that takes the finished collection is reached through the
			// collect path directly: the superseding phase may be one no
			// reconcile routes to collection.
			stale := collectingRecord(t, driver.workspace)
			flight, err := runtime.collectOnce(context.Background(), "assignment-1", stale)
			if err != nil || flight == nil {
				t.Fatalf("flight=%v err=%v; want the finished collection", flight, err)
			}
			if err := runtime.finishCollection("assignment-1", stale, flight); err != nil {
				t.Fatal(err)
			}
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			record := attemptRecord(t, runtime)
			if record.Phase != tc.wantPhase || record.Assignment.Epoch != tc.wantEpoch || collects.Load() != 1 || runtime.collectionRegistered(stale) {
				t.Fatalf("phase=%q epoch=%d collections=%d registered=%v; want the result discarded and the record untouched",
					record.Phase, record.Assignment.Epoch, collects.Load(), runtime.collectionRegistered(stale))
			}
		})
	}
}

// A throttle command that arrives while an attempt is being collected is
// refused and leaves the collection alone. The field interleaving: a tick
// starts a collection and releases the host lock, the watchdog still sees the
// attempt running and sends a hard stop, and an accepted stop moved the
// attempt to stopped, where a throttled attempt is never collected again, so
// the finished collection stayed registered and refused every later resume.
func TestThrottleDuringCollectionIsRefused(t *testing.T) {
	for _, kind := range []domain.ThrottleCommandKind{
		domain.ThrottleCommandWarn, domain.ThrottleCommandDrain, domain.ThrottleCommandHardStop,
	} {
		t.Run(string(kind), func(t *testing.T) {
			runtime, driver, collects, finish := startGatedCollection(t)
			acks, err := runtime.DeliverThrottle(context.Background(), []domain.ThrottleCommand{testThrottle(runtime, kind, "throttle-1")})
			if err != nil || len(acks) != 1 {
				t.Fatalf("acks = %+v, err = %v", acks, err)
			}
			if acks[0].Accepted || !strings.Contains(acks[0].Error, "being collected") {
				t.Fatalf("acknowledgement = %+v; want the command refused because the attempt is being collected", acks[0])
			}
			if record := attemptRecord(t, runtime); record.Phase != PhaseCollecting || record.PendingThrottle != nil ||
				driver.stopCalls != 0 || driver.checkpointCalls != 0 {
				t.Fatalf("phase=%q pending=%v stops=%d checkpoints=%d; want the collection untouched",
					record.Phase, record.PendingThrottle, driver.stopCalls, driver.checkpointCalls)
			}
			finish()
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			record := attemptRecord(t, runtime)
			if record.Phase != PhaseCompleted || collects.Load() != 1 || runtime.collectionRegistered(record) {
				t.Fatalf("phase=%q collections=%d registered=%v; want the collection's result taken once and nothing left registered",
					record.Phase, collects.Load(), runtime.collectionRegistered(record))
			}
		})
	}
}

// A lease that lapsed on the worker's own clock while the attempt was being
// collected does not discard the result. The coordinator re-claims a lapsed
// assignment from an observation that shows it present, and fences a stale
// result by assignment epoch and lease token; the worker's copy of the lease
// is not that authority.
func TestLapsedLeaseDoesNotDiscardCollection(t *testing.T) {
	runtime, _, collects, finish := startGatedCollection(t)
	if err := runtime.journal.update(func(state *journalState) error {
		record := state.Attempts["assignment-1"]
		record.Assignment.LeaseExpiresAt = runtimeTestNow.Add(-time.Minute)
		state.Attempts["assignment-1"] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	finish()
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if record := attemptRecord(t, runtime); record.Phase != PhaseCompleted || collects.Load() != 1 {
		t.Fatalf("phase=%q collections=%d; want the result taken once", record.Phase, collects.Load())
	}
}
