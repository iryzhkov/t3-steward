package workerruntime

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

var exchangeSessions atomic.Int64

// exchangeAnswersWithin serves one snapshot exchange on host and fails the
// test when it is not answered within the bound, which callers scale with
// testtiming.Bound.
func exchangeAnswersWithin(t *testing.T, host *CatalogHost, within time.Duration, unblock func()) {
	t.Helper()
	id := fmt.Sprintf("snapshot-%d", exchangeSessions.Add(1))
	now := time.Now()
	envelope, err := workerproto.NewEnvelope(workerproto.MessageSnapshot, id, id, "coordinator", "normandy", 9, "worker-1", 1, now, now.Add(time.Minute), workerproto.SnapshotRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if err = workerproto.SignEnvelope(&envelope, "ssh:coordinator", "coordinator-key", []byte("coordinator-secret")); err != nil {
		t.Fatal(err)
	}
	frame := mustEncodeEnvelope(t, workerproto.Codec{MaxBytes: 8 << 20}, envelope)
	answered := make(chan error, 1)
	go func() {
		_, err := host.HandleFrame(context.Background(), frame)
		answered <- err
	}()
	select {
	case err := <-answered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(within):
		unblock()
		t.Fatal("the snapshot exchange waited behind a T3 call made by the reconcile tick")
	}
}

func reconcileFinishes(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testtiming.Bound(15 * time.Second)):
		t.Fatal("reconcile did not finish")
	}
}

func hostWithDriver(t *testing.T, driver Driver) (*CatalogHost, *Runtime) {
	t.Helper()
	host, projection := catalogHostFixture(t)
	if _, err := host.HandleFrame(context.Background(), mustEncodeEnvelope(t, workerproto.Codec{MaxBytes: 8 << 20}, catalogEnvelope(t, "catalog", CatalogRequest{Projection: projection}))); err != nil {
		t.Fatal(err)
	}
	runtime := host.service.Exchange.Runtime
	runtime.driver = driver
	return host, runtime
}

// slowPauseDriver is a driver whose quota-pause completion check, which asks
// T3 for the drained turn, its last message and its export, is slow.
type slowPauseDriver struct {
	*gatedCollectDriver
	checking chan struct{}
	answer   chan bool
}

func (d *slowPauseDriver) ObserveThread(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, error) {
	return backlog.DispatchThreadStopped, nil
}

func (d *slowPauseDriver) ObserveThreadTurn(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, string, error) {
	return backlog.DispatchThreadStopped, "turn-1", nil
}

func (d *slowPauseDriver) QuotaPauseCompleted(ctx context.Context, _ workerproto.ExecutionPackage, turn string) (bool, error) {
	d.checking <- struct{}{}
	select {
	case completed := <-d.answer:
		return completed && turn == "turn-1", nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// The completion check of a quota-paused attempt runs without the host lock,
// and while it is pending nothing resumes the attempt: the drained turn may
// have finished the task, and resuming it would start a filler turn.
func TestSlowPauseCompletionCheckDoesNotBlockAnExchange(t *testing.T) {
	var collects atomic.Int32
	inner := newGatedCollectDriver(filepath.Join(t.TempDir(), "workspace"), &collects)
	close(inner.release)
	driver := &slowPauseDriver{gatedCollectDriver: inner, checking: make(chan struct{}, 8), answer: make(chan bool, 8)}
	host, runtime := hostWithDriver(t, driver)
	record := stoppedRecord(t, inner.workspace)
	record.LocalThrottle = &LocalThrottleRequest{Kind: domain.ThrottleCommandDrain, StoppedTurnID: "turn-1", Reason: "quota", RequestedAt: runtimeTestNow}
	seedAttempt(t, runtime, record)

	done := make(chan error, 1)
	go func() { done <- host.Reconcile(context.Background()) }()
	receiveWithin(t, driver.checking, 5*time.Second, "the completion check")
	exchangeAnswersWithin(t, host, testtiming.Bound(3*time.Second), func() { driver.answer <- false })
	driver.answer <- true
	reconcileFinishes(t, done)
	// The collection that follows the completion needs its own observations;
	// the next tick finishes it.
	if err := host.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := attemptRecord(t, runtime)
	if got.Phase != PhaseCompleted || collects.Load() != 1 || inner.resumeCalls != 0 {
		t.Fatalf("phase=%q collections=%d resumes=%d; want the drained turn collected once and never resumed",
			got.Phase, collects.Load(), inner.resumeCalls)
	}
}

// failingPauseDriver's completion check times out the first time it is
// asked, as a probe cut off by its timeout or the tick's budget does, and
// reports the drained turn completed afterwards.
type failingPauseDriver struct {
	*gatedCollectDriver
	checks atomic.Int32
}

func (d *failingPauseDriver) ObserveThread(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, error) {
	return backlog.DispatchThreadStopped, nil
}

func (d *failingPauseDriver) ObserveThreadTurn(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, string, error) {
	return backlog.DispatchThreadStopped, "turn-1", nil
}

func (d *failingPauseDriver) QuotaPauseCompleted(context.Context, workerproto.ExecutionPackage, string) (bool, error) {
	if d.checks.Add(1) == 1 {
		return false, context.DeadlineExceeded
	}
	return true, nil
}

// A completion check that did not answer is not "not completed": the
// drained turn may have finished the task, so the attempt is not resumed on
// that tick even though its quota has recovered. Once the check answers, the
// finished turn is collected and never resumed.
func TestUnansweredPauseCompletionCheckDoesNotResume(t *testing.T) {
	var collects atomic.Int32
	inner := newGatedCollectDriver(filepath.Join(t.TempDir(), "workspace"), &collects)
	close(inner.release)
	driver := &failingPauseDriver{gatedCollectDriver: inner}
	host, runtime := hostWithDriver(t, driver)
	guard := &fakeQuotaGuard{resumeOK: true, resumeWhy: "bucket recovered"}
	runtime.config.Quota = guard
	record := stoppedRecord(t, inner.workspace)
	record.Assignment.LeaseExpiresAt = time.Now().Add(time.Hour)
	record.Package.Package.Timeout = 0
	record.LocalThrottle = &LocalThrottleRequest{Kind: domain.ThrottleCommandDrain, StoppedTurnID: "turn-1", Reason: "quota", RequestedAt: runtimeTestNow}
	seedAttempt(t, runtime, record)

	if err := host.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.checks.Load() != 1 || inner.resumeCalls != 0 || guard.resumeAsked != 0 {
		t.Fatalf("checks=%d resumes=%d resume-asked=%d; want the unanswered check to hold the resume",
			driver.checks.Load(), inner.resumeCalls, guard.resumeAsked)
	}
	for tick := 0; tick < 2; tick++ {
		if err := host.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	got := attemptRecord(t, runtime)
	if got.Phase != PhaseCompleted || collects.Load() != 1 || inner.resumeCalls != 0 {
		t.Fatalf("phase=%q collections=%d resumes=%d; want the finished turn collected once and never resumed",
			got.Phase, collects.Load(), inner.resumeCalls)
	}
}

// The turn identity that fences a quota pause is observed without the host
// lock too, and still lands on the pause once it answers.
func TestSlowPauseTurnObservationDoesNotBlockAnExchange(t *testing.T) {
	var collects atomic.Int32
	driver := newSlowTurnDriver(filepath.Join(t.TempDir(), "workspace"), &collects)
	host, runtime := hostWithDriver(t, driver)
	record := stoppedRecord(t, driver.workspace)
	record.Phase = PhaseRunning
	record.LocalThrottle = &LocalThrottleRequest{Kind: domain.ThrottleCommandDrain, Reason: "quota", RequestedAt: runtimeTestNow}
	seedAttempt(t, runtime, record)

	done := make(chan error, 1)
	go func() { done <- host.Reconcile(context.Background()) }()
	receiveWithin(t, driver.observing, 5*time.Second, "the turn observation")
	exchangeAnswersWithin(t, host, testtiming.Bound(3*time.Second), func() { driver.answer <- "turn-7" })
	driver.answer <- "turn-7"
	reconcileFinishes(t, done)
	got := attemptRecord(t, runtime)
	if got.Phase != PhaseStopped || got.LocalThrottle == nil || got.LocalThrottle.StoppedTurnID != "turn-7" || collects.Load() != 0 {
		t.Fatalf("phase=%q pause=%+v collections=%d; want the pause fenced on turn-7 and nothing collected",
			got.Phase, got.LocalThrottle, collects.Load())
	}
}

// blockingListT3 is a T3 whose thread listing blocks until released.
type blockingListT3 struct {
	recordingT3
	lists   atomic.Int32
	listing chan struct{}
	release chan struct{}
}

func (b *blockingListT3) ListThreads(ctx context.Context) ([]domain.Thread, error) {
	if b.lists.Add(1) == 1 {
		b.listing <- struct{}{}
		<-b.release
	}
	return b.recordingT3.ListThreads(ctx)
}

// A slow thread listing does not hold the cache's lock: a mutation that
// invalidates the cache meanwhile is not kept waiting, and the listing that
// was in flight across the invalidation is not kept as the cached state.
func TestSlowListingDoesNotBlockCacheInvalidation(t *testing.T) {
	inner := &blockingListT3{recordingT3: recordingT3{thread: &domain.Thread{ID: "thread-1"}},
		listing: make(chan struct{}, 1), release: make(chan struct{})}
	cache := NewCachedT3(inner)
	read := make(chan error, 1)
	go func() {
		_, err := cache.GetThread(context.Background(), "thread-1")
		read <- err
	}()
	receiveWithin(t, inner.listing, 5*time.Second, "the listing")
	invalidated := make(chan struct{})
	go func() {
		cache.invalidate()
		close(invalidated)
	}()
	select {
	case <-invalidated:
	case <-time.After(testtiming.Bound(2 * time.Second)):
		close(inner.release)
		t.Fatal("invalidation waited behind a slow thread listing")
	}
	close(inner.release)
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	if _, err := cache.GetThread(context.Background(), "thread-1"); err != nil {
		t.Fatal(err)
	}
	if got := inner.lists.Load(); got != 2 {
		t.Fatalf("listings = %d; want the one overtaken by the invalidation not cached", got)
	}
}

// stuckTurnDriver never answers a turn observation until its context ends,
// and records which assignments were asked about.
type stuckTurnDriver struct {
	*gatedCollectDriver
	mu    sync.Mutex
	asked map[string]int
}

func (d *stuckTurnDriver) ObserveThread(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, error) {
	return backlog.DispatchThreadStopped, nil
}

func (d *stuckTurnDriver) ObserveThreadTurn(ctx context.Context, pkg workerproto.ExecutionPackage) (backlog.DispatchThreadState, string, error) {
	d.mu.Lock()
	d.asked[pkg.Identity.AttemptID]++
	d.mu.Unlock()
	<-ctx.Done()
	return "", "", ctx.Err()
}

func (d *stuckTurnDriver) askedAbout() map[string]int {
	d.mu.Lock()
	defer d.mu.Unlock()
	copied := make(map[string]int, len(d.asked))
	for id, n := range d.asked {
		copied[id] = n
	}
	return copied
}

// Many attempts whose T3 observations hang cannot hold a tick for long or
// starve one another: a tick's probes are bounded in total, run a few at a
// time, and start from a different attempt on every tick, so each attempt is
// asked about within a few ticks. The task-wait probe is not consulted for an
// attempt whose turn observation failed, since the decision is deferred
// anyway.
func TestHungObservationsAreBoundedAndEveryAttemptIsProbed(t *testing.T) {
	defer setProbeLimits(t, 300*time.Millisecond, 400*time.Millisecond, 2)()
	var collects atomic.Int32
	inner := newGatedCollectDriver(filepath.Join(t.TempDir(), "workspace"), &collects)
	close(inner.release)
	driver := &stuckTurnDriver{gatedCollectDriver: inner, asked: map[string]int{}}
	host, runtime := hostWithDriver(t, driver)
	var waitProbes atomic.Int32
	runtime.config.LiveTaskWait = func(context.Context, workerproto.ExecutionPackage) (bool, error) {
		waitProbes.Add(1)
		return false, nil
	}
	const attempts = 6
	for i := range attempts {
		record := stoppedRecord(t, inner.workspace)
		record.Assignment.ID = fmt.Sprintf("assignment-%d", i)
		record.Assignment.AttemptID = fmt.Sprintf("attempt-%d", i)
		record.Package.Package.Identity.AttemptID = record.Assignment.AttemptID
		record.Package.Package.Identity.AssignmentID = record.Assignment.ID
		seedAttempt(t, runtime, record)
	}
	for tick := 0; tick < 4; tick++ {
		done := make(chan error, 1)
		started := time.Now()
		go func() { done <- host.Reconcile(context.Background()) }()
		exchangeAnswersWithin(t, host, testtiming.Bound(2*time.Second), func() {})
		reconcileFinishes(t, done)
		if elapsed := time.Since(started); elapsed > testtiming.Bound(1200*time.Millisecond) {
			t.Fatalf("tick %d took %s; want the probe budget to bound it", tick, elapsed)
		}
	}
	asked := driver.askedAbout()
	for i := range attempts {
		if asked[fmt.Sprintf("attempt-%d", i)] == 0 {
			t.Fatalf("attempt-%d was never probed in 4 ticks: %v", i, asked)
		}
	}
	if got := waitProbes.Load(); got != 0 {
		t.Fatalf("task-wait probes = %d; want none after failed turn observations", got)
	}
}

// countingTurnDriver answers turn observations at once and counts them.
type countingTurnDriver struct {
	*fakeDriver
	turns atomic.Int32
}

func (d *countingTurnDriver) ObserveThreadTurn(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, string, error) {
	d.turns.Add(1)
	return backlog.DispatchThreadStopped, "turn-1", nil
}

// A request whose attempt moved before the probes ran is replaced, not
// queued a second time: one driver call answers the latest state.
func TestReplacedProbeRequestIsProbedOnceForItsLatestKey(t *testing.T) {
	driver := &countingTurnDriver{fakeDriver: &fakeDriver{}}
	root := t.TempDir()
	journal, err := OpenJournal(root, "normandy", "worker-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := New(testConfig(func() time.Time { return runtimeTestNow }), journal, driver)
	if err != nil {
		t.Fatal(err)
	}
	probes := &t3Probes{}
	record := stoppedRecord(t, "workspace")
	if _, ready := probes.take(runtime, probeCollection, record); ready {
		t.Fatal("an unasked probe was ready")
	}
	moved := record
	moved.Phase = PhaseWaiting
	if _, ready := probes.take(runtime, probeCollection, moved); ready {
		t.Fatal("an unasked probe was ready")
	}
	if !probes.run(context.Background(), 0) {
		t.Fatal("nothing was probed")
	}
	if got := driver.turns.Load(); got != 1 {
		t.Fatalf("turn observations = %d; want one for the replaced request", got)
	}
	if answer, ready := probes.take(runtime, probeCollection, moved); !ready || answer.turnID != "turn-1" {
		t.Fatalf("answer for the latest key = %+v ready=%v", answer, ready)
	}
	if _, ready := probes.take(runtime, probeCollection, record); ready {
		t.Fatal("the superseded key was answered")
	}
}

// setProbeLimits shortens the probe bounds for one test and returns the
// function that restores them.
func setProbeLimits(t *testing.T, perProbe, budget time.Duration, concurrency int) func() {
	t.Helper()
	savedProbe, savedBudget, savedConcurrency := t3ProbeTimeout, t3ProbeBudget, t3ProbeConcurrency
	t3ProbeTimeout, t3ProbeBudget, t3ProbeConcurrency = perProbe, budget, concurrency
	return func() { t3ProbeTimeout, t3ProbeBudget, t3ProbeConcurrency = savedProbe, savedBudget, savedConcurrency }
}
