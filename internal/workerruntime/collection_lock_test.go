package workerruntime

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// gatedCollectDriver is a driver whose collection, standing in for a long
// verification command, runs until the test releases it. Every collection
// counts on collects, which several drivers may share.
type gatedCollectDriver struct {
	*fakeDriver
	collects *atomic.Int32
	// started receives once for every collection that begins.
	started chan struct{}
	// release lets every collection, running or future, finish.
	release chan struct{}
	// inspecting and resume, when set, hold InspectWorkspace: the pass
	// announces itself on inspecting and waits for resume.
	inspecting chan struct{}
	resume     chan struct{}
}

func newGatedCollectDriver(workspace string, collects *atomic.Int32) *gatedCollectDriver {
	return &gatedCollectDriver{
		fakeDriver: &fakeDriver{workspace: workspace, workspaceReady: true},
		collects:   collects,
		started:    make(chan struct{}, 8),
		release:    make(chan struct{}),
	}
}

func (d *gatedCollectDriver) Collect(ctx context.Context, _ workerproto.ExecutionPackage, _ string) error {
	d.collects.Add(1)
	d.started <- struct{}{}
	select {
	case <-d.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *gatedCollectDriver) InspectWorkspace(ctx context.Context, pkg workerproto.ExecutionPackage) (string, bool, error) {
	if d.inspecting != nil {
		d.inspecting <- struct{}{}
		<-d.resume
	}
	return d.fakeDriver.InspectWorkspace(ctx, pkg)
}

// newGatedRuntime opens a runtime over the journal at root. Two runtimes over
// one root share the journal and the collection registry, as a persistent
// worker's old and replacement runtimes do.
func newGatedRuntime(t *testing.T, root string, driver *gatedCollectDriver) *Runtime {
	t.Helper()
	journal, err := OpenJournal(root, "normandy", "worker-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := New(testConfig(func() time.Time { return runtimeTestNow }), journal, driver)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

// collectingRecord is the attempt a worker restarted mid-collection finds in
// its journal: its turn is over and its collection has been claimed.
func collectingRecord(t *testing.T, workspace string) AttemptRecord {
	t.Helper()
	offer := testOffer(t)
	record := AttemptRecord{
		Assignment: offer.Assignment, Package: offer.Package, Phase: PhaseCollecting,
		WorkspacePath: workspace, ThreadID: "thread-1", UpdatedAt: runtimeTestNow,
	}
	record.Assignment.State = domain.AssignmentClaimed
	record.Assignment.WorkerEpoch = "worker-1"
	return record
}

func seedAttempt(t *testing.T, runtime *Runtime, record AttemptRecord) {
	t.Helper()
	if err := runtime.journal.update(func(state *journalState) error {
		state.Attempts[record.Assignment.ID] = record
		state.Sequence++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func receiveWithin(t *testing.T, ch <-chan struct{}, within time.Duration, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(within):
		t.Fatalf("%s did not happen within %s", what, within)
	}
}

func releaseOnce(d *gatedCollectDriver) func() {
	var once sync.Once
	return func() { once.Do(func() { close(d.release) }) }
}

// A coordinator exchange is answered while a collection's verification runs.
// The field failure (omarchy-pc, 2026-09-24) was every exchange of the worker
// queuing behind the reconcile pass that ran a verification under the host
// lock: exchanges timed out, leases were not renewed and expired, and nothing
// else on the worker got offers or results for about 35 minutes. The snapshot
// the exchange returns reports the attempt as the collection it is.
func TestExchangeIsAnsweredWhileCollectionRuns(t *testing.T) {
	ctx := context.Background()
	host, projection := catalogHostFixture(t)
	codec := workerproto.Codec{MaxBytes: 8 << 20}
	if _, err := host.HandleFrame(ctx, mustEncodeEnvelope(t, codec, catalogEnvelope(t, "catalog", CatalogRequest{Projection: projection}))); err != nil {
		t.Fatal(err)
	}
	runtime := host.service.Exchange.Runtime
	var collects atomic.Int32
	driver := newGatedCollectDriver(filepath.Join(t.TempDir(), "workspace"), &collects)
	release := releaseOnce(driver)
	t.Cleanup(release)
	runtime.driver = driver
	seedAttempt(t, runtime, collectingRecord(t, driver.workspace))

	reconciled := make(chan error, 1)
	go func() { reconciled <- host.Reconcile(ctx) }()
	receiveWithin(t, driver.started, 5*time.Second, "the collection")

	now := time.Now()
	snapshot, err := workerproto.NewEnvelope(workerproto.MessageSnapshot, "snapshot", "snapshot", "coordinator", "normandy", 9, "worker-1", 1, now, now.Add(time.Minute), workerproto.SnapshotRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if err = workerproto.SignEnvelope(&snapshot, "ssh:coordinator", "coordinator-key", []byte("coordinator-secret")); err != nil {
		t.Fatal(err)
	}
	type answer struct {
		raw []byte
		err error
	}
	answered := make(chan answer, 1)
	go func() {
		raw, err := host.HandleFrame(ctx, mustEncodeEnvelope(t, codec, snapshot))
		answered <- answer{raw, err}
	}()
	var got answer
	select {
	case got = <-answered:
	case <-time.After(3 * time.Second):
		release()
		t.Fatal("the snapshot exchange waited behind a running collection")
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	var response workerproto.Envelope
	if err := codec.Decode(bytes.NewReader(got.raw), &response); err != nil {
		t.Fatal(err)
	}
	var observations workerproto.Observations
	if err := workerproto.DecodePayload(response, workerproto.MessageObservations, &observations); err != nil {
		t.Fatal(err)
	}
	if len(observations.Snapshot.Assignments) != 1 {
		t.Fatalf("snapshot assignments = %+v; want the attempt being collected", observations.Snapshot.Assignments)
	}
	observed := observations.Snapshot.Assignments[0]
	if observed.State != domain.AssignmentClaimed || observed.Journal == nil || observed.Journal.Phase != string(PhaseCollecting) {
		t.Fatalf("observation = %+v journal = %+v; want a claimed attempt that is collecting", observed, observed.Journal)
	}
	// The lease of an attempt being collected is still renewed.
	renewals, err := runtime.LeaseRenewals()
	if err != nil || len(renewals.Renewals) != 1 {
		t.Fatalf("renewals = %+v, err = %v; want the collecting attempt renewed", renewals, err)
	}

	release()
	select {
	case err := <-reconciled:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reconcile did not finish after the collection did")
	}
	// The reconcile that started the collection took its result once it
	// finished, with the host lock released in between.
	if record := attemptRecord(t, runtime); record.Phase != PhaseCompleted || collects.Load() != 1 {
		t.Fatalf("phase=%q collections=%d; want one collection, completed", record.Phase, collects.Load())
	}
}

// Two passes that reach the same attempt at once collect it exactly once,
// even when they are not serialized: one pass read the attempt as collecting
// before the other started, finished and recorded the collection. That is the
// situation of a persistent worker whose runtime is replaced while a pass of
// the old one is still running.
func TestConcurrentPassesCollectOnce(t *testing.T) {
	root := t.TempDir()
	var collects atomic.Int32
	workspace := filepath.Join(root, "workspace")
	firstDriver := newGatedCollectDriver(workspace, &collects)
	close(firstDriver.release)
	first := newGatedRuntime(t, root, firstDriver)
	secondDriver := newGatedCollectDriver(workspace, &collects)
	close(secondDriver.release)
	secondDriver.inspecting = make(chan struct{}, 1)
	secondDriver.resume = make(chan struct{})
	second := newGatedRuntime(t, root, secondDriver)
	seedAttempt(t, first, collectingRecord(t, workspace))

	// The second pass reads the attempt as collecting, with no collection
	// registered, and is held there.
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Reconcile(context.Background()) }()
	receiveWithin(t, secondDriver.inspecting, 5*time.Second, "the second pass")

	// The first pass collects the attempt and records the result.
	if err := first.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if record := attemptRecord(t, first); record.Phase != PhaseCompleted {
		t.Fatalf("phase after the first pass = %q; want completed", record.Phase)
	}

	close(secondDriver.resume)
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the second pass did not finish")
	}
	record := attemptRecord(t, first)
	if collects.Load() != 1 || record.Phase != PhaseCompleted || first.collectionRegistered(record) {
		t.Fatalf("collections=%d phase=%q registered=%v; want one collection and nothing left registered",
			collects.Load(), record.Phase, first.collectionRegistered(record))
	}
}
