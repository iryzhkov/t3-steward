package workerruntime

import (
	"bytes"
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// slowTurnDriver stands in for a T3 whose turn observation is slow: every
// ObserveThreadTurn announces itself on observing and blocks until the test
// sends the answer on answer. The plain observation always reports the thread
// stopped, so a stopped attempt reaches the collection decision.
type slowTurnDriver struct {
	*gatedCollectDriver
	observing chan struct{}
	answer    chan string
	turnCalls atomic.Int32
}

func newSlowTurnDriver(workspace string, collects *atomic.Int32) *slowTurnDriver {
	inner := newGatedCollectDriver(workspace, collects)
	close(inner.release)
	return &slowTurnDriver{gatedCollectDriver: inner, observing: make(chan struct{}, 8), answer: make(chan string, 8)}
}

func (d *slowTurnDriver) ObserveThread(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, error) {
	return backlog.DispatchThreadStopped, nil
}

func (d *slowTurnDriver) ObserveThreadTurn(ctx context.Context, _ workerproto.ExecutionPackage) (backlog.DispatchThreadState, string, error) {
	d.turnCalls.Add(1)
	d.observing <- struct{}{}
	select {
	case turn := <-d.answer:
		return backlog.DispatchThreadStopped, turn, nil
	case <-ctx.Done():
		return "", "", ctx.Err()
	}
}

// stoppedRecord is an attempt whose turn has ended and which nothing has
// collected yet: the next pass decides whether to collect it, which needs the
// provider turn identity.
func stoppedRecord(t *testing.T, workspace string) AttemptRecord {
	t.Helper()
	record := collectingRecord(t, workspace)
	record.Phase = PhaseStopped
	return record
}

func slowTurnHost(t *testing.T) (*CatalogHost, *Runtime, *slowTurnDriver, *atomic.Int32) {
	t.Helper()
	host, projection := catalogHostFixture(t)
	codec := workerproto.Codec{MaxBytes: 8 << 20}
	if _, err := host.HandleFrame(context.Background(), mustEncodeEnvelope(t, codec, catalogEnvelope(t, "catalog", CatalogRequest{Projection: projection}))); err != nil {
		t.Fatal(err)
	}
	runtime := host.service.Exchange.Runtime
	var collects atomic.Int32
	driver := newSlowTurnDriver(filepath.Join(t.TempDir(), "workspace"), &collects)
	runtime.driver = driver
	seedAttempt(t, runtime, stoppedRecord(t, driver.workspace))
	return host, runtime, driver, &collects
}

// A coordinator exchange is answered while a reconcile pass waits for a slow
// T3 turn observation. The observation used to run under the host lock every
// exchange needs, so one slow T3 call stalled every snapshot, offer and lease
// renewal of the worker for as long as the exchange timeout allowed. The
// observation now runs with the lock released, and its answer is acted on by
// a second pass that takes the lock again.
func TestSlowTurnObservationDoesNotBlockAnExchange(t *testing.T) {
	ctx := context.Background()
	host, runtime, driver, collects := slowTurnHost(t)

	reconciled := make(chan error, 1)
	go func() { reconciled <- host.Reconcile(ctx) }()
	receiveWithin(t, driver.observing, 5*time.Second, "the turn observation")

	type answer struct {
		raw []byte
		err error
	}
	answered := make(chan answer, 1)
	go func() {
		raw, err := host.HandleFrame(ctx, snapshotFrame(t))
		answered <- answer{raw, err}
	}()
	var got answer
	select {
	case got = <-answered:
	case <-time.After(3 * time.Second):
		driver.answer <- "turn-1"
		t.Fatal("the snapshot exchange waited behind a slow T3 turn observation")
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	var response workerproto.Envelope
	if err := (workerproto.Codec{MaxBytes: 8 << 20}).Decode(bytes.NewReader(got.raw), &response); err != nil {
		t.Fatal(err)
	}
	var observations workerproto.Observations
	if err := workerproto.DecodePayload(response, workerproto.MessageObservations, &observations); err != nil {
		t.Fatal(err)
	}
	if len(observations.Snapshot.Assignments) != 1 || observations.Snapshot.Assignments[0].State != domain.AssignmentClaimed {
		t.Fatalf("snapshot assignments = %+v; want the stopped attempt, still claimed", observations.Snapshot.Assignments)
	}
	// The exchange made no T3 turn observation of its own: it defers the
	// collection decision to the reconcile tick instead of waiting for T3
	// under the lock.
	if calls := driver.turnCalls.Load(); calls != 1 {
		t.Fatalf("turn observations = %d; want only the reconcile pass's one", calls)
	}

	driver.answer <- "turn-1"
	select {
	case err := <-reconciled:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("reconcile did not finish after the observation answered")
	}
	record := attemptRecord(t, runtime)
	if record.Phase != PhaseCompleted || collects.Load() != 1 || record.ObservedTurnID != "turn-1" {
		t.Fatalf("phase=%q collections=%d turn=%q; want the observed turn collected once", record.Phase, collects.Load(), record.ObservedTurnID)
	}
}

// An observation that went stale while the lock was released is discarded.
// Here the assignment is superseded by a higher epoch while T3 is answering
// for the old execution: that answer must not authorize collecting the new
// one, and nothing it says is written into the new record. The new execution
// is observed afresh, and only that observation decides.
func TestTurnObservationStaleAfterUnlockIsDiscarded(t *testing.T) {
	ctx := context.Background()
	host, runtime, driver, collects := slowTurnHost(t)

	reconciled := make(chan error, 1)
	go func() { reconciled <- host.Reconcile(ctx) }()
	receiveWithin(t, driver.observing, 5*time.Second, "the turn observation")

	superseding := stoppedRecord(t, driver.workspace)
	superseding.Assignment.Epoch++
	seedAttempt(t, runtime, superseding)

	driver.answer <- "turn-of-the-old-execution"
	driver.answer <- "turn-of-the-new-execution"
	select {
	case err := <-reconciled:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("reconcile did not finish after the observation answered")
	}
	record := attemptRecord(t, runtime)
	if record.ObservedTurnID != "turn-of-the-new-execution" || driver.turnCalls.Load() != 2 || record.Assignment.Epoch != superseding.Assignment.Epoch {
		t.Fatalf("turn=%q observations=%d epoch=%d; want the stale answer discarded and the new execution observed afresh",
			record.ObservedTurnID, driver.turnCalls.Load(), record.Assignment.Epoch)
	}
	if record.Phase == PhaseCompleted && collects.Load() != 1 {
		t.Fatalf("phase=%q collections=%d; want at most the new execution's one collection", record.Phase, collects.Load())
	}
}

// A slow workspace inspection of a preparing attempt does not hold the lock
// either, and its answer still advances the attempt in the same tick.
func TestSlowWorkspaceInspectionDoesNotBlockAnExchange(t *testing.T) {
	ctx := context.Background()
	host, projection := catalogHostFixture(t)
	if _, err := host.HandleFrame(ctx, mustEncodeEnvelope(t, workerproto.Codec{MaxBytes: 8 << 20}, catalogEnvelope(t, "catalog", CatalogRequest{Projection: projection}))); err != nil {
		t.Fatal(err)
	}
	runtime := host.service.Exchange.Runtime
	var collects atomic.Int32
	driver := newGatedCollectDriver(filepath.Join(t.TempDir(), "workspace"), &collects)
	close(driver.release)
	driver.inspecting = make(chan struct{}, 4)
	driver.resume = make(chan struct{})
	runtime.driver = driver
	record := collectingRecord(t, driver.workspace)
	record.Phase = PhasePreparing
	seedAttempt(t, runtime, record)

	reconciled := make(chan error, 1)
	go func() { reconciled <- host.Reconcile(ctx) }()
	receiveWithin(t, driver.inspecting, 5*time.Second, "the workspace inspection")

	answered := make(chan error, 1)
	go func() {
		_, err := host.HandleFrame(ctx, snapshotFrame(t))
		answered <- err
	}()
	select {
	case err := <-answered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		close(driver.resume)
		t.Fatal("the snapshot exchange waited behind a slow workspace inspection")
	}
	close(driver.resume)
	select {
	case err := <-reconciled:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("reconcile did not finish after the inspection answered")
	}
	if got := attemptRecord(t, runtime); got.Phase != PhasePrepared {
		t.Fatalf("phase = %q; want prepared from the inspection's answer", got.Phase)
	}
}
