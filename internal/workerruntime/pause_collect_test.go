package workerruntime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// RC1 field test: a task was quota-paused by the coordinator, resumed, and its
// resumed turn (the one that received the answer) finished. The first stopped
// observation only arms the collection fence, which costs one exchange, so the
// attempt rests in the stopped phase for a pass. From there the throttle
// history, which is never cleared, stopped every later observation and made
// the attempt report paused forever: nothing was collected and nothing
// verified. A resumed attempt whose turn ended is finished work.
func TestAResumedAttemptWhoseTurnEndedIsCollectedNotLeftPaused(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true}
	now := runtimeTestNow
	runtime := signalRuntime(t, root, driver, func() time.Time { return now })
	if _, err := runtime.AcceptOffers(ctx, workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	for _, command := range []domain.ThrottleCommand{
		testThrottle(runtime, domain.ThrottleCommandDrain, "drain-1"),
		testThrottle(runtime, domain.ThrottleCommandResume, "resume-1"),
	} {
		command.WorkspacePath = driver.workspace
		now = now.Add(time.Minute)
		if acks, err := runtime.DeliverThrottle(ctx, []domain.ThrottleCommand{command}); err != nil || !acks[0].Accepted {
			t.Fatalf("%s: %+v %v", command.ID, acks, err)
		}
	}
	// The coordinator reports parks, so the first stopped observation arms
	// the fence and the collection waits for the next acknowledged exchange.
	if err := runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ParkedReported: true}); err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 3; pass++ {
		driver.observations = []backlog.DispatchThreadState{backgroundStopped, backgroundStopped, backgroundStopped}
		if err := runtime.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		acknowledgeLatestSnapshot(t, runtime)
	}
	record := journalRecord(t, runtime)
	if got := observation(record, runtimeTestNow.Add(time.Minute), false).Control; got == domain.ControlPaused {
		t.Fatalf("a resumed attempt whose turn ended reports %q in phase %q", got, record.Phase)
	}
	if driver.collectCalls != 1 {
		t.Fatalf("collected %d times, want once (phase %q)", driver.collectCalls, record.Phase)
	}
}

const backgroundStopped = backlog.DispatchThreadStopped

// acknowledgeLatestSnapshot models the coordinator persisting the worker's
// latest snapshot and acknowledging it in its next request.
func acknowledgeLatestSnapshot(t *testing.T, runtime *Runtime) {
	t.Helper()
	snapshot, err := runtime.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{
		ParkedReported: true, ObservedWorkerEpoch: snapshot.WorkerEpoch, ObservedSequence: snapshot.Sequence,
	}); err != nil {
		t.Fatal(err)
	}
}
