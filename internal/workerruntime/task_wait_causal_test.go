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

// acknowledgeStoppedSnapshot models the next request built after the
// coordinator persisted a worker snapshot containing the stopped observation.
func acknowledgeStoppedSnapshot(t *testing.T, runtime *Runtime, driver *fakeDriver) {
	t.Helper()
	driver.observations = []backlog.DispatchThreadState{backlog.DispatchThreadStopped}
	snapshot, err := runtime.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{
		ParkedReported: true, ObservedWorkerEpoch: snapshot.WorkerEpoch, ObservedSequence: snapshot.Sequence,
	}); err != nil {
		t.Fatal(err)
	}
	driver.observations = []backlog.DispatchThreadState{backlog.DispatchThreadStopped}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestUnobservedResumeRequiresANewStoppedAcknowledgement(t *testing.T) {
	root := t.TempDir()
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadStopped}}
	runtime := signalRuntime(t, root, driver, func() time.Time { return runtimeTestNow })
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ApplyParkedAssignments(parkedStatement(5)); err != nil {
		t.Fatal(err)
	}
	old, err := runtime.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	fence := mustRecord(t, runtime, "assignment-1").StopObservedSequence
	// The resumed turn starts and stops between polls. An empty report whose
	// acknowledgement covers only the original parked turn cannot collect it.
	if err := runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{
		ParkedReported: true, ObservedWorkerEpoch: old.WorkerEpoch, ObservedSequence: old.Sequence,
	}); err != nil {
		t.Fatal(err)
	}
	driver.observations = []backlog.DispatchThreadState{backlog.DispatchThreadStopped}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.collectCalls != 0 || mustRecord(t, runtime, "assignment-1").StopObservedSequence <= fence {
		t.Fatal("the unseen resumed turn reused the parked turn's collection authority")
	}
	acknowledgeStoppedSnapshot(t, runtime, driver)
	if driver.collectCalls != 1 {
		t.Fatalf("final collection count=%d", driver.collectCalls)
	}
}

func TestThrottleResumeCannotReuseStoppedAcknowledgement(t *testing.T) {
	root := t.TempDir()
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadStopped}}
	runtime := signalRuntime(t, root, driver, func() time.Time { return runtimeTestNow })
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ApplyParkedAssignments(emptyStatement()); err != nil {
		t.Fatal(err)
	}
	old, err := runtime.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{
		ParkedReported: true, ObservedWorkerEpoch: old.WorkerEpoch, ObservedSequence: old.Sequence,
	}); err != nil {
		t.Fatal(err)
	}
	// Resume starts a new turn, which can register a wait and stop before the
	// next snapshot. Its predecessor's acknowledged stop cannot authorize it.
	acks, err := runtime.DeliverThrottle(context.Background(), []domain.ThrottleCommand{
		testThrottle(runtime, domain.ThrottleCommandResume, "resume"),
	})
	if err != nil || len(acks) != 1 || !acks[0].Accepted {
		t.Fatalf("resume: %+v %v", acks, err)
	}
	driver.observations = []backlog.DispatchThreadState{backlog.DispatchThreadStopped}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.collectCalls != 0 {
		t.Fatal("throttle resume reused the old turn's acknowledged stop")
	}
	acknowledgeStoppedSnapshot(t, runtime, driver)
	// A task with throttle history awaits the coordinator's collect command.
	if _, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{
		Commands: []domain.WorkerCommand{testCommand(t, runtime, domain.WorkerCommandCollect, "collect-after-resume")},
	}); err != nil {
		t.Fatal(err)
	}
	if driver.collectCalls != 1 {
		t.Fatalf("final collection count=%d", driver.collectCalls)
	}
}

// Registration can commit after the coordinator builds its empty statement,
// then the turn ends before that statement arrives. TTL freshness proves nothing.
func TestFreshPreStopEmptyStatementCannotAuthorizeCollection(t *testing.T) {
	root := t.TempDir()
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadStopped}}
	runtime := signalRuntime(t, root, driver, func() time.Time { return runtimeTestNow })
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ApplyParkedAssignments(emptyStatement()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.collectCalls != 0 {
		t.Fatalf("fresh pre-stop empty report authorized %d collections", driver.collectCalls)
	}
	fence := mustRecord(t, runtime, "assignment-1").StopObservedSequence
	if fence == 0 {
		t.Fatal("stopped observation was not durably fenced")
	}
	// A restart retains the stopped fence; wrong epoch, stale ack and no ack
	// cannot turn that stopped task into a collected one.
	restarted := signalRuntime(t, root, driver, func() time.Time { return runtimeTestNow })
	for _, ack := range []workerproto.SnapshotRequest{
		emptyStatement(),
		{ParkedReported: true, ObservedWorkerEpoch: "worker-1", ObservedSequence: fence - 1},
		{ParkedReported: true, ObservedWorkerEpoch: "other-worker-epoch", ObservedSequence: fence},
	} {
		if err := restarted.ApplyParkedAssignments(ack); err != nil {
			t.Fatal(err)
		}
		driver.observations = []backlog.DispatchThreadState{backlog.DispatchThreadStopped}
		if err := restarted.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		if driver.collectCalls != 0 {
			t.Fatalf("stale ack authorized collection: %+v", ack)
		}
	}
	acknowledgeStoppedSnapshot(t, restarted, driver)
	if driver.collectCalls != 1 {
		t.Fatalf("causal ack failed to permit collection: %d", driver.collectCalls)
	}
}
