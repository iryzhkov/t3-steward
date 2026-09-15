package workerruntime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// signalRuntime builds a runtime on the production path: no LiveTaskWait
// override, so the only thing it knows about parked assignments is what the
// coordinator told it over the worker protocol.
func signalRuntime(t *testing.T, root string, driver *fakeDriver, now func() time.Time) *Runtime {
	t.Helper()
	journal, err := OpenJournal(root, "normandy", "worker-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := New(testConfig(now), journal, driver)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func parkedStatement(revision int64) workerproto.SnapshotRequest {
	return workerproto.SnapshotRequest{ParkedReported: true, Parked: []workerproto.ParkedAssignment{{
		AssignmentID: "assignment-1", AssignmentEpoch: 2,
		AttemptID: "attempt-1", AttemptRevision: revision, WaitID: "tw-ci",
	}}}
}

func emptyStatement() workerproto.SnapshotRequest {
	return workerproto.SnapshotRequest{ParkedReported: true}
}

// The coordinator tells the worker which assignments are parked, in the
// exchange the worker already makes. The worker does not collect a parked
// attempt, resumes with it when told it is no longer parked, and verifies once.
func TestWorkerLearnsParkedAssignmentsFromTheCoordinatorStatement(t *testing.T) {
	root := t.TempDir()
	now := runtimeTestNow
	driver := &fakeDriver{
		workspace: filepath.Join(root, "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{
			backlog.DispatchThreadStopped, // the turn parks
			backlog.DispatchThreadActive,  // the wake resumed the same thread
			backlog.DispatchThreadStopped, // the resumed turn ends
		},
	}
	runtime := signalRuntime(t, root, driver, func() time.Time { return now })
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}

	if err := runtime.ApplyParkedAssignments(parkedStatement(5)); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := phaseOf(t, runtime, "assignment-1"); got != PhaseWaiting {
		t.Fatalf("the coordinator statement did not park the attempt: phase=%q", got)
	}
	if driver.collectCalls != 0 {
		t.Fatalf("a parked attempt was collected: %d", driver.collectCalls)
	}

	// The coordinator resumed the attempt, so its next statement omits it.
	if err := runtime.ApplyParkedAssignments(emptyStatement()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := phaseOf(t, runtime, "assignment-1"); got != PhaseRunning {
		t.Fatalf("the woken thread did not resume: phase=%q", got)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	acknowledgeStoppedSnapshot(t, runtime, driver)
	if got := phaseOf(t, runtime, "assignment-1"); got != PhaseCompleted || driver.collectCalls != 1 {
		t.Fatalf("the resumed turn produced %d collections in phase %q, want exactly one", driver.collectCalls, got)
	}
}

// A worker that restarts mid-wait must not collect before it has heard from the
// coordinator again. Its durable record says a statement exists but is stale,
// and stale is unknown, and unknown defers. The next exchange refreshes it, and
// the attempt still verifies exactly once after the wake.
func TestParkedStatementSurvivesRestartAndDefersWhileStale(t *testing.T) {
	root := t.TempDir()
	now := runtimeTestNow
	driver := &fakeDriver{
		workspace: filepath.Join(root, "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive},
	}
	runtime := signalRuntime(t, root, driver, func() time.Time { return now })
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ApplyParkedAssignments(emptyStatement()); err != nil {
		t.Fatal(err)
	}

	// The worker goes down. While it is down the coordinator parks the attempt,
	// so the statement the worker holds is both stale and wrong.
	now = now.Add(10 * time.Minute)
	driver.observations = []backlog.DispatchThreadState{backlog.DispatchThreadStopped}
	restarted := signalRuntime(t, root, driver, func() time.Time { return now })
	if err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.collectCalls != 0 {
		t.Fatalf("a restarted worker collected on a stale statement: %d", driver.collectCalls)
	}
	if got := phaseOf(t, restarted, "assignment-1"); got != PhaseStopped {
		t.Fatalf("phase after a deferred collection = %q", got)
	}

	// The next exchange delivers the current statement; the worker parks.
	driver.observations = []backlog.DispatchThreadState{backlog.DispatchThreadStopped}
	if err := restarted.ApplyParkedAssignments(parkedStatement(7)); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := phaseOf(t, restarted, "assignment-1"); got != PhaseWaiting || driver.collectCalls != 0 {
		t.Fatalf("phase=%q collections=%d after the refreshed statement", got, driver.collectCalls)
	}

	// The wake lands, the resumed turn ends, and the attempt is collected once.
	driver.observations = []backlog.DispatchThreadState{backlog.DispatchThreadActive, backlog.DispatchThreadStopped}
	if err := restarted.ApplyParkedAssignments(emptyStatement()); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	acknowledgeStoppedSnapshot(t, restarted, driver)
	if got := phaseOf(t, restarted, "assignment-1"); got != PhaseCompleted {
		t.Fatalf("phase after the resumed turn = %q", got)
	}
	if driver.collectCalls != 1 {
		t.Fatalf("collections across the restart = %d, want exactly one", driver.collectCalls)
	}
}

// A statement that names an attempt revision the coordinator has already moved
// past must not overwrite the newer one the worker already applied. Reports can
// overtake each other on a retried exchange.
func TestParkedStatementBehindTheAppliedRevisionIsIgnored(t *testing.T) {
	root := t.TempDir()
	now := runtimeTestNow
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true}
	runtime := signalRuntime(t, root, driver, func() time.Time { return now })
	if err := runtime.ApplyParkedAssignments(parkedStatement(9)); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ApplyParkedAssignments(parkedStatement(4)); err != nil {
		t.Fatal(err)
	}
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got := state.Parked["assignment-1"].AttemptRevision; got != 9 {
		t.Fatalf("a report behind the applied revision won: revision=%d", got)
	}
	// A newer revision does replace it: the park moved on with the attempt.
	if err := runtime.ApplyParkedAssignments(parkedStatement(11)); err != nil {
		t.Fatal(err)
	}
	if state, err = runtime.journal.snapshot(); err != nil {
		t.Fatal(err)
	}
	if got := state.Parked["assignment-1"].AttemptRevision; got != 11 {
		t.Fatalf("a newer report was refused: revision=%d", got)
	}
}

// A statement about a different execution of the same assignment says nothing
// about the one this worker holds, and silence is not "not parked".
func TestParkedStatementForAnotherAssignmentEpochDefers(t *testing.T) {
	root := t.TempDir()
	now := runtimeTestNow
	driver := &fakeDriver{
		workspace: filepath.Join(root, "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadStopped},
	}
	runtime := signalRuntime(t, root, driver, func() time.Time { return now })
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	statement := parkedStatement(5)
	statement.Parked[0].AssignmentEpoch = 1
	if err := runtime.ApplyParkedAssignments(statement); err != nil {
		t.Fatal(err)
	}
	record := mustRecord(t, runtime, "assignment-1")
	if _, err := runtime.liveTaskWait(context.Background(), record); !errors.Is(err, ErrParkedReportStale) {
		t.Fatalf("a statement about another execution was treated as evidence: %v", err)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.collectCalls != 0 {
		t.Fatalf("collected on a statement about another execution: %d", driver.collectCalls)
	}
}

// A coordinator that reports nothing at all is an older build. The worker keeps
// its previous behaviour rather than deferring forever, and the coordinator's
// own refusal remains the authority.
func TestWorkerWithoutACoordinatorStatementCollectsAsBefore(t *testing.T) {
	root := t.TempDir()
	driver := &fakeDriver{
		workspace: filepath.Join(root, "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadStopped},
	}
	runtime := signalRuntime(t, root, driver, func() time.Time { return runtimeTestNow })
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.collectCalls != 1 {
		t.Fatalf("collect calls = %d, want the pre-existing behaviour", driver.collectCalls)
	}
}

// A report that cannot be trusted whole is rejected whole: acting on part of a
// complete list would turn a missing entry into "not parked".
func TestParkedStatementValidation(t *testing.T) {
	valid := parkedStatement(3)
	if err := workerproto.ValidateSnapshotRequest(valid); err != nil {
		t.Fatal(err)
	}
	if err := workerproto.ValidateSnapshotRequest(workerproto.SnapshotRequest{}); err != nil {
		t.Fatal(err)
	}
	unflagged := valid
	unflagged.ParkedReported = false
	if err := workerproto.ValidateSnapshotRequest(unflagged); err == nil {
		t.Fatal("parked assignments without the reported flag were accepted")
	}
	repeated := workerproto.SnapshotRequest{ParkedReported: true, Parked: []workerproto.ParkedAssignment{valid.Parked[0], valid.Parked[0]}}
	if err := workerproto.ValidateSnapshotRequest(repeated); err == nil {
		t.Fatal("a repeated assignment was accepted")
	}
	incomplete := parkedStatement(3)
	incomplete.Parked[0].AttemptID = ""
	if err := workerproto.ValidateSnapshotRequest(incomplete); err == nil {
		t.Fatal("an incomplete parked identity was accepted")
	}
	oversized := workerproto.SnapshotRequest{ParkedReported: true}
	for index := 0; index <= workerproto.MaxParkedAssignments; index++ {
		oversized.Parked = append(oversized.Parked, workerproto.ParkedAssignment{
			AssignmentID:    string(rune('a'+index%26)) + string(rune('a'+index/26)) + string(rune('a'+index/676)),
			AssignmentEpoch: 1, AttemptID: "attempt", AttemptRevision: 1,
		})
	}
	if err := workerproto.ValidateSnapshotRequest(oversized); err == nil {
		t.Fatal("an unbounded report was accepted")
	}
}
