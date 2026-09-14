package workerruntime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// waitingRuntime is a claimed runtime whose task-bound wait answer the test
// controls, standing in for the coordinator the worker asks before collecting.
func waitingRuntime(t *testing.T, root string, driver *fakeDriver, live *bool, probeErr *error, now func() time.Time) *Runtime {
	t.Helper()
	journal, err := OpenJournal(root, "normandy", "worker-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	config := testConfig(now)
	config.LiveTaskWait = func(context.Context, workerproto.ExecutionPackage) (bool, error) {
		if probeErr != nil && *probeErr != nil {
			return false, *probeErr
		}
		return *live, nil
	}
	runtime, err := New(config, journal, driver)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func phaseOf(t *testing.T, runtime *Runtime, id string) Phase {
	t.Helper()
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return state.Attempts[id].Phase
}

// The whole worker-side park and resume: a stopped thread with a live wait is
// parked rather than collected, an active thread means the wake landed, and the
// outputs are collected only once the resumed turn ends with no live wait.
func TestWorkerParksInsteadOfCollectingWhileATaskWaitIsLive(t *testing.T) {
	root := t.TempDir()
	now := runtimeTestNow
	live := true
	driver := &fakeDriver{
		workspace: filepath.Join(root, "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{
			backlog.DispatchThreadStopped, // the turn parks
			backlog.DispatchThreadStopped, // still parked on the next pass
			backlog.DispatchThreadActive,  // the wake resumed the same thread
			backlog.DispatchThreadStopped, // the resumed turn ends
		},
	}
	runtime := waitingRuntime(t, root, driver, &live, nil, func() time.Time { return now })
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}

	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := phaseOf(t, runtime, "assignment-1"); got != PhaseWaiting {
		t.Fatalf("the ended turn was not parked: phase=%q", got)
	}
	if driver.collectCalls != 0 {
		t.Fatalf("outputs were collected from a parked task: %d collections", driver.collectCalls)
	}
	observed := observation(mustRecord(t, runtime, "assignment-1"), now)
	if observed.Control != domain.ControlWaitingExternal || observed.State != domain.AssignmentClaimed {
		t.Fatalf("a parked attempt reported %q/%q", observed.State, observed.Control)
	}

	// Parked, and staying parked, across an ordinary reconcile.
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := phaseOf(t, runtime, "assignment-1"); got != PhaseWaiting || driver.collectCalls != 0 {
		t.Fatalf("phase=%q collections=%d", got, driver.collectCalls)
	}

	// The coordinator resumed the attempt and the wake reached the thread.
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := phaseOf(t, runtime, "assignment-1"); got != PhaseRunning {
		t.Fatalf("the woken thread did not resume: phase=%q", got)
	}

	// The resumed turn ends with nothing live, so collection happens once.
	live = false
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := phaseOf(t, runtime, "assignment-1"); got != PhaseCompleted {
		t.Fatalf("the resumed turn was not collected: phase=%q", got)
	}
	if driver.collectCalls != 1 {
		t.Fatalf("collect calls = %d, want exactly one", driver.collectCalls)
	}
}

// A worker restart while parked keeps the attempt parked and still collects
// exactly once after the wake. The journal, not memory, holds the park.
func TestWorkerRestartWhileParkedCollectsExactlyOnce(t *testing.T) {
	root := t.TempDir()
	now := runtimeTestNow
	live := true
	driver := &fakeDriver{
		workspace: filepath.Join(root, "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadStopped},
	}
	runtime := waitingRuntime(t, root, driver, &live, nil, func() time.Time { return now })
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := phaseOf(t, runtime, "assignment-1"); got != PhaseWaiting {
		t.Fatalf("phase before restart = %q", got)
	}

	driver.observations = []backlog.DispatchThreadState{backlog.DispatchThreadStopped, backlog.DispatchThreadStopped}
	restarted := waitingRuntime(t, root, driver, &live, nil, func() time.Time { return now })
	if err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := phaseOf(t, restarted, "assignment-1"); got != PhaseWaiting || driver.collectCalls != 0 {
		t.Fatalf("a restart un-parked the attempt: phase=%q collections=%d", got, driver.collectCalls)
	}
	live = false
	if err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.collectCalls != 1 {
		t.Fatalf("collect calls across the restart = %d, want exactly one", driver.collectCalls)
	}
}

// A probe that cannot answer defers the decision. Collecting is the dangerous
// direction, and waiting one more reconcile costs nothing.
func TestWorkerDefersCollectionWhenTheWaitStateIsUnknown(t *testing.T) {
	root := t.TempDir()
	live := false
	probeErr := error(errors.New("coordinator unreachable"))
	driver := &fakeDriver{
		workspace: filepath.Join(root, "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadStopped, backlog.DispatchThreadStopped},
	}
	runtime := waitingRuntime(t, root, driver, &live, &probeErr, func() time.Time { return runtimeTestNow })
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.collectCalls != 0 {
		t.Fatalf("collected without knowing whether the task was parked: %d", driver.collectCalls)
	}
	probeErr = nil
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.collectCalls != 1 {
		t.Fatalf("collect calls once the probe answered = %d", driver.collectCalls)
	}
}

// A worker without the seam behaves exactly as it did: the coordinator's
// refusal is the backstop, not a second copy of the same rule here.
func TestWorkerWithoutTheTaskWaitSeamCollectsAsBefore(t *testing.T) {
	root := t.TempDir()
	driver := &fakeDriver{
		workspace: filepath.Join(root, "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadStopped},
	}
	runtime := newClaimedRuntime(t, root, driver)
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.collectCalls != 1 {
		t.Fatalf("collect calls = %d", driver.collectCalls)
	}
}

// The execution identity reaches the task process as exactly the six names,
// and the dispatch token is not one of them.
func TestTaskEnvironmentCarriesTheIdentityAndNotTheDispatchToken(t *testing.T) {
	identity := workerproto.ExecutionIdentity{
		WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1", AttemptRevision: 7,
		AssignmentID: "assignment-1", ThreadID: "thread-1", DispatchToken: "secret",
	}
	environment := identity.TaskEnvironment()
	if len(environment) != len(domain.TaskWaitEnvironmentNames()) {
		t.Fatalf("the injected environment is %d entries: %v", len(environment), environment)
	}
	if environment[domain.TaskWaitEnvAttemptRevision] != "7" {
		t.Fatalf("the attempt revision fence was not injected: %q", environment[domain.TaskWaitEnvAttemptRevision])
	}
	for name, value := range environment {
		if value == "secret" {
			t.Fatalf("%s carries the dispatch token into the agent process", name)
		}
	}
}

func mustRecord(t *testing.T, runtime *Runtime, id string) AttemptRecord {
	t.Helper()
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return state.Attempts[id]
}
