package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// otherRun adds a second run with one task to the fixture store and returns
// it, so a task can wait on a node that is not its own.
func otherRun(t *testing.T, store *Store, now time.Time, progress domain.ProgressState) domain.WorkflowRun {
	t.Helper()
	tasks := []domain.Task{{ID: "t2", Name: "deploy", WorkflowID: "w2"}}
	run, err := domain.BindRunSink(domain.WorkflowRun{ID: "r2", WorkflowID: "w2", Revision: 1, Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now}, tasks)
	if err != nil {
		t.Fatal(err)
	}
	attempt := domain.Attempt{ID: "a2", TaskID: "t2", WorkflowRunID: "r2", Number: 1, Revision: 1, Progress: progress, Control: domain.ControlRunning, UpdatedAt: now}
	if progress.Terminal() {
		attempt.Control = domain.ControlStopped
		run, err = domain.ProjectRunSink(run, tasks, []domain.Attempt{attempt}, nil, now)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}, Tasks: tasks, Attempts: []domain.Attempt{attempt}}); err != nil {
		t.Fatal(err)
	}
	return run
}

func nodeRegistration(attempt domain.Attempt, requestID string, target domain.NodeRef, state domain.NodeWaitState) domain.TaskWaitRegistration {
	registration := taskWaitRegistration(attempt, requestID, domain.WakeEach)
	registration.Kind = domain.WaitKindNode
	registration.Condition = ""
	registration.Node = &domain.NodeWaitCondition{Target: target, State: state}
	return registration
}

// A task-bound node wait parks the attempt with no local check row, is
// settled by the coordinator's node settlement pass, and wakes the attempt
// with the node trailer fields.
func TestTaskBoundNodeWaitParksAndIsSettledByTheCoordinator(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	otherRun(t, store, now, domain.ProgressActive)
	registration := nodeRegistration(attempt, "req-node", domain.NodeRef{RunID: "r2", TaskID: domain.SinkTaskName}, "")
	registered, err := store.RegisterTaskWait(ctx, registration, now)
	if err != nil {
		t.Fatal(err)
	}
	if registered.Kind != domain.WaitKindNode || registered.Node == nil || registered.Node.State != domain.NodeStateTerminal || registered.Condition == "" {
		t.Fatalf("registered = %+v", registered)
	}
	if parked := loadAttempt(t, store, attempt.ID); parked.Progress != domain.ProgressWaitingExternal {
		t.Fatalf("the attempt is not parked: %q", parked.Progress)
	}
	if rows, err := store.ListWaits(ctx, ""); err != nil || len(rows) != 0 {
		t.Fatalf("a coordinator kind left a local row: %v %v", rows, err)
	}
	// Nothing settles while the other run is active.
	if err := store.SettleNodeWaits(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, _ := store.ListTaskWaits(ctx)
	if waits[0].Settled() {
		t.Fatalf("settled while the target was active: %+v", waits[0])
	}
	// The other run fails; terminal is met, with the failed list.
	otherRun(t, store, now, domain.ProgressFailed)
	if err := store.SettleNodeWaits(ctx, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, _ = store.ListTaskWaits(ctx)
	result := waits[0].Result
	if result == nil || result.Outcome != domain.TaskWaitMet {
		t.Fatalf("the coordinator did not settle the node wait: %+v", waits[0])
	}
	if result.Fields["run"] != "r2" || result.Fields["progress"] != "failed" || result.Fields["failed"] != "t2" || result.Fields["result"] != "t3-steward task result r2" {
		t.Fatalf("fields = %v", result.Fields)
	}
	wakes, err := store.WakeTaskWaits(ctx, now.Add(3*time.Minute))
	if err != nil || len(wakes) != 1 {
		t.Fatalf("wakes=%v err=%v", wakes, err)
	}
	if resumed := loadAttempt(t, store, attempt.ID); resumed.Progress != domain.ProgressActive {
		t.Fatalf("the attempt did not resume: %q", resumed.Progress)
	}
	// The delivered message starts with the node trailer.
	clock := now.Add(4 * time.Minute)
	runner, control := fleetRunner(t, store, &clock, nil)
	runner.Tick(ctx, nil, nil)
	if len(control.texts) != 1 || !strings.HasPrefix(control.texts[0], "t3-steward-wait kind=node outcome=met wait="+registered.ID) {
		t.Fatalf("wake = %v", control.texts)
	}
	parsed, _ := wait.ParseWakeTrailer(control.texts[0])
	if parsed["failed"] != "t2" || parsed["result"] != "t3-steward task result r2" {
		t.Fatalf("trailer = %v", parsed)
	}
}

// --state succeeded on a run that fails wakes failed; a condition that
// already holds, an unknown target and a task's own sink are refused.
func TestTaskBoundNodeWaitStatesAndRefusals(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	otherRun(t, store, now, domain.ProgressActive)
	registered, err := store.RegisterTaskWait(ctx, nodeRegistration(attempt, "req-succeeded", domain.NodeRef{RunID: "r2", TaskID: "deploy"}, domain.NodeStateSucceeded), now)
	if err != nil {
		t.Fatal(err)
	}
	otherRun(t, store, now, domain.ProgressFailed)
	if err := store.SettleNodeWaits(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, _ := store.ListTaskWaits(ctx)
	for _, w := range waits {
		if w.ID == registered.ID && (w.Result == nil || w.Result.Outcome != domain.TaskWaitFailed) {
			t.Fatalf("succeeded on a failed run: %+v", w.Result)
		}
	}
	// The attempt resumed; park it again on conditions that must be refused.
	if _, err := store.WakeTaskWaits(ctx, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	resumed := loadAttempt(t, store, attempt.ID)
	if _, err := store.RegisterTaskWait(ctx, nodeRegistration(resumed, "req-held", domain.NodeRef{RunID: "r2", TaskID: "deploy"}, domain.NodeStateTerminal), now); err == nil || !strings.Contains(err.Error(), "already") {
		t.Fatalf("a condition that already holds parked the attempt: %v", err)
	}
	if _, err := store.RegisterTaskWait(ctx, nodeRegistration(resumed, "req-missing", domain.NodeRef{RunID: "absent", TaskID: "x"}, domain.NodeStateTerminal), now); err == nil {
		t.Fatal("an unknown target parked the attempt")
	}
	if _, err := store.RegisterTaskWait(ctx, nodeRegistration(resumed, "req-self", domain.NodeRef{RunID: attempt.WorkflowRunID, TaskID: domain.SinkTaskName}, domain.NodeStateTerminal), now); err == nil || !strings.Contains(err.Error(), "own run") {
		t.Fatalf("a task parked on its own run's sink: %v", err)
	}
	if still := loadAttempt(t, store, attempt.ID); still.Progress == domain.ProgressWaitingExternal {
		t.Fatal("a refused registration parked the attempt")
	}
}

// An interactive node wait with --state settles on that state, not on the
// terminal progress.
func TestInteractiveNodeWaitWithAState(t *testing.T) {
	ctx := context.Background()
	store, _, now := taskWaitFixture(t)
	otherRun(t, store, now, domain.ProgressActive)
	request := domain.NodeWaitRequest{ID: "nw-active", ThreadID: "thread", Name: "deploy active", Target: domain.NodeRef{RunID: "r2", TaskID: "deploy"}, State: domain.NodeStateActive, Timeout: time.Hour}
	registered, err := store.RegisterNodeWait(ctx, request, "operator", "host", now)
	if err != nil {
		t.Fatal(err)
	}
	// The attempt is already active, so registration observed the state.
	if registered.Observation == nil || registered.Observation.Outcome != domain.TaskWaitMet {
		t.Fatalf("registered = %+v", registered)
	}
	waiting := domain.NodeWaitRequest{ID: "nw-parked", ThreadID: "thread", Name: "deploy parked", Target: domain.NodeRef{RunID: "r2", TaskID: "deploy"}, State: domain.NodeStateWaitingExternal, Timeout: time.Hour}
	if w, err := store.RegisterNodeWait(ctx, waiting, "operator", "host", now); err != nil || w.SettledAt != nil {
		t.Fatalf("waiting-external settled on an active attempt: %+v err=%v", w, err)
	}
	otherRun(t, store, now, domain.ProgressWaitingExternal)
	if err := store.SettleNodeWaits(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, _ := store.ListNodeWaits(ctx)
	for _, w := range waits {
		if w.Request.ID == "nw-parked" && (w.Observation == nil || w.Observation.Outcome != domain.TaskWaitMet || w.Observation.Progress != domain.ProgressWaitingExternal) {
			t.Fatalf("waiting-external not observed: %+v", w.Observation)
		}
	}
}
