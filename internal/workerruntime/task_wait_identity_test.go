package workerruntime

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// countingProcessRunner reports how many commands verification actually ran, so
// a test can say "verified once" rather than "verified".
type countingProcessRunner struct{ runs int }

func (r *countingProcessRunner) Run(_ context.Context, request backlog.ProcessRequest) (backlog.ProcessResult, error) {
	r.runs++
	if request.Log != nil {
		_, _ = request.Log.Write([]byte("ok\n"))
	}
	return backlog.ProcessResult{ExitCode: 0}, nil
}

func (r *countingProcessRunner) Kill(string) error { return nil }

// The whole task-bound wait lifecycle with the identity coming only from the
// record the worker injected into the workspace: nothing here tells the task
// which attempt it is, and nothing overrides the revision it was given.
//
// The two defects this exercises compound. The package hands the task a
// revision the coordinator has already moved past, and a collection that
// deferred used to delete the record, so a woken turn could neither name itself
// nor name a revision the coordinator would accept. Both halves have to hold
// for a real fleet to park anything.
func TestTaskParksWakesAndIsCollectedOnceFromTheInjectedIdentity(t *testing.T) {
	ctx := context.Background()
	now := runtimeTestNow
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	pkg := testPackage()
	// The package was built while the attempt was at revision 1. By the time
	// the turn runs the coordinator has advanced it several times, which is the
	// ordinary course of a dispatch.
	pkg.Identity.AttemptRevision = 1
	attempt := domain.Attempt{
		ID: pkg.Identity.AttemptID, WorkflowRunID: pkg.Identity.WorkflowRunID, TaskID: pkg.Identity.TaskID,
		Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 9,
		AssignmentID: pkg.Identity.AssignmentID, ThreadID: pkg.Identity.ThreadID, UpdatedAt: now,
	}
	assignment := domain.Assignment{
		ID: pkg.Identity.AssignmentID, AttemptID: attempt.ID, WorkerID: pkg.WorkerID, WorkerEpoch: pkg.WorkerEpoch,
		State: domain.AssignmentClaimed, Epoch: pkg.Identity.AssignmentEpoch, LeaseToken: "lease-1",
		DispatchToken: pkg.Identity.DispatchToken, ThreadID: pkg.Identity.ThreadID,
		ExecutorDemand: &domain.ResourceDemand{},
		LeaseExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	// This integration fixture has no exchange loop. Explicit fresh zero-slot
	// inventory preserves its historical ungoverned capacity while proving the
	// retained worker epoch used by wake reacquisition.
	if err := store.SaveWorkerSnapshot(ctx, domain.WorkerSnapshot{
		WorkerID: pkg.WorkerID, WorkerEpoch: pkg.WorkerEpoch, CoordinatorEpoch: 1, Sequence: 1,
		Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour),
		Inventory: domain.WorkerInventory{ID: pkg.WorkerID, AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now},
	}); err != nil {
		t.Fatal(err)
	}

	workspace := t.TempDir()
	control := &recordingT3{thread: &domain.Thread{ID: pkg.Identity.ThreadID, Running: true}}
	verification := &countingProcessRunner{}
	publisher := &recordingPublisher{}
	driver := &LocalDriver{
		T3: control, Publisher: publisher, Now: func() time.Time { return now },
		Finalizer: backlog.AttemptFinalizer{
			StorageRoot: filepath.Join(artifactTestRoot(t), "artifacts"), Processes: verification,
			Now: func() time.Time { return now }, NewID: func(string) string { return "verification-1" },
		},
	}
	// Preparation injects the record. This is the call Prepare makes, and it is
	// the only thing that ever tells this task who it is.
	if err := driver.writeTaskIdentity(pkg, workspace); err != nil {
		t.Fatal(err)
	}

	// The task parks itself.
	first, err := registerFromInjectedIdentity(t, store, workspace, "req-1", now)
	if err != nil {
		t.Fatalf("the task could not park itself with the identity it was given: %v", err)
	}
	parked := coordinatorAttempt(t, store, attempt.ID)
	if parked.Progress != domain.ProgressWaitingExternal || parked.Revision != attempt.Revision+1 {
		t.Fatalf("the attempt was not parked on the live revision: %q at %d", parked.Progress, parked.Revision)
	}
	// This is the answer the worker's own probe reads: while it is held, nothing
	// is collected and nothing is verified.
	live, err := store.LiveTaskWaitAttempts(ctx)
	if err != nil || live[attempt.ID] != first.ID {
		t.Fatalf("the parked attempt is not held by its wait: %v %v", live, err)
	}

	// The condition settles and the coordinator resumes the same thread once.
	if _, err := store.SettleTaskWait(ctx, first.ID, domain.TaskWaitResult{
		Outcome: domain.TaskWaitMet, Reason: "the build finished",
	}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	wakes, err := authorizedWakeTaskWaits(t, store, now.Add(time.Minute))
	if err != nil || len(wakes) != 1 || wakes[0].ThreadID != pkg.Identity.ThreadID {
		t.Fatalf("wakes = %+v err=%v", wakes, err)
	}
	if !strings.Contains(wakes[0].Prompt(), "met") {
		t.Fatalf("the resumed turn was given no usable evidence: %q", wakes[0].Prompt())
	}

	// A collection pass arrives while the resumed turn is still running. It
	// defers, and the record it would need is still there afterwards.
	if err := driver.Collect(ctx, pkg, workspace); err == nil ||
		!strings.Contains(err.Error(), "result collection deferred") {
		t.Fatalf("collection of a resumed, running turn = %v, want a deferral", err)
	}

	// The resumed turn parks again, still naming itself from the same record.
	second, err := registerFromInjectedIdentity(t, store, workspace, "req-2", now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("the woken turn could not park itself again: %v", err)
	}
	if _, err := store.SettleTaskWait(ctx, second.ID, domain.TaskWaitResult{
		Outcome: domain.TaskWaitMet, Reason: "the review landed",
	}, now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if wakes, err := authorizedWakeTaskWaits(t, store, now.Add(3*time.Minute)); err != nil || len(wakes) != 1 {
		t.Fatalf("the second wake = %+v err=%v", wakes, err)
	}

	// The turn ends with nothing live, so the worker collects, exactly once.
	control.thread.Running = false
	control.thread.TurnState = "completed"
	control.message = "finished"
	control.archive = []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed",` +
		`"startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},` +
		`"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`)
	if live, err := store.LiveTaskWaitAttempts(ctx); err != nil || len(live) != 0 {
		t.Fatalf("a settled and woken wait still holds the attempt: %v %v", live, err)
	}
	if err := driver.Collect(ctx, pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if len(publisher.results) != 1 {
		t.Fatalf("the finished turn was published %d times", len(publisher.results))
	}
	if verification.runs != len(pkg.Verification) {
		t.Fatalf("verification ran %d commands, want %d", verification.runs, len(pkg.Verification))
	}
	if !publisher.results[0].Finalized.Completion.VerificationPassed {
		t.Fatalf("the collected result did not verify: %+v", publisher.results[0].Finalized.Completion)
	}
	if _, err := os.Lstat(filepath.Join(workspace, domain.TaskIdentityDir)); !os.IsNotExist(err) {
		t.Fatalf("the completed collection left the identity record behind: %v", err)
	}
}

// registerFromInjectedIdentity parks the attempt the way `wait add --task
// current` does: everything it sends comes from the record in the workspace,
// read back with the same checks the command applies.
func registerFromInjectedIdentity(t *testing.T, store *sqlite.Store, workspace, requestID string, now time.Time) (domain.TaskWait, error) {
	t.Helper()
	path := filepath.Join(workspace, filepath.FromSlash(domain.TaskIdentityFile))
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("the task has no identity to name itself with: %v", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("identity record mode = %v, want a private regular file", info.Mode())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	values, err := domain.ParseTaskIdentityFile(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	issued, err := strconv.ParseInt(values[domain.TaskWaitEnvAttemptRevision], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return store.RegisterTaskWait(context.Background(), domain.TaskWaitRegistration{
		RequestID:      requestID,
		WorkflowRunID:  values[domain.TaskWaitEnvWorkflowRunID],
		TaskID:         values[domain.TaskWaitEnvTaskID],
		AttemptID:      values[domain.TaskWaitEnvAttemptID],
		IssuedRevision: issued,
		ThreadID:       values[domain.TaskWaitEnvThreadID],
		Wake:           domain.WakeEach, MaxDuration: time.Hour,
		Name: "ci", Condition: "gh run view",
	}, now)
}

func coordinatorAttempt(t *testing.T, store *sqlite.Store, id string) domain.Attempt {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range records.Attempts {
		if attempt.ID == id {
			return attempt
		}
	}
	t.Fatalf("attempt %q is missing", id)
	return domain.Attempt{}
}
