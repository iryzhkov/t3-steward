package workerruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// wakeWindowFixture is the whole park-and-resume lifecycle assembled from the
// real parts: a real coordinator store, the real statement the coordinator
// sends a worker, a real worker runtime on the production path, and the real
// driver that owns the identity record on disk. Nothing here stands in for the
// component under test, because the defect lives between them.
type wakeWindowFixture struct {
	store     *sqlite.Store
	runtime   *Runtime
	driver    *LocalDriver
	control   *recordingT3
	publisher *recordingPublisher
	pkg       workerproto.ExecutionPackage
	workspace string
	now       time.Time
}

func newWakeWindowFixture(t *testing.T) *wakeWindowFixture {
	t.Helper()
	ctx := context.Background()
	now := runtimeTestNow
	// A published capture is made read-only on purpose, so the root has to be
	// one whose cleanup can still remove it.
	root := artifactTestRoot(t)
	store, err := sqlite.OpenMigrated(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	pkg := testPackage()
	attempt := domain.Attempt{
		ID: pkg.Identity.AttemptID, WorkflowRunID: pkg.Identity.WorkflowRunID, TaskID: pkg.Identity.TaskID,
		Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 9,
		AssignmentID: pkg.Identity.AssignmentID, ThreadID: pkg.Identity.ThreadID, UpdatedAt: now,
	}
	assignment := domain.Assignment{
		ID: pkg.Identity.AssignmentID, AttemptID: attempt.ID, WorkerID: pkg.WorkerID, WorkerEpoch: pkg.WorkerEpoch,
		State: domain.AssignmentClaimed, Epoch: pkg.Identity.AssignmentEpoch, LeaseToken: "lease-1",
		DispatchToken: pkg.Identity.DispatchToken, ThreadID: pkg.Identity.ThreadID,
		LeaseExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}

	control := &recordingT3{thread: &domain.Thread{ID: pkg.Identity.ThreadID, Running: true}}
	publisher := &recordingPublisher{}
	driver, err := NewLocalDriver(LocalDriver{
		Config: LocalDriverConfig{
			CatalogRevision: pkg.Environment.CatalogRevision,
			ArtifactRoot:    filepath.Join(root, "artifacts"),
			RunsRoot:        filepath.Join(root, "runs"),
			// The workspace outlives the collection here so the test can say
			// what is in it afterwards.
			RetainWorkspaces: true,
		},
		Catalog: &backlog.ProjectCatalog{},
		Finalizer: backlog.AttemptFinalizer{
			Processes: successfulProcessRunner{},
			Now:       func() time.Time { return now },
			NewID:     func(string) string { return "verification-1" },
		},
		Source: mapArtifactSource{}, Publisher: publisher, T3: control,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}

	// The workspace preparation produces: the directory the thread runs in, and
	// the identity record inside it.
	workspace := filepath.Join(driver.workspacePath(pkg), "workspace")
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := driver.writeTaskIdentity(pkg, workspace); err != nil {
		t.Fatal(err)
	}

	journal, err := OpenJournal(filepath.Join(root, "journal"), pkg.WorkerID, pkg.WorkerEpoch, pkg.CoordinatorEpoch)
	if err != nil {
		t.Fatal(err)
	}
	// No LiveTaskWait override: the only thing this worker knows about parked
	// assignments is what the coordinator tells it, which is the production path.
	runtime, err := New(testConfig(func() time.Time { return now }), journal, driver)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.AcceptOffers(ctx, workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase(pkg.Identity.AssignmentID, PhaseRunning, "", workspace, pkg.Identity.ThreadID); err != nil {
		t.Fatal(err)
	}
	return &wakeWindowFixture{
		store: store, runtime: runtime, driver: driver, control: control,
		publisher: publisher, pkg: pkg, workspace: workspace, now: now,
	}
}

// exchange runs one worker exchange and one reconcile pass: the coordinator
// states which of this worker's assignments are parked, and the worker acts on
// that statement and on what it can observe of the thread.
func (f *wakeWindowFixture) exchange(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	statement, err := backlog.ParkedAssignmentsFor(ctx, f.store, f.pkg.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.runtime.ApplyParkedAssignments(statement); err != nil {
		t.Fatal(err)
	}
	if err := f.runtime.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
}

func (f *wakeWindowFixture) turnEnded() {
	f.control.thread.Running = false
	f.control.thread.TurnState = "completed"
}

func (f *wakeWindowFixture) turnRunning() {
	f.control.thread.Running = true
	f.control.thread.TurnState = ""
}

func (f *wakeWindowFixture) identityPresent(t *testing.T) bool {
	t.Helper()
	_, err := os.Lstat(filepath.Join(f.workspace, filepath.FromSlash(domain.TaskIdentityFile)))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return err == nil
}

// deliverWake carries a committed wake all the way to the thread, which is the
// step that actually restarts the turn.
func (f *wakeWindowFixture) deliverWake(t *testing.T, wake domain.TaskWaitWakeContext, at time.Time) {
	t.Helper()
	ctx := context.Background()
	for _, wait := range wake.Waits {
		if _, err := f.store.TransitionTaskWake(ctx, wait.ID, "pending", "sending", at); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.TransitionTaskWake(ctx, wait.ID, "sending", "delivered", at); err != nil {
			t.Fatal(err)
		}
	}
}

// A settled wait does not un-park its attempt. Between the settlement and the
// moment the wake message reaches the thread there is a window in which the
// turn is over, the next one has not started, and the coordinator used to tell
// the worker that nothing was parked. The worker then collected: it removed the
// identity record, published a result for outputs the task had not written, and
// the turn that resumed a moment later could no longer name itself.
//
// This asserts the record is present at the start of the resumed turn, which is
// the property the whole task-bound wait feature rests on.
func TestParkedTaskKeepsItsIdentityThroughTheWakeWindow(t *testing.T) {
	ctx := context.Background()
	f := newWakeWindowFixture(t)

	// The task parks itself with the record it was given, and the turn ends.
	first, err := registerFromInjectedIdentity(t, f.store, f.workspace, "req-1", f.now)
	if err != nil {
		t.Fatalf("the task could not park itself: %v", err)
	}
	f.turnEnded()
	f.exchange(t)
	if got := phaseOf(t, f.runtime, "assignment-1"); got != PhaseWaiting {
		t.Fatalf("the ended turn was not parked: phase=%q", got)
	}

	// The condition settles. The coordinator now owns an outcome, but the
	// thread has not been told anything yet and is still stopped.
	if _, err := f.store.SettleTaskWait(ctx, first.ID, domain.TaskWaitResult{
		Outcome: domain.TaskWaitMet, Reason: "the build finished",
	}, f.now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	f.exchange(t)
	if got := phaseOf(t, f.runtime, "assignment-1"); got != PhaseWaiting {
		t.Fatalf("a settled but undelivered wait un-parked the attempt: phase=%q", got)
	}
	if !f.identityPresent(t) {
		t.Fatal("the identity record was destroyed between the settlement and the wake")
	}

	// The wake is committed against the attempt, and still not delivered.
	wakes, err := f.store.WakeTaskWaits(ctx, f.now.Add(time.Minute))
	if err != nil || len(wakes) != 1 {
		t.Fatalf("wakes = %+v err=%v", wakes, err)
	}
	f.exchange(t)
	if got := phaseOf(t, f.runtime, "assignment-1"); got != PhaseWaiting {
		t.Fatalf("a committed but undelivered wake un-parked the attempt: phase=%q", got)
	}
	if !f.identityPresent(t) {
		t.Fatal("the identity record was destroyed between the wake and its delivery")
	}
	if f.publisher.results != nil {
		t.Fatalf("a parked attempt published %d results", len(f.publisher.results))
	}

	// The message reaches the thread and the same turn resumes.
	f.deliverWake(t, wakes[0], f.now.Add(2*time.Minute))
	f.turnRunning()
	f.exchange(t)
	if got := phaseOf(t, f.runtime, "assignment-1"); got != PhaseRunning {
		t.Fatalf("the woken thread did not resume: phase=%q", got)
	}
	if !f.identityPresent(t) {
		t.Fatal("the resumed turn cannot name itself: the identity record is gone")
	}

	// Which is what the resumed turn needs it for: a second wait, registered
	// with the same record.
	second, err := registerFromInjectedIdentity(t, f.store, f.workspace, "req-2", f.now.Add(3*time.Minute))
	if err != nil {
		t.Fatalf("the resumed turn could not park itself again: %v", err)
	}

	// A request ID names one park. Replaying the first one now that its wait has
	// settled must refuse, because the caller would otherwise be told it is
	// parked and end its turn on a wait that holds nothing.
	if _, err := registerFromInjectedIdentity(t, f.store, f.workspace, "req-1", f.now.Add(3*time.Minute)); !errors.Is(err, domain.ErrTaskWaitReplaySettled) {
		t.Fatalf("a replayed request ID parked the task again: %v", err)
	}

	// The second wait settles, is delivered, and the resumed turn ends with
	// nothing parking it. Only now is the attempt collected, exactly once.
	if _, err := f.store.SettleTaskWait(ctx, second.ID, domain.TaskWaitResult{
		Outcome: domain.TaskWaitMet, Reason: "the review landed",
	}, f.now.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	secondWakes, err := f.store.WakeTaskWaits(ctx, f.now.Add(4*time.Minute))
	if err != nil || len(secondWakes) != 1 {
		t.Fatalf("the second wake = %+v err=%v", secondWakes, err)
	}
	f.deliverWake(t, secondWakes[0], f.now.Add(5*time.Minute))
	f.turnRunning()
	f.exchange(t)
	if got := phaseOf(t, f.runtime, "assignment-1"); got != PhaseRunning {
		t.Fatalf("the second wake did not resume the thread: phase=%q", got)
	}

	f.control.message = "finished"
	f.control.archive = []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed",` +
		`"startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},` +
		`"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`)
	f.turnEnded()
	f.exchange(t)
	if got := phaseOf(t, f.runtime, "assignment-1"); got != PhaseCompleted {
		t.Fatalf("the finished turn was not collected: phase=%q", got)
	}
	if len(f.publisher.results) != 1 {
		t.Fatalf("the finished turn published %d results, want exactly one", len(f.publisher.results))
	}
	if f.identityPresent(t) {
		t.Fatal("the completed collection left the identity record in the captured workspace")
	}
}
