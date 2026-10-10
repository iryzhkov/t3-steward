package workerruntime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestDrainAbsentUnknownKeepsPauseIntent(t *testing.T) {
	for _, control := range []domain.ControlState{domain.ControlDraining, domain.ControlPaused, domain.ControlPausedUncheckpointed} {
		t.Run(string(control), func(t *testing.T) {
			assignment := testOffer(t).Assignment
			assignment.State = domain.AssignmentUnknown
			assignment.WorkerEpoch = "old-worker"
			attempt := domain.Attempt{ID: assignment.AttemptID, AssignmentID: assignment.ID, Progress: domain.ProgressActive, Control: control, Revision: 1}
			snapshot := domain.WorkerSnapshot{WorkerID: "normandy", WorkerEpoch: "new-worker", CoordinatorEpoch: 9, Sequence: 1, Connected: true, ObservedAt: runtimeTestNow, ValidUntil: runtimeTestNow.Add(time.Hour)}
			transitions, err := backlog.PlanWorkerStateTransitions(sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}, snapshot, nil, runtimeTestNow)
			if err != nil || len(transitions) != 0 || !domain.AssignmentOwnsExecutorCapacity(attempt, assignment) {
				t.Fatalf("absence acknowledged pause or redispatched: %#v %v", transitions, err)
			}
		})
	}
}

func TestDrainForeignReleaseKeepsCustodyAndAllowsSibling(t *testing.T) {
	now := runtimeTestNow
	assignment := testOffer(t).Assignment
	assignment.State = domain.AssignmentClaimed
	assignment.WorkerEpoch = "worker-1"
	attempt := domain.Attempt{ID: assignment.AttemptID, AssignmentID: assignment.ID, Control: domain.ControlRunning, Progress: domain.ProgressActive, Revision: 1}
	sibling, siblingAttempt := assignment, attempt
	sibling.ID, sibling.AttemptID, sibling.ThreadID = "sibling-assignment", "sibling-attempt", "sibling-thread"
	siblingAttempt.ID, siblingAttempt.AssignmentID, siblingAttempt.Control = sibling.AttemptID, sibling.ID, domain.ControlPreparing
	snapshot := domain.WorkerSnapshot{WorkerID: "normandy", WorkerEpoch: "worker-1", CoordinatorEpoch: 9, Sequence: 1, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour), Assignments: []domain.WorkerAssignmentObservation{
		{AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, State: domain.AssignmentReleased, ThreadID: "foreign-thread", ObservedAt: now},
		{AssignmentID: sibling.ID, AssignmentEpoch: sibling.Epoch, State: domain.AssignmentClaimed, Control: domain.ControlRunning, ThreadID: sibling.ThreadID, ObservedAt: now},
	}}
	transitions, err := backlog.PlanWorkerStateTransitions(sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt, siblingAttempt}, Assignments: []domain.Assignment{assignment, sibling}}, snapshot, nil, now)
	if err != nil || len(transitions) != 1 || transitions[0].Assignment.ID != sibling.ID {
		t.Fatalf("foreign stop released custody or blocked unrelated evidence: %#v %v", transitions, err)
	}
}

type drainSupersedingDriver struct {
	*fakeDriver
	onStop func()
}

func (d *drainSupersedingDriver) StopThread(ctx context.Context, pkg workerproto.ExecutionPackage) error {
	if hook := d.onStop; hook != nil {
		d.onStop = nil
		hook()
	}
	return d.fakeDriver.StopThread(ctx, pkg)
}

func TestDrainLateStopCannotConfirmReplacementEpoch(t *testing.T) {
	ctx := context.Background()
	base := &fakeDriver{workspace: t.TempDir(), workspaceReady: true}
	runtime := newClaimedRuntime(t, t.TempDir(), base)
	driver := &drainSupersedingDriver{fakeDriver: base}
	runtime.driver = driver
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", base.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	command := testCommand(t, runtime, domain.WorkerCommandStop, "old-stop")
	driver.onStop = func() {
		offer := testOffer(t)
		offer.Assignment.Epoch++
		offer.Package.Package.Identity.AssignmentEpoch++
		var err error
		offer.Package, err = workerproto.BuildExecutionPackageManifest(offer.Package.Package)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.AcceptOffers(ctx, workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{offer}}); err != nil {
			t.Fatal(err)
		}
	}
	acks, err := runtime.DeliverCommands(ctx, workerproto.CommandDelivery{Commands: []domain.WorkerCommand{command}})
	if err != nil || len(acks.Acknowledgements) != 1 || acks.Acknowledgements[0].Accepted {
		t.Fatalf("old stop acknowledged replacement: %#v %v", acks, err)
	}
	record := journalRecord(t, runtime)
	if record.Assignment.Epoch != 3 || record.StopConfirmed || record.Phase != PhaseClaimed || len(record.CommandRequests) != 0 {
		t.Fatalf("late old stop changed replacement: %#v", record)
	}
}

func TestDrainLateThrottleCannotPauseReplacementEpoch(t *testing.T) {
	ctx := context.Background()
	base := &fakeDriver{workspace: t.TempDir(), workspaceReady: true}
	runtime := newClaimedRuntime(t, t.TempDir(), base)
	driver := &drainSupersedingDriver{fakeDriver: base}
	runtime.driver = driver
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", base.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	command := testThrottle(runtime, domain.ThrottleCommandHardStop, "old-throttle-stop")
	driver.onStop = func() {
		offer := testOffer(t)
		offer.Assignment.Epoch++
		offer.Package.Package.Identity.AssignmentEpoch++
		var err error
		offer.Package, err = workerproto.BuildExecutionPackageManifest(offer.Package.Package)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.AcceptOffers(ctx, workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{offer}}); err != nil {
			t.Fatal(err)
		}
	}
	acks, err := runtime.DeliverThrottle(ctx, []domain.ThrottleCommand{command})
	if err != nil || len(acks) != 1 || acks[0].Accepted {
		t.Fatalf("old throttle acknowledged replacement: %#v %v", acks, err)
	}
	record := journalRecord(t, runtime)
	if record.Assignment.Epoch != 3 || record.Phase != PhaseClaimed || len(record.ThrottleRequests) != 0 || record.StopConfirmed {
		t.Fatalf("late old throttle changed replacement: %#v", record)
	}
}

func TestDrainAcceptedDeferredStopRetainsCoordinatorReservations(t *testing.T) {
	// The worker API returns an accepted receipt with a deferred effect detail.
	// Feed that exact receipt into the owning reconciliation API.
	ctx := context.Background()
	driver := &fakeDriver{workspace: t.TempDir(), stopErr: errors.New("timeout waiting for provider")}
	runtime := newClaimedRuntime(t, t.TempDir(), driver)
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	command := testCommand(t, runtime, domain.WorkerCommandStop, "deferred-stop")
	acks, err := runtime.DeliverCommands(ctx, workerproto.CommandDelivery{Commands: []domain.WorkerCommand{command}})
	if err != nil {
		t.Fatal(err)
	}
	record := journalRecord(t, runtime)
	assignment := record.Assignment
	attempt := domain.Attempt{ID: assignment.AttemptID, AssignmentID: assignment.ID, Progress: domain.ProgressCancelled, Control: domain.ControlStopped, Revision: 1}
	snapshot := domain.WorkerSnapshot{WorkerID: "normandy", WorkerEpoch: "worker-1", CoordinatorEpoch: 9, Sequence: 1, Connected: true, ObservedAt: runtimeTestNow, ValidUntil: runtimeTestNow.Add(time.Hour)}
	transitions, err := backlog.PlanWorkerStateTransitions(sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}, snapshot,
		[]domain.WorkerCommandRecord{{Command: command, Acknowledgement: &acks.Acknowledgements[0]}}, runtimeTestNow)
	if err != nil || len(transitions) != 0 || !domain.AssignmentOwnsExecutorCapacity(attempt, assignment) {
		t.Fatalf("deferred stop settled custody: %#v %v", transitions, err)
	}

	// The same actual worker observation then settles the real persisted rows.
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for epoch := int64(1); epoch < 9; epoch++ {
		if _, err := store.AdvanceCoordinatorEpoch(ctx, epoch); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	driver.stopErr = nil
	snapshot, err = runtime.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	transitions, err = backlog.PlanWorkerStateTransitions(sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}, snapshot, nil, runtimeTestNow)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitWorkerStateTransitions(ctx, transitions); err != nil {
		t.Fatal(err)
	}
	stored, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Assignments[0].State != domain.AssignmentReleased || stored.Attempts[0].AssignmentID != "" ||
		domain.AssignmentOwnsExecutorCapacity(stored.Attempts[0], stored.Assignments[0]) {
		t.Fatalf("actual stop did not settle reservation: %#v", stored)
	}
}

func TestDrainStopResponsibilityRefusesNewExecutionCommands(t *testing.T) {
	ctx := context.Background()
	driver := &fakeDriver{workspace: t.TempDir(), workspaceReady: true, stopErr: errors.New("provider still running")}
	runtime := newClaimedRuntime(t, t.TempDir(), driver)
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	stop := testCommand(t, runtime, domain.WorkerCommandStop, "stop-before-racing-dispatch")
	acks, err := runtime.DeliverCommands(ctx, workerproto.CommandDelivery{Commands: []domain.WorkerCommand{stop}})
	if err != nil || len(acks.Acknowledgements) != 1 || !acks.Acknowledgements[0].Accepted || acks.Acknowledgements[0].Detail == "" {
		t.Fatalf("stop did not retain deferred responsibility: %#v %v", acks, err)
	}
	for _, kind := range []domain.WorkerCommandKind{domain.WorkerCommandPrepare, domain.WorkerCommandDispatch} {
		command := testCommand(t, runtime, kind, "racing-"+string(kind))
		acks, err := runtime.DeliverCommands(ctx, workerproto.CommandDelivery{Commands: []domain.WorkerCommand{command}})
		if err != nil || len(acks.Acknowledgements) != 1 || acks.Acknowledgements[0].Accepted {
			t.Fatalf("execution command bypassed durable stop: %#v %v", acks, err)
		}
	}
	record := journalRecord(t, runtime)
	if record.Phase != PhaseStopping || record.StopConfirmed || observation(record, runtimeTestNow, true).State != domain.AssignmentClaimed {
		t.Fatalf("racing commands changed stop custody: %#v", record)
	}
}

// Accepted means durable responsibility, including when the effect timed out.
// Reopening the journal must retry the stop rather than rediscovering running.
func TestDrainStopUnknownRetriesUntilCustodyConfirmed(t *testing.T) {
	ctx := context.Background()
	for _, phase := range []Phase{PhaseUnknown, PhaseRunning, PhaseDispatching, PhaseStopping, PhaseStopped} {
		t.Run(string(phase), func(t *testing.T) {
			root := t.TempDir()
			driver := &fakeDriver{workspace: root, workspaceReady: true, stopErr: errors.New("provider still running"), observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive, backlog.DispatchThreadActive}}
			runtime := newClaimedRuntime(t, root, driver)
			if err := runtime.markPhase("assignment-1", phase, "", root, "thread-1"); err != nil {
				t.Fatal(err)
			}
			command := testCommand(t, runtime, domain.WorkerCommandStop, "stop-drain")
			acks, err := runtime.DeliverCommands(ctx, workerproto.CommandDelivery{Commands: []domain.WorkerCommand{command}})
			if err != nil || len(acks.Acknowledgements) != 1 || !acks.Acknowledgements[0].Accepted {
				t.Fatalf("durable responsibility: %#v, %v", acks, err)
			}
			record := journalRecord(t, runtime)
			if record.StopConfirmed || observation(record, runtimeTestNow, true).State != domain.AssignmentClaimed {
				t.Fatalf("unproven stop released custody: %#v", record)
			}
			runtime = reopenTestRuntime(t, root, driver)
			if err := runtime.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			record = journalRecord(t, runtime)
			if record.Phase != PhaseStopping || record.StopConfirmed || driver.stopCalls < 2 {
				t.Fatalf("accepted stop lost on restart: phase=%s confirmed=%v calls=%d", record.Phase, record.StopConfirmed, driver.stopCalls)
			}
			driver.stopErr = nil
			if err := runtime.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			record = journalRecord(t, runtime)
			if !record.StopConfirmed || observation(record, runtimeTestNow, true).State != domain.AssignmentReleased {
				t.Fatalf("confirmed stop did not release: %#v", record)
			}
			calls := driver.stopCalls
			replayed, err := runtime.DeliverCommands(ctx, workerproto.CommandDelivery{Commands: []domain.WorkerCommand{command}})
			if err != nil || replayed.Acknowledgements[0] != acks.Acknowledgements[0] || driver.stopCalls != calls {
				t.Fatalf("stop replay changed durable ack or repeated effect: %#v, %v", replayed, err)
			}
		})
	}
}
