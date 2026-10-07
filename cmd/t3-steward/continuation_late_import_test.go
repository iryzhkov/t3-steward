package main

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// rearmFixture is an attempt that lost its lease on worker normandy: its
// assignment was offered at epoch 1, with the continuation decision of that
// first offer frozen, then released, and the attempt is ready to be offered
// again, which the real planner does by re-arming that assignment at epoch 2.
// The worker still holds the snapshot the epoch-1 dispatch took.
type rearmFixture struct {
	handOnWorker
	path    string
	planner coordinatorPlanner
	quota   backlog.QuotaBridgeReport
}

func newRearmFixture(t *testing.T) rearmFixture {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 9, 10, 16, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	// The uploads and the importer of handOnWorker are under coordinator
	// epoch 1.
	const epoch = 1
	cost := 9.0
	route := domain.ProviderRoute{ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "pool"}
	task := domain.Task{ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassRequired,
		Routes: []domain.ProviderRoute{route}, Importance: 5, Difficulty: 3, EstimatedCost: &cost, MaxTurns: 2}
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1,
		Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 3, AssignmentID: "assignment-1", UpdatedAt: now}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "normandy", WorkerEpoch: "worker-1",
		Route: route, State: domain.AssignmentOffered, Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{ID: task.WorkflowID, Version: 1, Name: "workflow", Project: "project",
			Class: domain.TaskClassRequired, TaskIDs: []string{task.ID}, CreatedAt: now}},
		WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID, Progress: domain.ProgressActive,
			Revision: 1, CreatedAt: now, UpdatedAt: now}},
		Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	// The first offer froze its decision: the worker advertised the
	// continuation capability, so the package declared it.
	if _, err := store.FreezeAssignmentContinuation(ctx, epoch, "coordinator", assignment, workerproto.ExecutionIdentity{
		WorkflowID: task.WorkflowID, WorkflowRunID: attempt.WorkflowRunID, TaskID: task.ID, AttemptID: attempt.ID, AttemptRevision: attempt.Revision,
		AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, DispatchToken: assignment.DispatchToken,
	}, sqlite.ContinuationDecision{Offered: true}); err != nil {
		t.Fatal(err)
	}
	// The lease was lost: the assignment is released and the attempt is ready
	// to be offered again.
	assignment.State, attempt.AssignmentID = domain.AssignmentReleased, ""
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(ctx, domain.WorkerSnapshot{
		WorkerID: "normandy", WorkerEpoch: "worker-1", CoordinatorEpoch: epoch, Sequence: 1, Connected: true,
		Inventory: domain.WorkerInventory{ID: "normandy", AcceptBacklog: true, Health: domain.WorkerHealthReady,
			Projects:   []domain.WorkerProjectInventory{{Name: "project", Available: true, UpdatedAt: now}},
			Providers:  []domain.WorkerProviderInventory{{InstanceID: "codex", Models: []string{"gpt"}, QuotaPoolID: "pool", Available: true}},
			ObservedAt: now},
		ObservedAt: now, ValidUntil: now.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	return rearmFixture{
		handOnWorker: handOnWorker{store: store, now: now, task: task, attempt: attempt, assignment: assignment},
		path:         path,
		planner: coordinatorPlanner{
			store: store, coordinator: backlog.FleetCoordinator{Store: store}, epoch: epoch,
			maxWorkerSnapshotAge: time.Hour, maxQuotaObservationAge: 5 * time.Minute,
			deadlineRiskWindow: time.Hour, checkpointMargin: 5 * time.Minute,
			now: func() time.Time { return now.Add(2 * time.Minute) },
		},
		quota: backlog.QuotaBridgeReport{
			Pools: []domain.QuotaPool{{ID: "pool", Provider: "openai", ProviderInstanceIDs: []string{"codex"},
				Admission: domain.AdmissionOpen, MaxConcurrent: 2}},
			Windows: []backlog.QuotaWindowBudget{{QuotaPoolID: "pool", WindowID: "primary", ObservedAt: now,
				Admission: domain.AdmissionOpen, Capacity: 100, ResetsAt: now.Add(time.Hour)}},
		},
	}
}

// dispatch is the attempt's assignment as the store has it now.
func (f rearmFixture) dispatch(t *testing.T) domain.Assignment {
	t.Helper()
	records, err := f.store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, assignment := range records.Assignments {
		if assignment.ID == f.assignment.ID {
			return assignment
		}
	}
	t.Fatal("the fixture's assignment is gone")
	return domain.Assignment{}
}

// cycle is the production boundary cycle with the real planner, whose worker
// phase scans the given workers through the real protocol clients.
func (f rearmFixture) cycle(sessions *coordinatorWorkerSessions) coordinatorBoundaryCycle {
	var scheduleCalls, adminCalls int
	return coordinatorBoundaryCycle{
		quota: fixedCoordinatorQuotaTicker{report: f.quota}, schedules: recordingCoordinatorScheduleTicker{calls: &scheduleCalls},
		planning: f.planner, admin: recordingCoordinatorAdminExecutor{calls: &adminCalls},
		workers: sessions, logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// sessions polls normandy, while it is configured, through one session.
func (f rearmFixture) sessions(t *testing.T, control *queuedUploadControl, workerIDs ...string) *coordinatorWorkerSessions {
	session := f.session(t, control)
	return &coordinatorWorkerSessions{
		workerIDs: workerIDs,
		handOn: func(ctx context.Context, _ string) (backlog.WorkerExchangeReport, error) {
			return handOnCoordinatorWorkerContinuations(ctx, session, backlog.WorkerExchangeReport{}, 1024)
		},
		exchange: func(ctx context.Context, _ string, _ backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error) {
			return exchangeCoordinatorWorker(ctx, session, 1024, func(context.Context) (backlog.WorkerExchangeReport, error) {
				return backlog.WorkerExchangeReport{}, nil
			})
		},
	}
}

type fixedCoordinatorQuotaTicker struct{ report backlog.QuotaBridgeReport }

func (q fixedCoordinatorQuotaTicker) Tick(context.Context) (backlog.QuotaBridgeReport, error) {
	return q.report, nil
}

// M16-6 Option B, test (i): review round 4's R8 scenario inverted. The real
// planner re-arms the released assignment at epoch 2 before the worker phase
// of the same pass polls normandy, whose snapshot of epoch 1 is therefore
// late. It is authenticated against the epoch-1 dispatch's V39 row, imported,
// published, and becomes the task's latest, which `task result`, explain and
// any later attempt's first offer read. Nothing waited for it.
//
// Test (iii): after a coordinator restart the worker offers the same upload
// again (its acknowledgement was lost); importing it is a no-op.
func TestALateSnapshotPolledAfterTheReArmIsImportedAndBecomesLatest(t *testing.T) {
	ctx := context.Background()
	f := newRearmFixture(t)
	continuation, snapshotID := f.snapshot(t, 1)
	control := &queuedUploadControl{uploads: []*pendingUpload{continuation}}
	f.cycle(f.sessions(t, control, "normandy")).TickWithWorkers(ctx)

	if got := f.dispatch(t); got.Epoch != 2 || got.State != domain.AssignmentOffered {
		t.Fatalf("the planner did not offer the attempt again in the same pass: %+v", got)
	}
	if latest := f.latest(t); latest == nil || latest.ID != snapshotID || !continuation.acked {
		t.Fatalf("the late snapshot was not imported: latest=%+v, acknowledged=%t", latest, continuation.acked)
	}
	if latest := f.latest(t); latest.ID != domain.ContinuationLiveArtifactID(f.attempt.ID, 1, 1) {
		t.Fatalf("latest = %s, want the epoch-1 snapshot", latest.ID)
	}

	// Restart: the same database is reopened by a new coordinator process,
	// and the worker offers the upload again.
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sqlite.OpenMigrated(f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	f.store, f.planner.store, f.planner.coordinator = reopened, reopened, backlog.FleetCoordinator{Store: reopened}
	continuation.acked = false
	f.cycle(f.sessions(t, control, "normandy")).TickWithWorkers(ctx)
	if !continuation.acked {
		t.Fatal("the replayed upload was not acknowledged")
	}
	records, err := reopened.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Artifacts) != 1 || records.Artifacts[0].ID != snapshotID {
		t.Fatalf("artifacts after the replay = %+v", records.Artifacts)
	}
	if got := f.dispatch(t); got.Epoch != 2 {
		t.Fatalf("the replay moved the dispatch: %+v", got)
	}
}

// Review round 4, D2: a worker removed from the configuration while it
// holds snapshots is not polled, and its released dispatch is re-armed
// without waiting. When the worker is added back, its late snapshot is
// imported under the V39 row of the dispatch that took it.
func TestARemovedWorkersSnapshotIsImportedWhenItIsAddedBack(t *testing.T) {
	ctx := context.Background()
	f := newRearmFixture(t)
	continuation, snapshotID := f.snapshot(t, 1)
	control := &queuedUploadControl{uploads: []*pendingUpload{continuation}}

	f.cycle(f.sessions(t, control)).TickWithWorkers(ctx)
	if got := f.dispatch(t); got.Epoch != 2 {
		t.Fatalf("the released dispatch waited for a removed worker: %+v", got)
	}
	if latest := f.latest(t); latest != nil || continuation.acked {
		t.Fatalf("a removed worker was polled: %+v", latest)
	}

	f.cycle(f.sessions(t, control, "normandy")).TickWithWorkers(ctx)
	if latest := f.latest(t); latest == nil || latest.ID != snapshotID || !continuation.acked {
		t.Fatalf("the re-added worker's snapshot was not imported: %+v", latest)
	}
}
