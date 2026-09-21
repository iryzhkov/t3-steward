package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestOlderOrdinaryAttemptPrecedesNewerActivationAtSharedCapacity(t *testing.T) {
	ctx := context.Background()
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)
	const (
		project          = "shared-project"
		ordinaryWorkflow = "workflow-ordinary-fairness"
		ordinaryRun      = "run-ordinary-fairness"
		ordinaryTask     = "task-ordinary-fairness"
		ordinaryAttempt  = "attempt-ordinary-fairness"
		parkedWorkflow   = "workflow-parked-fairness"
		parkedRun        = "run-parked-fairness"
		parkedTask       = "task-parked-fairness"
		parkedAttempt    = "attempt-parked-fairness"
		parkedAssignment = "assignment-parked-fairness"
	)
	route := domain.ProviderRoute{
		WorkerID: activationLeaseWorker, ProviderInstanceID: "claudeAgent",
		Model: "claude-fable-5-1", QuotaPoolID: "claude-main",
	}
	older := fixture.now.Add(-2 * time.Minute)
	ordinary := domain.Attempt{
		ID: ordinaryAttempt, WorkflowRunID: ordinaryRun, TaskID: ordinaryTask, Number: 1,
		Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
		Revision: 1, UpdatedAt: older,
	}
	parked := domain.Attempt{
		ID: parkedAttempt, WorkflowRunID: parkedRun, TaskID: parkedTask, Number: 1,
		AssignmentID: parkedAssignment, ThreadID: "thread-parked-fairness",
		Progress: domain.ProgressWaitingExternal, Control: domain.ControlWaitingExternal,
		Revision: 2, UpdatedAt: older,
	}
	if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{
			{ID: ordinaryWorkflow, Version: 1, Name: ordinaryWorkflow, Project: project, Class: domain.TaskClassRequired, TaskIDs: []string{ordinaryTask}, CreatedAt: older},
			{ID: parkedWorkflow, Version: 1, Name: parkedWorkflow, Project: project, Class: domain.TaskClassRequired, TaskIDs: []string{parkedTask}, CreatedAt: older},
		},
		WorkflowRuns: []domain.WorkflowRun{
			{ID: ordinaryRun, WorkflowID: ordinaryWorkflow, Progress: domain.ProgressActive, Revision: 1, CreatedAt: older, UpdatedAt: older},
			{ID: parkedRun, WorkflowID: parkedWorkflow, Progress: domain.ProgressActive, Revision: 1, CreatedAt: older, UpdatedAt: older},
		},
		Tasks: []domain.Task{
			{ID: ordinaryTask, WorkflowID: ordinaryWorkflow, Name: "ordinary", Class: domain.TaskClassRequired, Routes: []domain.ProviderRoute{route}, MaxTurns: 2},
			{ID: parkedTask, WorkflowID: parkedWorkflow, Name: "parked", Class: domain.TaskClassRequired, Routes: []domain.ProviderRoute{route}, MaxTurns: 2},
		},
		Attempts: []domain.Attempt{ordinary, parked},
		Assignments: []domain.Assignment{{
			ID: parkedAssignment, AttemptID: parkedAttempt, Project: project,
			WorkerID: activationLeaseWorker, WorkerEpoch: "worker-epoch-1", Route: route,
			ExecutorDemand: &domain.ResourceDemand{}, State: domain.AssignmentClaimed, Epoch: 1,
			ThreadID: parked.ThreadID, CreatedAt: older, UpdatedAt: older,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	snapshots, err := fixture.store.LoadWorkerSnapshots(ctx)
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("worker snapshots=%+v err=%v", snapshots, err)
	}
	snapshot := snapshots[0]
	snapshot.Sequence++
	snapshot.ObservedAt = fixture.now.Add(time.Second)
	snapshot.ValidUntil = fixture.now.Add(time.Hour)
	snapshot.Inventory.ObservedAt = snapshot.ObservedAt
	snapshot.Inventory.Projects = []domain.WorkerProjectInventory{{Name: project, Available: true, UpdatedAt: snapshot.ObservedAt}}
	if err := fixture.store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	wait, err := fixture.store.RegisterTaskWait(ctx, domain.TaskWaitRegistration{
		RequestID: "wait-parked-fairness", WorkflowRunID: parkedRun, TaskID: parkedTask,
		AttemptID: parkedAttempt, IssuedRevision: parked.Revision, ThreadID: parked.ThreadID,
		Wake: domain.WakeEach, MaxDuration: time.Hour, Name: "ready", Condition: "true",
	}, older)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, older); err != nil {
		t.Fatal(err)
	}

	quota := backlog.QuotaBridgeReport{
		Pools: []domain.QuotaPool{{
			ID: "claude-main", Provider: "claude", ProviderInstanceIDs: []string{"claudeAgent"},
			Admission: domain.AdmissionOpen, MaxConcurrent: 1,
		}},
		Windows: []backlog.QuotaWindowBudget{{
			QuotaPoolID: "claude-main", WindowID: "primary", ObservedAt: fixture.now,
			Admission: domain.AdmissionOpen, Capacity: 100, ResetsAt: fixture.now.Add(time.Hour),
		}},
		Derived: []backlog.QuotaPoolAdmissionSnapshot{{
			QuotaPoolID: "claude-main", Admission: domain.AdmissionOpen, ObservedAt: fixture.now,
		}},
	}
	planner := &coordinatorPlanner{
		store: fixture.store, coordinator: backlog.FleetCoordinator{Store: fixture.store}, epoch: 1,
		maxWorkerSnapshotAge: time.Hour, maxQuotaObservationAge: time.Hour,
		deadlineRiskWindow: time.Hour, checkpointMargin: time.Minute,
		now: func() time.Time { return fixture.now },
	}
	cycle := coordinatorBoundaryCycle{
		quota: fairnessQuota{quota}, schedules: fairnessSchedules{}, planning: planner,
		admin: fairnessAdmin{}, legacy: fairnessLegacy{}, supervision: &fixture.coordinator,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	cycle.Tick(ctx)

	state, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != "" {
		t.Fatalf("activation state=%q, want newer activation still queued", state.Activation.State)
	}
	records, err := fixture.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ordinaryOffer bool
	for _, assignment := range records.Assignments {
		if assignment.AttemptID == ordinaryAttempt && assignment.State == domain.AssignmentOffered {
			ordinaryOffer = true
		}
		if assignment.AttemptID != ordinaryAttempt && assignment.AttemptID != parkedAttempt &&
			assignment.WorkerID == activationLeaseWorker && assignment.State == domain.AssignmentOffered {
			t.Fatalf("newer activation received assignment before older ordinary attempt: %#v", assignment)
		}
	}
	if !ordinaryOffer {
		t.Fatal("older eligible ordinary attempt did not receive the shared capacity")
	}
	gotParked := loadFairnessAttempt(t, ctx, fixture.store, parkedAttempt)
	if gotParked.Control != domain.ControlWaitingExternal {
		t.Fatalf("older settled wake control=%q, want it held behind newer activation", gotParked.Control)
	}
}

func TestOlderActivationPrecedesYoungerOrdinaryAttemptAtSharedCapacity(t *testing.T) {
	ctx := context.Background()
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)

	const (
		project          = "activation-oldest-project"
		ordinaryWorkflow = "workflow-younger-ordinary"
		ordinaryRun      = "run-younger-ordinary"
		ordinaryTask     = "task-younger-ordinary"
		ordinaryAttempt  = "attempt-younger-ordinary"
	)
	fixture.now = fixture.now.Add(2 * time.Minute)
	youngerAt := fixture.now.Add(-time.Minute)
	route := domain.ProviderRoute{
		WorkerID: activationLeaseWorker, ProviderInstanceID: "claudeAgent",
		Model: "claude-fable-5-1", QuotaPoolID: "claude-main",
	}
	if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: ordinaryWorkflow, Version: 1, Name: ordinaryWorkflow, Project: project,
			Class: domain.TaskClassRequired, TaskIDs: []string{ordinaryTask}, CreatedAt: youngerAt,
		}},
		WorkflowRuns: []domain.WorkflowRun{{
			ID: ordinaryRun, WorkflowID: ordinaryWorkflow, Progress: domain.ProgressActive,
			Revision: 1, CreatedAt: youngerAt, UpdatedAt: youngerAt,
		}},
		Tasks: []domain.Task{{
			ID: ordinaryTask, WorkflowID: ordinaryWorkflow, Name: ordinaryTask,
			Class: domain.TaskClassRequired, Routes: []domain.ProviderRoute{route}, MaxTurns: 2,
		}},
		Attempts: []domain.Attempt{{
			ID: ordinaryAttempt, WorkflowRunID: ordinaryRun, TaskID: ordinaryTask, Number: 1,
			Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
			Revision: 1, UpdatedAt: youngerAt,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	snapshots, err := fixture.store.LoadWorkerSnapshots(ctx)
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("worker snapshots=%+v err=%v", snapshots, err)
	}
	snapshot := snapshots[0]
	snapshot.Sequence++
	snapshot.ObservedAt = fixture.now
	snapshot.ValidUntil = fixture.now.Add(time.Hour)
	snapshot.Inventory.ObservedAt = fixture.now
	snapshot.Inventory.Projects = []domain.WorkerProjectInventory{{
		Name: project, Available: true, UpdatedAt: fixture.now,
	}}
	if err := fixture.store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}

	quota := activationFairnessQuota(fixture.now, domain.QuotaPool{
		ID: "claude-main", Provider: "claude", ProviderInstanceIDs: []string{"claudeAgent"},
		Admission: domain.AdmissionOpen, MaxConcurrent: 1,
	})
	cycle := activationFairnessCycle(t, fixture, quota)
	cycle.Tick(ctx)

	state, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("older activation state=%q, want %q", state.Activation.State, domain.ActivationPendingDispatch)
	}
	ordinary := loadFairnessAttempt(t, ctx, fixture.store, ordinaryAttempt)
	if ordinary.Control != domain.ControlUnassigned || ordinary.AssignmentID != "" {
		t.Fatalf("younger ordinary attempt consumed shared capacity: %#v", ordinary)
	}
}

func TestOlderActivationWinsDespiteReverseWorkflowRecordOrder(t *testing.T) {
	ctx := context.Background()
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)

	const (
		olderWorkflow  = "workflow-z-older-activation"
		olderRun       = "run-z-older-activation"
		olderProducer  = "task-z-older-producer"
		olderProtected = "task-z-older-protected"
		olderGate      = "gate-z-older"
		olderIncident  = "incident-z-older"
		olderEvent     = "event-z-older"
	)
	olderAt := fixture.now.Add(-time.Minute)
	olderRecord := domain.SupervisionRecord{RunID: olderRun, Config: domain.SupervisionConfig{
		Route:                 domain.ProviderRoute{ProviderInstanceID: "claudeAgent", Model: "claude-fable-5-1"},
		PromptArtifactID:      "artifact-z-older",
		MaxActivations:        5,
		MaxTurnsPerActivation: 4,
		ActivationDeadline:    time.Hour,
	}}
	if _, err := fixture.store.PutSupervision(ctx, sqlite.SupervisionMaterialization{
		Record: olderRecord,
		Gates: []domain.Gate{{
			RunID: olderRun, State: domain.GateReadyForReview, GraphRevision: 1,
			Definition: domain.GateDefinition{
				ID: olderGate, Name: "review", ObservedTaskIDs: []string{olderProducer},
				ProtectedTaskIDs: []string{olderProtected},
			},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: olderWorkflow, Version: 1, Name: olderWorkflow, Class: domain.TaskClassRequired,
			TaskIDs: []string{olderProducer, olderProtected}, CreatedAt: olderAt,
		}},
		WorkflowRuns: []domain.WorkflowRun{{
			ID: olderRun, WorkflowID: olderWorkflow, GraphRevision: 1,
			Progress: domain.ProgressActive, Revision: 1, Supervision: &olderRecord,
			CreatedAt: olderAt, UpdatedAt: olderAt,
		}},
		Tasks: []domain.Task{{
			ID: olderProducer, WorkflowID: olderWorkflow, Name: "producer", Class: domain.TaskClassRequired,
		}, {
			ID: olderProtected, WorkflowID: olderWorkflow, Name: "protected", Class: domain.TaskClassRequired,
			Needs: []string{olderProducer},
		}},
		Attempts: []domain.Attempt{{
			ID: "attempt-z-older-producer", WorkflowRunID: olderRun, TaskID: olderProducer, Number: 1,
			Progress: domain.ProgressSucceeded, Control: domain.ControlStopped, Revision: 1, UpdatedAt: olderAt,
		}, {
			ID: "attempt-z-older-protected", WorkflowRunID: olderRun, TaskID: olderProtected, Number: 1,
			Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, Revision: 1, UpdatedAt: olderAt,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.OpenReviewIncident(ctx, sqlite.IncidentRequest{
		RunID: olderRun, IncidentID: olderIncident, RequestID: olderIncident,
		Actor:         domain.Actor{Kind: domain.ActorOperator, Principal: coordinatorSupervisionPrincipal},
		SourceEventID: olderEvent, GateID: olderGate,
		RequiredDisposition: domain.DispositionGateDecision,
		Reason:              "older review gate is ready", OpenedAt: olderAt,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.supervision.AppendSupervisionEvents(ctx, olderRun, []backlog.SupervisionEvent{{
		ID: olderEvent, RunID: olderRun, Kind: backlog.TriggerGateReviewReady,
		Reason: "older review gate is ready", GateID: olderGate,
		IncidentID: olderIncident, OccurredAt: olderAt,
	}}); err != nil {
		t.Fatal(err)
	}

	records, err := fixture.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, run := range records.WorkflowRuns {
		if run.ID == activationLeaseRun || run.ID == olderRun {
			order = append(order, run.ID)
		}
	}
	if len(order) != 2 || order[0] != activationLeaseRun || order[1] != olderRun {
		t.Fatalf("workflow record order=%v, want newer %q before older %q", order, activationLeaseRun, olderRun)
	}

	quota := activationFairnessQuota(fixture.now, domain.QuotaPool{
		ID: "claude-main", Provider: "claude", ProviderInstanceIDs: []string{"claudeAgent"},
		Admission: domain.AdmissionOpen, MaxConcurrent: 1,
	})
	cycle := activationFairnessCycle(t, fixture, quota)
	cycle.Tick(ctx)

	olderState, err := fixture.supervision.LoadSupervisionActivationState(ctx, olderRun)
	if err != nil {
		t.Fatal(err)
	}
	newerState, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if olderState.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("older activation state=%q, want %q", olderState.Activation.State, domain.ActivationPendingDispatch)
	}
	if newerState.Activation.State != "" && newerState.Activation.State != domain.ActivationIdle {
		t.Fatalf("newer activation state=%q, want it queued behind older activation", newerState.Activation.State)
	}
}

func TestDisjointSettledWakeDoesNotDelayActivation(t *testing.T) {
	ctx := context.Background()
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)
	addActivationFairnessAttempt(t, fixture, "wake", "worker-disjoint", "pool-disjoint", fixture.now.Add(-time.Minute), true)

	quota := activationFairnessQuota(fixture.now,
		domain.QuotaPool{ID: "claude-main", Provider: "claude", ProviderInstanceIDs: []string{"claudeAgent"}, Admission: domain.AdmissionOpen, MaxConcurrent: 1},
		domain.QuotaPool{ID: "pool-disjoint", Provider: "other", ProviderInstanceIDs: []string{"instance-wake"}, Admission: domain.AdmissionOpen, MaxConcurrent: 1},
	)
	cycle := activationFairnessCycle(t, fixture, quota)
	cycle.Tick(ctx)

	state, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("activation state=%q, want dispatch alongside disjoint wake", state.Activation.State)
	}
	if got := loadFairnessAttempt(t, ctx, fixture.store, "attempt-wake"); got.Control != domain.ControlResuming {
		t.Fatalf("disjoint wake control=%q, want normal planning to resume it", got.Control)
	}
}

func TestOrdinaryBatchWithRemainingSharedCapacityDoesNotDelayActivation(t *testing.T) {
	ctx := context.Background()
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)
	fixture.now = fixture.now.Add(10 * time.Minute)
	addActivationFairnessAttempt(t, fixture, "shared", "worker-shared", "claude-main", activationLeaseTime.Add(-2*time.Minute), false)
	addActivationFairnessAttempt(t, fixture, "disjoint", "worker-disjoint", "pool-disjoint", fixture.now.Add(-time.Minute), false)

	quota := activationFairnessQuota(fixture.now,
		domain.QuotaPool{ID: "claude-main", Provider: "claude", ProviderInstanceIDs: []string{"claudeAgent"}, Admission: domain.AdmissionOpen, MaxConcurrent: 2},
		domain.QuotaPool{ID: "pool-disjoint", Provider: "other", ProviderInstanceIDs: []string{"instance-disjoint"}, Admission: domain.AdmissionOpen, MaxConcurrent: 1},
	)
	cycle := activationFairnessCycle(t, fixture, quota)
	cycle.Tick(ctx)

	state, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("activation state=%q, want dispatch after older work used only one of two pool slots", state.Activation.State)
	}
	records, err := fixture.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	offered := map[string]bool{}
	for _, assignment := range records.Assignments {
		if assignment.State == domain.AssignmentOffered {
			offered[assignment.AttemptID] = true
		}
	}
	for _, attemptID := range []string{"attempt-shared", "attempt-disjoint"} {
		if !offered[attemptID] {
			t.Fatalf("ordinary batch did not commit %q: %+v", attemptID, offered)
		}
	}
}

func TestActivationFairnessCommitsOlderOrdinaryButNotYoungerAtTwoSlots(t *testing.T) {
	ctx := context.Background()
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)
	fixture.now = fixture.now.Add(10 * time.Minute)
	addActivationFairnessAttempt(t, fixture, "old", activationLeaseWorker, "claude-main", activationLeaseTime.Add(-time.Minute), false)
	addActivationFairnessAttempt(t, fixture, "young", activationLeaseWorker, "claude-main", fixture.now.Add(-time.Minute), false)
	snapshots, err := fixture.store.LoadWorkerSnapshots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := snapshots[0]
	snapshot.Sequence++
	snapshot.ObservedAt = fixture.now.Add(time.Duration(snapshot.Sequence) * time.Millisecond)
	snapshot.Inventory.ObservedAt = snapshot.ObservedAt
	snapshot.Inventory.Allocatable.ExecutorSlots = 2
	if err := fixture.store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}

	quota := activationFairnessQuota(fixture.now,
		domain.QuotaPool{ID: "claude-main", Provider: "claude", ProviderInstanceIDs: []string{"claudeAgent"}, Admission: domain.AdmissionOpen, MaxConcurrent: 2},
	)
	cycle := activationFairnessCycle(t, fixture, quota)
	cycle.Tick(ctx)

	state, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("activation state=%q, want middle-aged activation in second slot", state.Activation.State)
	}
	records, err := fixture.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	offered := map[string]bool{}
	for _, assignment := range records.Assignments {
		if assignment.State == domain.AssignmentOffered {
			offered[assignment.AttemptID] = true
		}
	}
	if !offered["attempt-old"] {
		t.Fatal("older ordinary attempt did not receive the first slot")
	}
	if offered["attempt-young"] {
		t.Fatal("younger ordinary attempt preempted the activation's second slot")
	}
}

func TestOlderWakeSharingEitherActivationConstraintWins(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		worker string
		pool   string
	}{
		{name: "same worker different pool", worker: activationLeaseWorker, pool: "pool-other"},
		{name: "same pool different worker", worker: "worker-other", pool: "claude-main"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := newActivationLeaseFixture(t)
			fixture.superviseRun(t)
			addActivationFairnessAttempt(t, fixture, "wake", testCase.worker, testCase.pool, fixture.now.Add(-time.Minute), true)

			pools := []domain.QuotaPool{{
				ID: "claude-main", Provider: "claude", ProviderInstanceIDs: []string{"claudeAgent"},
				Admission: domain.AdmissionOpen, MaxConcurrent: 1,
			}}
			if testCase.pool != "claude-main" {
				pools = append(pools, domain.QuotaPool{
					ID: testCase.pool, Provider: "other", ProviderInstanceIDs: []string{"instance-wake"},
					Admission: domain.AdmissionOpen, MaxConcurrent: 1,
				})
			}
			cycle := activationFairnessCycle(t, fixture, activationFairnessQuota(fixture.now, pools...))
			cycle.Tick(ctx)

			state, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
			if err != nil {
				t.Fatal(err)
			}
			if state.Activation.State != "" {
				t.Fatalf("activation state=%q, want older shared wake to win", state.Activation.State)
			}
			if got := loadFairnessAttempt(t, ctx, fixture.store, "attempt-wake"); got.Control != domain.ControlResuming {
				t.Fatalf("shared wake control=%q, want resuming", got.Control)
			}
		})
	}
}

func activationFairnessCycle(t *testing.T, fixture *activationLeaseFixture, quota backlog.QuotaBridgeReport) coordinatorBoundaryCycle {
	t.Helper()
	if err := fixture.store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{QuotaPools: quota.Pools}); err != nil {
		t.Fatal(err)
	}
	planner := &coordinatorPlanner{
		store: fixture.store, coordinator: backlog.FleetCoordinator{Store: fixture.store}, epoch: 1,
		maxWorkerSnapshotAge: time.Hour, maxQuotaObservationAge: time.Hour,
		deadlineRiskWindow: time.Hour, checkpointMargin: time.Minute,
		now: func() time.Time { return fixture.now },
	}
	return coordinatorBoundaryCycle{
		quota: fairnessQuota{quota}, schedules: fairnessSchedules{}, planning: planner,
		admin: fairnessAdmin{}, legacy: fairnessLegacy{}, supervision: &fixture.coordinator,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func activationFairnessQuota(now time.Time, pools ...domain.QuotaPool) backlog.QuotaBridgeReport {
	for index := range pools {
		pools[index].ChecksDisabled = true
	}
	report := backlog.QuotaBridgeReport{Pools: pools, ChecksDisabled: true}
	for _, pool := range pools {
		report.Windows = append(report.Windows, backlog.QuotaWindowBudget{
			QuotaPoolID: pool.ID, WindowID: "primary", ObservedAt: now,
			Admission: domain.AdmissionOpen, Capacity: 100, ResetsAt: now.Add(time.Hour),
		})
		report.Derived = append(report.Derived, backlog.QuotaPoolAdmissionSnapshot{
			QuotaPoolID: pool.ID, Admission: domain.AdmissionOpen, ObservedAt: now,
		})
	}
	return report
}

func addActivationFairnessAttempt(t *testing.T, fixture *activationLeaseFixture, suffix, workerID, poolID string, readyAt time.Time, parked bool) {
	t.Helper()
	ctx := context.Background()
	workflowID, runID := "workflow-"+suffix, "run-"+suffix
	taskID, attemptID := "task-"+suffix, "attempt-"+suffix
	instanceID, model := "instance-"+suffix, "model"
	if poolID == "claude-main" {
		instanceID = "claudeAgent"
	}
	if workerID == activationLeaseWorker {
		model = "claude-fable-5-1"
	}
	route := domain.ProviderRoute{WorkerID: workerID, ProviderInstanceID: instanceID, Model: model, QuotaPoolID: poolID}
	workerEpoch := "epoch-" + suffix
	if workerID == activationLeaseWorker {
		workerEpoch = "worker-epoch-1"
	}
	attempt := domain.Attempt{
		ID: attemptID, WorkflowRunID: runID, TaskID: taskID, Number: 1,
		Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, UpdatedAt: readyAt,
	}
	var assignments []domain.Assignment
	if parked {
		attempt.Progress, attempt.Control = domain.ProgressWaitingExternal, domain.ControlWaitingExternal
		attempt.AssignmentID, attempt.ThreadID = "assignment-"+suffix, "thread-"+suffix
		assignments = []domain.Assignment{{
			ID: attempt.AssignmentID, AttemptID: attempt.ID, Project: "project",
			WorkerID: workerID, WorkerEpoch: workerEpoch, Route: route,
			ExecutorDemand: &domain.ResourceDemand{}, State: domain.AssignmentClaimed, Epoch: 1,
			ThreadID: attempt.ThreadID, CreatedAt: readyAt, UpdatedAt: readyAt,
		}}
	}
	if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: workflowID, Version: 1, Name: workflowID, Project: "project", Class: domain.TaskClassRequired, TaskIDs: []string{taskID}, CreatedAt: readyAt}},
		WorkflowRuns: []domain.WorkflowRun{{ID: runID, WorkflowID: workflowID, Progress: domain.ProgressActive, Revision: 1, CreatedAt: readyAt, UpdatedAt: readyAt}},
		Tasks:        []domain.Task{{ID: taskID, WorkflowID: workflowID, Name: taskID, Class: domain.TaskClassRequired, Routes: []domain.ProviderRoute{route}, MaxTurns: 2}},
		Attempts:     []domain.Attempt{attempt}, Assignments: assignments,
	}); err != nil {
		t.Fatal(err)
	}
	if workerID == activationLeaseWorker {
		snapshots, err := fixture.store.LoadWorkerSnapshots(ctx)
		if err != nil {
			t.Fatal(err)
		}
		snapshot := snapshots[0]
		snapshot.Sequence++
		snapshot.ObservedAt = fixture.now.Add(time.Duration(snapshot.Sequence) * time.Millisecond)
		snapshot.ValidUntil = fixture.now.Add(time.Hour)
		snapshot.Inventory.ObservedAt = snapshot.ObservedAt
		snapshot.Inventory.Projects = []domain.WorkerProjectInventory{{Name: "project", Available: true, UpdatedAt: fixture.now}}
		if poolID != "claude-main" {
			snapshot.Inventory.Providers = append(snapshot.Inventory.Providers, domain.WorkerProviderInventory{
				InstanceID: instanceID, Models: []string{model}, QuotaPoolID: poolID, Available: true,
			})
		}
		if err := fixture.store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
			t.Fatal(err)
		}
	} else if err := fixture.store.SaveWorkerSnapshot(ctx, domain.WorkerSnapshot{
		WorkerID: workerID, WorkerEpoch: "epoch-" + suffix, CoordinatorEpoch: 1, Sequence: 1,
		Connected: true, ObservedAt: fixture.now, ValidUntil: fixture.now.Add(time.Hour),
		Inventory: domain.WorkerInventory{
			ID: workerID, AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: fixture.now,
			Projects:    []domain.WorkerProjectInventory{{Name: "project", Available: true, UpdatedAt: fixture.now}},
			Providers:   []domain.WorkerProviderInventory{{InstanceID: instanceID, Models: []string{model}, QuotaPoolID: poolID, Available: true}},
			Allocatable: domain.AllocatableCapacity{ExecutorSlots: 1},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if !parked {
		return
	}
	wait, err := fixture.store.RegisterTaskWait(ctx, domain.TaskWaitRegistration{
		RequestID: "wait-" + suffix, WorkflowRunID: runID, TaskID: taskID,
		AttemptID: attemptID, IssuedRevision: attempt.Revision, ThreadID: attempt.ThreadID,
		Wake: domain.WakeEach, MaxDuration: time.Hour, Name: "ready", Condition: "true",
	}, readyAt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, readyAt); err != nil {
		t.Fatal(err)
	}
}
