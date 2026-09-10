package backlogadmin

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestExecutePendingCommandsSurvivesRestartAndReplaysRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	now := adminTestNow
	records := sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "workflow", Class: domain.TaskClassRequired, TaskIDs: []string{"task-1"}}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressFailed, Revision: 3, CreatedAt: now, UpdatedAt: now, CompletedAt: &now}},
		Tasks:        []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassRequired}},
		Attempts:     []domain.Attempt{{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1, Progress: domain.ProgressFailed, Control: domain.ControlStopped, Revision: 4, UpdatedAt: now, CompletedAt: &now}},
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now })
	request := Mutation{
		Version: Version, Principal: Principal{ID: "operator"}, ID: "retry-1",
		Kind: domain.AdminCommandRetry, WorkflowRunID: "run-1", TaskID: "task-1",
		ExpectedRevision: 4, Reason: "retry fixed work",
	}
	submitted, err := service.Mutate(context.Background(), request)
	if err != nil || submitted.Command.State != domain.AdminCommandPending {
		t.Fatalf("submission = %#v, %v", submitted, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, err = New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now.Add(time.Minute) })
	report, err := service.ExecutePendingCommands(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Decisions) != 1 || report.Decisions[0].Command.State != domain.AdminCommandApplied {
		t.Fatalf("report = %#v", report)
	}
	if replay, err := service.Mutate(context.Background(), request); err != nil || replay.Command.State != domain.AdminCommandApplied {
		t.Fatalf("replay = %#v, %v", replay, err)
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Attempts) != 2 || loaded.WorkflowRuns[0].Progress != domain.ProgressQueued || loaded.WorkflowRuns[0].Revision != 4 {
		t.Fatalf("records = %#v", loaded)
	}
	if again, err := service.ExecutePendingCommands(context.Background()); err != nil || len(again.Decisions) != 0 {
		t.Fatalf("second execution = %#v, %v", again, err)
	}
}

func TestExecutePendingPausePersistsDeliveryIntentAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	now := adminTestNow
	route := domain.ProviderRoute{
		WorkerID: "normandy", ProviderInstanceID: "codex", Model: "model", QuotaPoolID: "pool",
	}
	records := sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "workflow", Class: domain.TaskClassRequired, TaskIDs: []string{"task-1"}}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive, Revision: 1}},
		Tasks:        []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassRequired}},
		Attempts: []domain.Attempt{{
			ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1,
			Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 3,
			AssignmentID: "assignment-1",
		}},
		Assignments: []domain.Assignment{{
			ID: "assignment-1", AttemptID: "attempt-1", WorkerID: "normandy", WorkerEpoch: "worker-epoch",
			Route: route, State: domain.AssignmentClaimed, Epoch: 1, ThreadID: "thread-1",
		}},
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(context.Background(), domain.WorkerSnapshot{
		WorkerID: "normandy", WorkerEpoch: "worker-epoch", CoordinatorEpoch: 1, Sequence: 1,
		Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Minute),
		Inventory: domain.WorkerInventory{ID: "normandy"},
		Assignments: []domain.WorkerAssignmentObservation{{
			AssignmentID: "assignment-1", AssignmentEpoch: 1, State: domain.AssignmentClaimed,
			ThreadID: "thread-1", WorkspacePath: "/runs/run-1/task-1/attempt-1/workspace",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now })
	request := Mutation{
		Version: Version, Principal: Principal{ID: "operator"}, ID: "pause-1",
		Kind: domain.AdminCommandPause, WorkflowRunID: "run-1", TaskID: "task-1",
		ExpectedRevision: 3, Reason: "operator pause", Payload: json.RawMessage(`{"now":true}`),
	}
	if _, err := service.Mutate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	plannedRecords, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	workers, err := store.LoadWorkerSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	application, _, err := planAdminCommand(plannedRecords, workers, nil, plannedRecords.AdminCommands[0], now)
	if err != nil {
		t.Fatal(err)
	}
	for name, tamper := range map[string]func(*domain.ThrottleAttemptTransition){
		"workspace": func(intent *domain.ThrottleAttemptTransition) {
			intent.Record.Command.WorkspacePath = "/different/workspace"
		},
		"route": func(intent *domain.ThrottleAttemptTransition) {
			intent.Record.Command.Route.Model = "different-model"
		},
	} {
		t.Run("rejects tampered "+name, func(t *testing.T) {
			tampered := application
			intent := *application.PauseIntent
			tamper(&intent)
			tampered.PauseIntent = &intent
			if _, err := store.ApplyAdminCommand(context.Background(), tampered); !errors.Is(err, sqlite.ErrInvalidAdminCommandOutcome) {
				t.Fatalf("tampered pause application error = %v", err)
			}
		})
	}
	unchanged, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	unchangedIntents, err := store.LoadThrottleAttemptRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.AdminCommands[0].State != domain.AdminCommandPending ||
		unchanged.Attempts[0].Control != domain.ControlRunning || len(unchangedIntents) != 0 {
		t.Fatalf("tampered pause application mutated state: records = %#v, intents = %#v", unchanged, unchangedIntents)
	}
	report, err := service.ExecutePendingCommands(context.Background())
	if err != nil || len(report.Decisions) != 1 || report.Decisions[0].Command.State != domain.AdminCommandApplied {
		t.Fatalf("pause execution = %#v, err = %v", report, err)
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	intents, err := store.LoadThrottleAttemptRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Attempts[0].Control != domain.ControlDraining || loaded.Attempts[0].Revision != 4 ||
		len(intents) != 1 || intents[0].Delivery != domain.ThrottleDeliveryPending ||
		intents[0].Command.Kind != domain.ThrottleCommandHardStop ||
		intents[0].Command.AssignmentID != "assignment-1" ||
		intents[0].Command.ThreadID != "thread-1" ||
		intents[0].Command.WorkspacePath != "/runs/run-1/task-1/attempt-1/workspace" ||
		intents[0].Command.Route.QuotaPoolID != "pool" {
		t.Fatalf("pause state = %#v, intents = %#v", loaded.Attempts, intents)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, err = New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := service.Mutate(context.Background(), request); err != nil || replay.Command.State != domain.AdminCommandApplied {
		t.Fatalf("pause replay = %#v, err = %v", replay, err)
	}
	if again, err := service.ExecutePendingCommands(context.Background()); err != nil || len(again.Decisions) != 0 {
		t.Fatalf("pause re-execution = %#v, err = %v", again, err)
	}
	intents, err = store.LoadThrottleAttemptRecords(context.Background())
	if err != nil || len(intents) != 1 {
		t.Fatalf("restart intents = %#v, err = %v", intents, err)
	}
	acknowledged, err := backlog.PlanThrottleAcknowledgements(
		intents,
		[]domain.ThrottleAcknowledgement{{
			CommandID: intents[0].Command.ID, AttemptID: "attempt-1", Accepted: true,
			Result: domain.ThrottleResultStopped, AcknowledgedAt: now.Add(time.Minute),
		}},
		now.Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitThrottleAttemptTransitions(context.Background(), acknowledged); err != nil {
		t.Fatal(err)
	}
	loaded, err = store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Attempts[0].Control != domain.ControlPausedUncheckpointed ||
		loaded.Attempts[0].AssignmentID != "assignment-1" ||
		loaded.Assignments[0].ThreadID != "thread-1" ||
		loaded.Assignments[0].Route.QuotaPoolID != "pool" {
		t.Fatalf("acknowledged pause lost execution identity: %#v", loaded)
	}
}

func TestExecutePendingCancelPersistsDAGCascadeAtomically(t *testing.T) {
	store := openAdminTestStore(t)
	now := adminTestNow
	records := sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-1", Version: 1, Name: "workflow", Class: domain.TaskClassRequired,
			TaskIDs: []string{"task-root", "task-child"},
		}},
		WorkflowRuns: []domain.WorkflowRun{{
			ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive, Revision: 4, CreatedAt: now, UpdatedAt: now,
		}},
		Tasks: []domain.Task{
			{ID: "task-root", WorkflowID: "workflow-1", Name: "root", Class: domain.TaskClassRequired},
			{ID: "task-child", WorkflowID: "workflow-1", Name: "child", Class: domain.TaskClassRequired, Needs: []string{"root"}},
		},
		Attempts: []domain.Attempt{
			{ID: "attempt-root", WorkflowRunID: "run-1", TaskID: "task-root", Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 2, UpdatedAt: now},
			{ID: "attempt-child", WorkflowRunID: "run-1", TaskID: "task-child", Number: 1, Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, Revision: 6, UpdatedAt: now},
		},
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now.Add(time.Minute) })
	request := Mutation{
		Version: Version, Principal: Principal{ID: "operator"}, ID: "cancel-root",
		Kind: domain.AdminCommandCancel, WorkflowRunID: "run-1", TaskID: "task-root",
		ExpectedRevision: 2, Reason: "stop dependent work",
	}
	if _, err := service.Mutate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	report, err := service.ExecutePendingCommands(context.Background())
	if err != nil || len(report.Decisions) != 1 || report.Decisions[0].Command.State != domain.AdminCommandApplied {
		t.Fatalf("execution = %#v, %v", report, err)
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	attempts := map[string]domain.Attempt{}
	for _, attempt := range loaded.Attempts {
		attempts[attempt.ID] = attempt
	}
	if attempts["attempt-root"].Progress != domain.ProgressCancelled || attempts["attempt-root"].Revision != 3 ||
		attempts["attempt-child"].Progress != domain.ProgressCancelled || attempts["attempt-child"].Revision != 7 {
		t.Fatalf("attempts = %#v", attempts)
	}
	if loaded.WorkflowRuns[0].Progress != domain.ProgressCancelled || loaded.WorkflowRuns[0].Revision != 5 ||
		loaded.WorkflowRuns[0].CompletedAt == nil {
		t.Fatalf("run = %#v", loaded.WorkflowRuns[0])
	}
}

func TestExecutePendingManualScheduleRunIsIdempotent(t *testing.T) {
	store := openAdminTestStore(t)
	now := adminTestNow
	records := sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "workflow", Class: domain.TaskClassRequired}},
		Schedules: []domain.Schedule{{
			ID: "schedule-1", Name: "nightly", Version: 1, WorkflowID: "workflow-1", Enabled: true,
			Overlap: domain.ScheduleOverlapForbid, Misfire: domain.ScheduleMisfireSkip,
			AfterFailure: domain.ScheduleFailureNextCycle, Revision: 1, CreatedAt: now, UpdatedAt: now,
		}},
		ScheduleTemplates: []domain.ScheduleTemplate{{
			ScheduleID: "schedule-1", Version: 1, WorkflowID: "workflow-1", Expression: "0 3 * * *", Timezone: "UTC",
			Overlap: domain.ScheduleOverlapForbid, Misfire: domain.ScheduleMisfireSkip,
			AfterFailure: domain.ScheduleFailureNextCycle, CreatedAt: now,
		}},
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now })
	request := Mutation{
		Version: Version, Principal: Principal{ID: "operator"}, ID: "manual-1",
		Kind: domain.AdminCommandScheduleRun, ScheduleID: "schedule-1", ExpectedRevision: 1, Reason: "run now",
	}
	if _, err := service.Mutate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	report, err := service.ExecutePendingCommands(context.Background())
	if err != nil || len(report.Decisions) != 1 || report.Decisions[0].Command.State != domain.AdminCommandApplied {
		t.Fatalf("execution = %#v, %v", report, err)
	}
	if replay, err := service.Mutate(context.Background(), request); err != nil || replay.Command.State != domain.AdminCommandApplied {
		t.Fatalf("replay = %#v, %v", replay, err)
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Triggers) != 1 || len(loaded.WorkflowRuns) != 1 || loaded.Schedules[0].ActiveRunID != loaded.WorkflowRuns[0].ID {
		t.Fatalf("records = %#v", loaded)
	}
}

func TestManualScheduleRunPolicyHonorsFailureHoldAndOpenRun(t *testing.T) {
	schedule := domain.Schedule{ID: "schedule-1", ActiveRunID: "run-1", AfterFailure: domain.ScheduleFailureHold}
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", Progress: domain.ProgressFailed}},
	}
	if err := manualScheduleRunPolicy(records, schedule); err == nil || !strings.Contains(err.Error(), "failure hold") {
		t.Fatalf("failure-hold error = %v", err)
	}
	records.WorkflowRuns[0].Progress = domain.ProgressActive
	if err := manualScheduleRunPolicy(records, schedule); err == nil || !strings.Contains(err.Error(), "open") {
		t.Fatalf("open-run error = %v", err)
	}
	schedule.AfterFailure = domain.ScheduleFailureNextCycle
	records.WorkflowRuns[0].Progress = domain.ProgressSucceeded
	if err := manualScheduleRunPolicy(records, schedule); err != nil {
		t.Fatalf("completed-run policy = %v", err)
	}
}

func TestPlanAdminStartRechecksSafetyAndOnlyBypassesOrdinaryAdmission(t *testing.T) {
	now := adminTestNow
	future := now.Add(time.Hour)
	route := domain.ProviderRoute{WorkerID: "normandy", ProviderInstanceID: "codex", Model: "gpt-5.6-sol", QuotaPoolID: "pool-1"}
	records := sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "workflow", Class: domain.TaskClassSurplus, TaskIDs: []string{"task-1"}}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressQueued, Revision: 1}},
		Tasks: []domain.Task{{
			ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassSurplus,
			NotBefore: &future, Placement: domain.Placement{Hosts: []string{"normandy"}, Capabilities: []string{"internet"}},
			Routes: []domain.ProviderRoute{route},
		}},
		Attempts:   []domain.Attempt{{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1, Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, Revision: 3}},
		QuotaPools: []domain.QuotaPool{{ID: "pool-1", ProviderInstanceIDs: []string{"codex"}, MaxConcurrent: 2, Admission: domain.AdmissionConstrained}},
	}
	workers := []domain.WorkerSnapshot{{
		WorkerID: "normandy", Connected: true, ValidUntil: now.Add(time.Minute),
		Inventory: domain.WorkerInventory{ID: "normandy", AcceptBacklog: true, Health: domain.WorkerHealthReady, Capabilities: []string{"internet"}, Providers: []domain.WorkerProviderInventory{{InstanceID: "codex", Models: []string{"gpt-5.6-sol"}, QuotaPoolID: "pool-1", Available: true}}},
	}}
	command := domain.AdminCommand{
		ID: "start-1", Kind: domain.AdminCommandStart, TargetType: domain.AdminTargetAttempt,
		TargetID: "attempt-1", ExpectedRevision: 3, State: domain.AdminCommandPending, CreatedAt: now,
	}
	application, _, err := planAdminCommand(records, workers, nil, command, now)
	if err != nil {
		t.Fatal(err)
	}
	if application.State != domain.AdminCommandApplied || application.Attempt == nil ||
		!application.Attempt.AdminForceStart || application.Attempt.Progress != domain.ProgressReady ||
		application.SafetyValidUntil == nil || !application.SafetyValidUntil.Equal(workers[0].ValidUntil) {
		t.Fatalf("application = %#v", application)
	}

	records.QuotaPools[0].Admission = domain.AdmissionClosed
	application, _, err = planAdminCommand(records, workers, nil, command, now)
	if err != nil {
		t.Fatal(err)
	}
	if application.State != domain.AdminCommandRejected || !strings.Contains(application.Failure, "closed") {
		t.Fatalf("closed application = %#v", application)
	}

	records.QuotaPools[0].Admission = domain.AdmissionOpen
	application, _, err = planAdminCommand(records, nil, nil, command, now)
	if err != nil {
		t.Fatal(err)
	}
	if application.State != domain.AdminCommandRejected || !strings.Contains(application.Failure, "worker") {
		t.Fatalf("worker application = %#v", application)
	}
}

func TestPlanAdminStartAllowsAnOpenAlternativeQuotaRoute(t *testing.T) {
	now := adminTestNow
	task := domain.Task{
		ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassRequired,
		Routes: []domain.ProviderRoute{
			{WorkerID: "normandy", ProviderInstanceID: "codex-primary", Model: "model", QuotaPoolID: "pool-closed"},
			{WorkerID: "normandy", ProviderInstanceID: "codex-fallback", Model: "model", QuotaPoolID: "pool-open"},
		},
	}
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressQueued, Revision: 1}},
		Tasks:        []domain.Task{task},
		Attempts:     []domain.Attempt{{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, Revision: 2}},
		QuotaPools: []domain.QuotaPool{
			{ID: "pool-closed", ProviderInstanceIDs: []string{"codex-primary"}, MaxConcurrent: 2, Admission: domain.AdmissionClosed},
			{ID: "pool-open", ProviderInstanceIDs: []string{"codex-fallback"}, MaxConcurrent: 2, Admission: domain.AdmissionOpen},
		},
	}
	workers := []domain.WorkerSnapshot{{
		WorkerID: "normandy", Connected: true, ValidUntil: now.Add(time.Minute),
		Inventory: domain.WorkerInventory{ID: "normandy", AcceptBacklog: true, Health: domain.WorkerHealthReady, Providers: []domain.WorkerProviderInventory{
			{InstanceID: "codex-primary", Models: []string{"model"}, QuotaPoolID: "pool-closed", Available: true},
			{InstanceID: "codex-fallback", Models: []string{"model"}, QuotaPoolID: "pool-open", Available: true},
		}},
	}}
	command := domain.AdminCommand{
		ID: "start-alternative", Kind: domain.AdminCommandStart, TargetType: domain.AdminTargetAttempt,
		TargetID: "attempt-1", ExpectedRevision: 2, State: domain.AdminCommandPending, CreatedAt: now,
	}
	application, _, err := planAdminCommand(records, workers, nil, command, now)
	if err != nil {
		t.Fatal(err)
	}
	if application.State != domain.AdminCommandApplied {
		t.Fatalf("application = %#v", application)
	}
	records.Tasks[0].Routes[1].WorkerID = "huyang"
	application, _, err = planAdminCommand(records, workers, nil, command, now)
	if err != nil {
		t.Fatal(err)
	}
	if application.State != domain.AdminCommandRejected || !strings.Contains(application.Failure, "closed") {
		t.Fatalf("cross-route application = %#v", application)
	}
}

func TestPlanAdminStartDerivesQuotaPoolFromWorkerInventory(t *testing.T) {
	now := adminTestNow
	task := domain.Task{
		ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassRequired,
		Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "model"}},
	}
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressQueued, Revision: 1}},
		Tasks:        []domain.Task{task},
		Attempts:     []domain.Attempt{{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, Revision: 2}},
		QuotaPools: []domain.QuotaPool{{
			ID: "pool-derived", ProviderInstanceIDs: []string{"codex"}, MaxConcurrent: 2, Admission: domain.AdmissionOpen,
		}},
	}
	workers := []domain.WorkerSnapshot{{
		WorkerID: "normandy", Connected: true, ValidUntil: now.Add(time.Minute),
		Inventory: domain.WorkerInventory{
			ID: "normandy", AcceptBacklog: true, Health: domain.WorkerHealthReady,
			Providers: []domain.WorkerProviderInventory{{
				InstanceID: "codex", Models: []string{"model"}, QuotaPoolID: "pool-derived", Available: true,
			}},
		},
	}}
	command := domain.AdminCommand{
		ID: "start-derived-pool", Kind: domain.AdminCommandStart, TargetType: domain.AdminTargetAttempt,
		TargetID: "attempt-1", ExpectedRevision: 2, State: domain.AdminCommandPending, CreatedAt: now,
	}
	application, _, err := planAdminCommand(records, workers, nil, command, now)
	if err != nil {
		t.Fatal(err)
	}
	if application.State != domain.AdminCommandApplied {
		t.Fatalf("application = %#v", application)
	}
}

func TestExecutePendingStartReplansWhenHardQuotaClosesBeforeApply(t *testing.T) {
	store := openAdminTestStore(t)
	now := adminTestNow
	task := domain.Task{
		ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassRequired,
		Routes: []domain.ProviderRoute{{
			WorkerID: "normandy", ProviderInstanceID: "codex", Model: "model", QuotaPoolID: "pool",
		}},
	}
	pool := domain.QuotaPool{
		ID: "pool", ProviderInstanceIDs: []string{"codex"}, MaxConcurrent: 2,
		Admission: domain.AdmissionOpen, UpdatedAt: now,
	}
	command := domain.AdminCommand{
		ID: "start-race", Kind: domain.AdminCommandStart, TargetType: domain.AdminTargetAttempt,
		TargetID: "attempt-1", ExpectedRevision: 2, Reason: "start safely",
		RequestedBy: "operator", State: domain.AdminCommandPending, CreatedAt: now,
	}
	records := sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "workflow", Class: domain.TaskClassRequired, TaskIDs: []string{task.ID}}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressQueued, Revision: 1}},
		Tasks:        []domain.Task{task},
		Attempts: []domain.Attempt{{
			ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1,
			Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, Revision: 2,
		}},
		QuotaPools:    []domain.QuotaPool{pool},
		AdminCommands: []domain.AdminCommand{command},
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	worker := domain.WorkerSnapshot{
		WorkerID: "normandy", WorkerEpoch: "worker-epoch", CoordinatorEpoch: 1, Sequence: 1,
		Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Minute),
		Inventory: domain.WorkerInventory{
			ID: "normandy", AcceptBacklog: true, Health: domain.WorkerHealthReady,
			Providers: []domain.WorkerProviderInventory{{
				InstanceID: "codex", Models: []string{"model"}, QuotaPoolID: "pool", Available: true,
			}},
		},
	}
	if err := store.SaveWorkerSnapshot(context.Background(), worker); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	workers, err := store.LoadWorkerSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	application, _, err := planAdminCommand(loaded, workers, nil, command, now)
	if err != nil || application.State != domain.AdminCommandApplied || application.SafetyFingerprint == "" {
		t.Fatalf("planned application = %#v, err = %v", application, err)
	}

	pool.Admission = domain.AdmissionClosed
	pool.UpdatedAt = now.Add(time.Second)
	if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{QuotaPools: []domain.QuotaPool{pool}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyAdminCommand(context.Background(), application); !errors.Is(err, sqlite.ErrStaleAdminSafetyFence) {
		t.Fatalf("stale safety apply error = %v", err)
	}
	unchanged, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.AdminCommands[0].State != domain.AdminCommandPending || unchanged.Attempts[0].Revision != 2 {
		t.Fatalf("stale safety application mutated state: %#v", unchanged)
	}

	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now.Add(2 * time.Second) })
	report, err := service.ExecutePendingCommands(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Decisions) != 1 || report.Decisions[0].Command.State != domain.AdminCommandRejected ||
		!strings.Contains(report.Decisions[0].Command.Failure, "closed") {
		t.Fatalf("replanned report = %#v", report)
	}
}

func TestPlanAdminResumeUsesAssignedQuotaRoute(t *testing.T) {
	now := adminTestNow
	task := domain.Task{
		ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassRequired,
		Routes: []domain.ProviderRoute{
			{WorkerID: "normandy", ProviderInstanceID: "codex-primary", Model: "model", QuotaPoolID: "pool-closed"},
			{WorkerID: "normandy", ProviderInstanceID: "codex-fallback", Model: "model", QuotaPoolID: "pool-open"},
		},
	}
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive, Revision: 1}},
		Tasks:        []domain.Task{task},
		Attempts: []domain.Attempt{{
			ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1,
			Progress: domain.ProgressActive, Control: domain.ControlPaused, Revision: 2, AssignmentID: "assignment-1",
		}},
		Assignments: []domain.Assignment{{
			ID: "assignment-1", AttemptID: "attempt-1", WorkerID: "normandy",
			Route: domain.ProviderRoute{QuotaPoolID: "pool-closed"}, State: domain.AssignmentClaimed,
		}},
		QuotaPools: []domain.QuotaPool{
			{ID: "pool-closed", Admission: domain.AdmissionClosed},
			{ID: "pool-open", Admission: domain.AdmissionOpen},
		},
	}
	workers := []domain.WorkerSnapshot{{
		WorkerID: "normandy", Connected: true, ValidUntil: now.Add(time.Minute),
		Inventory: domain.WorkerInventory{ID: "normandy", AcceptBacklog: true, Health: domain.WorkerHealthReady},
	}}
	command := domain.AdminCommand{
		ID: "resume-assigned", Kind: domain.AdminCommandResume, TargetType: domain.AdminTargetAttempt,
		TargetID: "attempt-1", ExpectedRevision: 2, State: domain.AdminCommandPending, CreatedAt: now,
	}
	application, _, err := planAdminCommand(records, workers, nil, command, now)
	if err != nil {
		t.Fatal(err)
	}
	if application.State != domain.AdminCommandRejected || !strings.Contains(application.Failure, "pool-closed=closed") {
		t.Fatalf("application = %#v", application)
	}
	records.QuotaPools[0].Admission = domain.AdmissionOpen
	application, _, err = planAdminCommand(records, nil, nil, command, now)
	if err != nil {
		t.Fatal(err)
	}
	if application.State != domain.AdminCommandRejected || !strings.Contains(application.Failure, "assigned worker") {
		t.Fatalf("missing worker application = %#v", application)
	}
}

func TestPlanAdminStartIgnoresReleasedLockOwner(t *testing.T) {
	now := adminTestNow
	task := domain.Task{
		ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassRequired,
		ResourceLocks: []string{"repository"}, Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "model", QuotaPoolID: "pool-1"}},
	}
	otherTask := domain.Task{
		ID: "task-2", WorkflowID: "workflow-1", Name: "other", Class: domain.TaskClassRequired,
		ResourceLocks: []string{"repository"},
	}
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressQueued, Revision: 1}},
		Tasks:        []domain.Task{task, otherTask},
		Attempts: []domain.Attempt{
			{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, Revision: 2},
			{ID: "attempt-2", WorkflowRunID: "run-1", TaskID: otherTask.ID, Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 2, AssignmentID: "assignment-2"},
		},
		Assignments: []domain.Assignment{{ID: "assignment-2", AttemptID: "attempt-2", State: domain.AssignmentReleased}},
		QuotaPools:  []domain.QuotaPool{{ID: "pool-1", ProviderInstanceIDs: []string{"codex"}, MaxConcurrent: 2, Admission: domain.AdmissionOpen}},
	}
	workers := []domain.WorkerSnapshot{{
		WorkerID: "normandy", Connected: true, ValidUntil: now.Add(time.Minute),
		Inventory: domain.WorkerInventory{ID: "normandy", AcceptBacklog: true, Health: domain.WorkerHealthReady, Providers: []domain.WorkerProviderInventory{{InstanceID: "codex", Models: []string{"model"}, QuotaPoolID: "pool-1", Available: true}}},
	}}
	command := domain.AdminCommand{
		ID: "start-released-lock", Kind: domain.AdminCommandStart, TargetType: domain.AdminTargetAttempt,
		TargetID: "attempt-1", ExpectedRevision: 2, State: domain.AdminCommandPending, CreatedAt: now,
	}
	application, _, err := planAdminCommand(records, workers, nil, command, now)
	if err != nil {
		t.Fatal(err)
	}
	if application.State != domain.AdminCommandApplied {
		t.Fatalf("released lock application = %#v", application)
	}

	records.Assignments[0].State = domain.AssignmentClaimed
	application, _, err = planAdminCommand(records, workers, nil, command, now)
	if err != nil {
		t.Fatal(err)
	}
	if application.State != domain.AdminCommandRejected || !strings.Contains(application.Failure, "resource lock") {
		t.Fatalf("claimed lock application = %#v", application)
	}
}

func TestPlanAdminAttemptTransitionsAndPayloads(t *testing.T) {
	now := adminTestNow
	base := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive, Revision: 2}},
		Tasks:        []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassRequired}},
	}
	tests := []struct {
		name         string
		kind         domain.AdminCommandKind
		progress     domain.ProgressState
		control      domain.ControlState
		payload      json.RawMessage
		wantProgress domain.ProgressState
		wantControl  domain.ControlState
		newAttempt   bool
	}{
		{name: "delay", kind: domain.AdminCommandDelay, progress: domain.ProgressReady, control: domain.ControlUnassigned, payload: json.RawMessage(`{"until":"2026-09-10T22:00:00Z"}`), wantProgress: domain.ProgressReady, wantControl: domain.ControlUnassigned},
		{name: "pause", kind: domain.AdminCommandPause, progress: domain.ProgressActive, control: domain.ControlRunning, payload: json.RawMessage(`{"now":false}`), wantProgress: domain.ProgressActive, wantControl: domain.ControlDraining},
		{name: "pause now", kind: domain.AdminCommandPause, progress: domain.ProgressActive, control: domain.ControlRunning, payload: json.RawMessage(`{"now":true}`), wantProgress: domain.ProgressActive, wantControl: domain.ControlDraining},
		{name: "cancel", kind: domain.AdminCommandCancel, progress: domain.ProgressActive, control: domain.ControlRunning, wantProgress: domain.ProgressCancelled, wantControl: domain.ControlStopped},
		{name: "skip failed", kind: domain.AdminCommandSkip, progress: domain.ProgressFailed, control: domain.ControlStopped, wantProgress: domain.ProgressSkipped, wantControl: domain.ControlStopped},
		{name: "retry", kind: domain.AdminCommandRetry, progress: domain.ProgressFailed, control: domain.ControlStopped, wantProgress: domain.ProgressFailed, wantControl: domain.ControlStopped, newAttempt: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			records := base
			records.Attempts = []domain.Attempt{{
				ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1,
				Progress: test.progress, Control: test.control, Revision: 4,
			}}
			var workers []domain.WorkerSnapshot
			if test.kind == domain.AdminCommandPause {
				records.Attempts[0].AssignmentID = "assignment-1"
				records.Assignments = []domain.Assignment{{
					ID: "assignment-1", AttemptID: "attempt-1", WorkerID: "normandy",
					WorkerEpoch: "worker-epoch", Route: domain.ProviderRoute{QuotaPoolID: "pool"},
					State: domain.AssignmentClaimed, Epoch: 1, ThreadID: "thread-1",
				}}
				workers = []domain.WorkerSnapshot{{
					WorkerID: "normandy", WorkerEpoch: "worker-epoch",
					Assignments: []domain.WorkerAssignmentObservation{{
						AssignmentID: "assignment-1", AssignmentEpoch: 1,
						ThreadID: "thread-1", WorkspacePath: "/runs/attempt-1/workspace",
					}},
				}}
			}
			command := domain.AdminCommand{
				ID: "command-" + test.name, Kind: test.kind, TargetType: domain.AdminTargetAttempt,
				TargetID: "attempt-1", ExpectedRevision: 4, Reason: "test", Payload: test.payload,
				State: domain.AdminCommandPending, CreatedAt: now,
			}
			application, _, err := planAdminCommand(records, workers, nil, command, now)
			if err != nil {
				t.Fatal(err)
			}
			if application.State != domain.AdminCommandApplied || application.Attempt == nil ||
				application.Attempt.Progress != test.wantProgress || application.Attempt.Control != test.wantControl ||
				(application.NewAttempt != nil) != test.newAttempt {
				t.Fatalf("application = %#v", application)
			}
			if test.kind == domain.AdminCommandDelay && (application.Attempt.AdminNotBefore == nil || !application.Attempt.AdminNotBefore.Equal(now.Add(2*time.Hour))) {
				t.Fatalf("delay = %#v", application.Attempt.AdminNotBefore)
			}
			if test.kind == domain.AdminCommandPause {
				wantKind := domain.ThrottleCommandDrain
				if test.name == "pause now" {
					wantKind = domain.ThrottleCommandHardStop
				}
				if application.PauseIntent == nil || application.PauseIntent.Record.Command.Kind != wantKind {
					t.Fatalf("pause intent = %#v", application.PauseIntent)
				}
			}
			if test.newAttempt && (application.NewAttempt.Number != 2 || application.NewAttempt.Revision != 1 || application.WorkflowRun == nil) {
				t.Fatalf("retry = %#v", application)
			}
		})
	}
}

func TestPlanAdminPauseRejectsChangedWorkerBinding(t *testing.T) {
	now := adminTestNow
	attempt := domain.Attempt{
		ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1,
		Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 4,
		AssignmentID: "assignment-1",
	}
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive, Revision: 1}},
		Tasks:        []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassRequired}},
		Attempts:     []domain.Attempt{attempt},
		Assignments: []domain.Assignment{{
			ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "normandy", WorkerEpoch: "worker-epoch",
			Route: domain.ProviderRoute{QuotaPoolID: "pool"}, State: domain.AssignmentClaimed,
			Epoch: 2, ThreadID: "thread-1",
		}},
	}
	workers := []domain.WorkerSnapshot{{
		WorkerID: "normandy", WorkerEpoch: "worker-epoch",
		Assignments: []domain.WorkerAssignmentObservation{{
			AssignmentID: "assignment-1", AssignmentEpoch: 2,
			ThreadID: "another-thread", WorkspacePath: "/runs/attempt-1/workspace",
		}},
	}}
	application, _, err := planAdminCommand(records, workers, nil, domain.AdminCommand{
		ID: "pause-invalid-binding", Kind: domain.AdminCommandPause, TargetType: domain.AdminTargetAttempt,
		TargetID: attempt.ID, ExpectedRevision: attempt.Revision, Reason: "pause", State: domain.AdminCommandPending,
		CreatedAt: now, Payload: json.RawMessage(`{"now":false}`),
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if application.State != domain.AdminCommandRejected ||
		!strings.Contains(application.Failure, "identity is incomplete or changed") ||
		application.PauseIntent != nil {
		t.Fatalf("invalid binding application = %#v", application)
	}
}

func TestPlanAdminRejectsInvalidAttemptTransitions(t *testing.T) {
	now := adminTestNow
	tests := []struct {
		name     string
		kind     domain.AdminCommandKind
		progress domain.ProgressState
		control  domain.ControlState
		payload  json.RawMessage
	}{
		{name: "start assigned", kind: domain.AdminCommandStart, progress: domain.ProgressActive, control: domain.ControlRunning},
		{name: "pause unassigned", kind: domain.AdminCommandPause, progress: domain.ProgressReady, control: domain.ControlUnassigned, payload: json.RawMessage(`{"now":false}`)},
		{name: "pause already draining", kind: domain.AdminCommandPause, progress: domain.ProgressActive, control: domain.ControlDraining, payload: json.RawMessage(`{"now":true}`)},
		{name: "resume running", kind: domain.AdminCommandResume, progress: domain.ProgressActive, control: domain.ControlRunning},
		{name: "retry ready", kind: domain.AdminCommandRetry, progress: domain.ProgressReady, control: domain.ControlUnassigned},
		{name: "skip succeeded", kind: domain.AdminCommandSkip, progress: domain.ProgressSucceeded, control: domain.ControlStopped},
		{name: "delay malformed", kind: domain.AdminCommandDelay, progress: domain.ProgressReady, control: domain.ControlUnassigned, payload: json.RawMessage(`{"until":"later"}`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			records := sqlite.CoordinatorRecords{
				WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive, Revision: 1}},
				Tasks:        []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "task"}},
				Attempts: []domain.Attempt{{
					ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1,
					Progress: test.progress, Control: test.control, Revision: 2,
				}},
			}
			command := domain.AdminCommand{
				ID: "invalid-" + test.name, Kind: test.kind, TargetType: domain.AdminTargetAttempt,
				TargetID: "attempt-1", ExpectedRevision: 2, State: domain.AdminCommandPending,
				Payload: test.payload, CreatedAt: now,
			}
			application, _, err := planAdminCommand(records, nil, nil, command, now)
			if err != nil {
				t.Fatal(err)
			}
			if application.State != domain.AdminCommandRejected || application.Failure == "" || application.Attempt != nil {
				t.Fatalf("application = %#v", application)
			}
		})
	}
}

func TestPlanAdminCancelCascadesThroughDAGAndProjectsRun(t *testing.T) {
	now := adminTestNow
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{
			ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive, Revision: 7,
		}},
		Tasks: []domain.Task{
			{ID: "task-root", WorkflowID: "workflow-1", Name: "root"},
			{ID: "task-child", WorkflowID: "workflow-1", Name: "child", Needs: []string{"root"}},
			{ID: "task-sibling", WorkflowID: "workflow-1", Name: "sibling"},
		},
		Attempts: []domain.Attempt{
			{ID: "attempt-root", WorkflowRunID: "run-1", TaskID: "task-root", Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 3},
			{ID: "attempt-child", WorkflowRunID: "run-1", TaskID: "task-child", Number: 1, Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, Revision: 5},
			{ID: "attempt-sibling", WorkflowRunID: "run-1", TaskID: "task-sibling", Number: 1, Progress: domain.ProgressSucceeded, Control: domain.ControlStopped, Revision: 2},
		},
	}
	command := domain.AdminCommand{
		ID: "cancel-root", Kind: domain.AdminCommandCancel, TargetType: domain.AdminTargetAttempt,
		TargetID: "attempt-root", ExpectedRevision: 3, State: domain.AdminCommandPending, CreatedAt: now,
	}
	application, _, err := planAdminCommand(records, nil, nil, command, now)
	if err != nil {
		t.Fatal(err)
	}
	if application.State != domain.AdminCommandApplied || application.Attempt == nil ||
		application.Attempt.Progress != domain.ProgressCancelled || application.Attempt.Revision != 4 {
		t.Fatalf("target application = %#v", application)
	}
	if len(application.RelatedAttempts) != 1 || application.RelatedAttempts[0].ID != "attempt-child" ||
		application.RelatedAttempts[0].Progress != domain.ProgressCancelled || application.RelatedAttempts[0].Revision != 6 {
		t.Fatalf("related attempts = %#v", application.RelatedAttempts)
	}
	if application.WorkflowRun == nil || application.WorkflowRun.Progress != domain.ProgressCancelled ||
		application.WorkflowRun.Revision != 8 || application.WorkflowRun.CompletedAt == nil {
		t.Fatalf("workflow run = %#v", application.WorkflowRun)
	}
}

func TestPlanAdminScheduleCommandsUseDeterministicManualIDs(t *testing.T) {
	now := adminTestNow
	records := sqlite.CoordinatorRecords{Schedules: []domain.Schedule{{
		ID: "schedule-1", Enabled: true, Revision: 5,
	}}}
	command := domain.AdminCommand{
		ID: "manual-1", Kind: domain.AdminCommandScheduleRun, TargetType: domain.AdminTargetSchedule,
		TargetID: "schedule-1", ExpectedRevision: 5, State: domain.AdminCommandPending, CreatedAt: now,
	}
	first, trigger, err := planAdminCommand(records, nil, nil, command, now)
	if err != nil {
		t.Fatal(err)
	}
	second, replay, err := planAdminCommand(records, nil, nil, command, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if first.State != domain.AdminCommandApplied || second.State != domain.AdminCommandApplied || trigger == nil || replay == nil ||
		trigger.TriggerID != replay.TriggerID || trigger.WorkflowRunID != replay.WorkflowRunID ||
		trigger.Source != domain.ScheduleTriggerManual {
		t.Fatalf("triggers = %#v %#v", trigger, replay)
	}

	delay := command
	delay.ID, delay.Kind, delay.Payload = "delay-next-1", domain.AdminCommandDelayNext, json.RawMessage(`{"until":"2026-09-10T22:30:00Z"}`)
	application, trigger, err := planAdminCommand(records, nil, nil, delay, now)
	if err != nil {
		t.Fatal(err)
	}
	if application.Schedule == nil || application.Schedule.NextNotBefore == nil || trigger != nil ||
		!application.Schedule.NextNotBefore.Equal(time.Date(2026, 9, 10, 22, 30, 0, 0, time.UTC)) {
		t.Fatalf("delay next = %#v, %#v", application, trigger)
	}
}
