package main

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestRecoveryObserverCreatesOneAtomicIncidentAndEventAcrossReplay(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	config := recoveryTestSupervisionConfig()
	run := domain.WorkflowRun{
		ID: "run-recovery", WorkflowID: "workflow-recovery", GraphRevision: 1,
		Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now,
		Supervision: &domain.SupervisionRecord{RunID: "run-recovery", Config: config},
	}
	taskIDs := []string{"failed-root", "parallel"}
	tasks := []domain.Task{
		{ID: "failed-root", WorkflowID: run.WorkflowID, Name: "failed root", Class: domain.TaskClassRequired},
		{ID: "parallel", WorkflowID: run.WorkflowID, Name: "parallel", Class: domain.TaskClassRequired},
	}
	attempts := []domain.Attempt{
		{ID: "attempt-root", WorkflowRunID: run.ID, TaskID: "failed-root", Number: 1, Progress: domain.ProgressFailed, Control: domain.ControlStopped, Failure: "compile failed", Revision: 1, UpdatedAt: now},
		{ID: "attempt-parallel", WorkflowRunID: run.ID, TaskID: "parallel", Number: 1, Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, UpdatedAt: now},
	}
	for i := 0; i < 42; i++ {
		id := fmt.Sprintf("blocked-%02d", i)
		taskIDs = append(taskIDs, id)
		tasks = append(tasks, domain.Task{ID: id, WorkflowID: run.WorkflowID, Name: id, Class: domain.TaskClassRequired, Needs: []string{"failed-root"}})
		attempts = append(attempts, domain.Attempt{ID: "attempt-" + id, WorkflowRunID: run.ID, TaskID: id, Number: 1, Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, Revision: 1, UpdatedAt: now})
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: run.WorkflowID, Version: 1, Name: "workflow", Class: domain.TaskClassRequired, TaskIDs: taskIDs, CreatedAt: now}},
		WorkflowRuns: []domain.WorkflowRun{run}, Tasks: tasks, Attempts: attempts,
		Artifacts: []domain.Artifact{{ID: "failure-log", WorkflowRunID: run.ID, TaskID: "failed-root", AttemptID: "attempt-root", Kind: domain.ArtifactLog, Name: "failure", SHA256: "abc123", StoragePath: "objects/abc123", CreatedAt: now}},
	}); err != nil {
		t.Fatal(err)
	}
	supervision := backlog.CoordinatorSupervisionStore{Store: store}
	if _, err := store.PutSupervision(ctx, sqlite.SupervisionMaterialization{Record: *run.Supervision}); err != nil {
		t.Fatal(err)
	}
	coordinator := coordinatorSupervision{store: supervision}
	if err := coordinator.observeRecoveryFailures(ctx, run, now); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.observeRecoveryFailures(ctx, run, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	state, err := supervision.LoadSupervisionAdminState(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Incidents) != 1 {
		t.Fatalf("incidents=%d want 1", len(state.Incidents))
	}
	recovery := state.Incidents[0].Incident.Recovery
	if recovery == nil {
		t.Fatal("recovery metadata missing")
	}
	if recovery.State != domain.RecoveryPendingDispatch || recovery.NextAction != domain.RecoveryDispatchRepair || recovery.Owner.Role != domain.RecoveryRoleRepairExecutor {
		t.Fatalf("recovery=%+v", *recovery)
	}
	if recovery.Diagnostic.FailureFingerprint == "" || recovery.Diagnostic.EvidenceFingerprint == "" || recovery.Diagnostic.StrategyFingerprint == "" {
		t.Fatalf("diagnostic identity incomplete: %+v", recovery.Diagnostic)
	}
	inbox, err := supervision.ListSupervisionInbox(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox) != 1 {
		t.Fatalf("inbox=%d want 1", len(inbox))
	}
	var event backlog.SupervisionEvent
	if err := json.Unmarshal(inbox[0].Record, &event); err != nil {
		t.Fatal(err)
	}
	if event.Kind != backlog.TriggerTaskJudgmentRequired || event.TaskID != "failed-root" || len(event.Artifacts) != 1 || event.Artifacts[0].Digest != "abc123" {
		t.Fatalf("event=%+v", event)
	}

	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range records.Attempts {
		if attempt.TaskID == "parallel" && attempt.Progress != domain.ProgressReady {
			t.Fatalf("parallel branch changed: %+v", attempt)
		}
	}
}

func TestRecoveryObserverRequiresExplicitOptIn(t *testing.T) {
	run := domain.WorkflowRun{ID: "legacy", Supervision: &domain.SupervisionRecord{Config: domain.SupervisionConfig{}}}
	if err := (coordinatorSupervision{}).observeRecoveryFailures(context.Background(), run, time.Now()); err != nil {
		t.Fatal(err)
	}
}

func recoveryTestSupervisionConfig() domain.SupervisionConfig {
	return domain.SupervisionConfig{
		Route:            domain.ProviderRoute{ProviderInstanceID: "reviewer", Model: "review-model", QuotaPoolID: "recovery-pool"},
		PromptArtifactID: "review-prompt", MaxActivations: 4, MaxTurnsPerActivation: 4, ActivationDeadline: time.Hour,
		Recovery: &domain.RecoveryConfig{
			Version:          domain.RecoveryContractV1,
			Route:            domain.ProviderRoute{ProviderInstanceID: "repairer", Model: "repair-model", QuotaPoolID: "recovery-pool"},
			PromptArtifactID: "repair-prompt", MaxAttemptsPerIncident: 3,
			IncidentDeadline: 24 * time.Hour, StalledAfter: 2 * time.Hour,
		},
	}
}
