package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestReviewRegressionReplayRejectsChangedEventIdentity(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	recovery := domain.RecoveryIncident{
		Contract: domain.RecoveryContractV1, Purpose: domain.RecoveryActivationRepair,
		Owner: domain.RecoveryOwner{Role: domain.RecoveryRoleRepairExecutor, Route: domain.ProviderRoute{ProviderInstanceID: "repairer", Model: "model"}, PromptArtifactID: "prompt"},
		State: domain.RecoveryPendingDispatch, NextAction: domain.RecoveryDispatchRepair,
		AttemptBudget: 3, Deadline: now.Add(time.Hour), LastProgressAt: now,
		Diagnostic: domain.RecoveryDiagnosticIdentity{FailureFingerprint: "failure", EvidenceFingerprint: "evidence", StrategyFingerprint: "strategy"},
	}
	config := domain.RecoveryConfig{Version: domain.RecoveryContractV1, Route: recovery.Owner.Route, PromptArtifactID: recovery.Owner.PromptArtifactID, MaxAttemptsPerIncident: 3, IncidentDeadline: time.Hour, StalledAfter: time.Minute}
	prepareRecoveryFence(t, store, "run", "attempt", now, config)
	request := RecoveryIncidentRequest{RunID: "run", IncidentID: "incident", EventID: "event-1", SourceTaskID: "task", SourceAttemptID: "attempt", ExpectedGraphRevision: 1, SourceAttemptRevision: 1, RecoveryConfig: config, Reason: "failed", Recovery: recovery, EventRecord: []byte(`{"id":"event-1"}`), OpenedAt: now}
	if _, _, err := store.OpenRecoveryIncident(ctx, request); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, "DELETE FROM coordinator_supervision_inbox WHERE id = ?", request.EventID); err != nil {
		t.Fatal(err)
	}
	request.EventID = "event-2"
	request.EventRecord = []byte(`{"id":"event-2"}`)
	if _, _, err := store.OpenRecoveryIncident(ctx, request); !errors.Is(err, ErrSupervisionRequestConflict) {
		t.Fatalf("changed event identity error = %v, want conflict", err)
	}
}
func prepareRecoveryFence(t *testing.T, store *Store, runID, attemptID string, now time.Time, config domain.RecoveryConfig) {
	t.Helper()
	run := domain.WorkflowRun{ID: runID, WorkflowID: "workflow-" + runID, GraphRevision: 1, Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now}
	attempt := domain.Attempt{ID: attemptID, WorkflowRunID: runID, TaskID: "task", Number: 1, Progress: domain.ProgressFailed, Control: domain.ControlStopped, Revision: 1, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}, Attempts: []domain.Attempt{attempt}}); err != nil {
		t.Fatal(err)
	}
	supervision := domain.SupervisionConfig{
		Route:            domain.ProviderRoute{ProviderInstanceID: "reviewer", Model: "review-model"},
		PromptArtifactID: "review-prompt", MaxActivations: 2, MaxTurnsPerActivation: 2,
		ActivationDeadline: time.Hour, Recovery: &config,
	}
	if _, err := store.PutSupervision(context.Background(), SupervisionMaterialization{Record: domain.SupervisionRecord{RunID: runID, Config: supervision}}); err != nil {
		t.Fatal(err)
	}
}
