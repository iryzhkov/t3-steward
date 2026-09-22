package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestReviewRegressionEventIDCollisionAcrossRunsIsRejectedAtomically(t *testing.T) {
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
	prepareRecoveryFence(t, store, "run-a", "attempt-a", now, config)
	prepareRecoveryFence(t, store, "run-b", "attempt-b", now, config)
	request := RecoveryIncidentRequest{RunID: "run-a", IncidentID: "incident-a", EventID: "shared-event", SourceTaskID: "task", SourceAttemptID: "attempt-a", ExpectedGraphRevision: 1, SourceAttemptRevision: 1, RecoveryConfig: config, Reason: "failed", Recovery: recovery, EventRecord: []byte(`{"id":"shared-event","runId":"run-a"}`), OpenedAt: now}
	if _, _, err := store.OpenRecoveryIncident(ctx, request); err != nil {
		t.Fatal(err)
	}
	request.RunID, request.IncidentID, request.SourceAttemptID = "run-b", "incident-b", "attempt-b"
	request.EventRecord = []byte(`{"id":"shared-event","runId":"run-b"}`)
	if _, _, err := store.OpenRecoveryIncident(ctx, request); err == nil {
		t.Fatal("cross-run event collision accepted")
	}
}
