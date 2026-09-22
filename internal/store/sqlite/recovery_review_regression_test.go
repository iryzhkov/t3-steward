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
	request := RecoveryIncidentRequest{RunID: "run", IncidentID: "incident", EventID: "event-1", SourceTaskID: "task", SourceAttemptID: "attempt", Reason: "failed", Recovery: recovery, EventRecord: []byte(`{"id":"event-1"}`), OpenedAt: now}
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
