package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestRecoveryRetryIsAtomicScopedAndIdempotent(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 21, 14, 0, 0, 0, time.UTC)
	store.SetClock(func() time.Time { return now.Add(time.Minute) })
	config := domain.RecoveryConfig{Version: domain.RecoveryContractV1,
		Route: domain.ProviderRoute{ProviderInstanceID: "repairer", Model: "model"}, PromptArtifactID: "repair-prompt",
		MaxAttemptsPerIncident: 2, IncidentDeadline: time.Hour, StalledAfter: time.Minute}
	prepareRecoveryFence(t, store, "run", "attempt-1", now, config)
	artifact := domain.Artifact{ID: "instruction", WorkflowRunID: "run", TaskID: "task", AttemptID: "attempt-1",
		Kind: domain.ArtifactCheckpoint, Name: "repair", SHA256: "content-a", StoragePath: "objects/content-a", CreatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Tasks: []domain.Task{{ID: "task", WorkflowID: "workflow-run", Name: "task", Class: domain.TaskClassRequired}}, Artifacts: []domain.Artifact{artifact}}); err != nil {
		t.Fatal(err)
	}
	recovery := domain.NewRecoveryIncident(config, domain.RecoveryDiagnosticIdentity{
		FailureFingerprint: "check-category", EvidenceFingerprint: "content-old", StrategyFingerprint: "strategy-old"}, now)
	event := []byte(`{"id":"event","runId":"run"}`)
	if _, _, err := store.OpenRecoveryIncident(ctx, RecoveryIncidentRequest{
		RunID: "run", IncidentID: "incident", EventID: "event", SourceTaskID: "task", SourceAttemptID: "attempt-1",
		ExpectedGraphRevision: 1, SourceAttemptRevision: 1, RecoveryConfig: config,
		Reason: "failed", Recovery: *recovery, EventRecord: event, OpenedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	expires := now.Add(time.Hour)
	activation := domain.Activation{ID: "activation", RunID: "run", Epoch: 1, Purpose: domain.RecoveryActivationRepair, GraphRevision: 1,
		Principal: "repair-principal", State: domain.ActivationActive, LeaseToken: "lease", LeaseExpiresAt: &expires, IncidentID: "incident"}
	raw, _ := json.Marshal(activation)
	if _, err := store.db.ExecContext(ctx, `INSERT INTO coordinator_supervision_activations(id, run_id, epoch, state, dispatch_identity, record)
		VALUES (?, ?, ?, ?, ?, ?)`, activation.ID, activation.RunID, activation.Epoch, activation.State, "dispatch", raw); err != nil {
		t.Fatal(err)
	}
	strategy := domain.RecoveryStrategyFingerprint(domain.ArtifactDigest{ArtifactID: "instruction", Digest: "content-a"}, nil)
	request := domain.RecoveryRetryRequest{
		OperationID: "repair-op", RunID: "run", IncidentID: "incident", ExpectedIncidentRevision: 1,
		ActivationID: "activation", ActivationEpoch: 1, Principal: "repair-principal",
		SourceAttemptID: "attempt-1", SourceAttemptRevision: 1,
		InstructionArtifact: domain.ArtifactDigest{ArtifactID: "instruction", Digest: "content-a"},
		Diagnostic:          domain.RecoveryDiagnosticIdentity{FailureFingerprint: "check-category", EvidenceFingerprint: "content-old", StrategyFingerprint: strategy},
		RequestedAt:         now.Add(time.Minute),
	}
	claimed := request
	claimed.OperationID = "claimed-strategy"
	claimed.Diagnostic.StrategyFingerprint = "caller-controlled"
	if _, err := store.CommitRecoveryRetry(ctx, claimed); err == nil {
		t.Fatal("caller-controlled strategy fingerprint accepted")
	}
	forgedEvidence := request
	forgedEvidence.OperationID = "forged-evidence"
	forgedEvidence.Diagnostic.EvidenceFingerprint = "caller-forged-evidence"
	if _, err := store.CommitRecoveryRetry(ctx, forgedEvidence); err == nil {
		t.Fatal("caller-controlled evidence fingerprint accepted")
	}
	receipt, err := store.CommitRecoveryRetry(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := store.CommitRecoveryRetry(ctx, request)
	if err != nil || replayed != receipt {
		t.Fatalf("replay=%+v err=%v want %+v", replayed, err, receipt)
	}
	supplement, found, err := store.LoadRecoverySupplement(ctx, receipt.AttemptID)
	if err != nil || !found || supplement.InstructionArtifact != request.InstructionArtifact || supplement.Diagnostic != request.Diagnostic {
		t.Fatalf("supplement=%+v found=%v err=%v", supplement, found, err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var source, retry *domain.Attempt
	for i := range records.Attempts {
		switch records.Attempts[i].ID {
		case "attempt-1":
			source = &records.Attempts[i]
		case receipt.AttemptID:
			retry = &records.Attempts[i]
		}
	}
	if source == nil || retry == nil || source.Progress != domain.ProgressFailed || retry.Number != 2 || retry.Progress != domain.ProgressReady {
		t.Fatalf("source=%+v retry=%+v", source, retry)
	}
	state, err := store.LoadSupervisionAdminState(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	got := state.Incidents[0].Incident
	if got.Recovery.AttemptsUsed != 1 || got.Recovery.Diagnostic.StrategyFingerprint != strategy {
		t.Fatalf("incident=%+v", got)
	}
	changed := request
	changed.Diagnostic.StrategyFingerprint = "different"
	if _, err := store.CommitRecoveryRetry(ctx, changed); !errors.Is(err, ErrSupervisionRequestConflict) {
		t.Fatalf("changed replay error=%v", err)
	}
}

func TestRecoveryRetryRejectsReviewerAndUnchangedDiagnosis(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 21, 14, 0, 0, 0, time.UTC)
	store.SetClock(func() time.Time { return now.Add(time.Minute) })
	config := domain.RecoveryConfig{Version: domain.RecoveryContractV1,
		Route: domain.ProviderRoute{ProviderInstanceID: "repairer", Model: "model"}, PromptArtifactID: "repair-prompt",
		MaxAttemptsPerIncident: 1, IncidentDeadline: time.Hour, StalledAfter: time.Minute}
	prepareRecoveryFence(t, store, "run", "attempt-1", now, config)
	artifact := domain.Artifact{ID: "instruction", WorkflowRunID: "run", SHA256: "content", StoragePath: "objects/content", CreatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Tasks: []domain.Task{{ID: "task", WorkflowID: "workflow-run", Name: "task", Class: domain.TaskClassRequired}}, Artifacts: []domain.Artifact{artifact}}); err != nil {
		t.Fatal(err)
	}
	diagnostic := domain.RecoveryDiagnosticIdentity{FailureFingerprint: "failure", EvidenceFingerprint: "evidence", StrategyFingerprint: "same"}
	recovery := domain.NewRecoveryIncident(config, diagnostic, now)
	if _, _, err := store.OpenRecoveryIncident(ctx, RecoveryIncidentRequest{RunID: "run", IncidentID: "incident", EventID: "event",
		SourceTaskID: "task", SourceAttemptID: "attempt-1", ExpectedGraphRevision: 1, SourceAttemptRevision: 1,
		RecoveryConfig: config, Reason: "failed", Recovery: *recovery, EventRecord: []byte(`{"id":"event"}`), OpenedAt: now}); err != nil {
		t.Fatal(err)
	}
	expires := now.Add(time.Hour)
	reviewer := domain.Activation{ID: "review", RunID: "run", Epoch: 1, GraphRevision: 1, Principal: "reviewer", State: domain.ActivationActive, LeaseToken: "lease", LeaseExpiresAt: &expires, IncidentID: "incident"}
	raw, _ := json.Marshal(reviewer)
	if _, err := store.db.ExecContext(ctx, `INSERT INTO coordinator_supervision_activations(id, run_id, epoch, state, dispatch_identity, record)
		VALUES (?, ?, ?, ?, ?, ?)`, reviewer.ID, reviewer.RunID, reviewer.Epoch, reviewer.State, "dispatch", raw); err != nil {
		t.Fatal(err)
	}
	request := domain.RecoveryRetryRequest{OperationID: "op", RunID: "run", IncidentID: "incident", ExpectedIncidentRevision: 1,
		ActivationID: "review", ActivationEpoch: 1, Principal: "reviewer", SourceAttemptID: "attempt-1", SourceAttemptRevision: 1,
		InstructionArtifact: domain.ArtifactDigest{ArtifactID: "instruction", Digest: "content"}, Diagnostic: diagnostic, RequestedAt: now.Add(time.Minute)}
	if _, err := store.CommitRecoveryRetry(ctx, request); err == nil {
		t.Fatal("reviewer/unchanged recovery retry accepted")
	}
	records, _ := store.LoadCoordinatorRecords(ctx)
	if len(records.Attempts) != 1 {
		t.Fatalf("attempts=%d want 1", len(records.Attempts))
	}
}
