package sqlite

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func insertRecoveryActivation(t *testing.T, store *Store, activation domain.Activation) {
	t.Helper()
	raw, err := json.Marshal(activation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO coordinator_supervision_activations(id, run_id, epoch, state, dispatch_identity, record)
		VALUES (?, ?, ?, ?, ?, ?)`, activation.ID, activation.RunID, activation.Epoch, activation.State, "dispatch-"+activation.ID, raw); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryRetriesAdvanceOneEpisodeToBoundedExhaustion(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 21, 18, 0, 0, 0, time.UTC)
	store.SetClock(func() time.Time { return now.Add(time.Minute) })
	config := domain.RecoveryConfig{Version: domain.RecoveryContractV1,
		Route: domain.ProviderRoute{ProviderInstanceID: "repairer", Model: "model"}, PromptArtifactID: "prompt",
		MaxAttemptsPerIncident: 2, IncidentDeadline: time.Hour, StalledAfter: time.Minute}
	prepareRecoveryFence(t, store, "run", "attempt-1", now, config)
	root := domain.RecoveryDiagnosticIdentity{FailureFingerprint: "failure-1", EvidenceFingerprint: "evidence-1", StrategyFingerprint: "initial"}
	recovery := domain.NewRecoveryIncident(config, root, now)
	if _, _, err := store.OpenRecoveryIncident(ctx, RecoveryIncidentRequest{
		RunID: "run", IncidentID: "incident", EventID: "event-1", SourceTaskID: "task", SourceAttemptID: "attempt-1",
		ExpectedGraphRevision: 1, SourceAttemptRevision: 1, RecoveryConfig: config, Reason: "A1 failed",
		Recovery: *recovery, EventRecord: []byte(`{"id":"event-1"}`), OpenedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	artifacts := []domain.Artifact{
		{ID: "repair-1", WorkflowRunID: "run", TaskID: "task", AttemptID: "attempt-1", Kind: domain.ArtifactCheckpoint, Name: "repair-1.md", SHA256: "strategy-one", Size: 1, MediaType: "text/markdown", StoragePath: "objects/one", CreatedAt: now},
		{ID: "repair-2", WorkflowRunID: "run", TaskID: "task", AttemptID: "attempt-1", Kind: domain.ArtifactCheckpoint, Name: "repair-2.md", SHA256: "strategy-two", Size: 1, MediaType: "text/markdown", StoragePath: "objects/two", CreatedAt: now},
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Tasks: []domain.Task{{ID: "task", WorkflowID: "workflow-run", Name: "task", Class: domain.TaskClassRequired}}}); err != nil {
		t.Fatal(err)
	}
	expires := now.Add(time.Hour)
	activation1 := domain.Activation{ID: "activation-1", RunID: "run", Epoch: 1, Purpose: domain.RecoveryActivationRepair, IncidentID: "incident", GraphRevision: 1, Principal: "repairer", State: domain.ActivationActive, LeaseToken: "lease-1", LeaseExpiresAt: &expires}
	insertRecoveryActivation(t, store, activation1)
	makeRequest := func(operation string, activation domain.Activation, source string, sourceRevision, incidentRevision int64, artifact domain.Artifact, failure, evidence string) domain.RecoveryRetryRequest {
		digest := domain.ArtifactDigest{ArtifactID: artifact.ID, Digest: artifact.SHA256}
		request := domain.RecoveryRetryRequest{OperationID: operation, RunID: "run", IncidentID: "incident", ExpectedIncidentRevision: incidentRevision, GraphRevision: 1,
			ActivationID: activation.ID, ActivationEpoch: activation.Epoch, Principal: activation.Principal,
			SourceAttemptID: source, SourceAttemptRevision: sourceRevision, InstructionArtifact: digest,
			Diagnostic:  domain.RecoveryDiagnosticIdentity{FailureFingerprint: failure, EvidenceFingerprint: evidence, StrategyFingerprint: domain.RecoveryStrategyFingerprint(digest, nil)},
			RequestedAt: now.Add(time.Minute)}
		prepareImportedRecoveryProposal(t, store, &request, activation, now)
		return request
	}
	first, err := store.CommitRecoveryRetry(ctx, makeRequest("retry-1", activation1, "attempt-1", 1, 1, artifacts[0], "failure-1", "evidence-1"))
	if err != nil {
		t.Fatal(err)
	}
	a2 := domain.Attempt{ID: first.AttemptID, WorkflowRunID: "run", TaskID: "task", Number: 2, Progress: domain.ProgressFailed, Control: domain.ControlStopped, Revision: 2, Failure: "A2 failed", UpdatedAt: now.Add(2 * time.Minute)}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{a2}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordRecoveryAttemptFailure(ctx, RecoveryAttemptFailureRequest{RunID: "run", IncidentID: "incident", EventID: "event-2", AttemptID: a2.ID, Reason: "A2 failed",
		ExpectedIncidentRevision: 2, ExpectedGraphRevision: 1, AttemptRevision: 2, FailureFingerprint: "failure-2", EvidenceFingerprint: "evidence-2", EventRecord: []byte(`{"id":"event-2"}`)}); err != nil {
		t.Fatal(err)
	}
	admin, err := store.LoadSupervisionAdminState(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	admin.Record.ActivationEpoch = 2
	recordRaw, err := json.Marshal(admin.Record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, "UPDATE coordinator_supervision SET record = ? WHERE run_id = ?", recordRaw, "run"); err != nil {
		t.Fatal(err)
	}
	activation2 := domain.Activation{ID: "activation-2", RunID: "run", Epoch: 2, Purpose: domain.RecoveryActivationRepair, IncidentID: "incident", GraphRevision: 1, Principal: "repairer", State: domain.ActivationActive, LeaseToken: "lease-2", LeaseExpiresAt: &expires}
	insertRecoveryActivation(t, store, activation2)
	second, err := store.CommitRecoveryRetry(ctx, makeRequest("retry-2", activation2, a2.ID, 2, 3, artifacts[1], "failure-2", "evidence-2"))
	if err != nil {
		t.Fatal(err)
	}
	a3 := domain.Attempt{ID: second.AttemptID, WorkflowRunID: "run", TaskID: "task", Number: 3, Progress: domain.ProgressFailed, Control: domain.ControlStopped, Revision: 2, Failure: "A3 failed", UpdatedAt: now.Add(3 * time.Minute)}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{a3}}); err != nil {
		t.Fatal(err)
	}
	got, err := store.RecordRecoveryAttemptFailure(ctx, RecoveryAttemptFailureRequest{RunID: "run", IncidentID: "incident", EventID: "event-3", AttemptID: a3.ID, Reason: "A3 failed",
		ExpectedIncidentRevision: 4, ExpectedGraphRevision: 1, AttemptRevision: 2, FailureFingerprint: "failure-3", EvidenceFingerprint: "evidence-3", EventRecord: []byte(`{"id":"event-3"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if got.Recovery.State != domain.RecoveryNeedsHuman || got.Recovery.AttemptsUsed != 2 || got.Recovery.RootDiagnostic != root {
		t.Fatalf("episode=%+v", got)
	}
	escalations, err := store.PendingSupervisionEscalations(ctx)
	if err != nil || len(escalations) != 1 {
		t.Fatalf("escalations=%+v err=%v", escalations, err)
	}
}
