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
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Tasks: []domain.Task{{ID: "task", WorkflowID: "workflow-run", Name: "task", Class: domain.TaskClassRequired}}}); err != nil {
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
		OperationID: "repair-op", RunID: "run", IncidentID: "incident", ExpectedIncidentRevision: 1, GraphRevision: 1,
		ActivationID: "activation", ActivationEpoch: 1, Principal: "repair-principal",
		SourceAttemptID: "attempt-1", SourceAttemptRevision: 1,
		InstructionArtifact: domain.ArtifactDigest{ArtifactID: "instruction", Digest: "content-a"},
		Diagnostic:          domain.RecoveryDiagnosticIdentity{FailureFingerprint: "check-category", EvidenceFingerprint: "content-old", StrategyFingerprint: strategy},
		RequestedAt:         now.Add(time.Minute),
	}
	prepareImportedRecoveryProposal(t, store, &request, activation, now)
	withoutReceipt := request
	withoutReceipt.OperationID = "direct-without-import-receipt"
	withoutReceipt.ProposalReceipt = domain.RecoveryProposalReceipt{}
	if _, err := store.CommitRecoveryRetry(ctx, withoutReceipt); err == nil {
		t.Fatal("direct recovery retry without importer receipt committed")
	}
	wrongProvenance := request
	wrongProvenance.OperationID = "wrong-provenance"
	wrongProvenance.ProposalReceipt.ProposalArtifact.Digest = "forged"
	if _, err := store.CommitRecoveryRetry(ctx, wrongProvenance); err == nil {
		t.Fatal("recovery retry with forged proposal provenance committed")
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
	staleGraph := request
	staleGraph.OperationID = "stale-graph"
	staleGraph.GraphRevision++
	if _, err := store.CommitRecoveryRetry(ctx, staleGraph); err == nil {
		t.Fatal("stale graph recovery retry accepted")
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

func prepareImportedRecoveryProposal(t *testing.T, store *Store, request *domain.RecoveryRetryRequest, activation domain.Activation, now time.Time) {
	t.Helper()
	assignment := domain.Assignment{
		ID: "repair-assignment-" + activation.ID, AttemptID: "repair-activation-attempt-" + activation.ID,
		WorkerID: "repair-worker", WorkerEpoch: "worker-epoch", State: domain.AssignmentCompleted, Epoch: 1,
		LeaseToken: "lease-" + activation.ID, DispatchToken: "dispatch-" + activation.ID, CreatedAt: now, UpdatedAt: now,
	}
	activationAttempt := domain.Attempt{
		ID: assignment.AttemptID, WorkflowRunID: request.RunID, TaskID: activation.ID, Number: 1,
		Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 1, AssignmentID: assignment.ID,
		SupervisionActivationID: activation.ID, SupervisionActivationEpoch: activation.Epoch, UpdatedAt: now,
	}
	producer := "worker:" + assignment.WorkerID
	proposal := domain.Artifact{
		ID: "recovery-proposal-" + activationAttempt.ID, WorkflowRunID: request.RunID, TaskID: activationAttempt.TaskID,
		AttemptID: activationAttempt.ID, Kind: domain.ArtifactInput, Name: "recovery/proposal.json",
		MediaType: "application/json", Size: 1, SHA256: "proposal-" + activation.ID,
		StoragePath: "objects/proposal-" + activation.ID, Producer: producer, CreatedAt: now,
	}
	instruction := domain.Artifact{
		ID: request.InstructionArtifact.ArtifactID, WorkflowRunID: request.RunID, TaskID: activationAttempt.TaskID,
		AttemptID: activationAttempt.ID, Kind: domain.ArtifactInput, Name: "recovery/instructions.md",
		MediaType: "text/markdown", Size: 1, SHA256: request.InstructionArtifact.Digest,
		StoragePath: "objects/" + request.InstructionArtifact.Digest, Producer: producer, CreatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{
		Attempts: []domain.Attempt{activationAttempt}, Assignments: []domain.Assignment{assignment},
		Artifacts: []domain.Artifact{proposal, instruction},
	}); err != nil {
		t.Fatal(err)
	}
	request.AssignmentID, request.AssignmentEpoch = assignment.ID, assignment.Epoch
	request.ProposalReceipt = domain.RecoveryProposalReceipt{
		ProposalArtifact:    domain.ArtifactDigest{ArtifactID: proposal.ID, Digest: proposal.SHA256},
		ActivationAttemptID: activationAttempt.ID,
		AssignmentID:        assignment.ID, AssignmentEpoch: assignment.Epoch,
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
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Tasks: []domain.Task{{ID: "task", WorkflowID: "workflow-run", Name: "task", Class: domain.TaskClassRequired}}}); err != nil {
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
	request := domain.RecoveryRetryRequest{OperationID: "op", RunID: "run", IncidentID: "incident", ExpectedIncidentRevision: 1, GraphRevision: 1,
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
