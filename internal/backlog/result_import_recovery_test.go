package backlog

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type recoveryProposalImportStore struct {
	*sqlite.Store
	calls int
	got   domain.RecoveryRetryRequest
}

func (s *recoveryProposalImportStore) CommitRecoveryRetry(ctx context.Context, request domain.RecoveryRetryRequest) (domain.RecoveryRetryReceipt, error) {
	s.calls++
	s.got = request
	retained, err := s.LoadArtifacts(ctx, []string{request.InstructionArtifact.ArtifactID, request.CheckpointArtifacts[0].ArtifactID})
	if err != nil {
		return domain.RecoveryRetryReceipt{}, err
	}
	if len(retained) != 2 || retained[0].SHA256 != request.InstructionArtifact.Digest ||
		retained[1].SHA256 != request.CheckpointArtifacts[0].Digest {
		return domain.RecoveryRetryReceipt{}, errors.New("retry reached before exact proposal payload entered coordinator custody")
	}
	return s.Store.CommitRecoveryRetry(ctx, request)
}

func TestRepairProposalImportsThroughWorkerCustodyBeforeRetry(t *testing.T) {
	ctx := context.Background()
	now := coordinatorTestTime
	sqlStore, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer sqlStore.Close()
	sqlStore.SetClock(func() time.Time { return now.Add(time.Minute) })
	store := &recoveryProposalImportStore{Store: sqlStore}
	attempt := domain.Attempt{
		ID: "activation-attempt-1", WorkflowRunID: "run-1", TaskID: "activation-1",
		SupervisionActivationID: "activation-1", SupervisionActivationEpoch: 3,
		Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped,
		Revision: 3, AssignmentID: "assignment-1", UpdatedAt: now,
	}
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1",
		State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 2,
		LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now,
	}
	sourceTask := domain.Task{
		ID: "task-a", WorkflowID: "workflow-1", Name: "A", Class: domain.TaskClassRequired,
		PromptArtifactID: "original-prompt", Verification: []string{"test-original"},
		ResourceLocks: []string{"original-lock"},
	}
	sourceAttempt := domain.Attempt{
		ID: "failed-attempt-1", WorkflowRunID: attempt.WorkflowRunID, TaskID: sourceTask.ID,
		Number: 1, Progress: domain.ProgressFailed, Control: domain.ControlStopped,
		Failure: "failed-check", Revision: 5, UpdatedAt: now,
	}
	if err := sqlStore.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{
			ID: attempt.WorkflowRunID, WorkflowID: "workflow-1", GraphRevision: 7,
			Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now,
		}},
		Tasks: []domain.Task{sourceTask}, Attempts: []domain.Attempt{sourceAttempt, attempt},
		Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	recoveryConfig := domain.RecoveryConfig{
		Version:          domain.RecoveryContractV1,
		Route:            domain.ProviderRoute{ProviderInstanceID: "repairer", Model: "repair-model"},
		PromptArtifactID: "repair-prompt", MaxAttemptsPerIncident: 2,
		IncidentDeadline: time.Hour, StalledAfter: time.Minute,
	}
	supervisionConfig := domain.SupervisionConfig{
		Route:            domain.ProviderRoute{ProviderInstanceID: "reviewer", Model: "review-model"},
		PromptArtifactID: "review-prompt", MaxActivations: 3, MaxTurnsPerActivation: 2,
		ActivationDeadline: time.Hour, Recovery: &recoveryConfig,
		Escalation: domain.SupervisionEscalation{NotifyThread: true, ThreadID: "operator-thread"},
	}
	record, err := sqlStore.PutSupervision(ctx, sqlite.SupervisionMaterialization{
		Record: domain.SupervisionRecord{RunID: attempt.WorkflowRunID, Config: supervisionConfig},
	})
	if err != nil {
		t.Fatal(err)
	}
	diagnostic := domain.RecoveryDiagnosticIdentity{
		FailureFingerprint: "failed-check", EvidenceFingerprint: "evidence-v1",
		StrategyFingerprint: "strategy-v1",
	}
	recovery := domain.NewRecoveryIncident(recoveryConfig, diagnostic, now)
	if _, _, err := sqlStore.OpenRecoveryIncident(ctx, sqlite.RecoveryIncidentRequest{
		RunID: attempt.WorkflowRunID, IncidentID: "incident-1", EventID: "failure-event-1",
		SourceTaskID: sourceTask.ID, SourceAttemptID: sourceAttempt.ID,
		ExpectedGraphRevision: 7, SourceAttemptRevision: sourceAttempt.Revision,
		RecoveryConfig: recoveryConfig, Reason: sourceAttempt.Failure, Recovery: *recovery,
		EventRecord: []byte(`{"id":"failure-event-1"}`), OpenedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	expires := now.Add(time.Hour)
	activation := domain.Activation{
		ID: attempt.SupervisionActivationID, RunID: attempt.WorkflowRunID,
		Epoch: attempt.SupervisionActivationEpoch, Purpose: domain.RecoveryActivationRepair,
		GraphRevision: 7, Principal: "repair-principal", State: domain.ActivationActive,
		LeaseToken: "activation-lease", LeaseExpiresAt: &expires, IncidentID: "incident-1",
	}
	record.ActivationEpoch = activation.Epoch
	record.ActivationsUsed = 1
	if err := sqlStore.CommitSupervisionActivationRows(ctx, sqlite.SupervisionActivationRowCommit{
		RunID: attempt.WorkflowRunID, ExpectedRecordRevision: record.Revision,
		Record: record, Activation: activation, RequestID: "activate-repair", CommittedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	instructions := []byte("use the retained corrected configuration\n")
	checkpoint := []byte("checkpoint bytes")
	instructionObject := resultObject("recovery-instructions-"+attempt.ID, "results/recovery/instructions.md",
		"input", "text/markdown", instructions)
	checkpointObject := resultObject("recovery-checkpoint-"+attempt.ID, "results/recovery/checkpoint.tar",
		"checkpoint", "application/x-tar", checkpoint)
	diagnostic.StrategyFingerprint = domain.RecoveryStrategyFingerprint(
		domain.ArtifactDigest{ArtifactID: instructionObject.ID, Digest: instructionObject.SHA256},
		[]domain.ArtifactDigest{{ArtifactID: checkpointObject.ID, Digest: checkpointObject.SHA256}},
	)
	proposal, err := domain.SealRecoveryProposal(domain.RecoveryProposal{
		Version: domain.RecoveryProposalVersion, OperationID: "proposal:assignment-1",
		RunID: "run-1", IncidentID: "incident-1", ExpectedIncidentRevision: 1, GraphRevision: 7,
		ActivationID: "activation-1", ActivationEpoch: 3,
		AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, Principal: "repair-principal",
		SourceAttemptID: "failed-attempt-1", SourceAttemptRevision: 5,
		InstructionArtifact: domain.ArtifactDigest{ArtifactID: instructionObject.ID, Digest: instructionObject.SHA256},
		CheckpointArtifacts: []domain.ArtifactDigest{{ArtifactID: checkpointObject.ID, Digest: checkpointObject.SHA256}},
		Diagnostic:          diagnostic, ProposedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	proposalRaw, _ := json.Marshal(proposal)
	archive := []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`)
	data := resultUploadOpener{
		instructionObject.ID: instructions, checkpointObject.ID: checkpoint,
		"recovery-proposal-" + attempt.ID: proposalRaw,
		"final-message-" + attempt.ID:     []byte("repair proposed\n"),
		"thread-archive-" + attempt.ID:    archive,
	}
	objects := []workerproto.ArtifactObject{
		instructionObject, checkpointObject,
		resultObject("recovery-proposal-"+attempt.ID, "results/recovery/proposal.json", "input", "application/json", proposalRaw),
		resultObject("final-message-"+attempt.ID, "results/final-message.md", "summary", "text/markdown", data["final-message-"+attempt.ID]),
		resultObject("thread-archive-"+attempt.ID, "results/thread.json", "log", "application/json", archive),
	}
	manifest := resultManifest(now, assignment, objects)
	response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
	importer := CoordinatorResultImporter{
		CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store,
		Artifacts:        CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store},
		MaxArtifactBytes: 4096, MaxTotalBytes: 16384, Now: func() time.Time { return now.Add(time.Minute) },
	}
	report, err := importer.Import(ctx, response, data)
	if err != nil {
		t.Fatal(err)
	}
	if store.calls != 1 || report.Recovery == nil || report.Recovery.AttemptNumber != 2 ||
		store.got.GraphRevision != 7 || len(report.Artifacts) != 5 {
		t.Fatalf("calls=%d request=%+v report=%+v", store.calls, store.got, report)
	}
	supplement, found, err := sqlStore.LoadRecoverySupplement(ctx, report.Recovery.AttemptID)
	if err != nil || !found || supplement.InstructionArtifact != proposal.InstructionArtifact ||
		len(supplement.CheckpointArtifacts) != 1 || supplement.CheckpointArtifacts[0] != proposal.CheckpointArtifacts[0] {
		t.Fatalf("supplement=%+v found=%v err=%v", supplement, found, err)
	}
	records, err := sqlStore.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var preservedSource, retry *domain.Attempt
	var preservedTask *domain.Task
	for index := range records.Attempts {
		switch records.Attempts[index].ID {
		case sourceAttempt.ID:
			preservedSource = &records.Attempts[index]
		case report.Recovery.AttemptID:
			retry = &records.Attempts[index]
		}
	}
	for index := range records.Tasks {
		if records.Tasks[index].ID == sourceTask.ID {
			preservedTask = &records.Tasks[index]
		}
	}
	if preservedSource == nil || preservedSource.Progress != domain.ProgressFailed ||
		retry == nil || retry.Number != 2 || retry.Progress != domain.ProgressReady ||
		preservedTask == nil || preservedTask.PromptArtifactID != sourceTask.PromptArtifactID ||
		len(preservedTask.Verification) != 1 || preservedTask.Verification[0] != sourceTask.Verification[0] ||
		len(preservedTask.ResourceLocks) != 1 || preservedTask.ResourceLocks[0] != sourceTask.ResourceLocks[0] {
		t.Fatalf("source=%+v retry=%+v task=%+v", preservedSource, retry, preservedTask)
	}
	replayed, err := importer.Import(ctx, response, data)
	if err != nil || store.calls != 2 || replayed.Recovery == nil || replayed.Recovery.AttemptID != report.Recovery.AttemptID {
		t.Fatalf("replay calls=%d report=%+v err=%v", store.calls, replayed, err)
	}

	tampered := proposal
	tampered.AssignmentEpoch++
	tampered, _ = domain.SealRecoveryProposal(tampered)
	tamperedRaw, _ := json.Marshal(tampered)
	tamperedData := resultUploadOpener{}
	for key, value := range data {
		tamperedData[key] = value
	}
	tamperedData["recovery-proposal-"+attempt.ID] = tamperedRaw
	tamperedObjects := append([]workerproto.ArtifactObject(nil), objects...)
	tamperedObjects[2] = resultObject("recovery-proposal-"+attempt.ID, "results/recovery/proposal.json", "input", "application/json", tamperedRaw)
	tamperedManifest := resultManifest(now, assignment, tamperedObjects)
	tamperedResponse := workerproto.ArtifactUploadResponse{Manifest: tamperedManifest, Custody: resultCustody(t, tamperedManifest, "coordinator")}
	if _, err := importer.Import(ctx, tamperedResponse, tamperedData); err == nil || store.calls != 2 {
		t.Fatalf("stale assignment proposal accepted: calls=%d err=%v", store.calls, err)
	}
}
