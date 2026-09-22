package workerruntime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestRepairActivationCollectsNewProposalBytesIntoResultCustody(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	workspace := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workspace, "recovery"), 0o700); err != nil {
		t.Fatal(err)
	}
	instructions := []byte("replace the invalid setting and rerun the original checks\n")
	checkpoint := []byte("checkpoint-tar-bytes")
	if err := os.WriteFile(filepath.Join(workspace, "recovery", "instructions.md"), instructions, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "recovery", "checkpoint.tar"), checkpoint, 0o600); err != nil {
		t.Fatal(err)
	}
	pkg := workerproto.ExecutionPackage{
		CoordinatorID: "coordinator", CoordinatorEpoch: 9, WorkerID: "normandy", WorkerEpoch: "worker-1",
		Identity: workerproto.ExecutionIdentity{
			WorkflowRunID: "run-1", AttemptID: "activation-attempt-1",
			AssignmentID: "assignment-1", AssignmentEpoch: 2,
		},
		Limits: workerproto.ExecutionLimits{MaxArtifactBytes: 1024, MaxTotalBytes: 4096},
		Supervision: &workerproto.SupervisionActivation{
			ActivationID: "activation-1", Purpose: string(domain.RecoveryActivationRepair),
			IncidentID: "incident-1", RunID: "run-1", Epoch: 3, GraphRevision: 7,
			Principal: "repair-principal",
			RecoveryProposal: &workerproto.RecoveryProposalScope{
				Version: domain.RecoveryProposalVersion, ExpectedIncidentRevision: 4,
				SourceAttemptID: "attempt-1", SourceAttemptRevision: 5,
				Diagnostic: domain.RecoveryDiagnosticIdentity{FailureFingerprint: "failed-check", EvidenceFingerprint: "evidence-v1"},
			},
		},
	}
	driver := LocalDriver{Now: func() time.Time { return now }}
	proposal, gotInstructions, gotCheckpoint, err := driver.collectRecoveryProposal(pkg, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if proposal == nil || string(gotInstructions) != string(instructions) || string(gotCheckpoint) != string(checkpoint) ||
		proposal.PayloadDigest == "" || proposal.GraphRevision != 7 || proposal.AssignmentEpoch != 2 {
		t.Fatalf("proposal=%+v instructions=%q checkpoint=%q", proposal, gotInstructions, gotCheckpoint)
	}
	if _, err := proposal.RetryRequest(); err != nil {
		t.Fatalf("proposal did not verify: %v", err)
	}
	custody := testCustodyStore(t, t.TempDir(), func() time.Time { return now })
	if err := custody.PublishResult(context.Background(), pkg, PublishedResult{
		FinalMessage: "repair proposal complete", ThreadArchive: []byte("{}"),
		RecoveryProposal: proposal, RecoveryInstructions: gotInstructions, RecoveryCheckpointTar: gotCheckpoint,
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := custody.PendingUploadByPurpose("result")
	if err != nil || pending == nil {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	if len(pending.Manifest.Objects) != 5 {
		t.Fatalf("objects=%+v", pending.Manifest.Objects)
	}
	var proposalObject workerproto.ArtifactObject
	for _, object := range pending.Manifest.Objects {
		if object.Path == "results/recovery/proposal.json" {
			proposalObject = object
		}
	}
	reader, err := custody.OpenArtifact(context.Background(), proposalObject)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var retained domain.RecoveryProposal
	if err := json.NewDecoder(reader).Decode(&retained); err != nil {
		t.Fatal(err)
	}
	if retained.PayloadDigest != proposal.PayloadDigest || retained.InstructionArtifact != proposal.InstructionArtifact {
		t.Fatalf("retained=%+v want=%+v", retained, *proposal)
	}

	missing := t.TempDir()
	if _, _, _, err := driver.collectRecoveryProposal(pkg, missing); err == nil {
		t.Fatal("repair activation without new instruction bytes was accepted")
	}
	reviewer := pkg
	reviewer.Supervision = &workerproto.SupervisionActivation{Purpose: "", RunID: "run-1"}
	if got, _, _, err := driver.collectRecoveryProposal(reviewer, missing); err != nil || got != nil {
		t.Fatalf("reviewer gained repair publication: proposal=%+v err=%v", got, err)
	}
}
