package backlog

import (
	"context"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type recoverySupplementPackageStore struct {
	supplement domain.RepairAttemptSupplement
	found      bool
}

func (s recoverySupplementPackageStore) LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error) {
	return sqlite.CoordinatorRecords{}, nil
}
func (s recoverySupplementPackageStore) LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error) {
	return nil, nil
}
func (s recoverySupplementPackageStore) LoadRecoverySupplement(context.Context, string) (domain.RepairAttemptSupplement, bool, error) {
	return s.supplement, s.found, nil
}

func TestRecoverySupplementBecomesOrdinaryAttemptInputs(t *testing.T) {
	instruction := domain.ArtifactDigest{ArtifactID: "repair-instruction", Digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	checkpoint := domain.ArtifactDigest{ArtifactID: "repair-checkpoint", Digest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	store := recoverySupplementPackageStore{found: true, supplement: domain.RepairAttemptSupplement{
		AttemptID: "attempt-2", IncidentID: "incident", InstructionArtifact: instruction, CheckpointArtifacts: []domain.ArtifactDigest{checkpoint},
	}}
	state := executionPackageState{
		run:     domain.WorkflowRun{ID: "run"},
		attempt: domain.Attempt{ID: "attempt-2"},
		artifacts: map[string]domain.Artifact{
			instruction.ArtifactID: {ID: instruction.ArtifactID, WorkflowRunID: "run", Name: "repair", SHA256: instruction.Digest, Size: 17, MediaType: "text/markdown", StoragePath: "objects/a"},
			checkpoint.ArtifactID:  {ID: checkpoint.ArtifactID, WorkflowRunID: "run", Name: "repair", SHA256: checkpoint.Digest, Size: 9, MediaType: "application/octet-stream", StoragePath: "objects/b"},
		},
	}
	inputs, recovery, err := appendRecoverySupplementInputs(context.Background(), store, state, nil)
	if err != nil {
		t.Fatal(err)
	}
	if recovery == nil || recovery.InstructionPath != "inputs/recovery/instructions.md" || recovery.IncidentID == "" {
		t.Fatalf("recovery context = %+v", recovery)
	}
	if _, _, err := appendRecoverySupplementInputs(context.Background(), store, state, []workerproto.ArtifactObject{{Path: "inputs/recovery/instructions.md"}}); err == nil {
		t.Fatal("reserved recovery input collision accepted")
	}
	if len(inputs) != 2 || inputs[0].ID != instruction.ArtifactID || inputs[0].Path != "inputs/recovery/instructions.md" ||
		inputs[1].ID != checkpoint.ArtifactID || inputs[1].Path != "inputs/recovery/checkpoint-01" {
		t.Fatalf("recovery inputs = %+v", inputs)
	}
}
