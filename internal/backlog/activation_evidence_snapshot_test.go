package backlog

import (
	"errors"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestActivationEvidenceSnapshotIsStableAndPreservesReviewContract(t *testing.T) {
	snapshot := ActivationSnapshot{
		ActivationID: "activation-1", RunID: "run-1", Epoch: 2,
		GraphRevision: 7, RecordRevision: 11, ConsumedThrough: 19,
		Triggers: []CoalescedTrigger{{Kind: TriggerGateReviewReady, Subject: "gate-current"}},
		Actions:  []ActivationAction{{Name: "decide", Constraints: []string{"requires exact evidence"}}},
		Tasks: []ActivationTaskView{
			{TaskID: "task-b", State: "succeeded"},
			{TaskID: "task-a", State: "succeeded"},
		},
		Gates: []ActivationGateView{{GateID: "gate-current", State: domain.GateReadyForReview, GraphRevision: 7}},
		TaskContracts: []domain.Task{{
			ID: "task-a", PromptArtifactID: "prompt-task-a",
			Outputs:      []domain.ArtifactDeclaration{{Name: "report"}},
			Verification: []string{"go test ./..."},
		}},
		Attempts: []domain.Attempt{{
			ID: "attempt-a", TaskID: "task-a", Number: 1,
			Revision: 4, Progress: domain.ProgressSucceeded,
		}},
		GateEvidence: []ActivationGateEvidence{{
			Gate: domain.Gate{
				Definition: domain.GateDefinition{
					ID: "gate-current", ObservedTaskIDs: []string{"task-a"},
					RubricArtifactID: "rubric-current",
				},
				RunID: "run-1", State: domain.GateReadyForReview,
				GraphRevision: 7, Revision: 3, EvidenceSnapshotID: "gate-evidence-1",
			},
			Evidence: &domain.EvidenceSnapshot{
				ID: "gate-evidence-1", GraphRevision: 7,
				Producers: []domain.ProducerEvidence{{
					TaskID: "task-a", AttemptID: "attempt-a", ResultRevision: 4,
					ArtifactDigests: []domain.ArtifactDigest{{ArtifactID: "result-a", Digest: "sha256:aaa"}},
				}},
			},
		}},
		OverseerPrompt: domain.ArtifactDigest{ArtifactID: "overseer-prompt", Digest: "sha256:prompt"},
		Artifacts: []domain.ArtifactDigest{
			{ArtifactID: "rubric-current", Digest: "sha256:rubric"},
			{ArtifactID: "prompt-task-a", Digest: "sha256:task-prompt"},
		},
	}
	first, err := BuildActivationEvidenceSnapshot(snapshot)
	if err != nil {
		t.Fatalf("build first snapshot: %v", err)
	}
	snapshot.Tasks[0], snapshot.Tasks[1] = snapshot.Tasks[1], snapshot.Tasks[0]
	second, err := BuildActivationEvidenceSnapshot(snapshot)
	if err != nil {
		t.Fatalf("build replay snapshot: %v", err)
	}
	if first.ID != second.ID || first.SHA256 != second.SHA256 || string(first.Data) != string(second.Data) {
		t.Fatalf("replay changed immutable snapshot: first %s/%s second %s/%s",
			first.ID, first.SHA256, second.ID, second.SHA256)
	}
	frozen, err := DecodeActivationEvidenceSnapshot(first.Data)
	if err != nil {
		t.Fatalf("decode retained snapshot: %v", err)
	}
	if frozen.GateEvidence[0].Gate.Definition.RubricArtifactID != "rubric-current" ||
		frozen.GateEvidence[0].Gate.Revision != 3 ||
		frozen.GateEvidence[0].Evidence.Producers[0].AttemptID != "attempt-a" ||
		frozen.TaskContracts[0].PromptArtifactID != "prompt-task-a" ||
		frozen.TaskContracts[0].Verification[0] != "go test ./..." ||
		frozen.OverseerPrompt.ArtifactID != "overseer-prompt" {
		t.Fatalf("retained snapshot lost review contract: %+v", frozen)
	}

	// Mutable current state may advance after the first freeze. Dispatch replay
	// continues to use the decoded retained bytes and digest.
	snapshot.RecordRevision = 12
	snapshot.Gates[0].State = domain.GateAccepted
	current, err := BuildActivationEvidenceSnapshot(snapshot)
	if err != nil {
		t.Fatalf("build advanced current snapshot: %v", err)
	}
	if current.SHA256 == first.SHA256 {
		t.Fatal("fixture did not change current mutable state")
	}
	replayed, err := BuildActivationEvidenceObject(frozen)
	if err != nil {
		t.Fatalf("rebuild retained object: %v", err)
	}
	if replayed.SHA256 != first.SHA256 || replayed.ID != first.ID {
		t.Fatalf("replay replaced frozen evidence: got %s/%s want %s/%s",
			replayed.ID, replayed.SHA256, first.ID, first.SHA256)
	}
}

func TestActivationPromptIrreducibleMandatoryContextIsTyped(t *testing.T) {
	evidence := domain.ArtifactDigest{ArtifactID: "activation-evidence-1", Digest: "sha256:aaa"}
	_, err := BuildActivationPromptEnvelope(ActivationSnapshot{
		ActivationID: "activation-1", RunID: "run-1", Epoch: 1,
		Triggers:         []CoalescedTrigger{{Kind: TriggerGateReviewReady, Subject: "gate-1"}},
		Actions:          []ActivationAction{{Name: "decide"}},
		Constraints:      []string{strings.Repeat("mandatory-review-constraint", 200)},
		EvidenceSnapshot: &evidence,
		ByteCap:          512,
	})
	var packageErr *ActivationPackageError
	if !errors.As(err, &packageErr) || packageErr.Code != ActivationPackageErrorPromptTooLarge {
		t.Fatalf("error = %v, want typed %s", err, ActivationPackageErrorPromptTooLarge)
	}
}

func TestCompactActivationBriefKeepsGateDespiteUnrelatedIncident(t *testing.T) {
	evidence := domain.ArtifactDigest{ArtifactID: "activation-evidence-1", Digest: "sha256:aaa"}
	envelope, err := BuildActivationPromptEnvelope(ActivationSnapshot{
		ActivationID: "activation-1", RunID: "run-1", Epoch: 1,
		GraphRevision: 9, RecordRevision: 12,
		Triggers: []CoalescedTrigger{{Kind: TriggerGateReviewReady, Subject: "gate-current"}},
		Actions:  []ActivationAction{{Name: "decide"}},
		Gates: []ActivationGateView{{
			GateID: "gate-current", State: domain.GateReadyForReview,
			GraphRevision: 9, EvidenceSnapshotID: "gate-evidence-current",
		}},
		Incidents: []ActivationIncidentView{{
			IncidentID: "incident-unrelated", State: domain.IncidentResolved,
			RequiredDisposition: domain.DispositionOperatorAction, Revision: 8,
		}},
		EvidenceSnapshot: &evidence,
	})
	if err != nil {
		t.Fatalf("build compact brief: %v", err)
	}
	rendered := envelope.Render()
	if !strings.Contains(rendered, "gate-current") || !strings.Contains(rendered, "gate-evidence-current") {
		t.Fatalf("unrelated incident hid current gate: %s", rendered)
	}
}
