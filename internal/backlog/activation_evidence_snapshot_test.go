package backlog

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
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

func TestActivationEvidenceSnapshotRedactsFreeTextWithoutMutatingContracts(t *testing.T) {
	const secret = "literal-secret-value"
	snapshot := ActivationSnapshot{
		ActivationID: "activation-1", RunID: "run-1", Epoch: 1,
		Tasks: []ActivationTaskView{{
			TaskID: "task-1", State: "failed", Verification: "failure included " + secret,
		}},
		TaskContracts: []domain.Task{{
			ID: "task-1", Verification: []string{"authoritative command " + secret},
		}},
		Incidents: []ActivationIncidentView{{
			IncidentID: "incident-1", Reason: "operator note " + secret,
		}},
		Triggers:    []CoalescedTrigger{{Kind: TriggerGateReviewReady, Subject: "incident-1", Reasons: []string{"trigger " + secret}}},
		Actions:     []ActivationAction{{Name: "review"}},
		Constraints: []string{"free-form constraint " + secret},
		Redactor:    Redactor{Literals: []string{secret}},
	}
	object, err := BuildActivationEvidenceSnapshot(snapshot)
	if err != nil {
		t.Fatalf("build redacted snapshot: %v", err)
	}
	if strings.Contains(string(object.Data), "operator note "+secret) ||
		strings.Contains(string(object.Data), "failure included "+secret) ||
		strings.Contains(string(object.Data), "free-form constraint "+secret) {
		t.Fatalf("snapshot retained secret in free text: %s", object.Data)
	}
	frozen, err := DecodeActivationEvidenceSnapshot(object.Data)
	if err != nil {
		t.Fatalf("decode redacted snapshot: %v", err)
	}
	if frozen.TaskContracts[0].Verification[0] != "authoritative command "+secret {
		t.Fatalf("authoritative contract changed: %+v", frozen.TaskContracts[0])
	}
	if snapshot.Tasks[0].Verification != "failure included "+secret ||
		snapshot.Incidents[0].Reason != "operator note "+secret {
		t.Fatalf("snapshot builder mutated its input: %+v", snapshot)
	}
}

func TestActivationPackageCarriesRetrievableFrozenEvidence(t *testing.T) {
	now := supervisionTestTime()
	record := supervisionTestRecord()
	record.RunID = "run-1"
	record.Revision = 11
	record.Config.PromptArtifactID = "overseer-prompt"
	input := ActivationPackageInput{
		Workflow:   domain.Workflow{ID: "workflow-1", Project: "project-1"},
		Run:        domain.WorkflowRun{ID: "run-1", WorkflowID: "workflow-1", GraphRevision: 7},
		Record:     record,
		Activation: domain.Activation{ID: "activation-1", RunID: "run-1", Epoch: 2},
		Dispatch:   ActivationDispatch{Epoch: 2, LeaseToken: "lease-1", LeaseExpiresAt: now.Add(time.Hour)},
		Attempt: domain.Attempt{
			ID: "activation-attempt", WorkflowRunID: "run-1", TaskID: "supervision",
			Revision: 1, SupervisionActivationID: "activation-1", SupervisionActivationEpoch: 2,
		},
		Assignment: domain.Assignment{
			ID: "assignment-1", AttemptID: "activation-attempt",
			WorkerID: "worker-1", WorkerEpoch: "worker-epoch-1", Epoch: 1,
			Route:         domain.ProviderRoute{ProviderInstanceID: "codex-main", Model: "gpt-5.6-sol"},
			DispatchToken: "dispatch-1", CreatedAt: now,
		},
		Triggers: []CoalescedTrigger{{Kind: TriggerGateReviewReady, Subject: "gate-1"}},
		Artifacts: []domain.Artifact{{
			ID: "overseer-prompt", WorkflowRunID: "run-1", Kind: domain.ArtifactInput,
			Name: "overseer.md", MediaType: "text/markdown", Size: 10,
			SHA256: strings.Repeat("a", 64), StoragePath: "objects/aa/prompt",
		}},
		SupervisorPrincipal: "supervisor-1",
		CoordinatorID:       "coordinator-1", CoordinatorEpoch: 1, CatalogRevision: "catalog-1",
		PrepareTimeout: time.Minute, VerificationTimeout: time.Minute,
		MaxArtifactBytes: 1 << 20, MaxTotalBytes: 2 << 20, Now: now,
	}
	object, err := BuildActivationEvidenceForPackage(input)
	if err != nil {
		t.Fatalf("build frozen evidence: %v", err)
	}
	input.Evidence, err = DecodeActivationEvidenceSnapshot(object.Data)
	if err != nil {
		t.Fatalf("decode frozen evidence: %v", err)
	}
	input.EvidenceArtifact = ActivationEvidenceArtifact("run-1", "supervision", "activation-attempt", object, "objects/aa/evidence", now)
	pkg, err := BuildActivationPackage(input)
	if err != nil {
		t.Fatalf("build activation package: %v", err)
	}
	if len(pkg.StaticInputs) != 1 {
		t.Fatalf("static inputs = %+v, want one frozen evidence object", pkg.StaticInputs)
	}
	if len(pkg.RequiredCapabilities) != 2 ||
		pkg.RequiredCapabilities[1] != workerproto.PackageCapabilitySupervisionEvidence {
		t.Fatalf("required capabilities = %v, want versioned evidence materialization", pkg.RequiredCapabilities)
	}
	got := pkg.StaticInputs[0]
	if got.ID != object.ID || got.SHA256 != object.SHA256 ||
		got.Path != "inputs/supervision-evidence.json" {
		t.Fatalf("frozen evidence input = %+v, want %s/%s", got, object.ID, object.SHA256)
	}
	if !strings.Contains(pkg.Supervision.Prompt, object.ID) ||
		!strings.Contains(pkg.Supervision.Prompt, object.SHA256) {
		t.Fatalf("compact prompt does not pin evidence object: %s", pkg.Supervision.Prompt)
	}
	finalPrompt := workerproto.RenderSupervisionPrompt(*pkg.Supervision)
	if len(finalPrompt) > workerproto.SupervisionPromptByteCap {
		t.Fatalf("model-bound prompt = %d bytes, cap %d", len(finalPrompt), workerproto.SupervisionPromptByteCap)
	}
	if pkg.Supervision.RecoveryProposal != nil {
		t.Fatal("reviewer package gained repair proposal authority")
	}

	repair := input
	repair.Record.Config.Recovery = &domain.RecoveryConfig{
		Version:          domain.RecoveryContractV1,
		Route:            domain.ProviderRoute{ProviderInstanceID: "repairer", Model: "repair-model"},
		PromptArtifactID: "repair-prompt", MaxAttemptsPerIncident: 2,
		IncidentDeadline: time.Hour, StalledAfter: time.Minute,
	}
	repair.Activation.Purpose = domain.RecoveryActivationRepair
	repair.Activation.IncidentID = "incident-1"
	repair.Artifacts = append(append([]domain.Artifact(nil), input.Artifacts...), domain.Artifact{
		ID: "repair-prompt", WorkflowRunID: "run-1", Kind: domain.ArtifactInput,
		Name: "repair.md", MediaType: "text/markdown", Size: 11,
		SHA256: strings.Repeat("b", 64), StoragePath: "objects/bb/prompt",
	})
	diagnostic := domain.RecoveryDiagnosticIdentity{FailureFingerprint: "failed-check", EvidenceFingerprint: "evidence-v1"}
	recovery := domain.NewRecoveryIncident(*repair.Record.Config.Recovery, diagnostic, now)
	repair.Incidents = []domain.ReviewIncident{{
		ID: "incident-1", RunID: "run-1", SourceTaskID: "task-1", SourceAttemptID: "attempt-1",
		Revision: 4, State: domain.IncidentOpen, Recovery: recovery,
	}}
	repair.Attempts = []domain.Attempt{{
		ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1",
		Progress: domain.ProgressFailed, Revision: 5,
	}}
	repairObject, err := BuildActivationEvidenceForPackage(repair)
	if err != nil {
		t.Fatalf("build repair evidence: %v", err)
	}
	repair.Evidence, err = DecodeActivationEvidenceSnapshot(repairObject.Data)
	if err != nil {
		t.Fatal(err)
	}
	repair.EvidenceArtifact = ActivationEvidenceArtifact("run-1", "supervision", "activation-attempt", repairObject, "objects/aa/repair-evidence", now)
	repairPkg, err := BuildActivationPackage(repair)
	if err != nil {
		t.Fatalf("build repair package: %v", err)
	}
	if repairPkg.Prompt.ID != "repair-prompt" || len(repairPkg.RequiredCapabilities) != 3 ||
		repairPkg.Supervision.RecoveryProposal == nil ||
		repairPkg.Supervision.RecoveryProposal.ExpectedIncidentRevision != 4 ||
		repairPkg.Supervision.RecoveryProposal.SourceAttemptRevision != 5 ||
		len(repairPkg.Supervision.Actions) != 1 || repairPkg.Supervision.Actions[0].Name != "retry" {
		t.Fatalf("repair package = %+v", repairPkg)
	}

	oversized := input
	oversized.SupervisorPrincipal = strings.Repeat("p", workerproto.SupervisionPromptByteCap)
	_, err = BuildActivationPackage(oversized)
	var packageErr *ActivationPackageError
	if !errors.As(err, &packageErr) || packageErr.Code != ActivationPackageErrorPromptTooLarge {
		t.Fatalf("oversized final prompt error = %v, want typed %s", err, ActivationPackageErrorPromptTooLarge)
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
