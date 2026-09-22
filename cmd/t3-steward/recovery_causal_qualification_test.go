package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

const (
	recoveryQualificationRun      = "run-recovery-qualification"
	recoveryQualificationWorkflow = "workflow-recovery-qualification"
	recoveryQualificationWorker   = "worker-recovery-qualification"
	recoveryQualificationEpoch    = "worker-epoch-1"
	recoveryQualificationGate     = "gate-a-success"
)

type recoveryQualificationUpload struct {
	*workerruntime.CustodyStore
}

func (o recoveryQualificationUpload) OpenWorkerUpload(ctx context.Context, object workerproto.ArtifactObject) (io.ReadCloser, error) {
	return o.OpenArtifact(ctx, object)
}

type recoveryQualificationTransport struct {
	now time.Time
}

func (t recoveryQualificationTransport) DeliverWorkerCommands(
	_ context.Context,
	snapshot domain.WorkerSnapshot,
	commands []domain.WorkerCommand,
) ([]domain.WorkerAcknowledgement, error) {
	acks := make([]domain.WorkerAcknowledgement, 0, len(commands))
	for _, command := range commands {
		acks = append(acks, domain.WorkerAcknowledgement{
			CommandID: command.ID, WorkerID: command.WorkerID, WorkerEpoch: command.WorkerEpoch,
			CoordinatorEpoch: command.CoordinatorEpoch, AssignmentID: command.AssignmentID,
			AssignmentEpoch: command.AssignmentEpoch, WorkerSequence: snapshot.Sequence,
			Accepted: true, AcknowledgedAt: t.now,
		})
	}
	return acks, nil
}

func TestRecoveryProposalCausallyReentersOriginalGateAndContinuesDependency(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("causal recovery qualification executes original verification through Linux systemd containment")
	}
	ctx := context.Background()
	now := time.Date(2026, 9, 22, 15, 0, 0, 0, time.UTC)
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.SetClock(func() time.Time { return now })

	normalRoute := domain.ProviderRoute{ProviderInstanceID: "executor", Model: "model", QuotaPoolID: "pool"}
	reviewRoute := domain.ProviderRoute{ProviderInstanceID: "reviewer", Model: "review-model", QuotaPoolID: "pool"}
	repairRoute := domain.ProviderRoute{ProviderInstanceID: "repairer", Model: "repair-model", QuotaPoolID: "pool"}
	config := domain.SupervisionConfig{
		Route: reviewRoute, PromptArtifactID: "review-prompt", MaxActivations: 4,
		MaxTurnsPerActivation: 3, ActivationDeadline: time.Hour,
		Recovery: &domain.RecoveryConfig{
			Version: domain.RecoveryContractV1, Route: repairRoute, PromptArtifactID: "repair-prompt",
			MaxAttemptsPerIncident: 2, IncidentDeadline: 12 * time.Hour, StalledAfter: time.Hour,
		},
	}
	workflow := domain.Workflow{
		ID: recoveryQualificationWorkflow, Version: 1, Name: "recovery qualification", Project: "qualification",
		Environment: domain.ExecutionEnvironment{Type: backlog.EnvironmentGit, Scope: backlog.EnvironmentScopeTask},
		Class:       domain.TaskClassRequired, TaskIDs: []string{"A", "C", "D"}, CreatedAt: now,
	}
	run := domain.WorkflowRun{
		ID: recoveryQualificationRun, WorkflowID: workflow.ID, GraphRevision: 1,
		Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now,
		Supervision: &domain.SupervisionRecord{RunID: recoveryQualificationRun, Config: config},
	}
	taskA := domain.Task{
		ID: "A", WorkflowID: workflow.ID, Name: "A", Class: domain.TaskClassRequired,
		PromptArtifactID: "prompt-a", Outputs: []domain.ArtifactDeclaration{{Name: "result.txt", MediaType: "text/plain"}},
		Verification: []string{"test -s result.txt"}, ResourceLocks: []string{"source-tree"},
		Routes: []domain.ProviderRoute{normalRoute}, MaxTurns: 2,
	}
	taskC := domain.Task{
		ID: "C", WorkflowID: workflow.ID, Name: "dependent", Class: domain.TaskClassRequired,
		Needs: []string{"A"}, PromptArtifactID: "prompt-c", Routes: []domain.ProviderRoute{normalRoute}, MaxTurns: 1,
	}
	taskD := domain.Task{
		ID: "D", WorkflowID: workflow.ID, Name: "parallel success", Class: domain.TaskClassRequired,
		PromptArtifactID: "prompt-d", Routes: []domain.ProviderRoute{normalRoute}, MaxTurns: 1,
	}
	a1 := domain.Attempt{
		ID: "attempt-a-1", WorkflowRunID: run.ID, TaskID: taskA.ID, Number: 1,
		Progress: domain.ProgressFailed, Control: domain.ControlStopped, Failure: "original verification failed",
		Revision: 5, UpdatedAt: now,
	}
	c1 := domain.Attempt{
		ID: "attempt-c-1", WorkflowRunID: run.ID, TaskID: taskC.ID, Number: 1,
		Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, Revision: 1, UpdatedAt: now,
	}
	d1 := domain.Attempt{
		ID: "attempt-d-1", WorkflowRunID: run.ID, TaskID: taskD.ID, Number: 1,
		Progress: domain.ProgressSucceeded, Control: domain.ControlStopped, Revision: 3, UpdatedAt: now,
	}
	definitionArtifact := func(id, taskID string) domain.Artifact {
		return domain.Artifact{
			ID: id, WorkflowRunID: run.ID, TaskID: taskID, Kind: domain.ArtifactInput,
			Name: id + ".md", MediaType: "text/markdown", Size: 8,
			SHA256: strings.Repeat("a", 64), StoragePath: "objects/" + id, Producer: "coordinator", CreatedAt: now,
		}
	}
	records := sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{workflow}, WorkflowRuns: []domain.WorkflowRun{run},
		Tasks: []domain.Task{taskA, taskC, taskD}, Attempts: []domain.Attempt{a1, c1, d1},
		QuotaPools: []domain.QuotaPool{{ID: "pool", ProviderInstanceIDs: []string{"executor", "reviewer", "repairer"}, MaxConcurrent: 4, Admission: domain.AdmissionOpen}},
		Artifacts: []domain.Artifact{
			definitionArtifact("prompt-a", "A"), definitionArtifact("prompt-c", "C"),
			definitionArtifact("prompt-d", "D"), definitionArtifact("review-prompt", ""),
			definitionArtifact("repair-prompt", ""),
		},
	}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSupervision(ctx, sqlite.SupervisionMaterialization{
		Record: domain.SupervisionRecord{RunID: run.ID, Config: config},
		Gates: []domain.Gate{{
			RunID: run.ID, State: domain.GatePendingEvidence, GraphRevision: 1,
			Definition: domain.GateDefinition{
				ID: recoveryQualificationGate, Name: "A success review",
				ObservedTaskIDs: []string{"A"}, ProtectedTaskIDs: []string{"C"},
			},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	worker := domain.WorkerSnapshot{
		WorkerID: recoveryQualificationWorker, WorkerEpoch: recoveryQualificationEpoch,
		CoordinatorEpoch: 1, Sequence: 1, Connected: true, ObservedAt: now, ValidUntil: now.Add(4 * time.Hour),
		Inventory: domain.WorkerInventory{
			ID: recoveryQualificationWorker, AcceptBacklog: true, Health: domain.WorkerHealthReady,
			Allocatable:  domain.AllocatableCapacity{ExecutorSlots: 4},
			Capabilities: []string{workerproto.CapabilityCampaignSupervision},
			Projects:     []domain.WorkerProjectInventory{{Name: "qualification", Available: true, UpdatedAt: now}},
			Providers: []domain.WorkerProviderInventory{
				{InstanceID: normalRoute.ProviderInstanceID, Models: []string{normalRoute.Model}, QuotaPoolID: "pool", Available: true},
				{InstanceID: reviewRoute.ProviderInstanceID, Models: []string{reviewRoute.Model}, QuotaPoolID: "pool", Available: true},
				{InstanceID: repairRoute.ProviderInstanceID, Models: []string{repairRoute.Model}, QuotaPoolID: "pool", Available: true},
			},
		},
	}
	if err := store.SaveWorkerSnapshot(ctx, worker); err != nil {
		t.Fatal(err)
	}
	supervisionStore := backlog.CoordinatorSupervisionStore{Store: store}
	coordinator := coordinatorSupervision{
		store: supervisionStore, logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:         func() time.Time { return now },
		workers:     func(ctx context.Context) ([]domain.WorkerSnapshot, error) { return store.LoadWorkerSnapshots(ctx) },
		activations: backlog.SupervisionActivationService{Store: supervisionStore, Now: func() time.Time { return now }},
		settings: coordinatorActivationSettings{
			CoordinatorID: "coordinator", CoordinatorEpoch: 1, SupervisorClient: "qualification-supervisor",
		},
		quotaMaxConcurrent: map[string]int{"pool": 4},
	}
	if err := coordinator.advanceRun(ctx, run, []domain.WorkerSnapshot{worker}, now); err != nil {
		t.Fatalf("observe A1 failure: %v", err)
	}
	admin, err := store.LoadSupervisionAdminState(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(admin.Incidents) != 1 || admin.Incidents[0].Incident.Recovery == nil {
		t.Fatalf("A1 failure did not create one recovery incident: %+v", admin.Incidents)
	}
	if admin.Gates[0].Gate.State != domain.GatePendingEvidence || admin.Gates[0].Evidence != nil {
		t.Fatalf("success review advanced before recovery: %+v", admin.Gates[0])
	}
	dBefore := mustAttempt(t, store, d1.ID)

	admission := backlog.WorkerAdmissionPolicy{OpenQuotaPools: map[string]struct{}{"pool": {}}}
	coordinator.DispatchActivations(ctx, admission)
	repairAttempt, repairAssignment := mustActivationAssignment(t, store, domain.RecoveryActivationRepair)
	repairPackage := buildQualificationActivationPackage(t, ctx, store, supervisionStore, repairAttempt, repairAssignment, now)
	if repairPackage.Supervision == nil || repairPackage.Supervision.RecoveryProposal == nil ||
		repairPackage.Prompt.ID != "repair-prompt" {
		t.Fatalf("repair package lacked frozen proposal scope: %+v", repairPackage)
	}
	claimQualificationAssignment(t, ctx, store, repairAssignment, now.Add(time.Minute))
	now = now.Add(time.Minute)
	coordinator.DispatchActivations(ctx, admission)

	completeQualificationAssignment(t, ctx, store, repairAssignment.ID, now)
	instructions := []byte("write a non-empty result.txt and rerun the original verification\n")
	checkpoint := []byte("retained checkpoint")
	instructionDigest := fmt.Sprintf("%x", sha256.Sum256(instructions))
	checkpointDigest := fmt.Sprintf("%x", sha256.Sum256(checkpoint))
	scope := repairPackage.Supervision.RecoveryProposal
	diagnostic := scope.Diagnostic
	instruction := domain.ArtifactDigest{ArtifactID: "repair-instructions", Digest: instructionDigest}
	checkpoints := []domain.ArtifactDigest{{ArtifactID: "repair-checkpoint", Digest: checkpointDigest}}
	diagnostic.StrategyFingerprint = domain.RecoveryStrategyFingerprint(instruction, checkpoints)
	proposal, err := domain.SealRecoveryProposal(domain.RecoveryProposal{
		Version: domain.RecoveryProposalVersion, OperationID: "proposal:" + repairAssignment.ID,
		RunID: run.ID, IncidentID: repairPackage.Supervision.IncidentID,
		ExpectedIncidentRevision: scope.ExpectedIncidentRevision, GraphRevision: repairPackage.Supervision.GraphRevision,
		ActivationID: repairPackage.Supervision.ActivationID, ActivationEpoch: repairPackage.Supervision.Epoch,
		AssignmentID: repairAssignment.ID, AssignmentEpoch: repairAssignment.Epoch,
		Principal:       repairPackage.Supervision.Principal,
		SourceAttemptID: scope.SourceAttemptID, SourceAttemptRevision: scope.SourceAttemptRevision,
		InstructionArtifact: instruction, CheckpointArtifacts: checkpoints, Diagnostic: diagnostic, ProposedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	custody := openQualificationCustody(t, now)
	if err := custody.PublishResult(ctx, repairPackage, workerruntime.PublishedResult{
		FinalMessage: "repair proposal complete", ThreadArchive: qualificationArchive(repairAssignment.ThreadID),
		RecoveryProposal: &proposal, RecoveryInstructions: instructions, RecoveryCheckpointTar: checkpoint,
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := custody.PendingUploadByPurpose("result")
	if err != nil || pending == nil {
		t.Fatalf("repair upload: pending=%+v err=%v", pending, err)
	}
	importer := backlog.CoordinatorResultImporter{
		CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store,
		Artifacts:        backlog.CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "coordinator-artifacts"), Catalog: store},
		MaxArtifactBytes: 1 << 20, MaxTotalBytes: 2 << 20, Now: func() time.Time { return now },
	}
	repairReport, err := importer.Import(ctx, workerproto.ArtifactUploadResponse{
		Manifest: pending.Manifest, Custody: pending.Custody,
	}, recoveryQualificationUpload{custody})
	if err != nil {
		t.Fatalf("import repair proposal: %v", err)
	}
	if repairReport.Recovery == nil || repairReport.Recovery.AttemptNumber != 2 {
		t.Fatalf("repair did not create A2: %+v", repairReport)
	}
	a2 := mustAttempt(t, store, repairReport.Recovery.AttemptID)
	if a2.Progress != domain.ProgressReady || a2.TaskID != taskA.ID {
		t.Fatalf("A2 not ready on original task: %+v", a2)
	}
	now = now.Add(time.Minute)
	coordinator.DispatchActivations(ctx, admission)
	repairSpent, err := supervisionStore.LoadSupervisionActivationState(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if repairSpent.Activation.State != domain.ActivationSpent || repairSpent.Activation.Outcome != domain.ActivationOutcomeDecided {
		t.Fatalf("custody-backed repair proposal was not retained as a decision: %+v", repairSpent.Activation)
	}

	a2Assignment := planQualificationTask(t, ctx, store, coordinator, a2.ID, now)
	catalog, err := backlog.NewProjectCatalog(
		[]backlog.ProjectDefinition{{
			Name: "qualification", Repository: "ssh://git/qualification", DefaultRef: "main",
			T3ProjectTemplate: "development", SetupProfile: "test",
		}},
		[]backlog.SetupProfile{{Name: "test", Commands: []string{"true"}, Timeout: time.Minute}},
	)
	if err != nil {
		t.Fatal(err)
	}
	builder := backlog.CoordinatorOfferBuilder{
		Store: store, Catalog: catalog, CatalogRevision: "catalog-1",
		CoordinatorID: "coordinator", CoordinatorEpoch: 1, VerificationTimeout: time.Minute,
		MaxArtifactBytes: 1 << 20, MaxTotalBytes: 2 << 20,
		WorkerCapabilities: map[string][]string{
			recoveryQualificationWorker: workerproto.SupportedPackageCapabilities(),
		},
	}
	a2Leased := a2Assignment
	a2Leased.LeaseExpiresAt = now.Add(time.Hour)
	a2Offer, err := builder.BuildAssignmentOffer(ctx, a2Leased, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("build A2 package: %v; assignment=%+v", err, a2Assignment)
	}
	a2Assignment = a2Offer.Assignment
	a2Package := a2Offer.Package.Package
	if a2Package.Identity.TaskID != taskA.ID || a2Package.Prompt.ID != taskA.PromptArtifactID ||
		len(a2Package.Outputs) != 1 || a2Package.Outputs[0].Name != taskA.Outputs[0].Name ||
		len(a2Package.Verification) != 1 || a2Package.Verification[0] != taskA.Verification[0] ||
		a2Package.Recovery == nil || a2Package.Environment.Project != workflow.Project {
		t.Fatalf("A2 package changed original contract or lost supplement: %+v", a2Package)
	}
	claimQualificationAssignment(t, ctx, store, a2Assignment, now.Add(time.Minute))
	now = now.Add(time.Minute)
	completeQualificationAssignment(t, ctx, store, a2Assignment.ID, now)
	a2 = mustAttempt(t, store, a2.ID)

	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "result.txt"), []byte("repaired\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	finalized, err := (backlog.AttemptFinalizer{
		StorageRoot: qualificationFinalizationRoot(t), Now: func() time.Time { return now },
	}).Finalize(ctx, backlog.AttemptFinalization{
		Task: taskA, Attempt: a2, WorkspaceDir: workspace, ExplicitSuccess: true,
	})
	if err != nil {
		t.Fatalf("execute original A2 verification: %v", err)
	}
	if !finalized.Completion.ExplicitSuccess || !finalized.Completion.VerificationPassed || finalized.Completion.Failure != "" {
		t.Fatalf("original A2 contract failed: %+v", finalized.Completion)
	}
	a2Custody := openQualificationCustody(t, now)
	if err := a2Custody.PublishResult(ctx, a2Package, workerruntime.PublishedResult{
		Finalized: finalized, FinalMessage: "A2 repaired output", ThreadArchive: qualificationArchive(a2Assignment.ThreadID),
	}); err != nil {
		t.Fatal(err)
	}
	a2Pending, err := a2Custody.PendingUploadByPurpose("result")
	if err != nil || a2Pending == nil {
		t.Fatalf("A2 upload: pending=%+v err=%v", a2Pending, err)
	}
	a2Report, err := importer.Import(ctx, workerproto.ArtifactUploadResponse{
		Manifest: a2Pending.Manifest, Custody: a2Pending.Custody,
	}, recoveryQualificationUpload{a2Custody})
	if err != nil {
		t.Fatalf("import A2 result: %v", err)
	}
	if len(a2Report.Transition) != 1 || a2Report.Transition[0].Attempt.Progress != domain.ProgressSucceeded {
		t.Fatalf("A2 did not succeed through result import: %+v", a2Report)
	}

	now = now.Add(time.Minute)
	run = mustRun(t, store, run.ID)
	if err := coordinator.advanceRun(ctx, run, []domain.WorkerSnapshot{worker}, now); err != nil {
		t.Fatalf("advance A2 success into review: %v", err)
	}
	admin, err = store.LoadSupervisionAdminState(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	recoveryResolved := false
	for _, incident := range admin.Incidents {
		if incident.Incident.ID == repairPackage.Supervision.IncidentID && incident.Incident.State == domain.IncidentResolved &&
			incident.Incident.Recovery != nil && incident.Incident.Recovery.State == domain.RecoveryResolved {
			recoveryResolved = true
		}
	}
	if !recoveryResolved {
		t.Fatalf("A2 success did not resolve its recovery episode: %+v", admin.Incidents)
	}
	gate := admin.Gates[0]
	if gate.Gate.State != domain.GateReadyForReview || gate.Evidence == nil ||
		len(gate.Evidence.Producers) != 1 || gate.Evidence.Producers[0].AttemptID != a2.ID {
		t.Fatalf("review evidence was not bound to A2: %+v", gate)
	}
	// The first boundary retires the spent repair activation to a fresh idle
	// epoch; the second dispatches the independently scoped reviewer.
	coordinator.DispatchActivations(ctx, admission)
	coordinator.DispatchActivations(ctx, admission)
	reviewerAttempt, reviewerAssignment := mustActivationAssignment(t, store, "")
	reviewerPackage := buildQualificationActivationPackage(t, ctx, store, supervisionStore, reviewerAttempt, reviewerAssignment, now)
	if reviewerPackage.Supervision == nil || reviewerPackage.Supervision.RecoveryProposal != nil {
		t.Fatalf("reviewer package gained repair authority: %+v", reviewerPackage.Supervision)
	}
	claimQualificationAssignment(t, ctx, store, reviewerAssignment, now.Add(time.Minute))
	now = now.Add(time.Minute)
	coordinator.DispatchActivations(ctx, admission)
	active, err := supervisionStore.LoadSupervisionActivationState(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if active.Activation.ID != reviewerAttempt.SupervisionActivationID || active.Activation.State != domain.ActivationActive {
		t.Fatalf("reviewer activation not active: %+v", active.Activation)
	}
	if _, err := store.DecideGate(ctx, sqlite.GateDecisionRequest{
		RunID: run.ID, GateID: recoveryQualificationGate, RequestID: "accept-a2",
		Actor: domain.Actor{
			Kind: domain.ActorOverseer, Principal: coordinator.settings.principalID(),
			ActivationEpoch: active.Activation.Epoch,
		},
		ExpectedGraphRevision: gate.Gate.GraphRevision, ExpectedGateRevision: gate.Gate.Revision,
		Evidence: *gate.Evidence, Outcome: domain.GateDecisionAccept,
		Reason: "A2 output and original verification match", DecidedAt: now,
	}); err != nil {
		t.Fatalf("independent reviewer accept A2: %v", err)
	}
	admin, err = store.LoadSupervisionAdminState(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if admin.Gates[0].Gate.State != domain.GateAccepted {
		t.Fatalf("gate was not accepted: %+v", admin.Gates[0])
	}

	cAssignment := planQualificationTask(t, ctx, store, coordinator, c1.ID, now.Add(time.Minute))
	if cAssignment.AttemptID != c1.ID {
		t.Fatalf("dependent C did not continue: %+v", cAssignment)
	}
	dAfter := mustAttempt(t, store, d1.ID)
	if dAfter != dBefore {
		t.Fatalf("parallel D changed during recovery:\nbefore=%+v\nafter=%+v", dBefore, dAfter)
	}
}

func buildQualificationActivationPackage(
	t *testing.T,
	ctx context.Context,
	store *sqlite.Store,
	supervision backlog.CoordinatorSupervisionStore,
	attempt domain.Attempt,
	assignment domain.Assignment,
	now time.Time,
) workerproto.ExecutionPackage {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	state, err := supervision.LoadSupervisionActivationState(ctx, attempt.WorkflowRunID)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := store.LoadSupervisionAdminState(ctx, attempt.WorkflowRunID)
	if err != nil {
		t.Fatal(err)
	}
	run := mustRun(t, store, attempt.WorkflowRunID)
	var workflow domain.Workflow
	for _, candidate := range records.Workflows {
		if candidate.ID == run.WorkflowID {
			workflow = candidate
		}
	}
	gates := make([]backlog.ActivationGateFacts, 0, len(admin.Gates))
	for _, facts := range admin.Gates {
		gates = append(gates, backlog.ActivationGateFacts{Gate: facts.Gate, Evidence: facts.Evidence})
	}
	incidents := make([]domain.ReviewIncident, 0, len(admin.Incidents))
	for _, facts := range admin.Incidents {
		incidents = append(incidents, facts.Incident)
	}
	inbox := backlog.CoalesceSupervisionEvents(run.ID, 0, state.Pending)
	dispatch := backlog.ActivationDispatch{
		Identity: state.Activation.DispatchIdentity, Epoch: state.Activation.Epoch,
		LeaseToken: state.Activation.LeaseToken, RequiredCapability: backlog.SupervisionWorkerCapability,
	}
	if state.Activation.LeaseExpiresAt != nil {
		dispatch.LeaseExpiresAt = *state.Activation.LeaseExpiresAt
	}
	if state.Activation.Deadline != nil {
		dispatch.Deadline = *state.Activation.Deadline
	}
	input := backlog.ActivationPackageInput{
		Workflow: workflow, Run: run, Record: state.Record, Activation: state.Activation,
		Dispatch: dispatch, Attempt: attempt, Assignment: assignment, Triggers: inbox.Triggers,
		Tasks: domain.TasksForRun(run, records.Tasks), Attempts: records.Attempts,
		Gates: gates, Incidents: incidents, Artifacts: records.Artifacts,
		SupervisorPrincipal: state.Activation.Principal,
		CoordinatorID:       "coordinator", CoordinatorEpoch: 1, CatalogRevision: "catalog-1",
		PrepareTimeout: time.Minute, VerificationTimeout: time.Minute,
		MaxArtifactBytes: 1 << 20, MaxTotalBytes: 2 << 20, Now: now,
	}
	object, err := backlog.BuildActivationEvidenceForPackage(input)
	if err != nil {
		t.Fatal(err)
	}
	input.Evidence, err = backlog.DecodeActivationEvidenceSnapshot(object.Data)
	if err != nil {
		t.Fatal(err)
	}
	input.EvidenceArtifact = backlog.ActivationEvidenceArtifact(run.ID, attempt.TaskID, attempt.ID, object, "objects/"+object.ID, now)
	pkg, err := backlog.BuildActivationPackage(input)
	if err != nil {
		t.Fatal(err)
	}
	return pkg
}

func claimQualificationAssignment(t *testing.T, ctx context.Context, store *sqlite.Store, assignment domain.Assignment, at time.Time) {
	t.Helper()
	if _, err := store.ClaimAssignment(ctx, domain.AssignmentClaimRequest{
		CoordinatorEpoch: 1, WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch,
		AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, LeaseToken: assignment.LeaseToken,
		ClaimedAt: at, LeaseExpiresAt: at.Add(time.Hour),
	}); err != nil {
		t.Fatalf("claim %s: %v", assignment.ID, err)
	}
}

func completeQualificationAssignment(
	t *testing.T,
	ctx context.Context,
	store *sqlite.Store,
	assignmentID string,
	now time.Time,
) {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var assignment domain.Assignment
	for _, candidate := range records.Assignments {
		if candidate.ID == assignmentID {
			assignment = candidate
		}
	}
	snapshots, err := store.LoadWorkerSnapshots(ctx)
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("load worker snapshot: snapshots=%+v err=%v", snapshots, err)
	}
	snapshot := snapshots[0]
	snapshot.Sequence++
	snapshot.ObservedAt = now
	snapshot.ValidUntil = now.Add(time.Hour)
	snapshot.Assignments = []domain.WorkerAssignmentObservation{{
		AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
		State: domain.AssignmentCompleted, ThreadID: assignment.ThreadID, ObservedAt: now,
	}}
	coordinator := backlog.FleetCoordinator{Store: store, Now: func() time.Time { return now }}
	transport := recoveryQualificationTransport{now: now}
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.ReconcileWorkerCommands(ctx, snapshot, transport); err != nil {
		t.Fatalf("plan collection for %s: %v", assignmentID, err)
	}
	snapshot.Sequence++
	snapshot.ObservedAt = now.Add(time.Second)
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.ReconcileWorkerCommands(ctx, snapshot, transport); err != nil {
		t.Fatalf("settle collection for %s: %v", assignmentID, err)
	}
	settled := mustAssignment(t, store, assignmentID)
	attempt := mustAttempt(t, store, settled.AttemptID)
	if settled.State != domain.AssignmentCompleted || attempt.Progress != domain.ProgressVerifying {
		t.Fatalf("worker completion did not reach import boundary: assignment=%+v attempt=%+v", settled, attempt)
	}
}

func planQualificationTask(
	t *testing.T,
	ctx context.Context,
	store *sqlite.Store,
	coordinator coordinatorSupervision,
	attemptID string,
	now time.Time,
) domain.Assignment {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	workers, err := store.LoadWorkerSnapshots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	snapshots, err := coordinatorSupervisionSnapshots(ctx, store, records.WorkflowRuns, workers, true)
	if err != nil {
		t.Fatal(err)
	}
	input, err := backlog.BuildCoordinatorPlanInput(backlog.CoordinatorPlanningStateInput{
		DisableQuotaChecks: true, Now: now, CoordinatorEpoch: 1,
		Workflows: records.Workflows, WorkflowRuns: records.WorkflowRuns, Tasks: records.Tasks,
		Attempts: records.Attempts, Assignments: records.Assignments, WorkerSnapshots: workers, QuotaPools: records.QuotaPools,
		MaxWorkerSnapshotAge: 5 * time.Hour, MaxQuotaObservationAge: time.Hour,
		DeadlineRiskWindow: time.Hour, CheckpointMargin: 0, SupervisionSnapshots: snapshots,
	})
	if err != nil {
		t.Fatalf("build plan input for %s: %v", attemptID, err)
	}
	planned, err := (backlog.FleetCoordinator{Store: store, Now: func() time.Time { return now }}).PlanAndCommit(ctx, input)
	if err != nil {
		t.Fatalf("plan %s: %v", attemptID, err)
	}
	for _, assignment := range planned.Assignments {
		if assignment.AttemptID == attemptID {
			return assignment
		}
	}
	t.Fatalf("attempt %s was not continued; plan=%+v", attemptID, planned.Plan)
	return domain.Assignment{}
}

func qualificationFinalizationRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	t.Cleanup(func() {
		_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			mode := os.FileMode(0o600)
			if info.IsDir() {
				mode = 0o700
			}
			_ = os.Chmod(path, mode)
			return nil
		})
	})
	return root
}

func openQualificationCustody(t *testing.T, now time.Time) *workerruntime.CustodyStore {
	t.Helper()
	custody, err := workerruntime.OpenCustodyStore(workerruntime.CustodyConfig{
		Root: t.TempDir(), CoordinatorID: "coordinator", CoordinatorEpoch: 1,
		WorkerID: recoveryQualificationWorker, WorkerEpoch: recoveryQualificationEpoch,
		MaxArtifactBytes: 1 << 20, MaxTotalBytes: 2 << 20, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return custody
}

func qualificationArchive(threadID string) []byte {
	return []byte(fmt.Sprintf(`{"thread":{"id":%q,"latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-22T15:00:00Z","completedAt":"2026-09-22T15:01:00Z"},"session":{"threadId":%q,"status":"ready","activeTurnId":null,"lastError":null}}}`, threadID, threadID))
}

func mustActivationAssignment(t *testing.T, store *sqlite.Store, purpose domain.RecoveryActivationPurpose) (domain.Attempt, domain.Assignment) {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state, err := (backlog.CoordinatorSupervisionStore{Store: store}).LoadSupervisionActivationState(context.Background(), recoveryQualificationRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.Purpose != purpose {
		t.Fatalf("activation purpose=%q want=%q", state.Activation.Purpose, purpose)
	}
	for _, attempt := range records.Attempts {
		if attempt.SupervisionActivationID == state.Activation.ID {
			for _, assignment := range records.Assignments {
				if assignment.AttemptID == attempt.ID {
					return attempt, assignment
				}
			}
		}
	}
	t.Fatalf("activation %s has no assignment", state.Activation.ID)
	return domain.Attempt{}, domain.Assignment{}
}

func mustAttempt(t *testing.T, store *sqlite.Store, id string) domain.Attempt {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range records.Attempts {
		if attempt.ID == id {
			return attempt
		}
	}
	t.Fatalf("attempt %s not found", id)
	return domain.Attempt{}
}

func mustAssignment(t *testing.T, store *sqlite.Store, id string) domain.Assignment {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, assignment := range records.Assignments {
		if assignment.ID == id {
			return assignment
		}
	}
	t.Fatalf("assignment %s not found", id)
	return domain.Assignment{}
}

func mustRun(t *testing.T, store *sqlite.Store, id string) domain.WorkflowRun {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range records.WorkflowRuns {
		if run.ID == id {
			return run
		}
	}
	t.Fatalf("run %s not found", id)
	return domain.WorkflowRun{}
}
