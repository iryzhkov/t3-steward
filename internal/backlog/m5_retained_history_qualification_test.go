package backlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

type m5NotificationControl struct {
	thread   *domain.Thread
	sends    []string
	observed map[string]bool
}

func (c *m5NotificationControl) GetThread(_ context.Context, id string) (*domain.Thread, error) {
	if c.thread != nil && c.thread.ID == id {
		return c.thread, nil
	}
	return nil, nil
}

func (c *m5NotificationControl) ResumeThread(context.Context, domain.Thread, string) error {
	return nil
}

func (c *m5NotificationControl) SendNodeWake(_ context.Context, _ domain.Thread, messageID, _ string) error {
	c.sends = append(c.sends, messageID)
	c.observed[messageID] = true
	return nil
}

func (c *m5NotificationControl) ObserveNodeWake(_ context.Context, _, messageID string) (bool, error) {
	return c.observed[messageID], nil
}

func TestM5RetainedAccumulatedHistorySurvivesRestartAndActivation(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	statePath := filepath.Join(root, "state.db")
	store, err := sqlite.OpenMigrated(statePath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	store.SetClock(func() time.Time { return now })
	submission := &SubmissionService{
		StorageRoot: filepath.Join(root, "bundles"), Store: store,
		MaxBytes: 1 << 20, MaxFiles: 20, Now: func() time.Time { return now },
		NewKey: func() string { return "generated-history-key" },
	}
	t.Cleanup(func() { _ = removeIngestedTree(submission.StorageRoot) })
	result, err := submission.SubmitDirectory(ctx, DirectorySubmission{
		IdempotencyKey: "m5-retained-history", BundleDir: m5HistoryBundle(t),
		Principal: "local:1000",
	})
	if err != nil {
		t.Fatalf("supported manifest/run ingestion: %v", err)
	}
	runID := result.Record.RunID

	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var producerTaskID, protectedTaskID string
	for _, task := range records.Tasks {
		switch task.Name {
		case "producer":
			producerTaskID = task.ID
		case "protected":
			protectedTaskID = task.ID
		}
	}
	var producerAttempt, protectedAttempt domain.Attempt
	for _, attempt := range records.Attempts {
		switch attempt.TaskID {
		case producerTaskID:
			producerAttempt = attempt
		case protectedTaskID:
			protectedAttempt = attempt
		}
	}
	if producerAttempt.ID == "" || protectedAttempt.ID == "" {
		t.Fatalf("ingestion did not create the declared attempts: %+v", records.Attempts)
	}
	initialSupervision, err := store.LoadSupervisionSnapshot(ctx, runID)
	if err != nil || len(initialSupervision.Gates) != 1 {
		t.Fatalf("ingestion did not create one gate: supervision=%+v err=%v", initialSupervision, err)
	}
	gateID := initialSupervision.Gates[0].Definition.ID
	producerAttempt.Progress = domain.ProgressSucceeded
	producerAttempt.Control = domain.ControlStopped
	producerAttempt.Revision++
	producerAttempt.UpdatedAt = now
	output := domain.Artifact{
		ID: "retained-output", WorkflowRunID: runID, TaskID: producerTaskID, AttemptID: producerAttempt.ID,
		Kind: domain.ArtifactOutput, Name: "result.txt", MediaType: "text/plain", Size: 8,
		SHA256: strings.Repeat("a", 64), StoragePath: "objects/retained-output",
		Producer: "worker", CreatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{producerAttempt}, Artifacts: []domain.Artifact{output},
	}); err != nil {
		t.Fatalf("persist producer result custody: %v", err)
	}
	advanced, err := store.AdvanceSupervisionGates(ctx, runID, now)
	if err != nil || len(advanced) != 1 {
		t.Fatalf("advance gate from stored result: advanced=%+v err=%v", advanced, err)
	}
	evidence := advanced[0].Evidence
	if evidence.ID == "" || len(evidence.Producers) != 1 ||
		evidence.Producers[0].AttemptID != producerAttempt.ID ||
		len(evidence.Producers[0].ArtifactDigests) != 1 ||
		evidence.Producers[0].ArtifactDigests[0].ArtifactID != output.ID {
		t.Fatalf("generated evidence did not resolve stored producer custody: %+v", evidence)
	}
	accepted, err := store.DecideGate(ctx, sqlite.GateDecisionRequest{
		RunID: runID, GateID: gateID, RequestID: "accept-retained-review",
		Actor:                 domain.Actor{Kind: domain.ActorOperator, Principal: "operator:m5"},
		ExpectedGraphRevision: 1, ExpectedGateRevision: advanced[0].Gate.Revision,
		Evidence: evidence, Outcome: domain.GateDecisionAccept,
		Reason: "stored result satisfies the retained rubric", DecidedAt: now,
	})
	if err != nil {
		t.Fatalf("accept retained gate through fenced decision API: %v", err)
	}
	if accepted.Gate.State != domain.GateAccepted {
		t.Fatalf("gate was not accepted: %+v", accepted.Gate)
	}
	acceptedGateRevision := accepted.Gate.Revision
	acceptedAttemptRevision := producerAttempt.Revision

	activationStore := CoordinatorSupervisionStore{Store: store}
	activationService := SupervisionActivationService{Store: activationStore, Now: func() time.Time { return now }}
	const prefixCount = 990
	for batchStart := 0; batchStart < prefixCount; batchStart += 45 {
		batchEnd := batchStart + 45
		if batchEnd > prefixCount {
			batchEnd = prefixCount
		}
		events := make([]SupervisionEvent, 0, batchEnd-batchStart)
		for index := batchStart; index < batchEnd; index++ {
			events = append(events, SupervisionEvent{
				ID: fmt.Sprintf("retained-prefix-%04d", index+1), RunID: runID,
				Kind: TriggerOperatorReassessment, Reason: "inert retained-history load",
				GraphRevision: 1, OccurredAt: now.Add(time.Duration(index) * time.Millisecond),
			})
		}
		if written, err := activationService.Observe(ctx, runID, events...); err != nil || written != len(events) {
			t.Fatalf("append inert prefix at %d: written=%d err=%v", batchStart, written, err)
		}
	}
	prefix, err := activationService.Advance(ctx, runID, ActivationSignal{
		Event: domain.ActivationEventTriggerFired, IncidentID: "prefix-review",
		Principal: "supervisor:m5",
	})
	if err != nil || prefix.Activation.ConsumedEventCursor != prefixCount {
		t.Fatalf("bind prefix high-water mark: plan=%+v err=%v", prefix, err)
	}
	if _, err = activationService.Advance(ctx, runID, ActivationSignal{
		Event: domain.ActivationEventDispatchConfirmed,
	}); err != nil {
		t.Fatalf("confirm prefix activation: %v", err)
	}
	consumed, err := activationService.Advance(ctx, runID, ActivationSignal{
		Event: domain.ActivationEventLimitReached, Outcome: domain.ActivationOutcomeDecided,
	})
	if err != nil || !consumed.CursorAdvanced || consumed.Record.EventCursor != prefixCount {
		t.Fatalf("consume prefix through real lifecycle: plan=%+v err=%v", consumed, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = sqlite.OpenMigrated(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now = now.Add(time.Minute)
	store.SetClock(func() time.Time { return now })
	activationStore = CoordinatorSupervisionStore{Store: store}
	activationService = SupervisionActivationService{Store: activationStore, Now: func() time.Time { return now }}
	reopened, err := activationStore.LoadSupervisionActivationState(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Record.EventCursor != prefixCount || len(reopened.Pending) != prefixCount ||
		reopened.Pending[0].Sequence != 1 || reopened.Pending[prefixCount-1].Sequence != prefixCount {
		t.Fatalf("restart did not retain consumed prefix and cursor: cursor=%d pending=%d", reopened.Record.EventCursor, len(reopened.Pending))
	}

	tail := SupervisionEvent{
		ID: "retained-tail-worker-loss", RunID: runID, Kind: TriggerRouteBlockPersistent,
		Reason: "stored producer needs bounded unknown-effect recovery",
		GateID: gateID, TaskID: producerTaskID, AttemptID: producerAttempt.ID,
		IncidentID: "incident-retained-tail", GraphRevision: 1, OccurredAt: now,
		Artifacts: []domain.ArtifactDigest{{ArtifactID: output.ID, Digest: "sha256:" + output.SHA256}},
	}
	if written, err := activationService.Observe(ctx, runID, tail); err != nil || written != 1 {
		t.Fatalf("append causal tail: written=%d err=%v", written, err)
	}
	if _, err := store.OpenReviewIncident(ctx, sqlite.IncidentRequest{
		RunID: runID, IncidentID: tail.IncidentID, RequestID: "open-retained-tail",
		Actor:         domain.Actor{Kind: domain.ActorOperator, Principal: "operator:m5"},
		SourceEventID: tail.ID, GateID: tail.GateID,
		RequiredDisposition: domain.DispositionOperatorAction,
		Reason:              tail.Reason, OpenedAt: now,
	}); err != nil {
		t.Fatalf("persist tail recovery incident: %v", err)
	}
	woken, err := activationService.Advance(ctx, runID, ActivationSignal{Event: domain.ActivationEventEventsArrived})
	if err != nil || woken.Activation.State != domain.ActivationIdle {
		t.Fatalf("wake spent activation for causal tail: plan=%+v err=%v", woken, err)
	}
	current, err := activationService.Advance(ctx, runID, ActivationSignal{
		Event: domain.ActivationEventTriggerFired, IncidentID: tail.IncidentID,
		Principal: "supervisor:m5",
	})
	if err != nil {
		t.Fatalf("select causal tail after restart: %v", err)
	}
	if current.Activation.ReadyTieID != tail.ID ||
		current.Activation.ConsumedEventCursor != prefixCount+1 ||
		current.Inbox.HighWaterMark != prefixCount+1 ||
		len(current.Inbox.Events) != 1 || current.Inbox.Events[0].AttemptID != producerAttempt.ID {
		t.Fatalf("tail was not the current causal subject: activation=%+v inbox=%+v", current.Activation, current.Inbox)
	}

	confirmed, err := activationService.Advance(ctx, runID, ActivationSignal{
		Event: domain.ActivationEventDispatchConfirmed,
	})
	if err != nil || confirmed.Activation.State != domain.ActivationActive {
		t.Fatalf("confirm causal tail activation: plan=%+v err=%v", confirmed, err)
	}
	boundedState := SupervisionActivationState{
		Record: confirmed.Record, Activation: confirmed.Activation,
		Pending: append([]SupervisionEvent(nil), current.Inbox.Events...),
	}
	boundedState.Activation.RecoveredCount = domain.MaxAutoRecoveredActivationsPerIncident
	ambiguous, err := PlanActivation(boundedState, ActivationSignal{
		Event: domain.ActivationEventThreadLost, IncidentID: tail.IncidentID,
		ExecutionObserved: true, Reason: "runtime effect is still unknown",
	}, now)
	if err != nil || ambiguous.Activation.State != domain.ActivationRecoveryRequired || ambiguous.CursorAdvanced {
		t.Fatalf("unknown runtime effect escaped containment: plan=%+v err=%v", ambiguous, err)
	}
	lost, err := PlanActivation(boundedState, ActivationSignal{
		Event: domain.ActivationEventThreadLost, IncidentID: tail.IncidentID,
		ExecutionObserved: true, RuntimeProvenStopped: true,
		Reason: "runtime stop was observed before replacement",
	}, now)
	if err != nil {
		t.Fatalf("plan bounded recovery from selected tail: %v", err)
	}
	if lost.Activation.State != domain.ActivationEscalated || !lost.Escalated ||
		lost.Activation.RecoveredCount != domain.MaxAutoRecoveredActivationsPerIncident {
		t.Fatalf("recovery exceeded its production bound without escalation: %+v", lost)
	}

	notificationID := "retained-tail-final-notification"
	if written, err := store.AppendSupervisionOutboxRows(ctx, runID, []sqlite.SupervisionOutboxRow{{
		ID: notificationID, RunID: runID, Delivery: "pending",
		Record: []byte(`{"id":"retained-tail-final-notification","kind":"escalation","incidentId":"incident-retained-tail","threadId":"thread-m5","reason":"bounded recovery exhausted for retained-tail-worker-loss"}`),
	}}); err != nil || written != 1 {
		t.Fatalf("append causal final notification: written=%d err=%v", written, err)
	}
	control := &m5NotificationControl{
		thread:   &domain.Thread{ID: "thread-m5", ProviderInstanceID: "test-provider"},
		observed: map[string]bool{},
	}
	runner := wait.New(store, control, nil)
	runner.SetClock(func() time.Time { return now })
	runner.DisableQuotaChecks = true
	runner.Tick(ctx, nil, nil)
	now = now.Add(time.Second)
	runner.Tick(ctx, nil, nil)
	runner.Tick(ctx, nil, nil)
	deliveries, err := store.ListSupervisionOutboxRows(ctx, runID)
	var finalDelivery sqlite.SupervisionOutboxRow
	for _, delivery := range deliveries {
		if delivery.ID == notificationID {
			finalDelivery = delivery
		}
	}
	if err != nil || finalDelivery.Delivery != "delivered" || len(control.sends) != 1 {
		t.Fatalf("final notification was not delivered and observed once: final=%+v sends=%v err=%v", finalDelivery, control.sends, err)
	}

	after, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var afterProducer domain.Attempt
	for _, attempt := range after.Attempts {
		if attempt.ID == producerAttempt.ID {
			afterProducer = attempt
		}
	}
	supervision, err := store.LoadSupervisionSnapshot(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if afterProducer.Revision != acceptedAttemptRevision || len(supervision.Gates) != 1 ||
		supervision.Gates[0].State != domain.GateAccepted ||
		supervision.Gates[0].Revision != acceptedGateRevision ||
		supervision.Gates[0].EvidenceSnapshotID != evidence.ID {
		t.Fatalf("accepted branch or gate changed: attempt=%+v supervision=%+v", afterProducer, supervision)
	}
	projection, err := store.LoadSupervisionProjection(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(projection.Decisions) != 1 ||
		projection.Decisions[0].Evidence.ID != evidence.ID ||
		projection.Decisions[0].Evidence.Producers[0].ArtifactDigests[0].ArtifactID != output.ID {
		t.Fatalf("accepted evidence identities no longer resolve: %+v", projection.Decisions)
	}

	snapshot := ActivationSnapshot{
		ActivationID: current.Activation.ID, RunID: runID, Epoch: current.Activation.Epoch,
		GraphRevision: 1, RecordRevision: current.Record.Revision,
		TurnsRemaining: current.Record.Config.MaxTurnsPerActivation,
		Triggers:       current.Inbox.Triggers, ConsumedThrough: current.Inbox.HighWaterMark,
		Tasks: []ActivationTaskView{
			{TaskID: producerTaskID, State: string(producerAttempt.Progress)},
			{TaskID: protectedTaskID, State: string(protectedAttempt.Progress)},
		},
		Gates: []ActivationGateView{{
			GateID: accepted.Gate.Definition.ID, State: accepted.Gate.State,
			GraphRevision: accepted.Gate.GraphRevision, EvidenceSnapshotID: evidence.ID,
			ObservedTaskIDs:  accepted.Gate.Definition.ObservedTaskIDs,
			ProtectedTaskIDs: accepted.Gate.Definition.ProtectedTaskIDs,
		}},
		Artifacts:   []domain.ArtifactDigest{{ArtifactID: output.ID, Digest: "sha256:" + output.SHA256}},
		Actions:     []ActivationAction{{Name: "reconcile retained worker loss"}},
		Constraints: []string{"bounded recovery", "do not rerun accepted producer"},
	}
	boundedEvidence, err := BuildActivationEvidenceSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.EvidenceSnapshot = &domain.ArtifactDigest{ArtifactID: boundedEvidence.ID, Digest: boundedEvidence.SHA256}
	envelope, err := BuildActivationPromptEnvelope(snapshot)
	if err != nil {
		t.Fatalf("bounded prompt from retained causal state: %v", err)
	}
	if envelope.Size() > envelope.ByteCap || len(envelope.Inputs) != 1 ||
		envelope.Inputs[0].Digest != boundedEvidence.SHA256 {
		t.Fatalf("bounded prompt lost retained evidence custody: size=%d cap=%d inputs=%+v", envelope.Size(), envelope.ByteCap, envelope.Inputs)
	}
}

func m5HistoryBundle(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"workflow.yaml": `version: 2
name: retained-history
environment:
  project: t3-steward
routes:
  - instance: workerInstance
    model: worker-model
supervision:
  route:
    instance: overseerInstance
    model: overseer-model
    quota_pool: overseer-pool
  prompt_file: prompts/overseer.md
  max_activations: 8
  max_turns_per_activation: 2
  activation_deadline: 90m
  idle_escalation_after: 12h
  escalation:
    notify_thread: true
gates:
  review:
    after: [producer]
    before: [protected]
    rubric_file: rubrics/review.md
tasks:
  producer:
    prompt_file: prompts/producer.md
    outputs: [result.txt]
  protected:
    prompt_file: prompts/protected.md
    needs: [producer]
`,
		"prompts/overseer.md":  "Review retained evidence and the current trigger without rerunning accepted branches.\n",
		"prompts/producer.md":  "produce result.txt\n",
		"prompts/protected.md": "consume the accepted result\n",
		"rubrics/review.md":    "accept only stored producer evidence\n",
	}
	for name, body := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}
