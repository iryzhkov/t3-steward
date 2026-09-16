package backlog

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func supervisionTestTime() time.Time {
	return time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
}

func supervisionTestRecord() domain.SupervisionRecord {
	return domain.SupervisionRecord{
		RunID: "run-1",
		Config: domain.SupervisionConfig{
			Route:                 domain.ProviderRoute{ProviderInstanceID: "codex-main", Model: "gpt-5.6-sol"},
			PromptArtifactID:      "artifact-prompt",
			MaxActivations:        3,
			MaxTurnsPerActivation: 2,
			ActivationDeadline:    2 * time.Hour,
			Escalation:            domain.SupervisionEscalation{NotifyThread: true, ThreadID: "thread-notify"},
		},
		ActivationEpoch: 1,
		Revision:        7,
	}
}

func supervisionTestEvent(id string, sequence int64, kind SupervisionTriggerKind) SupervisionEvent {
	return SupervisionEvent{
		ID: id, RunID: "run-1", Kind: kind, Sequence: sequence,
		Reason: "gate ready for review", GateID: "gate-implementation",
		OccurredAt: supervisionTestTime(),
	}
}

func liveActivation(now time.Time) domain.Activation {
	expiry := now.Add(10 * time.Minute)
	deadline := now.Add(time.Hour)
	return domain.Activation{
		ID:                  ActivationID("run-1", 1),
		RunID:               "run-1",
		Epoch:               1,
		DispatchIdentity:    ActivationDispatchIdentity("run-1", 1),
		State:               domain.ActivationActive,
		LeaseToken:          "lease-1",
		LeaseExpiresAt:      &expiry,
		Deadline:            &deadline,
		ConsumedEventCursor: 2,
	}
}

func TestActivationTriggerDispatchesDeterministicIdentity(t *testing.T) {
	now := supervisionTestTime()
	state := SupervisionActivationState{
		Record:  supervisionTestRecord(),
		Pending: []SupervisionEvent{supervisionTestEvent("event-1", 1, TriggerGateReviewReady)},
	}
	plan, err := PlanActivation(state, ActivationSignal{Event: domain.ActivationEventTriggerFired}, now)
	if err != nil {
		t.Fatalf("plan activation: %v", err)
	}
	if plan.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("state = %q, want pending-dispatch", plan.Activation.State)
	}
	if plan.Dispatch == nil || plan.Dispatch.Identity != ActivationDispatchIdentity("run-1", 1) {
		t.Fatalf("dispatch identity = %+v, want the derived identity", plan.Dispatch)
	}
	if plan.Dispatch.Retry {
		t.Fatal("a first dispatch must not be marked as a retry")
	}
	if plan.Dispatch.RequiredCapability != SupervisionWorkerCapability {
		t.Fatalf("required capability = %q, want %q", plan.Dispatch.RequiredCapability, SupervisionWorkerCapability)
	}
	if plan.Activation.ConsumedEventCursor != 1 {
		t.Fatalf("bound high-water mark = %d, want 1", plan.Activation.ConsumedEventCursor)
	}
	if plan.CursorAdvanced || plan.Record.EventCursor != 0 {
		t.Fatal("the cursor advances with the outcome, not at dispatch")
	}
	if plan.Record.ActivationsUsed != 0 {
		t.Fatalf("activations used = %d, want 0 before dispatch is confirmed", plan.Record.ActivationsUsed)
	}
}

func TestActivationOnlyOneValidActivationPerRun(t *testing.T) {
	state := SupervisionActivationState{
		Record:               supervisionTestRecord(),
		Pending:              []SupervisionEvent{supervisionTestEvent("event-1", 1, TriggerGateReviewReady)},
		OtherValidActivation: true,
	}
	_, err := PlanActivation(state, ActivationSignal{Event: domain.ActivationEventTriggerFired}, supervisionTestTime())
	if !errors.Is(err, domain.ErrSupervisionPrerequisite) {
		t.Fatalf("error = %v, want an unmet prerequisite for a second activation", err)
	}
}

func TestActivationUndeliveredDispatchRetriesWithOriginalIdentity(t *testing.T) {
	now := supervisionTestTime()
	record := supervisionTestRecord()
	original := ActivationDispatchIdentity("run-1", 1)
	state := SupervisionActivationState{
		Record: record,
		Activation: domain.Activation{
			ID: ActivationID("run-1", 1), RunID: "run-1", Epoch: 1,
			DispatchIdentity: original, State: domain.ActivationPendingDispatch, ConsumedEventCursor: 4,
		},
		Pending: []SupervisionEvent{supervisionTestEvent("event-1", 4, TriggerGateReviewReady)},
	}
	plan, err := PlanActivation(state, ActivationSignal{Event: domain.ActivationEventDispatchUndelivered}, now)
	if err != nil {
		t.Fatalf("plan activation: %v", err)
	}
	if plan.Activation.DispatchIdentity != original || plan.Dispatch == nil || plan.Dispatch.Identity != original {
		t.Fatalf("retry identity = %q, want the original %q", plan.Activation.DispatchIdentity, original)
	}
	if !plan.Dispatch.Retry {
		t.Fatal("an undelivered dispatch must be marked as a retry")
	}
	if plan.Record.ActivationsUsed != 0 {
		t.Fatalf("activations used = %d, want 0: a provably undelivered dispatch spends no budget", plan.Record.ActivationsUsed)
	}
	if plan.Activation.Epoch != 1 {
		t.Fatalf("epoch = %d, want 1: a retry is the same activation", plan.Activation.Epoch)
	}
}

func TestActivationUndeliveredDispatchRefusedAfterExecutionObserved(t *testing.T) {
	state := SupervisionActivationState{
		Record: supervisionTestRecord(),
		Activation: domain.Activation{
			RunID: "run-1", Epoch: 1, State: domain.ActivationPendingDispatch,
			DispatchIdentity: ActivationDispatchIdentity("run-1", 1),
		},
	}
	_, err := PlanActivation(state,
		ActivationSignal{Event: domain.ActivationEventDispatchUndelivered, ExecutionObserved: true}, supervisionTestTime())
	if !errors.Is(err, domain.ErrSupervisionPrerequisite) {
		t.Fatalf("error = %v, want a refusal: observed execution is not an undelivered dispatch", err)
	}
}

func TestActivationStartedThenVanishedCountsTowardBudget(t *testing.T) {
	now := supervisionTestTime()
	state := SupervisionActivationState{Record: supervisionTestRecord(), Activation: liveActivation(now)}
	plan, err := PlanActivation(state, ActivationSignal{
		Event: domain.ActivationEventThreadLost, ExecutionObserved: true, RuntimeProvenStopped: true,
	}, now)
	if err != nil {
		t.Fatalf("plan activation: %v", err)
	}
	if plan.Record.ActivationsUsed != 1 {
		t.Fatalf("activations used = %d, want 1: a started-then-vanished activation counts", plan.Record.ActivationsUsed)
	}
	if plan.Activation.Epoch != 2 || plan.Activation.State != domain.ActivationIdle {
		t.Fatalf("replacement = %s at epoch %d, want idle at epoch 2", plan.Activation.State, plan.Activation.Epoch)
	}
	if plan.Activation.RecoveredCount != 1 {
		t.Fatalf("recovered count = %d, want 1", plan.Activation.RecoveredCount)
	}
	if plan.CursorAdvanced {
		t.Fatal("a lost thread consumed nothing, so its inbox must survive")
	}
}

func TestActivationLostThreadWithoutProofRequiresRecovery(t *testing.T) {
	now := supervisionTestTime()
	state := SupervisionActivationState{Record: supervisionTestRecord(), Activation: liveActivation(now)}
	plan, err := PlanActivation(state,
		ActivationSignal{Event: domain.ActivationEventThreadLost, ExecutionObserved: true}, now)
	if err != nil {
		t.Fatalf("plan activation: %v", err)
	}
	if plan.Activation.State != domain.ActivationRecoveryRequired {
		t.Fatalf("state = %q, want recovery-required: an ambiguous runtime authorizes no replacement", plan.Activation.State)
	}
}

func TestActivationAutomaticRecoveryIsBoundedPerIncident(t *testing.T) {
	now := supervisionTestTime()
	activation := liveActivation(now)
	activation.RecoveredCount = domain.MaxAutoRecoveredActivationsPerIncident
	state := SupervisionActivationState{Record: supervisionTestRecord(), Activation: activation}
	plan, err := PlanActivation(state, ActivationSignal{
		Event: domain.ActivationEventThreadLost, ExecutionObserved: true, RuntimeProvenStopped: true,
		IncidentID: "incident-9", Reason: "overseer thread lost three times",
	}, now)
	if err != nil {
		t.Fatalf("plan activation: %v", err)
	}
	if plan.Activation.State != domain.ActivationEscalated || !plan.Escalated {
		t.Fatalf("state = %q, want escalated after %d automatic recoveries",
			plan.Activation.State, domain.MaxAutoRecoveredActivationsPerIncident)
	}
	if len(plan.Outbox) != 1 || plan.Outbox[0].IncidentID != "incident-9" {
		t.Fatalf("outbox = %+v, want one escalation for incident-9", plan.Outbox)
	}
}

func TestActivationLeaseExpiryRevokesAuthority(t *testing.T) {
	now := supervisionTestTime()
	activation := liveActivation(now)
	state := SupervisionActivationState{Record: supervisionTestRecord(), Activation: activation}

	if err := AuthorizeActivationDecision(state,
		domain.Actor{Kind: domain.ActorOverseer, Principal: "overseer", ActivationEpoch: 1}, 1, 7, now); err != nil {
		t.Fatalf("a live lease must authorize its own decision: %v", err)
	}
	expired := now.Add(time.Hour)
	err := AuthorizeActivationDecision(state,
		domain.Actor{Kind: domain.ActorOverseer, Principal: "overseer", ActivationEpoch: 1}, 1, 7, expired)
	if !errors.Is(err, domain.ErrSupervisionPrerequisite) {
		t.Fatalf("error = %v, want authority revoked once the lease expired", err)
	}

	plan, err := PlanActivation(state, ActivationSignal{Event: domain.ActivationEventLeaseExpired}, expired)
	if err != nil {
		t.Fatalf("plan activation: %v", err)
	}
	if plan.Activation.State != domain.ActivationRevoked || plan.Activation.Outcome != domain.ActivationOutcomeRevoked {
		t.Fatalf("state = %q outcome = %q, want revoked", plan.Activation.State, plan.Activation.Outcome)
	}
	if plan.Activation.LeaseToken != "" || plan.Activation.LeaseExpiresAt != nil {
		t.Fatal("a revoked activation must hold no lease")
	}
	if plan.CursorAdvanced {
		t.Fatal("a revoked activation preserves its unacknowledged inbox")
	}
}

func TestActivationDecisionRefusedOnStaleEpoch(t *testing.T) {
	now := supervisionTestTime()
	state := SupervisionActivationState{Record: supervisionTestRecord(), Activation: liveActivation(now)}
	err := AuthorizeActivationDecision(state,
		domain.Actor{Kind: domain.ActorOverseer, Principal: "overseer", ActivationEpoch: 0}, 0, 7, now)
	if !errors.Is(err, domain.ErrSupervisionStaleRevision) {
		t.Fatalf("error = %v, want a stale epoch refusal", err)
	}
}

func TestActivationEventsArrivingDuringReviewStayPending(t *testing.T) {
	now := supervisionTestTime()
	record := supervisionTestRecord()
	activation := liveActivation(now)
	activation.ConsumedEventCursor = 2
	during := supervisionTestEvent("event-3", 3, TriggerTaskJudgmentRequired)
	state := SupervisionActivationState{
		Record:     record,
		Activation: activation,
		Pending: []SupervisionEvent{
			supervisionTestEvent("event-1", 1, TriggerGateReviewReady),
			supervisionTestEvent("event-2", 2, TriggerGateReviewReady),
			during,
		},
	}
	plan, err := PlanActivation(state, ActivationSignal{Event: domain.ActivationEventLimitReached}, now)
	if err != nil {
		t.Fatalf("plan activation: %v", err)
	}
	if plan.Activation.State != domain.ActivationSpent {
		t.Fatalf("state = %q, want spent", plan.Activation.State)
	}
	if !plan.CursorAdvanced || plan.Record.EventCursor != 2 {
		t.Fatalf("cursor = %d (advanced %v), want the bound mark 2 recorded with the outcome",
			plan.Record.EventCursor, plan.CursorAdvanced)
	}
	remaining := CoalesceSupervisionEvents("run-1", plan.Record.EventCursor, state.Pending)
	if len(remaining.Events) != 1 || remaining.Events[0].ID != "event-3" {
		t.Fatalf("remaining inbox = %+v, want only the event that arrived during review", remaining.Events)
	}
}

func TestActivationCoalescingPreservesEveryEvidenceReference(t *testing.T) {
	first := supervisionTestEvent("event-1", 1, TriggerGateReviewReady)
	first.TaskID, first.AttemptID = "implement", "attempt-a"
	first.Artifacts = []domain.ArtifactDigest{{ArtifactID: "artifact-1", Digest: "sha256:aaa"}}
	first.GraphRevision = 4
	second := supervisionTestEvent("event-2", 2, TriggerGateReviewReady)
	second.TaskID, second.AttemptID = "implement", "attempt-b"
	second.Reason = "new producer evidence"
	second.Artifacts = []domain.ArtifactDigest{{ArtifactID: "artifact-2", Digest: "sha256:bbb"}}
	second.GraphRevision = 5
	other := supervisionTestEvent("event-3", 3, TriggerGateReviewReady)
	other.GateID = "gate-qualify"

	inbox := CoalesceSupervisionEvents("run-1", 0, []SupervisionEvent{second, first, other})
	if len(inbox.Triggers) != 2 {
		t.Fatalf("triggers = %d, want one per gate", len(inbox.Triggers))
	}
	folded := inbox.Triggers[0]
	if len(folded.EventIDs) != 2 || folded.EventIDs[0] != "event-1" || folded.EventIDs[1] != "event-2" {
		t.Fatalf("event ids = %v, want both in sequence order", folded.EventIDs)
	}
	if len(folded.Reasons) != 2 {
		t.Fatalf("reasons = %v, want both distinct reasons preserved", folded.Reasons)
	}
	if len(folded.Artifacts) != 2 {
		t.Fatalf("artifacts = %v, want every digest preserved", folded.Artifacts)
	}
	if len(folded.AttemptIDs) != 2 {
		t.Fatalf("attempt ids = %v, want every attempt preserved", folded.AttemptIDs)
	}
	if folded.GraphRevision != 5 || folded.LastSequence != 2 {
		t.Fatalf("folded = %+v, want the newest graph revision and sequence", folded)
	}
	if inbox.HighWaterMark != 3 {
		t.Fatalf("high-water mark = %d, want 3", inbox.HighWaterMark)
	}
}

func TestActivationDuplicateEventDeliveryIsIdempotent(t *testing.T) {
	event := supervisionTestEvent("event-1", 1, TriggerGateReviewReady)
	inbox := CoalesceSupervisionEvents("run-1", 0, []SupervisionEvent{event, event, event})
	if len(inbox.Events) != 1 {
		t.Fatalf("events = %d, want one: a repeated delivery is the same event", len(inbox.Events))
	}
	if inbox.Duplicates != 2 {
		t.Fatalf("duplicates = %d, want 2 observable repeats", inbox.Duplicates)
	}
	if len(inbox.Triggers) != 1 || len(inbox.Triggers[0].EventIDs) != 1 {
		t.Fatalf("triggers = %+v, want one reference to one event", inbox.Triggers)
	}
	consumed := CoalesceSupervisionEvents("run-1", 1, []SupervisionEvent{event})
	if consumed.NonEmpty() {
		t.Fatal("an event at or below the cursor was already consumed and must not wake anybody again")
	}
}

func TestActivationBudgetExhaustionEscalatesOnceForOneIncident(t *testing.T) {
	now := supervisionTestTime()
	record := supervisionTestRecord()
	record.ActivationsUsed = record.Config.MaxActivations
	state := SupervisionActivationState{
		Record:  record,
		Pending: []SupervisionEvent{supervisionTestEvent("event-1", 1, TriggerGateReviewReady)},
	}
	signal := ActivationSignal{Event: domain.ActivationEventTriggerFired, IncidentID: "incident-budget"}
	plan, err := PlanActivation(state, signal, now)
	if err != nil {
		t.Fatalf("plan activation: %v", err)
	}
	if plan.Activation.State != domain.ActivationEscalated {
		t.Fatalf("state = %q, want escalated when the budget is exhausted", plan.Activation.State)
	}
	if len(plan.Outbox) != 1 {
		t.Fatalf("outbox = %+v, want exactly one escalation", plan.Outbox)
	}

	// The same incident, escalated again, must not notify twice.
	state.Outbox = plan.Outbox
	repeat, err := PlanActivation(state, signal, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("plan activation: %v", err)
	}
	if len(repeat.Outbox) != 0 {
		t.Fatalf("outbox = %+v, want no second notification for incident-budget", repeat.Outbox)
	}

	// A different incident is a different thing to say.
	signal.IncidentID = "incident-other"
	fresh, err := PlanActivation(state, signal, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("plan activation: %v", err)
	}
	if len(fresh.Outbox) != 1 || fresh.Outbox[0].IncidentID != "incident-other" {
		t.Fatalf("outbox = %+v, want one escalation for the new incident", fresh.Outbox)
	}
}

func TestActivationOperatorContinuationIssuesFreshBudgetAndEpoch(t *testing.T) {
	now := supervisionTestTime()
	record := supervisionTestRecord()
	record.ActivationsUsed = record.Config.MaxActivations
	state := SupervisionActivationState{
		Record:     record,
		Activation: domain.Activation{RunID: "run-1", Epoch: 1, State: domain.ActivationEscalated},
	}
	operator := domain.Actor{Kind: domain.ActorOperator, Principal: "igor"}
	plan, err := PlanActivation(state, ActivationSignal{
		Event: domain.ActivationEventOperatorContinuation, Actor: operator, OperatorAuthorized: true,
		GrantActivations: 2, RequestID: "request-1", Reason: "reviewed manually",
	}, now)
	if err != nil {
		t.Fatalf("plan activation: %v", err)
	}
	if plan.Activation.Epoch != 2 || plan.Activation.State != domain.ActivationIdle {
		t.Fatalf("activation = %s at epoch %d, want idle at epoch 2", plan.Activation.State, plan.Activation.Epoch)
	}
	if plan.Record.BudgetGrantedActivations != record.ActivationsUsed+2 {
		t.Fatalf("granted budget = %d, want the used count plus the grant", plan.Record.BudgetGrantedActivations)
	}
	if plan.Record.ActivationsUsed != record.ActivationsUsed {
		t.Fatal("a continuation raises the budget; it never silently resets the counter")
	}
	if plan.Receipt == nil || plan.Receipt.RequestID != "request-1" || plan.Receipt.GrantedActivations != 2 {
		t.Fatalf("receipt = %+v, want an operator continuation receipt", plan.Receipt)
	}
}

func TestActivationContinuationRefusesOverseerAuthority(t *testing.T) {
	state := SupervisionActivationState{
		Record:     supervisionTestRecord(),
		Activation: domain.Activation{RunID: "run-1", Epoch: 1, State: domain.ActivationEscalated},
	}
	_, err := PlanActivation(state, ActivationSignal{
		Event:              domain.ActivationEventOperatorContinuation,
		Actor:              domain.Actor{Kind: domain.ActorOverseer, Principal: "overseer", ActivationEpoch: 1},
		OperatorAuthorized: true,
	}, supervisionTestTime())
	if !errors.Is(err, domain.ErrSupervisionUnauthorizedActor) {
		t.Fatalf("error = %v, want an overseer refused its own continuation", err)
	}
}

func TestActivationDispatchConfirmationSpendsBudgetOnce(t *testing.T) {
	now := supervisionTestTime()
	expiry := now.Add(5 * time.Minute)
	state := SupervisionActivationState{
		Record: supervisionTestRecord(),
		Activation: domain.Activation{
			ID: ActivationID("run-1", 1), RunID: "run-1", Epoch: 1,
			DispatchIdentity: ActivationDispatchIdentity("run-1", 1),
			State:            domain.ActivationPendingDispatch,
			LeaseToken:       "lease-1", LeaseExpiresAt: &expiry, ConsumedEventCursor: 3,
		},
	}
	plan, err := PlanActivation(state, ActivationSignal{Event: domain.ActivationEventDispatchConfirmed}, now)
	if err != nil {
		t.Fatalf("plan activation: %v", err)
	}
	if plan.Activation.State != domain.ActivationActive || plan.Record.ActivationsUsed != 1 {
		t.Fatalf("state = %q used = %d, want active and one activation spent",
			plan.Activation.State, plan.Record.ActivationsUsed)
	}
	if plan.Activation.StartedAt == nil {
		t.Fatal("a confirmed dispatch records when the activation started")
	}
}

func TestObserveSupervisionBlockEmitsOnlyThresholdCrossings(t *testing.T) {
	now := supervisionTestTime()
	threshold := 30 * time.Minute
	state, emitted := ObserveSupervisionBlock(SupervisionBlockObservation{}, "run-1", "quota pool exhausted", threshold, now)
	if emitted {
		t.Fatal("the first observation of a reason is not an incident")
	}
	state, emitted = ObserveSupervisionBlock(state, "run-1", "quota pool exhausted", threshold, now.Add(5*time.Minute))
	if emitted {
		t.Fatal("ordinary brief waiting must never raise an incident")
	}
	state, emitted = ObserveSupervisionBlock(state, "run-1", "quota pool exhausted", threshold, now.Add(31*time.Minute))
	if !emitted {
		t.Fatal("a reason that persisted past the threshold must raise exactly one incident")
	}
	_, emitted = ObserveSupervisionBlock(state, "run-1", "quota pool exhausted", threshold, now.Add(90*time.Minute))
	if emitted {
		t.Fatal("only reason transitions and threshold crossings are emitted, not every tick")
	}
	changed, emitted := ObserveSupervisionBlock(state, "run-1", "no eligible worker", threshold, now.Add(95*time.Minute))
	if emitted || changed.Reported {
		t.Fatal("a changed reason restarts the clock rather than reporting immediately")
	}
}

func TestActivationPromptEnvelopeIsBoundedAndUntrusted(t *testing.T) {
	inbox := CoalesceSupervisionEvents("run-1", 0, []SupervisionEvent{
		supervisionTestEvent("event-1", 1, TriggerGateReviewReady),
	})
	snapshot := ActivationSnapshot{
		ActivationID: ActivationID("run-1", 1), RunID: "run-1", Epoch: 1,
		GraphRevision: 4, RecordRevision: 7, TurnsRemaining: 2,
		Deadline: supervisionTestTime().Add(time.Hour),
		Tasks: []ActivationTaskView{{
			TaskID: "implement", State: "succeeded", AttemptID: "attempt-a",
			AttemptRevision: 12, Verification: "passed",
		}},
		Gates: []ActivationGateView{{
			GateID: "gate-implementation", State: domain.GateReadyForReview, GraphRevision: 4,
			EvidenceSnapshotID: "snapshot-1", ObservedTaskIDs: []string{"implement"},
			ProtectedTaskIDs: []string{"qualify"},
		}},
		Incidents: []ActivationIncidentView{{
			IncidentID: "incident-1", State: domain.IncidentOpen,
			RequiredDisposition: domain.DispositionGateDecision, Revision: 3, Reason: "gate ready",
		}},
		Triggers:  inbox.Triggers,
		Artifacts: []domain.ArtifactDigest{{ArtifactID: "artifact-1", Digest: "sha256:aaa"}},
		Actions: []ActivationAction{{
			Name:        "supervision decide",
			Constraints: []string{"names an evidence snapshot", "names expected revision 7"},
		}},
		ConsumedThrough: inbox.HighWaterMark,
	}
	envelope, err := BuildActivationPromptEnvelope(snapshot)
	if err != nil {
		t.Fatalf("build activation envelope: %v", err)
	}
	rendered := envelope.Render()
	if envelope.Size() > envelope.ByteCap {
		t.Fatalf("envelope is %d bytes, over its %d byte cap", envelope.Size(), envelope.ByteCap)
	}
	if !strings.Contains(rendered, "untrusted evidence") {
		t.Fatal("the envelope must say that worker output is untrusted evidence")
	}
	if !strings.Contains(rendered, "not a gate acceptance") {
		t.Fatal("the envelope must say that ending the turn is not a gate acceptance")
	}
	if len(envelope.Inputs) != 1 || envelope.Inputs[0].Digest != "sha256:aaa" {
		t.Fatalf("inputs = %+v, want the artifact named by digest and fetched on demand", envelope.Inputs)
	}
	for _, want := range []string{"gate-implementation", "snapshot-1", "attempt-a", "incident-1", "event-1"} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("envelope does not reference %q", want)
		}
	}
	if !strings.Contains(rendered, "supervision record revision: 7") {
		t.Fatal("the envelope must state the exact revision a decision has to name")
	}
}

func TestActivationPromptEnvelopeDemotesFactsToReferences(t *testing.T) {
	inbox := CoalesceSupervisionEvents("run-1", 0, []SupervisionEvent{
		supervisionTestEvent("event-1", 1, TriggerGateReviewReady),
	})
	snapshot := ActivationSnapshot{
		ActivationID: ActivationID("run-1", 1), RunID: "run-1", Epoch: 1, RecordRevision: 7,
		Triggers: inbox.Triggers,
		Actions:  []ActivationAction{{Name: "supervision escalate"}},
		ByteCap:  3000,
	}
	for index := 0; index < 30; index++ {
		snapshot.Tasks = append(snapshot.Tasks, ActivationTaskView{
			TaskID: "task-" + string(rune('a'+index%26)) + string(rune('a'+index/26)),
			State:  "failed", AttemptID: "attempt", Verification: "failed verification command",
		})
	}
	envelope, err := BuildActivationPromptEnvelope(snapshot)
	if err != nil {
		t.Fatalf("build activation envelope: %v", err)
	}
	if envelope.Size() > envelope.ByteCap {
		t.Fatalf("envelope is %d bytes, over its %d byte cap", envelope.Size(), envelope.ByteCap)
	}
	demoted := 0
	for _, fact := range envelope.Facts {
		if fact.Include == domain.FactIncludeReference {
			demoted++
			if fact.Reference == "" {
				t.Fatalf("fact %q was demoted without a reference to fetch", fact.ID)
			}
		}
	}
	if demoted == 0 {
		t.Fatal("an oversized snapshot must demote facts to references rather than drop evidence")
	}
}

func TestActivationSnapshotRequiresScopedActionsAndTriggers(t *testing.T) {
	_, err := BuildActivationPromptEnvelope(ActivationSnapshot{
		ActivationID: "activation-1", RunID: "run-1", Epoch: 1,
		Triggers: []CoalescedTrigger{{Kind: TriggerReviewTimeout, Subject: "run:run-1"}},
	})
	if err == nil {
		t.Fatal("an activation with no scoped action must be refused")
	}
}

// fakeSupervisionStore is the smallest store that can show the revision fence.
type fakeSupervisionStore struct {
	state     SupervisionActivationState
	commits   []SupervisionActivationCommit
	appended  []SupervisionEvent
	commitErr error
}

func (s *fakeSupervisionStore) LoadSupervisionActivationState(_ context.Context, runID string) (SupervisionActivationState, error) {
	if s.state.Record.RunID != runID {
		return SupervisionActivationState{}, ErrSupervisionNotConfigured
	}
	return s.state, nil
}

func (s *fakeSupervisionStore) CommitSupervisionActivation(_ context.Context, commit SupervisionActivationCommit) error {
	if s.commitErr != nil {
		return s.commitErr
	}
	if commit.ExpectedRecordRevision != s.state.Record.Revision {
		return domain.ErrSupervisionStaleRevision
	}
	s.commits = append(s.commits, commit)
	s.state.Record = commit.Record
	s.state.Activation = commit.Activation
	s.state.Outbox = append(s.state.Outbox, commit.Outbox...)
	return nil
}

func (s *fakeSupervisionStore) AppendSupervisionEvents(_ context.Context, _ string, events []SupervisionEvent) (int, error) {
	written := 0
	for _, event := range events {
		known := false
		for _, current := range s.appended {
			if current.ID == event.ID {
				known = true
				break
			}
		}
		if known {
			continue
		}
		s.appended = append(s.appended, event)
		written++
	}
	return written, nil
}

func TestSupervisionActivationServiceFencesOnRecordRevision(t *testing.T) {
	now := supervisionTestTime()
	store := &fakeSupervisionStore{state: SupervisionActivationState{
		Record:  supervisionTestRecord(),
		Pending: []SupervisionEvent{supervisionTestEvent("event-1", 1, TriggerGateReviewReady)},
	}}
	service := SupervisionActivationService{Store: store, Now: func() time.Time { return now }}
	plan, err := service.Advance(context.Background(), "run-1", ActivationSignal{Event: domain.ActivationEventTriggerFired})
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if len(store.commits) != 1 || store.commits[0].ExpectedRecordRevision != 7 {
		t.Fatalf("commits = %+v, want one fenced on the revision that was read", store.commits)
	}
	if plan.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("state = %q, want pending-dispatch", plan.Activation.State)
	}

	// A repeated trigger for an activation that is already pending dispatch is
	// refused by the state machine, so a reconnect cannot raise a second
	// overseer for the same run.
	if _, err := service.Advance(context.Background(), "run-1",
		ActivationSignal{Event: domain.ActivationEventTriggerFired}); !errors.Is(err, domain.ErrSupervisionIllegalTransition) {
		t.Fatalf("error = %v, want the repeated trigger refused", err)
	}

	count, err := service.Observe(context.Background(), "run-1",
		supervisionTestEvent("event-2", 0, TriggerReviewTimeout),
		supervisionTestEvent("event-2", 0, TriggerReviewTimeout))
	if err != nil {
		t.Fatalf("observe: %v", err)
	}
	if count != 1 {
		t.Fatalf("appended = %d, want one row for a repeated observation", count)
	}
}

func TestRequireSupervisionCapability(t *testing.T) {
	if err := RequireSupervisionCapability("worker-1", []string{"git", SupervisionWorkerCapability}); err != nil {
		t.Fatalf("an advertising worker must be accepted: %v", err)
	}
	err := RequireSupervisionCapability("worker-1", []string{"git", "huyang"})
	if err == nil || !strings.Contains(err.Error(), SupervisionWorkerCapability) {
		t.Fatalf("error = %v, want the missing capability named", err)
	}
}

func TestActivationTurnOutcomeIsNotGateAcceptance(t *testing.T) {
	started := supervisionTestTime()
	completed := started.Add(time.Minute)
	archive, err := json.Marshal(map[string]any{
		"thread": map[string]any{
			"id": "thread-1",
			"latestTurn": map[string]any{
				"turnId": "turn-1", "state": "completed",
				"startedAt": started, "completedAt": completed,
			},
			"session": map[string]any{"threadId": "thread-1", "status": "ready"},
		},
	})
	if err != nil {
		t.Fatalf("marshal archive: %v", err)
	}
	outcome, reason, err := ActivationTurnOutcome(archive, "thread-1", "looks good to me, accepting the gate", 0)
	if err != nil {
		t.Fatalf("activation turn outcome: %v", err)
	}
	if reason != "" {
		t.Fatalf("reason = %q, want a clean provider turn", reason)
	}
	if outcome != domain.ActivationOutcomeNoDecision {
		t.Fatalf("outcome = %q, want no-decision: exiting successfully is not acceptance", outcome)
	}
	decided, _, err := ActivationTurnOutcome(archive, "thread-1", "", 1)
	if err != nil {
		t.Fatalf("activation turn outcome: %v", err)
	}
	if decided != domain.ActivationOutcomeDecided {
		t.Fatalf("outcome = %q, want decided only because a decision was recorded", decided)
	}
}
