package main

// What happens to a run whose overseer ends its activation without deciding.
//
// This was the second defect of a live supervised qualification. The activation
// went to spent with outcome no-decision, its inbox was consumed with the
// outcome, and every later boundary therefore found nothing to do: the review
// incident stayed open, nothing escalated, nothing re-armed, and the run waited
// for an operator who had not been told to look. The plan forbids an automatic
// spin here, so the answer is not a replacement activation on the same evidence
// but one escalation plus an explicit operator verb that re-arms the run.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// superviseRun copies the run's supervision record onto the run row, which is
// how a submitted campaign carries it and how the dispatch pass finds it.
func (f *activationLeaseFixture) superviseRun(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	records, err := f.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	state, err := f.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	run := records.WorkflowRuns[0]
	record := state.Record
	run.Supervision = &record
	run.Revision++
	run.UpdatedAt = f.now
	if err := f.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{run},
	}); err != nil {
		t.Fatal(err)
	}
}

func (f *activationLeaseFixture) incident(t *testing.T) domain.ReviewIncident {
	t.Helper()
	state, err := f.store.LoadSupervisionAdminState(context.Background(), activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	for _, view := range state.Incidents {
		if view.Incident.ID == activationLeaseIncident {
			return view.Incident
		}
	}
	t.Fatalf("the fixture holds no incident %s", activationLeaseIncident)
	return domain.ReviewIncident{}
}

func (f *activationLeaseFixture) outbox(t *testing.T) []backlog.SupervisionOutboxEntry {
	t.Helper()
	entries, err := f.supervision.ListSupervisionOutbox(context.Background(), activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

// admittingQuota opens the pool the fixture's overseer route draws from.
// Placement runs before the lifecycle is advanced, so a closed pool would stop
// the dispatch pass before it ever read what the finished activation did.
func admittingQuota() backlog.WorkerAdmissionPolicy {
	return backlog.WorkerAdmissionPolicy{OpenQuotaPools: map[string]struct{}{"claude-main": {}}}
}

// escalations keeps only the escalation intents, so a wake intent from the
// original dispatch is not mistaken for one.
func escalationsOf(entries []backlog.SupervisionOutboxEntry) []backlog.SupervisionOutboxEntry {
	var found []backlog.SupervisionOutboxEntry
	for _, entry := range entries {
		if entry.IncidentID != "" && entry.ActivationID == "" {
			found = append(found, entry)
		}
	}
	return found
}

// endTurnWithoutDeciding takes the fixture's live activation to a finished turn
// that recorded no decision, and runs the dispatch pass over it.
func (f *activationLeaseFixture) endTurnWithoutDeciding(t *testing.T) {
	t.Helper()
	f.superviseRun(t)
	f.now = activationLeaseTime.Add(3 * time.Minute)
	f.finishActivationTurn(t)
	f.coordinator.DispatchActivations(context.Background(), admittingQuota())
}

func TestNoDecisionActivationEscalatesOnceAndDoesNotRedispatch(t *testing.T) {
	ctx := context.Background()
	fixture := newActivationLeaseFixture(t)
	activation := fixture.activate(t)
	fixture.endTurnWithoutDeciding(t)

	state, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != domain.ActivationSpent ||
		state.Activation.Outcome != domain.ActivationOutcomeNoDecision {
		t.Fatalf("activation = %#v, want spent with no decision", state.Activation)
	}
	if state.Activation.Epoch != activation.Epoch {
		t.Fatalf("activation epoch = %d, want the turn's own %d", state.Activation.Epoch, activation.Epoch)
	}
	if incident := fixture.incident(t); incident.State != domain.IncidentEscalated {
		t.Fatalf("incident is %q, want it escalated so an operator is told", incident.State)
	}
	escalations := escalationsOf(fixture.outbox(t))
	if len(escalations) != 1 {
		t.Fatalf("outbox holds %d escalations, want exactly one", len(escalations))
	}
	if escalations[0].ThreadID != activationLeaseThread ||
		escalations[0].IncidentID != activationLeaseIncident {
		t.Fatalf("escalation = %#v, want it addressed to the notify thread", escalations[0])
	}
	if !strings.Contains(escalations[0].Reason, "ended without a decision") {
		t.Fatalf("escalation reason = %q, want it to say the activation decided nothing", escalations[0].Reason)
	}

	// No spin: later boundaries dispatch nothing on the same evidence, and they
	// do not escalate a second time either.
	for _, at := range []time.Duration{4 * time.Minute, 5 * time.Minute} {
		fixture.now = activationLeaseTime.Add(at)
		fixture.coordinator.DispatchActivations(ctx, admittingQuota())
	}
	after, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if after.Activation.State != domain.ActivationSpent || after.Activation.Epoch != activation.Epoch {
		t.Fatalf("activation = %#v, want it left spent at its own epoch", after.Activation)
	}
	if again := escalationsOf(fixture.outbox(t)); len(again) != 1 {
		t.Fatalf("outbox holds %d escalations after two more boundaries, want still one", len(again))
	}
}

// The operator's re-arming path: one operator-reassessment trigger, and the
// next boundaries return the run to idle at the next epoch and dispatch a fresh
// activation there.
func TestOperatorReassessmentDispatchesAFreshActivationAtTheNextEpoch(t *testing.T) {
	ctx := context.Background()
	fixture := newActivationLeaseFixture(t)
	activation := fixture.activate(t)
	fixture.endTurnWithoutDeciding(t)

	fixture.now = activationLeaseTime.Add(10 * time.Minute)
	admin, err := backlogadmin.New(fixture.store, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	admin.SetSupervisionStore(backlogadmin.CoordinatorSupervisionStore{Store: fixture.store})
	projection, err := fixture.store.LoadSupervisionProjection(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Supervise(ctx, backlogadmin.Principal{ID: "operator", Roles: []string{"local-admin"}}, backlogadmin.SupervisionRequest{Version: backlogadmin.SupervisionVersion, Operation: backlogadmin.SupervisionReassess, RunID: activationLeaseRun, RequestKey: "request-reassess", ExpectedRevision: projection.Record.Revision, Reason: "an operator asked for another review"}); err != nil {
		t.Fatal(err)
	}

	// The first boundary returns the spent activation to idle at the next epoch;
	// the second wakes the replacement there, against an identity the spent
	// activation never used.
	fixture.coordinator.DispatchActivations(ctx, admittingQuota())
	fixture.now = activationLeaseTime.Add(11 * time.Minute)
	fixture.coordinator.DispatchActivations(ctx, admittingQuota())

	state, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.Epoch != activation.Epoch+1 {
		t.Fatalf("activation epoch = %d, want the reassessment to raise it to %d",
			state.Activation.Epoch, activation.Epoch+1)
	}
	if state.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("activation state = %q, want a fresh activation dispatched", state.Activation.State)
	}
	if state.Activation.ID == activation.ID {
		t.Fatalf("the replacement reused the spent activation's identity %q", state.Activation.ID)
	}
}

// A run with no activations left is not re-armed by a reassessment: it escalates
// and waits. Raising the budget is a separate, receipted decision.
func TestReassessmentWithNoBudgetEscalatesInsteadOfWaking(t *testing.T) {
	ctx := context.Background()
	fixture := newActivationLeaseFixture(t)
	fixture.activate(t)
	fixture.endTurnWithoutDeciding(t)

	// Declare a budget the run has already spent, the way a run that has had
	// every review it was granted arrives here. Materialization refreshes the
	// declared configuration and never the decision-owned counters, so lowering
	// the declared maximum is how the used count becomes the whole budget.
	state, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	record := state.Record
	if record.ActivationsUsed != 1 {
		t.Fatalf("the fixture used %d activations, want the one it dispatched", record.ActivationsUsed)
	}
	record.Config.MaxActivations = record.ActivationsUsed
	if _, err := fixture.store.PutSupervision(ctx, sqlite.SupervisionMaterialization{Record: record}); err != nil {
		t.Fatal(err)
	}
	fixture.superviseRun(t)
	exhausted, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if exhausted.Record.ActivationBudgetRemaining() {
		t.Fatal("the fixture still has activation budget, so this test proves nothing")
	}

	fixture.now = activationLeaseTime.Add(10 * time.Minute)
	if _, err := fixture.supervision.AppendSupervisionEvents(ctx, activationLeaseRun,
		[]backlog.SupervisionEvent{{
			ID:    "supervision-event:reassess:" + activationLeaseRun + ":request-exhausted",
			RunID: activationLeaseRun, Kind: backlog.TriggerOperatorReassessment,
			Reason: "an operator asked for another review", IncidentID: activationLeaseIncident,
			OccurredAt: fixture.now,
		}}); err != nil {
		t.Fatal(err)
	}
	fixture.coordinator.DispatchActivations(ctx, admittingQuota())

	after, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if after.Activation.State != domain.ActivationEscalated {
		t.Fatalf("activation state = %q, want an exhausted budget to escalate", after.Activation.State)
	}
	if after.Activation.LeaseToken != "" {
		t.Fatalf("an escalated activation still holds a lease: %#v", after.Activation)
	}
}
