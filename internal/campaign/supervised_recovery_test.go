package campaign

// Verification gate 4: what a supervised run does when the review cannot happen.
//
// A gate that cannot be decided is the normal failure of this feature, not an
// exceptional one: the overseer's route can be unavailable, its quota pool can
// be closed, its lease can expire without a decision, an operator can take it
// over, a producer can fail so there is nothing to review, and the run can be
// cancelled underneath all of it. The plan asks for three properties in every
// one of those cases -- explainable, bounded, and quiet -- and each test below
// asserts all three: the blocker names the reason, the run's activation budget
// and identities do not grow without limit, and the outbox gains no entry per
// retry.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// supervisionOutbox is every delivery intent the run has accumulated. The tests
// below count it rather than inspecting it, because the property the plan names
// is the absence of a storm.
func supervisionOutbox(ctx context.Context, t *testing.T, store *sqlite.Store, runID string) []sqlite.SupervisionOutboxRow {
	t.Helper()
	rows, err := store.ListSupervisionOutboxRows(ctx, runID)
	if err != nil {
		t.Fatalf("list supervision outbox of %q: %v", runID, err)
	}
	return rows
}

// blockedRouteBlockers plans the run with the overseer route reported
// unavailable, which is what the coordinator's own boundary reports when no
// worker can host the review.
func blockedRouteBlockers(
	t *testing.T,
	ctx context.Context,
	store *sqlite.Store,
	records sqlite.CoordinatorRecords,
	runID, taskID string,
	now time.Time,
) []backlog.PlanningBlocker {
	t.Helper()
	input := supervisedPlanInput(t, ctx, store, records, now)
	for id, snapshot := range input.SupervisionSnapshots {
		snapshot.RouteAvailable = false
		input.SupervisionSnapshots[id] = snapshot
	}
	plan, err := backlog.BuildPlan(input)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}
	for _, decision := range plan.Decisions {
		if decision.WorkflowRunID == runID && decision.TaskID == taskID {
			return decision.Blockers
		}
	}
	return nil
}

// No worker can host the overseer, or its quota pool is closed. Both are
// temporary conditions of the fleet rather than failures of the run: nothing is
// dispatched, no budget is spent, no notification is sent, and the protected
// task explains itself.
func TestSupervisedReviewIsBoundedWhenTheOverseerRouteCannotRun(t *testing.T) {
	ctx := context.Background()
	run := newOverseerRun(t)
	store := run.fixture.store
	review := overseerReadyGate(ctx, t, run)
	run.observe(t, "event-analysis-review", review.Gate.Definition.ID, "the analysis review is ready")

	// The only worker on the fleet does not advertise the campaign supervision
	// capability. Placement refuses and names it.
	incapableOnly := []domain.WorkerSnapshot{overseerIncapableSnapshot()}
	_, err := backlog.PlaceActivation(run.placementRequest(t, incapableOnly))
	if !errors.Is(err, backlog.ErrActivationUnplaceable) ||
		!strings.Contains(err.Error(), workerproto.CapabilityCampaignSupervision) {
		t.Fatalf("placement error = %v, want an unplaceable activation naming the capability", err)
	}

	// The capable worker exists but the overseer's own quota pool is not
	// admitting new work, which is what supervisor quota exhaustion looks like
	// from here. The refusal names the pool rather than the worker, because the
	// worker is fine and the budget is not.
	starved := run.placementRequest(t, []domain.WorkerSnapshot{baselineSnapshot()})
	starved.Admission = backlog.WorkerAdmissionPolicy{
		OpenQuotaPools: map[string]struct{}{baselineSupervisedPool: {}},
	}
	_, err = backlog.PlaceActivation(starved)
	if !errors.Is(err, backlog.ErrActivationUnplaceable) ||
		!strings.Contains(err.Error(), baselineOverseerPool) {
		t.Fatalf("starved placement error = %v, want the closed overseer pool named", err)
	}

	// Nothing was dispatched, so nothing was spent and nothing was sent. An
	// unplaceable overseer is a condition the fleet will resolve, and this
	// boundary neither consumes the run's bounded activations nor notifies a
	// human on every pass.
	state, err := run.supervision.LoadSupervisionActivationState(ctx, run.fixture.supervised)
	if err != nil {
		t.Fatal(err)
	}
	if state.Record.ActivationsUsed != 0 || state.Activation.State != "" {
		t.Fatalf("an unplaceable activation changed the record: %#v / %#v", state.Record, state.Activation)
	}
	if rows := supervisionOutbox(ctx, t, store, run.fixture.supervised); len(rows) != 0 {
		t.Fatalf("outbox = %d entries, want nothing sent for a placement that never happened", len(rows))
	}

	// The protected task explains itself, and the explanation is the route
	// rather than the gate: a reviewer that cannot be reached is a different
	// answer from a review that has not happened yet.
	records := reload(ctx, t, store)
	synthesis := supervisedTasksByName(records, run.fixture.supervised)["synthesis"]
	run.now = baselineTime.Add(3 * time.Minute)
	blockers := blockedRouteBlockers(t, ctx, store, records, run.fixture.supervised, synthesis.ID, run.now)
	if !hasSupervisionCode(blockers, domain.SupervisionBlockerRouteUnavailable) {
		t.Fatalf("synthesis blockers = %#v, want the unavailable overseer route", blockers)
	}

	// The event is still pending for whoever can eventually run the review: an
	// unplaceable activation loses no work.
	if len(state.Pending) != 1 || state.Pending[0].ID != "event-analysis-review" {
		t.Fatalf("pending events = %#v, want the unreviewed trigger preserved", state.Pending)
	}
}

// A review that ends without a decision, and a review an operator takes over,
// both recover the same way: the activation loses its authority, the run
// returns to idle at a fresh epoch, and the replacement is a new activation
// rather than the same one again.
func TestSupervisedNoDecisionTimeoutAndTakeoverRecoverAtAFreshEpoch(t *testing.T) {
	ctx := context.Background()
	run := newOverseerRun(t)
	store := run.fixture.store
	workers := []domain.WorkerSnapshot{overseerIncapableSnapshot(), baselineSnapshot()}
	review := overseerReadyGate(ctx, t, run)

	run.observe(t, "event-analysis-review", review.Gate.Definition.ID, "the analysis review is ready")
	first := run.advance(t, domain.ActivationEventTriggerFired)
	firstAttempt, firstAssignment := run.dispatch(t, first, workers)
	run.now = baselineTime.Add(3 * time.Minute)
	baselineClaim(ctx, t, store, firstAssignment, run.now)
	run.advance(t, domain.ActivationEventDispatchConfirmed, func(signal *backlog.ActivationSignal) {
		signal.ExecutionObserved = true
	})

	// No decision is recorded and the lease expires. Authority is gone at that
	// moment, and the unacknowledged inbox is preserved for the replacement.
	run.now = baselineTime.Add(10 * time.Minute)
	revoked := run.advance(t, domain.ActivationEventLeaseExpired)
	if revoked.Activation.State != domain.ActivationRevoked ||
		revoked.Activation.Outcome != domain.ActivationOutcomeRevoked {
		t.Fatalf("expired activation = %#v, want revoked", revoked.Activation)
	}
	if revoked.CursorAdvanced {
		t.Fatal("a revoked activation consumed the inbox it never reviewed")
	}
	late := sqlite.GateDecisionRequest{
		RunID: run.fixture.supervised, GateID: review.Gate.Definition.ID, RequestID: "decide-after-expiry",
		Actor: domain.Actor{
			Kind: domain.ActorOverseer, Principal: overseerPrincipal, ActivationEpoch: first.Activation.Epoch,
		},
		ExpectedGraphRevision: review.Gate.GraphRevision, ExpectedGateRevision: review.Gate.Revision,
		Evidence: *review.Evidence, Outcome: domain.GateDecisionAccept,
		Reason: "a decision formed after the lease expired", DecidedAt: run.now,
	}
	if _, err := store.DecideGate(ctx, late); err == nil {
		t.Fatal("an expired lease still accepted a gate")
	}

	// Recovery: the old runtime is reconciled and proven stopped, and the run
	// returns to idle at a fresh epoch so the replacement cannot reuse the
	// revoked activation's identity.
	run.finishActivationTurn(t, firstAttempt.ID)
	recovered := run.advance(t, domain.ActivationEventReconciliationAcknowledged,
		func(signal *backlog.ActivationSignal) { signal.RecoveryComplete = true })
	if recovered.Activation.State != domain.ActivationIdle ||
		recovered.Activation.Epoch != first.Activation.Epoch+1 {
		t.Fatalf("recovered activation = %#v, want idle at a fresh epoch", recovered.Activation)
	}

	second := run.advance(t, domain.ActivationEventTriggerFired)
	if second.Dispatch == nil || second.Dispatch.Identity == first.Dispatch.Identity {
		t.Fatalf("replacement dispatch = %#v, want a new identity", second.Dispatch)
	}
	secondAttempt, secondAssignment := run.dispatch(t, second, workers)
	if secondAttempt.ID == firstAttempt.ID || secondAssignment.ThreadID == firstAssignment.ThreadID {
		t.Fatalf("the replacement reused attempt %q / thread %q", secondAttempt.ID, secondAssignment.ThreadID)
	}
	run.now = baselineTime.Add(11 * time.Minute)
	baselineClaim(ctx, t, store, secondAssignment, run.now)
	run.advance(t, domain.ActivationEventDispatchConfirmed, func(signal *backlog.ActivationSignal) {
		signal.ExecutionObserved = true
	})

	// An operator takes the second review over. The takeover revokes and raises
	// the epoch in one move, so the decision the replaced overseer was about to
	// make lands on an epoch the run is no longer at.
	takeover := run.advance(t, domain.ActivationEventOperatorTakeover, func(signal *backlog.ActivationSignal) {
		signal.Actor = domain.Actor{Kind: domain.ActorOperator, Principal: "operator-a"}
	})
	if takeover.Activation.State != domain.ActivationRevoked ||
		takeover.Activation.Epoch != second.Activation.Epoch+1 {
		t.Fatalf("takeover = %#v, want revoked at a raised epoch", takeover.Activation)
	}
	lateAfterTakeover := late
	lateAfterTakeover.RequestID = "decide-after-takeover"
	lateAfterTakeover.Actor = domain.Actor{
		Kind: domain.ActorOverseer, Principal: overseerPrincipal, ActivationEpoch: second.Activation.Epoch,
	}
	if _, err := store.DecideGate(ctx, lateAfterTakeover); err == nil {
		t.Fatal("a replaced overseer decided the gate after the operator took over")
	}

	// The operator that took over decides for itself, and the gate moves.
	run.now = baselineTime.Add(12 * time.Minute)
	if _, err := store.DecideGate(ctx, sqlite.GateDecisionRequest{
		RunID: run.fixture.supervised, GateID: review.Gate.Definition.ID, RequestID: "operator-accept",
		Actor:                 domain.Actor{Kind: domain.ActorOperator, Principal: "operator-a"},
		ExpectedGraphRevision: review.Gate.GraphRevision, ExpectedGateRevision: review.Gate.Revision,
		Evidence: *review.Evidence, Outcome: domain.GateDecisionAccept,
		Reason: "the operator reviewed both analyses directly", DecidedAt: run.now,
	}); err != nil {
		t.Fatalf("the operator's own decision was refused: %v", err)
	}

	// Bounded and quiet: two reviews were dispatched, two activations were
	// spent, and the outbox holds one wake intent per dispatch identity rather
	// than one per boundary pass.
	state, err := run.supervision.LoadSupervisionActivationState(ctx, run.fixture.supervised)
	if err != nil {
		t.Fatal(err)
	}
	if state.Record.ActivationsUsed != 2 {
		t.Fatalf("activations used = %d, want one per dispatched review", state.Record.ActivationsUsed)
	}
	if rows := supervisionOutbox(ctx, t, store, run.fixture.supervised); len(rows) > 2 {
		t.Fatalf("outbox = %d entries, want at most one intent per dispatch identity", len(rows))
	}
}

// A producer that fails leaves nothing to review, and a run that is cancelled
// revokes supervision outright. Both are explainable at the protected task, and
// neither produces a notification per pass.
func TestSupervisedProducerFailureAndCancellationStayExplainableAndQuiet(t *testing.T) {
	ctx := context.Background()
	run := newOverseerRun(t)
	store := run.fixture.store
	coordinator := backlog.FleetCoordinator{Store: store, Now: func() time.Time { return run.now }}
	supervisedCommit(ctx, t, coordinator, store, reload(ctx, t, store), run.now)

	// One producer stops for input. A gate observes what a producer produced,
	// not what it is doing, so a producer waiting on a human is simply not
	// evidence yet and the gate stays closed without anything being decided.
	run.now = baselineTime.Add(2 * time.Minute)
	setProducerProgress(ctx, t, run, "tests", domain.ProgressNeedsInput)
	if advanced, err := store.AdvanceSupervisionGates(ctx, run.fixture.supervised, run.now); err != nil ||
		len(advanced) != 0 {
		t.Fatalf("gate advance while a producer needs input = %#v (err %v), want nothing", advanced, err)
	}

	// The other producer fails terminally. The gate observes it too, so it
	// never becomes reviewable: there is no evidence to review and no decision
	// that could release the protected task.
	failProducerAttempt(ctx, t, run, "interfaces")
	advanced, err := store.AdvanceSupervisionGates(ctx, run.fixture.supervised, run.now)
	if err != nil {
		t.Fatalf("advance gates after a producer failure: %v", err)
	}
	if len(advanced) != 0 {
		t.Fatalf("advanced gates = %#v, want none: a failed producer is not evidence", advanced)
	}
	// Repeating the pass is idempotent and silent, which is what "bounded"
	// means for a condition that persists across every boundary.
	if advanced, err = store.AdvanceSupervisionGates(ctx, run.fixture.supervised, run.now); err != nil ||
		len(advanced) != 0 {
		t.Fatalf("repeated gate advance = %#v (err %v), want nothing", advanced, err)
	}
	records := reload(ctx, t, store)
	synthesis := supervisedTasksByName(records, run.fixture.supervised)["synthesis"]
	blockers := supervisedBlockers(t, ctx, store, records, run.fixture.supervised, synthesis.ID, run.now)
	if len(blockers) == 0 {
		t.Fatal("synthesis is unexplained after its producer failed")
	}

	// The failure is a decision-requiring condition, so an incident is raised
	// for it exactly once: the same incident ID re-raised is the same incident.
	failedAttempt := latestAttemptOfTask(ctx, t, run, "interfaces")
	incidentRequest := sqlite.IncidentRequest{
		RunID: run.fixture.supervised, IncidentID: "incident-producer-failure",
		RequestID:     "request-incident-1",
		Actor:         domain.Actor{Kind: domain.ActorOperator, Principal: "coordinator"},
		SourceEventID: "event-producer-failed", SourceTaskID: failedAttempt.TaskID,
		SourceAttemptID: failedAttempt.ID, RequiredDisposition: domain.DispositionConcludeFailure,
		Reason: "the interfaces analysis failed terminally", OpenedAt: run.now,
	}
	if _, err := store.OpenReviewIncident(ctx, incidentRequest); err != nil {
		t.Fatalf("open the producer-failure incident: %v", err)
	}
	replayed, err := store.OpenReviewIncident(ctx, incidentRequest)
	if err != nil {
		t.Fatalf("re-raise the same incident: %v", err)
	}
	if !replayed.Replay {
		t.Fatalf("re-raising one incident = %#v, want the first answer replayed", replayed)
	}
	projection, err := store.LoadSupervisionProjection(ctx, run.fixture.supervised)
	if err != nil {
		t.Fatal(err)
	}
	if len(projection.Incidents) != 1 {
		t.Fatalf("incidents = %d, want one per distinct condition", len(projection.Incidents))
	}

	// The unresolved incident withholds settlement, which is what stops a
	// reviewable failure from settling before its disposition is recorded.
	snapshot := supervisionSnapshot(ctx, t, store, run.fixture.supervised)
	if verdict := domain.SupervisionSettlementBarrier(domain.SupervisionBarrier{
		Supervised: snapshot.Supervised, Gates: snapshot.Gates, Incidents: projection.Incidents,
	}, false); verdict.Settles || verdict.Reason != domain.SinkBarrierUnresolvedIncident {
		t.Fatalf("settlement verdict = %#v, want the open incident withholding", verdict)
	}

	// The run is cancelled. Supervision authority goes with it: a late approval
	// cannot reopen a terminal run, and the protected task says why.
	run.now = baselineTime.Add(5 * time.Minute)
	cancelled := supervisedRun(t, reload(ctx, t, store), run.fixture.supervised)
	cancelled.Progress = domain.ProgressCancelled
	cancelled.Revision++
	cancelled.UpdatedAt = run.now
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{cancelled},
	}); err != nil {
		t.Fatal(err)
	}
	finalGate := supervisionGateByName(t,
		supervisionSnapshot(ctx, t, store, run.fixture.supervised), "analysis_review")
	if _, err := store.DecideGate(ctx, sqlite.GateDecisionRequest{
		RunID: run.fixture.supervised, GateID: finalGate.Definition.ID, RequestID: "accept-after-cancel",
		Actor:                 domain.Actor{Kind: domain.ActorOperator, Principal: "operator-a"},
		ExpectedGraphRevision: finalGate.GraphRevision, ExpectedGateRevision: finalGate.Revision,
		Evidence: domain.EvidenceSnapshot{ID: "evidence-late", GraphRevision: finalGate.GraphRevision},
		Outcome:  domain.GateDecisionAccept, Reason: "a late approval after cancellation",
		DecidedAt: run.now,
	}); !errors.Is(err, domain.ErrSupervisionTerminal) {
		t.Fatalf("late approval after cancellation = %v, want ErrSupervisionTerminal", err)
	}

	// Quiet throughout: no delivery intent was written for a condition nobody
	// could be told about, and none accumulated per pass.
	if rows := supervisionOutbox(ctx, t, store, run.fixture.supervised); len(rows) != 0 {
		t.Fatalf("outbox = %d entries, want no notification storm", len(rows))
	}
}

// failProducerAttempt records the terminal failure of one producer task.
func failProducerAttempt(ctx context.Context, t *testing.T, run *overseerRun, taskName string) {
	t.Helper()
	setProducerProgress(ctx, t, run, taskName, domain.ProgressFailed)
}

// setProducerProgress moves one producer task's latest attempt to the given
// progress, which is how this file stages a producer that failed and a producer
// that is waiting for a human.
func setProducerProgress(
	ctx context.Context,
	t *testing.T,
	run *overseerRun,
	taskName string,
	progress domain.ProgressState,
) {
	t.Helper()
	attempt := latestAttemptOfTask(ctx, t, run, taskName)
	attempt.Progress = progress
	if progress.Terminal() {
		attempt.Control = domain.ControlStopped
	}
	attempt.Revision++
	attempt.UpdatedAt = run.now
	if err := run.fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{attempt},
	}); err != nil {
		t.Fatalf("record attempt progress %q of %q: %v", progress, taskName, err)
	}
}

func latestAttemptOfTask(ctx context.Context, t *testing.T, run *overseerRun, taskName string) domain.Attempt {
	t.Helper()
	records := reload(ctx, t, run.fixture.store)
	task := supervisedTasksByName(records, run.fixture.supervised)[taskName]
	if task.ID == "" {
		t.Fatalf("the supervised run declares no task %q", taskName)
	}
	var latest domain.Attempt
	for _, attempt := range attemptsOfRun(records.Attempts, run.fixture.supervised) {
		if attempt.TaskID == task.ID && attempt.Number >= latest.Number {
			latest = attempt
		}
	}
	if latest.ID == "" {
		t.Fatalf("task %q has no attempt", taskName)
	}
	return latest
}
