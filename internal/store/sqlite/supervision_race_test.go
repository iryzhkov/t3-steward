package sqlite

// The adversarial half of verification gate 5: every place where a supervision
// decision and an ordinary scheduling action can reach the store at the same
// moment, driven against one shared store with real goroutines.
//
// Each test asserts the permitted start boundary from the decision receipt --
// ReleasedAssignmentIDs, ClaimedTaskIDs and AttemptRevision -- rather than only
// the state the store ends in. The end state of a race is the same whichever
// side won; what distinguishes a correct interleaving from a lucky one is which
// work the decision reports as already started, and that is only in the receipt.

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// supervisionReadyGate is the gate every decision test below starts from: its
// producer has succeeded, so it is reviewable against bound evidence.
func supervisionReadyGate() domain.Gate {
	return domain.Gate{
		RunID: "run-1", State: domain.GateReadyForReview, GraphRevision: 1,
		Definition: domain.GateDefinition{
			ID: "run-1:review", Name: "review",
			ObservedTaskIDs: []string{"task-0"}, ProtectedTaskIDs: []string{"task-1"},
		},
	}
}

// seedSucceededProducer records the producer attempt a gate's evidence names.
func seedSucceededProducer(t *testing.T, store *Store, revision int64) domain.Attempt {
	t.Helper()
	producer := domain.Attempt{
		ID: "attempt-0", WorkflowRunID: "run-1", TaskID: "task-0", Number: 1,
		Progress: domain.ProgressSucceeded, Control: domain.ControlStopped,
		Revision: revision, UpdatedAt: supervisionTestTime,
	}
	if err := store.SaveCoordinatorRecords(context.Background(),
		CoordinatorRecords{Attempts: []domain.Attempt{producer}}); err != nil {
		t.Fatal(err)
	}
	return producer
}

func supervisionEvidence(revision int64) domain.EvidenceSnapshot {
	return domain.EvidenceSnapshot{
		ID: "evidence-1", GraphRevision: 1, TakenAt: supervisionTestTime,
		Producers: []domain.ProducerEvidence{
			{TaskID: "task-0", AttemptID: "attempt-0", ResultRevision: revision},
		},
	}
}

func supervisionAcceptRequest(requestID string, gateRevision, producerRevision int64) GateDecisionRequest {
	return GateDecisionRequest{
		RunID: "run-1", GateID: "run-1:review", RequestID: requestID,
		Actor: supervisionOperator(), ExpectedGraphRevision: 1, ExpectedGateRevision: gateRevision,
		Evidence: supervisionEvidence(producerRevision), Outcome: domain.GateDecisionAccept,
		Reason: "the producer meets the rubric", DecidedAt: supervisionTestTime,
	}
}

// A settlement projection and a hold that lands while it is being computed
// cannot both win: the projection's read set includes supervision, so the hold
// invalidates it and the settlement is recomputed rather than published over a
// decision it never saw.
func TestSupervisionHoldRacesRunCompletionProjection(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	ctx := context.Background()
	now := supervisionTestTime
	run := domain.WorkflowRun{
		ID: "run-1", WorkflowID: "workflow-1", GraphRevision: 1,
		Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	tasks := []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "only", Class: domain.TaskClassRequired}}
	bound, err := domain.BindRunSink(run, tasks)
	if err != nil {
		t.Fatal(err)
	}
	attempts := []domain.Attempt{{
		ID: "attempt-1", WorkflowRunID: run.ID, TaskID: "task-1", Number: 1, Revision: 1,
		Progress: domain.ProgressSucceeded, Control: domain.ControlStopped, UpdatedAt: now,
	}}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{bound}, Tasks: tasks, Attempts: attempts,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSupervision(ctx, SupervisionMaterialization{
		Record: domain.SupervisionRecord{RunID: run.ID, Config: supervisionTestConfig()},
	}); err != nil {
		t.Fatal(err)
	}

	before := loadSupervisionProjectionFixture(t, store, run.ID)
	projected, err := domain.ProjectRunSink(before.Run, before.Tasks, before.Attempts, before.Assignments, now)
	if err != nil {
		t.Fatal(err)
	}
	projected.Revision++
	projected.UpdatedAt = now

	var wg sync.WaitGroup
	var settleErr, holdErr error
	var hold SupervisionDecision
	wg.Add(2)
	go func() {
		defer wg.Done()
		settleErr = store.CommitWorkflowProjection(ctx, before, projected, before.Attempts, now)
	}()
	go func() {
		defer wg.Done()
		hold, holdErr = store.PlaceHold(ctx, supervisionRunHold("hold-1", "request-hold-1", "pause before settlement"))
	}()
	wg.Wait()
	if holdErr != nil {
		t.Fatalf("hold: %v", holdErr)
	}
	if hold.Hold == nil || hold.Hold.State != domain.HoldActive {
		t.Fatalf("hold decision = %#v", hold)
	}
	// The completing run held no live assignment, so the boundary the hold
	// reports is empty on both sides and no attempt was rewritten.
	if len(hold.ReleasedAssignmentIDs) != 0 || len(hold.ClaimedTaskIDs) != 0 || hold.AttemptRevision != 0 {
		t.Fatalf("hold receipt = %#v, want an empty start boundary", hold)
	}
	if settleErr != nil && !errors.Is(settleErr, ErrStaleWorkflowProjection) {
		t.Fatalf("settlement error = %v, want either success or a stale read set", settleErr)
	}
	if settleErr == nil {
		// The settlement won the writer. The hold that followed it is recorded
		// and changed nothing about the terminal run.
		return
	}
	after := loadSupervisionProjectionFixture(t, store, run.ID)
	if after.Supervision == nil || len(after.Supervision.Holds) != 1 {
		t.Fatalf("the invalidated read set does not carry the hold: %#v", after.Supervision)
	}
}

// A branch hold and the claim of the offer it narrows out have exactly one
// winner, and whichever it is, the receipt says which.
//
// The offer exists because the gate protecting the task is accepted, so this is
// the interleaving that matters most: work supervision had already permitted is
// withheld again while a worker is claiming it. Either the claim is past the
// start boundary and the hold reports it, or the hold released the offer and
// the worker is told no. There is no third answer and no rollback of a started
// attempt.
func TestSupervisionBranchHoldRacesClaimOfAGateProtectedTask(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	ctx := context.Background()
	gate := supervisionReadyGate()
	gate.State = domain.GateAccepted
	seedSupervisedRun(t, store, []domain.Gate{gate})
	seedSucceededProducer(t, store, 7)
	offered, err := store.CommitAssignmentPlan(ctx, supervisionPlanCommit())
	if err != nil || len(offered) != 1 {
		t.Fatalf("an accepted gate must admit the offer: %#v %v", offered, err)
	}

	hold := HoldRequest{
		RunID: "run-1", HoldID: "hold-branch", RequestID: "request-branch-hold",
		Actor:                 supervisionOperator(),
		Scope:                 domain.HoldScope{Kind: domain.HoldScopeBranch, BranchRootTaskID: "task-0"},
		ExpectedGraphRevision: 1, Reason: "the producer output needs a second look",
		PlacedAt: supervisionTestTime,
	}
	var wg sync.WaitGroup
	var claimErr, holdErr error
	var claimed domain.Assignment
	var decision SupervisionDecision
	wg.Add(2)
	go func() {
		defer wg.Done()
		claimed, claimErr = store.ClaimAssignment(ctx, supervisionClaim())
	}()
	go func() {
		defer wg.Done()
		decision, holdErr = store.PlaceHold(ctx, hold)
	}()
	wg.Wait()
	if holdErr != nil {
		t.Fatalf("branch hold: %v", holdErr)
	}
	if decision.Hold == nil || decision.Hold.State != domain.HoldActive {
		t.Fatalf("hold decision = %#v", decision)
	}
	switch {
	case claimErr == nil:
		// The claim was first. It is past the start boundary, so the hold
		// reports it and never rolls it back.
		if claimed.State != domain.AssignmentClaimed {
			t.Fatalf("claimed assignment = %#v", claimed)
		}
		if len(decision.ReleasedAssignmentIDs) != 0 {
			t.Fatalf("the hold released a claimed assignment: %#v", decision)
		}
		if len(decision.ClaimedTaskIDs) != 1 || decision.ClaimedTaskIDs[0] != "task-1" {
			t.Fatalf("the hold did not report the started work: %#v", decision)
		}
	case errors.Is(claimErr, ErrAssignmentClaim):
		// The hold was first. The offer is released, the attempt is back to
		// ready at a higher revision, and the worker was told no.
		if len(decision.ReleasedAssignmentIDs) != 1 || decision.ReleasedAssignmentIDs[0] != "assignment-1" {
			t.Fatalf("released = %#v, want the outstanding offer", decision.ReleasedAssignmentIDs)
		}
		if len(decision.ClaimedTaskIDs) != 0 {
			t.Fatalf("the hold reported started work that never started: %#v", decision)
		}
		if decision.AttemptRevision < 2 {
			t.Fatalf("attempt revision = %d, want the released attempt's new revision", decision.AttemptRevision)
		}
	default:
		t.Fatalf("unexpected claim error: %v", claimErr)
	}
}

// An acceptance and a retry that replaces the evidence it names cannot both
// win: the evidence identity is re-read inside the decision transaction, so an
// acceptance whose producer moved first is refused rather than bound to an
// attempt that no longer exists.
func TestSupervisionGateAcceptanceRacesEvidenceInvalidatingRetry(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	ctx := context.Background()
	seedSupervisedRun(t, store, []domain.Gate{supervisionReadyGate()})
	producer := seedSucceededProducer(t, store, 7)

	retried := producer
	retried.Revision = 8
	retried.UpdatedAt = supervisionTestTime.Add(time.Minute)

	var wg sync.WaitGroup
	var acceptErr, retryErr error
	var accepted SupervisionDecision
	wg.Add(2)
	go func() {
		defer wg.Done()
		accepted, acceptErr = store.DecideGate(ctx, supervisionAcceptRequest("request-accept", 1, 7))
	}()
	go func() {
		defer wg.Done()
		retryErr = store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{retried}})
	}()
	wg.Wait()
	if retryErr != nil {
		t.Fatalf("record the replaced producer: %v", retryErr)
	}
	switch {
	case acceptErr == nil:
		// The acceptance was first and is bound to the producer identity it
		// named, not to the one that replaced it.
		if accepted.Gate.State != domain.GateAccepted {
			t.Fatalf("accepted gate = %q", accepted.Gate.State)
		}
		if accepted.GateDecision.Evidence.Producers[0].ResultRevision != 7 {
			t.Fatalf("acceptance bound evidence = %#v, want the revision it named",
				accepted.GateDecision.Evidence.Producers)
		}
	case errors.Is(acceptErr, domain.ErrSupervisionStaleRevision):
		// The retry was first. The acceptance names evidence the run no longer
		// has, and the gate is untouched.
		snapshot, err := store.LoadSupervisionSnapshot(ctx, "run-1")
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.Gates[0].State != domain.GateReadyForReview {
			t.Fatalf("a refused acceptance moved the gate to %q", snapshot.Gates[0].State)
		}
		projection, err := store.LoadSupervisionProjection(ctx, "run-1")
		if err != nil {
			t.Fatal(err)
		}
		if len(projection.Decisions) != 0 {
			t.Fatalf("a refused acceptance wrote a decision: %#v", projection.Decisions)
		}
	default:
		t.Fatalf("unexpected acceptance error: %v", acceptErr)
	}
}

// A decision that names a graph revision, a gate revision or a producer attempt
// the run has moved past is refused, and writes nothing.
func TestSupervisionDecisionNamingStaleGraphOrAttemptIsRefused(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	ctx := context.Background()
	seedSupervisedRun(t, store, []domain.Gate{supervisionReadyGate()})
	seedSucceededProducer(t, store, 7)

	cases := []struct {
		name    string
		mutate  func(*GateDecisionRequest)
		wantErr error
	}{
		{
			name: "a graph revision the run has moved past",
			mutate: func(request *GateDecisionRequest) {
				request.ExpectedGraphRevision = 99
			},
			wantErr: domain.ErrSupervisionStaleRevision,
		},
		{
			name: "a gate revision another decision already bumped",
			mutate: func(request *GateDecisionRequest) {
				request.ExpectedGateRevision = 99
			},
			wantErr: domain.ErrSupervisionStaleRevision,
		},
		{
			name: "a producer attempt that is not the one that ran",
			mutate: func(request *GateDecisionRequest) {
				request.Evidence.Producers[0].AttemptID = "attempt-of-another-run"
			},
			wantErr: domain.ErrSupervisionStaleRevision,
		},
		{
			name: "a producer result revision the attempt has moved past",
			mutate: func(request *GateDecisionRequest) {
				request.Evidence.Producers[0].ResultRevision = 6
			},
			wantErr: domain.ErrSupervisionStaleRevision,
		},
	}
	for index, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			request := supervisionAcceptRequest("request-stale-"+string(rune('a'+index)), 1, 7)
			testCase.mutate(&request)
			if _, err := store.DecideGate(ctx, request); !errors.Is(err, testCase.wantErr) {
				t.Fatalf("error = %v, want %v", err, testCase.wantErr)
			}
			snapshot, err := store.LoadSupervisionSnapshot(ctx, "run-1")
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.Gates[0].State != domain.GateReadyForReview || snapshot.Gates[0].Revision != 1 {
				t.Fatalf("a refused decision moved the gate: %#v", snapshot.Gates[0])
			}
		})
	}
}

// Two deliveries of the same dispatch response, arriving at once, transition
// the activation once. A response that is lost instead retries with the
// original identity and spends no budget.
func TestSupervisionDuplicateAndLostActivationDispatchResponses(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	ctx := context.Background()
	seedSupervisedRun(t, store, nil)
	seedSupervisionInboxEvent(t, store, "event-1", 1)
	overseer := domain.Actor{Kind: domain.ActorOverseer, Principal: "overseer:run-1", ActivationEpoch: 1}

	planned, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-trigger",
		Actor: overseer, Event: domain.ActivationEventTriggerFired, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: 1, DispatchIdentity: "supervision:run-1:1",
		TransitionedAt: supervisionTestTime,
	})
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}

	// Lost: the dispatch left no durable assignment, so it is retried with the
	// identity it already had and the run's budget is untouched.
	undelivered, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-undelivered",
		Actor: overseer, Event: domain.ActivationEventDispatchUndelivered, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: planned.SupervisionRevision,
		TransitionedAt:              supervisionTestTime,
	})
	if err != nil {
		t.Fatalf("undelivered dispatch: %v", err)
	}
	if undelivered.Activation.State != domain.ActivationPendingDispatch ||
		undelivered.Activation.DispatchIdentity != planned.Activation.DispatchIdentity ||
		undelivered.Activation.Epoch != planned.Activation.Epoch {
		t.Fatalf("retried activation = %#v, want the original identity at the original epoch",
			undelivered.Activation)
	}
	if undelivered.Record.ActivationsUsed != 0 {
		t.Fatalf("a retry of an undelivered dispatch spent budget: %#v", undelivered.Record)
	}

	// A dispatch that was observed executing is not undeliverable, whatever the
	// caller believes.
	if _, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-undelivered-2",
		Actor: overseer, Event: domain.ActivationEventDispatchUndelivered, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: undelivered.SupervisionRevision,
		Observations:                ActivationObservations{ExecutionObserved: true},
		TransitionedAt:              supervisionTestTime,
	}); !errors.Is(err, domain.ErrSupervisionPrerequisite) {
		t.Fatalf("undelivered-after-execution error = %v, want a refusal", err)
	}

	// Duplicated: the same confirmation arrives twice at once. One transition
	// happens, the other replays the first answer, and the budget moves by one.
	expiry := supervisionTestTime.Add(time.Hour)
	confirm := ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-confirm",
		Actor: overseer, Event: domain.ActivationEventDispatchConfirmed, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: undelivered.SupervisionRevision,
		Observations:                ActivationObservations{LeaseValid: true},
		LeaseToken:                  "lease-1", LeaseExpiresAt: &expiry,
		TransitionedAt: supervisionTestTime,
	}
	var wg sync.WaitGroup
	answers := make([]SupervisionDecision, 2)
	errs := make([]error, 2)
	wg.Add(2)
	for index := range answers {
		go func(index int) {
			defer wg.Done()
			answers[index], errs[index] = store.RecordActivationTransition(ctx, confirm)
		}(index)
	}
	wg.Wait()
	var replays int
	for index, err := range errs {
		if err != nil {
			t.Fatalf("duplicate confirmation %d: %v", index, err)
		}
		if answers[index].Replay {
			replays++
		}
		if answers[index].Activation.State != domain.ActivationActive {
			t.Fatalf("duplicate confirmation %d = %#v, want an active activation", index, answers[index].Activation)
		}
	}
	if replays != 1 {
		t.Fatalf("replays = %d, want exactly one of the two duplicates replayed", replays)
	}
	state, err := store.LoadSupervisionActivationRows(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.Record.ActivationsUsed != 1 {
		t.Fatalf("activations used = %d, want one dispatch confirmed once", state.Record.ActivationsUsed)
	}

	// The same key with a different payload is a different request and is
	// refused rather than replayed.
	changed := confirm
	changed.LeaseToken = "lease-2"
	if _, err := store.RecordActivationTransition(ctx, changed); !errors.Is(err, ErrSupervisionRequestConflict) {
		t.Fatalf("changed-payload error = %v, want a request conflict", err)
	}
}

// Events that arrive while a review is running, and events that arrive as a
// spent activation returns to idle, both stay pending: an activation consumes
// only the high-water mark it was bound to, and the return to idle takes a
// fresh epoch so the next wake is a new activation rather than the same one.
func TestSupervisionEventsArrivingDuringReviewAndDuringTheIdleTransition(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	ctx := context.Background()
	seedSupervisedRun(t, store, nil)
	seedSupervisionInboxEvent(t, store, "event-1", 1)
	overseer := domain.Actor{Kind: domain.ActorOverseer, Principal: "overseer:run-1", ActivationEpoch: 1}

	planned, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-trigger",
		Actor: overseer, Event: domain.ActivationEventTriggerFired, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: 1, DispatchIdentity: "supervision:run-1:1",
		TransitionedAt: supervisionTestTime,
	})
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}
	expiry := supervisionTestTime.Add(time.Hour)
	confirmed, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-confirm",
		Actor: overseer, Event: domain.ActivationEventDispatchConfirmed, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: planned.SupervisionRevision,
		Observations:                ActivationObservations{LeaseValid: true},
		LeaseToken:                  "lease-1", LeaseExpiresAt: &expiry,
		TransitionedAt: supervisionTestTime,
	})
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}

	// An event arriving mid-review is not consumed by the activation reviewing:
	// the high-water mark is recorded with the outcome or not at all, so a live
	// review leaves both its own event and the new one pending.
	seedSupervisionInboxEvent(t, store, "event-2", 2)
	if during := pendingSupervisionEventIDs(t, store, "run-1"); len(during) != 2 {
		t.Fatalf("pending inbox during review = %v, want nothing consumed before an outcome", during)
	}

	spent, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-limit",
		Actor: overseer, Event: domain.ActivationEventLimitReached, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: confirmed.SupervisionRevision,
		Outcome:                     domain.ActivationOutcomeDecided,
		ConsumedEventCursor:         1,
		TransitionedAt:              supervisionTestTime,
	})
	if err != nil {
		t.Fatalf("limit reached: %v", err)
	}
	if spent.Activation.State != domain.ActivationSpent || spent.Activation.Epoch != 1 {
		t.Fatalf("spent activation = %#v", spent.Activation)
	}

	// The events that arrived during the review return the run to idle at a
	// fresh epoch, and are still pending for whoever wakes next.
	idle, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-events-arrived",
		Actor: overseer, Event: domain.ActivationEventEventsArrived, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: spent.SupervisionRevision,
		TransitionedAt:              supervisionTestTime,
	})
	if err != nil {
		t.Fatalf("events arrived: %v", err)
	}
	if idle.Activation.State != domain.ActivationIdle || idle.Activation.Epoch != 2 {
		t.Fatalf("idle activation = %#v, want idle at epoch 2", idle.Activation)
	}
	if idle.Record.ActivationEpoch != 2 {
		t.Fatalf("record epoch = %d, want the fence raised with the activation", idle.Record.ActivationEpoch)
	}
	pending := pendingSupervisionEventIDs(t, store, "run-1")
	if len(pending) != 1 || pending[0] != "event-2" {
		t.Fatalf("pending inbox after the idle transition = %v, want the unconsumed event preserved", pending)
	}
}

// pendingSupervisionEventIDs is the run's unconsumed inbox, in sequence order.
func pendingSupervisionEventIDs(t *testing.T, store *Store, runID string) []string {
	t.Helper()
	rows, err := store.LoadSupervisionActivationRows(context.Background(), runID)
	if err != nil {
		t.Fatalf("load activation rows of %q: %v", runID, err)
	}
	var pending []string
	for _, row := range rows.Inbox {
		if !row.Consumed {
			pending = append(pending, row.ID)
		}
	}
	return pending
}

// A decision formed before an operator took over, and a decision formed before
// the run was cancelled, both arrive too late.
func TestSupervisionLateApprovalAfterTakeoverAndAfterCancellation(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	ctx := context.Background()
	seedSupervisedRun(t, store, []domain.Gate{supervisionReadyGate()})
	seedSucceededProducer(t, store, 7)
	seedSupervisionInboxEvent(t, store, "event-1", 1)
	overseer := domain.Actor{Kind: domain.ActorOverseer, Principal: "overseer:run-1", ActivationEpoch: 1}

	planned, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-trigger",
		Actor: overseer, Event: domain.ActivationEventTriggerFired, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: 1, DispatchIdentity: "supervision:run-1:1",
		TransitionedAt: supervisionTestTime,
	})
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}
	expiry := supervisionTestTime.Add(time.Hour)
	confirmed, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-confirm",
		Actor: overseer, Event: domain.ActivationEventDispatchConfirmed, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: planned.SupervisionRevision,
		Observations:                ActivationObservations{LeaseValid: true},
		LeaseToken:                  "lease-1", LeaseExpiresAt: &expiry,
		TransitionedAt: supervisionTestTime,
	})
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}

	// An operator takes over. The activation is revoked and the epoch is
	// raised, so the decision the replaced overseer had already formed names an
	// epoch the run is no longer at.
	takeover, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-takeover",
		Actor: supervisionOperator(), Event: domain.ActivationEventOperatorTakeover, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: confirmed.SupervisionRevision,
		TransitionedAt:              supervisionTestTime,
	})
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if takeover.Activation.State != domain.ActivationRevoked || takeover.Activation.Epoch != 2 {
		t.Fatalf("revoked activation = %#v, want revoked at epoch 2", takeover.Activation)
	}
	late := supervisionAcceptRequest("request-late-accept", 1, 7)
	late.Actor = overseer
	if _, err := store.DecideGate(ctx, late); !errors.Is(err, domain.ErrSupervisionStaleRevision) &&
		!errors.Is(err, domain.ErrSupervisionUnauthorizedActor) {
		t.Fatalf("late decision after takeover = %v, want it fenced out", err)
	}

	// The operator that took over decides for itself, and then the run is
	// cancelled. A decision arriving after cancellation is refused as terminal:
	// a late approval cannot reopen a run that has ended.
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run := records.WorkflowRuns[0]
	run.Progress = domain.ProgressCancelled
	run.Revision++
	run.UpdatedAt = supervisionTestTime
	if err := store.SaveCoordinatorRecords(ctx,
		CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
		t.Fatal(err)
	}
	afterCancel := supervisionAcceptRequest("request-accept-after-cancel", 1, 7)
	if _, err := store.DecideGate(ctx, afterCancel); !errors.Is(err, domain.ErrSupervisionTerminal) {
		t.Fatalf("decision after cancellation = %v, want ErrSupervisionTerminal", err)
	}
	snapshot, err := store.LoadSupervisionSnapshot(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Gates[0].State != domain.GateReadyForReview {
		t.Fatalf("a refused late approval moved the gate to %q", snapshot.Gates[0].State)
	}
}

// A supervisor principal resolves to the one run its own live activation names.
// A second run is not reachable by naming it, and neither is any run once the
// activation that granted the capability stops being live.
func TestSupervisorPrincipalIsScopedToItsOwnLiveActivation(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	ctx := context.Background()
	seedNamedSupervisedRun(t, store, "run-a", runScopedGate("run-a"))
	seedNamedSupervisedRun(t, store, "run-b", runScopedGate("run-b"))
	const principal = "remote:campaign-supervisor"

	liveActivationOnRun(t, store, "run-a", principal, supervisionTestTime.Add(time.Hour))
	runID, epoch, err := store.SupervisorScopeForPrincipal(ctx, principal)
	if err != nil {
		t.Fatalf("resolve supervisor scope: %v", err)
	}
	if runID != "run-a" || epoch != 1 {
		t.Fatalf("scope = %q epoch %d, want run-a at epoch 1", runID, epoch)
	}
	// Naming the other run does not reach it: the scope is the coordinator's
	// conclusion about the credential, not a field of the request.
	if runID == "run-b" {
		t.Fatal("the supervisor resolved to a run it holds no activation on")
	}
	// The decision path refuses the other run for the same reason: run-b has no
	// activation at all, so no overseer actor covers it.
	if _, err := store.DecideGate(ctx, GateDecisionRequest{
		RunID: "run-b", GateID: "run-b:review", RequestID: "request-cross-run",
		Actor:                 domain.Actor{Kind: domain.ActorOverseer, Principal: principal, ActivationEpoch: 1},
		ExpectedGraphRevision: 1, ExpectedGateRevision: 1,
		Evidence: domain.EvidenceSnapshot{ID: "evidence-b", GraphRevision: 1},
		Outcome:  domain.GateDecisionAccept, Reason: "a run this overseer does not supervise",
		DecidedAt: supervisionTestTime,
	}); err == nil {
		t.Fatal("an overseer decided a gate of a run it holds no activation on")
	} else if !errors.Is(err, domain.ErrSupervisionUnauthorizedActor) &&
		!errors.Is(err, domain.ErrSupervisionStaleRevision) &&
		!strings.Contains(err.Error(), "epoch") {
		t.Fatalf("cross-run decision error = %v, want an authority refusal", err)
	}

	// An expired lease ends the read half of the capability at the same moment
	// it ends the write half.
	liveActivationOnRun(t, store, "run-a", principal, supervisionTestTime.Add(-time.Minute))
	if runID, _, err = store.SupervisorScopeForPrincipal(ctx, principal); err != nil || runID != "" {
		t.Fatalf("expired scope = %q (err %v), want no run at all", runID, err)
	}
}

// seedSupervisionInboxEvent appends one pending supervision event.
func seedSupervisionInboxEvent(t *testing.T, store *Store, id string, sequence int64) {
	t.Helper()
	if _, err := store.db.ExecContext(context.Background(), `
		INSERT INTO coordinator_supervision_inbox(id, run_id, sequence, consumed, record)
		VALUES (?, 'run-1', ?, 0, ?)`, id, sequence, `{"id":"`+id+`"}`); err != nil {
		t.Fatalf("append supervision event %q: %v", id, err)
	}
}

// liveActivationOnRun writes one dispatched activation bound to a principal,
// which is the only thing that grants that principal any supervisor scope.
func liveActivationOnRun(t *testing.T, store *Store, runID, principal string, leaseExpiry time.Time) {
	t.Helper()
	ctx := context.Background()
	rows, err := store.LoadSupervisionActivationRows(ctx, runID)
	if err != nil {
		t.Fatalf("load activation rows of %q: %v", runID, err)
	}
	expiry := leaseExpiry.UTC()
	record := rows.Record
	record.RunID = runID
	record.ActivationEpoch = 1
	if err := store.CommitSupervisionActivationRows(ctx, SupervisionActivationRowCommit{
		RunID: runID, ExpectedRecordRevision: rows.Record.Revision, Record: record,
		Activation: domain.Activation{
			ID: runID + ":activation-1", RunID: runID, Epoch: 1,
			DispatchIdentity: "supervision:" + runID + ":1", Principal: principal,
			State: domain.ActivationActive, LeaseToken: "lease-1", LeaseExpiresAt: &expiry,
		},
		RequestID:   "request-activation-" + runID + "-" + expiry.Format(time.RFC3339Nano),
		CommittedAt: supervisionTestTime,
	}); err != nil {
		t.Fatalf("commit activation of %q: %v", runID, err)
	}
}
