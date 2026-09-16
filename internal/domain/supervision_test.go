package domain

import (
	"errors"
	"testing"
	"time"
)

// These tests are the executable form of the transition tables in
// docs/plans/campaign-supervision-seams.md, section 3. One case per row, named
// after the row, so a change to the document and a change to the code have to
// meet here.

const (
	testGraphRevision = int64(7)
	testGateID        = "gate:run-1:implementation_review"
)

func testOverseer() Actor {
	return Actor{Kind: ActorOverseer, Principal: "run-1", ActivationEpoch: 3}
}

func testOperator() Actor {
	return Actor{Kind: ActorOperator, Principal: "igor"}
}

func testGate(state GateState) Gate {
	return Gate{
		Definition: GateDefinition{
			ID:               testGateID,
			Name:             "implementation_review",
			ObservedTaskIDs:  []string{"implement"},
			ProtectedTaskIDs: []string{"qualify"},
		},
		RunID:         "run-1",
		State:         state,
		GraphRevision: testGraphRevision,
	}
}

// Section 3.1. Each row of the gate table, including the refusals.
func TestGateTransitionTable(t *testing.T) {
	ready := func(in *GateTransitionInput) {
		in.ExpectedGraphRevision = testGraphRevision
		in.EvidenceMatches = true
	}
	cases := []struct {
		name    string
		state   GateState
		event   GateEvent
		mutate  func(*GateTransitionInput)
		want    GateState
		wantErr error
	}{
		{
			name: "pending-evidence, producers succeeded and verified", state: GatePendingEvidence,
			event:  GateEventProducersSucceeded,
			mutate: func(in *GateTransitionInput) { in.ProducersVerified = true },
			want:   GateReadyForReview,
		},
		{
			name: "pending-evidence, producers succeeded but not in custody", state: GatePendingEvidence,
			event: GateEventProducersSucceeded, wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "pending-evidence, producer failed or was skipped", state: GatePendingEvidence,
			event: GateEventProducerNotSucceeded, want: GatePendingEvidence,
		},
		{
			name: "pending-evidence, run cancelled", state: GatePendingEvidence,
			event: GateEventRunCancelled, want: GateCancelled,
		},
		{
			name: "pending-evidence cannot be accepted", state: GatePendingEvidence,
			event: GateEventAccept, mutate: ready, wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "ready-for-review, overseer accepts", state: GateReadyForReview,
			event: GateEventAccept, mutate: ready, want: GateAccepted,
		},
		{
			name: "ready-for-review, operator accepts", state: GateReadyForReview,
			event:  GateEventAccept,
			mutate: func(in *GateTransitionInput) { ready(in); in.Actor = testOperator() },
			want:   GateAccepted,
		},
		{
			name: "ready-for-review, acceptance on a stale graph revision", state: GateReadyForReview,
			event:   GateEventAccept,
			mutate:  func(in *GateTransitionInput) { ready(in); in.ExpectedGraphRevision = testGraphRevision - 1 },
			wantErr: ErrSupervisionStaleRevision,
		},
		{
			name: "ready-for-review, acceptance whose evidence moved", state: GateReadyForReview,
			event:   GateEventAccept,
			mutate:  func(in *GateTransitionInput) { ready(in); in.EvidenceMatches = false },
			wantErr: ErrSupervisionStaleRevision,
		},
		{
			name: "ready-for-review, unknown actor kind", state: GateReadyForReview,
			event:   GateEventAccept,
			mutate:  func(in *GateTransitionInput) { ready(in); in.Actor = Actor{Kind: "worker", Principal: "w"} },
			wantErr: ErrSupervisionUnauthorizedActor,
		},
		{
			name: "ready-for-review, rejection with corrections", state: GateReadyForReview,
			event: GateEventReject, mutate: ready, want: GateHeld,
		},
		{
			name: "ready-for-review, review deadline expires", state: GateReadyForReview,
			event: GateEventReviewDeadlineExpired, want: GateEscalated,
		},
		{
			name: "ready-for-review, activation budget exhausted", state: GateReadyForReview,
			event: GateEventActivationBudgetExhausted, want: GateEscalated,
		},
		{
			name: "ready-for-review, producer retried", state: GateReadyForReview,
			event: GateEventProducerReplaced, want: GatePendingEvidence,
		},
		{
			name: "accepted, producer retried before any successor was offered", state: GateAccepted,
			event: GateEventProducerReplaced, want: GatePendingEvidence,
		},
		{
			name: "accepted, producer retried after a successor started", state: GateAccepted,
			event:   GateEventProducerReplaced,
			mutate:  func(in *GateTransitionInput) { in.SuccessorOfferedOrStarted = true },
			wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "accepted, amendment outside the gate scope with a receipt", state: GateAccepted,
			event:  GateEventAmendmentOutsideScope,
			mutate: func(in *GateTransitionInput) { in.RevalidationReceiptID = "receipt-1" },
			want:   GateAccepted,
		},
		{
			name: "accepted, amendment outside the gate scope without a receipt", state: GateAccepted,
			event: GateEventAmendmentOutsideScope, wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "accepted, amendment touching the gate scope", state: GateAccepted,
			event: GateEventAmendmentTouchesScope, want: GatePendingEvidence,
		},
		{
			name: "held, operator-authorized reconsideration", state: GateHeld,
			event:  GateEventReconsider,
			mutate: func(in *GateTransitionInput) { in.Actor = testOperator(); in.FreshSnapshot = true },
			want:   GateReadyForReview,
		},
		{
			name: "held, reconsideration without a fresh snapshot", state: GateHeld,
			event:   GateEventReconsider,
			mutate:  func(in *GateTransitionInput) { in.Actor = testOperator() },
			wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "held, overseer tries to reconsider", state: GateHeld,
			event:   GateEventReconsider,
			mutate:  func(in *GateTransitionInput) { in.FreshSnapshot = true },
			wantErr: ErrSupervisionUnauthorizedActor,
		},
		{
			name: "held, new eligible producer evidence", state: GateHeld,
			event:  GateEventNewProducerEvidence,
			mutate: func(in *GateTransitionInput) { in.ProducersVerified = true },
			want:   GatePendingEvidence,
		},
		{
			name: "held, overseer re-decides the same rejected evidence", state: GateHeld,
			event: GateEventAccept, mutate: ready, wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "escalated, operator-authorized reconsideration", state: GateEscalated,
			event:  GateEventReconsider,
			mutate: func(in *GateTransitionInput) { in.Actor = testOperator(); in.FreshSnapshot = true },
			want:   GateReadyForReview,
		},
		{
			name: "escalated cannot be accepted", state: GateEscalated,
			event: GateEventAccept, mutate: ready, wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "accepted, run cancelled", state: GateAccepted,
			event: GateEventRunCancelled, want: GateCancelled,
		},
		{
			name: "cancelled is terminal", state: GateCancelled,
			event:   GateEventReconsider,
			mutate:  func(in *GateTransitionInput) { in.Actor = testOperator(); in.FreshSnapshot = true },
			wantErr: ErrSupervisionTerminal,
		},
		{
			name: "nothing mutates after terminal sink settlement", state: GateReadyForReview,
			event:   GateEventAccept,
			mutate:  func(in *GateTransitionInput) { ready(in); in.SinkSettled = true },
			wantErr: ErrSupervisionTerminal,
		},
		{
			name: "an event with no row is an illegal transition", state: GateAccepted,
			event:   GateEventProducersSucceeded,
			mutate:  func(in *GateTransitionInput) { in.ProducersVerified = true },
			wantErr: ErrSupervisionIllegalTransition,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			in := GateTransitionInput{Gate: testGate(testCase.state), Event: testCase.event, Actor: testOverseer()}
			if testCase.mutate != nil {
				testCase.mutate(&in)
			}
			got, err := GateTransition(in)
			if testCase.wantErr != nil {
				if !errors.Is(err, testCase.wantErr) {
					t.Fatalf("want %v, got state %q err %v", testCase.wantErr, got, err)
				}
				if got != "" {
					t.Fatalf("a refused transition returned state %q", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != testCase.want {
				t.Fatalf("want %q, got %q", testCase.want, got)
			}
		})
	}
}

// Section 3.2. Holds, including the two ownership refusals.
func TestHoldTransitionTable(t *testing.T) {
	branchHold := func(state HoldState, owner Actor) Hold {
		return Hold{
			ID: "hold-1", RunID: "run-1", Owner: owner, State: state,
			Scope:           HoldScope{Kind: HoldScopeBranch, BranchRootTaskID: "implement"},
			GraphRevision:   testGraphRevision,
			ResolvedTaskIDs: []string{"implement", "qualify"},
		}
	}
	cases := []struct {
		name    string
		hold    Hold
		event   HoldEvent
		mutate  func(*HoldTransitionInput)
		want    HoldState
		wantErr error
	}{
		{
			name: "overseer places a branch hold", hold: branchHold("", testOverseer()), event: HoldEventPlace,
			mutate: func(in *HoldTransitionInput) {
				in.ActorScopeCoversRun, in.BranchRootExists, in.ClosureRecomputed = true, true, true
			},
			want: HoldActive,
		},
		{
			name: "operator places a run hold", event: HoldEventPlace,
			hold:   Hold{ID: "hold-2", RunID: "run-1", Owner: testOperator(), Scope: HoldScope{Kind: HoldScopeRun}},
			mutate: func(in *HoldTransitionInput) { in.Actor = testOperator(); in.ActorScopeCoversRun = true },
			want:   HoldActive,
		},
		{
			name: "placing a hold outside the actor's scope", hold: branchHold("", testOverseer()), event: HoldEventPlace,
			mutate: func(in *HoldTransitionInput) {
				in.BranchRootExists, in.ClosureRecomputed = true, true
			},
			wantErr: ErrSupervisionUnauthorizedActor,
		},
		{
			name: "branch root missing at the recorded graph revision", hold: branchHold("", testOverseer()),
			event: HoldEventPlace,
			mutate: func(in *HoldTransitionInput) {
				in.ActorScopeCoversRun, in.ClosureRecomputed = true, true
			},
			wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "closure not recomputed in the same transaction", hold: branchHold("", testOverseer()),
			event: HoldEventPlace,
			mutate: func(in *HoldTransitionInput) {
				in.ActorScopeCoversRun, in.BranchRootExists = true, true
			},
			wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "duplicate request key with the same payload replays", hold: branchHold(HoldActive, testOverseer()),
			event: HoldEventDuplicateSamePayload, want: HoldActive,
		},
		{
			name: "duplicate request key with a different payload is refused", hold: branchHold(HoldActive, testOverseer()),
			event: HoldEventDuplicateChangedPayload, wantErr: ErrSupervisionStaleRevision,
		},
		{
			name: "the owner releases its own hold", hold: branchHold(HoldActive, testOverseer()),
			event: HoldEventRelease, want: HoldReleased,
		},
		{
			name: "an operator releases an overseer hold", hold: branchHold(HoldActive, testOverseer()),
			event:  HoldEventRelease,
			mutate: func(in *HoldTransitionInput) { in.Actor = testOperator() },
			want:   HoldReleased,
		},
		{
			name: "an overseer cannot release an operator hold", hold: branchHold(HoldActive, testOperator()),
			event: HoldEventRelease, wantErr: ErrSupervisionUnauthorizedActor,
		},
		{
			name: "a later overseer epoch is not the same owner", hold: branchHold(HoldActive, Actor{
				Kind: ActorOverseer, Principal: "run-1", ActivationEpoch: 2,
			}),
			event: HoldEventRelease, wantErr: ErrSupervisionUnauthorizedActor,
		},
		{
			name: "an amendment adding descendants keeps the hold", hold: branchHold(HoldActive, testOverseer()),
			event:  HoldEventAmendmentAddsDescendants,
			mutate: func(in *HoldTransitionInput) { in.ClosureRecomputed = true },
			want:   HoldActive,
		},
		{
			name: "an amendment that did not recompute the closure is refused", hold: branchHold(HoldActive, testOverseer()),
			event: HoldEventAmendmentAddsDescendants, wantErr: ErrSupervisionPrerequisite,
		},
		{
			name:  "a claimed attempt inside the closure is reported, not interrupted",
			hold:  branchHold(HoldActive, testOverseer()),
			event: HoldEventClosureMemberClaimed, want: HoldActive,
		},
		{
			name: "run settlement releases the hold", hold: branchHold(HoldActive, testOverseer()),
			event: HoldEventRunSettled, want: HoldReleased,
		},
		{
			name: "a released hold is terminal", hold: branchHold(HoldReleased, testOverseer()),
			event: HoldEventRelease, wantErr: ErrSupervisionTerminal,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			in := HoldTransitionInput{Hold: testCase.hold, Event: testCase.event, Actor: testOverseer()}
			if testCase.mutate != nil {
				testCase.mutate(&in)
			}
			got, err := HoldTransition(in)
			if testCase.wantErr != nil {
				if !errors.Is(err, testCase.wantErr) {
					t.Fatalf("want %v, got state %q err %v", testCase.wantErr, got, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != testCase.want {
				t.Fatalf("want %q, got %q", testCase.want, got)
			}
		})
	}
}

// Section 3.3. Activations, including the budget accounting the plan insists on:
// an activation that started and then vanished counts, a provably undelivered
// dispatch does not.
func TestActivationTransitionTable(t *testing.T) {
	cases := []struct {
		name    string
		state   ActivationState
		event   ActivationEvent
		mutate  func(*ActivationTransitionInput)
		want    ActivationTransitionResult
		wantErr error
	}{
		{
			name: "idle, trigger fires with a non-empty inbox", state: ActivationIdle,
			event: ActivationEventTriggerFired,
			mutate: func(in *ActivationTransitionInput) {
				in.Supervised, in.InboxNonEmpty, in.ActivationBudgetRemaining = true, true, true
			},
			want: ActivationTransitionResult{State: ActivationPendingDispatch, Epoch: 3},
		},
		{
			name: "idle, trigger fires with an exhausted activation budget", state: ActivationIdle,
			event: ActivationEventTriggerFired,
			mutate: func(in *ActivationTransitionInput) {
				in.Supervised, in.InboxNonEmpty = true, true
			},
			want: ActivationTransitionResult{State: ActivationEscalated, Epoch: 3},
		},
		{
			name: "idle, a second activation is refused", state: ActivationIdle,
			event: ActivationEventTriggerFired,
			mutate: func(in *ActivationTransitionInput) {
				in.Supervised, in.InboxNonEmpty, in.ActivationBudgetRemaining = true, true, true
				in.OtherValidActivation = true
			},
			wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "idle, an empty inbox does not wake an overseer", state: ActivationIdle,
			event: ActivationEventTriggerFired,
			mutate: func(in *ActivationTransitionInput) {
				in.Supervised, in.ActivationBudgetRemaining = true, true
			},
			wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "pending-dispatch, dispatch confirmed spends one activation", state: ActivationPendingDispatch,
			event:  ActivationEventDispatchConfirmed,
			mutate: func(in *ActivationTransitionInput) { in.LeaseValid = true },
			want:   ActivationTransitionResult{State: ActivationActive, Epoch: 3, CountsTowardBudget: true},
		},
		{
			name: "pending-dispatch, provably undelivered retries and spends nothing", state: ActivationPendingDispatch,
			event: ActivationEventDispatchUndelivered,
			want:  ActivationTransitionResult{State: ActivationPendingDispatch, Epoch: 3},
		},
		{
			name:  "pending-dispatch, undelivered is refused once execution was observed",
			state: ActivationPendingDispatch, event: ActivationEventDispatchUndelivered,
			mutate:  func(in *ActivationTransitionInput) { in.ExecutionObserved = true },
			wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "pending-dispatch, ambiguous dispatch requires recovery", state: ActivationPendingDispatch,
			event: ActivationEventDispatchAmbiguous,
			want:  ActivationTransitionResult{State: ActivationRecoveryRequired, Epoch: 3},
		},
		{
			name: "active, a decision under a live lease at the current epoch", state: ActivationActive,
			event: ActivationEventDecisionRecorded,
			mutate: func(in *ActivationTransitionInput) {
				in.LeaseValid, in.RevisionMatches, in.TurnsRemaining = true, true, true
				in.ExpectedEpoch = 3
			},
			want: ActivationTransitionResult{State: ActivationActive, Epoch: 3},
		},
		{
			name: "active, a decision from a stale epoch is fenced out", state: ActivationActive,
			event: ActivationEventDecisionRecorded,
			mutate: func(in *ActivationTransitionInput) {
				in.LeaseValid, in.RevisionMatches, in.TurnsRemaining = true, true, true
				in.ExpectedEpoch = 2
			},
			wantErr: ErrSupervisionStaleRevision,
		},
		{
			name: "active, a decision after the turn budget is exhausted", state: ActivationActive,
			event: ActivationEventDecisionRecorded,
			mutate: func(in *ActivationTransitionInput) {
				in.LeaseValid, in.RevisionMatches = true, true
				in.ExpectedEpoch = 3
			},
			wantErr: ErrSupervisionBudgetExhausted,
		},
		{
			name: "active, a decision without a live lease", state: ActivationActive,
			event: ActivationEventDecisionRecorded,
			mutate: func(in *ActivationTransitionInput) {
				in.RevisionMatches, in.TurnsRemaining = true, true
				in.ExpectedEpoch = 3
			},
			wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "active, turn limit or elapsed maximum reached", state: ActivationActive,
			event: ActivationEventLimitReached,
			want:  ActivationTransitionResult{State: ActivationSpent, Epoch: 3},
		},
		{
			name: "active, lease expiry revokes decision authority", state: ActivationActive,
			event: ActivationEventLeaseExpired,
			want:  ActivationTransitionResult{State: ActivationRevoked, Epoch: 3},
		},
		{
			// Takeover raises the epoch as well as revoking, so a decision the
			// replaced overseer had already formed names an epoch that is no
			// longer current and is fenced out rather than landing after the
			// operator and reversing it.
			name: "active, operator takeover revokes and raises the epoch", state: ActivationActive,
			event:  ActivationEventOperatorTakeover,
			mutate: func(in *ActivationTransitionInput) { in.Actor = testOperator() },
			want:   ActivationTransitionResult{State: ActivationRevoked, Epoch: 4},
		},
		{
			name: "active, an overseer cannot take supervision over from itself", state: ActivationActive,
			event: ActivationEventOperatorTakeover, wantErr: ErrSupervisionUnauthorizedActor,
		},
		{
			name: "active, thread lost and the runtime proven stopped", state: ActivationActive,
			event: ActivationEventThreadLost,
			mutate: func(in *ActivationTransitionInput) {
				in.RuntimeProvenStopped, in.ExecutionObserved = true, true
			},
			want: ActivationTransitionResult{State: ActivationIdle, Epoch: 4, CountsTowardBudget: true},
		},
		{
			name: "active, thread lost with an ambiguous runtime", state: ActivationActive,
			event: ActivationEventThreadLost,
			want:  ActivationTransitionResult{State: ActivationRecoveryRequired, Epoch: 3},
		},
		{
			// Idle at epoch+1: the replacement must not derive the identity of
			// the activation whose authority was just revoked.
			name: "revoked, reconciliation acknowledged releases reservations at a fresh epoch", state: ActivationRevoked,
			event:  ActivationEventReconciliationAcknowledged,
			mutate: func(in *ActivationTransitionInput) { in.RecoveryComplete = true },
			want:   ActivationTransitionResult{State: ActivationIdle, Epoch: 4},
		},
		{
			name: "revoked, reconciliation before recovery completes", state: ActivationRevoked,
			event: ActivationEventReconciliationAcknowledged, wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "revoked, an ambiguous old runtime grants no replacement", state: ActivationRevoked,
			event: ActivationEventReconciliationAmbiguous,
			want:  ActivationTransitionResult{State: ActivationRecoveryRequired, Epoch: 3},
		},
		{
			// Idle at epoch+1. An activation's ID, dispatch identity, attempt,
			// assignment and thread are all derived from (run, epoch), so
			// returning to idle at the same epoch would make the second review
			// round recompute the first one instead of starting a new one.
			name: "spent, new events with budget remaining wake a fresh epoch", state: ActivationSpent,
			event:  ActivationEventEventsArrived,
			mutate: func(in *ActivationTransitionInput) { in.ActivationBudgetRemaining = true },
			want:   ActivationTransitionResult{State: ActivationIdle, Epoch: 4},
		},
		{
			name: "spent, new events without budget escalate once", state: ActivationSpent,
			event: ActivationEventEventsArrived,
			want:  ActivationTransitionResult{State: ActivationEscalated, Epoch: 3},
		},
		{
			name: "any state, the run settles terminally", state: ActivationActive,
			event: ActivationEventRunSettled,
			want:  ActivationTransitionResult{State: ActivationClosed, Epoch: 3},
		},
		{
			name: "escalated, operator-authorized continuation gets a fresh budget", state: ActivationEscalated,
			event: ActivationEventOperatorContinuation,
			mutate: func(in *ActivationTransitionInput) {
				in.Actor, in.OperatorAuthorized = testOperator(), true
			},
			want: ActivationTransitionResult{State: ActivationIdle, Epoch: 4, FreshBudget: true},
		},
		{
			name: "escalated, an overseer cannot authorize its own continuation", state: ActivationEscalated,
			event: ActivationEventOperatorContinuation, wantErr: ErrSupervisionUnauthorizedActor,
		},
		{
			name: "escalated waits for an operator", state: ActivationEscalated,
			event: ActivationEventTriggerFired,
			mutate: func(in *ActivationTransitionInput) {
				in.Supervised, in.InboxNonEmpty, in.ActivationBudgetRemaining = true, true, true
			},
			wantErr: ErrSupervisionIllegalTransition,
		},
		{
			name: "closed is terminal", state: ActivationClosed,
			event:   ActivationEventEventsArrived,
			mutate:  func(in *ActivationTransitionInput) { in.ActivationBudgetRemaining = true },
			wantErr: ErrSupervisionTerminal,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			in := ActivationTransitionInput{
				Activation: Activation{ID: "act-1", RunID: "run-1", Epoch: 3, State: testCase.state},
				Event:      testCase.event,
				Actor:      testOverseer(),
			}
			if testCase.mutate != nil {
				testCase.mutate(&in)
			}
			got, err := ActivationTransition(in)
			if testCase.wantErr != nil {
				if !errors.Is(err, testCase.wantErr) {
					t.Fatalf("want %v, got %+v err %v", testCase.wantErr, got, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != testCase.want {
				t.Fatalf("want %+v, got %+v", testCase.want, got)
			}
		})
	}
}

// Section 3.4. Incidents, including the two rules v1 settles: escalation waits
// for an operator, and bulk close does not exist.
func TestIncidentTransitionTable(t *testing.T) {
	incident := func(state IncidentState) ReviewIncident {
		return ReviewIncident{
			ID: "incident-1", RunID: "run-1", State: state, Revision: 4,
			SourceEventID: "event-9", SourceTaskID: "implement", SourceAttemptID: "attempt-2",
			RequiredDisposition: DispositionConcludeFailure,
		}
	}
	cases := []struct {
		name    string
		state   IncidentState
		event   IncidentEvent
		mutate  func(*IncidentTransitionInput)
		want    IncidentState
		wantErr error
	}{
		{
			name: "a changed normalized reason opens an incident", event: IncidentEventRaise,
			mutate: func(in *IncidentTransitionInput) { in.ReasonChangedOrThresholdCrossed = true },
			want:   IncidentOpen,
		},
		{
			name: "an unchanged reason raises nothing", event: IncidentEventRaise,
			wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "open, the matching gate acceptance resolves it", state: IncidentOpen,
			event:  IncidentEventGateAccepted,
			mutate: func(in *IncidentTransitionInput) { in.MatchingGateIncident = true },
			want:   IncidentResolved,
		},
		{
			name: "open, another gate's acceptance does not resolve it", state: IncidentOpen,
			event: IncidentEventGateAccepted, wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "open, the overseer escalates", state: IncidentOpen,
			event:  IncidentEventEscalate,
			mutate: func(in *IncidentTransitionInput) { in.ActorScopeCoversRun = true },
			want:   IncidentEscalated,
		},
		{
			name: "open, escalation from outside the run's scope", state: IncidentOpen,
			event: IncidentEventEscalate, wantErr: ErrSupervisionUnauthorizedActor,
		},
		{
			name: "open, conclude-failure on a terminally failed task", state: IncidentOpen,
			event: IncidentEventConcludeFailure,
			mutate: func(in *IncidentTransitionInput) {
				in.ActorScopeCoversRun, in.TaskTerminallyFailed = true, true
				in.ExpectedRevision = 4
			},
			want: IncidentResolved,
		},
		{
			name: "open, conclude-failure on a task that is still running", state: IncidentOpen,
			event: IncidentEventConcludeFailure,
			mutate: func(in *IncidentTransitionInput) {
				in.ActorScopeCoversRun, in.ExpectedRevision = true, 4
			},
			wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "open, conclude-failure naming a stale incident revision", state: IncidentOpen,
			event: IncidentEventConcludeFailure,
			mutate: func(in *IncidentTransitionInput) {
				in.ActorScopeCoversRun, in.TaskTerminallyFailed = true, true
				in.ExpectedRevision = 3
			},
			wantErr: ErrSupervisionStaleRevision,
		},
		{
			name: "open, the review deadline expires", state: IncidentOpen,
			event: IncidentEventDeadlineExpired, want: IncidentEscalated,
		},
		{
			name: "open, bulk close is not in v1", state: IncidentOpen,
			event: IncidentEventBulkClose, wantErr: ErrSupervisionPrerequisite,
		},
		{
			name: "escalated, the operator resolves after remediation", state: IncidentEscalated,
			event: IncidentEventOperatorResolve,
			mutate: func(in *IncidentTransitionInput) {
				in.Actor, in.ExpectedRevision = testOperator(), 4
			},
			want: IncidentResolved,
		},
		{
			name: "escalated, the operator resolves at a stale revision", state: IncidentEscalated,
			event:   IncidentEventOperatorResolve,
			mutate:  func(in *IncidentTransitionInput) { in.Actor = testOperator() },
			wantErr: ErrSupervisionStaleRevision,
		},
		{
			name: "escalated, the operator cancels", state: IncidentEscalated,
			event:  IncidentEventOperatorCancel,
			mutate: func(in *IncidentTransitionInput) { in.Actor = testOperator() },
			want:   IncidentResolved,
		},
		{
			name: "escalated, the overseer cannot resolve", state: IncidentEscalated,
			event: IncidentEventOperatorResolve, wantErr: ErrSupervisionUnauthorizedActor,
		},
		{
			name: "escalated, the overseer cannot conclude failure either", state: IncidentEscalated,
			event: IncidentEventConcludeFailure,
			mutate: func(in *IncidentTransitionInput) {
				in.ActorScopeCoversRun, in.TaskTerminallyFailed = true, true
				in.ExpectedRevision = 4
			},
			wantErr: ErrSupervisionUnauthorizedActor,
		},
		{
			name: "open, run cancellation resolves by cancellation", state: IncidentOpen,
			event: IncidentEventRunCancelled, want: IncidentResolved,
		},
		{
			name: "resolved is terminal", state: IncidentResolved,
			event:   IncidentEventEscalate,
			mutate:  func(in *IncidentTransitionInput) { in.ActorScopeCoversRun = true },
			wantErr: ErrSupervisionTerminal,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			source := incident(testCase.state)
			if testCase.state == "" {
				source = ReviewIncident{ID: "incident-1", RunID: "run-1"}
			}
			in := IncidentTransitionInput{Incident: source, Event: testCase.event, Actor: testOverseer()}
			if testCase.mutate != nil {
				testCase.mutate(&in)
			}
			got, err := IncidentTransition(in)
			if testCase.wantErr != nil {
				if !errors.Is(err, testCase.wantErr) {
					t.Fatalf("want %v, got state %q err %v", testCase.wantErr, got, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != testCase.want {
				t.Fatalf("want %q, got %q", testCase.want, got)
			}
		})
	}
}

// One incident closing must not dismiss a newer pending one. The state machine
// acts on one incident at a time, which is what makes that true.
func TestResolvingOneIncidentLeavesANewerOneOpen(t *testing.T) {
	newer := ReviewIncident{ID: "incident-2", RunID: "run-1", State: IncidentOpen, Revision: 1}
	older := ReviewIncident{ID: "incident-1", RunID: "run-1", State: IncidentOpen, Revision: 4}
	resolved, err := IncidentTransition(IncidentTransitionInput{
		Incident: older, Event: IncidentEventGateAccepted, Actor: testOverseer(),
		MatchingGateIncident: true,
	})
	if err != nil || resolved != IncidentResolved {
		t.Fatalf("the older incident did not resolve: %q %v", resolved, err)
	}
	if newer.State != IncidentOpen {
		t.Fatal("resolving one incident changed another")
	}
}

func supervisedSnapshot(gates []Gate, holds []Hold) SupervisionSnapshot {
	return SupervisionSnapshot{
		RunID: "run-1", GraphRevision: testGraphRevision, Supervised: true,
		RouteAvailable: true, Gates: gates, Holds: holds,
	}
}

// The predicate's truth table: one protected task against every gate state,
// every hold scope and the run-level conditions.
func TestSupervisionAdmitsTruthTable(t *testing.T) {
	runHold := Hold{
		ID: "hold-run", RunID: "run-1", State: HoldActive, Reason: "pausing the run",
		Scope: HoldScope{Kind: HoldScopeRun}, Owner: testOperator(),
	}
	branchHold := Hold{
		ID: "hold-branch", RunID: "run-1", State: HoldActive, Reason: "reviewing the branch",
		Scope:           HoldScope{Kind: HoldScopeBranch, BranchRootTaskID: "implement"},
		ResolvedTaskIDs: []string{"implement", "qualify"}, Owner: testOverseer(),
	}
	releasedHold := branchHold
	releasedHold.ID, releasedHold.State = "hold-released", HoldReleased

	cases := []struct {
		name     string
		snapshot SupervisionSnapshot
		query    SupervisionQuery
		admitted bool
		codes    []SupervisionBlockerCode
	}{
		{
			name:     "an unsupervised run admits everything",
			snapshot: SupervisionSnapshot{RunID: "run-1", GraphRevision: testGraphRevision},
			query:    SupervisionQuery{TaskID: "qualify"}, admitted: true,
		},
		{
			name:     "a gate with no evidence yet blocks its protected task",
			snapshot: supervisedSnapshot([]Gate{testGate(GatePendingEvidence)}, nil),
			query:    SupervisionQuery{TaskID: "qualify"},
			codes:    []SupervisionBlockerCode{SupervisionBlockerGatePending},
		},
		{
			name:     "a gate awaiting review blocks its protected task",
			snapshot: supervisedSnapshot([]Gate{testGate(GateReadyForReview)}, nil),
			query:    SupervisionQuery{TaskID: "qualify"},
			codes:    []SupervisionBlockerCode{SupervisionBlockerGateAwaitingReview},
		},
		{
			name:     "a held gate blocks its protected task",
			snapshot: supervisedSnapshot([]Gate{testGate(GateHeld)}, nil),
			query:    SupervisionQuery{TaskID: "qualify"},
			codes:    []SupervisionBlockerCode{SupervisionBlockerGateHeld},
		},
		{
			name:     "an escalated gate blocks its protected task",
			snapshot: supervisedSnapshot([]Gate{testGate(GateEscalated)}, nil),
			query:    SupervisionQuery{TaskID: "qualify"},
			codes:    []SupervisionBlockerCode{SupervisionBlockerGateEscalated},
		},
		{
			name:     "a cancelled gate blocks its protected task",
			snapshot: supervisedSnapshot([]Gate{testGate(GateCancelled)}, nil),
			query:    SupervisionQuery{TaskID: "qualify"},
			codes:    []SupervisionBlockerCode{SupervisionBlockerGateCancelled},
		},
		{
			name:     "an accepted gate admits its protected task",
			snapshot: supervisedSnapshot([]Gate{testGate(GateAccepted)}, nil),
			query:    SupervisionQuery{TaskID: "qualify"}, admitted: true,
		},
		{
			name:     "a gate does not block the task it observes",
			snapshot: supervisedSnapshot([]Gate{testGate(GatePendingEvidence)}, nil),
			query:    SupervisionQuery{TaskID: "implement"}, admitted: true,
		},
		{
			name:     "a gate does not block an unrelated branch",
			snapshot: supervisedSnapshot([]Gate{testGate(GateReadyForReview)}, nil),
			query:    SupervisionQuery{TaskID: "document"}, admitted: true,
		},
		{
			name: "every gate protecting a task must be accepted",
			snapshot: func() SupervisionSnapshot {
				second := testGate(GateReadyForReview)
				second.Definition.ID = "gate:run-1:security_review"
				return supervisedSnapshot([]Gate{testGate(GateAccepted), second}, nil)
			}(),
			query: SupervisionQuery{TaskID: "qualify"},
			codes: []SupervisionBlockerCode{SupervisionBlockerGateAwaitingReview},
		},
		{
			name:     "a run hold blocks every task",
			snapshot: supervisedSnapshot(nil, []Hold{runHold}),
			query:    SupervisionQuery{TaskID: "document"},
			codes:    []SupervisionBlockerCode{SupervisionBlockerRunHold},
		},
		{
			name:     "a branch hold blocks its resolved closure",
			snapshot: supervisedSnapshot(nil, []Hold{branchHold}),
			query:    SupervisionQuery{TaskID: "qualify"},
			codes:    []SupervisionBlockerCode{SupervisionBlockerBranchHold},
		},
		{
			name:     "a branch hold does not block outside its closure",
			snapshot: supervisedSnapshot(nil, []Hold{branchHold}),
			query:    SupervisionQuery{TaskID: "document"}, admitted: true,
		},
		{
			name:     "a released hold blocks nothing",
			snapshot: supervisedSnapshot(nil, []Hold{releasedHold}),
			query:    SupervisionQuery{TaskID: "qualify"}, admitted: true,
		},
		{
			name: "an unavailable supervisor route keeps a waiting gate closed",
			snapshot: func() SupervisionSnapshot {
				snapshot := supervisedSnapshot([]Gate{testGate(GateReadyForReview)}, nil)
				snapshot.RouteAvailable = false
				return snapshot
			}(),
			query: SupervisionQuery{TaskID: "qualify"},
			codes: []SupervisionBlockerCode{
				SupervisionBlockerGateAwaitingReview, SupervisionBlockerRouteUnavailable,
			},
		},
		{
			name: "an unavailable supervisor route does not block unrelated branches",
			snapshot: func() SupervisionSnapshot {
				snapshot := supervisedSnapshot([]Gate{testGate(GateReadyForReview)}, nil)
				snapshot.RouteAvailable = false
				return snapshot
			}(),
			query: SupervisionQuery{TaskID: "document"}, admitted: true,
		},
		{
			name: "a cancelled run admits nothing",
			snapshot: func() SupervisionSnapshot {
				snapshot := supervisedSnapshot(nil, nil)
				snapshot.RunCancelled = true
				return snapshot
			}(),
			query: SupervisionQuery{TaskID: "document"},
			codes: []SupervisionBlockerCode{SupervisionBlockerRunCancelled},
		},
		{
			name: "a settled run admits nothing",
			snapshot: func() SupervisionSnapshot {
				snapshot := supervisedSnapshot(nil, nil)
				snapshot.RunTerminal = true
				return snapshot
			}(),
			query: SupervisionQuery{TaskID: "document"},
			codes: []SupervisionBlockerCode{SupervisionBlockerRunTerminal},
		},
		{
			name:     "a stale graph revision refuses rather than answering",
			snapshot: supervisedSnapshot([]Gate{testGate(GateAccepted)}, nil),
			query:    SupervisionQuery{TaskID: "qualify", ExpectedGraphRevision: testGraphRevision + 1},
			codes:    []SupervisionBlockerCode{SupervisionBlockerStaleSnapshot},
		},
		{
			name:     "the fence passes at the expected graph revision",
			snapshot: supervisedSnapshot([]Gate{testGate(GateAccepted)}, nil),
			query:    SupervisionQuery{TaskID: "qualify", ExpectedGraphRevision: testGraphRevision},
			admitted: true,
		},
		{
			name: "a final-settlement gate protects no worker task",
			snapshot: supervisedSnapshot([]Gate{{
				Definition: GateDefinition{
					ID: "gate:run-1:final", Name: "final", ObservedTaskIDs: []string{"qualify"}, Final: true,
				},
				RunID: "run-1", State: GatePendingEvidence, GraphRevision: testGraphRevision,
			}}, nil),
			query: SupervisionQuery{TaskID: "qualify"}, admitted: true,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			verdict := SupervisionAdmits(testCase.snapshot, testCase.query)
			if verdict.Admitted != testCase.admitted {
				t.Fatalf("admitted %v, want %v (blockers %+v)", verdict.Admitted, testCase.admitted, verdict.Blockers)
			}
			if len(verdict.Blockers) != len(testCase.codes) {
				t.Fatalf("want codes %v, got blockers %+v", testCase.codes, verdict.Blockers)
			}
			for index, code := range testCase.codes {
				if verdict.Blockers[index].Code != code {
					t.Fatalf("blocker %d is %q, want %q", index, verdict.Blockers[index].Code, code)
				}
				if verdict.Blockers[index].TaskID != testCase.query.TaskID {
					t.Fatalf("blocker %d names task %q", index, verdict.Blockers[index].TaskID)
				}
			}
			if verdict.Admitted && len(verdict.Blockers) != 0 {
				t.Fatal("an admitted verdict carries blockers")
			}
		})
	}
}

// The predicate is the same function for all four callers, so the same state
// must give the same answer however often it is asked and in whichever order
// the gates and holds happen to be listed.
func TestSupervisionAdmitsIsPureAndStable(t *testing.T) {
	gateOne := testGate(GateHeld)
	gateTwo := testGate(GateReadyForReview)
	gateTwo.Definition.ID = "gate:run-1:security_review"
	hold := Hold{
		ID: "hold-branch", RunID: "run-1", State: HoldActive, Owner: testOverseer(),
		Scope:           HoldScope{Kind: HoldScopeBranch, BranchRootTaskID: "implement"},
		ResolvedTaskIDs: []string{"qualify"},
	}
	forward := supervisedSnapshot([]Gate{gateOne, gateTwo}, []Hold{hold})
	reverse := supervisedSnapshot([]Gate{gateTwo, gateOne}, []Hold{hold})
	query := SupervisionQuery{TaskID: "qualify"}

	first := SupervisionAdmits(forward, query)
	second := SupervisionAdmits(forward, query)
	third := SupervisionAdmits(reverse, query)
	if len(first.Blockers) != 3 {
		t.Fatalf("want three blockers, got %+v", first.Blockers)
	}
	for index := range first.Blockers {
		if first.Blockers[index] != second.Blockers[index] || first.Blockers[index] != third.Blockers[index] {
			t.Fatalf("blocker %d is not stable: %+v %+v %+v",
				index, first.Blockers[index], second.Blockers[index], third.Blockers[index])
		}
	}
	if len(forward.Gates) != 2 || len(forward.Holds) != 1 {
		t.Fatal("the predicate mutated its snapshot")
	}
}

// Section 3.5. The precedence order, one case per rank plus the two orderings
// the document calls out deliberately.
func TestSupervisedRunStatusPrecedence(t *testing.T) {
	liveAttempt := Attempt{
		ID: "a1", WorkflowRunID: "run-1", TaskID: "implement",
		Progress: ProgressActive, Control: ControlRunning,
	}
	parkedAttempt := Attempt{
		ID: "a2", WorkflowRunID: "run-1", TaskID: "implement",
		Progress: ProgressWaitingExternal, Control: ControlWaitingExternal,
	}
	needsInputAttempt := Attempt{
		ID: "a3", WorkflowRunID: "run-1", TaskID: "implement",
		Progress: ProgressNeedsInput, Control: ControlStopped,
	}
	escalatedIncident := ReviewIncident{ID: "incident-1", RunID: "run-1", State: IncidentEscalated}
	openIncident := ReviewIncident{ID: "incident-2", RunID: "run-1", State: IncidentOpen}
	heldGateSnapshot := supervisedSnapshot([]Gate{testGate(GateHeld)}, nil)

	cases := []struct {
		name string
		in   SupervisedRunStatusInput
		want SupervisedRunStatus
	}{
		{
			name: "rank 1: a live turn outranks everything below it",
			in: SupervisedRunStatusInput{
				Snapshot: heldGateSnapshot, Attempts: []Attempt{liveAttempt, parkedAttempt},
				Incidents: []ReviewIncident{escalatedIncident}, TasksRemaining: true,
			},
			want: SupervisedRunActive,
		},
		{
			name: "rank 2: a parked attempt outranks a runnable task",
			in: SupervisedRunStatusInput{
				Snapshot: supervisedSnapshot(nil, nil), Attempts: []Attempt{parkedAttempt},
				ReadyTaskIDs: []string{"document"}, TasksRemaining: true,
			},
			want: SupervisedRunWaitingExternal,
		},
		{
			name: "rank 3: an admitted ready task outranks an escalated incident",
			in: SupervisedRunStatusInput{
				Snapshot:     supervisedSnapshot([]Gate{testGate(GateHeld)}, nil),
				ReadyTaskIDs: []string{"document"}, Incidents: []ReviewIncident{escalatedIncident},
				TasksRemaining: true,
			},
			want: SupervisedRunRunnable,
		},
		{
			name: "rank 3 is not reached by a task the predicate blocks",
			in: SupervisedRunStatusInput{
				Snapshot: heldGateSnapshot, ReadyTaskIDs: []string{"qualify"}, TasksRemaining: true,
			},
			want: SupervisedRunHeld,
		},
		{
			name: "rank 4: an escalation outranks a hold, because it needs a human",
			in: SupervisedRunStatusInput{
				Snapshot: heldGateSnapshot, Incidents: []ReviewIncident{escalatedIncident},
				TasksRemaining: true,
			},
			want: SupervisedRunEscalated,
		},
		{
			name: "rank 4: needs-input is an escalation too",
			in: SupervisedRunStatusInput{
				Snapshot: heldGateSnapshot, Attempts: []Attempt{needsInputAttempt}, TasksRemaining: true,
			},
			want: SupervisedRunEscalated,
		},
		{
			name: "rank 5: a gate awaiting review is held, an open incident is not an escalation",
			in: SupervisedRunStatusInput{
				Snapshot:  supervisedSnapshot([]Gate{testGate(GateReadyForReview)}, nil),
				Incidents: []ReviewIncident{openIncident}, TasksRemaining: true,
			},
			want: SupervisedRunHeld,
		},
		{
			name: "rank 5: an active hold over a ready task",
			in: SupervisedRunStatusInput{
				Snapshot: supervisedSnapshot(nil, []Hold{{
					ID: "hold-1", RunID: "run-1", State: HoldActive, Owner: testOperator(),
					Scope: HoldScope{Kind: HoldScopeRun},
				}}),
				ReadyTaskIDs: []string{"document"}, TasksRemaining: true,
			},
			want: SupervisedRunHeld,
		},
		{
			name: "rank 6: ordinary dependency blocking",
			in: SupervisedRunStatusInput{
				Snapshot: supervisedSnapshot([]Gate{testGate(GateAccepted)}, nil), TasksRemaining: true,
			},
			want: SupervisedRunBlocked,
		},
		{
			name: "rank 7: the sink settled",
			in: SupervisedRunStatusInput{
				Snapshot: supervisedSnapshot([]Gate{testGate(GateAccepted)}, nil), SinkSettled: true,
			},
			want: SupervisedRunTerminal,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := SupervisedRunStatusLabel(testCase.in); got != testCase.want {
				t.Fatalf("want %q, got %q", testCase.want, got)
			}
		})
	}
}

// A held task is a derived label. Nothing in the progress state machine learned
// a new value, which is the decision this test pins.
func TestHeldIsALabelAndNotAProgressState(t *testing.T) {
	for _, state := range []ProgressState{
		ProgressQueued, ProgressBlocked, ProgressReady, ProgressActive, ProgressNeedsInput,
		ProgressWaitingExternal, ProgressVerifying, ProgressSucceeded, ProgressFailed,
		ProgressCancelled, ProgressSkipped,
	} {
		if string(state) == string(SupervisedRunHeld) {
			t.Fatalf("a supervision label leaked into ProgressState as %q", state)
		}
	}
}

// Gate acceptance invalidation. Conservative by default: the only carry-forward
// is an amendment that touches neither set and wrote a revalidation receipt.
func TestGateAcceptanceInvalidation(t *testing.T) {
	gate := GateDefinition{
		ID: testGateID, Name: "implementation_review",
		ObservedTaskIDs: []string{"implement", "analyse"}, ProtectedTaskIDs: []string{"qualify"},
	}
	acceptance := GateDecision{
		ID: "decision-1", RunID: "run-1", GateID: testGateID, Actor: testOverseer(),
		ActivationEpoch: 3, GraphRevision: testGraphRevision, Outcome: GateDecisionAccept,
		RequestID: "req-1", DecidedAt: time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC),
		Evidence: EvidenceSnapshot{
			ID: "evidence-1", GraphRevision: testGraphRevision,
			Producers: []ProducerEvidence{{
				TaskID: "implement", AttemptID: "attempt-1", ResultRevision: 5,
				ArtifactDigests:  []ArtifactDigest{{ArtifactID: "art-1", Digest: "sha256:abc"}},
				CommitIdentities: []CommitIdentity{{Repository: "t3-steward", Commit: "d57d01a"}},
			}},
		},
	}
	rejection := acceptance
	rejection.Outcome = GateDecisionReject

	cases := []struct {
		name       string
		acceptance GateDecision
		change     GateChange
		stands     bool
		reason     AcceptanceInvalidation
	}{
		{
			name: "a new attempt on an observed task invalidates", acceptance: acceptance,
			change: GateChange{Kind: GateChangeNewAttempt, TaskID: "implement"},
			reason: AcceptanceInvalidObservedRetried,
		},
		{
			name: "a new attempt on the other observed task invalidates too", acceptance: acceptance,
			change: GateChange{Kind: GateChangeNewAttempt, TaskID: "analyse"},
			reason: AcceptanceInvalidObservedRetried,
		},
		{
			name: "a new attempt on an unobserved task leaves it standing", acceptance: acceptance,
			change: GateChange{Kind: GateChangeNewAttempt, TaskID: "document"},
			stands: true, reason: AcceptanceStandsUnaffected,
		},
		{
			name: "an amendment touching an observed task invalidates unconditionally", acceptance: acceptance,
			change: GateChange{
				Kind: GateChangeAmendment, AmendedTaskIDs: []string{"implement"},
				RevalidationReceiptID: "receipt-1",
			},
			reason: AcceptanceInvalidScopeAmended,
		},
		{
			name: "an amendment touching a protected task invalidates unconditionally", acceptance: acceptance,
			change: GateChange{
				Kind: GateChangeAmendment, AmendedTaskIDs: []string{"qualify"},
				RevalidationReceiptID: "receipt-1",
			},
			reason: AcceptanceInvalidScopeAmended,
		},
		{
			name: "an amendment outside both sets carries forward with a receipt", acceptance: acceptance,
			change: GateChange{
				Kind: GateChangeAmendment, AmendedTaskIDs: []string{"document"},
				RevalidationReceiptID: "receipt-1",
			},
			stands: true, reason: AcceptanceStandsRevalidated,
		},
		{
			name: "an amendment outside both sets without a receipt does not", acceptance: acceptance,
			change: GateChange{Kind: GateChangeAmendment, AmendedTaskIDs: []string{"document"}},
			reason: AcceptanceInvalidNoRevalidation,
		},
		{
			name: "a rejection is not an acceptance that can stand", acceptance: rejection,
			change: GateChange{Kind: GateChangeNewAttempt, TaskID: "document"},
			reason: AcceptanceInvalidNotAnAcceptance,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			verdict := GateAcceptanceStands(gate, testCase.acceptance, testCase.change)
			if verdict.Stands != testCase.stands || verdict.Reason != testCase.reason {
				t.Fatalf("want stands=%v reason=%q, got %+v", testCase.stands, testCase.reason, verdict)
			}
		})
	}
}

// Invalidation and the gate state machine agree: an invalidated acceptance goes
// back to pending-evidence, unless a protected task is already offered or
// started, which v1 refuses rather than pretending to undo.
func TestInvalidationAndGateTransitionAgree(t *testing.T) {
	gate := testGate(GateAccepted)
	acceptance := GateDecision{Outcome: GateDecisionAccept, GateID: gate.Definition.ID}
	verdict := GateAcceptanceStands(gate.Definition, acceptance,
		GateChange{Kind: GateChangeNewAttempt, TaskID: "implement"})
	if verdict.Stands {
		t.Fatal("a retried producer left the acceptance standing")
	}
	next, err := GateTransition(GateTransitionInput{Gate: gate, Event: GateEventProducerReplaced, Actor: testOverseer()})
	if err != nil || next != GatePendingEvidence {
		t.Fatalf("want pending-evidence, got %q %v", next, err)
	}
	_, err = GateTransition(GateTransitionInput{
		Gate: gate, Event: GateEventProducerReplaced, Actor: testOverseer(),
		SuccessorOfferedOrStarted: true,
	})
	if !errors.Is(err, ErrSupervisionPrerequisite) {
		t.Fatalf("want a refusal once a successor started, got %v", err)
	}
}

func TestSupervisionConfigValidation(t *testing.T) {
	valid := SupervisionConfig{
		Route:            ProviderRoute{ProviderInstanceID: "codex-main", Model: "gpt-5.6-sol"},
		PromptArtifactID: "artifact-prompt",
		MaxActivations:   12, MaxTurnsPerActivation: 3,
		ActivationDeadline: 2 * time.Hour, IdleEscalationAfter: 24 * time.Hour,
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*SupervisionConfig){
		"no provider instance": func(c *SupervisionConfig) { c.Route.ProviderInstanceID = "" },
		"no model":             func(c *SupervisionConfig) { c.Route.Model = "" },
		"no prompt":            func(c *SupervisionConfig) { c.PromptArtifactID = "" },
		"no activations":       func(c *SupervisionConfig) { c.MaxActivations = 0 },
		"unbounded activations": func(c *SupervisionConfig) {
			c.MaxActivations = MaxSupervisionActivations + 1
		},
		"no turns": func(c *SupervisionConfig) { c.MaxTurnsPerActivation = 0 },
		"unbounded turns": func(c *SupervisionConfig) {
			c.MaxTurnsPerActivation = MaxSupervisionTurnsPerActivation + 1
		},
		"no deadline": func(c *SupervisionConfig) { c.ActivationDeadline = 0 },
		"unbounded deadline": func(c *SupervisionConfig) {
			c.ActivationDeadline = MaxSupervisionActivationDeadline + time.Hour
		},
		"unbounded idle escalation": func(c *SupervisionConfig) {
			c.IdleEscalationAfter = MaxSupervisionIdleEscalation + time.Hour
		},
	} {
		t.Run(name, func(t *testing.T) {
			invalid := valid
			mutate(&invalid)
			if err := invalid.Validate(); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestGateDefinitionValidation(t *testing.T) {
	valid := GateDefinition{
		ID: testGateID, Name: "implementation_review",
		ObservedTaskIDs: []string{"implement"}, ProtectedTaskIDs: []string{"qualify"},
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	final := GateDefinition{
		ID: "gate:run-1:final", Name: "final", ObservedTaskIDs: []string{"qualify"}, Final: true,
	}
	if err := final.Validate(); err != nil {
		t.Fatalf("a final-settlement gate with no protected task was refused: %v", err)
	}
	for name, mutate := range map[string]func(*GateDefinition){
		"no id":                  func(d *GateDefinition) { d.ID = "" },
		"no name":                func(d *GateDefinition) { d.Name = "" },
		"nothing observed":       func(d *GateDefinition) { d.ObservedTaskIDs = nil },
		"nothing protected":      func(d *GateDefinition) { d.ProtectedTaskIDs = nil },
		"final but protecting":   func(d *GateDefinition) { d.Final = true },
		"observes what it holds": func(d *GateDefinition) { d.ProtectedTaskIDs = []string{"implement"} },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := valid
			mutate(&invalid)
			if err := invalid.Validate(); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// A clone or rerun inherits the supervision configuration and inherits no
// acceptance, hold or incident. The domain expresses that as: the record
// carries the config, and the gate, hold and incident records are separate
// values that a clone simply does not copy.
func TestSupervisionRecordBudgetAccounting(t *testing.T) {
	record := SupervisionRecord{
		RunID: "run-1", ActivationsUsed: 11,
		Config: SupervisionConfig{MaxActivations: 12},
	}
	if !record.ActivationBudgetRemaining() {
		t.Fatal("the last activation of the budget was refused")
	}
	record.ActivationsUsed = 12
	if record.ActivationBudgetRemaining() {
		t.Fatal("an exhausted budget still reported headroom")
	}
	// An operator-authorized continuation raises the granted budget; it never
	// resets the counter.
	record.BudgetGrantedActivations = 14
	if !record.ActivationBudgetRemaining() || record.ActivationsUsed != 12 {
		t.Fatalf("a continuation did not extend the budget honestly: %+v", record)
	}
}
