package backlog

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var turnOutcomeTestTime = time.Date(2026, time.September, 11, 1, 0, 0, 0, time.UTC)

func TestPlanTurnOutcomesCompletionAndThrottlePrecedence(t *testing.T) {
	tests := []struct {
		name         string
		marker       domain.TurnOutcomeMarker
		command      domain.ThrottleCommandKind
		control      domain.ControlState
		wantProgress domain.ProgressState
		wantControl  domain.ControlState
		wantDelivery domain.ThrottleDeliveryState
		wantTerminal bool
	}{
		{
			name: "done during drain", marker: domain.TurnOutcomeDone,
			command: domain.ThrottleCommandDrain, control: domain.ControlDraining,
			wantProgress: domain.ProgressSucceeded, wantControl: domain.ControlStopped,
			wantDelivery: domain.ThrottleDeliveryCancelled, wantTerminal: true,
		},
		{
			name: "continue during drain", marker: domain.TurnOutcomeContinue,
			command: domain.ThrottleCommandDrain, control: domain.ControlDraining,
			wantProgress: domain.ProgressActive, wantControl: domain.ControlDraining,
			wantDelivery: domain.ThrottleDeliveryPending,
		},
		{
			name: "missing status under closure", marker: domain.TurnOutcomeMissing,
			command: domain.ThrottleCommandDrain, control: domain.ControlDraining,
			wantProgress: domain.ProgressActive, wantControl: domain.ControlDraining,
			wantDelivery: domain.ThrottleDeliveryPending,
		},
		{
			name: "late done after hard stop intent", marker: domain.TurnOutcomeDone,
			command: domain.ThrottleCommandHardStop, control: domain.ControlDraining,
			wantProgress: domain.ProgressSucceeded, wantControl: domain.ControlStopped,
			wantDelivery: domain.ThrottleDeliveryCancelled, wantTerminal: true,
		},
		{
			name: "continue while resume is pending", marker: domain.TurnOutcomeContinue,
			command: domain.ThrottleCommandResume, control: domain.ControlResuming,
			wantProgress: domain.ProgressActive, wantControl: domain.ControlResuming,
			wantDelivery: domain.ThrottleDeliveryPending,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attempt := turnOutcomeAttempt("attempt")
			record := turnOutcomeThrottle("attempt", "directive", test.command, test.control)
			outcome := turnOutcome("outcome", "attempt", test.marker)
			outcome.VerificationPassed = test.marker == domain.TurnOutcomeDone

			transitions, err := PlanTurnOutcomes(
				[]domain.Attempt{attempt},
				[]domain.ThrottleAttemptRecord{record},
				[]domain.TurnOutcome{outcome},
				turnOutcomeTestTime,
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(transitions) != 1 || transitions[0].Throttle == nil {
				t.Fatalf("transitions = %#v", transitions)
			}
			got := transitions[0]
			if got.Attempt.Progress != test.wantProgress || got.Attempt.Control != test.wantControl {
				t.Fatalf("attempt = %#v", got.Attempt)
			}
			if got.Attempt.Revision != attempt.Revision+1 ||
				got.Attempt.LastTurnOutcomeID != outcome.ID ||
				got.Attempt.LastTurnOutcomeMarker != outcome.Marker {
				t.Fatalf("attempt outcome identity = %#v", got.Attempt)
			}
			if got.Throttle.Record.Delivery != test.wantDelivery ||
				got.Throttle.Record.Control != test.wantControl ||
				got.Throttle.Record.Revision != record.Revision+1 {
				t.Fatalf("throttle = %#v", got.Throttle.Record)
			}
			if (got.Attempt.CompletedAt != nil) != test.wantTerminal {
				t.Fatalf("completedAt = %v, want terminal %v", got.Attempt.CompletedAt, test.wantTerminal)
			}
		})
	}
}

func TestPlanTurnOutcomesDoneFailureAndInactiveOrdinaryOutcomes(t *testing.T) {
	attempts := []domain.Attempt{
		turnOutcomeAttempt("failed"),
		turnOutcomeAttempt("ordinary"),
	}
	failed := turnOutcome("done-failed", "failed", domain.TurnOutcomeDone)
	failed.Failure = "verification command failed"
	outcomes := []domain.TurnOutcome{
		turnOutcome("ordinary-continue", "ordinary", domain.TurnOutcomeContinue),
		failed,
	}
	transitions, err := PlanTurnOutcomes(attempts, nil, outcomes, turnOutcomeTestTime)
	if err != nil {
		t.Fatal(err)
	}
	if len(transitions) != 1 {
		t.Fatalf("transitions = %#v", transitions)
	}
	got := transitions[0].Attempt
	if got.ID != "failed" || got.Progress != domain.ProgressFailed ||
		got.Failure != failed.Failure || got.Control != domain.ControlStopped {
		t.Fatalf("failed done = %#v", got)
	}
	if transitions[0].Throttle != nil {
		t.Fatalf("unexpected throttle transition = %#v", transitions[0].Throttle)
	}
}

func TestPlanTurnOutcomesDuplicateTerminalOutcomeIsNoOp(t *testing.T) {
	attempt := turnOutcomeAttempt("attempt")
	attempt.Progress = domain.ProgressSucceeded
	attempt.Control = domain.ControlStopped
	attempt.LastTurnOutcomeID = "outcome"
	attempt.LastTurnOutcomeMarker = domain.TurnOutcomeDone
	attempt.CompletedAt = turnOutcomeTime(turnOutcomeTestTime)
	outcome := turnOutcome("outcome", "attempt", domain.TurnOutcomeDone)
	outcome.VerificationPassed = true

	transitions, err := PlanTurnOutcomes(
		[]domain.Attempt{attempt}, nil, []domain.TurnOutcome{outcome}, turnOutcomeTestTime.Add(time.Minute),
	)
	if err != nil || len(transitions) != 0 {
		t.Fatalf("duplicate transitions = %#v, err = %v", transitions, err)
	}
	outcome.Marker = domain.TurnOutcomeContinue
	if _, err := PlanTurnOutcomes(
		[]domain.Attempt{attempt}, nil, []domain.TurnOutcome{outcome}, turnOutcomeTestTime.Add(time.Minute),
	); err == nil {
		t.Fatal("changed duplicate marker succeeded")
	}
}

func TestPlanTurnOutcomesIsDeterministicForReorderedInputs(t *testing.T) {
	attempts := []domain.Attempt{turnOutcomeAttempt("z"), turnOutcomeAttempt("a")}
	records := []domain.ThrottleAttemptRecord{
		turnOutcomeThrottle("z", "directive-z", domain.ThrottleCommandDrain, domain.ControlDraining),
		turnOutcomeThrottle("a", "directive-a", domain.ThrottleCommandDrain, domain.ControlDraining),
	}
	outcomes := []domain.TurnOutcome{
		turnOutcome("outcome-z", "z", domain.TurnOutcomeContinue),
		turnOutcome("outcome-a", "a", domain.TurnOutcomeMissing),
	}
	forward, err := PlanTurnOutcomes(attempts, records, outcomes, turnOutcomeTestTime)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(attempts)
	slices.Reverse(records)
	slices.Reverse(outcomes)
	reverse, err := PlanTurnOutcomes(attempts, records, outcomes, turnOutcomeTestTime)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(forward, reverse) {
		t.Fatalf("reordered mismatch:\nforward %#v\nreverse %#v", forward, reverse)
	}
}

type turnOutcomeStoreFake struct {
	attempts []domain.Attempt
	throttle []domain.ThrottleAttemptRecord
	commits  [][]domain.TurnOutcomeTransition
	fail     error
}

func (s *turnOutcomeStoreFake) LoadTurnOutcomeState(context.Context) (
	[]domain.Attempt, []domain.ThrottleAttemptRecord, error,
) {
	return append([]domain.Attempt(nil), s.attempts...),
		append([]domain.ThrottleAttemptRecord(nil), s.throttle...), nil
}

func (s *turnOutcomeStoreFake) CommitTurnOutcomeTransitions(
	_ context.Context, transitions []domain.TurnOutcomeTransition,
) error {
	if s.fail != nil {
		return s.fail
	}
	s.commits = append(s.commits, append([]domain.TurnOutcomeTransition(nil), transitions...))
	for _, transition := range transitions {
		for index := range s.attempts {
			if s.attempts[index].ID == transition.Attempt.ID {
				if s.attempts[index].Revision != transition.ExpectedAttemptRevision {
					return errors.New("stale attempt revision")
				}
				s.attempts[index] = transition.Attempt
			}
		}
		if transition.Throttle != nil {
			for index := range s.throttle {
				if s.throttle[index].DirectiveID == transition.Throttle.Record.DirectiveID &&
					s.throttle[index].AttemptID == transition.Throttle.Record.AttemptID {
					s.throttle[index] = transition.Throttle.Record
				}
			}
		}
	}
	return nil
}

func TestReconcileTurnOutcomesPersistsCanonicalAndThrottleTogether(t *testing.T) {
	store := &turnOutcomeStoreFake{
		attempts: []domain.Attempt{turnOutcomeAttempt("attempt")},
		throttle: []domain.ThrottleAttemptRecord{
			turnOutcomeThrottle("attempt", "directive", domain.ThrottleCommandResume, domain.ControlResuming),
		},
	}
	outcome := turnOutcome("outcome", "attempt", domain.TurnOutcomeDone)
	outcome.VerificationPassed = true
	transitions, err := ReconcileTurnOutcomes(
		context.Background(), store, []domain.TurnOutcome{outcome}, turnOutcomeTestTime,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(transitions) != 1 || len(store.commits) != 1 ||
		store.attempts[0].Progress != domain.ProgressSucceeded ||
		store.throttle[0].Delivery != domain.ThrottleDeliveryCancelled {
		t.Fatalf("state = %#v %#v, transitions = %#v", store.attempts, store.throttle, transitions)
	}
	second, err := ReconcileTurnOutcomes(
		context.Background(), store, []domain.TurnOutcome{outcome}, turnOutcomeTestTime.Add(time.Minute),
	)
	if err != nil || len(second) != 0 || len(store.commits) != 1 {
		t.Fatalf("duplicate = %#v commits = %d err = %v", second, len(store.commits), err)
	}
}

func turnOutcomeAttempt(id string) domain.Attempt {
	return domain.Attempt{
		ID: id, WorkflowRunID: "run", TaskID: "task-" + id, Number: 1,
		Progress: domain.ProgressActive, Control: domain.ControlRunning,
		Revision: 3, UpdatedAt: turnOutcomeTestTime.Add(-time.Minute),
	}
}

func turnOutcome(id, attemptID string, marker domain.TurnOutcomeMarker) domain.TurnOutcome {
	return domain.TurnOutcome{
		ID: id, AttemptID: attemptID, Marker: marker,
		ObservedAt: turnOutcomeTestTime.Add(-time.Second),
	}
}

func turnOutcomeThrottle(
	attemptID, directiveID string,
	kind domain.ThrottleCommandKind,
	control domain.ControlState,
) domain.ThrottleAttemptRecord {
	command := domain.ThrottleCommand{
		ID: "command-" + directiveID, DirectiveID: directiveID, AttemptID: attemptID,
		AssignmentID: "assignment-" + attemptID, AssignmentEpoch: 4,
		WorkerID: "normandy", ThreadID: "thread-" + attemptID,
		WorkspacePath: "/runs/" + attemptID,
		Route: domain.ProviderRoute{
			WorkerID: "normandy", ProviderInstanceID: "codex",
			Model: "gpt-5.6-sol", QuotaPoolID: "pool",
		},
		Kind: kind, QuotaPoolID: "pool",
		CreatedAt: turnOutcomeTestTime.Add(-2 * time.Minute),
	}
	return domain.ThrottleAttemptRecord{
		DirectiveID: directiveID, AttemptID: attemptID, Revision: 2,
		Command: command, Delivery: domain.ThrottleDeliveryPending,
		Control: control, UpdatedAt: turnOutcomeTestTime.Add(-time.Minute),
	}
}
