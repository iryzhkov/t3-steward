package backlog

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// TurnOutcomeStore loads a consistent attempt/throttle snapshot and atomically
// persists each reconciled turn outcome.
type TurnOutcomeStore interface {
	LoadTurnOutcomeState(context.Context) ([]domain.Attempt, []domain.ThrottleAttemptRecord, error)
	CommitTurnOutcomeTransitions(context.Context, []domain.TurnOutcomeTransition) error
}

// ReconcileTurnOutcomes plans and atomically persists finished-turn outcomes.
func ReconcileTurnOutcomes(
	ctx context.Context,
	store TurnOutcomeStore,
	outcomes []domain.TurnOutcome,
	now time.Time,
) ([]domain.TurnOutcomeTransition, error) {
	if store == nil {
		return nil, fmt.Errorf("turn outcome store is required")
	}
	attempts, throttleRecords, err := store.LoadTurnOutcomeState(ctx)
	if err != nil {
		return nil, fmt.Errorf("load turn outcome state: %w", err)
	}
	transitions, err := PlanTurnOutcomes(attempts, throttleRecords, outcomes, now)
	if err != nil {
		return nil, err
	}
	if len(transitions) == 0 {
		return nil, nil
	}
	if err := store.CommitTurnOutcomeTransitions(ctx, transitions); err != nil {
		return nil, fmt.Errorf("commit turn outcomes: %w", err)
	}
	return transitions, nil
}

// PlanTurnOutcomes resolves completion markers against active throttle intent.
// Explicit done wins. Otherwise drain, hard-stop, and pending resume control
// override ordinary continue or missing status.
func PlanTurnOutcomes(
	attempts []domain.Attempt,
	throttleRecords []domain.ThrottleAttemptRecord,
	outcomes []domain.TurnOutcome,
	now time.Time,
) ([]domain.TurnOutcomeTransition, error) {
	if now.IsZero() {
		return nil, fmt.Errorf("turn outcome reconciliation time must be set")
	}
	attemptByID := make(map[string]domain.Attempt, len(attempts))
	for _, attempt := range attempts {
		if attempt.ID == "" || attempt.Revision < 0 {
			return nil, fmt.Errorf("turn outcome attempt identity and revision are invalid")
		}
		if _, exists := attemptByID[attempt.ID]; exists {
			return nil, fmt.Errorf("turn outcome attempts repeat %q", attempt.ID)
		}
		attemptByID[attempt.ID] = attempt
	}
	latestThrottle, err := latestThrottleRecordsByAttempt(throttleRecords)
	if err != nil {
		return nil, err
	}
	sortedOutcomes := append([]domain.TurnOutcome(nil), outcomes...)
	sort.Slice(sortedOutcomes, func(i, j int) bool {
		if sortedOutcomes[i].AttemptID != sortedOutcomes[j].AttemptID {
			return sortedOutcomes[i].AttemptID < sortedOutcomes[j].AttemptID
		}
		return sortedOutcomes[i].ID < sortedOutcomes[j].ID
	})

	seenAttempts := make(map[string]struct{}, len(sortedOutcomes))
	var transitions []domain.TurnOutcomeTransition
	for _, outcome := range sortedOutcomes {
		if err := validateTurnOutcome(outcome); err != nil {
			return nil, err
		}
		if _, exists := seenAttempts[outcome.AttemptID]; exists {
			return nil, fmt.Errorf("turn outcomes repeat attempt %q", outcome.AttemptID)
		}
		seenAttempts[outcome.AttemptID] = struct{}{}
		attempt, exists := attemptByID[outcome.AttemptID]
		if !exists {
			return nil, fmt.Errorf("turn outcome %q names unknown attempt %q", outcome.ID, outcome.AttemptID)
		}
		if attempt.LastTurnOutcomeID == outcome.ID {
			if attempt.LastTurnOutcomeMarker != outcome.Marker {
				return nil, fmt.Errorf("turn outcome %q marker changed from %q to %q",
					outcome.ID, attempt.LastTurnOutcomeMarker, outcome.Marker)
			}
			continue
		}
		if attempt.Progress.Terminal() {
			continue
		}

		throttle, throttled := latestThrottle[outcome.AttemptID]
		throttled = throttled && activeTurnThrottle(throttle)
		if outcome.Marker != domain.TurnOutcomeDone && !throttled {
			continue
		}

		transition := domain.TurnOutcomeTransition{
			OutcomeID:               outcome.ID,
			ExpectedAttemptRevision: attempt.Revision,
		}
		attempt.Revision++
		attempt.LastTurnOutcomeID = outcome.ID
		attempt.LastTurnOutcomeMarker = outcome.Marker
		attempt.UpdatedAt = now
		if outcome.Marker == domain.TurnOutcomeDone {
			attempt.Control = domain.ControlStopped
			attempt.CompletedAt = turnOutcomeTime(outcome.ObservedAt)
			attempt.FinalSummaryArtifactID = outcome.FinalSummaryArtifactID
			if outcome.VerificationPassed {
				attempt.Progress = domain.ProgressSucceeded
				attempt.Failure = ""
			} else {
				attempt.Progress = domain.ProgressFailed
				attempt.Failure = outcome.Failure
				if attempt.Failure == "" {
					attempt.Failure = "verification failed"
				}
			}
		} else {
			attempt.Control = throttle.Control
		}
		transition.Attempt = attempt

		if throttled {
			expectedRevision := throttle.Revision
			throttle.Revision++
			throttle.UpdatedAt = now
			if outcome.Marker == domain.TurnOutcomeDone {
				throttle.Control = domain.ControlStopped
				if throttle.Delivery == domain.ThrottleDeliveryPending {
					throttle.Delivery = domain.ThrottleDeliveryCancelled
					throttle.Failure = "attempt completed before throttle command settled"
				}
			}
			transition.Throttle = &domain.ThrottleAttemptTransition{
				ExpectedRevision: expectedRevision,
				Record:           throttle,
			}
		}
		transitions = append(transitions, transition)
	}
	sort.Slice(transitions, func(i, j int) bool {
		return transitions[i].Attempt.ID < transitions[j].Attempt.ID
	})
	return transitions, nil
}

func latestThrottleRecordsByAttempt(
	records []domain.ThrottleAttemptRecord,
) (map[string]domain.ThrottleAttemptRecord, error) {
	latest := make(map[string]domain.ThrottleAttemptRecord)
	for _, record := range records {
		if err := validateThrottleAttemptRecord(record); err != nil {
			return nil, err
		}
		current, exists := latest[record.AttemptID]
		if !exists || record.UpdatedAt.After(current.UpdatedAt) ||
			(record.UpdatedAt.Equal(current.UpdatedAt) && record.DirectiveID > current.DirectiveID) {
			latest[record.AttemptID] = cloneThrottleAttemptRecord(record)
		}
	}
	return latest, nil
}

func activeTurnThrottle(record domain.ThrottleAttemptRecord) bool {
	if record.Delivery == domain.ThrottleDeliveryCancelled {
		return false
	}
	switch record.Command.Kind {
	case domain.ThrottleCommandDrain, domain.ThrottleCommandHardStop:
		return record.Control == domain.ControlDraining ||
			record.Control == domain.ControlPaused ||
			record.Control == domain.ControlPausedUncheckpointed
	case domain.ThrottleCommandResume:
		return record.Control == domain.ControlResuming
	default:
		return false
	}
}

func validateTurnOutcome(outcome domain.TurnOutcome) error {
	if outcome.ID == "" || outcome.AttemptID == "" || outcome.ObservedAt.IsZero() {
		return fmt.Errorf("turn outcome identity and observation time are required")
	}
	switch outcome.Marker {
	case domain.TurnOutcomeDone:
		return nil
	case domain.TurnOutcomeContinue, domain.TurnOutcomeMissing:
		if outcome.VerificationPassed || outcome.Failure != "" || outcome.FinalSummaryArtifactID != "" {
			return fmt.Errorf("nonterminal turn outcome %q carries terminal result data", outcome.ID)
		}
		return nil
	default:
		return fmt.Errorf("turn outcome %q has invalid marker %q", outcome.ID, outcome.Marker)
	}
}

func turnOutcomeTime(value time.Time) *time.Time {
	cloned := value
	return &cloned
}
