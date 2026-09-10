package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCommitTurnOutcomeTransitionsAtomicReplayAndStaleRevision(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, time.September, 11, 1, 30, 0, 0, time.UTC)
	attemptA := turnOutcomeStoreAttempt("attempt-a", 2, now)
	attemptZ := turnOutcomeStoreAttempt("attempt-z", 4, now)
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{
		Attempts: []domain.Attempt{attemptZ, attemptA},
	}); err != nil {
		t.Fatal(err)
	}
	throttleA := throttleAttemptStoreTransition("directive-a", attemptA.ID, 0, domain.ThrottleDeliveryPending)
	throttleZ := throttleAttemptStoreTransition("directive-z", attemptZ.ID, 0, domain.ThrottleDeliveryPending)
	if err := store.CommitThrottleAttemptTransitions(
		context.Background(), []domain.ThrottleAttemptTransition{throttleZ, throttleA},
	); err != nil {
		t.Fatal(err)
	}

	doneA := turnOutcomeStoreTransition(attemptA, throttleA.Record, "outcome-a", now.Add(time.Minute))
	if err := store.CommitTurnOutcomeTransitions(
		context.Background(), []domain.TurnOutcomeTransition{doneA},
	); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitTurnOutcomeTransitions(
		context.Background(), []domain.TurnOutcomeTransition{doneA},
	); err != nil {
		t.Fatalf("exact replay: %v", err)
	}

	attempts, throttle, err := store.LoadTurnOutcomeState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 || attempts[0].ID != attemptA.ID ||
		attempts[0].Progress != domain.ProgressSucceeded ||
		attempts[0].Revision != attemptA.Revision+1 {
		t.Fatalf("attempts = %#v", attempts)
	}
	if len(throttle) != 2 || throttle[0].AttemptID != attemptA.ID ||
		throttle[0].Delivery != domain.ThrottleDeliveryCancelled ||
		throttle[0].Revision != throttleA.Record.Revision+1 {
		t.Fatalf("throttle = %#v", throttle)
	}

	staleA := doneA
	staleA.OutcomeID = "outcome-stale"
	staleA.Attempt.LastTurnOutcomeID = staleA.OutcomeID
	staleA.Attempt.Failure = "different stale payload"
	freshZ := turnOutcomeStoreTransition(attemptZ, throttleZ.Record, "outcome-z", now.Add(2*time.Minute))
	err = store.CommitTurnOutcomeTransitions(
		context.Background(), []domain.TurnOutcomeTransition{freshZ, staleA},
	)
	if !errors.Is(err, ErrStaleTurnOutcomeAttemptRevision) {
		t.Fatalf("stale error = %v", err)
	}
	afterAttempts, afterThrottle, err := store.LoadTurnOutcomeState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterAttempts, attempts) || !reflect.DeepEqual(afterThrottle, throttle) {
		t.Fatalf("stale batch persisted partially:\nattempts %#v\nthrottle %#v",
			afterAttempts, afterThrottle)
	}
}

func turnOutcomeStoreAttempt(id string, revision int64, now time.Time) domain.Attempt {
	return domain.Attempt{
		ID: id, WorkflowRunID: "run", TaskID: "task-" + id, Number: 1,
		Progress: domain.ProgressActive, Control: domain.ControlRunning,
		Revision: revision, UpdatedAt: now,
	}
}

func turnOutcomeStoreTransition(
	attempt domain.Attempt,
	throttle domain.ThrottleAttemptRecord,
	outcomeID string,
	now time.Time,
) domain.TurnOutcomeTransition {
	attempt.Revision++
	attempt.Progress = domain.ProgressSucceeded
	attempt.Control = domain.ControlStopped
	attempt.LastTurnOutcomeID = outcomeID
	attempt.LastTurnOutcomeMarker = domain.TurnOutcomeDone
	attempt.UpdatedAt = now
	attempt.CompletedAt = &now

	expectedThrottleRevision := throttle.Revision
	throttle.Revision++
	throttle.Delivery = domain.ThrottleDeliveryCancelled
	throttle.Control = domain.ControlStopped
	throttle.UpdatedAt = now
	return domain.TurnOutcomeTransition{
		OutcomeID:               outcomeID,
		ExpectedAttemptRevision: attempt.Revision - 1,
		Attempt:                 attempt,
		Throttle: &domain.ThrottleAttemptTransition{
			ExpectedRevision: expectedThrottleRevision,
			Record:           throttle,
		},
	}
}
