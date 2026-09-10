package backlog

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// DispatchThreadState is a worker's authoritative observation of one
// deterministic T3 thread ID.
type DispatchThreadState string

const (
	DispatchThreadMissing DispatchThreadState = "missing"
	DispatchThreadCreated DispatchThreadState = "created"
	DispatchThreadActive  DispatchThreadState = "active"
	DispatchThreadStopped DispatchThreadState = "stopped"
)

// ErrDispatchUncertain means the durable assignment must remain owned by its
// current worker because T3 execution could not be disproved.
var ErrDispatchUncertain = errors.New("assignment dispatch is uncertain")

// DispatchCommand contains the immutable identity a worker must use for T3
// thread creation. Worker-specific adapters may resolve the remaining prepared
// attempt inputs by AssignmentID.
type DispatchCommand struct {
	AssignmentID  string
	AttemptID     string
	WorkerID      string
	ThreadID      string
	DispatchToken string
}

// AssignmentDispatchStore persists dispatch identity and every reconciliation
// result before the coordinator makes another outward decision.
type AssignmentDispatchStore interface {
	PrepareAssignmentDispatch(context.Context, domain.Assignment) (domain.Assignment, error)
	CommitAssignmentDispatch(context.Context, domain.AssignmentDispatchTransition) error
}

// AssignmentDispatchWorker is the transport-neutral worker seam. ObserveThread
// must distinguish an authoritative missing thread from an unavailable worker.
type AssignmentDispatchWorker interface {
	ObserveThread(context.Context, DispatchCommand) (DispatchThreadState, error)
	CreateThread(context.Context, DispatchCommand) error
}

// ReconcileAssignmentDispatch ensures one assignment uses one persisted thread
// ID and dispatch token. An uncertain result retains ownership and is safe to
// retry after restart with the same input.
func ReconcileAssignmentDispatch(
	ctx context.Context,
	store AssignmentDispatchStore,
	worker AssignmentDispatchWorker,
	input domain.Assignment,
	now time.Time,
) (domain.Assignment, error) {
	if store == nil {
		return domain.Assignment{}, fmt.Errorf("assignment dispatch store is required")
	}
	if worker == nil {
		return domain.Assignment{}, fmt.Errorf("assignment dispatch worker is required")
	}
	if now.IsZero() {
		return domain.Assignment{}, fmt.Errorf("assignment dispatch reconciliation time must be set")
	}
	current, err := store.PrepareAssignmentDispatch(ctx, input)
	if err != nil {
		return domain.Assignment{}, fmt.Errorf("prepare assignment dispatch: %w", err)
	}
	if current.DispatchState == domain.DispatchStopped {
		return current, nil
	}

	command := dispatchCommand(current)
	observation, observeErr := worker.ObserveThread(ctx, command)
	if observeErr != nil {
		return markDispatchUnknown(ctx, store, current, now, observeErr)
	}
	switch observation {
	case DispatchThreadActive:
		return markDispatchConfirmed(ctx, store, current, now)
	case DispatchThreadStopped:
		return markDispatchStopped(ctx, store, current, now)
	case DispatchThreadMissing, DispatchThreadCreated:
		if current.DispatchConfirmedAt != nil {
			return markDispatchStopped(ctx, store, current, now)
		}
	default:
		return markDispatchUnknown(ctx, store, current, now,
			fmt.Errorf("worker returned invalid thread observation %q", observation))
	}

	creatingInput := current
	creatingInput.State = domain.AssignmentClaimed
	creating, err := advanceDispatch(ctx, store, creatingInput, domain.DispatchCreating, now, "")
	if err != nil {
		return domain.Assignment{}, err
	}
	if err := worker.CreateThread(ctx, command); err == nil {
		return markDispatchConfirmed(ctx, store, creating, now)
	} else {
		createErr := err
		observation, observeErr = worker.ObserveThread(ctx, command)
		if observeErr == nil {
			switch observation {
			case DispatchThreadActive:
				return markDispatchConfirmed(ctx, store, creating, now)
			case DispatchThreadStopped:
				return markDispatchStopped(ctx, store, creating, now)
			}
		}
		cause := fmt.Errorf("create thread response was not confirmed: %w", createErr)
		if observeErr != nil {
			cause = fmt.Errorf("%v; reconcile thread: %w", cause, observeErr)
		}
		return markDispatchUnknown(ctx, store, creating, now, cause)
	}
}

func dispatchCommand(assignment domain.Assignment) DispatchCommand {
	return DispatchCommand{
		AssignmentID:  assignment.ID,
		AttemptID:     assignment.AttemptID,
		WorkerID:      assignment.WorkerID,
		ThreadID:      assignment.ThreadID,
		DispatchToken: assignment.DispatchToken,
	}
}

func markDispatchConfirmed(
	ctx context.Context,
	store AssignmentDispatchStore,
	current domain.Assignment,
	now time.Time,
) (domain.Assignment, error) {
	if current.DispatchState == domain.DispatchConfirmed {
		return current, nil
	}
	next := current
	next.State = domain.AssignmentClaimed
	if next.DispatchConfirmedAt == nil {
		confirmedAt := now
		next.DispatchConfirmedAt = &confirmedAt
	}
	return advanceDispatch(ctx, store, next, domain.DispatchConfirmed, now, "")
}

func markDispatchStopped(
	ctx context.Context,
	store AssignmentDispatchStore,
	current domain.Assignment,
	now time.Time,
) (domain.Assignment, error) {
	next := current
	next.State = domain.AssignmentReleased
	return advanceDispatch(ctx, store, next, domain.DispatchStopped, now, "")
}

func markDispatchUnknown(
	ctx context.Context,
	store AssignmentDispatchStore,
	current domain.Assignment,
	now time.Time,
	cause error,
) (domain.Assignment, error) {
	next := current
	next.State = domain.AssignmentUnknown
	next, err := advanceDispatch(ctx, store, next, domain.DispatchUnknown, now, cause.Error())
	if err != nil {
		return domain.Assignment{}, err
	}
	return next, fmt.Errorf("%w for assignment %q: %v", ErrDispatchUncertain, next.ID, cause)
}

func advanceDispatch(
	ctx context.Context,
	store AssignmentDispatchStore,
	current domain.Assignment,
	state domain.DispatchState,
	now time.Time,
	dispatchError string,
) (domain.Assignment, error) {
	next := current
	next.DispatchState = state
	next.DispatchRevision = current.DispatchRevision + 1
	next.DispatchError = dispatchError
	next.UpdatedAt = now
	transition := domain.AssignmentDispatchTransition{
		ExpectedRevision: current.DispatchRevision,
		Assignment:       next,
	}
	if err := store.CommitAssignmentDispatch(ctx, transition); err != nil {
		return domain.Assignment{}, fmt.Errorf("commit assignment dispatch %q as %q: %w", current.ID, state, err)
	}
	return next, nil
}
