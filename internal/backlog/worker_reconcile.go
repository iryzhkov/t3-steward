package backlog

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

const (
	workerStateObservedPresent   = "worker-observed-present"
	workerStateObservedStopped   = "worker-observed-stopped"
	workerStateObservedCompleted = "worker-observed-completed"
	workerStateObservedAbsent    = "worker-observed-absent"
	workerStateCommandRejected   = "worker-command-rejected"
	workerStateDispatchAccepted  = "dispatch-accepted"
	workerStateStopAccepted      = "stop-accepted"
	workerStateCollectAccepted   = "collect-accepted"
)

// PlanWorkerStateTransitions deterministically reconciles current worker evidence
// into assignment and attempt projections. Snapshot assignment lists are complete:
// omission is authoritative only for unknown assignments or after a worker epoch change.
func PlanWorkerStateTransitions(
	records sqlite.CoordinatorRecords,
	snapshot domain.WorkerSnapshot,
	commandRecords []domain.WorkerCommandRecord,
	now time.Time,
) ([]domain.WorkerStateTransition, error) {
	if snapshot.WorkerID == "" || snapshot.WorkerEpoch == "" ||
		snapshot.CoordinatorEpoch < 1 || snapshot.Sequence < 1 || now.IsZero() {
		return nil, errors.New("worker state reconciliation requires a complete snapshot and time")
	}
	if !snapshot.Connected || !snapshot.ValidUntil.After(now) {
		return nil, fmt.Errorf("worker %q snapshot is disconnected or stale", snapshot.WorkerID)
	}

	attempts := make(map[string]domain.Attempt, len(records.Attempts))
	for _, attempt := range records.Attempts {
		attempts[attempt.ID] = attempt
	}
	observations := make(map[string]domain.WorkerAssignmentObservation, len(snapshot.Assignments))
	for _, observation := range snapshot.Assignments {
		if observation.AssignmentID == "" || observation.AssignmentEpoch < 1 ||
			observation.ObservedAt.IsZero() || observation.ObservedAt.After(snapshot.ObservedAt) {
			return nil, fmt.Errorf("worker %q has invalid assignment observation", snapshot.WorkerID)
		}
		key := workerAssignmentKey(observation.AssignmentID, observation.AssignmentEpoch)
		if _, exists := observations[key]; exists {
			return nil, fmt.Errorf("worker %q repeats assignment observation %q", snapshot.WorkerID, key)
		}
		observations[key] = observation
	}
	commands := make(map[string]domain.WorkerCommandRecord, len(commandRecords))
	for _, record := range commandRecords {
		commands[workerCommandKey(record.Command.AssignmentID, record.Command.AssignmentEpoch, record.Command.Kind)] = record
	}

	assignments := append([]domain.Assignment(nil), records.Assignments...)
	sort.Slice(assignments, func(i, j int) bool { return assignments[i].ID < assignments[j].ID })
	var transitions []domain.WorkerStateTransition
	for _, assignment := range assignments {
		if assignment.WorkerID != snapshot.WorkerID ||
			(assignment.State != domain.AssignmentClaimed && assignment.State != domain.AssignmentUnknown) {
			continue
		}
		attempt, ok := attempts[assignment.AttemptID]
		if !ok {
			return nil, fmt.Errorf("assignment %q refers to unknown attempt %q", assignment.ID, assignment.AttemptID)
		}
		observation, observed := observations[workerAssignmentKey(assignment.ID, assignment.Epoch)]
		nextAssignment, nextAttempt, reason, changed, err := planWorkerStateTransition(
			assignment, attempt, observation, observed, snapshot, commands, now,
		)
		if err != nil {
			return nil, err
		}
		if !changed {
			continue
		}
		transitions = append(transitions, domain.WorkerStateTransition{
			CoordinatorEpoch:        snapshot.CoordinatorEpoch,
			WorkerID:                snapshot.WorkerID,
			WorkerEpoch:             snapshot.WorkerEpoch,
			WorkerSequence:          snapshot.Sequence,
			TransitionedAt:          now,
			ExpectedAssignment:      assignment,
			ExpectedAttemptRevision: attempt.Revision,
			Assignment:              nextAssignment,
			Attempt:                 nextAttempt,
			Reason:                  reason,
		})
	}
	return transitions, nil
}

func planWorkerStateTransition(
	assignment domain.Assignment,
	attempt domain.Attempt,
	observation domain.WorkerAssignmentObservation,
	observed bool,
	snapshot domain.WorkerSnapshot,
	commands map[string]domain.WorkerCommandRecord,
	now time.Time,
) (domain.Assignment, domain.Attempt, string, bool, error) {
	attemptFinished := attempt.Progress.Terminal() || attempt.CompletedAt != nil
	if observed {
		if observation.AssignmentID != assignment.ID || observation.AssignmentEpoch != assignment.Epoch {
			return assignment, attempt, "", false, fmt.Errorf("assignment observation identity mismatch for %q", assignment.ID)
		}
		switch observation.State {
		case domain.AssignmentClaimed:
			if assignment.State == domain.AssignmentUnknown && !assignment.LeaseExpiresAt.After(now) && !attemptFinished {
				return assignment, attempt, "", false, nil
			}
			control := observation.Control
			if control == "" {
				control = domain.ControlRunning
			}
			switch control {
			case domain.ControlPreparing, domain.ControlRunning, domain.ControlDraining,
				domain.ControlPaused, domain.ControlPausedUncheckpointed, domain.ControlResuming:
			default:
				return assignment, attempt, "", false, fmt.Errorf("assignment %q has invalid observed control %q", assignment.ID, control)
			}
			nextAssignment := assignment
			nextAssignment.State = domain.AssignmentClaimed
			nextAssignment.WorkerEpoch = snapshot.WorkerEpoch
			nextAssignment.ThreadID = observation.ThreadID
			nextAssignment.UpdatedAt = now
			nextAttempt := attempt
			nextAttempt.AssignmentID = assignment.ID
			reason := workerStateObservedPresent
			if attemptFinished {
				nextAttempt.Control = domain.ControlStopped
				reason = "terminal-attempt-stop-required"
			} else {
				nextAttempt.Progress = domain.ProgressActive
				nextAttempt.Control = control
			}
			if observation.ThreadID != "" {
				nextAttempt.ThreadID = observation.ThreadID
			}
			nextAttempt.UpdatedAt = now
			return finishWorkerStateTransition(assignment, attempt, nextAssignment, nextAttempt, reason)
		case domain.AssignmentReleased:
			return releasedWorkerState(assignment, attempt, now, workerStateObservedStopped)
		case domain.AssignmentCompleted:
			if acceptedWorkerCommand(commands, assignment, domain.WorkerCommandCollect) {
				return completedWorkerState(assignment, attempt, snapshot.WorkerEpoch, observation.ThreadID, now, workerStateCollectAccepted)
			}
			return observedCompletedWorkerState(assignment, attempt, snapshot, observation.ThreadID, now)
		default:
			return assignment, attempt, "", false, fmt.Errorf("assignment %q has invalid observed state %q", assignment.ID, observation.State)
		}
	}

	if acceptedWorkerCommand(commands, assignment, domain.WorkerCommandStop) {
		return releasedWorkerState(assignment, attempt, now, workerStateStopAccepted)
	}
	if acceptedWorkerCommand(commands, assignment, domain.WorkerCommandCollect) {
		return completedWorkerState(assignment, attempt, assignment.WorkerEpoch, assignment.ThreadID, now, workerStateCollectAccepted)
	}
	if rejectedWorkerCommand(commands, assignment, domain.WorkerCommandPrepare) ||
		rejectedWorkerCommand(commands, assignment, domain.WorkerCommandDispatch) {
		return releasedWorkerState(assignment, attempt, now, workerStateCommandRejected)
	}
	if attemptFinished && attempt.Control != domain.ControlStopped {
		nextAttempt := attempt
		nextAttempt.Control = domain.ControlStopped
		nextAttempt.UpdatedAt = now
		nextAssignment := assignment
		nextAssignment.UpdatedAt = now
		return finishWorkerStateTransition(assignment, attempt, nextAssignment, nextAttempt, "terminal-attempt-stop-required")
	}
	if acceptedWorkerCommand(commands, assignment, domain.WorkerCommandDispatch) &&
		assignment.State == domain.AssignmentClaimed {
		nextAttempt := attempt
		nextAttempt.Progress = domain.ProgressActive
		nextAttempt.Control = domain.ControlRunning
		nextAttempt.UpdatedAt = now
		nextAssignment := assignment
		nextAssignment.UpdatedAt = now
		return finishWorkerStateTransition(assignment, attempt, nextAssignment, nextAttempt, workerStateDispatchAccepted)
	}
	if assignment.State == domain.AssignmentUnknown || assignment.WorkerEpoch != snapshot.WorkerEpoch {
		return releasedWorkerState(assignment, attempt, now, workerStateObservedAbsent)
	}
	return assignment, attempt, "", false, nil
}
func releasedWorkerState(
	assignment domain.Assignment,
	attempt domain.Attempt,
	now time.Time,
	reason string,
) (domain.Assignment, domain.Attempt, string, bool, error) {
	nextAssignment := assignment
	nextAssignment.State = domain.AssignmentReleased
	nextAssignment.LeaseExpiresAt = time.Time{}
	nextAssignment.UpdatedAt = now
	nextAttempt := attempt
	if nextAttempt.Control != domain.ControlStopped {
		nextAttempt.Control = domain.ControlUnassigned
		if !nextAttempt.Progress.Terminal() {
			nextAttempt.Progress = domain.ProgressReady
		}
	}
	nextAttempt.AssignmentID = ""
	nextAttempt.UpdatedAt = now
	return finishWorkerStateTransition(assignment, attempt, nextAssignment, nextAttempt, reason)
}

func observedCompletedWorkerState(
	assignment domain.Assignment,
	attempt domain.Attempt,
	snapshot domain.WorkerSnapshot,
	threadID string,
	now time.Time,
) (domain.Assignment, domain.Attempt, string, bool, error) {
	nextAssignment := assignment
	nextAssignment.State = domain.AssignmentClaimed
	nextAssignment.WorkerEpoch = snapshot.WorkerEpoch
	nextAssignment.LeaseExpiresAt = snapshot.ValidUntil
	if threadID != "" {
		nextAssignment.ThreadID = threadID
	}
	nextAssignment.UpdatedAt = now
	nextAttempt := attempt
	if !nextAttempt.Progress.Terminal() && nextAttempt.CompletedAt == nil {
		nextAttempt.Progress = domain.ProgressActive
	}
	nextAttempt.Control = domain.ControlStopped
	if threadID != "" {
		nextAttempt.ThreadID = threadID
	}
	nextAttempt.UpdatedAt = now
	return finishWorkerStateTransition(assignment, attempt, nextAssignment, nextAttempt, workerStateObservedCompleted)
}
func completedWorkerState(
	assignment domain.Assignment,
	attempt domain.Attempt,
	workerEpoch, threadID string,
	now time.Time,
	reason string,
) (domain.Assignment, domain.Attempt, string, bool, error) {
	nextAssignment := assignment
	nextAssignment.State = domain.AssignmentCompleted
	nextAssignment.WorkerEpoch = workerEpoch
	nextAssignment.LeaseExpiresAt = time.Time{}
	if threadID != "" {
		nextAssignment.ThreadID = threadID
	}
	nextAssignment.UpdatedAt = now
	nextAttempt := attempt
	if !nextAttempt.Progress.Terminal() && nextAttempt.CompletedAt == nil {
		nextAttempt.Progress = domain.ProgressVerifying
	}
	nextAttempt.Control = domain.ControlStopped
	if threadID != "" {
		nextAttempt.ThreadID = threadID
	}
	nextAttempt.UpdatedAt = now
	return finishWorkerStateTransition(assignment, attempt, nextAssignment, nextAttempt, reason)
}
func finishWorkerStateTransition(
	previousAssignment domain.Assignment,
	previousAttempt domain.Attempt,
	assignment domain.Assignment,
	attempt domain.Attempt,
	reason string,
) (domain.Assignment, domain.Attempt, string, bool, error) {
	assignmentComparison := assignment
	assignmentComparison.UpdatedAt = previousAssignment.UpdatedAt
	attemptComparison := attempt
	attemptComparison.Revision = previousAttempt.Revision
	attemptComparison.UpdatedAt = previousAttempt.UpdatedAt
	if reflect.DeepEqual(previousAssignment, assignmentComparison) &&
		reflect.DeepEqual(previousAttempt, attemptComparison) {
		return previousAssignment, previousAttempt, "", false, nil
	}
	assignment.UpdatedAt = attempt.UpdatedAt
	attempt.Revision = previousAttempt.Revision + 1
	return assignment, attempt, reason, true, nil
}

func acceptedWorkerCommand(records map[string]domain.WorkerCommandRecord, assignment domain.Assignment, kind domain.WorkerCommandKind) bool {
	record, ok := records[workerCommandKey(assignment.ID, assignment.Epoch, kind)]
	return ok && record.Acknowledgement != nil && record.Acknowledgement.Accepted
}

func rejectedWorkerCommand(records map[string]domain.WorkerCommandRecord, assignment domain.Assignment, kind domain.WorkerCommandKind) bool {
	record, ok := records[workerCommandKey(assignment.ID, assignment.Epoch, kind)]
	return ok && record.Acknowledgement != nil && !record.Acknowledgement.Accepted
}

func workerAssignmentKey(assignmentID string, assignmentEpoch int64) string {
	return fmt.Sprintf("%s/%d", assignmentID, assignmentEpoch)
}
