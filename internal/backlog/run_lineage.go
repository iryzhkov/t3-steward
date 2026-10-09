package backlog

import (
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// ResolveRunLineage turns a submitting task's claim about itself into the
// lineage a new run records.
//
// A claim naming an attempt the coordinator does not have, or naming it under
// another run or task, is refused: it is either a forgery or a corrupted
// identity file, and recording it would hand planning priority to whoever made
// it up. A claim naming a real attempt whose turn has ended records nothing:
// the submission is still accepted, as root work, because the identity file it
// came from can outlive its task and an operator shell may be standing in that
// workspace.
func ResolveRunLineage(records sqlite.CoordinatorRecords, parent domain.SubmissionParent, now time.Time) (*domain.RunLineage, error) {
	if err := parent.Validate(); err != nil {
		return nil, err
	}
	var attempt domain.Attempt
	found := false
	for _, candidate := range records.Attempts {
		if candidate.ID == parent.AttemptID {
			attempt, found = candidate, true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("submission names parent attempt %q, which this coordinator does not have", parent.AttemptID)
	}
	if attempt.WorkflowRunID != parent.RunID || attempt.TaskID != parent.TaskID {
		return nil, fmt.Errorf("submission names parent attempt %q under run %q task %q, but it belongs to run %q task %q",
			parent.AttemptID, parent.RunID, parent.TaskID, attempt.WorkflowRunID, attempt.TaskID)
	}
	if !attempt.TurnLive() {
		return nil, nil
	}
	started := attempt.UpdatedAt
	if attempt.StartedAt != nil && !attempt.StartedAt.IsZero() {
		started = *attempt.StartedAt
	} else {
		for _, assignment := range records.Assignments {
			if assignment.ID == attempt.AssignmentID && assignment.AttemptID == attempt.ID && !assignment.CreatedAt.IsZero() {
				started = assignment.CreatedAt
				break
			}
		}
	}
	if started.IsZero() || started.After(now) {
		started = now
	}
	return &domain.RunLineage{
		ParentRunID: parent.RunID, ParentTaskID: parent.TaskID, ParentAttemptID: parent.AttemptID,
		ParentStartedAt: started.UTC(), RecordedAt: now.UTC(),
	}, nil
}

// nestedReadySince is the ready time planning gives an attempt of a nested run:
// no later than its parent started, while the parent's turn is still live. A
// child therefore queues ahead of root work that became ready after its parent
// started, which is the work that could otherwise take the capacity the parked
// parent is waiting on. A child whose parent has finished is ordinary work.
func nestedReadySince(run domain.WorkflowRun, readySince time.Time, attempts map[string]domain.Attempt) (time.Time, string) {
	if run.Lineage == nil {
		return readySince, ""
	}
	parent, found := attempts[run.Lineage.ParentAttemptID]
	if !found || !parent.TurnLive() {
		return readySince, ""
	}
	if started := run.Lineage.ParentStartedAt; !started.IsZero() && started.Before(readySince) {
		return started, parent.ID
	}
	return readySince, parent.ID
}
