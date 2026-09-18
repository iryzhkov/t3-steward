package backlogadmin

import "github.com/iryzhkov/t3-steward/internal/domain"

// attemptEvidence assembles the worker's last report on an attempt from the
// worker snapshots the coordinator holds. The attempt's own thread id is the
// fallback when the worker has not reported the assignment.
func (v view) attemptEvidence(attempt domain.Attempt, assignment *domain.Assignment) *AttemptEvidence {
	evidence := AttemptEvidence{ThreadID: attempt.ThreadID}
	if assignment != nil {
		evidence.WorkerID = assignment.WorkerID
		evidence.ThreadID = firstNonEmpty(assignment.ThreadID, attempt.ThreadID)
		for _, worker := range v.workers {
			if worker.WorkerID != assignment.WorkerID {
				continue
			}
			for _, observed := range worker.Assignments {
				if observed.AssignmentID != assignment.ID || observed.AssignmentEpoch != assignment.Epoch {
					continue
				}
				evidence.Control = observed.Control
				evidence.ObservedAt = observed.ObservedAt
				evidence.ThreadID = firstNonEmpty(observed.ThreadID, evidence.ThreadID)
				if observed.Journal != nil {
					evidence.Phase = observed.Journal.Phase
					evidence.ThreadState = observed.Journal.ThreadState
					evidence.PauseReason = observed.Journal.PauseReason
					evidence.Failure = observed.Journal.Failure
				}
			}
		}
	}
	if evidence == (AttemptEvidence{}) {
		return nil
	}
	return &evidence
}
