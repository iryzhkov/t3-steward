package backlog

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"time"
)

// refreshVerdictBranches settles only unstarted declared branches. Failed reviews
// are failures, never repair verdicts. Iteration handles any task ordering.
func (e *DAGExecution) refreshVerdictBranches(now time.Time) {
	for changed := true; changed; {
		changed = false
		for _, task := range e.state.Tasks {
			attempt := &e.state.Attempts[e.currentAttemptIndex(task.ID)]
			if attempt.AssignmentID != "" || (attempt.Progress != domain.ProgressBlocked && attempt.Progress != domain.ProgressReady) {
				continue
			}
			reason := ""
			for _, need := range task.Needs {
				producer := e.state.Tasks[e.taskByName[need]]
				prior := e.state.Attempts[e.currentAttemptIndex(producer.ID)]
				expected, conditional := task.NeedsVerdict[need]
				if prior.Progress == domain.ProgressSkipped && prior.Failure == domain.VerdictBranchSkipped {
					reason = domain.VerdictBranchSkipped
				} else if conditional && prior.Progress == domain.ProgressSucceeded && prior.ReviewVerdict != nil && prior.ReviewVerdict.Verdict != expected {
					reason = domain.VerdictBranchSkipped
				} else if task.FixLoop != nil && prior.Progress.Terminal() && prior.Progress != domain.ProgressSucceeded {
					reason = "fix loop stopped by unsuccessful dependency"
				}
			}
			if reason != "" {
				attempt.Progress, attempt.Control = domain.ProgressSkipped, domain.ControlStopped
				attempt.Failure = reason
				if !now.IsZero() {
					attempt.UpdatedAt, attempt.CompletedAt = now, timePointer(now)
				}
				changed = true
			}
		}
	}
}

func (e *DAGExecution) verdictSatisfied(task domain.Task, dependency domain.Task) bool {
	expected, conditional := task.NeedsVerdict[dependency.Name]
	if !conditional {
		return true
	}
	a := e.state.Attempts[e.currentAttemptIndex(dependency.ID)]
	return a.Progress == domain.ProgressSucceeded && a.ReviewVerdict != nil && a.ReviewVerdict.Verdict == expected
}
