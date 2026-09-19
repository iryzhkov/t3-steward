package wait

import (
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// D-5. The wake prose stated "Observed attempt %s, run revision %d" from an
// observation the sink branch never fills in, so the wake for the one target
// "task run" and "campaign submit --notify-thread" register on said "Observed
// attempt , run revision 0". A run sink has no attempt: it is the run's join
// point.
func TestNodeWakeProseOmitsTheAttemptClauseForASink(t *testing.T) {
	sink := domain.NodeWait{
		Request: domain.NodeWaitRequest{
			Name:   "run-1/sink",
			Target: domain.NodeRef{RunID: "run-1", TaskID: domain.SinkTaskName},
		},
		Observation: &domain.NodeObservation{
			Target:   domain.NodeRef{RunID: "run-1", TaskID: domain.SinkTaskName},
			Progress: domain.ProgressSucceeded, Reason: "the run ended succeeded",
		},
	}
	prose := nodeWakeProse(sink)
	if strings.Contains(prose, "Observed attempt") {
		t.Fatalf("the sink wake names an attempt it does not have:\n%s", prose)
	}
	if strings.Contains(prose, "run revision 0") {
		t.Fatalf("the sink wake states a run revision it does not have:\n%s", prose)
	}
	if !strings.Contains(prose, "the run ended succeeded") {
		t.Fatalf("the sink wake lost its reason:\n%s", prose)
	}

	// A task node does have an attempt, and still names it.
	task := sink
	task.Observation = &domain.NodeObservation{
		Target:    domain.NodeRef{RunID: "run-1", TaskID: "task-1"},
		AttemptID: "attempt-9", RunRevision: 12,
		Progress: domain.ProgressSucceeded, Reason: "succeeded",
	}
	if prose := nodeWakeProse(task); !strings.Contains(prose, "Observed attempt attempt-9, run revision 12.") {
		t.Fatalf("a task wake lost its attempt evidence:\n%s", prose)
	}
}
