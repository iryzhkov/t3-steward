package backlogadmin

import (
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestExplanationNamesCurrentActivationDispatchFailureAndAction(t *testing.T) {
	records := sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow", TaskIDs: []string{"supervision"}}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run", WorkflowID: "workflow"}},
		Tasks:        []domain.Task{{ID: "supervision", WorkflowID: "workflow", Name: "supervision"}},
		Attempts:     []domain.Attempt{{ID: "attempt", WorkflowRunID: "run", TaskID: "supervision", Progress: domain.ProgressReady, Control: domain.ControlUnassigned}},
	}
	v := newView(records, nil, nil, RuntimeInfo{}, time.Now())
	v.dispatchFailures = map[string][]domain.ActivationDispatchFailure{"run": {{AttemptID: "attempt", SafeMessage: "immutable activation evidence is unavailable", NextAction: "t3-steward campaign supervision reassess run --request-id KEY --reason TEXT"}}}
	explanation, ok := v.explanation("run", "supervision")
	if !ok {
		t.Fatal("activation attempt not explained")
	}
	found := false
	for _, blocker := range explanation.Blockers {
		if blocker.Code == "activation-dispatch-failure" && strings.Contains(blocker.Detail, "reassess run") {
			found = true
		}
	}
	if !found {
		t.Fatalf("dispatch failure absent from explanation: %+v", explanation)
	}
}
