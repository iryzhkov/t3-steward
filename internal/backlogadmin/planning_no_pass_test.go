package backlogadmin

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"strings"
	"testing"
)

func TestPlanningExplainNoPassDoesNotClaimEligibility(t *testing.T) {
	v := newView(sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "wf"}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run", WorkflowID: "wf"}},
		Tasks:        []domain.Task{{ID: "task", WorkflowID: "wf"}},
		Attempts:     []domain.Attempt{{ID: "attempt", WorkflowRunID: "run", TaskID: "task", Progress: domain.ProgressReady, Control: domain.ControlUnassigned}},
	}, nil, nil, RuntimeInfo{}, adminTestNow)
	got, _ := v.explanation("run", "task")
	if got.Eligible || !strings.Contains(got.Summary, "no planning pass") || strings.Contains(got.Summary, "eligible to start") {
		t.Fatalf("no-pass explanation claims eligibility: %+v", got)
	}
}
