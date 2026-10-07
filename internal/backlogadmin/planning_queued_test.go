package backlogadmin

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"strings"
	"testing"
	"time"
)

func TestPlanningExplainQueuedWithoutPassDoesNotClaimEligibility(t *testing.T) {
	v := newView(sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "wf"}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run", WorkflowID: "wf"}},
		Tasks:        []domain.Task{{ID: "task", WorkflowID: "wf"}},
		Attempts:     []domain.Attempt{{ID: "attempt", WorkflowRunID: "run", TaskID: "task", Progress: domain.ProgressQueued, Control: domain.ControlUnassigned}},
	}, []domain.WorkerSnapshot{{WorkerID: "worker", Connected: true, ObservedAt: adminTestNow, ValidUntil: adminTestNow.Add(time.Minute), Inventory: domain.WorkerInventory{ID: "worker", AcceptBacklog: true, Health: domain.WorkerHealthReady}}}, nil, RuntimeInfo{MaxWorkerSnapshotAge: time.Minute}, adminTestNow)
	got, _ := v.explanation("run", "task")
	if got.Eligible || strings.Contains(got.Summary, "eligible to start") || !strings.Contains(got.Summary, "no planning pass") {
		t.Fatalf("queued no-pass task: %+v", got)
	}
}
