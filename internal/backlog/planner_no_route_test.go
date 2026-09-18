package backlog

import (
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// TestPlannerConsidersARouteLessTaskWithANilRoute documents U-2 as the planner
// sees it: a task that declares no route becomes a candidate on every eligible
// worker with a nil route and no blocker naming the missing route. Whether the
// candidate is then selected depends on the rest of the plan input, but a
// selected one makes PlanAndCommit fail the whole planning report ("planner
// proposed attempt ... without a provider route") and the quota constraint
// dereference candidate.Route, which is why intake and the readiness check now
// refuse a route-less task before it can reach here.
//
// This is a guard, not a regression test: it passes on the base too, because
// the planner is deliberately left alone. The regression tests for the refusal
// are TestViabilityRefusesATaskWithNoRoute, the two cmd tests and the legacy
// one; this test states the hazard those refusals exist for.
func TestPlannerConsidersARouteLessTaskWithANilRoute(t *testing.T) {
	now := plannerTestTime
	task := testTask("alpha")
	task.Class = domain.TaskClassRequired
	state := testDAGState(task)
	state.Attempts[0].Progress = domain.ProgressReady
	workflow := domain.Workflow{ID: "workflow", Project: "project", TaskIDs: []string{task.ID}}
	worker := routingWorker("worker-a", routingProvider("codex", "pool", true, "gpt"))
	snapshot := domain.WorkerSnapshot{
		WorkerID: "worker-a", WorkerEpoch: "worker-epoch", CoordinatorEpoch: 4,
		Sequence: 2, Connected: true, Inventory: worker,
		ObservedAt: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour),
	}
	window := quotaTestWindow()
	window.ObservedAt = now.Add(-time.Minute)
	planInput, err := BuildCoordinatorPlanInput(CoordinatorPlanningStateInput{
		Now: now, CoordinatorEpoch: 4,
		Workflows: []domain.Workflow{workflow}, WorkflowRuns: []domain.WorkflowRun{state.Run},
		Tasks: []domain.Task{task}, Attempts: append([]domain.Attempt(nil), state.Attempts...),
		WorkerSnapshots:      []domain.WorkerSnapshot{snapshot},
		QuotaPools:           []domain.QuotaPool{routingPool("pool", 2, 0, "codex")},
		QuotaWindows:         []QuotaWindowBudget{window},
		MaxWorkerSnapshotAge: time.Hour, MaxQuotaObservationAge: 5 * time.Minute,
		DeadlineRiskWindow: time.Hour, CheckpointMargin: 7 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlan(planInput)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Decisions) != 1 {
		t.Fatalf("decisions = %+v, want one for the route-less task", plan.Decisions)
	}
	decision := plan.Decisions[0]
	if len(decision.Candidates) != 1 {
		t.Fatalf("candidates = %+v, want the one eligible worker", decision.Candidates)
	}
	candidate := decision.Candidates[0]
	if candidate.WorkerID != "worker-a" {
		t.Fatalf("candidate worker = %q, want worker-a", candidate.WorkerID)
	}
	if candidate.Route != nil {
		t.Fatalf("the planner invented route %+v for a task that declared none", candidate.Route)
	}
	for _, blocker := range candidate.Blockers {
		if strings.Contains(blocker.Code, "route") {
			t.Fatalf("the planner now blocks a route-less candidate with %+v: "+
				"U-2 changed below the coordinator and this guard must be revisited", blocker)
		}
	}
}
