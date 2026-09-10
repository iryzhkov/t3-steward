package backlog

import (
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var plannerTestTime = time.Date(2026, time.September, 10, 12, 0, 0, 0, time.UTC)

func TestBuildPlanDeterministicIndependentReadyBranchesAndPure(t *testing.T) {
	first := plannerInput(
		[]domain.Task{testTask("beta"), testTask("alpha")},
		[]domain.WorkerInventory{plannerWorker("worker-b"), plannerWorker("worker-a")},
	)
	second := plannerInput(
		[]domain.Task{testTask("alpha"), testTask("beta")},
		[]domain.WorkerInventory{plannerWorker("worker-a"), plannerWorker("worker-b")},
	)
	before := plannerInput(
		[]domain.Task{testTask("beta"), testTask("alpha")},
		[]domain.WorkerInventory{plannerWorker("worker-b"), plannerWorker("worker-a")},
	)

	got, err := BuildPlan(first)
	if err != nil {
		t.Fatalf("BuildPlan(first): %v", err)
	}
	reordered, err := BuildPlan(second)
	if err != nil {
		t.Fatalf("BuildPlan(second): %v", err)
	}
	if !reflect.DeepEqual(got, reordered) {
		t.Fatalf("plan depends on input order:\nfirst: %#v\nsecond: %#v", got, reordered)
	}
	if !reflect.DeepEqual(first, before) {
		t.Fatalf("BuildPlan mutated its input:\nbefore: %#v\nafter: %#v", before, first)
	}

	want := []ProposedTask{
		{WorkflowRunID: "run", TaskID: "task-alpha", AttemptID: "alpha-1", WorkerID: "worker-a"},
		{WorkflowRunID: "run", TaskID: "task-beta", AttemptID: "beta-1", WorkerID: "worker-a"},
	}
	if !reflect.DeepEqual(got.Proposals, want) {
		t.Fatalf("proposals = %#v, want %#v", got.Proposals, want)
	}
	for _, decision := range got.Decisions {
		if !decision.Proposed || decision.Progress != domain.ProgressReady {
			t.Fatalf("decision = %#v, want proposed ready task", decision)
		}
	}
}

func TestBuildPlanOrdersWorkflowRuns(t *testing.T) {
	runZ := plannerInput([]domain.Task{testTask("alpha")}, []domain.WorkerInventory{plannerWorker("worker-a")}).Workflows[0]
	runZ.State.Run.ID = "run-z"
	runZ.State.Attempts[0].ID = "run-z-alpha-1"
	runZ.State.Attempts[0].WorkflowRunID = "run-z"
	runA := plannerInput([]domain.Task{testTask("alpha")}, []domain.WorkerInventory{plannerWorker("worker-a")}).Workflows[0]
	runA.State.Run.ID = "run-a"
	runA.State.Attempts[0].ID = "run-a-alpha-1"
	runA.State.Attempts[0].WorkflowRunID = "run-a"

	input := plannerInput(nil, []domain.WorkerInventory{plannerWorker("worker-a")})
	input.Workflows = []PlanningWorkflow{runZ, runA}
	got, err := BuildPlan(input)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if len(got.Proposals) != 2 ||
		got.Proposals[0].WorkflowRunID != "run-a" ||
		got.Proposals[1].WorkflowRunID != "run-z" {
		t.Fatalf("proposals = %#v, want workflow-run order", got.Proposals)
	}
}

func TestBuildPlanExplainsDependencyAndPlacementBlockers(t *testing.T) {
	tests := []struct {
		name          string
		tasks         []domain.Task
		workers       []domain.WorkerInventory
		decisionTask  string
		wantCode      string
		wantDependsOn string
		wantExcluded  map[string]string
	}{
		{
			name:          "dependency",
			tasks:         []domain.Task{testTask("build"), testTask("test", "build")},
			workers:       []domain.WorkerInventory{plannerWorker("worker-a")},
			decisionTask:  "test",
			wantCode:      PlanningBlockerDependency,
			wantDependsOn: "build",
		},
		{
			name:  "offline and capability exclusions",
			tasks: []domain.Task{plannerTaskWithCapabilities("render", "gpu")},
			workers: []domain.WorkerInventory{
				plannerWorkerWithHealth("gpu-offline", domain.WorkerHealthOffline, "gpu"),
				plannerWorker("cpu-ready"),
			},
			decisionTask: "render",
			wantCode:     PlanningBlockerNoEligibleWorker,
			wantExcluded: map[string]string{
				"cpu-ready":   ExclusionMissingCapability,
				"gpu-offline": ExclusionWorkerHealth,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := BuildPlan(plannerInput(test.tasks, test.workers))
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}
			decision := plannerDecision(t, got, test.decisionTask)
			if !hasPlanningBlocker(decision.Blockers, test.wantCode, test.wantDependsOn) {
				t.Fatalf("blockers = %#v, want code %q dependency %q", decision.Blockers, test.wantCode, test.wantDependsOn)
			}
			for workerID, code := range test.wantExcluded {
				if !hasPlacementExclusion(decision.Placement, workerID, code) {
					t.Fatalf("placement = %#v, want worker %q exclusion %q", decision.Placement, workerID, code)
				}
			}
			if decision.Proposed {
				t.Fatalf("decision unexpectedly proposed: %#v", decision)
			}
		})
	}
}

func TestBuildPlanAccountsForBatchResourceContention(t *testing.T) {
	alpha := testTask("alpha")
	alpha.ResourceLocks = []string{"database"}
	beta := testTask("beta")
	beta.ResourceLocks = []string{"database"}

	got, err := BuildPlan(plannerInput(
		[]domain.Task{beta, alpha},
		[]domain.WorkerInventory{plannerWorker("worker-a")},
	))
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if len(got.Proposals) != 1 || got.Proposals[0].TaskID != alpha.ID {
		t.Fatalf("proposals = %#v, want only alpha", got.Proposals)
	}
	decision := plannerDecision(t, got, "beta")
	if !hasPlanningOwnershipBlocker(decision.Blockers, PlanningBlockerResource, "database", "alpha-1") {
		t.Fatalf("beta blockers = %#v, want planned resource owner", decision.Blockers)
	}
}

func TestBuildPlanAccountsForWorkflowCheckoutContention(t *testing.T) {
	input := plannerInput(
		[]domain.Task{testTask("beta"), testTask("alpha")},
		[]domain.WorkerInventory{plannerWorker("worker-a")},
	)
	input.Workflows[0].Workflow.Environment.Scope = EnvironmentScopeWorkflow

	got, err := BuildPlan(input)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if len(got.Proposals) != 1 || got.Proposals[0].TaskID != "task-alpha" {
		t.Fatalf("proposals = %#v, want only alpha", got.Proposals)
	}
	decision := plannerDecision(t, got, "beta")
	if !hasPlanningOwnershipBlocker(decision.Blockers, PlanningBlockerCheckout, "", "alpha-1") {
		t.Fatalf("beta blockers = %#v, want planned checkout owner", decision.Blockers)
	}
}

func TestBuildPlanAppliesCandidateConstraintsDeterministically(t *testing.T) {
	cost := 2.5
	notBefore := plannerTestTime.Add(time.Hour)
	startedAt := plannerTestTime.Add(-time.Hour)
	task := testTask("alpha")
	task.EstimatedCost = &cost
	task.NotBefore = &notBefore
	input := plannerInput(
		[]domain.Task{task},
		[]domain.WorkerInventory{plannerWorker("worker-b"), plannerWorker("worker-a")},
	)
	input.Workflows[0].State.Attempts[0].StartedAt = &startedAt
	input.Constraints = []PlanningConstraint{planningConstraintFunc(func(candidate PlanningCandidate) []PlanningBlocker {
		*candidate.Task.EstimatedCost = 99
		*candidate.Task.NotBefore = plannerTestTime.Add(99 * time.Hour)
		*candidate.Attempt.StartedAt = plannerTestTime.Add(-99 * time.Hour)
		if candidate.WorkerID == "worker-a" {
			return []PlanningBlocker{{Code: "quota", Detail: "provider quota is unavailable"}}
		}
		return nil
	})}

	got, err := BuildPlan(input)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if len(got.Proposals) != 1 || got.Proposals[0].WorkerID != "worker-b" {
		t.Fatalf("proposals = %#v, want worker-b", got.Proposals)
	}
	decision := plannerDecision(t, got, "alpha")
	if len(decision.Candidates) != 2 ||
		decision.Candidates[0].WorkerID != "worker-a" ||
		len(decision.Candidates[0].Blockers) != 1 ||
		decision.Candidates[0].Blockers[0].WorkerID != "worker-a" {
		t.Fatalf("candidate evaluations = %#v", decision.Candidates)
	}
	if cost != 2.5 || !notBefore.Equal(plannerTestTime.Add(time.Hour)) ||
		!startedAt.Equal(plannerTestTime.Add(-time.Hour)) {
		t.Fatalf("constraint mutated planner input: cost=%v notBefore=%v startedAt=%v", cost, notBefore, startedAt)
	}
}

type planningConstraintFunc func(PlanningCandidate) []PlanningBlocker

func (constraint planningConstraintFunc) StartPlan(time.Time) PlanningConstraintSession {
	return constraint
}

func (constraint planningConstraintFunc) Evaluate(candidate PlanningCandidate) []PlanningBlocker {
	return constraint(candidate)
}

func (planningConstraintFunc) Reserve(PlanningCandidate) {}

func plannerInput(tasks []domain.Task, workers []domain.WorkerInventory) PlanInput {
	state := testDAGState(tasks...)
	return PlanInput{
		Now:                  plannerTestTime,
		MaxWorkerSnapshotAge: time.Hour,
		Workflows: []PlanningWorkflow{{
			Workflow: domain.Workflow{
				ID: "workflow", Project: "project",
				Environment: domain.ExecutionEnvironment{Scope: EnvironmentScopeTask},
			},
			State: state,
		}},
		Workers:                workers,
		ResourceOwners:         map[string]string{},
		WorkflowCheckoutOwners: map[string]string{},
	}
}

func plannerWorker(id string, capabilities ...string) domain.WorkerInventory {
	return plannerWorkerWithHealth(id, domain.WorkerHealthReady, capabilities...)
}

func plannerWorkerWithHealth(id string, health domain.WorkerHealth, capabilities ...string) domain.WorkerInventory {
	worker := placementWorker(id, capabilities...)
	worker.Health = health
	worker.ObservedAt = plannerTestTime
	for index := range worker.Projects {
		worker.Projects[index].UpdatedAt = plannerTestTime
	}
	return worker
}

func plannerTaskWithCapabilities(name string, capabilities ...string) domain.Task {
	task := testTask(name)
	task.Placement.Capabilities = append([]string(nil), capabilities...)
	return task
}

func plannerDecision(t *testing.T, plan Plan, taskName string) TaskPlanningDecision {
	t.Helper()
	for _, decision := range plan.Decisions {
		if decision.TaskName == taskName {
			return decision
		}
	}
	t.Fatalf("no planning decision for task %q in %#v", taskName, plan.Decisions)
	return TaskPlanningDecision{}
}

func hasPlanningBlocker(blockers []PlanningBlocker, code, dependency string) bool {
	for _, blocker := range blockers {
		if blocker.Code == code && blocker.DependsOn == dependency {
			return true
		}
	}
	return false
}

func hasPlanningOwnershipBlocker(blockers []PlanningBlocker, code, resource, owner string) bool {
	for _, blocker := range blockers {
		if blocker.Code == code && blocker.Resource == resource && blocker.OwnerID == owner {
			return true
		}
	}
	return false
}

func hasPlacementExclusion(placement WorkerPlacement, workerID, code string) bool {
	for _, evaluation := range placement.Evaluations {
		if evaluation.WorkerID != workerID {
			continue
		}
		for _, exclusion := range evaluation.Exclusions {
			if exclusion.Code == code {
				return true
			}
		}
	}
	return false
}
