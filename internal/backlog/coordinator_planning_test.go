package backlog

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestBuildCoordinatorPlanInputProducesQuotaBoundColdEstimateAndStopsReplanning(t *testing.T) {
	now := plannerTestTime
	cost := 12.0
	task := routingTask("alpha", "codex", "gpt")
	task.Class = domain.TaskClassRequired
	task.MaxTurns = 3
	task.Difficulty = 2
	task.EstimatedCost = &cost
	task.ResourceLocks = []string{"repository"}
	state := testDAGState(task)
	state.Attempts[0].Progress = domain.ProgressReady
	workflow := domain.Workflow{
		ID: "workflow", Project: "project", TaskIDs: []string{task.ID},
		Environment: domain.ExecutionEnvironment{Scope: EnvironmentScopeWorkflow},
	}
	worker := routingWorker("worker-a", routingProvider("codex", "pool", true, "gpt"))
	snapshot := domain.WorkerSnapshot{
		WorkerID: "worker-a", WorkerEpoch: "worker-epoch", CoordinatorEpoch: 4,
		Sequence: 2, Connected: true, Inventory: worker,
		ObservedAt: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour),
	}
	window := quotaTestWindow()
	window.ObservedAt = now.Add(-time.Minute)
	input := CoordinatorPlanningStateInput{
		Now: now, CoordinatorEpoch: 4,
		Workflows: []domain.Workflow{workflow}, WorkflowRuns: []domain.WorkflowRun{state.Run},
		Tasks: []domain.Task{task}, Attempts: append([]domain.Attempt(nil), state.Attempts...),
		WorkerSnapshots:      []domain.WorkerSnapshot{snapshot},
		QuotaPools:           []domain.QuotaPool{routingPool("pool", 2, 0, "codex")},
		QuotaWindows:         []QuotaWindowBudget{window},
		MaxWorkerSnapshotAge: time.Hour, MaxQuotaObservationAge: 5 * time.Minute,
		DeadlineRiskWindow: time.Hour, CheckpointMargin: 7 * time.Minute,
	}
	planInput, err := BuildCoordinatorPlanInput(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(planInput.Constraints) != 1 || len(planInput.RouteEstimates) != 1 {
		t.Fatalf("assembled input = %#v", planInput)
	}
	estimate := planInput.RouteEstimates[0].Estimate
	wantRuntime := time.Duration(SeedMinutes(task.Difficulty) * float64(time.Minute) * float64(task.MaxTurns))
	if estimate.RemainingCost != cost || estimate.ExpectedRuntime != wantRuntime ||
		estimate.CheckpointMargin != 7*time.Minute {
		t.Fatalf("cold estimate = %#v", estimate)
	}
	plan, err := BuildPlan(planInput)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Proposals) != 1 || plan.Proposals[0].AttemptID != state.Attempts[0].ID {
		t.Fatalf("initial plan = %#v", plan)
	}

	assignedAttempt := input.Attempts[0]
	assignedAttempt.AssignmentID = "assignment-alpha"
	input.Attempts[0] = assignedAttempt
	input.Assignments = []domain.Assignment{{
		ID: "assignment-alpha", AttemptID: assignedAttempt.ID, WorkerID: "worker-a",
		Route: domain.ProviderRoute{
			WorkerID: "worker-a", ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "pool",
		},
		State: domain.AssignmentOffered,
	}}
	reloaded, err := BuildCoordinatorPlanInput(input)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.ResourceOwners["repository"] != assignedAttempt.ID ||
		reloaded.WorkflowCheckoutOwners[state.Run.ID] != assignedAttempt.ID {
		t.Fatalf("reconstructed owners = resources %#v checkouts %#v", reloaded.ResourceOwners, reloaded.WorkflowCheckoutOwners)
	}
	replan, err := BuildPlan(reloaded)
	if err != nil {
		t.Fatal(err)
	}
	if len(replan.Proposals) != 0 || len(replan.Decisions) != 1 ||
		len(replan.Decisions[0].Blockers) == 0 ||
		replan.Decisions[0].Blockers[0].Code != PlanningBlockerAttemptState {
		t.Fatalf("assigned replan = %#v", replan)
	}

	input.Attempts[0].Progress = domain.ProgressVerifying
	input.Attempts[0].Control = domain.ControlStopped
	input.Assignments[0].State = domain.AssignmentCompleted
	verifying, err := BuildCoordinatorPlanInput(input)
	if err != nil {
		t.Fatalf("completed assignment awaiting result import blocked planning: %v", err)
	}
	if verifying.ResourceOwners["repository"] != "" || verifying.WorkflowCheckoutOwners[state.Run.ID] != "" {
		t.Fatalf("verifying attempt retained planning ownership: resources %#v checkouts %#v", verifying.ResourceOwners, verifying.WorkflowCheckoutOwners)
	}
}

func TestBuildCoordinatorPlanInputReleasesDependencyAfterImportedOutcome(t *testing.T) {
	now := plannerTestTime
	producer := routingTask("producer", "codex", "gpt")
	consumer := routingTask("consumer", "codex", "gpt")
	consumer.Needs = []string{producer.Name}
	state := testDAGState(producer, consumer)
	state.Attempts[0].Progress = domain.ProgressSucceeded
	state.Attempts[0].Control = domain.ControlStopped
	state.Attempts[0].AssignmentID = "assignment-producer"
	state.Attempts[1].Progress = domain.ProgressBlocked
	workflow := domain.Workflow{ID: "workflow", Project: "project", TaskIDs: []string{producer.ID, consumer.ID}}
	worker := routingWorker("worker-a", routingProvider("codex", "pool", true, "gpt"))
	snapshot := domain.WorkerSnapshot{
		WorkerID: "worker-a", WorkerEpoch: "worker-epoch", CoordinatorEpoch: 4,
		Sequence: 2, Connected: true, Inventory: worker,
		ObservedAt: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour),
	}
	window := quotaTestWindow()
	window.ObservedAt = now.Add(-time.Minute)
	input, err := BuildCoordinatorPlanInput(CoordinatorPlanningStateInput{
		Now: now, CoordinatorEpoch: 4,
		Workflows: []domain.Workflow{workflow}, WorkflowRuns: []domain.WorkflowRun{state.Run},
		Tasks: []domain.Task{producer, consumer}, Attempts: state.Attempts,
		Assignments:     []domain.Assignment{{ID: "assignment-producer", AttemptID: state.Attempts[0].ID, WorkerID: "worker-a", State: domain.AssignmentCompleted}},
		WorkerSnapshots: []domain.WorkerSnapshot{snapshot}, QuotaPools: []domain.QuotaPool{routingPool("pool", 2, 0, "codex")}, QuotaWindows: []QuotaWindowBudget{window},
		MaxWorkerSnapshotAge: time.Hour, MaxQuotaObservationAge: 5 * time.Minute, DeadlineRiskWindow: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlan(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Proposals) != 1 || plan.Proposals[0].AttemptID != state.Attempts[1].ID {
		t.Fatalf("dependency plan = %#v", plan)
	}
}
