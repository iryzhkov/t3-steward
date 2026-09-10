package backlog

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestBuildPlanSelectsOrderedProviderRouteAcrossWorkers(t *testing.T) {
	task := testTask("alpha")
	task.Routes = []domain.ProviderRoute{
		{WorkerID: "worker-b", ProviderInstanceID: "claude", Model: "opus", Options: map[string]string{"effort": "high"}},
		{ProviderInstanceID: "codex", Model: "gpt"},
	}
	workerA := routingWorker("worker-a",
		domain.WorkerProviderInventory{InstanceID: "codex", Models: []string{"gpt"}, QuotaPoolID: "codex-pool", Available: true},
	)
	workerB := routingWorker("worker-b",
		domain.WorkerProviderInventory{InstanceID: "claude", Models: []string{"opus"}, QuotaPoolID: "claude-pool", Available: true},
	)
	input := plannerInput([]domain.Task{task}, []domain.WorkerInventory{workerB, workerA})
	input.QuotaPools = []domain.QuotaPool{
		routingPool("codex-pool", 2, 0, "codex"),
		routingPool("claude-pool", 2, 0, "claude"),
	}
	input.RouteEstimates = []RouteEstimate{
		routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 10),
		routingEstimate("alpha-1", "worker-b", "claude", "opus", map[string]string{"effort": "high"}, 12),
	}
	before := cloneRoutingPlanInput(input)

	got, err := BuildPlan(input)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	reorderedInput := cloneRoutingPlanInput(input)
	reorderedInput.Workers[0], reorderedInput.Workers[1] = reorderedInput.Workers[1], reorderedInput.Workers[0]
	reorderedInput.QuotaPools[0], reorderedInput.QuotaPools[1] = reorderedInput.QuotaPools[1], reorderedInput.QuotaPools[0]
	reorderedInput.RouteEstimates[0], reorderedInput.RouteEstimates[1] = reorderedInput.RouteEstimates[1], reorderedInput.RouteEstimates[0]
	reordered, err := BuildPlan(reorderedInput)
	if err != nil {
		t.Fatalf("BuildPlan(reordered): %v", err)
	}
	if !reflect.DeepEqual(got, reordered) {
		t.Fatalf("route plan depends on fleet input order:\nfirst: %#v\nreordered: %#v", got, reordered)
	}
	if !reflect.DeepEqual(input, before) {
		t.Fatalf("BuildPlan mutated route input:\nbefore: %#v\nafter: %#v", before, input)
	}
	if len(got.Proposals) != 1 {
		t.Fatalf("proposals = %#v, want one", got.Proposals)
	}
	proposal := got.Proposals[0]
	if proposal.WorkerID != "worker-b" || proposal.Route == nil ||
		proposal.Route.ProviderInstanceID != "claude" || proposal.Route.Model != "opus" ||
		proposal.Route.QuotaPoolID != "claude-pool" || proposal.Estimate == nil ||
		proposal.Estimate.RemainingCost != 12 {
		t.Fatalf("proposal = %#v, want preferred claude route on worker-b", proposal)
	}
	decision := plannerDecision(t, got, "alpha")
	wantOrder := []struct {
		ordinal int
		worker  string
	}{
		{1, "worker-a"}, {1, "worker-b"}, {2, "worker-a"}, {2, "worker-b"},
	}
	if len(decision.Candidates) != len(wantOrder) {
		t.Fatalf("candidates = %#v", decision.Candidates)
	}
	for index, want := range wantOrder {
		candidate := decision.Candidates[index]
		if candidate.RouteOrdinal != want.ordinal || candidate.WorkerID != want.worker {
			t.Fatalf("candidate %d = %#v, want route %d worker %q", index, candidate, want.ordinal, want.worker)
		}
	}
}

func TestBuildPlanExplainsProviderRouteExclusions(t *testing.T) {
	tests := []struct {
		name       string
		route      domain.ProviderRoute
		provider   domain.WorkerProviderInventory
		pools      []domain.QuotaPool
		estimates  []RouteEstimate
		wantCode   string
		wantDetail string
	}{
		{
			name:       "worker mismatch",
			route:      domain.ProviderRoute{WorkerID: "worker-b", ProviderInstanceID: "codex", Model: "gpt"},
			provider:   routingProvider("codex", "pool", true, "gpt"),
			pools:      []domain.QuotaPool{routingPool("pool", 2, 0, "codex")},
			estimates:  []RouteEstimate{routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 10)},
			wantCode:   PlanningBlockerRouteWorkerMismatch,
			wantDetail: "requires worker",
		},
		{
			name:       "provider unavailable",
			route:      domain.ProviderRoute{ProviderInstanceID: "codex", Model: "gpt"},
			provider:   routingProvider("codex", "pool", false, "gpt"),
			pools:      []domain.QuotaPool{routingPool("pool", 2, 0, "codex")},
			estimates:  []RouteEstimate{routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 10)},
			wantCode:   PlanningBlockerProviderUnavailable,
			wantDetail: "unavailable",
		},
		{
			name:       "model unavailable",
			route:      domain.ProviderRoute{ProviderInstanceID: "codex", Model: "gpt"},
			provider:   routingProvider("codex", "pool", true, "other"),
			pools:      []domain.QuotaPool{routingPool("pool", 2, 0, "codex")},
			estimates:  []RouteEstimate{routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 10)},
			wantCode:   PlanningBlockerModelUnavailable,
			wantDetail: "model",
		},
		{
			name:       "quota pool missing",
			route:      domain.ProviderRoute{ProviderInstanceID: "codex", Model: "gpt"},
			provider:   routingProvider("codex", "pool", true, "gpt"),
			estimates:  []RouteEstimate{routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 10)},
			wantCode:   PlanningBlockerQuotaPoolUnavailable,
			wantDetail: "no fleet quota pool",
		},
		{
			name:       "route and inventory pools conflict",
			route:      domain.ProviderRoute{ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "other"},
			provider:   routingProvider("codex", "pool", true, "gpt"),
			pools:      []domain.QuotaPool{routingPool("pool", 2, 0, "codex"), routingPool("other", 2, 0, "other")},
			estimates:  []RouteEstimate{routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 10)},
			wantCode:   PlanningBlockerQuotaPoolUnavailable,
			wantDetail: "conflicts",
		},
		{
			name:       "ambiguous fleet pool mapping",
			route:      domain.ProviderRoute{ProviderInstanceID: "codex", Model: "gpt"},
			provider:   routingProvider("codex", "", true, "gpt"),
			pools:      []domain.QuotaPool{routingPool("pool", 2, 0, "codex"), routingPool("other", 2, 0, "codex")},
			estimates:  []RouteEstimate{routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 10)},
			wantCode:   PlanningBlockerQuotaPoolUnavailable,
			wantDetail: "multiple fleet quota pools",
		},
		{
			name:       "route estimate missing",
			route:      domain.ProviderRoute{ProviderInstanceID: "codex", Model: "gpt"},
			provider:   routingProvider("codex", "pool", true, "gpt"),
			pools:      []domain.QuotaPool{routingPool("pool", 2, 0, "codex")},
			wantCode:   PlanningBlockerRouteEstimateMissing,
			wantDetail: "no remaining-cost",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := testTask("alpha")
			task.Routes = []domain.ProviderRoute{test.route}
			input := plannerInput(
				[]domain.Task{task},
				[]domain.WorkerInventory{routingWorker("worker-a", test.provider)},
			)
			input.QuotaPools = test.pools
			input.RouteEstimates = test.estimates

			got, err := BuildPlan(input)
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}
			if len(got.Proposals) != 0 {
				t.Fatalf("proposals = %#v, want none", got.Proposals)
			}
			decision := plannerDecision(t, got, "alpha")
			if !candidateBlockerContains(decision.Candidates, test.wantCode, test.wantDetail) {
				t.Fatalf("candidates = %#v, want blocker %q containing %q", decision.Candidates, test.wantCode, test.wantDetail)
			}
		})
	}
}

func TestBuildPlanReservesSharedPoolConcurrencyWithinBatch(t *testing.T) {
	alpha := routingTask("alpha", "codex-a", "gpt")
	beta := routingTask("beta", "codex-b", "gpt")
	worker := routingWorker("worker-a",
		routingProvider("codex-a", "pool", true, "gpt"),
		routingProvider("codex-b", "pool", true, "gpt"),
	)
	input := plannerInput([]domain.Task{beta, alpha}, []domain.WorkerInventory{worker})
	input.QuotaPools = []domain.QuotaPool{routingPool("pool", 1, 0, "codex-a", "codex-b")}
	input.RouteEstimates = []RouteEstimate{
		routingEstimate("alpha-1", "worker-a", "codex-a", "gpt", nil, 10),
		routingEstimate("beta-1", "worker-a", "codex-b", "gpt", nil, 10),
	}

	got, err := BuildPlan(input)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if len(got.Proposals) != 1 || got.Proposals[0].TaskID != "task-alpha" {
		t.Fatalf("proposals = %#v, want only alpha", got.Proposals)
	}
	if !candidateBlockerContains(plannerDecision(t, got, "beta").Candidates, PlanningBlockerPoolConcurrency, "limit of 1") {
		t.Fatalf("beta candidates = %#v, want shared-pool concurrency blocker", plannerDecision(t, got, "beta").Candidates)
	}
}

func TestBuildPlanDerivesPoolFromFleetMapping(t *testing.T) {
	task := routingTask("alpha", "codex", "gpt")
	worker := routingWorker("worker-a", routingProvider("codex", "", true, "gpt"))
	input := plannerInput([]domain.Task{task}, []domain.WorkerInventory{worker})
	input.QuotaPools = []domain.QuotaPool{routingPool("shared", 2, 0, "codex")}
	input.RouteEstimates = []RouteEstimate{
		routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 10),
	}

	got, err := BuildPlan(input)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if len(got.Proposals) != 1 || got.Proposals[0].Route == nil ||
		got.Proposals[0].Route.QuotaPoolID != "shared" {
		t.Fatalf("proposals = %#v, want fleet-derived pool", got.Proposals)
	}
}

func TestBuildPlanUsesWorkerPoolForAmbiguousInstance(t *testing.T) {
	task := routingTask("alpha", "codex", "gpt")
	workerA := routingWorker("worker-a", routingProvider("codex", "pool-a", true, "gpt"))
	workerB := routingWorker("worker-b", routingProvider("codex", "pool-b", true, "gpt"))
	input := plannerInput([]domain.Task{task}, []domain.WorkerInventory{workerB, workerA})
	input.QuotaPools = []domain.QuotaPool{
		routingPool("pool-a", 2, 0, "codex"),
		routingPool("pool-b", 2, 0, "codex"),
	}
	input.RouteEstimates = []RouteEstimate{
		routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 10),
		routingEstimate("alpha-1", "worker-b", "codex", "gpt", nil, 10),
	}

	got, err := BuildPlan(input)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if len(got.Proposals) != 1 || got.Proposals[0].Route == nil ||
		got.Proposals[0].Route.QuotaPoolID != "pool-a" {
		t.Fatalf("proposals = %#v, want worker-specific pool-a", got.Proposals)
	}
	decision := plannerDecision(t, got, "alpha")
	if len(decision.Candidates) != 2 || decision.Candidates[1].Route == nil ||
		decision.Candidates[1].Route.QuotaPoolID != "pool-b" {
		t.Fatalf("candidates = %#v, want worker-specific pool bindings", decision.Candidates)
	}
}

func TestBuildPlanFallsBackUsingRouteSpecificQuotaEstimate(t *testing.T) {
	task := testTask("alpha")
	task.Class = domain.TaskClassRequired
	task.Routes = []domain.ProviderRoute{
		{ProviderInstanceID: "first", Model: "model"},
		{ProviderInstanceID: "second", Model: "model"},
	}
	worker := routingWorker("worker-a",
		routingProvider("first", "pool-a", true, "model"),
		routingProvider("second", "pool-b", true, "model"),
	)
	input := plannerInput([]domain.Task{task}, []domain.WorkerInventory{worker})
	input.QuotaPools = []domain.QuotaPool{
		routingPool("pool-a", 2, 0, "first"),
		routingPool("pool-b", 2, 0, "second"),
	}
	input.RouteEstimates = []RouteEstimate{
		routingEstimate("alpha-1", "worker-a", "first", "model", nil, 25),
		routingEstimate("alpha-1", "worker-a", "second", "model", nil, 15),
	}
	windowA := quotaTestWindow()
	windowA.QuotaPoolID = "pool-a"
	windowB := quotaTestWindow()
	windowB.QuotaPoolID = "pool-b"
	policy, err := NewQuotaAdmissionPolicy(QuotaAdmissionInput{Windows: []QuotaWindowBudget{windowB, windowA}})
	if err != nil {
		t.Fatalf("NewQuotaAdmissionPolicy: %v", err)
	}
	input.Constraints = []PlanningConstraint{policy}

	got, err := BuildPlan(input)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if len(got.Proposals) != 1 || got.Proposals[0].Route == nil ||
		got.Proposals[0].Route.ProviderInstanceID != "second" {
		t.Fatalf("proposals = %#v, want second route", got.Proposals)
	}
	decision := plannerDecision(t, got, "alpha")
	if !candidateBlockerContains(decision.Candidates, PlanningBlockerQuotaCapacity, "25.000 remaining cost") {
		t.Fatalf("candidates = %#v, want first-route quota blocker", decision.Candidates)
	}
	if len(decision.Candidates) != 2 || len(decision.Candidates[1].Blockers) != 0 {
		t.Fatalf("candidates = %#v, want unblocked second route", decision.Candidates)
	}
}

func TestBuildPlanAccountsForExistingPoolConcurrency(t *testing.T) {
	task := routingTask("alpha", "codex", "gpt")
	worker := routingWorker("worker-a", routingProvider("codex", "pool", true, "gpt"))
	input := plannerInput([]domain.Task{task}, []domain.WorkerInventory{worker})
	input.QuotaPools = []domain.QuotaPool{routingPool("pool", 2, 2, "codex")}
	input.RouteEstimates = []RouteEstimate{
		routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 10),
	}
	got, err := BuildPlan(input)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if !candidateBlockerContains(plannerDecision(t, got, "alpha").Candidates, PlanningBlockerPoolConcurrency, "2 active or planned") {
		t.Fatalf("candidates = %#v, want existing concurrency blocker", plannerDecision(t, got, "alpha").Candidates)
	}
}

func TestProviderRoutingRejectsInvalidFleetInput(t *testing.T) {
	validPool := routingPool("pool", 2, 0, "codex")
	validEstimate := routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 10)
	tests := []struct {
		name   string
		pools  []domain.QuotaPool
		values []RouteEstimate
	}{
		{name: "invalid concurrency", pools: []domain.QuotaPool{{ID: "pool", ProviderInstanceIDs: []string{"codex"}}}},
		{name: "duplicate pool", pools: []domain.QuotaPool{validPool, validPool}},
		{name: "invalid estimate", pools: []domain.QuotaPool{validPool}, values: []RouteEstimate{{AttemptID: "alpha-1"}}},
		{name: "duplicate estimate", pools: []domain.QuotaPool{validPool}, values: []RouteEstimate{validEstimate, validEstimate}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := plannerInput(nil, nil)
			input.QuotaPools = test.pools
			input.RouteEstimates = test.values
			if _, err := BuildPlan(input); err == nil {
				t.Fatal("BuildPlan succeeded, want routing validation error")
			}
		})
	}
}

func routingTask(name, instance, model string) domain.Task {
	task := testTask(name)
	task.Routes = []domain.ProviderRoute{{ProviderInstanceID: instance, Model: model}}
	return task
}

func routingWorker(id string, providers ...domain.WorkerProviderInventory) domain.WorkerInventory {
	worker := plannerWorker(id)
	worker.Providers = append([]domain.WorkerProviderInventory(nil), providers...)
	return worker
}

func routingProvider(instance, pool string, available bool, models ...string) domain.WorkerProviderInventory {
	return domain.WorkerProviderInventory{
		InstanceID: instance, Models: append([]string(nil), models...),
		QuotaPoolID: pool, Available: available,
	}
}

func routingPool(id string, maximum, active int, instances ...string) domain.QuotaPool {
	return domain.QuotaPool{
		ID: id, ProviderInstanceIDs: append([]string(nil), instances...),
		MaxConcurrent: maximum, ActiveAssignments: active,
	}
}

func routingEstimate(attempt, worker, instance, model string, options map[string]string, cost float64) RouteEstimate {
	return RouteEstimate{
		AttemptID: attempt, WorkerID: worker, ProviderInstanceID: instance, Model: model,
		Options: cloneStringMap(options),
		Estimate: TaskAdmissionEstimate{
			RemainingCost: cost, ExpectedRuntime: 20 * time.Minute, CheckpointMargin: 5 * time.Minute,
		},
	}
}

func candidateBlockerContains(candidates []CandidateEvaluation, code, detail string) bool {
	for _, candidate := range candidates {
		for _, blocker := range candidate.Blockers {
			if blocker.Code == code && strings.Contains(blocker.Detail, detail) {
				return true
			}
		}
	}
	return false
}

func cloneRoutingPlanInput(input PlanInput) PlanInput {
	result := input
	result.Workflows = append([]PlanningWorkflow(nil), input.Workflows...)
	for index := range result.Workflows {
		result.Workflows[index].Workflow.TaskIDs = append([]string(nil), input.Workflows[index].Workflow.TaskIDs...)
		result.Workflows[index].Workflow.InputArtifactIDs = append([]string(nil), input.Workflows[index].Workflow.InputArtifactIDs...)
		result.Workflows[index].State.Run.CompletedAt = clonePlanningTime(input.Workflows[index].State.Run.CompletedAt)
		result.Workflows[index].State.Tasks = append([]domain.Task(nil), input.Workflows[index].State.Tasks...)
		for taskIndex := range result.Workflows[index].State.Tasks {
			result.Workflows[index].State.Tasks[taskIndex] = clonePlanningTask(result.Workflows[index].State.Tasks[taskIndex])
		}
		result.Workflows[index].State.Attempts = append([]domain.Attempt(nil), input.Workflows[index].State.Attempts...)
		for attemptIndex := range result.Workflows[index].State.Attempts {
			result.Workflows[index].State.Attempts[attemptIndex] = clonePlanningAttempt(result.Workflows[index].State.Attempts[attemptIndex])
		}
	}
	result.Workers = append([]domain.WorkerInventory(nil), input.Workers...)
	for index := range result.Workers {
		result.Workers[index] = cloneRouteWorker(result.Workers[index])
	}
	result.QuotaPools = append([]domain.QuotaPool(nil), input.QuotaPools...)
	for index := range result.QuotaPools {
		result.QuotaPools[index].ProviderInstanceIDs = append([]string(nil), input.QuotaPools[index].ProviderInstanceIDs...)
	}
	result.RouteEstimates = append([]RouteEstimate(nil), input.RouteEstimates...)
	for index := range result.RouteEstimates {
		result.RouteEstimates[index].Options = cloneStringMap(input.RouteEstimates[index].Options)
	}
	result.ResourceOwners = cloneStringMap(input.ResourceOwners)
	result.WorkflowCheckoutOwners = cloneStringMap(input.WorkflowCheckoutOwners)
	result.Constraints = append([]PlanningConstraint(nil), input.Constraints...)
	result.Ordering.Attempts = make(map[string]PlanningAttemptOrdering, len(input.Ordering.Attempts))
	for attemptID, ordering := range input.Ordering.Attempts {
		result.Ordering.Attempts[attemptID] = ordering
	}
	return result
}
