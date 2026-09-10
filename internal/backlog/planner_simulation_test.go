package backlog

import (
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type fleetPlanningSimulation struct {
	name          string
	tasks         []domain.Task
	providers     []domain.WorkerProviderInventory
	pools         []domain.QuotaPool
	windows       []QuotaWindowBudget
	estimates     []RouteEstimate
	wantProposals []string
	wantExcluded  []simulationExclusion
}

type simulationExclusion struct {
	taskName        string
	routeOrdinal    int
	code            string
	poolID          string
	required        float64
	available       float64
	staleObservedAt *time.Time
}

func TestBuildPlanFleetQuotaSimulations(t *testing.T) {
	for _, simulation := range fleetPlanningSimulations() {
		t.Run(simulation.name, func(t *testing.T) {
			got := buildFleetSimulationPlan(t, simulation, false)
			reordered := buildFleetSimulationPlan(t, simulation, true)
			if !reflect.DeepEqual(got, reordered) {
				t.Fatalf("plan depends on fleet input order:\nfirst: %#v\nreordered: %#v", got, reordered)
			}

			if proposals := simulationProposalSignatures(got); !reflect.DeepEqual(proposals, simulation.wantProposals) {
				t.Fatalf("proposals = %v, want %v", proposals, simulation.wantProposals)
			}
			for _, decision := range got.Decisions {
				if decision.Order.Reason == "" {
					t.Fatalf("decision for %q has no deterministic ordering explanation", decision.TaskName)
				}
			}
			for _, want := range simulation.wantExcluded {
				assertSimulationExclusion(t, got, want)
			}
		})
	}
}

func fleetPlanningSimulations() []fleetPlanningSimulation {
	requiredRoutes := []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt"}}
	lowWindow := simulationWindow("codex-pool")
	lowTasks := []domain.Task{
		simulationTask("alpha", domain.TaskClassRequired, requiredRoutes...),
		simulationTask("beta", domain.TaskClassRequired, requiredRoutes...),
	}

	surplusWindow := simulationWindow("codex-pool")
	surplusWindow.SurplusStartsAt = plannerTestTime.Add(-time.Hour)
	surplusWindow.ResetsAt = plannerTestTime.Add(12 * time.Hour)
	surplusTasks := []domain.Task{
		simulationTask("alpha", domain.TaskClassSurplus, requiredRoutes...),
		simulationTask("beta", domain.TaskClassSurplus, requiredRoutes...),
	}

	alternativeRoutes := []domain.ProviderRoute{
		{ProviderInstanceID: "first", Model: "model"},
		{ProviderInstanceID: "second", Model: "model"},
	}
	poolAWindow := simulationWindow("pool-a")
	poolBWindow := simulationWindow("pool-b")
	competingTasks := []domain.Task{
		simulationTask("alpha", domain.TaskClassRequired, alternativeRoutes...),
		simulationTask("beta", domain.TaskClassRequired, alternativeRoutes...),
		simulationTask("gamma", domain.TaskClassRequired, alternativeRoutes...),
	}
	staleAt := plannerTestTime.Add(-6 * time.Minute)
	stalePoolAWindow := poolAWindow
	stalePoolAWindow.ObservedAt = staleAt
	staleTasks := []domain.Task{
		simulationTask("alpha", domain.TaskClassRequired, alternativeRoutes...),
		simulationTask("beta", domain.TaskClassRequired, alternativeRoutes...),
	}

	return []fleetPlanningSimulation{
		{
			name:      "low quota reserves one required task and excludes the next",
			tasks:     lowTasks,
			providers: []domain.WorkerProviderInventory{routingProvider("codex", "codex-pool", true, "gpt")},
			pools:     []domain.QuotaPool{routingPool("codex-pool", 10, 0, "codex")},
			windows:   []QuotaWindowBudget{lowWindow},
			estimates: []RouteEstimate{
				routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 15),
				routingEstimate("beta-1", "worker-a", "codex", "gpt", nil, 15),
			},
			wantProposals: []string{"task-alpha:codex:codex-pool"},
			wantExcluded: []simulationExclusion{{
				taskName: "beta", routeOrdinal: 1, code: PlanningBlockerQuotaCapacity,
				poolID: "codex-pool", required: 15, available: 5,
			}},
		},
		{
			name:      "late week surplus uses only the reservable remainder",
			tasks:     surplusTasks,
			providers: []domain.WorkerProviderInventory{routingProvider("codex", "codex-pool", true, "gpt")},
			pools:     []domain.QuotaPool{routingPool("codex-pool", 10, 0, "codex")},
			windows:   []QuotaWindowBudget{surplusWindow},
			estimates: []RouteEstimate{
				routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 15),
				routingEstimate("beta-1", "worker-a", "codex", "gpt", nil, 15),
			},
			wantProposals: []string{"task-alpha:codex:codex-pool"},
			wantExcluded: []simulationExclusion{{
				taskName: "beta", routeOrdinal: 1, code: PlanningBlockerQuotaCapacity,
				poolID: "codex-pool", required: 15, available: 5,
			}},
		},
		{
			name:  "competing provider pools reserve independently and fall back in order",
			tasks: competingTasks,
			providers: []domain.WorkerProviderInventory{
				routingProvider("first", "pool-a", true, "model"),
				routingProvider("second", "pool-b", true, "model"),
			},
			pools:         []domain.QuotaPool{routingPool("pool-a", 10, 0, "first"), routingPool("pool-b", 10, 0, "second")},
			windows:       []QuotaWindowBudget{poolAWindow, poolBWindow},
			estimates:     simulationAlternativeEstimates([]string{"alpha", "beta", "gamma"}, 15, 12),
			wantProposals: []string{"task-alpha:first:pool-a", "task-beta:second:pool-b"},
			wantExcluded: []simulationExclusion{
				{taskName: "beta", routeOrdinal: 1, code: PlanningBlockerQuotaCapacity, poolID: "pool-a", required: 15, available: 5},
				{taskName: "gamma", routeOrdinal: 1, code: PlanningBlockerQuotaCapacity, poolID: "pool-a", required: 15, available: 5},
				{taskName: "gamma", routeOrdinal: 2, code: PlanningBlockerQuotaCapacity, poolID: "pool-b", required: 12, available: 8},
			},
		},
		{
			name:  "stale observation fails closed while a fresh alternative remains usable",
			tasks: staleTasks,
			providers: []domain.WorkerProviderInventory{
				routingProvider("first", "pool-a", true, "model"),
				routingProvider("second", "pool-b", true, "model"),
			},
			pools:         []domain.QuotaPool{routingPool("pool-a", 10, 0, "first"), routingPool("pool-b", 10, 0, "second")},
			windows:       []QuotaWindowBudget{stalePoolAWindow, poolBWindow},
			estimates:     simulationAlternativeEstimates([]string{"alpha", "beta"}, 12, 12),
			wantProposals: []string{"task-alpha:second:pool-b"},
			wantExcluded: []simulationExclusion{
				{taskName: "alpha", routeOrdinal: 1, code: PlanningBlockerQuotaObservationStale, poolID: "pool-a", required: 12, available: 20, staleObservedAt: &staleAt},
				{taskName: "beta", routeOrdinal: 1, code: PlanningBlockerQuotaObservationStale, poolID: "pool-a", required: 12, available: 20, staleObservedAt: &staleAt},
				{taskName: "beta", routeOrdinal: 2, code: PlanningBlockerQuotaCapacity, poolID: "pool-b", required: 12, available: 8},
			},
		},
	}
}

func buildFleetSimulationPlan(t *testing.T, simulation fleetPlanningSimulation, reversed bool) Plan {
	t.Helper()
	tasks := append([]domain.Task(nil), simulation.tasks...)
	providers := append([]domain.WorkerProviderInventory(nil), simulation.providers...)
	pools := append([]domain.QuotaPool(nil), simulation.pools...)
	windows := append([]QuotaWindowBudget(nil), simulation.windows...)
	estimates := append([]RouteEstimate(nil), simulation.estimates...)
	if reversed {
		reverseSimulationSlice(tasks)
		reverseSimulationSlice(providers)
		reverseSimulationSlice(pools)
		reverseSimulationSlice(windows)
		reverseSimulationSlice(estimates)
	}
	policy, err := NewQuotaAdmissionPolicy(quotaTestInput(windows...))
	if err != nil {
		t.Fatalf("NewQuotaAdmissionPolicy: %v", err)
	}
	input := plannerInput(tasks, []domain.WorkerInventory{
		routingWorker("worker-a", providers...),
		plannerWorkerWithHealth("worker-z", domain.WorkerHealthOffline),
	})
	if reversed {
		input.Workers[0], input.Workers[1] = input.Workers[1], input.Workers[0]
	}
	input.QuotaPools = pools
	input.RouteEstimates = estimates
	input.Constraints = []PlanningConstraint{policy}
	plan, err := BuildPlan(input)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	return plan
}

func simulationWindow(poolID string) QuotaWindowBudget {
	window := quotaTestWindow()
	window.QuotaPoolID = poolID
	return window
}

func simulationTask(name string, class domain.TaskClass, routes ...domain.ProviderRoute) domain.Task {
	task := testTask(name)
	task.Class = class
	task.Routes = append([]domain.ProviderRoute(nil), routes...)
	return task
}

func simulationAlternativeEstimates(taskNames []string, firstCost, secondCost float64) []RouteEstimate {
	estimates := make([]RouteEstimate, 0, len(taskNames)*2)
	for _, name := range taskNames {
		attemptID := name + "-1"
		estimates = append(estimates,
			routingEstimate(attemptID, "worker-a", "first", "model", nil, firstCost),
			routingEstimate(attemptID, "worker-a", "second", "model", nil, secondCost),
		)
	}
	return estimates
}

func simulationProposalSignatures(plan Plan) []string {
	result := make([]string, 0, len(plan.Proposals))
	for _, proposal := range plan.Proposals {
		if proposal.Route == nil {
			result = append(result, proposal.TaskID+"::<none>")
			continue
		}
		result = append(result, proposal.TaskID+":"+proposal.Route.ProviderInstanceID+":"+proposal.Route.QuotaPoolID)
	}
	return result
}

func assertSimulationExclusion(t *testing.T, plan Plan, want simulationExclusion) {
	t.Helper()
	decision := plannerDecision(t, plan, want.taskName)
	for _, candidate := range decision.Candidates {
		if candidate.RouteOrdinal != want.routeOrdinal {
			continue
		}
		for _, blocker := range candidate.Blockers {
			if blocker.Code != want.code || blocker.QuotaPoolID != want.poolID ||
				blocker.RequiredCost != want.required || blocker.Available != want.available {
				continue
			}
			if blocker.Detail == "" {
				t.Fatalf("%s route %d blocker has no explanation: %#v", want.taskName, want.routeOrdinal, blocker)
			}
			if want.staleObservedAt != nil {
				if blocker.ObservedAt == nil || !blocker.ObservedAt.Equal(*want.staleObservedAt) ||
					blocker.MaxObservationAgeSeconds != (5*time.Minute).Seconds() {
					t.Fatalf("%s stale blocker = %#v, want observation %s and five-minute freshness limit", want.taskName, blocker, want.staleObservedAt)
				}
			}
			return
		}
	}
	t.Fatalf("%s route %d has no exclusion %#v in %#v", want.taskName, want.routeOrdinal, want, decision.Candidates)
}

func reverseSimulationSlice[T any](values []T) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}
