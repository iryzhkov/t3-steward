package backlog

import (
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// roleTask is a task the coordinator resolved from role "execute" to
// codex/gpt in pool-a; its receipt also names claude/opus in pool-b.
func roleTask(name string) domain.Task {
	task := routingTask(name, "codex", "gpt")
	task.Role = "execute"
	task.Routes[0].Options = map[string]string{"effort": "medium"}
	task.RoleSelection = &domain.RoleSelection{
		Role: "execute", Route: "codex/gpt", Effort: "medium", Ranking: domain.RouteRankingV1,
		Candidates: []domain.RoleCandidateVerdict{
			{Ordinal: 1, Band: "healthy", Pool: "pool-a", Route: "codex/gpt", Eligible: true, Effort: "medium"},
			{Ordinal: 2, Band: "healthy", Pool: "pool-b", Route: "claude/opus", Eligible: true, Effort: "high"},
		},
	}
	return task
}

// reresolutionInput plans tasks on one worker offering both candidates, with
// pool-a holding activeA of its limit of limitA and pool-b empty.
func reresolutionInput(limitA, activeA int, tasks ...domain.Task) PlanInput {
	worker := routingWorker("worker",
		routingProvider("codex", "pool-a", true, "gpt"), routingProvider("claude", "pool-b", true, "opus"))
	input := plannerInput(tasks, []domain.WorkerInventory{worker})
	for index := range input.Workflows[0].State.Attempts {
		input.Workflows[0].State.Attempts[index].Progress = domain.ProgressReady
	}
	input.QuotaPools = []domain.QuotaPool{routingPool("pool-a", limitA, activeA, "codex"), routingPool("pool-b", 2, 0, "claude")}
	for _, task := range tasks {
		input.RouteEstimates = append(input.RouteEstimates,
			routingEstimate(task.Name+"-1", "worker", "codex", "gpt", map[string]string{"effort": "medium"}, 10))
	}
	return input
}

func TestSaturatedRoleTaskIsReresolvedWithRecordedReason(t *testing.T) {
	plan, err := BuildPlan(reresolutionInput(1, 1, roleTask("alpha")))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Proposals) != 1 {
		t.Fatalf("proposals = %+v, want alpha moved to the candidate with room", plan.Proposals)
	}
	proposal := plan.Proposals[0]
	if proposal.Route == nil || proposal.Route.ProviderInstanceID != "claude" || proposal.Route.Model != "opus" ||
		proposal.Route.QuotaPoolID != "pool-b" || proposal.Route.Options["effort"] != "high" {
		t.Fatalf("route = %+v, want claude/opus in pool-b at the candidate's effort", proposal.Route)
	}
	receipt := proposal.Placement.RouteReresolution
	if receipt == nil || receipt.Role != "execute" || receipt.FromRoute != "codex/gpt" || receipt.FromPool != "pool-a" ||
		receipt.ToRoute != "claude/opus" || receipt.ToPool != "pool-b" || receipt.Effort != "high" ||
		receipt.Ranking != domain.RouteRankingV1 || !receipt.DecidedAt.Equal(plannerTestTime) ||
		!strings.Contains(receipt.Reason, `quota pool "pool-a" has 1 active or planned assignments at its concurrency limit of 1`) {
		t.Fatalf("placement receipt = %+v", receipt)
	}
	if decision := plannerDecision(t, plan, "alpha"); decision.RouteReresolution == nil || *decision.RouteReresolution != *receipt {
		t.Fatalf("decision receipt = %+v, want the placement's", decision.RouteReresolution)
	}
}

// Planned assignments count: the first role task takes pool-a's only slot in
// this pass, and the second is moved because of it.
func TestRoleReresolutionCountsPlannedAssignments(t *testing.T) {
	plan, err := BuildPlan(reresolutionInput(1, 0, roleTask("alpha"), roleTask("beta")))
	if err != nil {
		t.Fatal(err)
	}
	pools := map[string]string{}
	for _, proposal := range plan.Proposals {
		pools[proposal.AttemptID] = proposal.Route.QuotaPoolID
	}
	if pools["alpha-1"] != "pool-a" || pools["beta-1"] != "pool-b" {
		t.Fatalf("pools = %v, want alpha in pool-a and beta re-resolved to pool-b", pools)
	}
	if plannerDecision(t, plan, "alpha").RouteReresolution != nil || plannerDecision(t, plan, "beta").RouteReresolution == nil {
		t.Fatal("only the task planned into a saturated pool may be re-resolved")
	}
}

func TestRoleTaskWithRoomKeepsItsRoute(t *testing.T) {
	plan, err := BuildPlan(reresolutionInput(2, 1, roleTask("alpha")))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Proposals) != 1 || plan.Proposals[0].Route.QuotaPoolID != "pool-a" || plan.Proposals[0].Placement.RouteReresolution != nil {
		t.Fatalf("proposals = %+v, want the resolved route kept while its pool has room", plan.Proposals)
	}
}

func TestReviewReresolutionEffortDoesNotExceedAlternativePolicy(t *testing.T) {
	task := roleTask("alpha")
	task.RoleEffort = "medium"
	task.RoleSelection.Candidates[1].Effort = "low"
	plan, err := BuildPlan(reresolutionInput(1, 1, task))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Proposals) != 0 || plannerDecision(t, plan, "alpha").RouteReresolution != nil {
		t.Fatal("override above candidate policy must leave task queued")
	}
}

func TestReviewReresolutionPreservesDownstreamCrossProviderSelection(t *testing.T) {
	producer := roleTask("alpha")
	reviewer := routingTask("beta", "claude", "opus")
	reviewer.Role = "review"
	reviewer.Routes[0].Options = map[string]string{"effort": "medium"}
	reviewer.RoleSelection = &domain.RoleSelection{
		Role: "review", Route: "claude/opus", Effort: "medium",
		Diversity: domain.RoleDiversity{CrossProvider: true, ProducerFamilies: []string{"openai"}},
	}
	reviewer.Needs = []string{producer.Name}
	input := reresolutionInput(1, 1, producer, reviewer)
	input.RouteEstimates = append(input.RouteEstimates,
		routingEstimate("beta-1", "worker", "claude", "opus", map[string]string{"effort": "medium"}, 10))
	plan, err := BuildPlan(input)
	if err != nil {
		t.Fatal(err)
	}
	var producerRoute domain.ProviderRoute
	for _, p := range plan.Proposals {
		if p.AttemptID == "alpha-1" {
			producerRoute = *p.Route
		}
	}
	if producerRoute.ProviderInstanceID != "" || plannerDecision(t, plan, "alpha").RouteReresolution != nil {
		t.Fatal("producer with frozen dependent review must remain on its original route")
	}
	producerRoute = producer.Routes[0]
	for i := range input.Workflows[0].State.Attempts {
		a := &input.Workflows[0].State.Attempts[i]
		if a.ID == "alpha-1" {
			a.Progress, a.Control = domain.ProgressSucceeded, domain.ControlStopped
		}
	}
	next, err := BuildPlan(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Proposals) != 1 || next.Proposals[0].AttemptID != "beta-1" {
		t.Fatalf("reviewer was not planned after original producer succeeded: %+v", next.Proposals)
	}
	for _, p := range next.Proposals {
		if p.AttemptID == "beta-1" && p.Route.ProviderInstanceID == producerRoute.ProviderInstanceID {
			t.Fatalf("producer and cross-provider reviewer both planned on %s; frozen reviewer receipt still says %+v",
				p.Route.ProviderInstanceID, reviewer.RoleSelection.Diversity)
		}
	}
}

func TestRoleReresolutionAllowsLowerEffortOverride(t *testing.T) {
	task := roleTask("alpha")
	task.RoleEffort = "low"
	plan, err := BuildPlan(reresolutionInput(1, 1, task))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Proposals) != 1 || plan.Proposals[0].Route.Options["effort"] != "low" {
		t.Fatalf("lower effort override lost: %+v", plan.Proposals)
	}
}

func TestRoleReresolutionProtectsReviewDependencyInputs(t *testing.T) {
	producer := roleTask("alpha")
	reviewer := routingTask("beta", "claude", "opus")
	reviewer.ReviewJudge = true
	reviewer.DependencyInputs = map[string][]string{producer.ID: {"implementation"}}
	plan, err := BuildPlan(reresolutionInput(1, 1, producer, reviewer))
	if err != nil {
		t.Fatal(err)
	}
	if plannerDecision(t, plan, "alpha").RouteReresolution != nil {
		t.Fatal("producer of constrained dependency input moved")
	}
}

// Explicit pins and every other excluded case stay on the saturated route and
// report the concurrency blocker instead of moving.
func TestRoleReresolutionNeverMovesExcludedTasks(t *testing.T) {
	cases := map[string]func(*domain.Task, *PlanInput){
		"explicit pin": func(task *domain.Task, _ *PlanInput) {
			task.Role, task.RoleSelection = "", nil
		},
		"pin keeping an old receipt": func(task *domain.Task, _ *PlanInput) {
			task.Role = ""
		},
		"a retry": func(_ *domain.Task, input *PlanInput) {
			input.Workflows[0].State.Attempts[0].Number = 2
		},
		"an attempt that started": func(_ *domain.Task, input *PlanInput) {
			started := plannerTestTime.Add(-time.Minute)
			input.Workflows[0].State.Attempts[0].StartedAt = &started
		},
		"a candidate without a recorded effort": func(task *domain.Task, _ *PlanInput) {
			task.RoleSelection.Candidates[1].Effort = ""
		},
		"override without a recorded policy ceiling": func(task *domain.Task, _ *PlanInput) {
			task.RoleEffort = "low"
			task.RoleSelection.Candidates[1].Effort = ""
		},
		"a review-independence constraint": func(task *domain.Task, _ *PlanInput) {
			task.RoleSelection.Diversity.ProducerFamilies = []string{"anthropic"}
		},
		"a gated alternative": func(task *domain.Task, _ *PlanInput) {
			task.RoleSelection.Candidates[1].Band = "gated"
		},
		"an ineligible alternative": func(task *domain.Task, _ *PlanInput) {
			task.RoleSelection.Candidates[1].Eligible = false
		},
		"a saturated alternative": func(_ *domain.Task, input *PlanInput) {
			input.QuotaPools[1].ActiveAssignments = input.QuotaPools[1].MaxConcurrent
		},
		"a closed alternative": func(_ *domain.Task, input *PlanInput) {
			input.QuotaPools[1].Admission = domain.AdmissionClosed
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			task := roleTask("alpha")
			input := reresolutionInput(1, 1, task)
			task = input.Workflows[0].State.Tasks[0]
			mutate(&task, &input)
			input.Workflows[0].State.Tasks[0] = task
			plan, err := BuildPlan(input)
			if err != nil {
				t.Fatal(err)
			}
			decision := plannerDecision(t, plan, "alpha")
			if len(plan.Proposals) != 0 || decision.RouteReresolution != nil {
				t.Fatalf("proposals = %+v receipt = %+v, want the task left on its saturated route", plan.Proposals, decision.RouteReresolution)
			}
			if !candidateBlockerContains(decision.Candidates, PlanningBlockerPoolConcurrency, "pool-a") {
				t.Fatalf("candidates = %+v, want the pool-a concurrency blocker", decision.Candidates)
			}
		})
	}
}
