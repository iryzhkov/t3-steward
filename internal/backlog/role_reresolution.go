package backlog

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// roleAlternative is one other candidate of a role task's own receipt that the
// planner may try when the resolved route's quota pool is saturated.
type roleAlternative struct {
	task    domain.Task
	receipt *domain.RouteReresolution
}

// saturatedRoleAlternatives returns, best first, the other eligible candidates
// of a role task whose resolved route's quota pool is at its concurrency limit,
// counting active assignments and the assignments this pass has already
// planned. Each alternative is the task with that candidate as its only route.
//
// It returns nothing, and the task is planned exactly as submitted, unless
// every one of these holds:
//   - the task was resolved from a role (an explicit pin is never moved);
//   - its attempt is the first, ready and has never been assigned or started;
//   - it carries no review-independence constraint, so moving it can never put
//     a reviewer on its producer's provider family;
//   - the resolved route's pool is saturated and the candidate's pool has room
//     and is not closed or draining.
//
// A candidate needs a recorded policy effort. The task's own effort override
// must not exceed it. Receipts without candidate efforts are not re-resolved.
func (router *providerRouter) saturatedRoleAlternatives(task domain.Task, attempt domain.Attempt, now time.Time) []roleAlternative {
	selection := task.RoleSelection
	if task.Role == "" || selection == nil || len(task.Routes) != 1 || len(selection.Candidates) == 0 {
		return nil
	}
	if attempt.Number != 1 || attempt.AssignmentID != "" || attempt.StartedAt != nil ||
		attempt.Progress != domain.ProgressReady || attempt.AdminForceStart {
		return nil
	}
	if task.ReviewOutput != nil || task.ReviewRequirements != nil || task.ReviewJudge ||
		selection.Diversity.CrossProvider || len(selection.Diversity.ProducerFamilies) != 0 {
		return nil
	}
	current := task.Routes[0]
	if current.WorkerID != "" {
		return nil
	}
	currentRoute := current.ProviderInstanceID + "/" + current.Model
	fromPool := router.candidatePool(current.QuotaPoolID, current.ProviderInstanceID, verdictPool(selection.Candidates, currentRoute))
	if fromPool == "" || !router.poolSaturated(fromPool) {
		return nil
	}

	candidates := append([]domain.RoleCandidateVerdict(nil), selection.Candidates...)
	sort.SliceStable(candidates, func(i, j int) bool {
		left, right := domain.RouteRankBand(candidates[i].Band), domain.RouteRankBand(candidates[j].Band)
		if left != right {
			return left < right
		}
		return candidates[i].Ordinal < candidates[j].Ordinal
	})
	gated := domain.RouteRankBand("gated")
	var alternatives []roleAlternative
	for _, candidate := range candidates {
		if !candidate.Eligible || candidate.Route == currentRoute || domain.RouteRankBand(candidate.Band) >= gated {
			continue
		}
		instance, model, ok := strings.Cut(candidate.Route, "/")
		if !ok || instance == "" || model == "" {
			continue
		}
		effort := task.RoleEffort
		if effort == "" {
			effort = candidate.Effort
		}
		levels := map[string]int{"low": 1, "medium": 2, "high": 3}
		if levels[effort] == 0 || levels[candidate.Effort] == 0 || levels[effort] > levels[candidate.Effort] {
			continue
		}
		toPool := router.candidatePool("", instance, candidate.Pool)
		if toPool == "" || toPool == fromPool || router.poolSaturated(toPool) {
			continue
		}
		if admission := router.pools[toPool].Admission; admission == domain.AdmissionClosed || admission == domain.AdmissionDraining {
			continue
		}
		route := domain.ProviderRoute{ProviderInstanceID: instance, Model: model, Options: map[string]string{"effort": effort}}
		router.aliasEstimates(attempt.ID, current, route)
		moved := clonePlanningTask(task)
		moved.Routes = []domain.ProviderRoute{route}
		from := router.pools[fromPool]
		to := router.pools[toPool]
		alternatives = append(alternatives, roleAlternative{task: moved, receipt: &domain.RouteReresolution{
			Role: task.Role, FromRoute: currentRoute, FromPool: fromPool,
			ToRoute: candidate.Route, ToPool: toPool, Effort: effort, Ranking: selection.Ranking,
			Reason: fmt.Sprintf("quota pool %q has %d active or planned assignments at its concurrency limit of %d; role candidate %s in pool %q has room (%d of %d)",
				fromPool, from.ActiveAssignments+router.batchAssignments[fromPool], from.MaxConcurrent,
				candidate.Route, toPool, to.ActiveAssignments+router.batchAssignments[toPool], to.MaxConcurrent),
			DecidedAt: now.UTC(),
		}})
	}
	return alternatives
}

// candidatePool names the fleet quota pool a route draws on: the route's own
// pool, else the pool its receipt recorded, else the only pool its provider
// instance belongs to. It is empty when none of those is a known pool.
func (router *providerRouter) candidatePool(routePool, instance, recorded string) string {
	for _, poolID := range []string{routePool, recorded} {
		if _, known := router.pools[poolID]; poolID != "" && known {
			return poolID
		}
	}
	if pools := router.instancePools[instance]; len(pools) == 1 {
		return pools[0]
	}
	return ""
}

// poolSaturated reports whether a pool's active and already planned
// assignments have reached its concurrency limit.
func (router *providerRouter) poolSaturated(poolID string) bool {
	pool, known := router.pools[poolID]
	return known && pool.ActiveAssignments+router.batchAssignments[poolID] >= pool.MaxConcurrent
}

// aliasEstimates gives the alternative route the remaining-cost and runtime
// estimate the resolved route has on every worker. Cold-start estimates depend
// on the task, not on the route, so the alternative is estimated exactly as the
// route it replaces.
func (router *providerRouter) aliasEstimates(attemptID string, from, to domain.ProviderRoute) {
	for workerID := range router.workers {
		source, target := cloneProviderRoute(from), cloneProviderRoute(to)
		source.WorkerID, target.WorkerID = workerID, workerID
		estimate, found := router.estimates[routeEstimateKey(attemptID, source)]
		if !found {
			continue
		}
		if _, exists := router.estimates[routeEstimateKey(attemptID, target)]; !exists {
			router.estimates[routeEstimateKey(attemptID, target)] = estimate
		}
	}
}

func verdictPool(verdicts []domain.RoleCandidateVerdict, route string) string {
	for _, verdict := range verdicts {
		if verdict.Route == route {
			return verdict.Pool
		}
	}
	return ""
}

// hasDependentReviewConstraint protects frozen downstream review selections.
// Producer routes stay fixed when any dependent review carries an independence
// constraint; changing the producer would invalidate that immutable receipt.
func hasDependentReviewConstraint(producer domain.Task, tasks []domain.Task) bool {
	for _, task := range tasks {
		selection := task.RoleSelection
		constrained := task.ReviewOutput != nil || task.ReviewRequirements != nil || task.ReviewJudge ||
			(selection != nil && (selection.Diversity.CrossProvider || len(selection.Diversity.ProducerFamilies) != 0))
		if !constrained {
			continue
		}
		for _, name := range task.Needs {
			if name == producer.Name || name == producer.ID {
				return true
			}
		}
		for name := range task.DependencyInputs {
			if name == producer.Name || name == producer.ID {
				return true
			}
		}
	}
	return false
}

// planEntry plans one task, first on the alternatives of a saturated role task
// and otherwise as submitted. An alternative is used only if it produces a
// proposal; when none does, the decision is the one for the submitted route,
// so explain reports the saturated pool rather than an alternative's blocker.
func planEntry(input PlanInput, router *providerRouter, constraints []PlanningConstraintSession, entry planningTaskEntry, resourceOwners, checkoutOwners map[string]string) (TaskPlanningDecision, *ProposedTask, domain.Task, error) {
	if entry.ready && !hasDependentReviewConstraint(entry.task, entry.state.Tasks) {
		for _, alternative := range router.saturatedRoleAlternatives(entry.task, entry.attempt, input.Now) {
			decision, proposal, err := planTask(input, router, constraints, entry.workflow, entry.state,
				alternative.task, entry.attempt, entry.order, resourceOwners, checkoutOwners)
			if err != nil {
				return TaskPlanningDecision{}, nil, domain.Task{}, err
			}
			if proposal == nil {
				continue
			}
			decision.RouteReresolution = domain.CloneRouteReresolution(alternative.receipt)
			if proposal.Placement != nil {
				proposal.Placement.RouteReresolution = domain.CloneRouteReresolution(alternative.receipt)
			}
			return decision, proposal, alternative.task, nil
		}
	}
	decision, proposal, err := planTask(input, router, constraints, entry.workflow, entry.state,
		entry.task, entry.attempt, entry.order, resourceOwners, checkoutOwners)
	return decision, proposal, entry.task, err
}
