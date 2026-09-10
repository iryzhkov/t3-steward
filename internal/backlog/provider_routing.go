package backlog

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const (
	PlanningBlockerRouteWorkerMismatch  = "route-worker-mismatch"
	PlanningBlockerProviderUnavailable  = "provider-instance-unavailable"
	PlanningBlockerModelUnavailable     = "provider-model-unavailable"
	PlanningBlockerQuotaPoolUnavailable = "route-quota-pool-unavailable"
	PlanningBlockerRouteEstimateMissing = "route-estimate-missing"
	PlanningBlockerPoolConcurrency      = "quota-pool-concurrency"
)

// RouteEstimate is the remaining cost and runtime for one attempt on one
// worker/provider/model/options combination.
type RouteEstimate struct {
	AttemptID          string
	WorkerID           string
	ProviderInstanceID string
	Model              string
	Options            map[string]string
	Estimate           TaskAdmissionEstimate
}

type providerRouter struct {
	workers          map[string]domain.WorkerInventory
	pools            map[string]domain.QuotaPool
	instancePools    map[string][]string
	estimates        map[string]TaskAdmissionEstimate
	batchAssignments map[string]int
}

type routedCandidate struct {
	candidate PlanningCandidate
	blockers  []PlanningBlocker
}

func newProviderRouter(input PlanInput) (*providerRouter, error) {
	router := &providerRouter{
		workers:          make(map[string]domain.WorkerInventory, len(input.Workers)),
		pools:            make(map[string]domain.QuotaPool, len(input.QuotaPools)),
		instancePools:    make(map[string][]string),
		estimates:        make(map[string]TaskAdmissionEstimate, len(input.RouteEstimates)),
		batchAssignments: make(map[string]int),
	}
	for _, worker := range input.Workers {
		router.workers[worker.ID] = cloneRouteWorker(worker)
	}
	pools := append([]domain.QuotaPool(nil), input.QuotaPools...)
	sort.Slice(pools, func(i, j int) bool { return pools[i].ID < pools[j].ID })
	for _, pool := range pools {
		if err := validateRoutingPool(pool); err != nil {
			return nil, err
		}
		if _, duplicate := router.pools[pool.ID]; duplicate {
			return nil, fmt.Errorf("provider routing repeats quota pool %q", pool.ID)
		}
		pool.ProviderInstanceIDs = append([]string(nil), pool.ProviderInstanceIDs...)
		sort.Strings(pool.ProviderInstanceIDs)
		router.pools[pool.ID] = pool
		for _, instanceID := range pool.ProviderInstanceIDs {
			router.instancePools[instanceID] = append(router.instancePools[instanceID], pool.ID)
		}
	}
	estimates := append([]RouteEstimate(nil), input.RouteEstimates...)
	sort.Slice(estimates, func(i, j int) bool {
		return routeEstimateInputKey(estimates[i]) < routeEstimateInputKey(estimates[j])
	})
	for _, estimate := range estimates {
		if err := validateRouteEstimate(estimate); err != nil {
			return nil, err
		}
		key := routeEstimateKey(estimate.AttemptID, domain.ProviderRoute{
			WorkerID: estimate.WorkerID, ProviderInstanceID: estimate.ProviderInstanceID,
			Model: estimate.Model, Options: estimate.Options,
		})
		if _, duplicate := router.estimates[key]; duplicate {
			return nil, fmt.Errorf("provider routing repeats estimate for attempt %q worker %q instance %q model %q", estimate.AttemptID, estimate.WorkerID, estimate.ProviderInstanceID, estimate.Model)
		}
		router.estimates[key] = estimate.Estimate
	}
	return router, nil
}

func (router *providerRouter) Candidates(task domain.Task, attempt domain.Attempt, workerIDs []string) []routedCandidate {
	if len(task.Routes) == 0 {
		result := make([]routedCandidate, 0, len(workerIDs))
		for _, workerID := range workerIDs {
			result = append(result, routedCandidate{candidate: PlanningCandidate{
				Task: task, Attempt: attempt, WorkerID: workerID,
			}})
		}
		return result
	}

	result := make([]routedCandidate, 0, len(task.Routes)*len(workerIDs))
	for routeIndex, preference := range task.Routes {
		for _, workerID := range workerIDs {
			result = append(result, router.resolveCandidate(task, attempt, workerID, routeIndex+1, preference))
		}
	}
	return result
}

func (router *providerRouter) Reserve(candidate PlanningCandidate) {
	if candidate.Route == nil || candidate.Route.QuotaPoolID == "" {
		return
	}
	router.batchAssignments[candidate.Route.QuotaPoolID]++
}

func (router *providerRouter) resolveCandidate(task domain.Task, attempt domain.Attempt, workerID string, routeOrdinal int, preference domain.ProviderRoute) routedCandidate {
	route := cloneProviderRoute(preference)
	route.WorkerID = workerID
	candidate := PlanningCandidate{
		Task: task, Attempt: attempt, WorkerID: workerID, RouteOrdinal: routeOrdinal, Route: &route,
	}
	base := PlanningBlocker{
		WorkerID: workerID, RouteOrdinal: routeOrdinal,
		ProviderInstanceID: preference.ProviderInstanceID, Model: preference.Model,
	}
	if preference.WorkerID != "" && preference.WorkerID != workerID {
		blocker := base
		blocker.Code = PlanningBlockerRouteWorkerMismatch
		blocker.Detail = fmt.Sprintf("route %d requires worker %q", routeOrdinal, preference.WorkerID)
		return routedCandidate{candidate: candidate, blockers: []PlanningBlocker{blocker}}
	}

	worker := router.workers[workerID]
	provider, found := workerProvider(worker, preference.ProviderInstanceID)
	if !found || !provider.Available {
		blocker := base
		blocker.Code = PlanningBlockerProviderUnavailable
		if found {
			blocker.Detail = fmt.Sprintf("provider instance %q is unavailable on worker %q", preference.ProviderInstanceID, workerID)
		} else {
			blocker.Detail = fmt.Sprintf("provider instance %q is not installed on worker %q", preference.ProviderInstanceID, workerID)
		}
		return routedCandidate{candidate: candidate, blockers: []PlanningBlocker{blocker}}
	}
	if !containsString(provider.Models, preference.Model) {
		blocker := base
		blocker.Code = PlanningBlockerModelUnavailable
		blocker.Detail = fmt.Sprintf("model %q is unavailable from provider instance %q on worker %q", preference.Model, preference.ProviderInstanceID, workerID)
		return routedCandidate{candidate: candidate, blockers: []PlanningBlocker{blocker}}
	}

	poolID, poolBlocker := router.resolvePool(routeOrdinal, workerID, preference, provider)
	if poolBlocker != nil {
		return routedCandidate{candidate: candidate, blockers: []PlanningBlocker{*poolBlocker}}
	}
	route.QuotaPoolID = poolID
	candidate.Route = &route
	pool := router.pools[poolID]
	var blockers []PlanningBlocker
	if pool.ActiveAssignments+router.batchAssignments[poolID] >= pool.MaxConcurrent {
		blockers = append(blockers, PlanningBlocker{
			Code:     PlanningBlockerPoolConcurrency,
			Detail:   fmt.Sprintf("quota pool %q has %d active or planned assignments at its limit of %d", poolID, pool.ActiveAssignments+router.batchAssignments[poolID], pool.MaxConcurrent),
			WorkerID: workerID, RouteOrdinal: routeOrdinal,
			ProviderInstanceID: route.ProviderInstanceID, Model: route.Model, QuotaPoolID: poolID,
		})
	}
	if estimate, found := router.estimates[routeEstimateKey(attempt.ID, route)]; found {
		candidate.Estimate = cloneTaskAdmissionEstimate(estimate)
	} else {
		blockers = append(blockers, PlanningBlocker{
			Code:     PlanningBlockerRouteEstimateMissing,
			Detail:   fmt.Sprintf("route %d has no remaining-cost and runtime estimate for attempt %q", routeOrdinal, attempt.ID),
			WorkerID: workerID, RouteOrdinal: routeOrdinal,
			ProviderInstanceID: route.ProviderInstanceID, Model: route.Model, QuotaPoolID: poolID,
		})
	}
	return routedCandidate{candidate: candidate, blockers: blockers}
}

func (router *providerRouter) resolvePool(routeOrdinal int, workerID string, preference domain.ProviderRoute, provider domain.WorkerProviderInventory) (string, *PlanningBlocker) {
	poolID := preference.QuotaPoolID
	if poolID != "" && provider.QuotaPoolID != "" && poolID != provider.QuotaPoolID {
		return "", router.poolBlocker(routeOrdinal, workerID, preference, poolID,
			fmt.Sprintf("route quota pool %q conflicts with worker inventory pool %q", poolID, provider.QuotaPoolID))
	}
	if poolID == "" {
		poolID = provider.QuotaPoolID
	}
	if poolID == "" {
		pools := router.instancePools[preference.ProviderInstanceID]
		if len(pools) > 1 {
			return "", router.poolBlocker(routeOrdinal, workerID, preference, "",
				fmt.Sprintf("provider instance %q maps to multiple fleet quota pools; worker inventory must select one", preference.ProviderInstanceID))
		}
		if len(pools) == 1 {
			poolID = pools[0]
		}
	}
	pool, found := router.pools[poolID]
	if poolID == "" || !found {
		return "", router.poolBlocker(routeOrdinal, workerID, preference, poolID,
			fmt.Sprintf("provider instance %q has no fleet quota pool", preference.ProviderInstanceID))
	}
	if !containsString(pool.ProviderInstanceIDs, preference.ProviderInstanceID) {
		return "", router.poolBlocker(routeOrdinal, workerID, preference, poolID,
			fmt.Sprintf("quota pool %q does not include provider instance %q", poolID, preference.ProviderInstanceID))
	}
	return poolID, nil
}

func (router *providerRouter) poolBlocker(routeOrdinal int, workerID string, route domain.ProviderRoute, poolID, detail string) *PlanningBlocker {
	return &PlanningBlocker{
		Code: PlanningBlockerQuotaPoolUnavailable, Detail: detail,
		WorkerID: workerID, RouteOrdinal: routeOrdinal,
		ProviderInstanceID: route.ProviderInstanceID, Model: route.Model, QuotaPoolID: poolID,
	}
}

func validateRoutingPool(pool domain.QuotaPool) error {
	if strings.TrimSpace(pool.ID) != pool.ID || pool.ID == "" {
		return errors.New("provider routing quota pool ID must be nonempty and trimmed")
	}
	if pool.MaxConcurrent <= 0 {
		return fmt.Errorf("provider routing quota pool %q maximum concurrency must be positive", pool.ID)
	}
	if pool.ActiveAssignments < 0 {
		return fmt.Errorf("provider routing quota pool %q active assignments cannot be negative", pool.ID)
	}
	instances := append([]string(nil), pool.ProviderInstanceIDs...)
	for _, instanceID := range instances {
		if strings.TrimSpace(instanceID) != instanceID || instanceID == "" {
			return fmt.Errorf("provider routing quota pool %q instance IDs must be nonempty and trimmed", pool.ID)
		}
	}
	sort.Strings(instances)
	for index := 1; index < len(instances); index++ {
		if instances[index] == instances[index-1] {
			return fmt.Errorf("provider routing quota pool %q repeats provider instance %q", pool.ID, instances[index])
		}
	}
	return nil
}

func validateRouteEstimate(estimate RouteEstimate) error {
	fields := []struct {
		label string
		value string
	}{
		{"attempt", estimate.AttemptID},
		{"worker", estimate.WorkerID},
		{"provider instance", estimate.ProviderInstanceID},
		{"model", estimate.Model},
	}
	for _, field := range fields {
		if strings.TrimSpace(field.value) != field.value || field.value == "" {
			return fmt.Errorf("provider routing estimate %s must be nonempty and trimmed", field.label)
		}
	}
	for option, value := range estimate.Options {
		if strings.TrimSpace(option) != option || option == "" || strings.TrimSpace(value) != value || value == "" {
			return errors.New("provider routing estimate options must have nonempty trimmed names and values")
		}
	}
	if !positiveFinite(estimate.Estimate.RemainingCost) {
		return fmt.Errorf("provider routing estimate remaining cost must be positive and finite")
	}
	if estimate.Estimate.ExpectedRuntime <= 0 {
		return fmt.Errorf("provider routing estimate runtime must be positive")
	}
	if estimate.Estimate.CheckpointMargin < 0 {
		return fmt.Errorf("provider routing estimate checkpoint margin cannot be negative")
	}
	return nil
}

func routeEstimateInputKey(estimate RouteEstimate) string {
	return routeEstimateKey(estimate.AttemptID, domain.ProviderRoute{
		WorkerID: estimate.WorkerID, ProviderInstanceID: estimate.ProviderInstanceID,
		Model: estimate.Model, Options: estimate.Options,
	})
}

func routeEstimateKey(attemptID string, route domain.ProviderRoute) string {
	keys := make([]string, 0, len(route.Options))
	for key := range route.Options {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var builder strings.Builder
	builder.WriteString(attemptID)
	builder.WriteByte(0)
	builder.WriteString(route.WorkerID)
	builder.WriteByte(0)
	builder.WriteString(route.ProviderInstanceID)
	builder.WriteByte(0)
	builder.WriteString(route.Model)
	for _, key := range keys {
		builder.WriteByte(0)
		builder.WriteString(key)
		builder.WriteByte('=')
		builder.WriteString(route.Options[key])
	}
	return builder.String()
}

func workerProvider(worker domain.WorkerInventory, instanceID string) (domain.WorkerProviderInventory, bool) {
	for _, provider := range worker.Providers {
		if provider.InstanceID == instanceID {
			return provider, true
		}
	}
	return domain.WorkerProviderInventory{}, false
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func cloneRouteWorker(worker domain.WorkerInventory) domain.WorkerInventory {
	worker.Capabilities = append([]string(nil), worker.Capabilities...)
	worker.Projects = append([]domain.WorkerProjectInventory(nil), worker.Projects...)
	worker.Providers = append([]domain.WorkerProviderInventory(nil), worker.Providers...)
	for index := range worker.Providers {
		worker.Providers[index].Models = append([]string(nil), worker.Providers[index].Models...)
	}
	return worker
}

func cloneProviderRoute(route domain.ProviderRoute) domain.ProviderRoute {
	route.Options = cloneStringMap(route.Options)
	return route
}

func cloneProviderRoutePointer(route *domain.ProviderRoute) *domain.ProviderRoute {
	if route == nil {
		return nil
	}
	copied := cloneProviderRoute(*route)
	return &copied
}

func cloneTaskAdmissionEstimate(estimate TaskAdmissionEstimate) *TaskAdmissionEstimate {
	copied := estimate
	return &copied
}

func cloneTaskAdmissionEstimatePointer(estimate *TaskAdmissionEstimate) *TaskAdmissionEstimate {
	if estimate == nil {
		return nil
	}
	return cloneTaskAdmissionEstimate(*estimate)
}
