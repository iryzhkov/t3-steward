package backlog

import (
	"slices"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// SupervisionRouteRequest is everything the route-availability answer depends
// on: the route the run declared, the workers that could host it, and whether
// this coordinator has a supervisor admin client to dispatch an overseer as.
//
// The three conditions live together because they answer one question, "could
// an overseer decide this run's gates right now", and an answer that consults
// only some of them is wrong in the most expensive way: it reports a gate as
// merely pending when in fact nothing in the deployment can ever decide it.
type SupervisionRouteRequest struct {
	Route   domain.ProviderRoute
	Workers []domain.WorkerSnapshot
	// SupervisorClientConfigured reports that exactly one
	// backlog_v2.coordinator.admin_clients entry declares supervisor: true. An
	// overseer authenticates as that client and as no other, so without one the
	// coordinator dispatches no activation at all and the route is unavailable
	// however capable the fleet is.
	SupervisorClientConfigured bool
}

// SupervisionRouteAvailable reports whether some fresh worker can actually run
// this run's overseer: this coordinator must have a supervisor admin client,
// and the worker must host the configured provider instance and model and
// advertise the campaign-supervision capability.
//
// The snapshot inventory is the authority, not the coordinator's
// configuration, because the capability describes the build running on that
// host. This is the same rule the worker exchange applies before it sets a
// causal acknowledgement.
func SupervisionRouteAvailable(request SupervisionRouteRequest) bool {
	if !request.SupervisorClientConfigured {
		return false
	}
	for _, worker := range request.Workers {
		if request.Route.WorkerID != "" && worker.WorkerID != request.Route.WorkerID {
			continue
		}
		if !slices.Contains(worker.Inventory.Capabilities, workerproto.CapabilityCampaignSupervision) {
			continue
		}
		if supervisionWorkerServesRoute(worker, request.Route) {
			return true
		}
	}
	return false
}

func supervisionWorkerServesRoute(worker domain.WorkerSnapshot, route domain.ProviderRoute) bool {
	for _, provider := range worker.Inventory.Providers {
		if provider.InstanceID != route.ProviderInstanceID {
			continue
		}
		if route.Model == "" || slices.Contains(provider.Models, route.Model) {
			return true
		}
	}
	return false
}

// SupervisionRouteBlockCause names, in one phrase, why the supervisor route
// cannot run. It is the wording the review incident records, the planner
// blocker carries and both "campaign check" and "campaign supervision show"
// print, so an operator meets the same sentence wherever the block surfaces.
//
// The two causes are told apart because the remedies are different. A route no
// worker hosts is fixed on a worker; a missing supervisor client is fixed in
// the coordinator's own configuration, and silence about it sent operators
// looking at workers that were never the problem.
func SupervisionRouteBlockCause(supervisorClientConfigured bool) string {
	if !supervisorClientConfigured {
		return SupervisionNoSupervisorClient
	}
	return SupervisionRouteUnadmitted
}

const (
	// SupervisionNoSupervisorClient is the cause reported when this coordinator
	// has no backlog_v2.coordinator.admin_clients entry with supervisor: true.
	// No overseer can be dispatched at all, so the gate waits for a person.
	SupervisionNoSupervisorClient = "no supervisor client configured; operator decision required"
	// SupervisionRouteUnadmitted is the cause reported when a supervisor client
	// exists but no fresh worker can currently host the overseer route.
	SupervisionRouteUnadmitted = "the supervisor route cannot be admitted"
)

// SupervisionPlanningBlockers is the planner's own supervision verdict,
// exported so that "campaign explain" reports exactly the blockers the planner
// applied rather than a second implementation of the same rules.
//
// A second copy is what produced the defect this exists to close: explain
// reported a gate-protected task as eligible to start while the planner was
// withholding it, and the two answers had no shared code to disagree in.
func SupervisionPlanningBlockers(
	snapshots map[string]domain.SupervisionSnapshot,
	runID string,
	task domain.Task,
) []PlanningBlocker {
	return supervisionBlockers(snapshots, runID, task)
}
