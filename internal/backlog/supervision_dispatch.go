package backlog

// Turning a planned activation into assigned work.
//
// PlanActivation decides that an overseer should wake and issues its lease and
// its deterministic dispatch identity. This file is what happens next: the
// activation becomes an ordinary attempt and an ordinary assignment, offered to
// an ordinary worker, claimed with the ordinary fences and executed as an
// ordinary T3 session. Nothing here is a second dispatch engine; every durable
// effect lands through the assignment machinery a task already uses, which is
// what makes lease expiry, undelivered retry, started-then-vanished and
// operator revocation reconcile without a second implementation of each.
//
// What is deliberately different is only what an activation is not. It carries
// no declared task, so it is absent from the run's graph and from its sink; it
// takes no workspace lock, so it can never wait on the task it reviews; and its
// turn is never verified as a task result.

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ErrActivationUnplaceable reports that no worker can run this activation now.
//
// It is a temporary condition, not a failure of the run: the gate stays closed,
// unrelated branches keep being scheduled, and the coordinator's own route
// availability check is what turns a persistent one into an escalation.
var ErrActivationUnplaceable = errors.New("supervision activation: no eligible worker")

// ActivationAttemptID is the deterministic attempt identity of one activation
// dispatch.
//
// It is derived from the dispatch identity rather than minted, so that a
// coordinator which restarts between planning the activation and committing its
// assignment recomputes the same attempt instead of creating a second one. A
// retry of a provably undelivered dispatch reuses the same dispatch identity and
// therefore lands on the same attempt, which is exactly what "retried with the
// original identity" has to mean once the retry is durable.
func ActivationAttemptID(dispatchIdentity string) string {
	return stableCoordinatorID("activation-attempt", dispatchIdentity)
}

// ActivationPlacement is one worker chosen to run one activation, with the
// snapshot identity the commit is fenced on.
type ActivationPlacement struct {
	WorkerID string
	// WorkerEpoch and SnapshotSequence are the snapshot the placement read. The
	// store refuses the commit when the worker has moved since, exactly as it
	// does for a task assignment.
	WorkerEpoch      string
	SnapshotSequence int64
	Route            domain.ProviderRoute
	// Excluded explains every worker that was not chosen, in worker order, so a
	// refusal is actionable rather than merely negative.
	Excluded []string
}

// ActivationPlacementRequest is everything placement reads.
type ActivationPlacementRequest struct {
	// Route is the run's independently configured overseer route. A worker that
	// does not host it is not a candidate: a supervised run never silently
	// falls back to a weaker model or to the workers' own pool.
	Route   domain.ProviderRoute
	Workers []domain.WorkerSnapshot
	Epoch   int64
	Now     time.Time
	// Admission is the existing fail-closed quota predicate, called rather than
	// reimplemented. An overseer obeys the same automatic admission gates as
	// every other route; a stronger model buys no priority.
	Admission WorkerAdmissionPolicy
}

// PlaceActivation chooses the worker an activation is offered to.
//
// The three conditions are the plan's, in the order a reader would ask them:
// the worker is current and accepting work, it advertises the campaign
// supervision capability, and it hosts the run's configured overseer route.
// Capability is read from the worker's own durable snapshot inventory, never
// from coordinator configuration, because the capability describes the build
// running on that host and only that host can report it.
func PlaceActivation(request ActivationPlacementRequest) (ActivationPlacement, error) {
	if request.Route.ProviderInstanceID == "" || request.Route.Model == "" {
		return ActivationPlacement{}, errors.New("supervision activation: the overseer route needs a provider instance and a model")
	}
	now := request.Now.UTC()
	workers := append([]domain.WorkerSnapshot(nil), request.Workers...)
	sort.Slice(workers, func(left, right int) bool { return workers[left].WorkerID < workers[right].WorkerID })
	placement := ActivationPlacement{}
	for _, worker := range workers {
		exclude := func(reason string) {
			placement.Excluded = append(placement.Excluded, fmt.Sprintf("%s: %s", worker.WorkerID, reason))
		}
		if request.Epoch != 0 && worker.CoordinatorEpoch != request.Epoch {
			exclude("the snapshot belongs to another coordinator epoch")
			continue
		}
		if !worker.Connected || !worker.ValidUntil.After(now) ||
			!worker.Inventory.AcceptBacklog || worker.Inventory.Health != domain.WorkerHealthReady {
			exclude("the worker is disconnected, stale, or not accepting work")
			continue
		}
		if request.Route.WorkerID != "" && worker.WorkerID != request.Route.WorkerID {
			exclude("the overseer route names another worker")
			continue
		}
		if err := RequireSupervisionCapability(worker.WorkerID, worker.Inventory.Capabilities); err != nil {
			exclude(err.Error())
			continue
		}
		route, hosted := activationRouteOnWorker(worker, request.Route)
		if !hosted {
			exclude("the worker does not host the configured overseer provider and model")
			continue
		}
		if !request.Admission.AllowsNewWork(route.QuotaPoolID) {
			exclude(fmt.Sprintf("quota pool %q is not admitting new work", route.QuotaPoolID))
			continue
		}
		placement.WorkerID = worker.WorkerID
		placement.WorkerEpoch = worker.WorkerEpoch
		placement.SnapshotSequence = worker.Sequence
		placement.Route = route
		return placement, nil
	}
	return placement, fmt.Errorf("%w for route %s/%s: %s", ErrActivationUnplaceable,
		request.Route.ProviderInstanceID, request.Route.Model, strings.Join(placement.Excluded, "; "))
}

// activationRouteOnWorker binds the configured route to the worker that will
// run it, resolving the quota pool from the worker's own provider inventory.
//
// The pool has to come from the worker rather than from the manifest: the
// admission predicate is keyed on it, and a route that named a pool the worker
// does not actually draw from would be admitted against the wrong limit.
func activationRouteOnWorker(worker domain.WorkerSnapshot, route domain.ProviderRoute) (domain.ProviderRoute, bool) {
	for _, provider := range worker.Inventory.Providers {
		if provider.InstanceID != route.ProviderInstanceID || !provider.Available {
			continue
		}
		if route.Model != "" && !slices.Contains(provider.Models, route.Model) {
			continue
		}
		bound := route
		bound.WorkerID = worker.WorkerID
		if bound.QuotaPoolID == "" {
			bound.QuotaPoolID = provider.QuotaPoolID
		}
		return bound, bound.QuotaPoolID != ""
	}
	return domain.ProviderRoute{}, false
}

// ActivationAssignmentCommit is one activation's attempt and assignment,
// offered together, fenced on the coordinator epoch and the worker snapshot the
// placement read.
type ActivationAssignmentCommit struct {
	RunID                  string
	ActivationID           string
	Epoch                  int64
	CoordinatorEpoch       int64
	Attempt                domain.Attempt
	Assignment             domain.Assignment
	WorkerEpoch            string
	WorkerSnapshotSequence int64
	CommittedAt            time.Time
}

// ActivationAssignment builds the attempt and the offered assignment that carry
// one activation to its worker.
//
// Every identity is derived from the activation's own deterministic dispatch
// identity, so the whole record set is reproducible: planning it twice produces
// the same attempt, the same assignment, the same lease and dispatch tokens and
// the same T3 thread. That is what makes a retry of an undelivered dispatch a
// retry rather than a second overseer.
func ActivationAssignment(
	activation domain.Activation,
	dispatch ActivationDispatch,
	placement ActivationPlacement,
	now time.Time,
) (domain.Attempt, domain.Assignment, error) {
	switch {
	case strings.TrimSpace(activation.ID) == "" || strings.TrimSpace(activation.RunID) == "":
		return domain.Attempt{}, domain.Assignment{}, errors.New("supervision activation: the activation has no identity")
	case strings.TrimSpace(dispatch.Identity) == "" || dispatch.Epoch < 1:
		return domain.Attempt{}, domain.Assignment{}, errors.New("supervision activation: the dispatch has no identity or epoch")
	case placement.WorkerID == "" || placement.WorkerEpoch == "":
		return domain.Attempt{}, domain.Assignment{}, errors.New("supervision activation: the dispatch has no placed worker")
	case dispatch.RequiredCapability != SupervisionWorkerCapability:
		return domain.Attempt{}, domain.Assignment{}, fmt.Errorf(
			"supervision activation: the dispatch requires %q, not %q",
			dispatch.RequiredCapability, SupervisionWorkerCapability)
	}
	now = now.UTC()
	attemptID := ActivationAttemptID(dispatch.Identity)
	attempt := domain.Attempt{
		SupervisionActivationID:    activation.ID,
		SupervisionActivationEpoch: dispatch.Epoch,
		ID:                         attemptID,
		WorkflowRunID:              activation.RunID,
		// The activation names itself as its own task. There is no declared task
		// to name, and leaving the field empty would make an activation attempt
		// indistinguishable from a corrupt one in every audit row that records a
		// task ID.
		TaskID:    activation.ID,
		Number:    1,
		Progress:  domain.ProgressReady,
		Control:   domain.ControlUnassigned,
		UpdatedAt: now,
	}
	assignmentID := stableCoordinatorID("assignment", attemptID)
	assignment := domain.Assignment{
		ID:        assignmentID,
		AttemptID: attemptID,
		WorkerID:  placement.WorkerID,
		Route:     placement.Route,
		State:     domain.AssignmentOffered,
		// The assignment epoch is the activation epoch. An activation at a new
		// epoch is a different attempt with a different assignment, so the two
		// counters cannot disagree.
		Epoch:         dispatch.Epoch,
		LeaseToken:    stableCoordinatorID("lease", assignmentID),
		DispatchToken: stableCoordinatorID("dispatch", assignmentID),
		ThreadID:      stableCoordinatorID("thread", assignmentID),
	}
	return attempt, assignment, nil
}
