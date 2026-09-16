package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// The coordinator-side supervision boundary.
//
// It does three things per boundary, all of them observations rather than
// decisions: it notices that a gate's producers have succeeded and makes the
// gate ready for review, it raises one review incident per newly ready gate so
// the decision has an identity to close, and it escalates to the configured
// notify thread when the supervisor route cannot run at all, because a gate that
// nobody can decide must not stall silently.
//
// It never decides a gate. Acceptance and rejection arrive through the admin
// supervision operation, from an overseer activation or from an operator.
type coordinatorSupervision struct {
	store  backlog.CoordinatorSupervisionStore
	logger *slog.Logger
	now    func() time.Time
	// workers is the durable worker view the route-availability answer is read
	// from. It is a function rather than a snapshot so each boundary reads what
	// the fleet looks like now.
	workers func(context.Context) ([]domain.WorkerSnapshot, error)
	// activations drives the overseer activation lifecycle. A zero value
	// disables activation dispatch and leaves the observations above unchanged,
	// which is the deployment that has no supervisor credential configured.
	activations backlog.SupervisionActivationService
	settings    coordinatorActivationSettings
}

func (c coordinatorSupervision) at() time.Time {
	if c.now != nil {
		return c.now().UTC()
	}
	return time.Now().UTC()
}

// Tick advances every supervised run of this coordinator.
func (c coordinatorSupervision) Tick(ctx context.Context) {
	records, err := c.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		c.logger.Error("load records for supervision boundary", "error", err)
		return
	}
	var snapshots []domain.WorkerSnapshot
	if c.workers != nil {
		if snapshots, err = c.workers(ctx); err != nil {
			c.logger.Error("load worker snapshots for supervision boundary", "error", err)
		}
	}
	now := c.at()
	for _, run := range records.WorkflowRuns {
		if run.Supervision == nil || run.Progress.Terminal() {
			continue
		}
		if err := c.advanceRun(ctx, run, snapshots, now); err != nil {
			c.logger.Error("supervision boundary failed", "run", run.ID, "error", err)
		}
	}
}

func (c coordinatorSupervision) advanceRun(
	ctx context.Context,
	run domain.WorkflowRun,
	workers []domain.WorkerSnapshot,
	now time.Time,
) (err error) {
	advanced, err := c.store.AdvanceSupervisionGates(ctx, run.ID, now)
	if err != nil {
		return err
	}
	routeAvailable := supervisionRouteAvailable(run.Supervision.Config.Route, workers)
	// Route availability is a condition of every boundary while a gate awaits
	// review, not only of the boundary that made one ready: the worker that
	// would decide an open gate can be decommissioned at any later tick, and
	// the run would otherwise wait forever with nobody told.
	defer func() {
		if sweepErr := c.escalateRouteBlock(ctx, run, routeAvailable, now); sweepErr != nil && err == nil {
			err = sweepErr
		}
	}()
	if len(advanced) == 0 {
		return nil
	}
	for _, gate := range advanced {
		incidentID := supervisionGateIncidentID(run.ID, gate.Gate.Definition.ID, gate.Evidence.ID)
		eventID := "supervision-event:" + incidentID
		reason := fmt.Sprintf("gate %s is ready for review", gate.Gate.Definition.Name)
		_, err := c.store.OpenReviewIncident(ctx, sqlite.IncidentRequest{
			RunID: run.ID, IncidentID: incidentID, RequestID: incidentID,
			Actor:         domain.Actor{Kind: domain.ActorOperator, Principal: coordinatorSupervisionPrincipal},
			SourceEventID: eventID, GateID: gate.Gate.Definition.ID,
			RequiredDisposition: domain.DispositionGateDecision,
			Reason:              reason, OpenedAt: now,
		})
		switch {
		case err == nil:
		case errors.Is(err, sqlite.ErrSupervisionRequestConflict):
			// The same gate and the same evidence raise the same incident, so a
			// repeated boundary adds nothing.
			continue
		default:
			return err
		}
		if _, err := c.store.AppendSupervisionEvents(ctx, run.ID, []backlog.SupervisionEvent{{
			ID: eventID, RunID: run.ID, Kind: backlog.TriggerGateReviewReady,
			Reason: reason, GateID: gate.Gate.Definition.ID, IncidentID: incidentID,
			GraphRevision: gate.Gate.GraphRevision, OccurredAt: now,
		}}); err != nil {
			return err
		}
		if routeAvailable {
			continue
		}
		// No worker can run this run's overseer, so the gate stays closed and
		// nobody would ever be told why. One escalation says so, deduplicated on
		// the incident.
		if err := c.escalate(ctx, *run.Supervision, incidentID, eventID,
			"the supervisor route cannot be admitted, so "+reason+" and no activation can decide it", now); err != nil {
			return err
		}
	}
	return nil
}

// escalateRouteBlock raises one escalation for every open gate review this run
// cannot obtain, once the block has persisted past the configured threshold.
//
// The threshold is idle_escalation_after: a review nobody can perform is
// exactly the idle the author configured it for. A run that configured none
// escalates on the boundary that observes the block.
func (c coordinatorSupervision) escalateRouteBlock(
	ctx context.Context,
	run domain.WorkflowRun,
	routeAvailable bool,
	now time.Time,
) error {
	if routeAvailable || run.Supervision == nil {
		return nil
	}
	projection, err := c.store.SupervisionProjection(ctx, run.ID)
	if err != nil {
		return err
	}
	snapshot := projection.Snapshot
	snapshot.RouteAvailable = routeAvailable
	threshold := run.Supervision.Config.IdleEscalationAfter
	for _, incident := range backlog.RouteBlockEscalations(snapshot, projection.Incidents, threshold, now) {
		reason := fmt.Sprintf(
			"the supervisor route cannot be admitted, so gate %s has waited for a review since %s and no activation can decide it",
			incident.GateID, incident.OpenedAt.UTC().Format(time.RFC3339))
		if err := c.escalate(ctx, *run.Supervision, incident.ID, incident.SourceEventID, reason, now); err != nil {
			return err
		}
	}
	return nil
}

func (c coordinatorSupervision) escalate(
	ctx context.Context,
	record domain.SupervisionRecord,
	incidentID, eventID, reason string,
	now time.Time,
) error {
	// The destination is resolved before the incident is escalated, so that an
	// escalation nobody will receive says so in the reason it is recorded with.
	// That reason is what explain and status show.
	submitterThread, err := c.store.SubmitterNotifyThread(ctx, record.RunID)
	if err != nil {
		return err
	}
	threadID, undeliverable := backlog.SupervisionEscalationThread(record, submitterThread)
	recorded := reason
	if undeliverable != "" {
		recorded = reason + "; " + undeliverable
	}
	_, err = c.store.ResolveReviewIncident(ctx, sqlite.IncidentResolutionRequest{
		RunID: record.RunID, IncidentID: incidentID, RequestID: "escalate:" + incidentID,
		Actor: domain.Actor{Kind: domain.ActorOperator, Principal: coordinatorSupervisionPrincipal},
		Event: domain.IncidentEventEscalate, Reason: recorded, ResolvedAt: now,
	})
	if err != nil && !errors.Is(err, sqlite.ErrSupervisionRequestConflict) {
		return err
	}
	entry, wanted := backlog.EscalationOutboxEntry(record, threadID, incidentID, recorded, []string{eventID}, now)
	if !wanted {
		// Either the run asked for no notification at all, or it asked for one
		// and named no destination. Both leave the escalation visible on its
		// incident and send nothing; discovering a recipient is forbidden.
		if undeliverable != "" {
			c.logger.Warn("supervision escalation is undeliverable",
				"run", record.RunID, "incident", incidentID, "reason", undeliverable)
		}
		return nil
	}
	_, err = c.store.AppendSupervisionOutbox(ctx, record.RunID, []backlog.SupervisionOutboxEntry{entry})
	return err
}

// coordinatorSupervisionPrincipal is the actor the coordinator records for the
// observations it makes itself. It is an operator kind because it is not an
// overseer: it holds no activation and decides no gate.
const coordinatorSupervisionPrincipal = "coordinator"

func supervisionGateIncidentID(runID, gateID, evidenceID string) string {
	return "incident:" + runID + ":" + gateID + ":" + evidenceID
}

// supervisionRouteAvailable reports whether some fresh worker can actually run
// this run's overseer: it must host the configured provider instance and model,
// and it must advertise the campaign-supervision capability.
//
// The snapshot inventory is the authority, not the coordinator's configuration,
// because the capability describes the build running on that host. This is the
// same rule worker_exchange.go applies before it sets a causal acknowledgement.
func supervisionRouteAvailable(route domain.ProviderRoute, workers []domain.WorkerSnapshot) bool {
	for _, worker := range workers {
		if route.WorkerID != "" && worker.WorkerID != route.WorkerID {
			continue
		}
		if !slices.Contains(worker.Inventory.Capabilities, workerproto.CapabilityCampaignSupervision) {
			continue
		}
		if supervisionWorkerServesRoute(worker, route) {
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

// coordinatorSupervisionSnapshots resolves the supervision snapshot of every
// supervised run for one planning pass, and answers RouteAvailable honestly.
//
// The store reports RouteAvailable true because provider admission is not
// observable from a database. Here it is: a gate nobody can decide is reported
// as blocked on its supervisor route rather than as merely waiting.
func coordinatorSupervisionSnapshots(
	ctx context.Context,
	store *sqlite.Store,
	runs []domain.WorkflowRun,
	workers []domain.WorkerSnapshot,
) (map[string]domain.SupervisionSnapshot, error) {
	var snapshots map[string]domain.SupervisionSnapshot
	for _, run := range runs {
		if run.Supervision == nil {
			continue
		}
		snapshot, err := store.LoadSupervisionSnapshot(ctx, run.ID)
		if err != nil {
			return nil, fmt.Errorf("load supervision snapshot of run %q: %w", run.ID, err)
		}
		if !snapshot.Supervised {
			continue
		}
		snapshot.RouteAvailable = supervisionRouteAvailable(run.Supervision.Config.Route, workers)
		if snapshots == nil {
			snapshots = make(map[string]domain.SupervisionSnapshot)
		}
		snapshots[run.ID] = snapshot
	}
	return snapshots, nil
}
