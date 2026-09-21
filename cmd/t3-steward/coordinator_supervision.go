package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
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
	// yieldToOlderWork gives already-eligible ordinary attempts and settled wakes
	// their existing admission path before a newer activation consumes a shared
	// worker slot or quota pool. It is nil in focused lifecycle tests.
	yieldToOlderWork   func(context.Context, activationFairnessCandidate) (bool, error)
	quotaMaxConcurrent map[string]int
	// warnedNoSupervisorClient remembers the runs this process has already
	// warned about, so a deployment with no supervisor client says so once per
	// run rather than once per boundary. The boundary runs on the interval the
	// scheduler is configured with, and a log line at that rate is noise an
	// operator learns to filter, which is the same silence it replaced.
	//
	// It is deliberately in memory only. The warning is about this process's
	// configuration, so a restart that still has no supervisor client should say
	// so again; the durable half of the report is the escalated incident.
	warnedNoSupervisorClient map[string]bool
}

// supervisorClientConfigured reports whether an overseer could be dispatched at
// all. It is the third condition of route availability; see
// backlog.SupervisionRouteRequest.
func (c coordinatorSupervision) supervisorClientConfigured() bool { return c.settings.configured() }

// routeBlockCause names why the supervisor route cannot run, in the wording
// every surface reports.
func (c coordinatorSupervision) routeBlockCause() string {
	return backlog.SupervisionRouteBlockCause(c.supervisorClientConfigured())
}

// warnMissingSupervisorClientOnce says, once per run, that a gate is waiting
// for a review nobody can perform because this coordinator has no supervisor
// admin client.
//
// Saying it at all is the point. Before this the coordinator dispatched no
// activation, logged nothing and left the gate looking merely pending, so the
// only evidence that anything was wrong was work that never started.
func (c coordinatorSupervision) warnMissingSupervisorClientOnce(runID string) {
	if c.warnedNoSupervisorClient == nil || c.warnedNoSupervisorClient[runID] {
		return
	}
	c.warnedNoSupervisorClient[runID] = true
	c.logger.Warn("supervised run has a gate ready for review and no supervisor client is configured",
		"run", runID, "reason", backlog.SupervisionNoSupervisorClient,
		"remedy", "declare exactly one backlog_v2.coordinator.admin_clients entry with supervisor: true")
}

// supervisionAwaitsReview reports whether any gate of this run is ready for a
// decision right now.
func supervisionAwaitsReview(snapshot domain.SupervisionSnapshot) bool {
	for _, gate := range snapshot.Gates {
		if gate.State == domain.GateReadyForReview {
			return true
		}
	}
	return false
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
	routeAvailable := backlog.SupervisionRouteAvailable(backlog.SupervisionRouteRequest{
		Route: run.Supervision.Config.Route, Workers: workers,
		SupervisorClientConfigured: c.supervisorClientConfigured(),
	})
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
		if err := c.escalate(ctx, *run.Supervision, incidentID, []string{eventID},
			c.routeBlockCause()+", so "+reason+" and no activation can decide it", now); err != nil {
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
	if !c.supervisorClientConfigured() && supervisionAwaitsReview(snapshot) {
		c.warnMissingSupervisorClientOnce(run.ID)
	}
	threshold := run.Supervision.Config.IdleEscalationAfter
	for _, incident := range backlog.RouteBlockEscalations(snapshot, projection.Incidents, threshold, now) {
		reason := fmt.Sprintf(
			"%s, so gate %s has waited for a review since %s and no activation can decide it",
			c.routeBlockCause(), incident.GateID, incident.OpenedAt.UTC().Format(time.RFC3339))
		if err := c.escalate(ctx, *run.Supervision, incident.ID, []string{incident.SourceEventID}, reason, now); err != nil {
			return err
		}
	}
	return nil
}

// escalate records one escalation on its incident and, when the run asked for a
// notification and named a destination, queues one delivery intent for it.
//
// eventIDs are the supervision events the escalation is about. They travel with
// the delivery so the thread that receives it can name what went unreviewed.
func (c coordinatorSupervision) escalate(
	ctx context.Context,
	record domain.SupervisionRecord,
	incidentID string,
	eventIDs []string,
	reason string,
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
	entry, wanted := backlog.EscalationOutboxEntry(record, threadID, incidentID, recorded, eventIDs, now)
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

// coordinatorSupervisionSnapshots resolves the supervision snapshot of every
// supervised run for one planning pass, and answers RouteAvailable honestly.
//
// The store reports RouteAvailable true because provider admission is not
// observable from a database. Here it is: a gate nobody can decide is reported
// as blocked on its supervisor route rather than as merely waiting. A
// coordinator with no supervisor admin client answers false for the same
// reason, because it will dispatch no overseer at all.
func coordinatorSupervisionSnapshots(
	ctx context.Context,
	store *sqlite.Store,
	runs []domain.WorkflowRun,
	workers []domain.WorkerSnapshot,
	supervisorClientConfigured bool,
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
		snapshot.RouteAvailable = backlog.SupervisionRouteAvailable(backlog.SupervisionRouteRequest{
			Route: run.Supervision.Config.Route, Workers: workers,
			SupervisorClientConfigured: supervisorClientConfigured,
		})
		if snapshots == nil {
			snapshots = make(map[string]domain.SupervisionSnapshot)
		}
		snapshots[run.ID] = snapshot
	}
	return snapshots, nil
}
