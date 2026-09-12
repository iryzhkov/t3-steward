package backlog

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// FleetCoordinatorStore is the durable boundary owned by the authoritative
// coordinator. Worker transports never mutate coordinator state directly.
type FleetCoordinatorStore interface {
	CoordinatorEpoch(context.Context) (int64, error)
	LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
	LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error)
	CommitAssignmentPlan(context.Context, domain.AssignmentPlanCommit) ([]domain.Assignment, error)
	CommitWorkerStateTransitions(context.Context, []domain.WorkerStateTransition) ([]domain.Assignment, error)
	CommitWorkerCommands(context.Context, []domain.WorkerCommand) ([]domain.WorkerCommand, error)
	LoadWorkerCommandRecords(context.Context) ([]domain.WorkerCommandRecord, error)
	LoadPendingWorkerCommands(context.Context, string, string, int64, int64, time.Time) ([]domain.WorkerCommand, error)
	AcknowledgeWorkerCommand(context.Context, domain.WorkerAcknowledgement) (domain.WorkerAcknowledgement, error)
}

// WorkerCommandTransport delivers already-durable commands. Implementations must
// treat command IDs as idempotency keys because delivery may be retried.
type WorkerCommandTransport interface {
	DeliverWorkerCommands(context.Context, domain.WorkerSnapshot, []domain.WorkerCommand) ([]domain.WorkerAcknowledgement, error)
}

type FleetCoordinator struct {
	Store FleetCoordinatorStore
	Now   func() time.Time
}

type AssignmentPlanningReport struct {
	Plan        Plan                `json:"plan"`
	Assignments []domain.Assignment `json:"assignments,omitempty"`
}

type WorkerDeliveryReport struct {
	Reconciled       []domain.Assignment            `json:"reconciled,omitempty"`
	Planned          []domain.WorkerCommand         `json:"planned,omitempty"`
	Pending          []domain.WorkerCommand         `json:"pending,omitempty"`
	Withheld         []domain.WorkerCommand         `json:"withheld,omitempty"`
	Acknowledgements []domain.WorkerAcknowledgement `json:"acknowledgements,omitempty"`
}

// PlanAndCommit makes the durable assignment transaction the only mutation
// resulting from a deterministic planner run.
func (c FleetCoordinator) PlanAndCommit(ctx context.Context, input PlanInput) (AssignmentPlanningReport, error) {
	if c.Store == nil {
		return AssignmentPlanningReport{}, errors.New("fleet coordinator store is required")
	}
	now := input.Now
	if now.IsZero() {
		now = c.now()
		input.Now = now
	}
	epoch, err := c.Store.CoordinatorEpoch(ctx)
	if err != nil {
		return AssignmentPlanningReport{}, err
	}
	snapshots, err := c.Store.LoadWorkerSnapshots(ctx)
	if err != nil {
		return AssignmentPlanningReport{}, err
	}
	input.Workers = plannerWorkers(snapshots, epoch, now)
	plan, err := BuildPlan(input)
	if err != nil {
		return AssignmentPlanningReport{}, err
	}
	report := AssignmentPlanningReport{Plan: plan}
	if len(plan.Proposals) == 0 {
		return report, nil
	}

	attempts := planningAttempts(input.Workflows)
	snapshotByWorker := make(map[string]domain.WorkerSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot.CoordinatorEpoch == epoch {
			snapshotByWorker[snapshot.WorkerID] = snapshot
		}
	}
	items := make([]domain.AssignmentPlanItem, 0, len(plan.Proposals))
	for _, proposal := range plan.Proposals {
		attempt, ok := attempts[proposal.AttemptID]
		if !ok {
			return AssignmentPlanningReport{}, fmt.Errorf("planner proposed unknown attempt %q", proposal.AttemptID)
		}
		if proposal.Route == nil {
			return AssignmentPlanningReport{}, fmt.Errorf("planner proposed attempt %q without a provider route", proposal.AttemptID)
		}
		if proposal.Estimate == nil {
			return AssignmentPlanningReport{}, fmt.Errorf("planner proposed attempt %q without a quota estimate", proposal.AttemptID)
		}
		snapshot, ok := snapshotByWorker[proposal.WorkerID]
		if !ok {
			return AssignmentPlanningReport{}, fmt.Errorf("planner proposed worker %q without a current snapshot", proposal.WorkerID)
		}
		assignmentID := stableCoordinatorID("assignment", attempt.ID)
		assignment := domain.Assignment{
			ID:            assignmentID,
			AttemptID:     attempt.ID,
			WorkerID:      proposal.WorkerID,
			Route:         *proposal.Route,
			Estimate:      cloneTaskAdmissionEstimatePointer(proposal.Estimate),
			State:         domain.AssignmentOffered,
			Epoch:         int64(attempt.Number),
			LeaseToken:    stableCoordinatorID("lease", assignmentID),
			DispatchToken: stableCoordinatorID("dispatch", assignmentID),
			ThreadID:      stableCoordinatorID("thread", assignmentID),
		}
		items = append(items, domain.AssignmentPlanItem{
			Assignment:              assignment,
			ExpectedAttemptRevision: attempt.Revision,
			WorkerEpoch:             snapshot.WorkerEpoch,
			WorkerSnapshotSequence:  snapshot.Sequence,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Assignment.ID < items[j].Assignment.ID
	})
	assignments, err := c.Store.CommitAssignmentPlan(ctx, domain.AssignmentPlanCommit{
		CoordinatorEpoch: epoch,
		CommittedAt:      now,
		Items:            items,
	})
	if err != nil {
		return AssignmentPlanningReport{}, err
	}
	report.Assignments = assignments
	return report, nil
}

// ReconcileWorkerCommands derives commands from durable coordinator state,
// commits them before delivery, and persists every acknowledgement returned even
// when the transport also reports a lost or partial response.
func (c FleetCoordinator) ReconcileWorkerCommands(
	ctx context.Context,
	snapshot domain.WorkerSnapshot,
	transport WorkerCommandTransport,
) (WorkerDeliveryReport, error) {
	return c.reconcileWorkerCommands(ctx, snapshot, transport, nil)
}

// ReconcileWorkerCommandsWithAdmission applies a current fail-closed quota
// policy after state reconciliation and again immediately before delivery.
func (c FleetCoordinator) ReconcileWorkerCommandsWithAdmission(
	ctx context.Context,
	snapshot domain.WorkerSnapshot,
	transport WorkerCommandTransport,
	admission WorkerAdmissionPolicy,
) (WorkerDeliveryReport, error) {
	return c.reconcileWorkerCommands(ctx, snapshot, transport, &admission)
}

func (c FleetCoordinator) reconcileWorkerCommands(
	ctx context.Context,
	snapshot domain.WorkerSnapshot,
	transport WorkerCommandTransport,
	admission *WorkerAdmissionPolicy,
) (WorkerDeliveryReport, error) {
	if c.Store == nil {
		return WorkerDeliveryReport{}, errors.New("fleet coordinator store is required")
	}
	if transport == nil {
		return WorkerDeliveryReport{}, errors.New("worker command transport is required")
	}
	now := c.now()
	epoch, err := c.Store.CoordinatorEpoch(ctx)
	if err != nil {
		return WorkerDeliveryReport{}, err
	}
	if snapshot.CoordinatorEpoch != epoch {
		return WorkerDeliveryReport{}, fmt.Errorf("worker %q snapshot belongs to coordinator epoch %d, current %d", snapshot.WorkerID, snapshot.CoordinatorEpoch, epoch)
	}
	records, err := c.Store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return WorkerDeliveryReport{}, err
	}
	commandRecords, err := c.Store.LoadWorkerCommandRecords(ctx)
	if err != nil {
		return WorkerDeliveryReport{}, err
	}
	transitions, err := PlanWorkerStateTransitions(records, snapshot, commandRecords, now)
	if err != nil {
		return WorkerDeliveryReport{}, err
	}
	report := WorkerDeliveryReport{}
	if len(transitions) > 0 {
		report.Reconciled, err = c.Store.CommitWorkerStateTransitions(ctx, transitions)
		if err != nil {
			return WorkerDeliveryReport{}, err
		}
		records, err = c.Store.LoadCoordinatorRecords(ctx)
		if err != nil {
			return WorkerDeliveryReport{}, err
		}
	}
	planned, err := PlanWorkerCommands(records, snapshot, commandRecords, now)
	if err != nil {
		return WorkerDeliveryReport{}, err
	}
	if admission != nil {
		planned, report.Withheld = admission.filterCommands(records.Assignments, records.Attempts, planned)
	}
	report.Planned = planned
	if len(planned) > 0 {
		if _, err := c.Store.CommitWorkerCommands(ctx, planned); err != nil {
			return WorkerDeliveryReport{}, err
		}
	}
	pending, err := c.Store.LoadPendingWorkerCommands(
		ctx, snapshot.WorkerID, snapshot.WorkerEpoch,
		snapshot.CoordinatorEpoch, snapshot.Sequence, now,
	)
	if err != nil {
		return WorkerDeliveryReport{}, err
	}
	if admission != nil {
		var withheld []domain.WorkerCommand
		pending, withheld = admission.filterCommands(records.Assignments, records.Attempts, pending)
		report.Withheld = append(report.Withheld, withheld...)
		sort.Slice(report.Withheld, func(i, j int) bool { return report.Withheld[i].ID < report.Withheld[j].ID })
	}
	report.Pending = pending
	if len(pending) == 0 {
		return report, nil
	}
	acknowledgements, deliveryErr := transport.DeliverWorkerCommands(ctx, snapshot, pending)
	report.Acknowledgements = acknowledgements
	for _, acknowledgement := range acknowledgements {
		if _, err := c.Store.AcknowledgeWorkerCommand(ctx, acknowledgement); err != nil {
			return report, err
		}
	}
	return report, deliveryErr
}

// PlanWorkerCommands is deterministic for a coordinator snapshot. Existing
// command records are reused by the delivery path; this function emits only
// commands whose stable identity has never been persisted.
func PlanWorkerCommands(
	records sqlite.CoordinatorRecords,
	snapshot domain.WorkerSnapshot,
	commandRecords []domain.WorkerCommandRecord,
	now time.Time,
) ([]domain.WorkerCommand, error) {
	if snapshot.WorkerID == "" || snapshot.WorkerEpoch == "" ||
		snapshot.CoordinatorEpoch < 1 || snapshot.Sequence < 1 || now.IsZero() {
		return nil, errors.New("worker command planning requires a complete snapshot and time")
	}
	if !snapshot.Connected || !snapshot.ValidUntil.After(now) {
		return nil, fmt.Errorf("worker %q snapshot is disconnected or stale", snapshot.WorkerID)
	}
	attemptByID := make(map[string]domain.Attempt, len(records.Attempts))
	for _, attempt := range records.Attempts {
		attemptByID[attempt.ID] = attempt
	}
	existing := make(map[string]domain.WorkerCommandRecord, len(commandRecords))
	for _, record := range commandRecords {
		key := workerCommandKey(record.Command.AssignmentID, record.Command.AssignmentEpoch, record.Command.Kind)
		existing[key] = record
	}
	observations := make(map[string]domain.WorkerAssignmentObservation, len(snapshot.Assignments))
	for _, observation := range snapshot.Assignments {
		observations[observation.AssignmentID] = observation
	}

	assignments := append([]domain.Assignment(nil), records.Assignments...)
	sort.Slice(assignments, func(i, j int) bool { return assignments[i].ID < assignments[j].ID })
	var commands []domain.WorkerCommand
	for _, assignment := range assignments {
		if assignment.WorkerID != snapshot.WorkerID ||
			assignment.WorkerEpoch != snapshot.WorkerEpoch ||
			(assignment.State != domain.AssignmentClaimed && assignment.State != domain.AssignmentUnknown) {
			continue
		}
		attempt, ok := attemptByID[assignment.AttemptID]
		if !ok {
			return nil, fmt.Errorf("assignment %q refers to unknown attempt %q", assignment.ID, assignment.AttemptID)
		}
		kind, ok := nextWorkerCommand(assignment, attempt, observations[assignment.ID], existing)
		if !ok {
			continue
		}
		commands = append(commands, domain.WorkerCommand{
			ID:                     stableCoordinatorID("command", workerCommandKey(assignment.ID, assignment.Epoch, kind)),
			Kind:                   kind,
			WorkerID:               snapshot.WorkerID,
			WorkerEpoch:            snapshot.WorkerEpoch,
			CoordinatorEpoch:       snapshot.CoordinatorEpoch,
			AssignmentID:           assignment.ID,
			AssignmentEpoch:        assignment.Epoch,
			ExpectedWorkerSequence: snapshot.Sequence,
			CreatedAt:              now,
		})
		// Worker command sequence is a strict compare-and-swap fence. Only one
		// newly planned command may consume a snapshot sequence; the next tick
		// observes the resulting sequence before planning another command.
		break
	}
	return commands, nil
}

func nextWorkerCommand(
	assignment domain.Assignment,
	attempt domain.Attempt,
	observation domain.WorkerAssignmentObservation,
	existing map[string]domain.WorkerCommandRecord,
) (domain.WorkerCommandKind, bool) {
	has := func(kind domain.WorkerCommandKind) (domain.WorkerCommandRecord, bool) {
		record, ok := existing[workerCommandKey(assignment.ID, assignment.Epoch, kind)]
		return record, ok
	}
	if observation.AssignmentID == assignment.ID &&
		observation.AssignmentEpoch == assignment.Epoch &&
		observation.State == domain.AssignmentCompleted {
		if _, ok := has(domain.WorkerCommandCollect); !ok {
			return domain.WorkerCommandCollect, true
		}
		return "", false
	}
	if attempt.Control == domain.ControlStopped {
		if _, ok := has(domain.WorkerCommandStop); !ok {
			return domain.WorkerCommandStop, true
		}
		return "", false
	}
	if assignment.State != domain.AssignmentClaimed {
		return "", false
	}
	prepare, prepared := has(domain.WorkerCommandPrepare)
	if !prepared {
		return domain.WorkerCommandPrepare, true
	}
	if prepare.Acknowledgement != nil && !prepare.Acknowledgement.Accepted &&
		strings.Contains(prepare.Acknowledgement.Detail, "stale worker command identity or sequence") {
		return domain.WorkerCommandPrepare, true
	}
	if prepare.Acknowledgement == nil || !prepare.Acknowledgement.Accepted {
		return "", false
	}
	if attempt.Control == domain.ControlPreparing || attempt.Control == domain.ControlResuming {
		if _, ok := has(domain.WorkerCommandDispatch); !ok {
			return domain.WorkerCommandDispatch, true
		}
	}
	return "", false
}

func plannerWorkers(snapshots []domain.WorkerSnapshot, epoch int64, now time.Time) []domain.WorkerInventory {
	workers := make([]domain.WorkerInventory, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot.CoordinatorEpoch != epoch {
			continue
		}
		inventory := snapshot.Inventory
		inventory.ID = snapshot.WorkerID
		inventory.ObservedAt = snapshot.ObservedAt
		if !snapshot.Connected || !snapshot.ValidUntil.After(now) {
			inventory.Health = domain.WorkerHealthOffline
		}
		workers = append(workers, inventory)
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })
	return workers
}

func planningAttempts(workflows []PlanningWorkflow) map[string]domain.Attempt {
	result := make(map[string]domain.Attempt)
	for _, workflow := range workflows {
		for _, attempt := range workflow.State.Attempts {
			result[attempt.ID] = attempt
		}
	}
	return result
}

func workerCommandKey(assignmentID string, assignmentEpoch int64, kind domain.WorkerCommandKind) string {
	return fmt.Sprintf("%s/%d/%s", assignmentID, assignmentEpoch, kind)
}

func stableCoordinatorID(kind, identity string) string {
	sum := sha256.Sum256([]byte(kind + "\x00" + identity))
	return fmt.Sprintf("%s-%x", kind, sum[:16])
}

func (c FleetCoordinator) now() time.Time {
	if c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}
