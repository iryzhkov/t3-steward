package backlog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// WorkerExchangeStore is the authoritative persistence boundary for one worker
// exchange. Worker claims never become active solely from a transport response.
type WorkerExchangeStore interface {
	FleetCoordinatorStore
	SaveWorkerSnapshot(context.Context, domain.WorkerSnapshot) error
	ClaimAssignment(context.Context, domain.AssignmentClaimRequest) (domain.Assignment, error)
	RenewAssignmentLease(context.Context, domain.AssignmentLeaseRenewal) (domain.Assignment, error)
	ExpireAssignmentLeases(context.Context, int64, time.Time) ([]domain.Assignment, error)
	ThrottleDeliveryStore
	LoadQuotaAdmissions(context.Context) ([]domain.QuotaAdmissionRecord, error)
}

// WorkerControlTransport is one fresh, epoch-bound coordinator session.
type WorkerControlTransport interface {
	WorkerCommandTransport
	// WorkerID names the worker on the far end, so the coordinator can build a
	// statement about that worker's assignments and no others.
	WorkerID() string
	Snapshot(context.Context, workerproto.SnapshotRequest) (domain.WorkerSnapshot, error)
	DeliverOffers(context.Context, []workerproto.AssignmentOffer) ([]domain.AssignmentClaimRequest, error)
	DeliverLeaseRenewals(context.Context, []domain.AssignmentLeaseRenewal) (domain.WorkerSnapshot, error)
	DeliverThrottle(context.Context, domain.WorkerSnapshot, []domain.ThrottleCommand) ([]domain.ThrottleAcknowledgement, error)
}

// AssignmentOfferBuilder resolves the immutable package for an already-durable
// offered assignment.
type AssignmentOfferBuilder interface {
	BuildAssignmentOffer(context.Context, domain.Assignment, time.Time) (workerproto.AssignmentOffer, error)
}

type WorkerExchangeReport struct {
	Snapshot    domain.WorkerSnapshot
	Expired     []domain.Assignment
	Offered     []domain.Assignment
	Claimed     []domain.Assignment
	Renewed     []domain.Assignment
	Withheld    []domain.Assignment
	Delivery    WorkerDeliveryReport
	Throttle    []ThrottleDeliveryReport
	Imports     []ResultImportReport
	Checkpoints []domain.Artifact
}

// TaskWaitParkStore exposes the attempts the coordinator's task-bound waits
// still park. A store that does not implement it has none, and every worker is
// told that nothing is parked, which is exactly true for it.
type TaskWaitParkStore interface {
	ParkedTaskWaitAttempts(context.Context) (map[string]string, error)
	LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
}

// The coordinator's own store must satisfy it. The assertion below is what
// makes that a compile error rather than a silent empty statement: a store that
// does not implement the interface is told it has nothing parked, which is the
// right answer for a store that holds no waits and the worst possible one for
// the store that holds them all.
var _ TaskWaitParkStore = (*sqlite.Store)(nil)

// parkedAssignments states, for one worker, which of its claimed assignments
// are parked on a task-bound wait.
//
// Parked outlasts the wait's own settlement: the statement stays true until the
// wake reaches the thread, because until then the attempt has no running turn
// and one is coming. Ending it at settlement told the worker nothing was parked
// while the attempt sat between two turns, and it collected there.
//
// The worker cannot ask: the restricted protocol gives it no read of
// coordinator state, and widening it for this would trade a lifecycle bug for
// an authority one. So the coordinator says it, in the exchange the worker
// already makes, and scopes the statement to that worker's own assignments.
//
// The list is complete rather than incremental. A worker that receives it
// replaces everything it believed, because an incremental list cannot express
// "this one is no longer parked" without another message to lose.
func (c FleetCoordinator) parkedAssignments(ctx context.Context, workerID string) (workerproto.SnapshotRequest, error) {
	return ParkedAssignmentsFor(ctx, c.Store, workerID)
}

// ParkedAssignmentsFor builds the statement parkedAssignments sends, for any
// store that can answer it. It is exported so that the worker side of the park
// can be exercised against the statement the coordinator really produces,
// rather than against a hand-written copy of it.
func ParkedAssignmentsFor(ctx context.Context, source any, workerID string) (workerproto.SnapshotRequest, error) {
	request := workerproto.SnapshotRequest{ParkedReported: true}
	// Read the durable worker observation BEFORE the parked state. This ack
	// proves that the report was constructed after that worker observation;
	// receiving a recently-built empty list by itself provides no such fence.
	if snapshots, ok := source.(interface {
		LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error)
	}); ok {
		observed, err := snapshots.LoadWorkerSnapshots(ctx)
		if err != nil {
			return workerproto.SnapshotRequest{}, err
		}
		for _, snapshot := range observed {
			if snapshot.WorkerID == workerID && snapshot.Sequence > 0 && slices.Contains(snapshot.Inventory.Capabilities, workerproto.CapabilityTaskWaitCollectionFence) {
				request.ObservedWorkerEpoch = snapshot.WorkerEpoch
				request.ObservedSequence = snapshot.Sequence
				break
			}
		}
	}
	waits, ok := source.(TaskWaitParkStore)
	if !ok || workerID == "" {
		return request, nil
	}
	parked, err := waits.ParkedTaskWaitAttempts(ctx)
	if err != nil {
		return workerproto.SnapshotRequest{}, fmt.Errorf("load parked task waits: %w", err)
	}
	records, err := waits.LoadCoordinatorRecords(ctx)
	if err != nil {
		return workerproto.SnapshotRequest{}, err
	}
	if err := stateCampaignRefs(&request, records); err != nil {
		return workerproto.SnapshotRequest{}, err
	}
	if len(parked) == 0 {
		return request, nil
	}
	revisions := make(map[string]int64, len(records.Attempts))
	for _, attempt := range records.Attempts {
		revisions[attempt.ID] = attempt.Revision
	}
	for _, assignment := range records.Assignments {
		if assignment.WorkerID != workerID {
			continue
		}
		waitID, held := parked[assignment.AttemptID]
		if !held {
			continue
		}
		request.Parked = append(request.Parked, workerproto.ParkedAssignment{
			AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
			AttemptID: assignment.AttemptID, AttemptRevision: revisions[assignment.AttemptID],
			WaitID: waitID,
		})
	}
	sort.Slice(request.Parked, func(i, j int) bool { return request.Parked[i].AssignmentID < request.Parked[j].AssignmentID })
	if len(request.Parked) > workerproto.MaxParkedAssignments {
		return workerproto.SnapshotRequest{}, fmt.Errorf("worker %q has %d parked assignments, above the protocol limit", workerID, len(request.Parked))
	}
	return request, nil
}

// ReconcileWorker observes a worker, offers only assignments already committed
// by the current authority, persists returned claims, refreshes the observation,
// then derives and delivers durable lifecycle commands.
func (c FleetCoordinator) ReconcileWorker(
	ctx context.Context,
	transport WorkerControlTransport,
	builder AssignmentOfferBuilder,
	admission WorkerAdmissionPolicy,
	directives []domain.ThrottleDirective,
	pools []domain.QuotaPool,
	offerTTL time.Duration,
	leaseDuration time.Duration,
) (WorkerExchangeReport, error) {
	store, ok := c.Store.(WorkerExchangeStore)
	if !ok || store == nil {
		return WorkerExchangeReport{}, errors.New("worker exchange store is required")
	}
	if transport == nil || builder == nil || offerTTL <= 0 || leaseDuration <= 0 {
		return WorkerExchangeReport{}, errors.New("worker exchange transport, package builder, and positive lifetimes are required")
	}
	epoch, err := store.CoordinatorEpoch(ctx)
	if err != nil {
		return WorkerExchangeReport{}, err
	}
	now := c.now()
	expired, err := store.ExpireAssignmentLeases(ctx, epoch, now)
	if err != nil {
		return WorkerExchangeReport{}, err
	}
	parked, err := c.parkedAssignments(ctx, transport.WorkerID())
	if err != nil {
		return WorkerExchangeReport{}, err
	}
	snapshot, err := transport.Snapshot(ctx, parked)
	if err != nil {
		return WorkerExchangeReport{}, err
	}
	if snapshot.CoordinatorEpoch != epoch {
		return WorkerExchangeReport{}, fmt.Errorf("worker snapshot coordinator epoch %d does not match authority %d", snapshot.CoordinatorEpoch, epoch)
	}
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		return WorkerExchangeReport{}, err
	}
	report := WorkerExchangeReport{Snapshot: snapshot, Expired: expired}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return report, err
	}
	assignments := offeredAssignmentsForWorker(records.Assignments, snapshot)
	assignments, report.Withheld = admission.filterOffers(assignments, records.Attempts)
	offers := make([]workerproto.AssignmentOffer, 0, len(assignments))
	for _, assignment := range assignments {
		leased := assignment
		leased.LeaseExpiresAt = now.Add(leaseDuration)
		offer, err := builder.BuildAssignmentOffer(ctx, leased, now.Add(offerTTL))
		if err == nil {
			err = validateBuiltOffer(offer, leased, now)
		}
		if err == nil {
			err = requireOfferedCapabilities(offer, snapshot)
		}
		if err != nil {
			// An assignment that cannot be packaged must not block offers,
			// commands, or collection for every other assignment.
			slog.Warn("assignment offer withheld", "assignment", assignment.ID, "error", err)
			report.Withheld = append(report.Withheld, assignment)
			continue
		}
		offers = append(offers, offer)
		report.Offered = append(report.Offered, assignment)
	}
	if len(offers) != 0 {
		claims, err := transport.DeliverOffers(ctx, offers)
		if err != nil {
			return report, err
		}
		offered := make(map[string]domain.Assignment, len(offers))
		for _, offer := range offers {
			offered[offer.Assignment.ID] = offer.Assignment
		}
		claimedIDs := make(map[string]struct{}, len(claims))
		for _, claim := range claims {
			assignment, exists := offered[claim.AssignmentID]
			_, duplicate := claimedIDs[claim.AssignmentID]
			if !exists || duplicate || claim.WorkerID != snapshot.WorkerID || claim.WorkerEpoch != snapshot.WorkerEpoch ||
				claim.CoordinatorEpoch != epoch || claim.AssignmentEpoch != assignment.Epoch ||
				claim.LeaseToken != assignment.LeaseToken || claim.LeaseExpiresAt.After(assignment.LeaseExpiresAt) {
				slog.Warn("worker returned an invalid claim; ignored", "worker", snapshot.WorkerID, "assignment", claim.AssignmentID)
				continue
			}
			claimedIDs[claim.AssignmentID] = struct{}{}
			claimed, err := store.ClaimAssignment(ctx, claim)
			if err != nil {
				slog.Warn("assignment claim could not be persisted; it stays offered", "assignment", claim.AssignmentID, "error", err)
				continue
			}
			report.Claimed = append(report.Claimed, claimed)
		}
		if len(claimedIDs) != len(offered) {
			slog.Warn("worker withheld some offers", "worker", snapshot.WorkerID, "claimed", len(claimedIDs), "offered", len(offered))
		}
		if parked, err = c.parkedAssignments(ctx, transport.WorkerID()); err != nil {
			return report, err
		}
		snapshot, err = transport.Snapshot(ctx, parked)
		if err != nil {
			return report, err
		}
		if snapshot.CoordinatorEpoch != epoch || snapshot.WorkerID != report.Snapshot.WorkerID ||
			snapshot.WorkerEpoch != report.Snapshot.WorkerEpoch {
			return report, errors.New("worker identity changed during exchange")
		}
		if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
			return report, err
		}
		report.Snapshot = snapshot
	}
	records, err = store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return report, err
	}
	renewals := leaseRenewalsForWorker(records.Assignments, report.Snapshot, now, leaseDuration)
	if len(renewals) != 0 {
		renewedSnapshot, renewErr := transport.DeliverLeaseRenewals(ctx, renewals)
		if renewErr != nil {
			return report, renewErr
		}
		if renewedSnapshot.CoordinatorEpoch != epoch || renewedSnapshot.WorkerID != report.Snapshot.WorkerID ||
			renewedSnapshot.WorkerEpoch != report.Snapshot.WorkerEpoch || renewedSnapshot.Sequence <= report.Snapshot.Sequence {
			return report, errors.New("worker identity or sequence changed during lease renewal")
		}
		for _, renewal := range renewals {
			renewed, renewErr := store.RenewAssignmentLease(ctx, renewal)
			if renewErr != nil {
				return report, renewErr
			}
			report.Renewed = append(report.Renewed, renewed)
		}
		if err := store.SaveWorkerSnapshot(ctx, renewedSnapshot); err != nil {
			return report, err
		}
		report.Snapshot = renewedSnapshot
	}
	delivery, err := c.ReconcileWorkerCommandsWithAdmission(ctx, report.Snapshot, transport, admission)
	report.Delivery = delivery
	if err != nil {
		return report, err
	}
	if admission.QuotaChecksDisabled {
		return report, nil
	}
	if err := c.reconcileWorkerThrottle(ctx, store, transport, report.Snapshot, directives, pools, now, &report); err != nil {
		return report, err
	}
	return report, nil
}

func leaseRenewalsForWorker(assignments []domain.Assignment, snapshot domain.WorkerSnapshot, now time.Time, leaseDuration time.Duration) []domain.AssignmentLeaseRenewal {
	observed := make(map[string]domain.WorkerAssignmentObservation, len(snapshot.Assignments))
	for _, observation := range snapshot.Assignments {
		observed[workerAssignmentKey(observation.AssignmentID, observation.AssignmentEpoch)] = observation
	}
	var renewals []domain.AssignmentLeaseRenewal
	for _, assignment := range assignments {
		observation, ok := observed[workerAssignmentKey(assignment.ID, assignment.Epoch)]
		if !ok || observation.State != domain.AssignmentClaimed || assignment.State != domain.AssignmentClaimed ||
			assignment.WorkerID != snapshot.WorkerID || assignment.WorkerEpoch != snapshot.WorkerEpoch ||
			!assignment.LeaseExpiresAt.After(now) {
			continue
		}
		expiresAt := now.Add(leaseDuration)
		if !expiresAt.After(assignment.LeaseExpiresAt) {
			continue
		}
		renewals = append(renewals, domain.AssignmentLeaseRenewal{
			CoordinatorEpoch: snapshot.CoordinatorEpoch, WorkerID: snapshot.WorkerID,
			WorkerEpoch: snapshot.WorkerEpoch, WorkerSequence: snapshot.Sequence,
			AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
			LeaseToken: assignment.LeaseToken, RenewedAt: now, LeaseExpiresAt: expiresAt,
		})
	}
	sort.Slice(renewals, func(i, j int) bool { return renewals[i].AssignmentID < renewals[j].AssignmentID })
	return renewals
}

type workerThrottleTransport struct {
	workerID  string
	snapshot  domain.WorkerSnapshot
	transport WorkerControlTransport
}

type workerThrottleStore struct {
	WorkerExchangeStore
	workerID string
}

func (s workerThrottleStore) LoadThrottleAttemptRecords(ctx context.Context) ([]domain.ThrottleAttemptRecord, error) {
	records, err := s.WorkerExchangeStore.LoadThrottleAttemptRecords(ctx)
	if err != nil {
		return nil, err
	}
	filtered := records[:0]
	for _, record := range records {
		if record.Command.WorkerID == s.workerID {
			filtered = append(filtered, record)
		}
	}
	return filtered, nil
}

func (t workerThrottleTransport) DeliverThrottleCommands(ctx context.Context, workerID string, commands []domain.ThrottleCommand) ([]domain.ThrottleAcknowledgement, error) {
	if workerID != t.workerID {
		return nil, fmt.Errorf("throttle batch for worker %q reached session for %q", workerID, t.workerID)
	}
	return t.transport.DeliverThrottle(ctx, t.snapshot, commands)
}

func (c FleetCoordinator) reconcileWorkerThrottle(ctx context.Context, store WorkerExchangeStore, transport WorkerControlTransport, snapshot domain.WorkerSnapshot, directives []domain.ThrottleDirective, pools []domain.QuotaPool, now time.Time, report *WorkerExchangeReport) error {
	throttleStore := workerThrottleStore{WorkerExchangeStore: store, workerID: snapshot.WorkerID}
	adapter := workerThrottleTransport{workerID: snapshot.WorkerID, snapshot: snapshot, transport: transport}
	pending, err := ReconcilePendingThrottleCommands(ctx, throttleStore, adapter, now)
	report.Throttle = append(report.Throttle, pending)
	if err != nil {
		return err
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return err
	}
	attempts := make(map[string]domain.Attempt, len(records.Attempts))
	for _, attempt := range records.Attempts {
		attempts[attempt.ID] = attempt
	}
	assignments := make(map[string]domain.Assignment, len(records.Assignments))
	for _, assignment := range records.Assignments {
		assignments[assignment.ID] = assignment
	}
	var bindings []ThrottleAttemptBinding
	for _, observation := range snapshot.Assignments {
		assignment, ok := assignments[observation.AssignmentID]
		attempt, attemptOK := attempts[assignment.AttemptID]
		if ok && attemptOK && !attempt.AdminForceStart &&
			(assignment.State == domain.AssignmentClaimed || assignment.State == domain.AssignmentUnknown) &&
			attempt.AssignmentID == assignment.ID && assignment.ThreadID != "" &&
			observation.AssignmentEpoch == assignment.Epoch && observation.WorkspacePath != "" {
			bindings = append(bindings, ThrottleAttemptBinding{Attempt: attempt, Assignment: assignment, WorkspacePath: observation.WorkspacePath})
		}
	}
	delivered, err := ReconcileThrottleDeliveries(ctx, throttleStore, adapter, directives, bindings, now)
	report.Throttle = append(report.Throttle, delivered)
	if err != nil {
		return err
	}
	expired, err := ReconcileThrottleDeadlineExpirations(ctx, throttleStore, adapter, now)
	report.Throttle = append(report.Throttle, expired)
	if err != nil {
		return err
	}
	admissions, err := store.LoadQuotaAdmissions(ctx)
	if err != nil {
		return err
	}
	resumed, err := ReconcileThrottleResumes(ctx, throttleStore, adapter, admissions, pools, now)
	report.Throttle = append(report.Throttle, resumed)
	return err
}

func offeredAssignmentsForWorker(assignments []domain.Assignment, snapshot domain.WorkerSnapshot) []domain.Assignment {
	result := make([]domain.Assignment, 0)
	for _, assignment := range assignments {
		if assignment.State == domain.AssignmentOffered && assignment.WorkerID == snapshot.WorkerID &&
			assignment.WorkerEpoch == snapshot.WorkerEpoch {
			result = append(result, assignment)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

// requireOfferedCapabilities is the exchange-side capability gate: an offer is
// withheld from a worker whose durable snapshot inventory does not advertise
// what the package requires.
//
// It reads snapshot.Inventory.Capabilities, the durable observation, and not
// the handshake negotiation, for the same reason the causal acknowledgement
// fields do: a handshake capability is a claim made in passing, while the
// inventory is the worker's own published statement about the build running on
// that host.
//
// The gate is deliberately narrow. Placement is where a worker that cannot run
// an activation stops being a candidate, and campaign check is where an
// unsatisfiable requirement is reported as impossible before a run exists.
// This is the last boundary before the package crosses the wire, and it exists
// so that an activation can never reach an older worker through any path that
// skipped the earlier two, including a plan committed before the worker was
// downgraded to a build that no longer advertises the capability.
func requireOfferedCapabilities(offer workerproto.AssignmentOffer, snapshot domain.WorkerSnapshot) error {
	if !offer.Package.Package.IsActivation() {
		return nil
	}
	if !slices.Contains(snapshot.Inventory.Capabilities, workerproto.CapabilityCampaignSupervision) {
		return fmt.Errorf(
			"worker %q does not advertise capability %q; a supervision activation is not offered to it",
			snapshot.WorkerID, workerproto.CapabilityCampaignSupervision)
	}
	return nil
}

func validateBuiltOffer(offer workerproto.AssignmentOffer, assignment domain.Assignment, now time.Time) error {
	if !reflect.DeepEqual(offer.Assignment, assignment) {
		return errors.New("offer changed durable assignment identity")
	}
	if !now.Before(offer.ExpiresAt) || offer.ExpiresAt.After(assignment.LeaseExpiresAt) {
		return errors.New("offer expiry is outside the assignment lease")
	}
	if err := workerproto.ValidateExecutionPackageManifest(offer.Package, offer.Package.Size); err != nil {
		return err
	}
	return nil
}
