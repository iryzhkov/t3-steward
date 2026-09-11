package backlog

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
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
	Snapshot(context.Context) (domain.WorkerSnapshot, error)
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
	snapshot, err := transport.Snapshot(ctx)
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
		if err != nil {
			return report, fmt.Errorf("build assignment offer %q: %w", assignment.ID, err)
		}
		if err := validateBuiltOffer(offer, leased, now); err != nil {
			return report, fmt.Errorf("build assignment offer %q: %w", assignment.ID, err)
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
				return report, fmt.Errorf("worker %q returned an invalid claim for assignment %q", snapshot.WorkerID, claim.AssignmentID)
			}
			claimedIDs[claim.AssignmentID] = struct{}{}
			claimed, err := store.ClaimAssignment(ctx, claim)
			if err != nil {
				return report, err
			}
			report.Claimed = append(report.Claimed, claimed)
		}
		if len(claimedIDs) != len(offered) {
			return report, fmt.Errorf("worker %q claimed %d of %d offered assignments", snapshot.WorkerID, len(claimedIDs), len(offered))
		}
		snapshot, err = transport.Snapshot(ctx)
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
		if ok && attemptOK && observation.AssignmentEpoch == assignment.Epoch && observation.WorkspacePath != "" {
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
