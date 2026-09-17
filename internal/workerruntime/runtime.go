package workerruntime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// Driver is the worker-local execution seam. Every method must be safe to
// call again after a crash: the runtime retries from its durable journal.
type Driver interface {
	Prepare(context.Context, workerproto.ExecutionPackage) (string, error)
	InspectWorkspace(context.Context, workerproto.ExecutionPackage) (string, bool, error)
	ObserveThread(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, error)
	CreateThread(context.Context, workerproto.ExecutionPackage, string) error
	StopThread(context.Context, workerproto.ExecutionPackage) error
	Collect(context.Context, workerproto.ExecutionPackage, string) error
	// CollectFailure publishes a terminal failed result for an attempt that
	// never produced a collectable T3 outcome (preparation or dispatch failed
	// before any provider effect, or the execution could not be recovered).
	CollectFailure(context.Context, workerproto.ExecutionPackage, string, string) error
	// Settle retries the provider-side settlement of a collected attempt whose
	// result is already durable.
	Settle(context.Context, workerproto.ExecutionPackage) error
	Cleanup(context.Context, workerproto.ExecutionPackage, string) error
	Warn(context.Context, workerproto.ExecutionPackage, domain.ThrottleCommand) error
	Checkpoint(context.Context, workerproto.ExecutionPackage, domain.ThrottleCommand) (*domain.CheckpointMetadata, error)
	Resume(context.Context, workerproto.ExecutionPackage, domain.ThrottleCommand) error
}

// DefaultRetention is how long terminal attempt records and their workspaces
// stay on the worker before the reconcile loop prunes them.
const DefaultRetention = 72 * time.Hour

// MaxPrepareAttempts bounds how often preparation is retried before the
// attempt is failed with the last preparation error.
const MaxPrepareAttempts = 3

type Config struct {
	ObserveInventory func(context.Context, domain.WorkerInventory) (domain.WorkerInventory, error)
	// LiveTaskWait overrides how the worker answers whether an attempt is
	// parked on a task-bound wait. It exists for an embedded worker that can
	// read coordinator state directly, and for tests.
	//
	// Leave it nil in production. The answer then comes from the coordinator's
	// own statement, carried on the snapshot exchange the worker already makes,
	// which keeps the restricted worker protocol restricted: the worker is told
	// what is parked and never asks.
	LiveTaskWait func(context.Context, workerproto.ExecutionPackage) (bool, error)
	// CampaignRefs is the worker-local store of campaign-scoped commits. It is
	// nil for a worker that keeps none, which then has nothing to release.
	CampaignRefs CampaignRefCustodian
	// Quota is the host watchdog's bucket state, consulted for the threads
	// this runtime owns: a bucket in the drain or stop phase pauses the
	// attempt through the throttle path, and the same recovery rules resume
	// it. Nil means no watchdog state on this host and no local pauses.
	Quota            QuotaGuard
	WorkerID         string
	WorkerEpoch      string
	CoordinatorID    string
	CoordinatorEpoch int64
	SnapshotTTL      time.Duration
	LeaseDuration    time.Duration
	MaxPackageBytes  int64
	Inventory        domain.WorkerInventory
	Retention        time.Duration
	Now              func() time.Time
	Logger           *slog.Logger
}

type Runtime struct {
	desiredInventory domain.WorkerInventory
	config           Config
	journal          *Journal
	driver           Driver
	log              *slog.Logger
	// reportQuota records that the coordinator asked for bucket observations
	// on its last snapshot request. It is not durable: the coordinator asks
	// again on every exchange.
	reportQuota bool
}

func New(config Config, journal *Journal, driver Driver) (*Runtime, error) {
	if journal == nil || driver == nil {
		return nil, errors.New("worker runtime: journal and driver are required")
	}
	if config.WorkerID == "" || config.WorkerEpoch == "" || config.CoordinatorID == "" || config.CoordinatorEpoch < 1 {
		return nil, errors.New("worker runtime: complete worker and coordinator identity is required")
	}
	if config.SnapshotTTL <= 0 || config.LeaseDuration <= 0 || config.MaxPackageBytes <= 0 {
		return nil, errors.New("worker runtime: positive snapshot, lease, and package limits are required")
	}
	state, err := journal.snapshot()
	if err != nil {
		return nil, err
	}
	if state.WorkerID != config.WorkerID || state.WorkerEpoch != config.WorkerEpoch || state.CoordinatorEpoch != config.CoordinatorEpoch {
		return nil, errors.New("worker runtime: configuration does not match journal authority")
	}
	if config.Inventory.ID != config.WorkerID {
		return nil, errors.New("worker runtime: inventory does not match worker identity")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Retention <= 0 {
		config.Retention = DefaultRetention
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Runtime{config: config, desiredInventory: config.Inventory, journal: journal, driver: driver, log: logger.With("component", "worker-runtime")}, nil
}

// advertisedCapabilities merges the capabilities an operator configured for
// this host with the package capabilities this build actually implements.
//
// The two answer different questions and only the worker can answer the second.
// Configuration describes the host: that it has Git, or a Huyang checkout, or
// reachable internet. A package capability describes the binary: whether this
// build knows how to honour a declaration the coordinator may send. A
// coordinator reading its own configuration file cannot know which build is
// running on the far end, so a worker that reported only its configured list
// would look equally capable before and after an upgrade.
//
// Reporting the build's own list is what lets the coordinator exclude an older
// worker before assignment rather than discovering the gap mid-attempt. The
// worker still refuses a package requiring an unsupported capability when it
// validates one, so this is the explanation, not the only defence.
func advertisedCapabilities(configured []string) []string {
	merged := append([]string(nil), configured...)
	if !slices.Contains(merged, workerproto.CapabilityTaskWaitCollectionFence) {
		merged = append(merged, workerproto.CapabilityTaskWaitCollectionFence)
	}
	if !slices.Contains(merged, workerproto.CapabilityCampaignSupervision) {
		merged = append(merged, workerproto.CapabilityCampaignSupervision)
	}
	if !slices.Contains(merged, workerproto.CapabilityQuotaObservations) {
		merged = append(merged, workerproto.CapabilityQuotaObservations)
	}
	for _, capability := range workerproto.SupportedPackageCapabilities() {
		if !slices.Contains(merged, capability) {
			merged = append(merged, capability)
		}
	}
	slices.Sort(merged)
	return merged
}

func (r *Runtime) Snapshot(ctx context.Context) (domain.WorkerSnapshot, error) {
	if r.config.ObserveInventory != nil {
		inventory, err := r.config.ObserveInventory(ctx, r.desiredInventory)
		if err != nil {
			return domain.WorkerSnapshot{}, err
		}
		r.config.Inventory = inventory
	}
	if err := r.Reconcile(ctx); err != nil {
		return domain.WorkerSnapshot{}, err
	}
	now := r.now()
	var quota []domain.WorkerQuotaObservation
	if r.reportQuota && r.config.Quota != nil {
		observed, err := r.config.Quota.Observations(ctx)
		if err != nil {
			r.log.Warn("host quota observations unavailable; snapshot carries none", "error", err)
		} else {
			quota = observed
		}
	}
	var snapshot domain.WorkerSnapshot
	err := r.journal.update(func(state *journalState) error {
		state.Sequence++
		assignments := make([]domain.WorkerAssignmentObservation, 0, len(state.Attempts))
		for _, id := range sortedAttemptIDs(state.Attempts) {
			record := state.Attempts[id]
			assignments = append(assignments, observation(record, now))
		}
		inventory := r.config.Inventory
		inventory.Capabilities = advertisedCapabilities(inventory.Capabilities)
		snapshot = domain.WorkerSnapshot{
			WorkerID: r.config.WorkerID, WorkerEpoch: r.config.WorkerEpoch,
			CoordinatorEpoch: r.config.CoordinatorEpoch, Sequence: state.Sequence,
			Connected: true, Inventory: inventory, Assignments: assignments,
			QuotaObservations: quota,
			ObservedAt:        now, ValidUntil: now.Add(r.config.SnapshotTTL),
		}
		return nil
	})
	return snapshot, err
}

// AcceptOffers claims every valid offer. An offer for an assignment the worker
// already holds is an idempotent replay when only the coordinator epoch inside
// the package changed; a higher assignment epoch supersedes the old execution,
// which is stopped before the new claim is recorded.
func (r *Runtime) AcceptOffers(ctx context.Context, offers workerproto.AssignmentOffers) (workerproto.AssignmentClaims, error) {
	now := r.now()
	claims := workerproto.AssignmentClaims{}
	err := r.journal.update(func(state *journalState) error {
		for _, offer := range offers.Offers {
			if err := r.validateOffer(offer, now); err != nil {
				return err
			}
			id := offer.Assignment.ID
			if existing, ok := state.Attempts[id]; ok {
				switch {
				case offer.Assignment.Epoch > existing.Assignment.Epoch:
					if err := r.containSuperseded(ctx, existing); err != nil {
						r.log.Warn("superseded assignment could not be contained; offer withheld",
							"assignment", id, "epoch", existing.Assignment.Epoch, "error", err)
						continue
					}
				case offer.Assignment.Epoch < existing.Assignment.Epoch:
					r.log.Warn("offer for an older assignment epoch ignored", "assignment", id,
						"offered_epoch", offer.Assignment.Epoch, "held_epoch", existing.Assignment.Epoch)
					continue
				default:
					if !samePackageIgnoringCoordinatorEpoch(existing.Package.Package, offer.Package.Package) {
						r.log.Warn("offer replay changed the execution package; offer withheld", "assignment", id)
						continue
					}
					if existing.Package.SHA256 != offer.Package.SHA256 {
						existing.Package = offer.Package
						existing.UpdatedAt = now
						state.Attempts[id] = existing
						state.Sequence++
					}
					claims.Claims = append(claims.Claims, claim(existing.Assignment, r.config, now))
					continue
				}
			}
			record := AttemptRecord{
				Assignment: offer.Assignment, Package: offer.Package, Phase: PhaseClaimed,
				CommandRequests:  make(map[string]domain.WorkerCommand),
				CommandResults:   make(map[string]domain.WorkerAcknowledgement),
				ThrottleRequests: make(map[string]domain.ThrottleCommand),
				ThrottleResults:  make(map[string]domain.ThrottleAcknowledgement), UpdatedAt: now,
			}
			record.Assignment.State = domain.AssignmentClaimed
			record.Assignment.WorkerEpoch = r.config.WorkerEpoch
			record.Assignment.LeaseExpiresAt = minTime(offer.Assignment.LeaseExpiresAt, now.Add(r.config.LeaseDuration))
			state.Attempts[id] = record
			state.Sequence++
			claims.Claims = append(claims.Claims, claim(record.Assignment, r.config, now))
		}
		return nil
	})
	return claims, err
}

// containSuperseded proves that an older execution of the same assignment can
// no longer produce effects before its record is replaced. A phase before
// dispatch can still own a prepared contained server, so its custody is
// released through the driver instead of being assumed absent.
func (r *Runtime) containSuperseded(ctx context.Context, existing AttemptRecord) error {
	switch existing.Phase {
	case PhaseClaimed, PhasePreparing, PhasePrepared:
		return r.stopPreparation(ctx, existing.Package.Package)
	case PhaseCompleted, PhaseFailed:
		return nil
	}
	if existing.Package.Package.Identity.ThreadID == "" {
		return nil
	}
	return r.driver.StopThread(ctx, existing.Package.Package)
}

func samePackageIgnoringCoordinatorEpoch(left, right workerproto.ExecutionPackage) bool {
	left.CoordinatorEpoch = 0
	right.CoordinatorEpoch = 0
	return reflect.DeepEqual(left, right)
}

func (r *Runtime) LeaseRenewals() (workerproto.LeaseRenewals, error) {
	now := r.now()
	state, err := r.journal.snapshot()
	if err != nil {
		return workerproto.LeaseRenewals{}, err
	}
	result := workerproto.LeaseRenewals{}
	for _, id := range sortedAttemptIDs(state.Attempts) {
		record := state.Attempts[id]
		if terminalPhase(record.Phase) {
			continue
		}
		result.Renewals = append(result.Renewals, domain.AssignmentLeaseRenewal{
			CoordinatorEpoch: r.config.CoordinatorEpoch, WorkerID: r.config.WorkerID,
			WorkerEpoch: r.config.WorkerEpoch, WorkerSequence: state.Sequence,
			AssignmentID: id, AssignmentEpoch: record.Assignment.Epoch,
			LeaseToken: record.Assignment.LeaseToken, RenewedAt: now,
			LeaseExpiresAt: now.Add(r.config.LeaseDuration),
		})
	}
	return result, nil
}

func (r *Runtime) ApplyLeaseRenewals(renewals workerproto.LeaseRenewals) error {
	now := r.now()
	maxExpiry := now.Add(r.config.LeaseDuration)
	return r.journal.update(func(state *journalState) error {
		for _, renewal := range renewals.Renewals {
			record, ok := state.Attempts[renewal.AssignmentID]
			if !ok {
				return fmt.Errorf("worker runtime: renewal for unknown assignment %q", renewal.AssignmentID)
			}
			// The worker sequence advances on every exchange, so a renewal
			// planned from an earlier snapshot is still authorized; the epochs
			// and the lease token are the fence. A bad renewal is skipped so
			// the other assignments keep their leases.
			if renewal.CoordinatorEpoch != r.config.CoordinatorEpoch || renewal.WorkerID != r.config.WorkerID ||
				renewal.WorkerEpoch != r.config.WorkerEpoch || renewal.AssignmentEpoch != record.Assignment.Epoch ||
				renewal.LeaseToken != record.Assignment.LeaseToken {
				r.log.Warn("stale or unauthorized lease renewal ignored", "assignment", renewal.AssignmentID)
				continue
			}
			if !renewal.LeaseExpiresAt.After(now) || !renewal.LeaseExpiresAt.After(record.Assignment.LeaseExpiresAt) ||
				renewal.LeaseExpiresAt.After(maxExpiry) {
				r.log.Warn("lease renewal outside the authorized live interval ignored", "assignment", renewal.AssignmentID)
				continue
			}
			record.Assignment.LeaseExpiresAt = renewal.LeaseExpiresAt
			record.UpdatedAt = now
			state.Attempts[renewal.AssignmentID] = record
			state.Sequence++
		}
		return nil
	})
}

func (r *Runtime) DeliverCommands(ctx context.Context, delivery workerproto.CommandDelivery) (workerproto.Acknowledgements, error) {
	result := workerproto.Acknowledgements{}
	for _, command := range delivery.Commands {
		ack, err := r.deliverCommand(ctx, command, delivery.Packages[command.AssignmentID])
		if err != nil {
			return result, err
		}
		result.Acknowledgements = append(result.Acknowledgements, ack)
	}
	return result, nil
}

// deliverCommand acknowledges a command once the worker has durably taken
// responsibility for it. Execution outcomes reach the coordinator through
// observations and results, not through the acknowledgement: a preparation
// that must be retried is still an accepted command.
func (r *Runtime) deliverCommand(ctx context.Context, command domain.WorkerCommand, supplied workerproto.ExecutionPackageManifest) (domain.WorkerAcknowledgement, error) {
	state, err := r.journal.snapshot()
	if err != nil {
		return domain.WorkerAcknowledgement{}, err
	}
	if record, ok := state.Attempts[command.AssignmentID]; ok {
		if ack, ok := record.CommandResults[command.ID]; ok {
			if original, found := record.CommandRequests[command.ID]; !found || original != command {
				return domain.WorkerAcknowledgement{}, errors.New("worker runtime: command id was reused with different content")
			}
			return ack, nil
		}
	}
	if err := r.validateCommand(command, state); err != nil {
		return r.rejectCommand(command, err.Error())
	}
	record := state.Attempts[command.AssignmentID]
	if supplied.Package.ID != "" && supplied.SHA256 != record.Package.SHA256 &&
		!samePackageIgnoringCoordinatorEpoch(supplied.Package, record.Package.Package) {
		return r.rejectCommand(command, "execution package changed after claim")
	}
	var effectErr error
	switch command.Kind {
	case domain.WorkerCommandPrepare:
		effectErr = r.prepare(ctx, command.AssignmentID)
	case domain.WorkerCommandDispatch:
		effectErr = r.dispatch(ctx, command.AssignmentID)
	case domain.WorkerCommandStop:
		effectErr = r.stop(ctx, command.AssignmentID)
	case domain.WorkerCommandCollect:
		// A commanded collection is still a collection: it publishes outputs
		// and destroys the identity record the resumed turn needs. It goes
		// through the same park check as the self-driven path.
		effectErr = r.collectUnlessWaiting(ctx, command.AssignmentID, record)
	default:
		return r.rejectCommand(command, "unsupported command kind")
	}
	detail := ""
	if effectErr != nil {
		detail = effectErr.Error()
		r.log.Warn("worker command effect deferred", "command", command.ID, "kind", command.Kind,
			"assignment", command.AssignmentID, "error", effectErr)
	}
	return r.finishCommand(command, true, detail)
}

func (r *Runtime) DeliverThrottle(ctx context.Context, commands []domain.ThrottleCommand) ([]domain.ThrottleAcknowledgement, error) {
	results := make([]domain.ThrottleAcknowledgement, 0, len(commands))
	for _, command := range commands {
		ack, err := r.deliverThrottle(ctx, command)
		if err != nil {
			return results, err
		}
		results = append(results, ack)
	}
	return results, nil
}

func (r *Runtime) deliverThrottle(ctx context.Context, command domain.ThrottleCommand) (domain.ThrottleAcknowledgement, error) {
	state, err := r.journal.snapshot()
	if err != nil {
		return domain.ThrottleAcknowledgement{}, err
	}
	record, ok := state.Attempts[command.AssignmentID]
	if ok {
		if ack, found := record.ThrottleResults[command.ID]; found {
			if original, exists := record.ThrottleRequests[command.ID]; !exists || !reflect.DeepEqual(original, command) {
				return domain.ThrottleAcknowledgement{}, errors.New("worker runtime: throttle command id was reused with different content")
			}
			return ack, nil
		}
	}
	if !ok || command.WorkerID != r.config.WorkerID || command.AttemptID != record.Assignment.AttemptID ||
		command.AssignmentEpoch != record.Assignment.Epoch || command.ThreadID != record.Package.Package.Identity.ThreadID ||
		command.WorkspacePath != record.WorkspacePath || command.Route.WorkerID != r.config.WorkerID ||
		command.QuotaPoolID != record.Package.Package.Route.QuotaPoolID {
		return r.finishThrottle(command, false, "", nil, "stale or mismatched throttle command")
	}
	if err := r.journal.update(func(state *journalState) error {
		current := state.Attempts[command.AssignmentID]
		current.PendingThrottle = &command
		current.UpdatedAt = r.now()
		state.Attempts[command.AssignmentID] = current
		state.Sequence++
		return nil
	}); err != nil {
		return domain.ThrottleAcknowledgement{}, err
	}
	return r.executeThrottle(ctx, command)
}

func (r *Runtime) executeThrottle(ctx context.Context, command domain.ThrottleCommand) (domain.ThrottleAcknowledgement, error) {
	state, err := r.journal.snapshot()
	if err != nil {
		return domain.ThrottleAcknowledgement{}, err
	}
	record := state.Attempts[command.AssignmentID]
	pkg := record.Package.Package
	if command.Kind == domain.ThrottleCommandResume && pkg.Timeout > 0 && !r.now().Before(pkg.CreatedAt.Add(pkg.Timeout)) {
		return r.finishThrottle(command, false, "", nil, "task timeout expired")
	}
	var result domain.ThrottleAcknowledgementResult
	var checkpoint *domain.CheckpointMetadata
	switch command.Kind {
	case domain.ThrottleCommandWarn:
		err = r.driver.Warn(ctx, pkg, command)
		result = domain.ThrottleResultWarned
	case domain.ThrottleCommandDrain:
		checkpoint, err = r.driver.Checkpoint(ctx, pkg, command)
		if err == nil && checkpoint == nil {
			err = errors.New("checkpoint evidence is missing")
		}
		if err != nil {
			result = domain.ThrottleResultCheckpointFailed
		} else {
			result = domain.ThrottleResultCheckpointed
		}
	case domain.ThrottleCommandHardStop:
		err = r.driver.StopThread(ctx, pkg)
		result = domain.ThrottleResultStopped
	case domain.ThrottleCommandResume:
		err = r.driver.Resume(ctx, pkg, command)
		result = domain.ThrottleResultResumed
	default:
		err = errors.New("unsupported throttle command kind")
	}
	detail := ""
	if err != nil {
		detail = err.Error()
	}
	return r.finishThrottle(command, err == nil, result, checkpoint, detail)
}

// Reconcile advances every durable attempt as far as local evidence allows.
// A failure on one attempt is recorded on that attempt and never prevents
// the others from progressing; only journal I/O errors are returned.
func (r *Runtime) Reconcile(ctx context.Context) error {
	if observer, ok := r.driver.(interface{ BeginObservationPass() }); ok {
		observer.BeginObservationPass()
	}
	state, err := r.journal.snapshot()
	if err != nil {
		return err
	}
	now := r.now()
	for _, id := range sortedAttemptIDs(state.Attempts) {
		if err := ctx.Err(); err != nil {
			return err
		}
		record := state.Attempts[id]
		if taskTimeoutExpired(record, now) {
			if err := r.expireTask(ctx, id, record); err != nil {
				return err
			}
			continue
		}
		if record.PendingThrottle != nil {
			if _, err := r.executeThrottle(ctx, *record.PendingThrottle); err != nil {
				return err
			}
			continue
		}
		if err := r.reconcileAttempt(ctx, id, record, now); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) reconcileAttempt(ctx context.Context, id string, record AttemptRecord, now time.Time) error {
	var err error
	switch record.Phase {
	case PhaseRunning:
		paused, pauseErr := r.pauseForQuota(ctx, id, &record)
		if pauseErr != nil {
			err = pauseErr
			break
		}
		if paused {
			break
		}
		threadState, observeErr := r.driver.ObserveThread(ctx, record.Package.Package)
		switch {
		case observeErr != nil:
			r.log.Warn("T3 observation unavailable; attempt keeps running", "assignment", id, "error", observeErr)
		case threadState == backlog.DispatchThreadStopped && record.LocalThrottle != nil:
			// The drain request was honoured: the thread checkpointed and
			// ended its turn. This is the pause taking effect, not the
			// attempt finishing, so nothing is collected.
			err = r.markLocalPauseStopped(id, nil)
		case threadState == backlog.DispatchThreadStopped:
			if err = r.markPhase(id, PhaseStopped, "", record.WorkspacePath, record.Package.Package.Identity.ThreadID); err == nil {
				err = r.collectUnlessWaiting(ctx, id, record)
			}
		case threadState == backlog.DispatchThreadMissing:
			err = r.markUnknown(id, "running T3 thread is missing")
		}
		if observeErr == nil {
			err = errors.Join(err, r.noteThreadState(id, threadState))
		}
	case PhaseWaiting:
		err = r.reconcileWaiting(ctx, id, record)
	case PhaseStopped:
		if hasCommandRequest(record, domain.WorkerCommandStop) {
			if !record.StopConfirmed {
				err = r.stop(ctx, id)
			} else if now.Sub(record.UpdatedAt) >= r.config.Retention {
				err = r.prune(ctx, id, record)
			}
			break
		}
		if record.LocalThrottle != nil {
			err = r.reconcileLocalPause(ctx, id, record, now)
			break
		}
		if len(record.ThrottleRequests) == 0 {
			threadState, observeErr := r.driver.ObserveThread(ctx, record.Package.Package)
			switch {
			case observeErr != nil:
				r.log.Warn("T3 observation unavailable; stopped attempt waits", "assignment", id, "error", observeErr)
			case threadState == backlog.DispatchThreadActive:
				err = r.markPhase(id, PhaseRunning, "", record.WorkspacePath, record.Package.Package.Identity.ThreadID)
			case threadState == backlog.DispatchThreadStopped && !hasCommandRequest(record, domain.WorkerCommandStop):
				err = r.collectUnlessWaiting(ctx, id, record)
			}
		}
	case PhasePreparing:
		workspace, exists, inspectErr := r.driver.InspectWorkspace(ctx, record.Package.Package)
		if inspectErr != nil {
			r.log.Warn("workspace observation failed", "assignment", id, "error", inspectErr)
			break
		}
		if exists {
			err = r.markPhase(id, PhasePrepared, "", workspace, "")
		} else {
			err = r.prepare(ctx, id)
		}
		if err == nil && hasCommandRequest(record, domain.WorkerCommandDispatch) {
			err = r.dispatchIfPrepared(ctx, id)
		}
	case PhasePrepared:
		if hasCommandRequest(record, domain.WorkerCommandDispatch) {
			err = r.dispatchIfPrepared(ctx, id)
		}
	case PhaseDispatching:
		err = r.reconcileDispatch(ctx, id)
	case PhaseStopping:
		if stopErr := r.driver.StopThread(ctx, record.Package.Package); stopErr != nil {
			r.log.Warn("stop outcome is unproven; retrying next reconcile", "assignment", id, "error", stopErr)
		} else {
			err = r.confirmStop(id)
		}
	case PhaseCollecting:
		err = r.collect(ctx, id)
	case PhaseUnknown:
		err = r.recoverUnknown(ctx, id, record)
	case PhaseCompleted:
		if record.SettlePending {
			if settleErr := r.driver.Settle(ctx, record.Package.Package); settleErr != nil {
				r.log.Warn("T3 settlement still unproven; retrying next reconcile", "assignment", id, "error", settleErr)
			} else {
				err = r.journal.update(func(state *journalState) error {
					if current, ok := state.Attempts[id]; ok {
						current.SettlePending = false
						state.Attempts[id] = current
						state.Sequence++
					}
					return nil
				})
			}
			break
		}
		if now.Sub(record.UpdatedAt) >= r.config.Retention {
			err = r.prune(ctx, id, record)
		}
	}
	if err != nil {
		if isJournalError(err) {
			return err
		}
		r.log.Warn("attempt reconciliation deferred", "assignment", id, "phase", record.Phase, "error", err)
	}
	return nil
}

// recoverUnknown re-observes an attempt whose last effect was ambiguous. The
// deterministic T3 thread identity makes the outcome provable later: a missing
// thread means the execution never started, a stopped thread can be collected,
// and an active thread is simply running.
func (r *Runtime) recoverUnknown(ctx context.Context, id string, record AttemptRecord) error {
	pkg := record.Package.Package
	threadState, err := r.driver.ObserveThread(ctx, pkg)
	if err != nil {
		r.log.Warn("unknown attempt cannot be observed yet", "assignment", id, "error", err)
		return nil
	}
	switch threadState {
	case backlog.DispatchThreadActive:
		return r.markPhase(id, PhaseRunning, "", record.WorkspacePath, pkg.Identity.ThreadID)
	case backlog.DispatchThreadStopped:
		if err := r.markPhase(id, PhaseStopped, "", record.WorkspacePath, pkg.Identity.ThreadID); err != nil {
			return err
		}
		return r.collectUnlessWaiting(ctx, id, record)
	case backlog.DispatchThreadMissing:
		reason := record.Failure
		if reason == "" {
			reason = "execution could not be recovered"
		}
		return r.markFailed(ctx, id, "T3 thread never started: "+reason)
	default:
		return nil
	}
}

func (r *Runtime) prune(ctx context.Context, id string, record AttemptRecord) error {
	if err := r.driver.Cleanup(ctx, record.Package.Package, record.WorkspacePath); err != nil {
		return err
	}
	return r.journal.update(func(state *journalState) error {
		if current, ok := state.Attempts[id]; ok && (current.Phase == PhaseCompleted || (current.Phase == PhaseStopped && current.StopConfirmed)) {
			delete(state.Attempts, id)
			state.Sequence++
		}
		return nil
	})
}

// setPrepareAttempts records how many preparation attempts have been spent on
// one assignment. It is written before the attempt runs so that a worker which
// dies inside it still comes back knowing the attempt happened.
func (r *Runtime) setPrepareAttempts(id string, attempts int) error {
	return r.journal.update(func(state *journalState) error {
		current, ok := state.Attempts[id]
		if !ok || current.PrepareAttempts == attempts {
			return nil
		}
		current.PrepareAttempts = attempts
		current.UpdatedAt = r.now()
		state.Attempts[id] = current
		state.Sequence++
		return nil
	})
}

func (r *Runtime) prepare(ctx context.Context, id string) error {
	state, err := r.journal.snapshot()
	if err != nil {
		return err
	}
	record := state.Attempts[id]
	if record.Phase == PhasePrepared || record.Phase == PhaseRunning || record.Phase == PhaseStopped ||
		record.Phase == PhaseCompleted || record.Phase == PhaseDispatching || record.Phase == PhaseCollecting {
		return nil
	}
	if record.Phase != PhaseClaimed && record.Phase != PhasePreparing {
		return fmt.Errorf("prepare is invalid in phase %q", record.Phase)
	}
	if record.Phase != PhasePreparing {
		if err := r.markPhase(id, PhasePreparing, "", "", ""); err != nil {
			return err
		}
	}
	// The attempt is counted before it runs, not after it fails. A worker that
	// restarts between the driver failing and the journal write would otherwise
	// come back believing no attempt had been made: the budget would never
	// terminate, and the preparation logs on disk would keep taking ordinals
	// until retention itself failed.
	attempts := record.PrepareAttempts + 1
	if err := r.setPrepareAttempts(id, attempts); err != nil {
		return err
	}
	workspace, err := r.driver.Prepare(ctx, record.Package.Package)
	if err != nil {
		if errors.Is(err, ErrContainedCustody) {
			// An uncertain contained preparation must never spend budget: its
			// outcome is unknown, and exhausting the budget would turn not
			// knowing into a terminal decision. The reservation is therefore
			// given back. A worker that dies inside this window keeps the
			// reservation, which costs one attempt of an uncertain preparation
			// and is the safe direction: the alternative, counting nothing
			// until the driver reports, is what let an ordinary failure lose
			// its count and the budget run forever.
			if releaseErr := r.setPrepareAttempts(id, record.PrepareAttempts); releaseErr != nil {
				return releaseErr
			}
			return err
		}
		// The first preparation failure is the one that explains why the
		// repository could not be prepared; every later one often only
		// reports the debris the first one left. Keep it durably, before
		// the terminal decision, and quote both in the terminal reason.
		//
		// An earlier attempt that left no cause is one whose worker restarted
		// inside that window. This error is then not the first one, and saying
		// so is better than quoting it as if it were: the first cause is in the
		// preparation log the lost attempt retained.
		first := record.FirstPrepareFailure
		lostFirst := first == "" && record.PrepareAttempts > 0
		if first == "" && !lostFirst {
			first = err.Error()
		}
		if attempts >= MaxPrepareAttempts {
			quoted := first
			if lostFirst {
				quoted = fmt.Sprintf("not retained, the worker restarted before it was recorded; read %s",
					backlog.PreparationLogName(record.Package.Package.Identity.AttemptID, 1))
			}
			reason := fmt.Sprintf("preparation failed %d times; first error: %s; last error: %v", attempts, quoted, err)
			if markErr := r.markFailed(ctx, id, reason); markErr != nil {
				return markErr
			}
			return nil
		}
		if updateErr := r.journal.update(func(state *journalState) error {
			current, ok := state.Attempts[id]
			if !ok {
				return nil
			}
			current.PrepareAttempts = attempts
			if current.FirstPrepareFailure == "" {
				current.FirstPrepareFailure = first
			}
			current.Failure = "preparation failed: " + err.Error()
			current.UpdatedAt = r.now()
			state.Attempts[id] = current
			state.Sequence++
			return nil
		}); updateErr != nil {
			return updateErr
		}
		return fmt.Errorf("preparation attempt %d failed: %w", attempts, err)
	}
	if strings.TrimSpace(workspace) == "" {
		return r.markFailed(ctx, id, "preparation returned an empty workspace")
	}
	return r.markPhase(id, PhasePrepared, "", workspace, "")
}

func (r *Runtime) dispatchIfPrepared(ctx context.Context, id string) error {
	state, err := r.journal.snapshot()
	if err != nil {
		return err
	}
	if state.Attempts[id].Phase != PhasePrepared {
		return nil
	}
	return r.dispatch(ctx, id)
}

func (r *Runtime) dispatch(ctx context.Context, id string) error {
	state, err := r.journal.snapshot()
	if err != nil {
		return err
	}
	record := state.Attempts[id]
	switch record.Phase {
	case PhaseRunning, PhaseStopped, PhaseStopping, PhaseWaiting, PhaseCollecting, PhaseCompleted, PhaseFailed:
		return nil
	case PhaseClaimed, PhasePreparing:
		if err := r.prepare(ctx, id); err != nil {
			return err
		}
		if state, err = r.journal.snapshot(); err != nil {
			return err
		}
		record = state.Attempts[id]
		if record.Phase != PhasePrepared {
			return nil
		}
	}
	if taskTimeoutExpired(record, r.now()) {
		return r.expireTask(ctx, id, record)
	}
	if record.Phase != PhasePrepared && record.Phase != PhaseDispatching {
		return fmt.Errorf("dispatch is invalid in phase %q", record.Phase)
	}
	if record.Phase != PhaseDispatching {
		if err := r.markPhase(id, PhaseDispatching, "", record.WorkspacePath, ""); err != nil {
			return err
		}
	}
	return r.reconcileDispatch(ctx, id)
}

func (r *Runtime) reconcileDispatch(ctx context.Context, id string) error {
	state, err := r.journal.snapshot()
	if err != nil {
		return err
	}
	record := state.Attempts[id]
	pkg := record.Package.Package
	threadState, err := r.driver.ObserveThread(ctx, pkg)
	if err != nil {
		return fmt.Errorf("T3 observation unavailable before dispatch: %w", err)
	}
	switch threadState {
	case backlog.DispatchThreadActive:
		return r.markPhase(id, PhaseRunning, "", record.WorkspacePath, pkg.Identity.ThreadID)
	case backlog.DispatchThreadStopped:
		return r.markPhase(id, PhaseStopped, "", record.WorkspacePath, pkg.Identity.ThreadID)
	case backlog.DispatchThreadMissing:
		if err := r.driver.CreateThread(ctx, pkg, record.WorkspacePath); err != nil {
			observed, observeErr := r.driver.ObserveThread(ctx, pkg)
			switch {
			case observeErr == nil && observed == backlog.DispatchThreadActive:
				return r.markPhase(id, PhaseRunning, "", record.WorkspacePath, pkg.Identity.ThreadID)
			case observeErr == nil && observed == backlog.DispatchThreadStopped:
				return r.markPhase(id, PhaseStopped, "", record.WorkspacePath, pkg.Identity.ThreadID)
			case observeErr == nil && observed == backlog.DispatchThreadMissing:
				// No thread exists, so the failed create had no provider effect.
				return r.markFailed(ctx, id, "T3 thread creation failed: "+err.Error())
			default:
				return r.markUnknown(id, "T3 create outcome is ambiguous: "+err.Error())
			}
		}
		observed, err := r.driver.ObserveThread(ctx, pkg)
		if err != nil || observed == backlog.DispatchThreadMissing {
			detail := "T3 create could not be proven"
			if err != nil {
				detail += ": " + err.Error()
			}
			return r.markUnknown(id, detail)
		}
		// A successful deterministic create-and-start response plus a visible
		// thread proves the effect. T3 may project the new session as stopped
		// briefly before its asynchronous provider startup becomes visible.
		return r.markPhase(id, PhaseRunning, "", record.WorkspacePath, pkg.Identity.ThreadID)
	default:
		return r.markUnknown(id, "T3 returned an unknown dispatch state")
	}
}

func (r *Runtime) stop(ctx context.Context, id string) error {
	state, err := r.journal.snapshot()
	if err != nil {
		return err
	}
	record := state.Attempts[id]
	switch record.Phase {
	case PhaseCompleted, PhaseFailed:
		return nil
	case PhaseClaimed, PhasePreparing, PhasePrepared:
		return r.markFailed(ctx, id, "stopped by the coordinator before dispatch")
	case PhaseStopped:
		if err := r.driver.StopThread(ctx, record.Package.Package); err != nil {
			return fmt.Errorf("stop settlement is unproven: %w", err)
		}
		return r.confirmStop(id)
	case PhaseUnknown:
		return r.recoverUnknown(ctx, id, record)
	}
	if err := r.markPhase(id, PhaseStopping, "", record.WorkspacePath, record.ThreadID); err != nil {
		return err
	}
	if err := r.driver.StopThread(ctx, record.Package.Package); err != nil {
		return fmt.Errorf("stop outcome is unproven: %w", err)
	}
	return r.confirmStop(id)
}

// confirmStop records successful provider stop separately from a naturally
// stopped thread or an accepted command whose provider effect is still unknown.
func (r *Runtime) confirmStop(id string) error {
	return r.journal.update(func(state *journalState) error {
		record := state.Attempts[id]
		record.Phase = PhaseStopped
		record.StopConfirmed = true
		record.ThreadID = record.Package.Package.Identity.ThreadID
		record.UpdatedAt = r.now()
		state.Attempts[id] = record
		state.Sequence++
		return nil
	})
}

// collectUnlessWaiting parks the attempt instead of collecting it while the
// coordinator still calls it parked.
//
// Parked is the coordinator's word, not a reading of wait liveness: an attempt
// whose wait has settled is still parked until the wake carrying that outcome
// has reached its thread. Between the two the parked turn has ended and the
// resumed one has not started, which looks exactly like a finished attempt from
// here.
//
// A probe that cannot answer defers the decision. Collecting is the dangerous
// direction: it publishes the outputs a parked task has not written yet, lets
// the coordinator verify against them, and removes the identity record the
// resumed turn needs. Waiting one more reconcile costs nothing.
func (r *Runtime) collectUnlessWaiting(ctx context.Context, id string, record AttemptRecord) error {
	// Failed attempts have already fenced any task effects. Publishing their
	// failure is not output collection and cannot depend on a provider turn that
	// may never have started (for example, an exhausted preparation budget).
	if record.Phase == PhaseFailed {
		return r.collect(ctx, id)
	}
	// Stopped/stopped observations cannot distinguish a task that parked and
	// resumed entirely between polls. Bind this decision to the provider turn.
	if observer, ok := r.driver.(interface {
		ObserveThreadTurn(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, string, error)
	}); ok {
		observed, turnID, err := observer.ObserveThreadTurn(ctx, record.Package.Package)
		if err != nil {
			r.log.Warn("provider turn identity is unavailable; collection deferred", "assignment", id, "error", err)
			return nil
		}
		if observed == backlog.DispatchThreadActive {
			return r.markPhase(id, PhaseRunning, "", record.WorkspacePath, record.ThreadID)
		}
		if observed != backlog.DispatchThreadStopped || turnID == "" {
			return nil
		}
		if err := r.journal.update(func(state *journalState) error {
			current := state.Attempts[id]
			if current.ObservedTurnID != turnID {
				current.ObservedTurnID = turnID
				current.StopObservedSequence = 0
				state.Attempts[id] = current
				state.Sequence++
			}
			record = current
			return nil
		}); err != nil {
			return err
		}
	}
	// The stopped observation must become durable before an empty coordinator
	// statement can authorize collection. A report received earlier in this
	// very exchange is insufficient even when its TTL has not elapsed.
	if r.config.LiveTaskWait == nil {
		if err := r.journal.update(func(state *journalState) error {
			current := state.Attempts[id]
			if current.StopObservedSequence == 0 {
				state.Sequence++
				current.StopObservedSequence = state.Sequence
				state.Attempts[id] = current
			}
			record = current
			return nil
		}); err != nil {
			return err
		}
	}
	waiting, err := r.liveTaskWait(ctx, record)
	if err != nil {
		r.log.Warn("task-bound wait state is unavailable; collection deferred", "assignment", id, "error", err)
		return nil
	}
	if !waiting {
		if record.Phase == PhaseWaiting {
			// The wake landed and the resumed turn has ended. The attempt leaves
			// the parked phase through stopped, the one phase collection is
			// valid from, so there is a single collection path either way.
			//
			// This is the step that ends a park and, through the driver, removes
			// the task's identity record. It is logged because it is irreversible
			// and because it used to happen in a window where the coordinator had
			// settled the wait but the wake had not reached the thread.
			r.log.Info("nothing parks this attempt any more; the park ends and the outputs are collected",
				"assignment", id, "thread", record.Package.Package.Identity.ThreadID,
				"workspace", record.WorkspacePath)
			if err := r.markPhase(id, PhaseStopped, "", record.WorkspacePath, record.ThreadID); err != nil {
				return err
			}
		}
		return r.collect(ctx, id)
	}
	if record.Phase == PhaseWaiting {
		return nil
	}
	r.log.Info("turn ended on a live task-bound wait; the task is parked, not finished",
		"assignment", id, "thread", record.Package.Package.Identity.ThreadID)
	return r.markPhase(id, PhaseWaiting, "", record.WorkspacePath, record.Package.Package.Identity.ThreadID)
}

// reconcileWaiting advances a parked attempt. The wake message restarts the
// same thread, so an active thread means the task resumed; a thread that is
// still stopped is re-checked against the wait, and collected only once no wait
// is live.
func (r *Runtime) reconcileWaiting(ctx context.Context, id string, record AttemptRecord) error {
	if hasCommandRequest(record, domain.WorkerCommandStop) {
		return r.stop(ctx, id)
	}
	threadState, err := r.driver.ObserveThread(ctx, record.Package.Package)
	if err != nil {
		r.log.Warn("T3 observation unavailable; parked attempt waits", "assignment", id, "error", err)
		return nil
	}
	switch threadState {
	case backlog.DispatchThreadActive:
		return r.markPhase(id, PhaseRunning, "", record.WorkspacePath, record.Package.Package.Identity.ThreadID)
	case backlog.DispatchThreadStopped:
		return r.collectUnlessWaiting(ctx, id, record)
	case backlog.DispatchThreadMissing:
		return r.markUnknown(id, "parked T3 thread is missing")
	default:
		return nil
	}
}

func (r *Runtime) collect(ctx context.Context, id string) error {
	state, err := r.journal.snapshot()
	if err != nil {
		return err
	}
	record := state.Attempts[id]
	switch record.Phase {
	case PhaseCompleted:
		return nil
	case PhaseFailed:
		if err := r.driver.CollectFailure(ctx, record.Package.Package, record.WorkspacePath, record.Failure); err != nil {
			return fmt.Errorf("publish failed result: %w", err)
		}
		return r.markPhase(id, PhaseCompleted, record.Failure, record.WorkspacePath, record.ThreadID)
	case PhaseStopped, PhaseCollecting:
	default:
		return fmt.Errorf("collect is invalid in phase %q", record.Phase)
	}
	if record.LocalThrottle != nil {
		// A local quota pause is in force: the turn ended because this
		// worker stopped it, and it resumes when the bucket recovers.
		return errors.New("collection deferred: attempt is paused by the quota watchdog")
	}
	if record.Phase != PhaseCollecting {
		if err := r.markPhase(id, PhaseCollecting, "", record.WorkspacePath, record.ThreadID); err != nil {
			return err
		}
	}
	if record.WorkspacePath != "" {
		// A workspace that vanished (host cleanup, an older binary's eager
		// cleanup, a rollback) can never be finalized; publish the failure
		// instead of retrying collection forever.
		if _, exists, err := r.driver.InspectWorkspace(ctx, record.Package.Package); err == nil && !exists {
			if err := r.markFailed(ctx, id, "workspace is missing; outputs cannot be collected"); err != nil {
				return err
			}
			return r.collect(ctx, id)
		}
	}
	if err := r.collectWithPauseEvidence(ctx, record); err != nil {
		if !errors.Is(err, ErrSettleUnproven) {
			return fmt.Errorf("collection deferred: %w", err)
		}
		// The result is durable in custody; only the provider settlement is
		// still unproven. Complete the attempt and retry settlement later
		// instead of repeating collection.
		r.log.Warn("result published; T3 settlement deferred", "assignment", id, "error", err)
		return r.journal.update(func(state *journalState) error {
			current, ok := state.Attempts[id]
			if !ok {
				return fmt.Errorf("worker journal: unknown assignment %q", id)
			}
			current.Phase = PhaseCompleted
			current.Failure = ""
			current.SettlePending = true
			current.UpdatedAt = r.now()
			state.Attempts[id] = current
			state.Sequence++
			return nil
		})
	}
	return r.markPhase(id, PhaseCompleted, "", record.WorkspacePath, record.ThreadID)
}

func (r *Runtime) validateOffer(offer workerproto.AssignmentOffer, now time.Time) error {
	if !now.Before(offer.ExpiresAt) || offer.Assignment.ID == "" || offer.Assignment.WorkerID != r.config.WorkerID ||
		offer.Assignment.Epoch < 1 || offer.Assignment.LeaseToken == "" || !now.Before(offer.Assignment.LeaseExpiresAt) ||
		offer.Assignment.State != domain.AssignmentOffered {
		return errors.New("worker runtime: invalid or expired assignment offer")
	}
	if err := workerproto.ValidateExecutionPackageManifest(offer.Package, r.config.MaxPackageBytes); err != nil {
		return fmt.Errorf("worker runtime: execution package: %w", err)
	}
	pkg := offer.Package.Package
	if pkg.CoordinatorID != r.config.CoordinatorID || pkg.CoordinatorEpoch != r.config.CoordinatorEpoch ||
		pkg.WorkerID != r.config.WorkerID || pkg.WorkerEpoch != r.config.WorkerEpoch ||
		pkg.Identity.AssignmentID != offer.Assignment.ID || pkg.Identity.AssignmentEpoch != offer.Assignment.Epoch ||
		pkg.Identity.AttemptID != offer.Assignment.AttemptID || pkg.Identity.DispatchToken != offer.Assignment.DispatchToken ||
		pkg.Identity.ThreadID != offer.Assignment.ThreadID {
		return errors.New("worker runtime: offer and package identity do not match")
	}
	return nil
}

func (r *Runtime) validateCommand(command domain.WorkerCommand, state journalState) error {
	if command.ID == "" || command.WorkerID != r.config.WorkerID || command.WorkerEpoch != r.config.WorkerEpoch ||
		command.CoordinatorEpoch != r.config.CoordinatorEpoch || command.ExpectedWorkerSequence > state.Sequence {
		return errors.New("stale worker command identity or sequence")
	}
	record, ok := state.Attempts[command.AssignmentID]
	if !ok || command.AssignmentEpoch != record.Assignment.Epoch {
		return errors.New("unknown or stale assignment")
	}
	return nil
}

func (r *Runtime) rejectCommand(command domain.WorkerCommand, detail string) (domain.WorkerAcknowledgement, error) {
	return r.finishCommand(command, false, detail)
}

func (r *Runtime) finishCommand(command domain.WorkerCommand, accepted bool, detail string) (domain.WorkerAcknowledgement, error) {
	var result domain.WorkerAcknowledgement
	err := r.journal.update(func(state *journalState) error {
		record, ok := state.Attempts[command.AssignmentID]
		if !ok {
			result = domain.WorkerAcknowledgement{
				CommandID: command.ID, WorkerID: r.config.WorkerID, WorkerEpoch: r.config.WorkerEpoch,
				CoordinatorEpoch: r.config.CoordinatorEpoch, AssignmentID: command.AssignmentID,
				AssignmentEpoch: command.AssignmentEpoch, WorkerSequence: state.Sequence,
				Accepted: false, Detail: detail, AcknowledgedAt: r.now(),
			}
			return nil
		}
		if existing, ok := record.CommandResults[command.ID]; ok {
			result = existing
			return nil
		}
		state.Sequence++
		result = domain.WorkerAcknowledgement{
			CommandID: command.ID, WorkerID: r.config.WorkerID, WorkerEpoch: r.config.WorkerEpoch,
			CoordinatorEpoch: r.config.CoordinatorEpoch, AssignmentID: command.AssignmentID,
			AssignmentEpoch: command.AssignmentEpoch, WorkerSequence: state.Sequence,
			Accepted: accepted, Detail: detail, AcknowledgedAt: r.now(),
		}
		if record.CommandRequests == nil {
			record.CommandRequests = make(map[string]domain.WorkerCommand)
		}
		if record.CommandResults == nil {
			record.CommandResults = make(map[string]domain.WorkerAcknowledgement)
		}
		record.CommandRequests[command.ID] = command
		record.CommandResults[command.ID] = result
		record.UpdatedAt = result.AcknowledgedAt
		state.Attempts[command.AssignmentID] = record
		return nil
	})
	return result, err
}

func (r *Runtime) finishThrottle(command domain.ThrottleCommand, accepted bool, result domain.ThrottleAcknowledgementResult, checkpoint *domain.CheckpointMetadata, detail string) (domain.ThrottleAcknowledgement, error) {
	var acknowledgement domain.ThrottleAcknowledgement
	err := r.journal.update(func(state *journalState) error {
		record, ok := state.Attempts[command.AssignmentID]
		if !ok {
			acknowledgement = domain.ThrottleAcknowledgement{CommandID: command.ID, AttemptID: command.AttemptID, Accepted: false, Error: detail, AcknowledgedAt: r.now()}
			return nil
		}
		if existing, ok := record.ThrottleResults[command.ID]; ok {
			acknowledgement = existing
			return nil
		}
		acknowledgement = domain.ThrottleAcknowledgement{
			CommandID: command.ID, AttemptID: command.AttemptID, Accepted: accepted,
			Result: result, Checkpoint: checkpoint, Error: detail, AcknowledgedAt: r.now(),
		}
		if record.ThrottleRequests == nil {
			record.ThrottleRequests = make(map[string]domain.ThrottleCommand)
		}
		if record.ThrottleResults == nil {
			record.ThrottleResults = make(map[string]domain.ThrottleAcknowledgement)
		}
		record.ThrottleRequests[command.ID] = command
		record.ThrottleResults[command.ID] = acknowledgement
		record.PendingThrottle = nil
		if accepted {
			switch command.Kind {
			case domain.ThrottleCommandDrain:
				record.Phase = PhaseStopped
			case domain.ThrottleCommandHardStop:
				record.Phase = PhaseStopped
			case domain.ThrottleCommandResume:
				record.StopObservedSequence = 0
				record.Phase = PhaseRunning
			}
		}
		record.UpdatedAt = acknowledgement.AcknowledgedAt
		state.Attempts[command.AssignmentID] = record
		state.Sequence++
		return nil
	})
	return acknowledgement, err
}

func (r *Runtime) markPhase(id string, phase Phase, failure, workspace, thread string) error {
	return r.journal.update(func(state *journalState) error {
		record, ok := state.Attempts[id]
		if !ok {
			return fmt.Errorf("worker journal: unknown assignment %q", id)
		}
		if phase == PhaseRunning {
			record.StopObservedSequence = 0
		}
		record.Phase = phase
		record.Failure = failure
		if workspace != "" {
			record.WorkspacePath = workspace
		}
		if thread != "" {
			record.ThreadID = thread
		}
		record.UpdatedAt = r.now()
		state.Attempts[id] = record
		state.Sequence++
		return nil
	})
}

func (r *Runtime) markUnknown(id, detail string) error {
	r.log.Warn("attempt execution is unknown until re-observed", "assignment", id, "detail", detail)
	return r.markPhase(id, PhaseUnknown, detail, "", "")
}

// markFailed records a deterministic, effect-free failure. The coordinator
// observes it as a completed assignment and collects a failed result.
func (r *Runtime) markFailed(ctx context.Context, id, detail string) error {
	// A missing provider thread does not prove its dedicated server or verifier
	// stopped. Confirm custody before publishing any terminal failure.
	state, err := r.journal.snapshot()
	if err != nil {
		return err
	}
	record, exists := state.Attempts[id]
	if !exists {
		return errors.New("failed assignment missing from journal")
	}
	if err := r.stopPreparation(ctx, record.Package.Package); err != nil {
		return err
	}
	r.log.Warn("attempt failed on the worker", "assignment", id, "detail", detail)
	return r.markPhase(id, PhaseFailed, detail, "", "")
}

func (r *Runtime) now() time.Time { return r.config.Now().UTC() }

func isJournalError(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "worker journal:")
}

func hasCommandRequest(record AttemptRecord, kind domain.WorkerCommandKind) bool {
	for _, command := range record.CommandRequests {
		if command.Kind == kind {
			return true
		}
	}
	return false
}

func claim(assignment domain.Assignment, config Config, now time.Time) domain.AssignmentClaimRequest {
	return domain.AssignmentClaimRequest{
		CoordinatorEpoch: config.CoordinatorEpoch, WorkerID: config.WorkerID,
		WorkerEpoch: config.WorkerEpoch, AssignmentID: assignment.ID,
		AssignmentEpoch: assignment.Epoch, LeaseToken: assignment.LeaseToken,
		ClaimedAt: now, LeaseExpiresAt: assignment.LeaseExpiresAt,
	}
}

func observation(record AttemptRecord, now time.Time) domain.WorkerAssignmentObservation {
	state := record.Assignment.State
	control := domain.ControlState("")
	switch record.Phase {
	case PhaseClaimed, PhasePreparing, PhasePrepared, PhaseDispatching:
		state = domain.AssignmentClaimed
		control = domain.ControlPreparing
	case PhaseRunning:
		state = domain.AssignmentClaimed
		control = domain.ControlRunning
	case PhaseStopping, PhaseCollecting:
		state = domain.AssignmentClaimed
		control = domain.ControlRunning
	case PhaseWaiting:
		// The attempt keeps its assignment, workspace, artifacts and locks, but
		// reports a control state that holds no provider slot and releases the
		// executor capacity it reserved.
		state = domain.AssignmentClaimed
		control = domain.ControlWaitingExternal
	case PhaseStopped:
		// A thread that ended on its own is still owned by a live execution
		// that waits for collection; a delivered throttle command or the
		// worker's own quota pause makes the stop a pause.
		state = domain.AssignmentClaimed
		control = domain.ControlRunning
		if record.StopConfirmed && hasCommandRequest(record, domain.WorkerCommandStop) {
			state = domain.AssignmentReleased
			control = domain.ControlStopped
		} else if len(record.ThrottleRequests) != 0 || record.LocalThrottle != nil {
			control = domain.ControlPaused
		}
	case PhaseCompleted, PhaseFailed:
		state = domain.AssignmentCompleted
		control = domain.ControlStopped
	case PhaseUnknown:
		state = domain.AssignmentUnknown
		control = domain.ControlStopped
	}
	failure := record.Failure
	if len(failure) > 2048 {
		failure = failure[:2048]
	}
	pauseReason := ""
	if record.LocalThrottle != nil {
		pauseReason = record.LocalThrottle.Reason
	}
	return domain.WorkerAssignmentObservation{
		Journal: &domain.WorkerJournalExcerpt{
			Phase: string(record.Phase), Failure: failure, PauseReason: pauseReason, ThreadState: record.ObservedThreadState,
			PackageSHA256: record.Package.SHA256, GraphRevision: record.Package.Package.GraphRevision, TaskRevision: record.Package.Package.TaskRevision, UpdatedAt: record.UpdatedAt,
		},
		AssignmentID: record.Assignment.ID, AssignmentEpoch: record.Assignment.Epoch,
		State: state, Control: control, ThreadID: record.ThreadID,
		WorkspacePath: record.WorkspacePath, ObservedAt: now,
	}
}

// stopPreparation releases any contained execution custody the driver holds for
// this package. Drivers without contained preparation hold none.
func (r *Runtime) stopPreparation(ctx context.Context, pkg workerproto.ExecutionPackage) error {
	stopper, ok := r.driver.(interface {
		StopPreparation(context.Context, workerproto.ExecutionPackage) error
	})
	if !ok {
		return nil
	}
	return stopper.StopPreparation(ctx, pkg)
}

func terminalPhase(phase Phase) bool {
	return slices.Contains([]Phase{PhaseCompleted, PhaseFailed, PhaseUnknown}, phase)
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}
