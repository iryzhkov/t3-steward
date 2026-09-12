package workerruntime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type Driver interface {
	Prepare(context.Context, workerproto.ExecutionPackage) (string, error)
	InspectWorkspace(context.Context, workerproto.ExecutionPackage) (string, bool, error)
	ObserveThread(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, error)
	CreateThread(context.Context, workerproto.ExecutionPackage, string) error
	StopThread(context.Context, workerproto.ExecutionPackage) error
	Collect(context.Context, workerproto.ExecutionPackage, string) error
	Cleanup(context.Context, workerproto.ExecutionPackage, string) error
	Warn(context.Context, workerproto.ExecutionPackage, domain.ThrottleCommand) error
	Checkpoint(context.Context, workerproto.ExecutionPackage, domain.ThrottleCommand) (*domain.CheckpointMetadata, error)
	Resume(context.Context, workerproto.ExecutionPackage, domain.ThrottleCommand) error
}

type Config struct {
	WorkerID         string
	WorkerEpoch      string
	CoordinatorID    string
	CoordinatorEpoch int64
	SnapshotTTL      time.Duration
	LeaseDuration    time.Duration
	MaxPackageBytes  int64
	Inventory        domain.WorkerInventory
	Now              func() time.Time
}

type Runtime struct {
	config  Config
	journal *Journal
	driver  Driver
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
	return &Runtime{config: config, journal: journal, driver: driver}, nil
}

func (r *Runtime) Snapshot(ctx context.Context) (domain.WorkerSnapshot, error) {
	if err := r.Reconcile(ctx); err != nil {
		return domain.WorkerSnapshot{}, err
	}
	now := r.now()
	var snapshot domain.WorkerSnapshot
	err := r.journal.update(func(state *journalState) error {
		state.Sequence++
		assignments := make([]domain.WorkerAssignmentObservation, 0, len(state.Attempts))
		for _, id := range sortedAttemptIDs(state.Attempts) {
			record := state.Attempts[id]
			assignments = append(assignments, observation(record, now))
		}
		snapshot = domain.WorkerSnapshot{
			WorkerID: r.config.WorkerID, WorkerEpoch: r.config.WorkerEpoch,
			CoordinatorEpoch: r.config.CoordinatorEpoch, Sequence: state.Sequence,
			Connected: true, Inventory: r.config.Inventory, Assignments: assignments,
			ObservedAt: now, ValidUntil: now.Add(r.config.SnapshotTTL),
		}
		return nil
	})
	return snapshot, err
}

func (r *Runtime) AcceptOffers(_ context.Context, offers workerproto.AssignmentOffers) (workerproto.AssignmentClaims, error) {
	now := r.now()
	claims := workerproto.AssignmentClaims{}
	err := r.journal.update(func(state *journalState) error {
		for _, offer := range offers.Offers {
			if err := r.validateOffer(offer, now); err != nil {
				return err
			}
			id := offer.Assignment.ID
			if existing, ok := state.Attempts[id]; ok {
				if existing.Package.SHA256 != offer.Package.SHA256 || existing.Assignment.Epoch != offer.Assignment.Epoch {
					return fmt.Errorf("worker runtime: changed replay for assignment %q", id)
				}
				claims.Claims = append(claims.Claims, claim(existing.Assignment, r.config, now))
				continue
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

func (r *Runtime) LeaseRenewals() (workerproto.LeaseRenewals, error) {
	now := r.now()
	state, err := r.journal.snapshot()
	if err != nil {
		return workerproto.LeaseRenewals{}, err
	}
	result := workerproto.LeaseRenewals{}
	for _, id := range sortedAttemptIDs(state.Attempts) {
		record := state.Attempts[id]
		if terminalPhase(record.Phase) || !now.Before(record.Assignment.LeaseExpiresAt) {
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
		expectedSequence := state.Sequence
		for _, renewal := range renewals.Renewals {
			record, ok := state.Attempts[renewal.AssignmentID]
			if !ok {
				return fmt.Errorf("worker runtime: renewal for unknown assignment %q", renewal.AssignmentID)
			}
			if renewal.CoordinatorEpoch != r.config.CoordinatorEpoch || renewal.WorkerID != r.config.WorkerID ||
				renewal.WorkerEpoch != r.config.WorkerEpoch || renewal.AssignmentEpoch != record.Assignment.Epoch ||
				renewal.LeaseToken != record.Assignment.LeaseToken || renewal.WorkerSequence != expectedSequence {
				return errors.New("worker runtime: stale or unauthorized lease renewal")
			}
			if !renewal.LeaseExpiresAt.After(now) || !renewal.LeaseExpiresAt.After(record.Assignment.LeaseExpiresAt) ||
				renewal.LeaseExpiresAt.After(maxExpiry) {
				return errors.New("worker runtime: lease renewal is outside the authorized live interval")
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
	if supplied.Package.ID != "" && supplied.SHA256 != record.Package.SHA256 {
		return r.rejectCommand(command, "execution package changed after claim")
	}
	if command.Kind != domain.WorkerCommandStop && !r.now().Before(record.Assignment.LeaseExpiresAt) {
		if err := r.markUnknown(command.AssignmentID, "assignment lease expired before command"); err != nil {
			return domain.WorkerAcknowledgement{}, err
		}
		return r.rejectCommand(command, "assignment lease expired")
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
		effectErr = r.collect(ctx, command.AssignmentID)
	default:
		return r.rejectCommand(command, "unsupported command kind")
	}
	accepted := effectErr == nil
	detail := ""
	if effectErr != nil {
		detail = effectErr.Error()
	}
	return r.finishCommand(command, accepted, detail)
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
		if !r.now().Before(record.Assignment.LeaseExpiresAt) {
			err = errors.New("assignment lease expired before resume")
		} else {
			err = r.driver.Resume(ctx, pkg, command)
		}
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

func (r *Runtime) Reconcile(ctx context.Context) error {
	state, err := r.journal.snapshot()
	if err != nil {
		return err
	}
	for _, id := range sortedAttemptIDs(state.Attempts) {
		record := state.Attempts[id]
		if record.PendingThrottle != nil {
			if _, err := r.executeThrottle(ctx, *record.PendingThrottle); err != nil {
				return err
			}
			continue
		}
		if !terminalPhase(record.Phase) && !r.now().Before(record.Assignment.LeaseExpiresAt) {
			if err := r.markPhase(id, PhaseStopping, "lease expired; stopping before reconciliation", "", ""); err != nil {
				return err
			}
			if err := r.driver.StopThread(ctx, record.Package.Package); err != nil {
				return r.markUnknown(id, "lease expired and stop could not be proven: "+err.Error())
			}
			if err := r.markUnknown(id, "lease expired; execution stopped and requires coordinator reconciliation"); err != nil {
				return err
			}
			continue
		}
		switch record.Phase {
		case PhaseRunning:
			threadState, observeErr := r.driver.ObserveThread(ctx, record.Package.Package)
			if observeErr != nil {
				err = r.markUnknown(id, "T3 observation unavailable: "+observeErr.Error())
			} else if threadState == backlog.DispatchThreadStopped {
				if err = r.markPhase(id, PhaseStopped, "", record.WorkspacePath, record.Package.Package.Identity.ThreadID); err == nil {
					err = r.collect(ctx, id)
				}
			} else if threadState == backlog.DispatchThreadMissing {
				err = r.markUnknown(id, "running T3 thread is missing")
			}
		case PhaseStopped:
			if len(record.ThrottleRequests) == 0 {
				threadState, observeErr := r.driver.ObserveThread(ctx, record.Package.Package)
				if observeErr != nil {
					err = r.markUnknown(id, "T3 observation unavailable: "+observeErr.Error())
				} else if threadState == backlog.DispatchThreadActive {
					err = r.markPhase(id, PhaseRunning, "", record.WorkspacePath, record.Package.Package.Identity.ThreadID)
				} else if threadState == backlog.DispatchThreadStopped && !hasCommandRequest(record, domain.WorkerCommandStop) {
					err = r.collect(ctx, id)
				}
			}
		case PhasePreparing:
			workspace, exists, inspectErr := r.driver.InspectWorkspace(ctx, record.Package.Package)
			if inspectErr != nil {
				return r.markUnknown(id, "workspace observation failed: "+inspectErr.Error())
			}
			if exists {
				err = r.markPhase(id, PhasePrepared, "", workspace, "")
			} else {
				err = r.prepare(ctx, id)
			}
		case PhaseDispatching:
			err = r.reconcileDispatch(ctx, id)
		case PhaseStopping:
			if stopErr := r.driver.StopThread(ctx, record.Package.Package); stopErr != nil {
				err = r.markUnknown(id, "stop outcome is unproven: "+stopErr.Error())
			} else {
				err = r.markPhase(id, PhaseStopped, "", record.WorkspacePath, record.Package.Package.Identity.ThreadID)
			}
		case PhaseCollecting:
			err = r.collect(ctx, id)
		case PhaseCompleted:
			err = r.driver.Cleanup(ctx, record.Package.Package, record.WorkspacePath)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) prepare(ctx context.Context, id string) error {
	state, err := r.journal.snapshot()
	if err != nil {
		return err
	}
	record := state.Attempts[id]
	if record.Phase == PhasePrepared || record.Phase == PhaseRunning || record.Phase == PhaseStopped || record.Phase == PhaseCompleted {
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
	workspace, err := r.driver.Prepare(ctx, record.Package.Package)
	if err != nil {
		_ = r.markUnknown(id, "preparation outcome is unproven: "+err.Error())
		return err
	}
	if strings.TrimSpace(workspace) == "" {
		_ = r.markUnknown(id, "preparation returned an empty workspace")
		return errors.New("worker runtime: preparation returned an empty workspace")
	}
	return r.markPhase(id, PhasePrepared, "", workspace, "")
}

func (r *Runtime) dispatch(ctx context.Context, id string) error {
	state, err := r.journal.snapshot()
	if err != nil {
		return err
	}
	record := state.Attempts[id]
	if record.Phase == PhaseRunning {
		return nil
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
	threadState, err := r.driver.ObserveThread(ctx, record.Package.Package)
	if err != nil {
		_ = r.markUnknown(id, "T3 observation unavailable: "+err.Error())
		return err
	}
	switch threadState {
	case backlog.DispatchThreadActive:
		return r.markPhase(id, PhaseRunning, "", record.WorkspacePath, record.Package.Package.Identity.ThreadID)
	case backlog.DispatchThreadStopped:
		return r.markPhase(id, PhaseStopped, "", record.WorkspacePath, record.Package.Package.Identity.ThreadID)
	case backlog.DispatchThreadMissing:
		if err := r.driver.CreateThread(ctx, record.Package.Package, record.WorkspacePath); err != nil {
			observed, observeErr := r.driver.ObserveThread(ctx, record.Package.Package)
			if observeErr == nil && observed == backlog.DispatchThreadActive {
				return r.markPhase(id, PhaseRunning, "", record.WorkspacePath, record.Package.Package.Identity.ThreadID)
			}
			_ = r.markUnknown(id, "T3 create outcome is ambiguous: "+err.Error())
			return err
		}
		observed, err := r.driver.ObserveThread(ctx, record.Package.Package)
		if err != nil || observed == backlog.DispatchThreadMissing {
			detail := "T3 create could not be proven"
			if err != nil {
				detail += ": " + err.Error()
			}
			_ = r.markUnknown(id, detail)
			return errors.New(detail)
		}
		// A successful deterministic create-and-start response plus a visible
		// thread proves the effect. T3 may project the new session as stopped
		// briefly before its asynchronous provider startup becomes visible.
		return r.markPhase(id, PhaseRunning, "", record.WorkspacePath, record.Package.Package.Identity.ThreadID)
	default:
		_ = r.markUnknown(id, "T3 returned an unknown dispatch state")
		return errors.New("worker runtime: unknown T3 dispatch state")
	}
}

func (r *Runtime) stop(ctx context.Context, id string) error {
	state, err := r.journal.snapshot()
	if err != nil {
		return err
	}
	record := state.Attempts[id]
	if record.Phase == PhaseCompleted {
		return nil
	}
	if record.Phase == PhaseStopped {
		if err := r.driver.StopThread(ctx, record.Package.Package); err != nil {
			_ = r.markUnknown(id, "stop settlement is unproven: "+err.Error())
			return err
		}
		return nil
	}
	if record.Phase == PhaseUnknown {
		return errors.New("stop requires explicit recovery from unknown execution")
	}
	if err := r.markPhase(id, PhaseStopping, "", record.WorkspacePath, record.ThreadID); err != nil {
		return err
	}
	if err := r.driver.StopThread(ctx, record.Package.Package); err != nil {
		_ = r.markUnknown(id, "stop outcome is unproven: "+err.Error())
		return err
	}
	return r.markPhase(id, PhaseStopped, "", record.WorkspacePath, record.Package.Package.Identity.ThreadID)
}

func (r *Runtime) collect(ctx context.Context, id string) error {
	state, err := r.journal.snapshot()
	if err != nil {
		return err
	}
	record := state.Attempts[id]
	if record.Phase == PhaseCompleted {
		return nil
	}
	if record.Phase != PhaseStopped && record.Phase != PhaseCollecting {
		return fmt.Errorf("collect is invalid in phase %q", record.Phase)
	}
	if record.Phase != PhaseCollecting {
		if err := r.markPhase(id, PhaseCollecting, "", record.WorkspacePath, record.ThreadID); err != nil {
			return err
		}
	}
	if err := r.driver.Collect(ctx, record.Package.Package, record.WorkspacePath); err != nil {
		_ = r.markUnknown(id, "collection or artifact custody is unproven: "+err.Error())
		return err
	}
	if err := r.markPhase(id, PhaseCompleted, "", record.WorkspacePath, record.ThreadID); err != nil {
		return err
	}
	return r.driver.Cleanup(ctx, record.Package.Package, record.WorkspacePath)
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
			return fmt.Errorf("worker runtime: unknown assignment %q", id)
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
	return r.markPhase(id, PhaseUnknown, detail, "", "")
}

func (r *Runtime) now() time.Time { return r.config.Now().UTC() }

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
	case PhaseStopped:
		state = domain.AssignmentClaimed
		control = domain.ControlPaused
	case PhaseCompleted:
		state = domain.AssignmentCompleted
		control = domain.ControlStopped
	case PhaseUnknown:
		state = domain.AssignmentUnknown
		control = domain.ControlStopped
	}
	return domain.WorkerAssignmentObservation{
		AssignmentID: record.Assignment.ID, AssignmentEpoch: record.Assignment.Epoch,
		State: state, Control: control, ThreadID: record.ThreadID,
		WorkspacePath: record.WorkspacePath, ObservedAt: now,
	}
}
func terminalPhase(phase Phase) bool {
	return slices.Contains([]Phase{PhaseCompleted, PhaseUnknown}, phase)
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}
