package backlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ThrottleAttemptBinding is the immutable execution identity affected by a
// fleet throttle directive.
type ThrottleAttemptBinding struct {
	Attempt       domain.Attempt
	Assignment    domain.Assignment
	WorkspacePath string
}

// ThrottleDeliveryStore durably records intent before worker communication and
// acknowledgements after it.
type ThrottleDeliveryStore interface {
	LoadThrottleAttemptRecords(context.Context) ([]domain.ThrottleAttemptRecord, error)
	CommitThrottleAttemptTransitions(context.Context, []domain.ThrottleAttemptTransition) error
}

// ThrottleWorkerTransport is implemented by the future worker protocol. Command
// IDs are stable, so workers must treat delivery as idempotent.
type ThrottleWorkerTransport interface {
	DeliverThrottleCommands(context.Context, string, []domain.ThrottleCommand) ([]domain.ThrottleAcknowledgement, error)
}

// ThrottleDeliveryReport describes communication attempted during one
// reconciliation cycle.
type ThrottleDeliveryReport struct {
	Commands         []domain.ThrottleCommand
	Acknowledgements []domain.ThrottleAcknowledgement
}

// ReconcileThrottleDeliveries persists pending command intent, delivers
// canonical per-worker batches, then persists every valid acknowledgement.
// Partial responses leave unacknowledged commands pending for replay.
func ReconcileThrottleDeliveries(
	ctx context.Context,
	store ThrottleDeliveryStore,
	transport ThrottleWorkerTransport,
	directives []domain.ThrottleDirective,
	bindings []ThrottleAttemptBinding,
	now time.Time,
) (ThrottleDeliveryReport, error) {
	if store == nil {
		return ThrottleDeliveryReport{}, fmt.Errorf("throttle delivery store is required")
	}
	if transport == nil {
		return ThrottleDeliveryReport{}, fmt.Errorf("throttle worker transport is required")
	}
	previous, err := store.LoadThrottleAttemptRecords(ctx)
	if err != nil {
		return ThrottleDeliveryReport{}, fmt.Errorf("load throttle attempt records: %w", err)
	}
	transitions, commands, err := PlanThrottleDeliveries(previous, directives, bindings, now)
	if err != nil {
		return ThrottleDeliveryReport{}, err
	}
	return reconcilePlannedThrottleCommands(ctx, store, transport, previous, transitions, commands, now)
}

// ReconcileThrottleDeadlineExpirations persists and delivers hard-stop commands
// only after a draining attempt's checkpoint deadline has expired.
func ReconcileThrottleDeadlineExpirations(
	ctx context.Context,
	store ThrottleDeliveryStore,
	transport ThrottleWorkerTransport,
	now time.Time,
) (ThrottleDeliveryReport, error) {
	if store == nil {
		return ThrottleDeliveryReport{}, fmt.Errorf("throttle delivery store is required")
	}
	if transport == nil {
		return ThrottleDeliveryReport{}, fmt.Errorf("throttle worker transport is required")
	}
	previous, err := store.LoadThrottleAttemptRecords(ctx)
	if err != nil {
		return ThrottleDeliveryReport{}, fmt.Errorf("load throttle attempt records: %w", err)
	}
	transitions, commands, err := PlanThrottleDeadlineExpirations(previous, now)
	if err != nil {
		return ThrottleDeliveryReport{}, err
	}
	return reconcilePlannedThrottleCommands(ctx, store, transport, previous, transitions, commands, now)
}

// ReconcileThrottleResumes persists and delivers resume commands for paused
// attempts whose quota pool is open or recovering.
func ReconcileThrottleResumes(
	ctx context.Context,
	store ThrottleDeliveryStore,
	transport ThrottleWorkerTransport,
	admissions []domain.QuotaAdmissionRecord,
	now time.Time,
) (ThrottleDeliveryReport, error) {
	if store == nil {
		return ThrottleDeliveryReport{}, fmt.Errorf("throttle delivery store is required")
	}
	if transport == nil {
		return ThrottleDeliveryReport{}, fmt.Errorf("throttle worker transport is required")
	}
	previous, err := store.LoadThrottleAttemptRecords(ctx)
	if err != nil {
		return ThrottleDeliveryReport{}, fmt.Errorf("load throttle attempt records: %w", err)
	}
	transitions, commands, err := PlanThrottleResumes(previous, admissions, now)
	if err != nil {
		return ThrottleDeliveryReport{}, err
	}
	return reconcilePlannedThrottleCommands(ctx, store, transport, previous, transitions, commands, now)
}

func reconcilePlannedThrottleCommands(
	ctx context.Context,
	store ThrottleDeliveryStore,
	transport ThrottleWorkerTransport,
	previous []domain.ThrottleAttemptRecord,
	transitions []domain.ThrottleAttemptTransition,
	commands []domain.ThrottleCommand,
	now time.Time,
) (ThrottleDeliveryReport, error) {
	if len(transitions) > 0 {
		if err := store.CommitThrottleAttemptTransitions(ctx, transitions); err != nil {
			return ThrottleDeliveryReport{}, fmt.Errorf("commit throttle delivery intent: %w", err)
		}
	}
	report := ThrottleDeliveryReport{Commands: cloneThrottleCommands(commands)}
	if len(commands) == 0 {
		return report, nil
	}

	current := applyThrottleAttemptTransitions(previous, transitions)
	byWorker := make(map[string][]domain.ThrottleCommand)
	for _, command := range commands {
		byWorker[command.WorkerID] = append(byWorker[command.WorkerID], command)
	}
	workers := make([]string, 0, len(byWorker))
	for workerID := range byWorker {
		workers = append(workers, workerID)
	}
	sort.Strings(workers)

	var deliveryErrors []error
	for _, workerID := range workers {
		acks, deliverErr := transport.DeliverThrottleCommands(ctx, workerID, cloneThrottleCommands(byWorker[workerID]))
		report.Acknowledgements = append(report.Acknowledgements, acks...)
		if deliverErr != nil {
			deliveryErrors = append(deliveryErrors, fmt.Errorf("deliver throttle commands to worker %q: %w", workerID, deliverErr))
		}
	}
	sort.Slice(report.Acknowledgements, func(i, j int) bool {
		if report.Acknowledgements[i].CommandID != report.Acknowledgements[j].CommandID {
			return report.Acknowledgements[i].CommandID < report.Acknowledgements[j].CommandID
		}
		return report.Acknowledgements[i].AcknowledgedAt.Before(report.Acknowledgements[j].AcknowledgedAt)
	})
	ackTransitions, err := PlanThrottleAcknowledgements(current, report.Acknowledgements, now)
	if err != nil {
		deliveryErrors = append(deliveryErrors, err)
	} else if len(ackTransitions) > 0 {
		if err := store.CommitThrottleAttemptTransitions(ctx, ackTransitions); err != nil {
			deliveryErrors = append(deliveryErrors, fmt.Errorf("commit throttle acknowledgements: %w", err))
		}
	}
	return report, errors.Join(deliveryErrors...)
}

// PlanThrottleDeliveries deterministically selects running attempts affected by
// committed directives. Persisting its transitions makes command delivery safe
// to retry after a lost response.
func PlanThrottleDeliveries(
	previous []domain.ThrottleAttemptRecord,
	directives []domain.ThrottleDirective,
	bindings []ThrottleAttemptBinding,
	now time.Time,
) ([]domain.ThrottleAttemptTransition, []domain.ThrottleCommand, error) {
	if now.IsZero() {
		return nil, nil, fmt.Errorf("throttle delivery time must be set")
	}
	previousByKey, err := indexThrottleAttemptRecords(previous)
	if err != nil {
		return nil, nil, err
	}
	sortedDirectives := append([]domain.ThrottleDirective(nil), directives...)
	sort.Slice(sortedDirectives, func(i, j int) bool { return sortedDirectives[i].ID < sortedDirectives[j].ID })
	sortedBindings := append([]ThrottleAttemptBinding(nil), bindings...)
	sort.Slice(sortedBindings, func(i, j int) bool { return sortedBindings[i].Attempt.ID < sortedBindings[j].Attempt.ID })

	seenDirectives := make(map[string]struct{}, len(sortedDirectives))
	seenAttempts := make(map[string]struct{}, len(sortedBindings))
	for _, binding := range sortedBindings {
		if err := validateThrottleBinding(binding); err != nil {
			return nil, nil, err
		}
		if _, exists := seenAttempts[binding.Attempt.ID]; exists {
			return nil, nil, fmt.Errorf("throttle bindings repeat attempt %q", binding.Attempt.ID)
		}
		seenAttempts[binding.Attempt.ID] = struct{}{}
	}

	var transitions []domain.ThrottleAttemptTransition
	var commands []domain.ThrottleCommand
	for _, directive := range sortedDirectives {
		kind, err := commandKindForDirective(directive)
		if err != nil {
			return nil, nil, err
		}
		if _, exists := seenDirectives[directive.ID]; exists {
			return nil, nil, fmt.Errorf("throttle deliveries repeat directive %q", directive.ID)
		}
		seenDirectives[directive.ID] = struct{}{}
		for _, binding := range sortedBindings {
			if binding.Assignment.Route.QuotaPoolID != directive.QuotaPoolID ||
				!binding.Attempt.Control.HoldsProviderSlot() {
				continue
			}
			key := throttleAttemptKey(directive.ID, binding.Attempt.ID)
			if existing, ok := previousByKey[key]; ok {
				if existing.Delivery == domain.ThrottleDeliveryPending && existing.Command.Kind == kind {
					expected := throttleCommand(directive, binding, kind, 1, existing.Command.CreatedAt)
					if !reflect.DeepEqual(existing.Command, expected) {
						return nil, nil, fmt.Errorf("throttle command %q execution identity changed", existing.Command.ID)
					}
					commands = append(commands, cloneThrottleCommand(existing.Command))
				}
				continue
			}
			command := throttleCommand(directive, binding, kind, 1, now)
			control := binding.Attempt.Control
			if kind == domain.ThrottleCommandDrain || kind == domain.ThrottleCommandHardStop {
				control = domain.ControlDraining
			}
			record := domain.ThrottleAttemptRecord{
				DirectiveID: directive.ID,
				AttemptID:   binding.Attempt.ID,
				Revision:    1,
				Command:     command,
				Delivery:    domain.ThrottleDeliveryPending,
				Control:     control,
				UpdatedAt:   now,
			}
			transitions = append(transitions, domain.ThrottleAttemptTransition{Record: record})
			commands = append(commands, cloneThrottleCommand(command))
		}
	}
	sortThrottleAttemptTransitions(transitions)
	sortThrottleCommands(commands)
	return transitions, commands, nil
}

// PlanThrottleDeadlineExpirations converts drain commands that did not produce
// a checkpoint by their deadline into durable hard-stop intent.
func PlanThrottleDeadlineExpirations(
	records []domain.ThrottleAttemptRecord,
	now time.Time,
) ([]domain.ThrottleAttemptTransition, []domain.ThrottleCommand, error) {
	if now.IsZero() {
		return nil, nil, fmt.Errorf("throttle deadline time must be set")
	}
	indexed, err := indexThrottleAttemptRecords(records)
	if err != nil {
		return nil, nil, err
	}
	var transitions []domain.ThrottleAttemptTransition
	var commands []domain.ThrottleCommand
	for _, record := range indexed {
		if record.Command.Kind != domain.ThrottleCommandDrain ||
			record.Control != domain.ControlDraining ||
			record.Command.Deadline == nil || now.Before(*record.Command.Deadline) {
			continue
		}
		previousRevision := record.Revision
		record.Revision++
		record.PriorCommandIDs = append(record.PriorCommandIDs, record.Command.ID)
		record.Command = followupThrottleCommand(record.Command, domain.ThrottleCommandHardStop, record.Revision, nil, now)
		record.Delivery = domain.ThrottleDeliveryPending
		record.Result = ""
		record.AcknowledgedAt = nil
		record.Failure = ""
		record.UpdatedAt = now
		transitions = append(transitions, domain.ThrottleAttemptTransition{
			ExpectedRevision: previousRevision,
			Record:           record,
		})
		commands = append(commands, cloneThrottleCommand(record.Command))
	}
	sortThrottleAttemptTransitions(transitions)
	sortThrottleCommands(commands)
	return transitions, commands, nil
}

// PlanThrottleResumes creates replay-safe resume commands for paused attempts
// only after their quota pool is open or recovering. The original placement,
// thread, workspace, and route are retained from the stopped command.
func PlanThrottleResumes(
	records []domain.ThrottleAttemptRecord,
	admissions []domain.QuotaAdmissionRecord,
	now time.Time,
) ([]domain.ThrottleAttemptTransition, []domain.ThrottleCommand, error) {
	if now.IsZero() {
		return nil, nil, fmt.Errorf("throttle resume time must be set")
	}
	indexed, err := indexThrottleAttemptRecords(records)
	if err != nil {
		return nil, nil, err
	}
	admissionByPool := make(map[string]domain.QuotaAdmissionRecord, len(admissions))
	for _, admission := range admissions {
		if admission.QuotaPoolID == "" {
			return nil, nil, fmt.Errorf("quota admission identity is required")
		}
		if _, exists := admissionByPool[admission.QuotaPoolID]; exists {
			return nil, nil, fmt.Errorf("quota admissions repeat pool %q", admission.QuotaPoolID)
		}
		admissionByPool[admission.QuotaPoolID] = admission
	}

	latest := make(map[string]domain.ThrottleAttemptRecord)
	for _, record := range indexed {
		current, exists := latest[record.AttemptID]
		if !exists || record.UpdatedAt.After(current.UpdatedAt) ||
			(record.UpdatedAt.Equal(current.UpdatedAt) && record.DirectiveID > current.DirectiveID) {
			latest[record.AttemptID] = record
		}
	}

	var transitions []domain.ThrottleAttemptTransition
	var commands []domain.ThrottleCommand
	for _, record := range latest {
		if record.Control != domain.ControlPaused && record.Control != domain.ControlPausedUncheckpointed &&
			!(record.Control == domain.ControlResuming && record.Delivery == domain.ThrottleDeliveryPending) {
			continue
		}
		if record.Control == domain.ControlResuming && record.Delivery == domain.ThrottleDeliveryPending {
			commands = append(commands, cloneThrottleCommand(record.Command))
			continue
		}
		admission, exists := admissionByPool[record.Command.QuotaPoolID]
		if !exists || (admission.Admission != domain.AdmissionOpen && admission.Admission != domain.AdmissionRecovering) {
			continue
		}
		previousRevision := record.Revision
		record.Revision++
		record.PriorCommandIDs = append(record.PriorCommandIDs, record.Command.ID)
		reason := fmt.Sprintf("quota pool %s recovered", record.Command.QuotaPoolID)
		if admission.Reason != "" {
			reason += ": " + admission.Reason
		}
		record.Command = followupThrottleCommand(record.Command, domain.ThrottleCommandResume, record.Revision, record.Checkpoint, now)
		record.Command.Reason = reason
		record.Delivery = domain.ThrottleDeliveryPending
		record.Result = ""
		record.Control = domain.ControlResuming
		record.AcknowledgedAt = nil
		record.Failure = ""
		record.UpdatedAt = now
		transitions = append(transitions, domain.ThrottleAttemptTransition{
			ExpectedRevision: previousRevision,
			Record:           record,
		})
		commands = append(commands, cloneThrottleCommand(record.Command))
	}
	sortThrottleAttemptTransitions(transitions)
	sortThrottleCommands(commands)
	return transitions, commands, nil
}

// PlanThrottleAcknowledgements applies valid worker responses. Exact duplicate
// acknowledgements and replay after a command is settled are no-ops.
func PlanThrottleAcknowledgements(
	records []domain.ThrottleAttemptRecord,
	acknowledgements []domain.ThrottleAcknowledgement,
	now time.Time,
) ([]domain.ThrottleAttemptTransition, error) {
	if now.IsZero() {
		return nil, fmt.Errorf("throttle acknowledgement time must be set")
	}
	byCommand := make(map[string]domain.ThrottleAttemptRecord, len(records))
	priorCommands := make(map[string]string)
	for _, record := range records {
		if err := validateThrottleAttemptRecord(record); err != nil {
			return nil, err
		}
		if _, exists := byCommand[record.Command.ID]; exists {
			return nil, fmt.Errorf("throttle attempt records repeat command %q", record.Command.ID)
		}
		byCommand[record.Command.ID] = record
		for _, commandID := range record.PriorCommandIDs {
			if commandID == "" {
				return nil, fmt.Errorf("throttle attempt record %q has an empty prior command", record.Command.ID)
			}
			if priorAttemptID, exists := priorCommands[commandID]; exists && priorAttemptID != record.AttemptID {
				return nil, fmt.Errorf("throttle attempt records repeat prior command %q", commandID)
			}
			priorCommands[commandID] = record.AttemptID
		}
	}
	acks := append([]domain.ThrottleAcknowledgement(nil), acknowledgements...)
	sort.Slice(acks, func(i, j int) bool { return acks[i].CommandID < acks[j].CommandID })
	seen := make(map[string]domain.ThrottleAcknowledgement, len(acks))
	var transitions []domain.ThrottleAttemptTransition
	for _, acknowledgement := range acks {
		if acknowledgement.CommandID == "" || acknowledgement.AttemptID == "" || acknowledgement.AcknowledgedAt.IsZero() {
			return nil, fmt.Errorf("throttle acknowledgement identity and time are required")
		}
		if prior, exists := seen[acknowledgement.CommandID]; exists {
			if reflect.DeepEqual(prior, acknowledgement) {
				continue
			}
			return nil, fmt.Errorf("throttle acknowledgements conflict for command %q", acknowledgement.CommandID)
		}
		seen[acknowledgement.CommandID] = acknowledgement
		record, exists := byCommand[acknowledgement.CommandID]
		if !exists {
			if priorAttemptID, settled := priorCommands[acknowledgement.CommandID]; settled {
				if priorAttemptID != acknowledgement.AttemptID {
					return nil, fmt.Errorf("throttle acknowledgement %q names attempt %q, want %q",
						acknowledgement.CommandID, acknowledgement.AttemptID, priorAttemptID)
				}
				continue
			}
			return nil, fmt.Errorf("throttle acknowledgement names unknown command %q", acknowledgement.CommandID)
		}
		if record.AttemptID != acknowledgement.AttemptID {
			return nil, fmt.Errorf("throttle acknowledgement %q names attempt %q, want %q",
				acknowledgement.CommandID, acknowledgement.AttemptID, record.AttemptID)
		}
		if record.Delivery != domain.ThrottleDeliveryPending {
			continue
		}
		if err := applyThrottleAcknowledgement(&record, acknowledgement); err != nil {
			return nil, err
		}
		record.Revision++
		record.AcknowledgedAt = cloneTime(&acknowledgement.AcknowledgedAt)
		record.UpdatedAt = now
		transitions = append(transitions, domain.ThrottleAttemptTransition{
			ExpectedRevision: record.Revision - 1,
			Record:           record,
		})
	}
	sortThrottleAttemptTransitions(transitions)
	return transitions, nil
}

func applyThrottleAcknowledgement(record *domain.ThrottleAttemptRecord, acknowledgement domain.ThrottleAcknowledgement) error {
	if !acknowledgement.Accepted {
		record.Delivery = domain.ThrottleDeliveryRejected
		record.Result = ""
		record.Failure = acknowledgement.Error
		if record.Command.Kind == domain.ThrottleCommandResume {
			if record.Checkpoint != nil {
				record.Control = domain.ControlPaused
			} else {
				record.Control = domain.ControlPausedUncheckpointed
			}
		}
		return nil
	}
	record.Delivery = domain.ThrottleDeliveryAcknowledged
	record.Result = acknowledgement.Result
	record.Failure = ""
	switch record.Command.Kind {
	case domain.ThrottleCommandWarn:
		if acknowledgement.Result != domain.ThrottleResultWarned {
			return invalidThrottleResult(record.Command, acknowledgement.Result)
		}
	case domain.ThrottleCommandDrain:
		switch acknowledgement.Result {
		case domain.ThrottleResultCheckpointed:
			if err := validateCheckpoint(acknowledgement.Checkpoint); err != nil {
				return fmt.Errorf("checkpoint acknowledgement for command %q: %w", record.Command.ID, err)
			}
			record.Checkpoint = cloneCheckpoint(acknowledgement.Checkpoint)
			record.Control = domain.ControlPaused
		case domain.ThrottleResultCheckpointFailed:
			record.Control = domain.ControlDraining
			record.Failure = acknowledgement.Error
		default:
			return invalidThrottleResult(record.Command, acknowledgement.Result)
		}
	case domain.ThrottleCommandHardStop:
		if acknowledgement.Result != domain.ThrottleResultStopped {
			return invalidThrottleResult(record.Command, acknowledgement.Result)
		}
		record.Checkpoint = nil
		record.Control = domain.ControlPausedUncheckpointed
	case domain.ThrottleCommandResume:
		if acknowledgement.Result != domain.ThrottleResultResumed {
			return invalidThrottleResult(record.Command, acknowledgement.Result)
		}
		record.Control = domain.ControlRunning
	default:
		return fmt.Errorf("throttle command %q has invalid kind %q", record.Command.ID, record.Command.Kind)
	}
	return nil
}

func invalidThrottleResult(command domain.ThrottleCommand, result domain.ThrottleAcknowledgementResult) error {
	return fmt.Errorf("throttle command %q kind %q cannot accept result %q", command.ID, command.Kind, result)
}

func validateCheckpoint(checkpoint *domain.CheckpointMetadata) error {
	if checkpoint == nil || checkpoint.ArtifactID == "" || checkpoint.Path == "" ||
		checkpoint.SHA256 == "" || checkpoint.Size <= 0 || checkpoint.CapturedAt.IsZero() {
		return fmt.Errorf("complete checkpoint metadata is required")
	}
	cleaned := filepath.Clean(checkpoint.Path)
	if filepath.IsAbs(checkpoint.Path) || cleaned == "." || cleaned == ".." ||
		strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return fmt.Errorf("checkpoint path %q must be safe and relative", checkpoint.Path)
	}
	return nil
}

func validateThrottleBinding(binding ThrottleAttemptBinding) error {
	if binding.Attempt.ID == "" || binding.Assignment.ID == "" || binding.Assignment.WorkerID == "" ||
		binding.Assignment.ThreadID == "" || binding.WorkspacePath == "" {
		return fmt.Errorf("throttle binding execution identity must be complete")
	}
	if binding.Attempt.AssignmentID != binding.Assignment.ID ||
		binding.Assignment.AttemptID != binding.Attempt.ID {
		return fmt.Errorf("throttle binding attempt %q does not match assignment %q", binding.Attempt.ID, binding.Assignment.ID)
	}
	if binding.Assignment.Epoch <= 0 || binding.Assignment.Route.QuotaPoolID == "" {
		return fmt.Errorf("throttle binding attempt %q has incomplete route identity", binding.Attempt.ID)
	}
	return nil
}

func commandKindForDirective(directive domain.ThrottleDirective) (domain.ThrottleCommandKind, error) {
	if directive.ID == "" || directive.QuotaPoolID == "" || directive.AdmissionRevision <= 0 {
		return "", fmt.Errorf("throttle directive identity must be complete")
	}
	switch directive.Severity {
	case domain.ThrottleWarn:
		return domain.ThrottleCommandWarn, nil
	case domain.ThrottleDrain:
		return domain.ThrottleCommandDrain, nil
	case domain.ThrottleStop:
		return domain.ThrottleCommandHardStop, nil
	default:
		return "", fmt.Errorf("throttle directive %q has invalid severity %q", directive.ID, directive.Severity)
	}
}

func indexThrottleAttemptRecords(records []domain.ThrottleAttemptRecord) (map[string]domain.ThrottleAttemptRecord, error) {
	result := make(map[string]domain.ThrottleAttemptRecord, len(records))
	for _, record := range records {
		if err := validateThrottleAttemptRecord(record); err != nil {
			return nil, err
		}
		key := throttleAttemptKey(record.DirectiveID, record.AttemptID)
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("throttle attempt records repeat directive %q attempt %q", record.DirectiveID, record.AttemptID)
		}
		result[key] = cloneThrottleAttemptRecord(record)
	}
	return result, nil
}

func validateThrottleAttemptRecord(record domain.ThrottleAttemptRecord) error {
	if record.DirectiveID == "" || record.AttemptID == "" || record.Revision <= 0 ||
		record.Command.ID == "" || record.Command.DirectiveID != record.DirectiveID ||
		record.Command.AttemptID != record.AttemptID || record.Command.Kind == "" ||
		record.Command.WorkerID == "" || record.Command.ThreadID == "" ||
		record.Command.WorkspacePath == "" || record.Command.QuotaPoolID == "" {
		return fmt.Errorf("throttle attempt record identity is invalid")
	}
	switch record.Delivery {
	case domain.ThrottleDeliveryPending, domain.ThrottleDeliveryAcknowledged, domain.ThrottleDeliveryRejected:
		return nil
	default:
		return fmt.Errorf("throttle attempt record %q has invalid delivery state %q", record.Command.ID, record.Delivery)
	}
}

func throttleCommand(
	directive domain.ThrottleDirective,
	binding ThrottleAttemptBinding,
	kind domain.ThrottleCommandKind,
	revision int64,
	now time.Time,
) domain.ThrottleCommand {
	command := domain.ThrottleCommand{
		DirectiveID:     directive.ID,
		AttemptID:       binding.Attempt.ID,
		AssignmentID:    binding.Assignment.ID,
		AssignmentEpoch: binding.Assignment.Epoch,
		WorkerID:        binding.Assignment.WorkerID,
		ThreadID:        binding.Assignment.ThreadID,
		WorkspacePath:   binding.WorkspacePath,
		Route:           cloneProviderRoute(binding.Assignment.Route),
		Kind:            kind,
		QuotaPoolID:     directive.QuotaPoolID,
		BucketEpochs:    cloneDomainBucketEpochs(directive.BucketEpochs),
		Reason:          directive.Reason,
		Deadline:        cloneTime(directive.Deadline),
		CreatedAt:       now,
	}
	command.ID = throttleCommandID(command.DirectiveID, command.AttemptID, kind, revision)
	return command
}

func followupThrottleCommand(
	previous domain.ThrottleCommand,
	kind domain.ThrottleCommandKind,
	revision int64,
	checkpoint *domain.CheckpointMetadata,
	now time.Time,
) domain.ThrottleCommand {
	command := cloneThrottleCommand(previous)
	command.ID = throttleCommandID(command.DirectiveID, command.AttemptID, kind, revision)
	command.Kind = kind
	command.Deadline = nil
	command.Checkpoint = cloneCheckpoint(checkpoint)
	command.CreatedAt = now
	return command
}

func throttleCommandID(directiveID, attemptID string, kind domain.ThrottleCommandKind, revision int64) string {
	hash := sha256.Sum256(fmt.Appendf(nil, "%s\x00%s\x00%s\x00%d", directiveID, attemptID, kind, revision))
	return "throttle-command-" + hex.EncodeToString(hash[:16])
}

func applyThrottleAttemptTransitions(
	records []domain.ThrottleAttemptRecord,
	transitions []domain.ThrottleAttemptTransition,
) []domain.ThrottleAttemptRecord {
	byKey, _ := indexThrottleAttemptRecords(records)
	for _, transition := range transitions {
		byKey[throttleAttemptKey(transition.Record.DirectiveID, transition.Record.AttemptID)] =
			cloneThrottleAttemptRecord(transition.Record)
	}
	result := make([]domain.ThrottleAttemptRecord, 0, len(byKey))
	for _, record := range byKey {
		result = append(result, record)
	}
	sort.Slice(result, func(i, j int) bool {
		return throttleAttemptKey(result[i].DirectiveID, result[i].AttemptID) <
			throttleAttemptKey(result[j].DirectiveID, result[j].AttemptID)
	})
	return result
}

func throttleAttemptKey(directiveID, attemptID string) string {
	return directiveID + "\x00" + attemptID
}

func sortThrottleAttemptTransitions(transitions []domain.ThrottleAttemptTransition) {
	sort.Slice(transitions, func(i, j int) bool {
		return throttleAttemptKey(transitions[i].Record.DirectiveID, transitions[i].Record.AttemptID) <
			throttleAttemptKey(transitions[j].Record.DirectiveID, transitions[j].Record.AttemptID)
	})
}

func sortThrottleCommands(commands []domain.ThrottleCommand) {
	sort.Slice(commands, func(i, j int) bool {
		if commands[i].WorkerID != commands[j].WorkerID {
			return commands[i].WorkerID < commands[j].WorkerID
		}
		return commands[i].ID < commands[j].ID
	})
}

func cloneThrottleAttemptRecord(record domain.ThrottleAttemptRecord) domain.ThrottleAttemptRecord {
	record.Command = cloneThrottleCommand(record.Command)
	record.PriorCommandIDs = append([]string(nil), record.PriorCommandIDs...)
	record.Checkpoint = cloneCheckpoint(record.Checkpoint)
	record.AcknowledgedAt = cloneTime(record.AcknowledgedAt)
	return record
}

func cloneThrottleCommands(commands []domain.ThrottleCommand) []domain.ThrottleCommand {
	result := make([]domain.ThrottleCommand, len(commands))
	for index, command := range commands {
		result[index] = cloneThrottleCommand(command)
	}
	return result
}

func cloneThrottleCommand(command domain.ThrottleCommand) domain.ThrottleCommand {
	command.Route = cloneProviderRoute(command.Route)
	command.BucketEpochs = cloneDomainBucketEpochs(command.BucketEpochs)
	command.Deadline = cloneTime(command.Deadline)
	command.Checkpoint = cloneCheckpoint(command.Checkpoint)
	return command
}

func cloneCheckpoint(checkpoint *domain.CheckpointMetadata) *domain.CheckpointMetadata {
	if checkpoint == nil {
		return nil
	}
	cloned := *checkpoint
	return &cloned
}
