package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ErrStaleAdminSafetyFence means the durable start/resume safety inputs
// changed after policy planning and the still-pending command must be replanned.
var ErrStaleAdminSafetyFence = errors.New("stale admin safety fence")

// ApplyAdminCommand atomically fences the pending command and target revision,
// persists its state transition, and records the terminal audit event.
func (s *Store) ApplyAdminCommand(ctx context.Context, application domain.AdminCommandApplication) (domain.AdminCommandDecision, error) {
	if application.CommandID == "" || application.ExpectedCommandState != domain.AdminCommandPending ||
		(application.State != domain.AdminCommandApplied && application.State != domain.AdminCommandRejected && application.State != domain.AdminCommandFailed) ||
		application.AppliedAt.IsZero() {
		return domain.AdminCommandDecision{}, ErrInvalidAdminCommandOutcome
	}
	if application.State == domain.AdminCommandApplied && application.Failure != "" {
		return domain.AdminCommandDecision{}, ErrInvalidAdminCommandOutcome
	}
	if application.State != domain.AdminCommandApplied && application.Failure == "" {
		return domain.AdminCommandDecision{}, ErrInvalidAdminCommandOutcome
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.AdminCommandDecision{}, fmt.Errorf("begin admin command application: %w", err)
	}
	defer tx.Rollback()

	command, found, err := loadAdminCommandTx(ctx, tx, application.CommandID)
	if err != nil {
		return domain.AdminCommandDecision{}, err
	}
	if !found {
		return domain.AdminCommandDecision{}, fmt.Errorf("%w: command %q", ErrAdminTargetNotFound, application.CommandID)
	}
	eventID := adminOutcomeEventID(command.ID)
	if command.State != application.ExpectedCommandState {
		event, ok, eventErr := loadAuditEventTx(ctx, tx, eventID)
		if eventErr != nil {
			return domain.AdminCommandDecision{}, eventErr
		}
		if ok && command.State == application.State && command.Failure == application.Failure {
			if err := tx.Commit(); err != nil {
				return domain.AdminCommandDecision{}, fmt.Errorf("commit admin application replay: %w", err)
			}
			return domain.AdminCommandDecision{Command: command, Event: event}, nil
		}
		return domain.AdminCommandDecision{}, fmt.Errorf("%w: command %q is %q", ErrAdminCommandStateConflict, command.ID, command.State)
	}

	if application.State == domain.AdminCommandApplied &&
		(command.Kind == domain.AdminCommandPause) != (application.PauseIntent != nil) {
		return domain.AdminCommandDecision{}, fmt.Errorf("%w: pause intent does not match command kind", ErrInvalidAdminCommandOutcome)
	}
	if application.State == domain.AdminCommandApplied &&
		(command.Kind == domain.AdminCommandStart || command.Kind == domain.AdminCommandResume || command.Kind == domain.AdminCommandPause) {
		if application.SafetyFingerprint == "" {
			return domain.AdminCommandDecision{}, fmt.Errorf("%w: start/resume/pause application has no safety fingerprint", ErrInvalidAdminCommandOutcome)
		}
		if (command.Kind == domain.AdminCommandStart || command.Kind == domain.AdminCommandResume) &&
			application.SafetyValidUntil == nil {
			return domain.AdminCommandDecision{}, fmt.Errorf("%w: start/resume application has no safety validity", ErrInvalidAdminCommandOutcome)
		}
		currentFingerprint, fingerprintErr := loadAdminSafetyFingerprintTx(ctx, tx)
		if fingerprintErr != nil {
			return domain.AdminCommandDecision{}, fingerprintErr
		}
		if currentFingerprint != application.SafetyFingerprint {
			return domain.AdminCommandDecision{}, fmt.Errorf("%w: command %q safety inputs changed", ErrStaleAdminSafetyFence, command.ID)
		}
		if application.SafetyValidUntil != nil {
			workers, workersErr := loadAdminWorkerSnapshotsTx(ctx, tx)
			if workersErr != nil {
				return domain.AdminCommandDecision{}, workersErr
			}
			expectedValidUntil, ok := domain.AdminWorkerSafetyValidUntil(workers, application.AppliedAt.UTC())
			if !ok || !application.SafetyValidUntil.Equal(expectedValidUntil) {
				return domain.AdminCommandDecision{}, fmt.Errorf("%w: start/resume worker safety validity is inconsistent", ErrInvalidAdminCommandOutcome)
			}
			if s.now().UTC().After(expectedValidUntil.UTC()) {
				return domain.AdminCommandDecision{}, fmt.Errorf("%w: command %q worker safety observation expired", ErrStaleAdminSafetyFence, command.ID)
			}
		}
	}

	target, contextFields, err := loadAdminTargetTx(ctx, tx, command.TargetType, command.TargetID)
	if err != nil {
		return domain.AdminCommandDecision{}, err
	}
	if application.State == domain.AdminCommandApplied && command.TargetType == domain.AdminTargetAttempt {
		snapshot, loadErr := loadWorkflowProjectionTx(ctx, tx, contextFields.WorkflowRunID)
		if loadErr != nil {
			return domain.AdminCommandDecision{}, loadErr
		}
		if snapshot.Run.Sink != nil && snapshot.Run.Sink.Progress.Terminal() {
			application.State = domain.AdminCommandRejected
			application.Failure = "run sink is final; submit a new workflow"
			application.Attempt, application.RelatedAttempts, application.NewAttempt, application.WorkflowRun = nil, nil, nil, nil
		}
	}
	currentTarget := (*domain.AdminTargetSnapshot)(nil)
	expectedTarget := application.ExpectedTargetRevision
	if expectedTarget != command.ExpectedRevision || target.Revision != expectedTarget {
		currentTarget = &target
		application.State = domain.AdminCommandRejected
		application.Failure = fmt.Sprintf("stale execution revision: expected %d, current %d", expectedTarget, target.Revision)
		application.Attempt, application.RelatedAttempts, application.NewAttempt, application.WorkflowRun, application.Schedule = nil, nil, nil, nil, nil
	}

	if application.State == domain.AdminCommandApplied {
		switch command.TargetType {
		case domain.AdminTargetAttempt:
			if application.Attempt == nil || application.Attempt.ID != command.TargetID ||
				application.Attempt.Revision != expectedTarget+1 {
				return domain.AdminCommandDecision{}, errors.New("applied attempt command has an invalid target transition")
			}
			if err := updateAdminAttemptTx(ctx, tx, *application.Attempt, expectedTarget); err != nil {
				return domain.AdminCommandDecision{}, err
			}
			if application.PauseIntent != nil {
				if err := insertAdminPauseIntentTx(ctx, tx, command, *application.Attempt, *application.PauseIntent); err != nil {
					return domain.AdminCommandDecision{}, err
				}
			}
			for _, related := range application.RelatedAttempts {
				if related.ID == command.TargetID || related.Revision < 1 {
					return domain.AdminCommandDecision{}, errors.New("applied attempt command has an invalid related transition")
				}
				if err := updateAdminAttemptTx(ctx, tx, related, related.Revision-1); err != nil {
					return domain.AdminCommandDecision{}, err
				}
			}
			if application.NewAttempt != nil {
				if err := insertAdminAttemptTx(ctx, tx, *application.NewAttempt); err != nil {
					return domain.AdminCommandDecision{}, err
				}
			}
			if application.WorkflowRun != nil {
				if err := updateAdminWorkflowRunTx(ctx, tx, *application.WorkflowRun); err != nil {
					return domain.AdminCommandDecision{}, err
				}
			}
		case domain.AdminTargetSchedule:
			if command.Kind == domain.AdminCommandScheduleRun {
				if application.ScheduleTrigger == nil || application.ScheduleTrigger.ScheduleID != command.TargetID {
					return domain.AdminCommandDecision{}, errors.New("applied manual schedule command has an invalid trigger")
				}
				if _, err := commitScheduleTriggerTx(ctx, tx, *application.ScheduleTrigger); err != nil {
					if errors.Is(err, ErrManualScheduleRunOpen) || errors.Is(err, ErrScheduleFailureHeld) {
						application.State = domain.AdminCommandRejected
						application.Failure = err.Error()
						application.ScheduleTrigger = nil
					} else {
						return domain.AdminCommandDecision{}, err
					}
				}
			} else {
				if application.Schedule == nil || application.Schedule.ID != command.TargetID ||
					application.Schedule.Revision != expectedTarget+1 {
					return domain.AdminCommandDecision{}, errors.New("applied schedule command has an invalid target transition")
				}
				if err := updateAdminScheduleTx(ctx, tx, *application.Schedule, expectedTarget); err != nil {
					return domain.AdminCommandDecision{}, err
				}
			}
		default:
			return domain.AdminCommandDecision{}, errors.New("unsupported admin target transition")
		}
	}

	command.State, command.Failure = application.State, application.Failure
	appliedAt := application.AppliedAt.UTC()
	command.AppliedAt = &appliedAt
	raw, err := json.Marshal(command)
	if err != nil {
		return domain.AdminCommandDecision{}, fmt.Errorf("encode applied admin command %q: %w", command.ID, err)
	}
	result, err := tx.ExecContext(ctx,
		"UPDATE coordinator_admin_commands SET state = ?, record = ? WHERE id = ? AND state = ?",
		command.State, raw, command.ID, application.ExpectedCommandState)
	if err != nil {
		return domain.AdminCommandDecision{}, fmt.Errorf("update applied admin command %q: %w", command.ID, err)
	}
	updated, err := result.RowsAffected()
	if err != nil || updated != 1 {
		return domain.AdminCommandDecision{}, fmt.Errorf("%w: command %q changed concurrently", ErrAdminCommandStateConflict, command.ID)
	}
	detail, err := json.Marshal(adminEventDetail{
		CommandKind: command.Kind, State: command.State, ExpectedRevision: command.ExpectedRevision,
		CurrentTarget: currentTarget, Failure: command.Failure,
	})
	if err != nil {
		return domain.AdminCommandDecision{}, fmt.Errorf("encode admin application audit detail: %w", err)
	}
	event := domain.AuditEvent{
		ID: eventID, Kind: "admin-command-" + string(command.State),
		WorkflowRunID: contextFields.WorkflowRunID, TaskID: contextFields.TaskID,
		AttemptID: contextFields.AttemptID, TargetType: command.TargetType, TargetID: command.TargetID,
		Actor: "coordinator", Reason: command.Reason, Detail: detail, CreatedAt: appliedAt,
	}
	event, err = insertAuditEventTx(ctx, tx, event)
	if err != nil {
		return domain.AdminCommandDecision{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.AdminCommandDecision{}, fmt.Errorf("commit admin command application: %w", err)
	}
	return domain.AdminCommandDecision{Command: command, Event: event, CurrentTarget: currentTarget}, nil
}

func loadAdminSafetyFingerprintTx(ctx context.Context, tx *sql.Tx) (string, error) {
	var state domain.AdminSafetyState
	var err error
	if state.Tasks, err = loadJSON[domain.Task](ctx, tx, "coordinator_tasks"); err != nil {
		return "", err
	}
	if state.Attempts, err = loadJSON[domain.Attempt](ctx, tx, "coordinator_attempts"); err != nil {
		return "", err
	}
	if state.Assignments, err = loadJSON[domain.Assignment](ctx, tx, "coordinator_assignments"); err != nil {
		return "", err
	}
	if state.QuotaPools, err = loadJSON[domain.QuotaPool](ctx, tx, "coordinator_quota_pools"); err != nil {
		return "", err
	}
	if state.Workers, err = loadAdminWorkerSnapshotsTx(ctx, tx); err != nil {
		return "", err
	}
	if state.QuotaAdmissions, err = loadJSON[domain.QuotaAdmissionRecord](ctx, tx, "coordinator_quota_admissions"); err != nil {
		return "", err
	}
	runs, err := loadJSON[domain.WorkflowRun](ctx, tx, "coordinator_workflow_runs")
	if err != nil {
		return "", err
	}
	state.Tasks = domain.TasksWithGraphAdditions(runs, state.Tasks)
	fingerprint, err := domain.AdminSafetyFingerprint(state)
	if err != nil {
		return "", fmt.Errorf("fingerprint current admin safety state: %w", err)
	}
	return fingerprint, nil
}

func loadAdminWorkerSnapshotsTx(ctx context.Context, tx *sql.Tx) ([]domain.WorkerSnapshot, error) {
	rows, err := tx.QueryContext(ctx, "SELECT record FROM coordinator_worker_snapshots ORDER BY worker_id")
	if err != nil {
		return nil, fmt.Errorf("load coordinator_worker_snapshots: %w", err)
	}
	defer rows.Close()
	var snapshots []domain.WorkerSnapshot
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan coordinator_worker_snapshots: %w", err)
		}
		var snapshot domain.WorkerSnapshot
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			return nil, fmt.Errorf("decode coordinator_worker_snapshots: %w", err)
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate coordinator_worker_snapshots: %w", err)
	}
	return snapshots, nil
}

func insertAdminPauseIntentTx(
	ctx context.Context,
	tx *sql.Tx,
	adminCommand domain.AdminCommand,
	attempt domain.Attempt,
	transition domain.ThrottleAttemptTransition,
) error {
	record := transition.Record
	command := record.Command
	if adminCommand.Kind != domain.AdminCommandPause || attempt.Control != domain.ControlDraining ||
		transition.ExpectedRevision != 0 || record.Revision != 1 ||
		record.DirectiveID == "" || record.AttemptID != attempt.ID ||
		command.ID == "" || command.DirectiveID != record.DirectiveID ||
		command.AttemptID != attempt.ID || command.AssignmentID != attempt.AssignmentID ||
		command.WorkerID == "" || command.ThreadID == "" || command.WorkspacePath == "" ||
		command.QuotaPoolID == "" || record.Delivery != domain.ThrottleDeliveryPending ||
		record.Control != domain.ControlDraining ||
		(command.Kind != domain.ThrottleCommandDrain && command.Kind != domain.ThrottleCommandHardStop) {
		return fmt.Errorf("%w: pause delivery intent is invalid", ErrInvalidAdminCommandOutcome)
	}
	var assignmentRaw []byte
	if err := tx.QueryRowContext(ctx,
		"SELECT record FROM coordinator_assignments WHERE id = ?", attempt.AssignmentID,
	).Scan(&assignmentRaw); err != nil {
		return fmt.Errorf("load pause assignment %q: %w", attempt.AssignmentID, err)
	}
	var assignment domain.Assignment
	if err := json.Unmarshal(assignmentRaw, &assignment); err != nil {
		return fmt.Errorf("decode pause assignment %q: %w", attempt.AssignmentID, err)
	}
	if assignment.AttemptID != attempt.ID || assignment.WorkerID != command.WorkerID ||
		assignment.Epoch != command.AssignmentEpoch || assignment.ThreadID != command.ThreadID ||
		assignment.WorkerEpoch == "" || assignment.Route.QuotaPoolID != command.QuotaPoolID ||
		!reflect.DeepEqual(assignment.Route, command.Route) {
		return fmt.Errorf("%w: pause delivery identity changed", ErrInvalidAdminCommandOutcome)
	}
	snapshots, err := loadAdminWorkerSnapshotsTx(ctx, tx)
	if err != nil {
		return err
	}
	observed := false
	for _, snapshot := range snapshots {
		if snapshot.WorkerID != assignment.WorkerID || snapshot.WorkerEpoch != assignment.WorkerEpoch {
			continue
		}
		for _, observation := range snapshot.Assignments {
			if observation.AssignmentID == assignment.ID &&
				observation.AssignmentEpoch == assignment.Epoch &&
				observation.ThreadID == assignment.ThreadID &&
				observation.WorkspacePath == command.WorkspacePath {
				observed = true
				break
			}
		}
	}
	if !observed {
		return fmt.Errorf("%w: pause worker observation identity changed", ErrInvalidAdminCommandOutcome)
	}
	replayed, err := compareThrottleAttemptTransition(ctx, tx, transition)
	if err != nil {
		return err
	}
	if replayed {
		return nil
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode admin pause intent %q/%q: %w", record.DirectiveID, record.AttemptID, err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO coordinator_throttle_attempts(directive_id, attempt_id, revision, delivery, record)
		 VALUES (?, ?, ?, ?, ?)`,
		record.DirectiveID, record.AttemptID, record.Revision, record.Delivery, raw,
	); err != nil {
		return fmt.Errorf("save admin pause intent %q/%q: %w", record.DirectiveID, record.AttemptID, err)
	}
	return nil
}

func updateAdminAttemptTx(ctx context.Context, tx *sql.Tx, attempt domain.Attempt, expectedRevision int64) error {
	raw, err := json.Marshal(attempt)
	if err != nil {
		return fmt.Errorf("encode attempt %q: %w", attempt.ID, err)
	}
	result, err := tx.ExecContext(ctx,
		"UPDATE coordinator_attempts SET revision = ?, record = ? WHERE id = ? AND revision = ?",
		attempt.Revision, raw, attempt.ID, expectedRevision)
	if err != nil {
		return fmt.Errorf("update attempt %q: %w", attempt.ID, err)
	}
	updated, err := result.RowsAffected()
	if err != nil || updated != 1 {
		return fmt.Errorf("stale attempt %q revision", attempt.ID)
	}
	return nil
}

func insertAdminAttemptTx(ctx context.Context, tx *sql.Tx, attempt domain.Attempt) error {
	raw, err := json.Marshal(attempt)
	if err != nil {
		return fmt.Errorf("encode retry attempt %q: %w", attempt.ID, err)
	}
	_, err = tx.ExecContext(ctx,
		"INSERT INTO coordinator_attempts(id, workflow_run_id, task_id, number, revision, record) VALUES (?, ?, ?, ?, ?, ?)",
		attempt.ID, attempt.WorkflowRunID, attempt.TaskID, attempt.Number, attempt.Revision, raw)
	if err != nil {
		return fmt.Errorf("insert retry attempt %q: %w", attempt.ID, err)
	}
	return nil
}

func updateAdminWorkflowRunTx(ctx context.Context, tx *sql.Tx, run domain.WorkflowRun) error {
	raw, err := json.Marshal(run)
	if err != nil {
		return fmt.Errorf("encode workflow run %q: %w", run.ID, err)
	}
	result, err := tx.ExecContext(ctx,
		"UPDATE coordinator_workflow_runs SET progress = ?, revision = ?, record = ? WHERE id = ? AND revision = ?",
		run.Progress, run.Revision, raw, run.ID, run.Revision-1)
	if err != nil {
		return fmt.Errorf("update workflow run %q: %w", run.ID, err)
	}
	updated, err := result.RowsAffected()
	if err != nil || updated != 1 {
		return fmt.Errorf("stale workflow run %q revision", run.ID)
	}
	return nil
}

func updateAdminScheduleTx(ctx context.Context, tx *sql.Tx, schedule domain.Schedule, expectedRevision int64) error {
	raw, err := json.Marshal(schedule)
	if err != nil {
		return fmt.Errorf("encode schedule %q: %w", schedule.ID, err)
	}
	result, err := tx.ExecContext(ctx,
		"UPDATE coordinator_schedules SET active_run_id = ?, revision = ?, current_version = ?, record = ? WHERE id = ? AND revision = ?",
		schedule.ActiveRunID, schedule.Revision, schedule.Version, raw, schedule.ID, expectedRevision)
	if err != nil {
		return fmt.Errorf("update schedule %q: %w", schedule.ID, err)
	}
	updated, err := result.RowsAffected()
	if err != nil || updated != 1 {
		return fmt.Errorf("stale schedule %q revision", schedule.ID)
	}
	return nil
}
