package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const coordinatorMigrationV10 = `
CREATE TABLE coordinator_audit_events (
	sequence INTEGER PRIMARY KEY AUTOINCREMENT,
	id TEXT NOT NULL UNIQUE,
	kind TEXT NOT NULL,
	workflow_run_id TEXT NOT NULL,
	task_id TEXT NOT NULL,
	attempt_id TEXT NOT NULL,
	target_type TEXT NOT NULL,
	target_id TEXT NOT NULL,
	created_at TEXT NOT NULL,
	record TEXT NOT NULL
);
CREATE INDEX coordinator_audit_events_run_sequence
	ON coordinator_audit_events(workflow_run_id, sequence);
CREATE INDEX coordinator_audit_events_target_sequence
	ON coordinator_audit_events(target_type, target_id, sequence);

-- Admin commands predate the audit-event table. Backfill enough immutable
-- history to preserve command replay and outcome replay across the migration.
INSERT INTO coordinator_audit_events(
	id, kind, workflow_run_id, task_id, attempt_id,
	target_type, target_id, created_at, record
)
SELECT
	'admin-command:' || command.id || ':submission',
	CASE WHEN command.state = 'rejected' THEN 'admin-command-rejected'
		ELSE 'admin-command-submitted' END,
	COALESCE(json_extract(attempt.record, '$.workflowRunId'), ''),
	COALESCE(json_extract(attempt.record, '$.taskId'), ''),
	CASE WHEN command.target_type = 'attempt' THEN command.target_id ELSE '' END,
	command.target_type,
	command.target_id,
	json_extract(command.record, '$.createdAt'),
	json_object(
		'id', 'admin-command:' || command.id || ':submission',
		'sequence', 0,
		'kind', CASE WHEN command.state = 'rejected' THEN 'admin-command-rejected'
			ELSE 'admin-command-submitted' END,
		'workflowRunId', COALESCE(json_extract(attempt.record, '$.workflowRunId'), ''),
		'taskId', COALESCE(json_extract(attempt.record, '$.taskId'), ''),
		'attemptId', CASE WHEN command.target_type = 'attempt' THEN command.target_id ELSE '' END,
		'targetType', command.target_type,
		'targetId', command.target_id,
		'actor', COALESCE(json_extract(command.record, '$.requestedBy'), ''),
		'reason', COALESCE(json_extract(command.record, '$.reason'), ''),
		'detail', json_object(
			'commandKind', json_extract(command.record, '$.kind'),
			'state', command.state,
			'expectedRevision', json_extract(command.record, '$.expectedRevision'),
			'failure', COALESCE(json_extract(command.record, '$.failure'), '')
		),
		'createdAt', json_extract(command.record, '$.createdAt')
	)
FROM coordinator_admin_commands AS command
LEFT JOIN coordinator_attempts AS attempt
	ON command.target_type = 'attempt' AND attempt.id = command.target_id;

INSERT INTO coordinator_audit_events(
	id, kind, workflow_run_id, task_id, attempt_id,
	target_type, target_id, created_at, record
)
SELECT
	'admin-command:' || command.id || ':outcome',
	'admin-command-' || command.state,
	COALESCE(json_extract(attempt.record, '$.workflowRunId'), ''),
	COALESCE(json_extract(attempt.record, '$.taskId'), ''),
	CASE WHEN command.target_type = 'attempt' THEN command.target_id ELSE '' END,
	command.target_type,
	command.target_id,
	json_extract(command.record, '$.appliedAt'),
	json_object(
		'id', 'admin-command:' || command.id || ':outcome',
		'sequence', 0,
		'kind', 'admin-command-' || command.state,
		'workflowRunId', COALESCE(json_extract(attempt.record, '$.workflowRunId'), ''),
		'taskId', COALESCE(json_extract(attempt.record, '$.taskId'), ''),
		'attemptId', CASE WHEN command.target_type = 'attempt' THEN command.target_id ELSE '' END,
		'targetType', command.target_type,
		'targetId', command.target_id,
		'actor', 'coordinator',
		'reason', COALESCE(json_extract(command.record, '$.reason'), ''),
		'detail', json_object(
			'commandKind', json_extract(command.record, '$.kind'),
			'state', command.state,
			'expectedRevision', json_extract(command.record, '$.expectedRevision'),
			'failure', COALESCE(json_extract(command.record, '$.failure'), '')
		),
		'createdAt', json_extract(command.record, '$.appliedAt')
	)
FROM coordinator_admin_commands AS command
LEFT JOIN coordinator_attempts AS attempt
	ON command.target_type = 'attempt' AND attempt.id = command.target_id
WHERE command.state != 'pending' AND json_extract(command.record, '$.appliedAt') IS NOT NULL;
`

var (
	ErrAdminCommandConflict       = errors.New("admin command replay conflicts with immutable request")
	ErrAdminCommandStateConflict  = errors.New("admin command state conflict")
	ErrAdminTargetNotFound        = errors.New("admin command target not found")
	ErrInvalidAdminCommand        = errors.New("invalid admin command")
	ErrInvalidAdminCommandOutcome = errors.New("invalid admin command outcome")
)

type adminEventDetail struct {
	CommandKind      domain.AdminCommandKind     `json:"commandKind"`
	State            domain.AdminCommandState    `json:"state"`
	ExpectedRevision int64                       `json:"expectedRevision"`
	CurrentTarget    *domain.AdminTargetSnapshot `json:"currentTarget,omitempty"`
	Failure          string                      `json:"failure,omitempty"`
}

func (s *Store) SubmitAdminCommand(ctx context.Context, command domain.AdminCommand) (domain.AdminCommandDecision, error) {
	if err := validateAdminCommand(command); err != nil {
		return domain.AdminCommandDecision{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.AdminCommandDecision{}, fmt.Errorf("begin admin command submission: %w", err)
	}
	defer tx.Rollback()

	existing, found, err := loadAdminCommandTx(ctx, tx, command.ID)
	if err != nil {
		return domain.AdminCommandDecision{}, err
	}
	eventID := adminSubmissionEventID(command.ID)
	if found {
		if !sameAdminCommandRequest(existing, command) {
			return domain.AdminCommandDecision{}, fmt.Errorf("%w: command %q", ErrAdminCommandConflict, command.ID)
		}
		event, ok, err := loadAuditEventTx(ctx, tx, eventID)
		if err != nil {
			return domain.AdminCommandDecision{}, err
		}
		if !ok {
			return domain.AdminCommandDecision{}, fmt.Errorf("admin command %q is missing submission audit event", command.ID)
		}
		decision, err := adminDecision(existing, event)
		if err != nil {
			return domain.AdminCommandDecision{}, err
		}
		if err := tx.Commit(); err != nil {
			return domain.AdminCommandDecision{}, fmt.Errorf("commit admin command replay: %w", err)
		}
		return decision, nil
	}

	target, contextFields, err := loadAdminTargetTx(ctx, tx, command.TargetType, command.TargetID)
	if err != nil {
		return domain.AdminCommandDecision{}, err
	}
	var currentTarget *domain.AdminTargetSnapshot
	if target.Revision != command.ExpectedRevision {
		currentTarget = &target
		command.State = domain.AdminCommandRejected
		command.Failure = fmt.Sprintf("stale revision: expected %d, current %d", command.ExpectedRevision, target.Revision)
		appliedAt := command.CreatedAt
		command.AppliedAt = &appliedAt
	}
	if err := insertAdminCommandTx(ctx, tx, command); err != nil {
		return domain.AdminCommandDecision{}, err
	}
	eventKind := "admin-command-submitted"
	if command.State == domain.AdminCommandRejected {
		eventKind = "admin-command-rejected"
	}
	detail, err := json.Marshal(adminEventDetail{
		CommandKind: command.Kind, State: command.State, ExpectedRevision: command.ExpectedRevision,
		CurrentTarget: currentTarget, Failure: command.Failure,
	})
	if err != nil {
		return domain.AdminCommandDecision{}, fmt.Errorf("encode admin audit detail: %w", err)
	}
	event := domain.AuditEvent{
		ID: eventID, Kind: eventKind, WorkflowRunID: contextFields.WorkflowRunID,
		TaskID: contextFields.TaskID, AttemptID: contextFields.AttemptID,
		TargetType: command.TargetType, TargetID: command.TargetID,
		Actor: command.RequestedBy, Reason: command.Reason, Detail: detail, CreatedAt: command.CreatedAt,
	}
	event, err = insertAuditEventTx(ctx, tx, event)
	if err != nil {
		return domain.AdminCommandDecision{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.AdminCommandDecision{}, fmt.Errorf("commit admin command submission: %w", err)
	}
	return domain.AdminCommandDecision{Command: command, Event: event, CurrentTarget: currentTarget}, nil
}

func (s *Store) CompleteAdminCommand(ctx context.Context, outcome domain.AdminCommandOutcome) (domain.AdminCommandDecision, error) {
	if err := validateAdminCommandOutcome(outcome); err != nil {
		return domain.AdminCommandDecision{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.AdminCommandDecision{}, fmt.Errorf("begin admin command outcome: %w", err)
	}
	defer tx.Rollback()

	command, found, err := loadAdminCommandTx(ctx, tx, outcome.CommandID)
	if err != nil {
		return domain.AdminCommandDecision{}, err
	}
	if !found {
		return domain.AdminCommandDecision{}, fmt.Errorf("%w: command %q", ErrAdminTargetNotFound, outcome.CommandID)
	}
	eventID := adminOutcomeEventID(command.ID)
	if command.State != outcome.ExpectedState {
		if sameAdminCommandOutcome(command, outcome) {
			event, ok, err := loadAuditEventTx(ctx, tx, eventID)
			if err != nil {
				return domain.AdminCommandDecision{}, err
			}
			if !ok {
				return domain.AdminCommandDecision{}, fmt.Errorf("completed admin command %q is missing outcome audit event", command.ID)
			}
			if err := tx.Commit(); err != nil {
				return domain.AdminCommandDecision{}, fmt.Errorf("commit admin outcome replay: %w", err)
			}
			return domain.AdminCommandDecision{Command: command, Event: event}, nil
		}
		return domain.AdminCommandDecision{}, fmt.Errorf(
			"%w: command %q expected %q, current %q",
			ErrAdminCommandStateConflict, command.ID, outcome.ExpectedState, command.State,
		)
	}

	command.State = outcome.State
	command.Failure = outcome.Failure
	appliedAt := outcome.AppliedAt.UTC()
	command.AppliedAt = &appliedAt
	raw, err := json.Marshal(command)
	if err != nil {
		return domain.AdminCommandDecision{}, fmt.Errorf("encode admin command outcome %q: %w", command.ID, err)
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE coordinator_admin_commands SET state = ?, record = ? WHERE id = ? AND state = ?`,
		command.State, raw, command.ID, outcome.ExpectedState,
	)
	if err != nil {
		return domain.AdminCommandDecision{}, fmt.Errorf("update admin command %q: %w", command.ID, err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return domain.AdminCommandDecision{}, fmt.Errorf("inspect admin command %q outcome: %w", command.ID, err)
	}
	if updated != 1 {
		return domain.AdminCommandDecision{}, fmt.Errorf("%w: command %q changed concurrently", ErrAdminCommandStateConflict, command.ID)
	}

	_, contextFields, err := loadAdminTargetTx(ctx, tx, command.TargetType, command.TargetID)
	if err != nil {
		return domain.AdminCommandDecision{}, err
	}
	detail, err := json.Marshal(adminEventDetail{
		CommandKind: command.Kind, State: command.State, ExpectedRevision: command.ExpectedRevision,
		Failure: command.Failure,
	})
	if err != nil {
		return domain.AdminCommandDecision{}, fmt.Errorf("encode admin outcome audit detail: %w", err)
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
		return domain.AdminCommandDecision{}, fmt.Errorf("commit admin command outcome: %w", err)
	}
	return domain.AdminCommandDecision{Command: command, Event: event}, nil
}

func (s *Store) LoadAuditEvents(ctx context.Context, workflowRunID string) ([]domain.AuditEvent, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin coordinator audit event load: %w", err)
	}
	defer tx.Rollback()
	events, err := loadAuditEventsTx(ctx, tx, workflowRunID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit coordinator audit event load: %w", err)
	}
	return events, nil
}

func loadAuditEventsTx(ctx context.Context, tx *sql.Tx, workflowRunID string) ([]domain.AuditEvent, error) {
	query := `SELECT sequence, record FROM coordinator_audit_events`
	args := make([]any, 0, 1)
	if workflowRunID != "" {
		query += ` WHERE workflow_run_id = ?`
		args = append(args, workflowRunID)
	}
	query += ` ORDER BY sequence`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("load coordinator audit events: %w", err)
	}
	defer rows.Close()

	var events []domain.AuditEvent
	for rows.Next() {
		var sequence int64
		var raw []byte
		if err := rows.Scan(&sequence, &raw); err != nil {
			return nil, fmt.Errorf("scan coordinator audit event: %w", err)
		}
		var event domain.AuditEvent
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil, fmt.Errorf("decode coordinator audit event: %w", err)
		}
		event.Sequence = sequence
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate coordinator audit events: %w", err)
	}
	return events, nil
}

type adminAuditContext struct {
	WorkflowRunID string
	TaskID        string
	AttemptID     string
}

func loadAdminTargetTx(
	ctx context.Context,
	tx *sql.Tx,
	targetType domain.AdminTargetType,
	targetID string,
) (domain.AdminTargetSnapshot, adminAuditContext, error) {
	var table string
	switch targetType {
	case domain.AdminTargetAttempt:
		table = "coordinator_attempts"
	case domain.AdminTargetWorkflowRun:
		table = "coordinator_workflow_runs"
	case domain.AdminTargetSchedule:
		table = "coordinator_schedules"
	default:
		return domain.AdminTargetSnapshot{}, adminAuditContext{}, fmt.Errorf("%w: target type %q", ErrInvalidAdminCommand, targetType)
	}
	var revision int64
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT revision, record FROM "+table+" WHERE id = ?", targetID).Scan(&revision, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.AdminTargetSnapshot{}, adminAuditContext{}, fmt.Errorf("%w: %s %q", ErrAdminTargetNotFound, targetType, targetID)
	}
	if err != nil {
		return domain.AdminTargetSnapshot{}, adminAuditContext{}, fmt.Errorf("load admin target %s %q: %w", targetType, targetID, err)
	}
	contextFields := adminAuditContext{}
	switch targetType {
	case domain.AdminTargetAttempt:
		var attempt domain.Attempt
		if err := json.Unmarshal(raw, &attempt); err != nil {
			return domain.AdminTargetSnapshot{}, adminAuditContext{}, fmt.Errorf("decode admin target attempt %q: %w", targetID, err)
		}
		contextFields = adminAuditContext{WorkflowRunID: attempt.WorkflowRunID, TaskID: attempt.TaskID, AttemptID: attempt.ID}
	case domain.AdminTargetWorkflowRun:
		contextFields.WorkflowRunID = targetID
	}
	return domain.AdminTargetSnapshot{
		Type: targetType, ID: targetID, Revision: revision, Record: append(json.RawMessage(nil), raw...),
	}, contextFields, nil
}

func validateAdminCommand(command domain.AdminCommand) error {
	if strings.TrimSpace(command.ID) != command.ID || command.ID == "" ||
		strings.TrimSpace(string(command.Kind)) != string(command.Kind) || command.Kind == "" ||
		strings.TrimSpace(string(command.TargetType)) != string(command.TargetType) ||
		strings.TrimSpace(command.TargetID) != command.TargetID || command.TargetID == "" ||
		command.ExpectedRevision < 0 ||
		strings.TrimSpace(command.Reason) != command.Reason || command.Reason == "" ||
		strings.TrimSpace(command.RequestedBy) != command.RequestedBy || command.RequestedBy == "" ||
		command.State != domain.AdminCommandPending || command.Failure != "" ||
		command.CreatedAt.IsZero() || command.AppliedAt != nil ||
		(len(command.Payload) != 0 && !json.Valid(command.Payload)) ||
		!validAdminCommandKind(command.TargetType, command.Kind) {
		return fmt.Errorf("%w: malformed command %q", ErrInvalidAdminCommand, command.ID)
	}
	return nil
}

func validAdminCommandKind(targetType domain.AdminTargetType, kind domain.AdminCommandKind) bool {
	switch targetType {
	case domain.AdminTargetAttempt:
		switch kind {
		case domain.AdminCommandStart, domain.AdminCommandDelay, domain.AdminCommandPause,
			domain.AdminCommandResume, domain.AdminCommandCancel, domain.AdminCommandRetry,
			domain.AdminCommandSkip:
			return true
		}
	case domain.AdminTargetWorkflowRun:
		return kind == domain.AdminCommandCancel
	case domain.AdminTargetSchedule:
		switch kind {
		case domain.AdminCommandScheduleRun, domain.AdminCommandDelayNext,
			domain.AdminCommandEnable, domain.AdminCommandDisable:
			return true
		}
	}
	return false
}

func validateAdminCommandOutcome(outcome domain.AdminCommandOutcome) error {
	failureRequired := outcome.State == domain.AdminCommandRejected || outcome.State == domain.AdminCommandFailed
	if strings.TrimSpace(outcome.CommandID) != outcome.CommandID || outcome.CommandID == "" ||
		outcome.ExpectedState != domain.AdminCommandPending ||
		(outcome.State != domain.AdminCommandApplied && !failureRequired) ||
		outcome.AppliedAt.IsZero() ||
		(failureRequired && strings.TrimSpace(outcome.Failure) == "") ||
		(outcome.State == domain.AdminCommandApplied && outcome.Failure != "") {
		return fmt.Errorf("%w: command %q", ErrInvalidAdminCommandOutcome, outcome.CommandID)
	}
	return nil
}

func insertAdminCommandTx(ctx context.Context, tx *sql.Tx, command domain.AdminCommand) error {
	raw, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("encode admin command %q: %w", command.ID, err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO coordinator_admin_commands(id, target_type, target_id, state, record)
		 VALUES (?, ?, ?, ?, ?)`,
		command.ID, command.TargetType, command.TargetID, command.State, raw,
	); err != nil {
		return fmt.Errorf("insert admin command %q: %w", command.ID, err)
	}
	return nil
}

func loadAdminCommandTx(ctx context.Context, tx *sql.Tx, id string) (domain.AdminCommand, bool, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT record FROM coordinator_admin_commands WHERE id = ?`, id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.AdminCommand{}, false, nil
	}
	if err != nil {
		return domain.AdminCommand{}, false, fmt.Errorf("load admin command %q: %w", id, err)
	}
	var command domain.AdminCommand
	if err := json.Unmarshal(raw, &command); err != nil {
		return domain.AdminCommand{}, false, fmt.Errorf("decode admin command %q: %w", id, err)
	}
	return command, true, nil
}

func insertAuditEventTx(ctx context.Context, tx *sql.Tx, event domain.AuditEvent) (domain.AuditEvent, error) {
	if err := validateAuditEvent(event); err != nil {
		return domain.AuditEvent{}, err
	}
	existing, found, err := loadAuditEventTx(ctx, tx, event.ID)
	if err != nil {
		return domain.AuditEvent{}, err
	}
	if found {
		if !sameAuditEvent(existing, event) {
			return domain.AuditEvent{}, fmt.Errorf("audit event %q is immutable and already has different content", event.ID)
		}
		return existing, nil
	}
	event.Sequence = 0
	raw, err := json.Marshal(event)
	if err != nil {
		return domain.AuditEvent{}, fmt.Errorf("encode audit event %q: %w", event.ID, err)
	}
	result, err := tx.ExecContext(ctx, `
		INSERT INTO coordinator_audit_events(
			id, kind, workflow_run_id, task_id, attempt_id, target_type, target_id, created_at, record
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, event.ID, event.Kind, event.WorkflowRunID, event.TaskID, event.AttemptID,
		event.TargetType, event.TargetID, event.CreatedAt.UTC().Format(time.RFC3339Nano), raw)
	if err != nil {
		return domain.AuditEvent{}, fmt.Errorf("insert audit event %q: %w", event.ID, err)
	}
	sequence, err := result.LastInsertId()
	if err != nil {
		return domain.AuditEvent{}, fmt.Errorf("read audit event %q sequence: %w", event.ID, err)
	}
	event.Sequence = sequence
	raw, err = json.Marshal(event)
	if err != nil {
		return domain.AuditEvent{}, fmt.Errorf("encode sequenced audit event %q: %w", event.ID, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE coordinator_audit_events SET record = ? WHERE sequence = ?`, raw, sequence); err != nil {
		return domain.AuditEvent{}, fmt.Errorf("persist audit event %q sequence: %w", event.ID, err)
	}
	return event, nil
}

func loadAuditEventTx(ctx context.Context, tx *sql.Tx, id string) (domain.AuditEvent, bool, error) {
	var sequence int64
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT sequence, record FROM coordinator_audit_events WHERE id = ?`, id).Scan(&sequence, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.AuditEvent{}, false, nil
	}
	if err != nil {
		return domain.AuditEvent{}, false, fmt.Errorf("load audit event %q: %w", id, err)
	}
	var event domain.AuditEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return domain.AuditEvent{}, false, fmt.Errorf("decode audit event %q: %w", id, err)
	}
	event.Sequence = sequence
	return event, true, nil
}

func validateAuditEvent(event domain.AuditEvent) error {
	if strings.TrimSpace(event.ID) != event.ID || event.ID == "" ||
		strings.TrimSpace(event.Kind) != event.Kind || event.Kind == "" ||
		event.CreatedAt.IsZero() || (len(event.Detail) != 0 && !json.Valid(event.Detail)) {
		return fmt.Errorf("invalid audit event %q", event.ID)
	}
	return nil
}

func sameAdminCommandRequest(left, right domain.AdminCommand) bool {
	return left.ID == right.ID && left.Kind == right.Kind && left.TargetType == right.TargetType &&
		left.TargetID == right.TargetID && left.ExpectedRevision == right.ExpectedRevision &&
		left.Reason == right.Reason && left.RequestedBy == right.RequestedBy &&
		sameJSON(left.Payload, right.Payload)
}

func sameAdminCommandOutcome(command domain.AdminCommand, outcome domain.AdminCommandOutcome) bool {
	return command.State == outcome.State && command.Failure == outcome.Failure &&
		command.AppliedAt != nil
}

func sameAuditEvent(left, right domain.AuditEvent) bool {
	left.Sequence, right.Sequence = 0, 0
	return reflect.DeepEqual(left, right)
}

func sameJSON(left, right json.RawMessage) bool {
	var leftCompact, rightCompact bytes.Buffer
	if err := json.Compact(&leftCompact, left); err != nil {
		return len(left) == 0 && len(right) == 0
	}
	if err := json.Compact(&rightCompact, right); err != nil {
		return false
	}
	return bytes.Equal(leftCompact.Bytes(), rightCompact.Bytes())
}

func adminDecision(command domain.AdminCommand, event domain.AuditEvent) (domain.AdminCommandDecision, error) {
	var detail adminEventDetail
	if err := json.Unmarshal(event.Detail, &detail); err != nil {
		return domain.AdminCommandDecision{}, fmt.Errorf("decode admin command %q audit detail: %w", command.ID, err)
	}
	return domain.AdminCommandDecision{Command: command, Event: event, CurrentTarget: detail.CurrentTarget}, nil
}

func adminSubmissionEventID(commandID string) string {
	return "admin-command:" + commandID + ":submission"
}

func adminOutcomeEventID(commandID string) string {
	return "admin-command:" + commandID + ":outcome"
}
