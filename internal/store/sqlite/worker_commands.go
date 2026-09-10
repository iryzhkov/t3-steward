package sqlite

import (
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

const coordinatorMigrationV9 = `
ALTER TABLE coordinator_assignments ADD COLUMN lease_expires_at TEXT NOT NULL DEFAULT '';
UPDATE coordinator_assignments SET lease_expires_at =
	COALESCE(json_extract(record, '$.leaseExpiresAt'), '');
CREATE INDEX coordinator_assignments_lease_expiry
	ON coordinator_assignments(assignment_state, lease_expires_at);
CREATE TABLE coordinator_worker_commands (
	id TEXT PRIMARY KEY,
	kind TEXT NOT NULL,
	worker_id TEXT NOT NULL,
	worker_epoch TEXT NOT NULL,
	coordinator_epoch INTEGER NOT NULL,
	assignment_id TEXT NOT NULL,
	assignment_epoch INTEGER NOT NULL,
	expected_worker_sequence INTEGER NOT NULL,
	acknowledged INTEGER NOT NULL DEFAULT 0,
	record TEXT NOT NULL
);
CREATE INDEX coordinator_worker_commands_pending
	ON coordinator_worker_commands(worker_id, worker_epoch, coordinator_epoch, acknowledged, id);
CREATE UNIQUE INDEX coordinator_worker_commands_assignment_kind
	ON coordinator_worker_commands(assignment_id, assignment_epoch, kind);
CREATE TABLE coordinator_worker_acknowledgements (
	command_id TEXT PRIMARY KEY,
	worker_sequence INTEGER NOT NULL,
	accepted INTEGER NOT NULL,
	record TEXT NOT NULL,
	FOREIGN KEY(command_id) REFERENCES coordinator_worker_commands(id)
);
`

var (
	ErrWorkerCommand          = errors.New("worker command rejected")
	ErrWorkerAcknowledgement  = errors.New("worker acknowledgement rejected")
	ErrAssignmentLeaseRenewal = errors.New("assignment lease renewal rejected")
)

// CommitWorkerCommands durably records commands before they can be delivered.
// Exact replay is idempotent; new commands require the exact fresh worker
// snapshot named by each command.
func (s *Store) CommitWorkerCommands(ctx context.Context, commands []domain.WorkerCommand) ([]domain.WorkerCommand, error) {
	if len(commands) == 0 {
		return nil, fmt.Errorf("%w: empty command batch", ErrWorkerCommand)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin worker command batch: %w", err)
	}
	defer tx.Rollback()

	result := make([]domain.WorkerCommand, 0, len(commands))
	seen := make(map[string]struct{}, len(commands))
	for _, command := range commands {
		if err := validateWorkerCommand(command); err != nil {
			return nil, err
		}
		if _, exists := seen[command.ID]; exists {
			return nil, fmt.Errorf("%w: duplicate command %q", ErrWorkerCommand, command.ID)
		}
		seen[command.ID] = struct{}{}

		current, exists, err := loadWorkerCommandTx(ctx, tx, command.ID)
		if err != nil {
			return nil, err
		}
		if exists {
			if !reflect.DeepEqual(current, command) {
				return nil, fmt.Errorf("%w: command %q replay changes identity", ErrWorkerCommand, command.ID)
			}
			result = append(result, current)
			continue
		}
		if err := requireCoordinatorEpoch(ctx, tx, command.CoordinatorEpoch); err != nil {
			return nil, err
		}
		snapshot, exists, err := loadWorkerSnapshotTx(ctx, tx, command.WorkerID)
		if err != nil {
			return nil, err
		}
		if !exists || snapshot.WorkerEpoch != command.WorkerEpoch ||
			snapshot.CoordinatorEpoch != command.CoordinatorEpoch ||
			snapshot.Sequence != command.ExpectedWorkerSequence {
			return nil, fmt.Errorf("%w: worker %q snapshot changed", ErrStaleWorkerSnapshot, command.WorkerID)
		}
		if !workerAccepts(snapshot, command.CreatedAt) {
			return nil, fmt.Errorf("%w: worker %q is disconnected, stale, or not ready", ErrWorkerUnavailable, command.WorkerID)
		}
		assignment, err := loadAssignmentTx(ctx, tx, command.AssignmentID)
		if err != nil {
			return nil, err
		}
		if err := validateCommandAssignment(command, assignment); err != nil {
			return nil, err
		}
		raw, err := json.Marshal(command)
		if err != nil {
			return nil, fmt.Errorf("encode worker command %q: %w", command.ID, err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO coordinator_worker_commands(
				id, kind, worker_id, worker_epoch, coordinator_epoch, assignment_id,
				assignment_epoch, expected_worker_sequence, acknowledged, record
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
		`, command.ID, command.Kind, command.WorkerID, command.WorkerEpoch, command.CoordinatorEpoch,
			command.AssignmentID, command.AssignmentEpoch, command.ExpectedWorkerSequence, raw); err != nil {
			return nil, fmt.Errorf("%w: commit command %q: %v", ErrWorkerCommand, command.ID, err)
		}
		result = append(result, command)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit worker command batch: %w", err)
	}
	return result, nil
}

// LoadPendingWorkerCommands returns commands only through the worker's exact
// current, fresh snapshot. A worker restart or coordinator restart therefore
// fences commands issued against the old session.
func (s *Store) LoadPendingWorkerCommands(
	ctx context.Context,
	workerID, workerEpoch string,
	coordinatorEpoch, workerSequence int64,
	now time.Time,
) ([]domain.WorkerCommand, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin pending worker commands: %w", err)
	}
	defer tx.Rollback()
	if err := requireCoordinatorEpoch(ctx, tx, coordinatorEpoch); err != nil {
		return nil, err
	}
	if err := requireCurrentWorker(ctx, tx, workerID, workerEpoch, coordinatorEpoch, workerSequence, now); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT id, record FROM coordinator_worker_commands
		WHERE worker_id = ? AND worker_epoch = ? AND coordinator_epoch = ?
		  AND expected_worker_sequence <= ? AND acknowledged = 0
		ORDER BY id
	`, workerID, workerEpoch, coordinatorEpoch, workerSequence)
	if err != nil {
		return nil, fmt.Errorf("load pending worker commands: %w", err)
	}
	defer rows.Close()
	var commands []domain.WorkerCommand
	for rows.Next() {
		var id, raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, fmt.Errorf("scan pending worker command: %w", err)
		}
		var command domain.WorkerCommand
		if err := json.Unmarshal([]byte(raw), &command); err != nil {
			return nil, fmt.Errorf("decode worker command %q: %w", id, err)
		}
		commands = append(commands, command)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending worker commands: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("finish pending worker commands: %w", err)
	}
	return commands, nil
}

// AcknowledgeWorkerCommand records one idempotent response after checking every
// coordinator, worker, assignment, and snapshot fence.
func (s *Store) AcknowledgeWorkerCommand(ctx context.Context, acknowledgement domain.WorkerAcknowledgement) (domain.WorkerAcknowledgement, error) {
	if err := validateWorkerAcknowledgement(acknowledgement); err != nil {
		return domain.WorkerAcknowledgement{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.WorkerAcknowledgement{}, fmt.Errorf("begin worker acknowledgement: %w", err)
	}
	defer tx.Rollback()

	current, exists, err := loadWorkerAcknowledgementTx(ctx, tx, acknowledgement.CommandID)
	if err != nil {
		return domain.WorkerAcknowledgement{}, err
	}
	if exists {
		if !reflect.DeepEqual(current, acknowledgement) {
			return domain.WorkerAcknowledgement{}, fmt.Errorf("%w: acknowledgement for %q conflicts with durable response", ErrWorkerAcknowledgement, acknowledgement.CommandID)
		}
		if err := tx.Commit(); err != nil {
			return domain.WorkerAcknowledgement{}, err
		}
		return current, nil
	}
	if err := requireCoordinatorEpoch(ctx, tx, acknowledgement.CoordinatorEpoch); err != nil {
		return domain.WorkerAcknowledgement{}, err
	}
	if err := requireCurrentWorker(ctx, tx, acknowledgement.WorkerID, acknowledgement.WorkerEpoch,
		acknowledgement.CoordinatorEpoch, acknowledgement.WorkerSequence, acknowledgement.AcknowledgedAt); err != nil {
		return domain.WorkerAcknowledgement{}, err
	}
	command, exists, err := loadWorkerCommandTx(ctx, tx, acknowledgement.CommandID)
	if err != nil {
		return domain.WorkerAcknowledgement{}, err
	}
	if !exists || command.WorkerID != acknowledgement.WorkerID ||
		command.WorkerEpoch != acknowledgement.WorkerEpoch ||
		command.CoordinatorEpoch != acknowledgement.CoordinatorEpoch ||
		command.AssignmentID != acknowledgement.AssignmentID ||
		command.AssignmentEpoch != acknowledgement.AssignmentEpoch {
		return domain.WorkerAcknowledgement{}, fmt.Errorf("%w: command identity mismatch for %q", ErrWorkerAcknowledgement, acknowledgement.CommandID)
	}
	raw, err := json.Marshal(acknowledgement)
	if err != nil {
		return domain.WorkerAcknowledgement{}, fmt.Errorf("encode worker acknowledgement %q: %w", acknowledgement.CommandID, err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO coordinator_worker_acknowledgements(command_id, worker_sequence, accepted, record)
		VALUES (?, ?, ?, ?)
	`, acknowledgement.CommandID, acknowledgement.WorkerSequence, acknowledgement.Accepted, raw); err != nil {
		return domain.WorkerAcknowledgement{}, fmt.Errorf("commit worker acknowledgement %q: %w", acknowledgement.CommandID, err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE coordinator_worker_commands SET acknowledged = 1 WHERE id = ? AND acknowledged = 0`,
		acknowledgement.CommandID); err != nil {
		return domain.WorkerAcknowledgement{}, fmt.Errorf("complete worker command %q: %w", acknowledgement.CommandID, err)
	}
	if err := tx.Commit(); err != nil {
		return domain.WorkerAcknowledgement{}, fmt.Errorf("commit worker acknowledgement %q: %w", acknowledgement.CommandID, err)
	}
	return acknowledgement, nil
}

// RenewAssignmentLease extends a live claim only from the exact current worker
// snapshot. Exact replay returns the durable assignment.
func (s *Store) RenewAssignmentLease(ctx context.Context, renewal domain.AssignmentLeaseRenewal) (domain.Assignment, error) {
	if err := validateAssignmentLeaseRenewal(renewal); err != nil {
		return domain.Assignment{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Assignment{}, fmt.Errorf("begin assignment lease renewal: %w", err)
	}
	defer tx.Rollback()
	if err := requireCoordinatorEpoch(ctx, tx, renewal.CoordinatorEpoch); err != nil {
		return domain.Assignment{}, err
	}
	assignment, err := loadAssignmentTx(ctx, tx, renewal.AssignmentID)
	if err != nil {
		return domain.Assignment{}, err
	}
	if assignment.WorkerID != renewal.WorkerID || assignment.WorkerEpoch != renewal.WorkerEpoch ||
		assignment.Epoch != renewal.AssignmentEpoch || assignment.LeaseToken != renewal.LeaseToken ||
		assignment.State != domain.AssignmentClaimed {
		return domain.Assignment{}, fmt.Errorf("%w: assignment identity or state mismatch for %q", ErrAssignmentLeaseRenewal, renewal.AssignmentID)
	}
	if assignment.LeaseExpiresAt.Equal(renewal.LeaseExpiresAt) {
		if err := tx.Commit(); err != nil {
			return domain.Assignment{}, err
		}
		return assignment, nil
	}
	if !assignment.LeaseExpiresAt.After(renewal.RenewedAt) ||
		!renewal.LeaseExpiresAt.After(assignment.LeaseExpiresAt) {
		return domain.Assignment{}, fmt.Errorf("%w: assignment %q lease is expired or not extended", ErrAssignmentLeaseRenewal, renewal.AssignmentID)
	}
	if err := requireCurrentWorker(ctx, tx, renewal.WorkerID, renewal.WorkerEpoch,
		renewal.CoordinatorEpoch, renewal.WorkerSequence, renewal.RenewedAt); err != nil {
		return domain.Assignment{}, err
	}
	assignment.LeaseExpiresAt = renewal.LeaseExpiresAt
	assignment.UpdatedAt = renewal.RenewedAt
	if err := updateAssignmentLeaseTx(ctx, tx, assignment, domain.AssignmentClaimed); err != nil {
		return domain.Assignment{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Assignment{}, fmt.Errorf("commit assignment lease renewal %q: %w", assignment.ID, err)
	}
	return assignment, nil
}

// ExpireAssignmentLeases marks elapsed claims unknown. Unknown is deliberately
// nonterminal: the coordinator must prove the old execution stopped before any
// reassignment or dispatch.
func (s *Store) ExpireAssignmentLeases(ctx context.Context, coordinatorEpoch int64, now time.Time) ([]domain.Assignment, error) {
	if coordinatorEpoch < 1 || now.IsZero() {
		return nil, fmt.Errorf("%w: invalid coordinator epoch or expiry time", ErrAssignmentLeaseRenewal)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin assignment lease expiry: %w", err)
	}
	defer tx.Rollback()
	if err := requireCoordinatorEpoch(ctx, tx, coordinatorEpoch); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT id, record FROM coordinator_assignments
		WHERE assignment_state = ?
		ORDER BY id
	`, domain.AssignmentClaimed)
	if err != nil {
		return nil, fmt.Errorf("load expired assignment leases: %w", err)
	}
	var expired []domain.Assignment
	for rows.Next() {
		var id, raw string
		if err := rows.Scan(&id, &raw); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan expired assignment lease: %w", err)
		}
		var assignment domain.Assignment
		if err := json.Unmarshal([]byte(raw), &assignment); err != nil {
			rows.Close()
			return nil, fmt.Errorf("decode expired assignment %q: %w", id, err)
		}
		if !assignment.LeaseExpiresAt.IsZero() && !assignment.LeaseExpiresAt.After(now) {
			expired = append(expired, assignment)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate expired assignment leases: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for index := range expired {
		expired[index].State = domain.AssignmentUnknown
		if expired[index].DispatchState == domain.DispatchCreating ||
			expired[index].DispatchState == domain.DispatchConfirmed {
			expired[index].DispatchState = domain.DispatchUnknown
		}
		expired[index].UpdatedAt = now
		if err := updateAssignmentLeaseTx(ctx, tx, expired[index], domain.AssignmentClaimed); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit assignment lease expiry: %w", err)
	}
	return expired, nil
}

func requireCurrentWorker(ctx context.Context, tx *sql.Tx, workerID, workerEpoch string, coordinatorEpoch, workerSequence int64, now time.Time) error {
	snapshot, exists, err := loadWorkerSnapshotTx(ctx, tx, workerID)
	if err != nil {
		return err
	}
	if !exists || snapshot.WorkerEpoch != workerEpoch ||
		snapshot.CoordinatorEpoch != coordinatorEpoch || snapshot.Sequence != workerSequence {
		return fmt.Errorf("%w: worker %q snapshot is not current", ErrStaleWorkerSnapshot, workerID)
	}
	if !workerAccepts(snapshot, now) {
		return fmt.Errorf("%w: worker %q is disconnected, stale, or not ready", ErrWorkerUnavailable, workerID)
	}
	return nil
}

func validateWorkerCommand(command domain.WorkerCommand) error {
	switch command.Kind {
	case domain.WorkerCommandPrepare, domain.WorkerCommandDispatch, domain.WorkerCommandStop, domain.WorkerCommandCollect:
	default:
		return fmt.Errorf("%w: invalid kind %q", ErrWorkerCommand, command.Kind)
	}
	if strings.TrimSpace(command.ID) != command.ID || command.ID == "" ||
		command.WorkerID == "" || command.WorkerEpoch == "" || command.CoordinatorEpoch < 1 ||
		command.AssignmentID == "" || command.AssignmentEpoch < 1 ||
		command.ExpectedWorkerSequence < 1 || command.CreatedAt.IsZero() {
		return fmt.Errorf("%w: invalid command identity or snapshot", ErrWorkerCommand)
	}
	return nil
}

func validateCommandAssignment(command domain.WorkerCommand, assignment domain.Assignment) error {
	if assignment.WorkerID != command.WorkerID || assignment.WorkerEpoch != command.WorkerEpoch ||
		assignment.Epoch != command.AssignmentEpoch {
		return fmt.Errorf("%w: assignment identity mismatch for %q", ErrWorkerCommand, command.ID)
	}
	switch command.Kind {
	case domain.WorkerCommandPrepare, domain.WorkerCommandDispatch:
		if assignment.State != domain.AssignmentClaimed {
			return fmt.Errorf("%w: command %q requires a claimed assignment", ErrWorkerCommand, command.ID)
		}
	case domain.WorkerCommandStop, domain.WorkerCommandCollect:
		if assignment.State != domain.AssignmentClaimed && assignment.State != domain.AssignmentUnknown {
			return fmt.Errorf("%w: command %q cannot target assignment state %q", ErrWorkerCommand, command.ID, assignment.State)
		}
	}
	return nil
}

func validateWorkerAcknowledgement(acknowledgement domain.WorkerAcknowledgement) error {
	if acknowledgement.CommandID == "" || acknowledgement.WorkerID == "" ||
		acknowledgement.WorkerEpoch == "" || acknowledgement.CoordinatorEpoch < 1 ||
		acknowledgement.AssignmentID == "" || acknowledgement.AssignmentEpoch < 1 ||
		acknowledgement.WorkerSequence < 1 || acknowledgement.AcknowledgedAt.IsZero() {
		return fmt.Errorf("%w: invalid acknowledgement identity or snapshot", ErrWorkerAcknowledgement)
	}
	return nil
}

func validateAssignmentLeaseRenewal(renewal domain.AssignmentLeaseRenewal) error {
	if renewal.CoordinatorEpoch < 1 || renewal.WorkerID == "" || renewal.WorkerEpoch == "" ||
		renewal.WorkerSequence < 1 || renewal.AssignmentID == "" || renewal.AssignmentEpoch < 1 ||
		renewal.LeaseToken == "" || renewal.RenewedAt.IsZero() ||
		!renewal.LeaseExpiresAt.After(renewal.RenewedAt) {
		return fmt.Errorf("%w: invalid renewal identity or lease", ErrAssignmentLeaseRenewal)
	}
	return nil
}

func loadWorkerCommandTx(ctx context.Context, tx *sql.Tx, commandID string) (domain.WorkerCommand, bool, error) {
	var raw string
	err := tx.QueryRowContext(ctx,
		`SELECT record FROM coordinator_worker_commands WHERE id = ?`, commandID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.WorkerCommand{}, false, nil
	}
	if err != nil {
		return domain.WorkerCommand{}, false, fmt.Errorf("load worker command %q: %w", commandID, err)
	}
	var command domain.WorkerCommand
	if err := json.Unmarshal([]byte(raw), &command); err != nil {
		return domain.WorkerCommand{}, false, fmt.Errorf("decode worker command %q: %w", commandID, err)
	}
	return command, true, nil
}

func loadWorkerAcknowledgementTx(ctx context.Context, tx *sql.Tx, commandID string) (domain.WorkerAcknowledgement, bool, error) {
	var raw string
	err := tx.QueryRowContext(ctx,
		`SELECT record FROM coordinator_worker_acknowledgements WHERE command_id = ?`, commandID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.WorkerAcknowledgement{}, false, nil
	}
	if err != nil {
		return domain.WorkerAcknowledgement{}, false, fmt.Errorf("load worker acknowledgement %q: %w", commandID, err)
	}
	var acknowledgement domain.WorkerAcknowledgement
	if err := json.Unmarshal([]byte(raw), &acknowledgement); err != nil {
		return domain.WorkerAcknowledgement{}, false, fmt.Errorf("decode worker acknowledgement %q: %w", commandID, err)
	}
	return acknowledgement, true, nil
}

func updateAssignmentLeaseTx(ctx context.Context, tx *sql.Tx, assignment domain.Assignment, expectedState domain.AssignmentState) error {
	raw, err := json.Marshal(assignment)
	if err != nil {
		return fmt.Errorf("encode assignment %q: %w", assignment.ID, err)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE coordinator_assignments
		SET assignment_state = ?, dispatch_state = ?, lease_expires_at = ?, record = ?
		WHERE id = ? AND assignment_state = ?
	`, assignment.State, assignment.DispatchState,
		assignment.LeaseExpiresAt.UTC().Format(time.RFC3339Nano), raw,
		assignment.ID, expectedState)
	if err != nil {
		return fmt.Errorf("update assignment lease %q: %w", assignment.ID, err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return fmt.Errorf("%w: assignment %q changed concurrently", ErrAssignmentLeaseRenewal, assignment.ID)
	}
	return nil
}
