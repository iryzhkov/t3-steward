package sqlite

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const coordinatorMigrationV11 = `
CREATE TABLE IF NOT EXISTS coordinator_submissions (
	key TEXT PRIMARY KEY,
	digest TEXT NOT NULL,
	workflow_id TEXT NOT NULL,
	run_id TEXT NOT NULL,
	state TEXT NOT NULL,
	created_at TEXT NOT NULL,
	accepted_at TEXT,
	record BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS coordinator_submissions_state_idx
	ON coordinator_submissions(state, created_at);
`

var ErrSubmissionConflict = errors.New("submission idempotency key already has different content")

// ReserveSubmission durably fixes request content and result identities before
// the coordinator publishes files or workflow metadata.
func (s *Store) ReserveSubmission(ctx context.Context, proposed domain.SubmissionRecord) (domain.SubmissionRecord, bool, error) {
	if err := validateSubmissionRecord(proposed, false); err != nil {
		return domain.SubmissionRecord{}, false, err
	}
	proposed.State = domain.SubmissionPending
	proposed.AcceptedAt = nil
	raw, err := json.Marshal(proposed)
	if err != nil {
		return domain.SubmissionRecord{}, false, fmt.Errorf("encode submission %q: %w", proposed.Key, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.SubmissionRecord{}, false, fmt.Errorf("begin submission reservation: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO coordinator_submissions(
			key, digest, workflow_id, run_id, state, created_at, accepted_at, record
		) VALUES (?, ?, ?, ?, ?, ?, NULL, ?)
	`, proposed.Key, proposed.Digest, proposed.WorkflowID, proposed.RunID,
		proposed.State, proposed.CreatedAt.UTC().Format(time.RFC3339Nano), raw)
	if err != nil {
		return domain.SubmissionRecord{}, false, fmt.Errorf("reserve submission %q: %w", proposed.Key, err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return domain.SubmissionRecord{}, false, fmt.Errorf("inspect submission %q reservation: %w", proposed.Key, err)
	}
	record, err := loadSubmissionTx(ctx, tx, proposed.Key)
	if err != nil {
		return domain.SubmissionRecord{}, false, err
	}
	if !sameSubmissionRequest(record, proposed) {
		return domain.SubmissionRecord{}, false, fmt.Errorf("%w: %q", ErrSubmissionConflict, proposed.Key)
	}
	if err := tx.Commit(); err != nil {
		return domain.SubmissionRecord{}, false, fmt.Errorf("commit submission %q reservation: %w", proposed.Key, err)
	}
	return record, inserted == 0, nil
}

// CompleteSubmission publishes the immutable accepted result after workflow
// files and coordinator metadata are durable. Exact completion replay is safe.
func (s *Store) CompleteSubmission(ctx context.Context, key, digest string, acceptedAt time.Time) (domain.SubmissionRecord, bool, error) {
	if acceptedAt.IsZero() {
		return domain.SubmissionRecord{}, false, errors.New("submission acceptance time is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.SubmissionRecord{}, false, fmt.Errorf("begin submission completion: %w", err)
	}
	defer tx.Rollback()
	record, err := loadSubmissionTx(ctx, tx, key)
	if err != nil {
		return domain.SubmissionRecord{}, false, err
	}
	if record.Digest != digest {
		return domain.SubmissionRecord{}, false, fmt.Errorf("%w: %q", ErrSubmissionConflict, key)
	}
	if record.State == domain.SubmissionAccepted {
		if _, err := insertSubmissionAcceptedAuditEvent(ctx, tx, record); err != nil {
			return domain.SubmissionRecord{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return domain.SubmissionRecord{}, false, err
		}
		return record, true, nil
	}
	if record.State != domain.SubmissionPending {
		return domain.SubmissionRecord{}, false, fmt.Errorf("submission %q has invalid state %q", key, record.State)
	}
	at := acceptedAt.UTC()
	record.State = domain.SubmissionAccepted
	record.AcceptedAt = &at
	raw, err := json.Marshal(record)
	if err != nil {
		return domain.SubmissionRecord{}, false, fmt.Errorf("encode completed submission %q: %w", key, err)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE coordinator_submissions
		SET state = ?, accepted_at = ?, record = ?
		WHERE key = ? AND digest = ? AND state = ?
	`, record.State, at.Format(time.RFC3339Nano), raw, key, digest, domain.SubmissionPending)
	if err != nil {
		return domain.SubmissionRecord{}, false, fmt.Errorf("complete submission %q: %w", key, err)
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		if err == nil {
			err = fmt.Errorf("updated %d rows", affected)
		}
		return domain.SubmissionRecord{}, false, fmt.Errorf("complete submission %q: %w", key, err)
	}
	if _, err := insertSubmissionAcceptedAuditEvent(ctx, tx, record); err != nil {
		return domain.SubmissionRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.SubmissionRecord{}, false, fmt.Errorf("commit submission %q completion: %w", key, err)
	}
	return record, false, nil
}

func insertSubmissionAcceptedAuditEvent(
	ctx context.Context,
	tx *sql.Tx,
	record domain.SubmissionRecord,
) (domain.AuditEvent, error) {
	detail, err := json.Marshal(record)
	if err != nil {
		return domain.AuditEvent{}, fmt.Errorf("encode submission %q audit detail: %w", record.Key, err)
	}
	event := domain.AuditEvent{
		ID: "submission-accepted:" + record.Key, Kind: "submission-accepted",
		WorkflowRunID: record.RunID, TargetType: domain.AuditTargetSubmission, TargetID: record.Key,
		Actor: "coordinator", Reason: "submission accepted", Detail: detail, CreatedAt: *record.AcceptedAt,
	}
	return insertAuditEventTx(ctx, tx, event)
}

func (s *Store) LoadSubmission(ctx context.Context, key string) (domain.SubmissionRecord, bool, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT record FROM coordinator_submissions WHERE key = ?`, key).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.SubmissionRecord{}, false, nil
	}
	if err != nil {
		return domain.SubmissionRecord{}, false, fmt.Errorf("load submission %q: %w", key, err)
	}
	record, err := decodeSubmission(key, raw)
	return record, true, err
}

func loadSubmissionTx(ctx context.Context, tx *sql.Tx, key string) (domain.SubmissionRecord, error) {
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT record FROM coordinator_submissions WHERE key = ?`, key).Scan(&raw); err != nil {
		return domain.SubmissionRecord{}, fmt.Errorf("load submission %q: %w", key, err)
	}
	return decodeSubmission(key, raw)
}

func decodeSubmission(key string, raw []byte) (domain.SubmissionRecord, error) {
	var record domain.SubmissionRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return domain.SubmissionRecord{}, fmt.Errorf("decode submission %q: %w", key, err)
	}
	if err := validateSubmissionRecord(record, record.State == domain.SubmissionAccepted); err != nil {
		return domain.SubmissionRecord{}, fmt.Errorf("decode submission %q: %w", key, err)
	}
	return record, nil
}

func validateSubmissionRecord(record domain.SubmissionRecord, accepted bool) error {
	if strings.TrimSpace(record.Key) != record.Key || record.Key == "" || len(record.Key) > 256 {
		return errors.New("submission idempotency key must be 1-256 trimmed characters")
	}
	digest, err := hex.DecodeString(record.Digest)
	if err != nil || len(digest) != 32 {
		return errors.New("submission digest must be a SHA-256 hex string")
	}
	if strings.TrimSpace(record.WorkflowID) != record.WorkflowID || record.WorkflowID == "" ||
		strings.TrimSpace(record.RunID) != record.RunID || record.RunID == "" ||
		record.CreatedAt.IsZero() {
		return errors.New("submission result identities and creation time are required")
	}
	if accepted {
		if record.State != domain.SubmissionAccepted || record.AcceptedAt == nil || record.AcceptedAt.IsZero() {
			return errors.New("accepted submission requires an acceptance time")
		}
	} else if record.State != "" && record.State != domain.SubmissionPending {
		return fmt.Errorf("invalid pending submission state %q", record.State)
	}
	return nil
}

func sameSubmissionRequest(left, right domain.SubmissionRecord) bool {
	return left.Key == right.Key && left.Digest == right.Digest &&
		left.WorkflowID == right.WorkflowID && left.RunID == right.RunID
}
