package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// QuarantineKeyPrefix namespaces a quarantine marker so that it cannot collide
// with the immutable idempotency record of the same key. A key whose content was
// accepted once keeps that record forever, and the deterministic conflict that a
// later, different content produces is a separate durable fact about content
// that can never be accepted.
const QuarantineKeyPrefix = "quarantine:"

// QuarantineKey names the quarantine marker of one intake idempotency key.
func QuarantineKey(key string) string { return QuarantineKeyPrefix + key }

// QuarantineSubmission records a deterministic intake conflict durably. It
// reports whether this exact content was already quarantined, so that a source
// which re-reads the same content on every cycle can report the reason once and
// stay silent afterwards. A different digest for the same key replaces the
// marker, because new content deserves a new attempt and a new report.
func (s *Store) QuarantineSubmission(
	ctx context.Context,
	key, digest, reason string,
	at time.Time,
) (domain.SubmissionRecord, bool, error) {
	if strings.TrimSpace(key) != key || key == "" {
		return domain.SubmissionRecord{}, false, errors.New("quarantined submission key must be trimmed and nonempty")
	}
	if at.IsZero() {
		return domain.SubmissionRecord{}, false, errors.New("quarantine time is required")
	}
	record := domain.SubmissionRecord{
		Key:       QuarantineKey(key),
		Digest:    digest,
		State:     domain.SubmissionQuarantined,
		Reason:    strings.TrimSpace(reason),
		CreatedAt: at.UTC(),
	}
	if err := validateSubmissionRecord(record, false); err != nil {
		return domain.SubmissionRecord{}, false, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return domain.SubmissionRecord{}, false, fmt.Errorf("encode quarantined submission %q: %w", key, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.SubmissionRecord{}, false, fmt.Errorf("begin submission quarantine: %w", err)
	}
	defer tx.Rollback()
	existing, found, err := loadSubmissionIfExistsTx(ctx, tx, record.Key)
	if err != nil {
		return domain.SubmissionRecord{}, false, err
	}
	if found && existing.State == domain.SubmissionQuarantined && existing.Digest == record.Digest {
		return existing, true, nil
	}
	if found && existing.State != domain.SubmissionQuarantined {
		return domain.SubmissionRecord{}, false, fmt.Errorf("submission %q has invalid state %q", record.Key, existing.State)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO coordinator_submissions(
			key, digest, workflow_id, run_id, state, created_at, accepted_at, record
		) VALUES (?, ?, '', '', ?, ?, NULL, ?)
		ON CONFLICT(key) DO UPDATE SET digest = excluded.digest, state = excluded.state,
			created_at = excluded.created_at, record = excluded.record
	`, record.Key, record.Digest, record.State,
		record.CreatedAt.Format(time.RFC3339Nano), raw); err != nil {
		return domain.SubmissionRecord{}, false, fmt.Errorf("quarantine submission %q: %w", key, err)
	}
	detail, err := json.Marshal(record)
	if err != nil {
		return domain.SubmissionRecord{}, false, fmt.Errorf("encode submission %q audit detail: %w", key, err)
	}
	// The event identity includes the digest, so quarantining changed content is
	// a new observation rather than a conflicting rewrite of the old one.
	event := domain.AuditEvent{
		ID: "submission-quarantined:" + key + ":" + record.Digest, Kind: "submission-quarantined",
		TargetType: domain.AuditTargetSubmission, TargetID: key, Actor: "coordinator",
		Reason: record.Reason, Detail: detail, CreatedAt: record.CreatedAt,
	}
	if _, err := insertAuditEventTx(ctx, tx, event); err != nil {
		return domain.SubmissionRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.SubmissionRecord{}, false, fmt.Errorf("commit submission %q quarantine: %w", key, err)
	}
	return record, false, nil
}

// LoadSubmissionQuarantine returns the marker recorded for one intake key.
func (s *Store) LoadSubmissionQuarantine(ctx context.Context, key string) (domain.SubmissionRecord, bool, error) {
	record, found, err := s.LoadSubmission(ctx, QuarantineKey(key))
	if err != nil || !found {
		return domain.SubmissionRecord{}, false, err
	}
	if record.State != domain.SubmissionQuarantined {
		return domain.SubmissionRecord{}, false, fmt.Errorf("submission %q has invalid state %q", QuarantineKey(key), record.State)
	}
	return record, true, nil
}

// ReleaseSubmissionQuarantine drops the marker of one intake key so that its
// content is tried again. The audit event that reported the quarantine stays.
func (s *Store) ReleaseSubmissionQuarantine(ctx context.Context, key string) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM coordinator_submissions WHERE key = ? AND state = ?
	`, QuarantineKey(key), domain.SubmissionQuarantined)
	if err != nil {
		return fmt.Errorf("release submission quarantine %q: %w", key, err)
	}
	return nil
}

// ListQuarantinedSubmissions returns every quarantine marker, oldest first, so
// that an operator can see what intake is refusing and why.
func (s *Store) ListQuarantinedSubmissions(ctx context.Context) ([]domain.SubmissionRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT key, record FROM coordinator_submissions WHERE state = ? ORDER BY created_at, key
	`, domain.SubmissionQuarantined)
	if err != nil {
		return nil, fmt.Errorf("list quarantined submissions: %w", err)
	}
	defer rows.Close()
	var records []domain.SubmissionRecord
	for rows.Next() {
		var key string
		var raw []byte
		if err := rows.Scan(&key, &raw); err != nil {
			return nil, fmt.Errorf("scan quarantined submission: %w", err)
		}
		record, err := decodeSubmission(key, raw)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list quarantined submissions: %w", err)
	}
	return records, nil
}
