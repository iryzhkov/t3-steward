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

// ReleaseQuarantinedSubmission clears one retained historical marker on an
// operator's instruction and records the reason. It never retries a file or
// reenables intake. Releasing a key that holds no marker is not an error;
// it reports that there was nothing to release, which is what makes repeating
// the operation safe.
func (s *Store) ReleaseQuarantinedSubmission(
	ctx context.Context,
	key, actor, reason string,
	at time.Time,
) (domain.QuarantineRelease, error) {
	if strings.TrimSpace(key) != key || key == "" {
		return domain.QuarantineRelease{}, errors.New("quarantined submission key must be trimmed and nonempty")
	}
	if strings.TrimSpace(actor) != actor || actor == "" {
		return domain.QuarantineRelease{}, errors.New("quarantine release actor is required")
	}
	if strings.TrimSpace(reason) != reason || reason == "" {
		return domain.QuarantineRelease{}, errors.New("quarantine release reason is required")
	}
	if at.IsZero() {
		return domain.QuarantineRelease{}, errors.New("quarantine release time is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.QuarantineRelease{}, fmt.Errorf("begin quarantine release: %w", err)
	}
	defer tx.Rollback()
	record, found, err := loadSubmissionIfExistsTx(ctx, tx, QuarantineKey(key))
	if err != nil {
		return domain.QuarantineRelease{}, err
	}
	if !found || record.State != domain.SubmissionQuarantined {
		return domain.QuarantineRelease{Key: key, ReleasedAt: at.UTC()}, tx.Commit()
	}
	if _, err := tx.ExecContext(ctx,
		"DELETE FROM coordinator_submissions WHERE key = ? AND state = ?",
		QuarantineKey(key), domain.SubmissionQuarantined); err != nil {
		return domain.QuarantineRelease{}, fmt.Errorf("release submission quarantine %q: %w", key, err)
	}
	release := domain.QuarantineRelease{
		Key: key, Released: true, Digest: record.Digest,
		Reason: record.Reason, ReleasedAt: at.UTC(),
	}
	detail, err := json.Marshal(release)
	if err != nil {
		return domain.QuarantineRelease{}, fmt.Errorf("encode quarantine release %q detail: %w", key, err)
	}
	// The event identity includes the digest that was released, so releasing
	// the same marker twice is the same observation and a marker recorded again
	// for new content is a new one.
	event := domain.AuditEvent{
		ID:   "submission-quarantine-released:" + key + ":" + record.Digest,
		Kind: "submission-quarantine-released", TargetType: domain.AuditTargetSubmission,
		TargetID: key, Actor: actor, Reason: reason, Detail: detail, CreatedAt: release.ReleasedAt,
	}
	if _, err := insertAuditEventTx(ctx, tx, event); err != nil {
		return domain.QuarantineRelease{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.QuarantineRelease{}, fmt.Errorf("commit quarantine release %q: %w", key, err)
	}
	return release, nil
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
