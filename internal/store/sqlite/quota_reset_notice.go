package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// RecordQuotaNoticeDelivery remembers that a thread was warned, drained or
// stopped for one quota window, so that the advisory owed to it when that
// window resets survives a restart.
//
// The row identity is the thread, the bucket and the window. A thread that
// escalates from a warning to a stop within the same window keeps one row:
// the wording is updated, the time it was first told is kept, and an
// advisory that was already settled is never reopened.
func (s *Store) RecordQuotaNoticeDelivery(ctx context.Context, n domain.QuotaResetNotice) error {
	if n.ThreadID == "" || n.Epoch == "" || n.ResetsAt.IsZero() {
		// Without a reported reset time there is no window to follow up on.
		return nil
	}
	stopped := 0
	if n.StoppedByWatchdog {
		stopped = 1
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO quota_reset_notices(thread_id, bucket, epoch, kind, limit_name, threshold, used_percent, warned_at, resets_at, stopped, notified_at, outcome)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, '')
		 ON CONFLICT(thread_id, bucket, epoch) DO UPDATE SET
		   kind = excluded.kind,
		   limit_name = excluded.limit_name,
		   threshold = excluded.threshold,
		   used_percent = excluded.used_percent,
		   resets_at = excluded.resets_at,
		   stopped = MAX(quota_reset_notices.stopped, excluded.stopped)`,
		n.ThreadID, n.Key.String(), n.Epoch, string(n.Kind), n.LimitName, n.Threshold, n.UsedPercent,
		n.WarnedAt.UTC().Format(time.RFC3339Nano), n.ResetsAt.UTC().Format(time.RFC3339Nano), stopped)
	return err
}

// PendingQuotaResetNotices returns the recorded notices whose window reset
// time is at or before the given time and whose advisory has not been
// settled yet, oldest window first.
func (s *Store) PendingQuotaResetNotices(ctx context.Context, resetBy time.Time) ([]domain.QuotaResetNotice, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT thread_id, bucket, epoch, kind, limit_name, threshold, used_percent, warned_at, resets_at, stopped
		 FROM quota_reset_notices WHERE notified_at IS NULL AND resets_at <= ? ORDER BY resets_at, warned_at`,
		resetBy.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.QuotaResetNotice
	for rows.Next() {
		var (
			n                            domain.QuotaResetNotice
			bucket, kind, warned, resets string
			stopped                      int
		)
		if err := rows.Scan(&n.ThreadID, &bucket, &n.Epoch, &kind, &n.LimitName, &n.Threshold, &n.UsedPercent, &warned, &resets, &stopped); err != nil {
			return nil, err
		}
		n.Key = domain.ParseBucketKey(bucket)
		n.Kind = domain.ActionKind(kind)
		n.WarnedAt, _ = time.Parse(time.RFC3339Nano, warned)
		n.ResetsAt, _ = time.Parse(time.RFC3339Nano, resets)
		n.StoppedByWatchdog = stopped == 1
		out = append(out, n)
	}
	return out, rows.Err()
}

// SettleQuotaResetNotice claims the single advisory owed for one thread,
// bucket and window. It returns false when another pass, or the same daemon
// before a restart, already settled it; the caller must not send anything
// then. Claiming before sending is deliberate: a crash between the claim and
// the send loses one advisory, where the opposite order would repeat it.
func (s *Store) SettleQuotaResetNotice(ctx context.Context, threadID string, key domain.BucketKey, epoch string, at time.Time, outcome string) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE quota_reset_notices SET notified_at = ?, outcome = ?
		 WHERE thread_id = ? AND bucket = ? AND epoch = ? AND notified_at IS NULL`,
		at.UTC().Format(time.RFC3339Nano), outcome, threadID, key.String(), epoch)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// QuotaResetNotice returns one recorded notice, for inspection and tests.
func (s *Store) QuotaResetNotice(ctx context.Context, threadID string, key domain.BucketKey, epoch string) (domain.QuotaResetNotice, bool, error) {
	var (
		n                            domain.QuotaResetNotice
		bucket, kind, warned, resets string
		stopped                      int
		notified                     sql.NullString
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT thread_id, bucket, epoch, kind, limit_name, threshold, used_percent, warned_at, resets_at, stopped, notified_at, outcome
		 FROM quota_reset_notices WHERE thread_id = ? AND bucket = ? AND epoch = ?`,
		threadID, key.String(), epoch).
		Scan(&n.ThreadID, &bucket, &n.Epoch, &kind, &n.LimitName, &n.Threshold, &n.UsedPercent, &warned, &resets, &stopped, &notified, &n.Outcome)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.QuotaResetNotice{}, false, nil
	}
	if err != nil {
		return domain.QuotaResetNotice{}, false, err
	}
	n.Key = domain.ParseBucketKey(bucket)
	n.Kind = domain.ActionKind(kind)
	n.WarnedAt, _ = time.Parse(time.RFC3339Nano, warned)
	n.ResetsAt, _ = time.Parse(time.RFC3339Nano, resets)
	n.StoppedByWatchdog = stopped == 1
	if notified.Valid {
		if t, err := time.Parse(time.RFC3339Nano, notified.String); err == nil {
			n.NotifiedAt = &t
		}
	}
	return n, true, nil
}

// PruneQuotaResetNotices forgets notices first delivered before the cutoff,
// settled or not. An advisory that old is no longer worth sending.
func (s *Store) PruneQuotaResetNotices(ctx context.Context, before time.Time) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM quota_reset_notices WHERE warned_at < ?`, before.UTC().Format(time.RFC3339Nano))
	return err
}
