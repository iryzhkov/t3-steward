// Package sqlite persists bucket states, resume intents, log positions and
// the audit log in one SQLite database.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite" // database/sql driver

	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
)

// Store is the SQLite-backed state store.
type Store struct {
	db *sql.DB
}

var migrations = []string{
	`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);`,
	`CREATE TABLE IF NOT EXISTS buckets (
		key TEXT PRIMARY KEY,
		state TEXT NOT NULL,
		updated_at TEXT NOT NULL
	);`,
	`CREATE TABLE IF NOT EXISTS actions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		at TEXT NOT NULL,
		kind TEXT NOT NULL,
		bucket TEXT NOT NULL,
		thread_id TEXT NOT NULL,
		dry_run INTEGER NOT NULL,
		detail TEXT NOT NULL,
		err TEXT NOT NULL
	);`,
	`CREATE INDEX IF NOT EXISTS actions_at ON actions(at);`,
	`CREATE TABLE IF NOT EXISTS resume_intents (
		thread_id TEXT PRIMARY KEY,
		intent TEXT NOT NULL,
		status TEXT NOT NULL,
		updated_at TEXT NOT NULL
	);`,
	`CREATE TABLE IF NOT EXISTS log_positions (
		path TEXT PRIMARY KEY,
		inode INTEGER NOT NULL,
		offset INTEGER NOT NULL
	);`,
	`CREATE TABLE IF NOT EXISTS events_seen (
		event_id TEXT PRIMARY KEY,
		seen_at TEXT NOT NULL
	);`,
	`CREATE TABLE IF NOT EXISTS thread_notices (
		thread_id TEXT NOT NULL,
		bucket TEXT NOT NULL,
		epoch TEXT NOT NULL,
		kind TEXT NOT NULL,
		at TEXT NOT NULL,
		PRIMARY KEY (thread_id, bucket, epoch, kind)
	);`,
	`CREATE TABLE IF NOT EXISTS kv (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL
	);`,
}

// Open opens or creates the database, creating the parent directory with
// user-only permissions.
func Open(path string) (*Store, error) {
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, fmt.Errorf("create state directory: %w", err)
		}
	}
	dsn := path
	if path != ":memory:" {
		dsn = "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open state database: %w", err)
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	if path != ":memory:" {
		_ = os.Chmod(path, 0o600)
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	for _, stmt := range migrations {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("migrate state database: %w", err)
		}
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM schema_version`).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		if _, err := s.db.Exec(`INSERT INTO schema_version(version) VALUES (1)`); err != nil {
			return err
		}
	}
	return nil
}

// LoadBucket returns the stored state for a key; a zero state when none.
func (s *Store) LoadBucket(ctx context.Context, key domain.BucketKey) (domain.BucketState, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT state FROM buckets WHERE key = ?`, key.String()).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.BucketState{}, nil
	}
	if err != nil {
		return domain.BucketState{}, err
	}
	var st domain.BucketState
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		return domain.BucketState{}, fmt.Errorf("decode bucket %s: %w", key, err)
	}
	return st, nil
}

// SaveBucket upserts a bucket state.
func (s *Store) SaveBucket(ctx context.Context, state domain.BucketState) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO buckets(key, state, updated_at) VALUES (?, ?, ?)
		 ON CONFLICT(key) DO UPDATE SET state = excluded.state, updated_at = excluded.updated_at`,
		state.Key.String(), string(raw), time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// ListBuckets returns every stored bucket state.
func (s *Store) ListBuckets(ctx context.Context) ([]domain.BucketState, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT state FROM buckets ORDER BY key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.BucketState
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var st domain.BucketState
		if err := json.Unmarshal([]byte(raw), &st); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// RecordAction appends to the audit log.
func (s *Store) RecordAction(ctx context.Context, a domain.ActionRecord) error {
	at := a.At
	if at.IsZero() {
		at = time.Now()
	}
	dry := 0
	if a.DryRun {
		dry = 1
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO actions(at, kind, bucket, thread_id, dry_run, detail, err) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		at.UTC().Format(time.RFC3339Nano), string(a.Kind), a.Bucket, a.ThreadID, dry, a.Detail, a.Err)
	return err
}

// RecentActions returns the newest audit rows, newest first.
func (s *Store) RecentActions(ctx context.Context, limit int) ([]domain.ActionRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT at, kind, bucket, thread_id, dry_run, detail, err FROM actions ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ActionRecord
	for rows.Next() {
		var (
			at, kind string
			r        domain.ActionRecord
			dry      int
		)
		if err := rows.Scan(&at, &kind, &r.Bucket, &r.ThreadID, &dry, &r.Detail, &r.Err); err != nil {
			return nil, err
		}
		r.At, _ = time.Parse(time.RFC3339Nano, at)
		r.Kind = domain.ActionKind(kind)
		r.DryRun = dry == 1
		out = append(out, r)
	}
	return out, rows.Err()
}

// SaveResumeIntent upserts an intent.
func (s *Store) SaveResumeIntent(ctx context.Context, intent domain.ResumeIntent) error {
	raw, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO resume_intents(thread_id, intent, status, updated_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(thread_id) DO UPDATE SET intent = excluded.intent, status = excluded.status, updated_at = excluded.updated_at`,
		intent.ThreadID, string(raw), string(intent.Status), time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// LoadResumeIntent returns the intent for a thread, or false when none.
func (s *Store) LoadResumeIntent(ctx context.Context, threadID string) (domain.ResumeIntent, bool, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT intent FROM resume_intents WHERE thread_id = ?`, threadID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ResumeIntent{}, false, nil
	}
	if err != nil {
		return domain.ResumeIntent{}, false, err
	}
	var intent domain.ResumeIntent
	if err := json.Unmarshal([]byte(raw), &intent); err != nil {
		return domain.ResumeIntent{}, false, err
	}
	return intent, true, nil
}

// ListResumeIntents returns intents, optionally filtered by status.
func (s *Store) ListResumeIntents(ctx context.Context, statuses ...domain.ResumeStatus) ([]domain.ResumeIntent, error) {
	query := `SELECT intent FROM resume_intents`
	args := []any{}
	if len(statuses) > 0 {
		query += ` WHERE status IN (`
		for i, st := range statuses {
			if i > 0 {
				query += `,`
			}
			query += `?`
			args = append(args, string(st))
		}
		query += `)`
	}
	query += ` ORDER BY updated_at`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ResumeIntent
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var intent domain.ResumeIntent
		if err := json.Unmarshal([]byte(raw), &intent); err != nil {
			return nil, err
		}
		out = append(out, intent)
	}
	return out, rows.Err()
}

// LoadLogPosition returns the stored read position of a log file.
func (s *Store) LoadLogPosition(ctx context.Context, path string) (domain.LogPosition, bool, error) {
	var p domain.LogPosition
	p.Path = path
	err := s.db.QueryRowContext(ctx, `SELECT inode, offset FROM log_positions WHERE path = ?`, path).Scan(&p.Inode, &p.Offset)
	if errors.Is(err, sql.ErrNoRows) {
		return p, false, nil
	}
	if err != nil {
		return p, false, err
	}
	return p, true, nil
}

// SaveLogPosition upserts a read position.
func (s *Store) SaveLogPosition(ctx context.Context, p domain.LogPosition) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO log_positions(path, inode, offset) VALUES (?, ?, ?)
		 ON CONFLICT(path) DO UPDATE SET inode = excluded.inode, offset = excluded.offset`,
		p.Path, p.Inode, p.Offset)
	return err
}

// DeleteLogPosition forgets a file.
func (s *Store) DeleteLogPosition(ctx context.Context, path string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM log_positions WHERE path = ?`, path)
	return err
}

// MarkEventSeen records an event id. It returns false when the id was
// already known.
func (s *Store) MarkEventSeen(ctx context.Context, eventID string, at time.Time) (bool, error) {
	if eventID == "" {
		return true, nil
	}
	res, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO events_seen(event_id, seen_at) VALUES (?, ?)`,
		eventID, at.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// PruneEvents forgets event ids older than the cutoff.
func (s *Store) PruneEvents(ctx context.Context, before time.Time) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM events_seen WHERE seen_at < ?`, before.UTC().Format(time.RFC3339Nano))
	return err
}

// MarkThreadNotice records that a thread received a notice of one kind for a
// bucket epoch. It returns false when the notice was already recorded.
func (s *Store) MarkThreadNotice(ctx context.Context, threadID string, bucket domain.BucketKey, epoch string, kind domain.ActionKind, at time.Time) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO thread_notices(thread_id, bucket, epoch, kind, at) VALUES (?, ?, ?, ?, ?)`,
		threadID, bucket.String(), epoch, string(kind), at.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// ClearThreadNotices forgets notices for a bucket, used when a bucket rearms.
func (s *Store) ClearThreadNotices(ctx context.Context, bucket domain.BucketKey) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM thread_notices WHERE bucket = ?`, bucket.String())
	return err
}

// GetKV reads a miscellaneous value.
func (s *Store) GetKV(ctx context.Context, key string) (string, bool, error) {
	var v string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM kv WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return v, err == nil, err
}

// SetKV writes a miscellaneous value.
func (s *Store) SetKV(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO kv(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}
