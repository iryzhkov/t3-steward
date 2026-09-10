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

	"github.com/iryzhkov/t3-steward/internal/archive"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
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
	`CREATE TABLE IF NOT EXISTS observations (
		bucket TEXT NOT NULL,
		observed_at TEXT NOT NULL,
		used_percent REAL NOT NULL,
		resets_at TEXT NOT NULL,
		event_id TEXT NOT NULL,
		thread_id TEXT NOT NULL,
		model TEXT NOT NULL,
		PRIMARY KEY (bucket, event_id)
	);`,
	`CREATE INDEX IF NOT EXISTS observations_at ON observations(observed_at);`,
	`CREATE TABLE IF NOT EXISTS usage_samples (
		event_id TEXT PRIMARY KEY,
		provider TEXT NOT NULL,
		thread_id TEXT NOT NULL,
		model TEXT NOT NULL,
		observed_at TEXT NOT NULL,
		input_tokens INTEGER NOT NULL,
		cache_write_tokens INTEGER NOT NULL,
		cache_read_tokens INTEGER NOT NULL,
		output_tokens INTEGER NOT NULL,
		cost_usd REAL NOT NULL
	);`,
	`CREATE INDEX IF NOT EXISTS usage_samples_at ON usage_samples(observed_at);`,
	`CREATE TABLE IF NOT EXISTS dispatched_threads (
		thread_id TEXT PRIMARY KEY,
		task_id TEXT NOT NULL,
		project TEXT NOT NULL,
		dispatched_at TEXT NOT NULL
	);`,
	`CREATE TABLE IF NOT EXISTS backlog_tasks (
		id TEXT PRIMARY KEY,
		state TEXT NOT NULL,
		status TEXT NOT NULL,
		updated_at TEXT NOT NULL
	);`,
	`CREATE TABLE IF NOT EXISTS waits (
		id TEXT PRIMARY KEY,
		thread_id TEXT NOT NULL,
		status TEXT NOT NULL,
		state TEXT NOT NULL,
		updated_at TEXT NOT NULL
	);`,
	`CREATE TABLE IF NOT EXISTS archives (
		thread_id TEXT PRIMARY KEY,
		record TEXT NOT NULL,
		archived_at TEXT NOT NULL
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
	for _, col := range []struct{ name, ddl string }{
		{"kind", "ALTER TABLE usage_samples ADD COLUMN kind TEXT NOT NULL DEFAULT ''"},
		{"cumulative_tokens", "ALTER TABLE usage_samples ADD COLUMN cumulative_tokens INTEGER NOT NULL DEFAULT 0"},
	} {
		if has, err := s.hasColumn("usage_samples", col.name); err != nil {
			return err
		} else if !has {
			if _, err := s.db.Exec(col.ddl); err != nil {
				return fmt.Errorf("migrate usage_samples.%s: %w", col.name, err)
			}
		}
	}

	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM schema_version`).Scan(&count); err != nil {
		return fmt.Errorf("read schema version count: %w", err)
	}
	if count == 0 {
		if _, err := s.db.Exec(`INSERT INTO schema_version(version) VALUES (1)`); err != nil {
			return fmt.Errorf("record schema version 1: %w", err)
		}
	}

	var version int
	if err := s.db.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version > currentSchemaVersion {
		return fmt.Errorf("state database schema version %d is newer than supported version %d", version, currentSchemaVersion)
	}
	if version < 2 {
		if err := s.applyVersionedMigration(2, coordinatorMigrationV2); err != nil {
			return err
		}
		version = 2
	}
	if version < 3 {
		if err := s.applyVersionedMigration(3, coordinatorMigrationV3); err != nil {
			return err
		}
		version = 3
	}
	if version < 4 {
		if err := s.applyVersionedMigration(4, coordinatorMigrationV4); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) applyVersionedMigration(version int, ddl string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin schema migration %d: %w", version, err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(ddl); err != nil {
		return fmt.Errorf("apply schema migration %d: %w", version, err)
	}
	if _, err := tx.Exec(`INSERT INTO schema_version(version) VALUES (?)`, version); err != nil {
		return fmt.Errorf("record schema version %d: %w", version, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schema migration %d: %w", version, err)
	}
	return nil
}

// hasColumn reports whether a table has a column.
func (s *Store) hasColumn(table, column string) (bool, error) {
	rows, err := s.db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid     int
			name    string
			ctype   string
			notnull int
			dflt    any
			pk      int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
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

// RecordObservation stores one accepted quota reading. Duplicate event ids
// per bucket are ignored.
func (s *Store) RecordObservation(ctx context.Context, o domain.Observation) error {
	resets := ""
	if o.ResetsAt != nil {
		resets = o.ResetsAt.UTC().Format(time.RFC3339)
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO observations(bucket, observed_at, used_percent, resets_at, event_id, thread_id, model)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		o.Key.String(), o.ObservedAt.UTC().Format(time.RFC3339Nano), o.UsedPercent, resets, o.EventID, o.ThreadID, o.Model)
	return err
}

// Observations returns readings in [from, to), oldest first.
func (s *Store) Observations(ctx context.Context, from, to time.Time) ([]domain.Observation, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT bucket, observed_at, used_percent, resets_at, event_id, thread_id, model FROM observations
		 WHERE observed_at >= ? AND observed_at < ? ORDER BY observed_at`,
		from.UTC().Format(time.RFC3339Nano), to.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Observation
	for rows.Next() {
		var (
			bucket, at, resets string
			o                  domain.Observation
		)
		if err := rows.Scan(&bucket, &at, &o.UsedPercent, &resets, &o.EventID, &o.ThreadID, &o.Model); err != nil {
			return nil, err
		}
		o.Key = domain.ParseBucketKey(bucket)
		o.ObservedAt, _ = time.Parse(time.RFC3339Nano, at)
		if resets != "" {
			if t, err := time.Parse(time.RFC3339, resets); err == nil {
				o.ResetsAt = &t
			}
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// RecordUsage stores one token usage sample. Duplicates are ignored.
func (s *Store) RecordUsage(ctx context.Context, u domain.UsageSample) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO usage_samples(event_id, provider, thread_id, model, observed_at, input_tokens, cache_write_tokens, cache_read_tokens, output_tokens, cost_usd, kind, cumulative_tokens)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		u.SourceEventID, u.ProviderInstanceID, u.ThreadID, u.Model, u.ObservedAt.UTC().Format(time.RFC3339Nano),
		u.InputTokens, u.CacheWriteTokens, u.CacheReadTokens, u.OutputTokens, u.CostUSD, u.Kind, u.CumulativeTokens)
	return err
}

// UsageSamples returns samples in [from, to), oldest first.
func (s *Store) UsageSamples(ctx context.Context, from, to time.Time) ([]domain.UsageSample, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT event_id, provider, thread_id, model, observed_at, input_tokens, cache_write_tokens, cache_read_tokens, output_tokens, cost_usd, kind, cumulative_tokens
		 FROM usage_samples WHERE observed_at >= ? AND observed_at < ? ORDER BY observed_at`,
		from.UTC().Format(time.RFC3339Nano), to.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.UsageSample
	for rows.Next() {
		var (
			at string
			u  domain.UsageSample
		)
		if err := rows.Scan(&u.SourceEventID, &u.ProviderInstanceID, &u.ThreadID, &u.Model, &at, &u.InputTokens, &u.CacheWriteTokens, &u.CacheReadTokens, &u.OutputTokens, &u.CostUSD, &u.Kind, &u.CumulativeTokens); err != nil {
			return nil, err
		}
		u.ObservedAt, _ = time.Parse(time.RFC3339Nano, at)
		out = append(out, u)
	}
	return out, rows.Err()
}

// PruneHistory deletes observations and usage samples older than the cutoff.
func (s *Store) PruneHistory(ctx context.Context, before time.Time) error {
	cutoff := before.UTC().Format(time.RFC3339Nano)
	if _, err := s.db.ExecContext(ctx, `DELETE FROM observations WHERE observed_at < ?`, cutoff); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM usage_samples WHERE observed_at < ?`, cutoff)
	return err
}

// RegisterDispatchedThread records that a thread was started by the
// watchdog (backlog runner or scheduled job), so that it is not counted as
// interactive use.
func (s *Store) RegisterDispatchedThread(ctx context.Context, threadID, taskID, project string, at time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO dispatched_threads(thread_id, task_id, project, dispatched_at) VALUES (?, ?, ?, ?)`,
		threadID, taskID, project, at.UTC().Format(time.RFC3339Nano))
	return err
}

// DispatchedThreads returns the ids of every thread the watchdog started.
func (s *Store) DispatchedThreads(ctx context.Context) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT thread_id, task_id FROM dispatched_threads`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var id, task string
		if err := rows.Scan(&id, &task); err != nil {
			return nil, err
		}
		out[id] = task
	}
	return out, rows.Err()
}

// SaveTaskState upserts a backlog task's state, stored as JSON with the
// status duplicated in a column for listing.
func (s *Store) SaveTaskState(ctx context.Context, id, status string, state any) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO backlog_tasks(id, state, status, updated_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET state = excluded.state, status = excluded.status, updated_at = excluded.updated_at`,
		id, string(raw), status, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// LoadTaskStates returns every stored task state as raw JSON keyed by id.
func (s *Store) LoadTaskStates(ctx context.Context) (map[string]json.RawMessage, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, state FROM backlog_tasks`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]json.RawMessage{}
	for rows.Next() {
		var id, raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, err
		}
		out[id] = json.RawMessage(raw)
	}
	return out, rows.Err()
}

// SaveWait upserts a wait, stored as JSON with status and thread columns
// for listing.
func (s *Store) SaveWait(ctx context.Context, w wait.Wait) error {
	raw, err := json.Marshal(w)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO waits(id, thread_id, status, state, updated_at) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET thread_id = excluded.thread_id, status = excluded.status, state = excluded.state, updated_at = excluded.updated_at`,
		w.ID, w.ThreadID, string(w.Status), string(raw), time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// ListWaits returns waits, optionally for one thread, oldest first.
func (s *Store) ListWaits(ctx context.Context, threadID string) ([]wait.Wait, error) {
	query := `SELECT state FROM waits`
	var args []any
	if threadID != "" {
		query += ` WHERE thread_id = ?`
		args = append(args, threadID)
	}
	query += ` ORDER BY updated_at`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []wait.Wait
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var w wait.Wait
		if err := json.Unmarshal([]byte(raw), &w); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// SaveArchive upserts an archive record.
func (s *Store) SaveArchive(ctx context.Context, r archive.Record) error {
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO archives(thread_id, record, archived_at) VALUES (?, ?, ?)
		 ON CONFLICT(thread_id) DO UPDATE SET record = excluded.record, archived_at = excluded.archived_at`,
		r.ThreadID, string(raw), r.ArchivedAt.UTC().Format(time.RFC3339Nano))
	return err
}

// ListArchives returns every archive record, oldest first.
func (s *Store) ListArchives(ctx context.Context) ([]archive.Record, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT record FROM archives ORDER BY archived_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []archive.Record
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var r archive.Record
		if err := json.Unmarshal([]byte(raw), &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// BusyThreads lists threads the steward still has business with.
func (s *Store) BusyThreads(ctx context.Context) (map[string]string, error) {
	out := map[string]string{}
	intents, err := s.ListResumeIntents(ctx, domain.ResumePending, domain.ResumeEligible, domain.ResumeResuming)
	if err != nil {
		return nil, err
	}
	for _, i := range intents {
		out[i.ThreadID] = "resume intent " + string(i.Status)
	}
	tasks, err := s.LoadTaskStates(ctx)
	if err != nil {
		return nil, err
	}
	for id, raw := range tasks {
		var st struct {
			Status   string `json:"status"`
			ThreadID string `json:"threadId"`
		}
		if json.Unmarshal(raw, &st) == nil && st.ThreadID != "" && (st.Status == "running" || st.Status == "pending" || st.Status == "needs-input") {
			out[st.ThreadID] = "backlog task " + id + " is " + st.Status
		}
	}
	waits, err := s.ListWaits(ctx, "")
	if err != nil {
		return nil, err
	}
	for _, w := range waits {
		if w.Status == wait.StatusWaiting || w.Settled() {
			out[w.ThreadID] = "wait " + w.ID + " is " + string(w.Status)
		}
	}
	return out, nil
}
