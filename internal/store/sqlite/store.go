// Package sqlite persists bucket states, resume intents, log positions and
// the audit log in one SQLite database.
package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	modernsqlite "modernc.org/sqlite"

	"github.com/iryzhkov/t3-steward/internal/archive"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// Store is the SQLite-backed state store.
type Store struct {
	db               *sql.DB
	now              func() time.Time
	path             string
	ownerLock        *os.File
	usageRecordHook  usageMutationHook
	pruneHistoryHook usageMutationHook
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

// Open opens an existing database without creating directories, changing
// journal mode, or applying schema migrations. Query and admin clients use it
// so merely inspecting coordinator state cannot alter the schema.
func Open(path string) (*Store, error) {
	return open(path, false)
}

// OpenReadOnly opens an immutable read-only database without creating SQLite
// journal sidecars. Snapshot verification uses it against protected copies.
func OpenReadOnly(path string) (*Store, error) {
	if path == "" || path == ":memory:" {
		return nil, errors.New("read-only state database must be a file")
	}
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("open existing state database: %w", err)
	}
	dsn := sqliteFileURL(path) + "?mode=ro&immutable=1&_pragma=query_only(1)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open read-only state database: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("open read-only state database: %w", err)
	}
	return &Store{db: db, now: time.Now, path: path}, nil
}

func open(path string, create bool) (*Store, error) {
	if path != ":memory:" {
		if create {
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				return nil, fmt.Errorf("create state directory: %w", err)
			}
		} else if _, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("open existing state database: %w", err)
		}
	}
	dsn := path
	if path != ":memory:" {
		mode := "rw"
		if create {
			mode = "rwc"
		}
		dsn = sqliteFileURL(path) + "?mode=" + mode + "&_pragma=busy_timeout(5000)"
		if create {
			dsn += "&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
		}
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open state database: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("open state database: %w", err)
	}
	return &Store{db: db, now: time.Now, path: path}, nil
}

func sqliteFileURL(path string) string {
	return (&url.URL{Scheme: "file", Path: path}).String()
}

// OpenMigrated opens or creates a database and explicitly applies every
// supported migration. Only coordinator startup and maintenance paths should
// use it.
func OpenMigrated(path string) (*Store, error) {
	return openMigrated(path, nil)
}

// OpenMigratedInstrumented opens a migrated store through a connector wrapper.
// It exists for bounded diagnostics such as statement counting and timing; the
// normal coordinator path continues to use OpenMigrated and database/sql's
// registered sqlite driver directly.
func OpenMigratedInstrumented(path string, instrument func(driver.Connector) driver.Connector) (*Store, error) {
	if instrument == nil {
		return nil, errors.New("sqlite connector instrumenter is required")
	}
	return openMigrated(path, instrument)
}

func openMigrated(path string, instrument func(driver.Connector) driver.Connector) (*Store, error) {
	var store *Store
	var err error
	if instrument == nil {
		store, err = open(path, true)
	} else {
		store, err = openInstrumented(path, instrument)
	}
	if err != nil {
		return nil, err
	}
	if err := store.Migrate(); err != nil {
		store.Close()
		return nil, err
	}
	if path != ":memory:" {
		_ = os.Chmod(path, 0o600)
	}
	return store, nil
}

func openInstrumented(path string, instrument func(driver.Connector) driver.Connector) (*Store, error) {
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, fmt.Errorf("create state directory: %w", err)
		}
	}
	dsn := path
	if path != ":memory:" {
		dsn = sqliteFileURL(path) + "?mode=rwc&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	}
	connector, err := modernsqlite.NewConnector(dsn)
	if err != nil {
		return nil, fmt.Errorf("open state database: %w", err)
	}
	db := sql.OpenDB(instrument(connector))
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("open state database: %w", err)
	}
	return &Store{db: db, now: time.Now, path: path}, nil
}

// Close releases coordinator ownership, when held, and closes the database.
func (s *Store) Close() error {
	var releaseErr error
	if s.ownerLock != nil {
		releaseErr = releaseCoordinatorLock(s.ownerLock)
		s.ownerLock = nil
	}
	if err := s.db.Close(); err != nil {
		return err
	}
	return releaseErr
}

// SetClock replaces the wall clock used for time-sensitive transactional fences.
func (s *Store) SetClock(now func() time.Time) {
	if now == nil {
		s.now = time.Now
		return
	}
	s.now = now
}

// SchemaVersion returns the recorded schema version without changing it.
func (s *Store) SchemaVersion(ctx context.Context) (int, error) {
	var version int
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(version) FROM schema_version`).Scan(&version); err != nil {
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	return version, nil
}

// IntegrityCheck performs SQLite's complete read-only consistency check.
func (s *Store) IntegrityCheck(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA integrity_check`)
	if err != nil {
		return fmt.Errorf("run sqlite integrity check: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return fmt.Errorf("read sqlite integrity check: %w", err)
		}
		if result != "ok" {
			return fmt.Errorf("sqlite integrity check failed: %s", result)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate sqlite integrity check: %w", err)
	}
	return nil
}

// Migrate explicitly advances the database through every supported schema
// version. It is a coordinator-owned lifecycle operation.
func (s *Store) Migrate() error {
	return s.migrateThrough(currentSchemaVersion)
}

// migrateThrough applies every migration up to and including target and stops
// there. Production always passes currentSchemaVersion; a compatibility test
// passes an older version to build a database exactly as a previous release
// left it, so that the forward migration under test runs against real state
// rather than a reconstruction of it.
//
// Refusing a database newer than this binary is judged against
// currentSchemaVersion and not against target, because that refusal is a
// statement about what this build can understand at all.
func (s *Store) migrateThrough(target int) error {
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
	for _, migration := range versionedMigrations {
		if version >= migration.version || migration.version > target {
			continue
		}
		if err := s.applyVersionedMigration(migration.version, migration.ddl); err != nil {
			return err
		}
		version = migration.version
	}
	return s.backfillGraphHistory(context.Background())
}

// versionedMigrations is the ordered list of schema versions above 1 and the
// DDL that reaches each one. Migrate walks it in order and stops at
// currentSchemaVersion, which is the newest entry this binary knows about.
//
// The list is a package variable rather than a literal inside Migrate so that a
// compatibility test can build a database at an older version and then migrate
// it forward through the same code the coordinator runs.
var versionedMigrations = []struct {
	version int
	ddl     string
}{
	{2, coordinatorMigrationV2},
	{3, coordinatorMigrationV3},
	{4, coordinatorMigrationV4},
	{5, coordinatorMigrationV5},
	{6, coordinatorMigrationV6},
	{7, coordinatorMigrationV7},
	{8, coordinatorMigrationV8},
	{9, coordinatorMigrationV9},
	{10, coordinatorMigrationV10},
	{11, coordinatorMigrationV11},
	{12, coordinatorMigrationV12},
	{13, coordinatorMigrationV13},
	{14, coordinatorMigrationV14},
	{15, coordinatorMigrationV15},
	{16, coordinatorMigrationV16},
	{17, coordinatorMigrationV17},
	{18, coordinatorMigrationV18},
	{19, coordinatorMigrationV19},
	{20, coordinatorMigrationV20},
	{21, coordinatorMigrationV21},
	{22, coordinatorMigrationV22},
	{23, coordinatorMigrationV23},
	{24, coordinatorMigrationV24},
	{25, coordinatorMigrationV25},
	{26, coordinatorMigrationV26},
	{27, coordinatorMigrationV27},
	{28, coordinatorMigrationV28},
	{29, coordinatorMigrationV29},
}

func (s *Store) applyVersionedMigration(version int, ddl string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin schema migration %d: %w", version, err)
	}
	defer tx.Rollback()
	if version == 29 {
		if err := applyCoordinatorMigrationV29(tx); err != nil {
			return fmt.Errorf("apply schema migration %d: %w", version, err)
		}
	} else if _, err := tx.Exec(ddl); err != nil {
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

// ThreadNotices returns the threads that received a notice of one kind for a
// bucket epoch, each with the time recorded for it.
func (s *Store) ThreadNotices(ctx context.Context, bucket domain.BucketKey, epoch string, kind domain.ActionKind) (map[string]time.Time, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT thread_id, at FROM thread_notices WHERE bucket = ? AND epoch = ? AND kind = ?`,
		bucket.String(), epoch, string(kind))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]time.Time{}
	for rows.Next() {
		var threadID, at string
		if err := rows.Scan(&threadID, &at); err != nil {
			return nil, err
		}
		parsed, err := time.Parse(time.RFC3339Nano, at)
		if err != nil {
			return nil, fmt.Errorf("decode notice time for %s: %w", threadID, err)
		}
		out[threadID] = parsed
	}
	return out, rows.Err()
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

const (
	coordinatorUsageCursorKeyName    = "coordinator.usage-cursor-key.v1"
	coordinatorUsageCursorMarkerName = "coordinator.usage-cursor-key.initialized.v1"
)

// CoordinatorUsageCursorKey loads the coordinator-local cursor authentication
// key, creating it transactionally on first use. The key never crosses a
// protocol boundary; the encoded value remains part of coordinator DB backups.
func (s *Store) CoordinatorUsageCursorKey(ctx context.Context) ([]byte, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var encoded, marker string
	keyErr := tx.QueryRowContext(ctx, `SELECT value FROM kv WHERE key = ?`, coordinatorUsageCursorKeyName).Scan(&encoded)
	markerErr := tx.QueryRowContext(ctx, `SELECT value FROM kv WHERE key = ?`, coordinatorUsageCursorMarkerName).Scan(&marker)
	switch {
	case errors.Is(keyErr, sql.ErrNoRows) && errors.Is(markerErr, sql.ErrNoRows):
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, fmt.Errorf("generate coordinator usage cursor key: %w", err)
		}
		encoded = base64.RawURLEncoding.EncodeToString(key)
		if _, err := tx.ExecContext(ctx, `INSERT INTO kv(key, value) VALUES (?, ?), (?, ?)`,
			coordinatorUsageCursorKeyName, encoded, coordinatorUsageCursorMarkerName, "1"); err != nil {
			return nil, fmt.Errorf("persist coordinator usage cursor key: %w", err)
		}
	case keyErr != nil || markerErr != nil || marker != "1":
		return nil, errors.New("coordinator usage cursor key is missing or corrupt")
	}
	key, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(key) != 32 {
		return nil, errors.New("coordinator usage cursor key is corrupt")
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit coordinator usage cursor key: %w", err)
	}
	return key, nil
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

// RecordUsage stores one token usage sample. Duplicates are ignored per worker.
func (s *Store) RecordUsage(ctx context.Context, u domain.UsageSample) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := recordUsageWithHook(ctx, tx, u, s.usageRecordHook); err != nil {
		return err
	}
	return tx.Commit()
}

// UsageSamples returns samples in [from, to), oldest first, joined to the
// immutable dispatch binding. Rows without one are explicitly unattributed.
func (s *Store) UsageSamples(ctx context.Context, from, to time.Time) ([]domain.UsageSample, error) {
	rows, err := s.db.QueryContext(ctx, attributedUsageSelect+
		` WHERE u.observed_at >= ? AND u.observed_at < ?
		   ORDER BY u.observed_at, u.event_id`,
		from.UTC().Format(time.RFC3339Nano), to.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	return scanAttributedUsage(rows)
}

// PruneHistory atomically deletes observations, usage samples, and delivery
// bookkeeping older than the cutoff. Receipt deletion is joined to the exact
// worker-scoped sample identity so a later event-id reuse cannot inherit an ack.
func (s *Store) PruneHistory(ctx context.Context, before time.Time) error {
	cutoff := before.UTC().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM observations WHERE observed_at < ?`, cutoff); err != nil {
		return err
	}
	if err := usageMutationCheckpoint(s.pruneHistoryHook, "observations-pruned"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM coordinator_worker_usage_receipts
		WHERE EXISTS (SELECT 1 FROM usage_samples AS u
			WHERE u.worker_id = coordinator_worker_usage_receipts.worker_id
			AND u.event_id = coordinator_worker_usage_receipts.event_id
			AND u.observed_at < ?)`, cutoff); err != nil {
		return err
	}
	if err := usageMutationCheckpoint(s.pruneHistoryHook, "receipts-pruned"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM worker_usage_forwarded
		WHERE EXISTS (SELECT 1 FROM usage_samples AS u
			WHERE u.worker_id = worker_usage_forwarded.worker_id
			AND u.event_id = worker_usage_forwarded.event_id
			AND u.observed_at < ?)`, cutoff); err != nil {
		return err
	}
	if err := usageMutationCheckpoint(s.pruneHistoryHook, "forwarded-pruned"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM usage_samples WHERE observed_at < ?`, cutoff); err != nil {
		return err
	}
	if err := usageMutationCheckpoint(s.pruneHistoryHook, "usage-pruned"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM usage_diagnostic_overflow
		WHERE NOT EXISTS (SELECT 1 FROM usage_samples AS u
			WHERE u.worker_id = usage_diagnostic_overflow.worker_id AND u.diagnostic_code = 'overflow')`); err != nil {
		return err
	}
	if err := usageMutationCheckpoint(s.pruneHistoryHook, "overflow-pruned"); err != nil {
		return err
	}
	return tx.Commit()
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
		switch {
		case w.Status == wait.StatusWaiting:
			// The thread is parked on this check and has to be here for it.
			out[w.ThreadID] = "wait " + w.ID + " is " + string(w.Status)
		case w.Settled() && w.TaskWaitID == "":
			// An interactive outcome with no wake delivered yet: this host's
			// runner still owes the thread a message. A delivered one is
			// StatusWoken and holds nothing.
			out[w.ThreadID] = "wait " + w.ID + " is " + string(w.Status)
		}
		// A settled task-bound row holds nothing. Its outcome belongs to the
		// coordinator's own wait record, which resumes the attempt and delivers
		// the wake; this row is the check that produced it and is never woken,
		// so treating it as custody pinned every thread that ever parked on a
		// task-bound wait for the life of the database. Those threads were then
		// never hidden by the UI archive and never bundled into cold storage,
		// which is what filled the T3 session list with finished steward runs.
		// A task whose attempt is genuinely still in flight is held by the
		// worker journal and by its coordinator records instead.
	}
	native, err := s.ListNodeWaits(ctx)
	if err != nil {
		return nil, err
	}
	for _, w := range native {
		if w.Delivery != "delivered" && w.Delivery != "cancelled" {
			out[w.Request.ThreadID] = "native wait " + w.Request.ID + " is " + w.Delivery
		}
	}
	return out, nil
}
