package quotatelemetry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	_ "modernc.org/sqlite"
)

// ErrNoStore is the read path's refusal when the store file does not exist.
// The read path never creates it.
var ErrNoStore = errors.New("no quota telemetry store")

// StorePath is where the recorder keeps its file: a quota-telemetry
// directory beside the coordinator's state database.
func StorePath(statePath string) (string, error) {
	if statePath == "" || statePath == ":memory:" {
		return "", fmt.Errorf("quota telemetry needs a state database file, not %q", statePath)
	}
	return filepath.Join(filepath.Dir(statePath), "quota-telemetry", "recorder.sqlite"), nil
}

// Store is the recorder's own SQLite file: a meta table and one append-only
// events table. Nothing else depends on it, and it is not part of coordinator
// backups; a lost file is recreated with a new coverage start.
type Store struct {
	db       *sql.DB
	path     string
	readOnly bool
}

const storeSchema = `
CREATE TABLE IF NOT EXISTS meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
	seq INTEGER PRIMARY KEY AUTOINCREMENT,
	event_id TEXT NOT NULL UNIQUE,
	kind TEXT NOT NULL,
	at_ns INTEGER NOT NULL,
	quota_pool_id TEXT NOT NULL,
	route TEXT NOT NULL,
	record TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_at ON events(at_ns);
CREATE INDEX IF NOT EXISTS events_kind_at ON events(kind, at_ns);
CREATE INDEX IF NOT EXISTS events_kind_route ON events(kind, route);
`

// Meta keys.
const (
	metaSchemaVersion   = "schemaVersion"
	metaAuditWatermark  = "auditWatermark"
	metaOpenCursor      = "openCursor"
	metaCoverageFrom    = "coverageFrom"
	metaTicks           = "ticks"
	metaFailures        = "failures"
	metaSkippedReadings = "skippedReadings"
	metaSkippedChecks   = "skippedChecks"
	metaLastError       = "lastError"
	metaLastErrorAt     = "lastErrorAt"
	metaLastSuccessAt   = "lastSuccessAt"
	metaSkippedRecords  = "skippedRecords"
	metaFailedTicks     = "failedTicks"
	metaFailedSince     = "failedSince"
)

// OpenStore opens or creates the recorder's store: the directory 0700, the
// file 0600, write-ahead logging. Only the recorder calls it. A file written
// by a newer schema is refused rather than appended to.
func OpenStore(path string) (*Store, error) {
	if path == "" || path == ":memory:" {
		return nil, fmt.Errorf("quota telemetry store must be a file, not %q", path)
	}
	dir := filepath.Dir(path)
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create quota telemetry directory: %w", err)
		}
		if info, err = os.Lstat(dir); err != nil {
			return nil, fmt.Errorf("inspect quota telemetry directory: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("inspect quota telemetry directory: %w", err)
	}
	// A symlink in place of the directory or the file is refused rather than
	// followed, so the recorder never writes or re-permissions anything else.
	if !info.IsDir() {
		return nil, fmt.Errorf("quota telemetry directory %s is not a directory", dir)
	}
	// Access for others is removed; the owner's own bits are left alone, so
	// a directory an operator made read-only stays read-only.
	if info.Mode().Perm()&0o077 != 0 {
		if err := os.Chmod(dir, info.Mode().Perm()&0o700); err != nil {
			return nil, fmt.Errorf("protect quota telemetry directory: %w", err)
		}
	}
	if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
		return nil, fmt.Errorf("quota telemetry store %s is not a regular file", path)
	}
	// Create the file with its final mode before SQLite opens it, so it is
	// never readable by others, whatever the umask; SQLite gives the WAL and
	// shared-memory files the database file's mode.
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|noFollow, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create quota telemetry store: %w", err)
	}
	file.Close()
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, fmt.Errorf("protect quota telemetry store: %w", err)
	}
	dsn := fileURL(path) + "?mode=rw&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open quota telemetry store: %w", err)
	}
	db.SetMaxOpenConns(1)
	store := &Store{db: db, path: path}
	if err := store.initialize(); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) initialize() error {
	ctx := context.Background()
	if _, err := s.db.ExecContext(ctx, storeSchema); err != nil {
		return fmt.Errorf("initialize quota telemetry store %s: %w", s.path, err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO meta(key, value) VALUES (?, ?)`,
		metaSchemaVersion, strconv.Itoa(SchemaVersion)); err != nil {
		return fmt.Errorf("initialize quota telemetry store %s: %w", s.path, err)
	}
	return s.checkSchema(ctx)
}

// OpenReader opens an existing store read-only for the read command. It never
// creates the file or its directory, and the connection cannot write.
func OpenReader(path string) (*Store, error) {
	if info, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w at %s", ErrNoStore, path)
	} else if err != nil {
		return nil, fmt.Errorf("open quota telemetry store: %w", err)
	} else if info.Size() == 0 {
		return nil, fmt.Errorf("quota telemetry store %s is empty: the recorder has created it and not yet written it; try again in a minute", path)
	}
	dsn := fileURL(path) + "?mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(5000)"
	if !exists(path+"-wal") && !exists(path+"-shm") {
		// The recorder is not running and its last close checkpointed
		// everything into the file. Opening a WAL database read-only would
		// still create the two sidecar files, so the file is read as
		// immutable instead, which creates nothing.
		dsn += "&immutable=1"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open quota telemetry store: %w", err)
	}
	db.SetMaxOpenConns(1)
	store := &Store{db: db, path: path, readOnly: true}
	if err := store.checkSchema(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func exists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

func (s *Store) checkSchema(ctx context.Context) error {
	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, metaSchemaVersion).Scan(&raw); err != nil {
		return fmt.Errorf("%s is not a quota telemetry store: %w", s.path, err)
	}
	version, err := strconv.Atoi(raw)
	if err != nil {
		return fmt.Errorf("quota telemetry store %s has an unreadable schema version %q", s.path, raw)
	}
	if version > SchemaVersion {
		return fmt.Errorf("quota telemetry store %s has schema version %d; this build reads schema version %d, so use a newer t3-steward",
			s.path, version, SchemaVersion)
	}
	return nil
}

func fileURL(path string) string {
	return (&url.URL{Scheme: "file", Path: path}).String()
}

// Close releases the connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// Path is the store file.
func (s *Store) Path() string { return s.path }

// Append stores events, each at most once by event id.
func (s *Store) Append(ctx context.Context, events []Event) error {
	return s.commit(ctx, events, nil, nil, nil)
}

// commit writes events and meta values in one transaction. An event is stored
// at most once by event id; one of replacements overwrites the stored event
// with its id. failure, when set, aborts the transaction before the commit, as
// a crash would.
func (s *Store) commit(ctx context.Context, events, replacements []Event, meta map[string]string, failure error) error {
	if s.readOnly {
		return errors.New("quota telemetry store is open read-only")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin quota telemetry write: %w", err)
	}
	defer tx.Rollback()
	const insert = `INSERT OR IGNORE INTO events(event_id, kind, at_ns, quota_pool_id, route, record) VALUES (?, ?, ?, ?, ?, ?)`
	const replace = `INSERT INTO events(event_id, kind, at_ns, quota_pool_id, route, record) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(event_id) DO UPDATE SET kind = excluded.kind, at_ns = excluded.at_ns,
			quota_pool_id = excluded.quota_pool_id, route = excluded.route, record = excluded.record`
	for index, event := range append(events[:len(events):len(events)], replacements...) {
		raw, err := encodeEvent(event)
		if err != nil {
			return err
		}
		pool, route := event.QuotaPoolID, ""
		switch {
		case event.Work != nil:
			route = event.Work.Route.Name()
		case event.Reading != nil:
			route = event.Reading.BucketKey
		}
		statement := insert
		if index >= len(events) {
			statement = replace
		}
		if _, err := tx.ExecContext(ctx, statement, event.EventID, event.Kind, event.At.UnixNano(), pool, route, raw); err != nil {
			return fmt.Errorf("append quota telemetry event %q: %w", event.EventID, err)
		}
	}
	for key, value := range meta {
		if _, err := tx.ExecContext(ctx, `INSERT INTO meta(key, value) VALUES (?, ?)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value); err != nil {
			return fmt.Errorf("write quota telemetry meta %q: %w", key, err)
		}
	}
	if failure != nil {
		return failure
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit quota telemetry write: %w", err)
	}
	return nil
}

// encodeEvent marshals one event, cutting long strings until the record fits
// in MaxRecordBytes.
func encodeEvent(event Event) ([]byte, error) {
	if event.EventID == "" || event.Kind == "" || event.At.IsZero() {
		return nil, fmt.Errorf("quota telemetry event %q needs an id, a kind and a time", event.EventID)
	}
	event.SchemaVersion = SchemaVersion
	event.Deltas = nil
	for _, limit := range []int{-1, 512, 128, 32} {
		if limit > 0 {
			event = cutStrings(event, limit)
		}
		raw, err := json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("encode quota telemetry event %q: %w", event.EventID, err)
		}
		if len(raw) <= MaxRecordBytes {
			return raw, nil
		}
	}
	minimal := Event{SchemaVersion: SchemaVersion, EventID: event.EventID, Kind: event.Kind, At: event.At,
		RecordedAt: event.RecordedAt, QuotaPoolID: truncateUTF8(event.QuotaPoolID, 32), Truncated: true}
	return json.Marshal(minimal)
}

// cutStrings copies the event with every free-length string cut to limit.
func cutStrings(event Event, limit int) Event {
	cut := func(value *string) {
		if len(*value) > limit {
			*value = truncateUTF8(*value, limit)
			event.Truncated = true
		}
	}
	if event.Recorder != nil {
		note := *event.Recorder
		cut(&note.LastError)
		cut(&note.Reason)
		event.Recorder = &note
	}
	if event.Check != nil {
		check := *event.Check
		cut(&check.Command)
		event.Check = &check
	}
	if event.Reading != nil {
		reading := *event.Reading
		for _, field := range []*string{&reading.Source, &reading.BucketKey, &reading.ProviderInstanceID, &reading.AccountID,
			&reading.LimitID, &reading.Window, &reading.Phase, &reading.Epoch, &reading.LimitName} {
			cut(field)
		}
		event.Reading = &reading
	}
	if event.Work != nil {
		work := *event.Work
		for _, field := range []*string{&work.RunID, &work.TaskID, &work.TaskName, &work.AttemptID, &work.AssignmentID,
			&work.WorkerID, &work.Project, &work.Route.ProviderInstanceID, &work.Route.Model, &work.Route.QuotaPoolID,
			&work.ExecutionRole, &work.Outcome, &work.TaskType.Provenance.TaskName, &work.TaskType.Provenance.Class,
			&work.TaskType.Provenance.SupervisionActivationID, &work.TaskType.Provenance.ExecutionRole} {
			cut(field)
		}
		if work.Route.Effort != nil {
			effort := *work.Route.Effort
			cut(&effort)
			work.Route.Effort = &effort
		}
		event.Work = &work
	}
	return event
}

// truncateUTF8 cuts s to at most limit bytes without splitting a character.
func truncateUTF8(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	for limit > 0 && !utf8.RuneStart(s[limit]) {
		limit--
	}
	return s[:limit]
}

// Meta is the store's persistent state and counters.
type Meta struct {
	SchemaVersion   int
	AuditWatermark  *int64
	OpenCursor      string
	CoverageFrom    *time.Time
	Ticks           int64
	Failures        int64
	SkippedReadings int64
	SkippedChecks   int64
	SkippedRecords  int64
	LastError       string
	LastErrorAt     *time.Time
	LastSuccessAt   *time.Time
	// FailedTicks and FailedSince describe a failed span no successful tick
	// has recorded as a gap yet.
	FailedTicks int64
	FailedSince *time.Time
}

// Meta reads the meta table.
func (s *Store) Meta(ctx context.Context) (Meta, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key, value FROM meta`)
	if err != nil {
		return Meta{}, fmt.Errorf("read quota telemetry meta: %w", err)
	}
	defer rows.Close()
	values := map[string]string{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return Meta{}, fmt.Errorf("scan quota telemetry meta: %w", err)
		}
		values[key] = value
	}
	if err := rows.Err(); err != nil {
		return Meta{}, fmt.Errorf("iterate quota telemetry meta: %w", err)
	}
	number := func(key string) int64 {
		value, _ := strconv.ParseInt(values[key], 10, 64)
		return value
	}
	instant := func(key string) *time.Time {
		value, err := time.Parse(time.RFC3339Nano, values[key])
		if err != nil {
			return nil
		}
		return &value
	}
	meta := Meta{
		SchemaVersion: int(number(metaSchemaVersion)), OpenCursor: values[metaOpenCursor],
		CoverageFrom: instant(metaCoverageFrom), Ticks: number(metaTicks), Failures: number(metaFailures),
		SkippedReadings: number(metaSkippedReadings), SkippedChecks: number(metaSkippedChecks),
		SkippedRecords: number(metaSkippedRecords), FailedTicks: number(metaFailedTicks), FailedSince: instant(metaFailedSince),
		LastError: values[metaLastError], LastErrorAt: instant(metaLastErrorAt), LastSuccessAt: instant(metaLastSuccessAt),
	}
	if raw, found := values[metaAuditWatermark]; found {
		if watermark, err := strconv.ParseInt(raw, 10, 64); err == nil {
			meta.AuditWatermark = &watermark
		}
	}
	return meta, nil
}

func formatInstant(at time.Time) string { return at.UTC().Format(time.RFC3339Nano) }

// event reads one stored event by id.
func (s *Store) event(ctx context.Context, id string) (Event, bool, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT record FROM events WHERE event_id = ?`, id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return Event{}, false, nil
	}
	if err != nil {
		return Event{}, false, fmt.Errorf("read quota telemetry event %q: %w", id, err)
	}
	var event Event
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		return Event{}, false, fmt.Errorf("decode quota telemetry event %q: %w", id, err)
	}
	return event, true, nil
}

// openWork is one recorded assignment epoch with no finish.
type openWork struct {
	key      string
	dispatch *Event
	start    *Event
}

// openWorkAfter lists up to limit assignment epochs that have a dispatch or a
// start and no finish, ordered by key after cursor. The returned cursor is the
// last key listed, or empty when the list reached the end, so successive calls
// rotate through every open assignment even when there are more than limit.
func (s *Store) openWorkAfter(ctx context.Context, cursor string, limit int) ([]openWork, string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT substr(e.event_id, length(e.kind) + 2) AS work_key, e.record
		FROM events AS e
		WHERE e.kind IN ('dispatch', 'start')
			AND NOT EXISTS (SELECT 1 FROM events AS f WHERE f.event_id = 'finish:' || substr(e.event_id, length(e.kind) + 2))
			AND substr(e.event_id, length(e.kind) + 2) > ?
		ORDER BY work_key, e.kind
		LIMIT ?`, cursor, 2*limit+2)
	if err != nil {
		return nil, "", fmt.Errorf("list open quota telemetry work: %w", err)
	}
	defer rows.Close()
	var work []openWork
	rowCount := 0
	for rows.Next() {
		rowCount++
		var key, raw string
		if err := rows.Scan(&key, &raw); err != nil {
			return nil, "", fmt.Errorf("scan open quota telemetry work: %w", err)
		}
		var event Event
		if err := json.Unmarshal([]byte(raw), &event); err != nil || event.Work == nil {
			continue
		}
		if len(work) == 0 || work[len(work)-1].key != key {
			work = append(work, openWork{key: key})
		}
		current := &work[len(work)-1]
		if event.Kind == KindDispatch {
			current.dispatch = &event
		} else {
			current.start = &event
		}
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate open quota telemetry work: %w", err)
	}
	if rowCount < 2*limit+2 {
		// Everything after the cursor was listed: the next call starts over.
		if len(work) > limit {
			return work[:limit], work[limit-1].key, nil
		}
		return work, "", nil
	}
	// The last key may be cut between its two rows; it is listed next time.
	work = work[:len(work)-1]
	if len(work) > limit {
		work = work[:limit]
	}
	if len(work) == 0 {
		return nil, "", nil
	}
	return work, work[len(work)-1].key, nil
}

// pruneLimits bound one retention pass.
type pruneLimits struct {
	maxAge          time.Duration
	maxRows         int64
	batch           int
	maxTransactions int
}

// Retention defaults: thirty days, half a million rows, oldest first, at most
// 5,000 rows per transaction and 20 transactions per pass.
var defaultPruneLimits = pruneLimits{maxAge: 30 * 24 * time.Hour, maxRows: 500000, batch: 5000, maxTransactions: 20}

// PruneResult reports one retention pass.
type PruneResult struct {
	Deleted      int64
	Transactions int
	// Complete is false when the pass stopped at its transaction limit with
	// rows still due; the next pass continues.
	Complete bool
}

// Prune deletes events older than thirty days and, beyond half a million
// rows, the oldest, in bounded batches.
func (s *Store) Prune(ctx context.Context, now time.Time) (PruneResult, error) {
	return s.prune(ctx, now, defaultPruneLimits)
}

func (s *Store) prune(ctx context.Context, now time.Time, limits pruneLimits) (PruneResult, error) {
	var result PruneResult
	cutoff := now.Add(-limits.maxAge).UnixNano()
	for result.Transactions < limits.maxTransactions {
		deleted, err := s.deleteBatch(ctx, `DELETE FROM events WHERE seq IN (
			SELECT seq FROM events WHERE at_ns < ? ORDER BY at_ns, seq LIMIT ?)`, cutoff, limits.batch)
		if err != nil {
			return result, err
		}
		if deleted == 0 {
			break
		}
		result.Deleted += deleted
		result.Transactions++
	}
	for result.Transactions < limits.maxTransactions {
		var rows int64
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&rows); err != nil {
			return result, fmt.Errorf("count quota telemetry events: %w", err)
		}
		excess := rows - limits.maxRows
		if excess <= 0 {
			result.Complete = true
			return result, nil
		}
		deleted, err := s.deleteBatch(ctx, `DELETE FROM events WHERE seq IN (
			SELECT seq FROM events ORDER BY at_ns, seq LIMIT ?)`, min(excess, int64(limits.batch)))
		if err != nil {
			return result, err
		}
		result.Deleted += deleted
		result.Transactions++
	}
	return result, nil
}

func (s *Store) deleteBatch(ctx context.Context, statement string, args ...any) (int64, error) {
	if s.readOnly {
		return 0, errors.New("quota telemetry store is open read-only")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin quota telemetry prune: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, statement, args...)
	if err != nil {
		return 0, fmt.Errorf("prune quota telemetry events: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count pruned quota telemetry events: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit quota telemetry prune: %w", err)
	}
	return deleted, nil
}

// StoreInfo describes the store for the read command's header.
type StoreInfo struct {
	Path           string     `json:"path"`
	SchemaVersion  int        `json:"schemaVersion"`
	OldestRetained *time.Time `json:"oldestRetained"`
	Rows           int64      `json:"rows"`
}

// Info reads the store's size and the oldest retained event time.
func (s *Store) Info(ctx context.Context) (StoreInfo, error) {
	info := StoreInfo{Path: s.path, SchemaVersion: SchemaVersion}
	var oldest sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*), MIN(at_ns) FROM events`).Scan(&info.Rows, &oldest); err != nil {
		return StoreInfo{}, fmt.Errorf("read quota telemetry store size: %w", err)
	}
	if oldest.Valid {
		at := time.Unix(0, oldest.Int64).UTC()
		info.OldestRetained = &at
	}
	return info, nil
}

// Filter selects events for the read command.
type Filter struct {
	// Since is the earliest event time; zero means every retained event.
	Since time.Time
	// Route is a substring of "instance/model", or of a reading's bucket key.
	Route string
	// Pool matches quotaPoolId, and readings whose provider instance is one
	// of PoolInstances.
	Pool          string
	PoolInstances []string
	// Kinds defaults to every kind.
	Kinds []string
	// Limit keeps the newest events; it defaults to DefaultQueryLimit.
	Limit int
}

// Read command limits.
const (
	DefaultQueryLimit = 200
	MaxQueryLimit     = 10000
)

// maxDeltaRows bounds the readings and spans one query loads for deltas.
const maxDeltaRows = 50000

// QueryResult is what the read command prints.
type QueryResult struct {
	Store  StoreInfo
	Meta   Meta
	Gaps   []Event
	Events []Event
}

// Query returns the newest matching events, oldest first, with deltas
// computed for finish events.
func (s *Store) Query(ctx context.Context, filter Filter, now time.Time) (QueryResult, error) {
	var result QueryResult
	var err error
	if result.Store, err = s.Info(ctx); err != nil {
		return QueryResult{}, err
	}
	if result.Meta, err = s.Meta(ctx); err != nil {
		return QueryResult{}, err
	}
	// Event times are stored as Unix nanoseconds, which cover the years 1678
	// to 2262; a since outside them is clamped rather than wrapped.
	since := int64(math.MinInt64)
	switch {
	case filter.Since.IsZero() || filter.Since.Year() < 1700:
	case filter.Since.Year() > 2250:
		since = math.MaxInt64
	default:
		since = filter.Since.UnixNano()
	}
	kinds := filter.Kinds
	if len(kinds) == 0 {
		kinds = Kinds
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = DefaultQueryLimit
	}
	query := `SELECT record FROM events WHERE at_ns >= ? AND kind IN (` + placeholders(len(kinds)) + `)`
	args := []any{since}
	for _, kind := range kinds {
		args = append(args, kind)
	}
	if filter.Route != "" {
		query += ` AND instr(route, ?) > 0`
		args = append(args, filter.Route)
	}
	if filter.Pool != "" {
		clause := `quota_pool_id = ?`
		args = append(args, filter.Pool)
		for _, instance := range filter.PoolInstances {
			clause += ` OR (kind = 'reading' AND substr(route, 1, ?) = ?)`
			args = append(args, len(instance)+1, instance+"/")
		}
		query += ` AND (` + clause + `)`
	}
	query += ` ORDER BY at_ns DESC, seq DESC LIMIT ?`
	args = append(args, limit)
	events, err := s.scanEvents(ctx, query, args...)
	if err != nil {
		return QueryResult{}, err
	}
	for left, right := 0, len(events)-1; left < right; left, right = left+1, right-1 {
		events[left], events[right] = events[right], events[left]
	}
	if err := s.attachDeltas(ctx, events, now); err != nil {
		return QueryResult{}, err
	}
	result.Events = events
	gaps, err := s.scanEvents(ctx, `SELECT record FROM events WHERE kind = ? AND at_ns >= ? ORDER BY at_ns DESC, seq DESC LIMIT 100`,
		KindRecorder, since)
	if err != nil {
		return QueryResult{}, err
	}
	result.Gaps = []Event{}
	for index := len(gaps) - 1; index >= 0; index-- {
		if gaps[index].Recorder != nil && gaps[index].Recorder.State == RecorderGap {
			result.Gaps = append(result.Gaps, gaps[index])
		}
	}
	return result, nil
}

func placeholders(count int) string {
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
}

func (s *Store) scanEvents(ctx context.Context, query string, args ...any) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query quota telemetry events: %w", err)
	}
	defer rows.Close()
	events := []Event{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan quota telemetry event: %w", err)
		}
		var event Event
		if err := json.Unmarshal([]byte(raw), &event); err != nil {
			return nil, fmt.Errorf("decode quota telemetry event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate quota telemetry events: %w", err)
	}
	return events, nil
}

// latestReadingPerKeyQuery lists the newest reading of each bucket key in
// [?1, ?2), at most ?3 keys. It steps from one key to the next through the
// (kind, route) index, so its cost grows with the number of keys, not of
// retained readings.
const latestReadingPerKeyQuery = `WITH RECURSIVE keys(route) AS (
	SELECT (SELECT MIN(route) FROM events WHERE kind = 'reading' AND route >= ?1 AND route < ?2)
	UNION ALL
	SELECT (SELECT MIN(route) FROM events WHERE kind = 'reading' AND route > keys.route AND route < ?2)
	FROM keys WHERE keys.route IS NOT NULL
	LIMIT ?3
)
SELECT (SELECT record FROM events WHERE kind = 'reading' AND route = keys.route ORDER BY seq DESC LIMIT 1)
FROM keys WHERE keys.route IS NOT NULL`

// attachDeltas computes the deltas of every finish event among events. The
// spans are loaded once for all of them; the readings per finish, from the
// two 30-minute windows around its start and its finish, with the newest
// retained reading of each bucket key of its provider instance, so a key
// with no reading near the interval still has a delta that says so.
func (s *Store) attachDeltas(ctx context.Context, events []Event, now time.Time) error {
	var first, last time.Time
	for _, event := range events {
		if event.Kind != KindFinish || event.Work == nil || event.Work.StartedAt == nil || event.Work.FinishedAt == nil {
			continue
		}
		if first.IsZero() || event.Work.StartedAt.Before(first) {
			first = *event.Work.StartedAt
		}
		if event.Work.FinishedAt.After(last) {
			last = *event.Work.FinishedAt
		}
	}
	if first.IsZero() {
		return nil
	}
	from := first.Add(-deltaReadingWindow - overlapLookback).UnixNano()
	to := last.Add(deltaReadingWindow).UnixNano()
	starts, err := s.scanEvents(ctx, `SELECT record FROM events WHERE kind = ? AND at_ns BETWEEN ? AND ? ORDER BY at_ns LIMIT ?`,
		KindStart, from, to, maxDeltaRows)
	if err != nil {
		return err
	}
	finishes, err := s.scanEvents(ctx, `SELECT record FROM events WHERE kind = ? AND at_ns >= ? ORDER BY at_ns LIMIT ?`,
		KindFinish, from, maxDeltaRows)
	if err != nil {
		return err
	}
	ended := map[string]time.Time{}
	for _, finish := range finishes {
		if finish.Work != nil {
			ended[finish.Work.Key()] = finish.At
		}
	}
	spans := make([]WorkSpan, 0, len(starts))
	for _, start := range starts {
		if start.Work == nil {
			continue
		}
		span := WorkSpan{Key: start.Work.Key(), AttemptID: start.Work.AttemptID, Pool: PoolOf(start.Work.Route), Start: start.At}
		if end, found := ended[span.Key]; found {
			span.End = &end
		}
		spans = append(spans, span)
	}
	// The newest retained reading of each bucket key, per provider instance.
	latestByInstance := map[string][]Event{}
	for index := range events {
		event := &events[index]
		if event.Kind != KindFinish || event.Work == nil || event.Work.StartedAt == nil || event.Work.FinishedAt == nil {
			continue
		}
		prefix := event.Work.Route.ProviderInstanceID + "/"
		latest, loaded := latestByInstance[prefix]
		if !loaded {
			latest, err = s.scanEvents(ctx, latestReadingPerKeyQuery, prefix, prefix[:len(prefix)-1]+"0", maxDeltaRows)
			if err != nil {
				return err
			}
			latestByInstance[prefix] = latest
		}
		window := `SELECT record FROM events WHERE kind = 'reading' AND at_ns BETWEEN ? AND ? AND substr(route, 1, ?) = ? LIMIT ?`
		start, finish := *event.Work.StartedAt, *event.Work.FinishedAt
		before, err := s.scanEvents(ctx, window, start.Add(-deltaReadingWindow).UnixNano(), start.UnixNano(), len(prefix), prefix, maxDeltaRows)
		if err != nil {
			return err
		}
		after, err := s.scanEvents(ctx, window, finish.UnixNano(), finish.Add(deltaReadingWindow).UnixNano(), len(prefix), prefix, maxDeltaRows)
		if err != nil {
			return err
		}
		readings := append(append(before, after...), latest...)
		event.Deltas = ComputeDeltas(*event, readings, spans, now)
	}
	return nil
}
