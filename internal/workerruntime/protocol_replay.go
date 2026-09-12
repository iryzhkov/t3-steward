package workerruntime

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
	_ "modernc.org/sqlite"
)

const (
	protocolReplayVersion          = 1 // Legacy JSON input version.
	abandonedRequestAge            = 2 * time.Minute
	protocolReplayMaxAge           = 24 * time.Hour
	protocolReplayMaxBytes         = 32 << 20
	protocolReplayMaxResponseBytes = 8 << 20
	protocolReplayMaxMetadataBytes = 8 << 20
)

// These types describe only the read-once legacy migration format.
type protocolReplayRecord struct {
	Digest    string    `json:"digest"`
	Response  []byte    `json:"response,omitempty"`
	Ready     bool      `json:"ready"`
	StartedAt time.Time `json:"startedAt,omitempty"`
}
type protocolReplayState struct {
	Version          int                             `json:"version"`
	CoordinatorID    string                          `json:"coordinatorId"`
	WorkerID         string                          `json:"workerId"`
	CoordinatorEpoch int64                           `json:"coordinatorEpoch"`
	WorkerEpoch      string                          `json:"workerEpoch"`
	Sessions         map[string]int64                `json:"sessions"`
	Requests         map[string]protocolReplayRecord `json:"requests"`
	RequestOrder     []string                        `json:"requestOrder"`
}

// ProtocolReplayStore owns worker-local replay data. Each Begin opens one
// connection and holds the interprocess lock through handler completion. SQL
// commits bracket the effect; no SQL transaction is held across the handler.
// A killed handler leaves a durable pending identity for idempotent recovery.
type ProtocolReplayStore struct {
	root, path, lockPath    string
	coordinatorID, workerID string
	coordinatorEpoch        int64
	workerEpoch             string
	now                     func() time.Time
	maxBytes                int64
	maxAge                  time.Duration
}

func OpenProtocolReplayStore(root, coordinatorID, workerID string, coordinatorEpoch int64, workerEpoch string) (*ProtocolReplayStore, error) {
	if root == "" || coordinatorID == "" || workerID == "" || coordinatorEpoch < 1 || workerEpoch == "" {
		return nil, errors.New("protocol replay store: root, identities, and epochs are required")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if absolute == string(filepath.Separator) {
		return nil, errors.New("protocol replay store: filesystem root is not allowed")
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, err
	}
	s := &ProtocolReplayStore{
		root: absolute, path: filepath.Join(absolute, "protocol-replay.sqlite"),
		lockPath:      filepath.Join(absolute, "protocol-replay.lock"),
		coordinatorID: coordinatorID, workerID: workerID,
		coordinatorEpoch: coordinatorEpoch, workerEpoch: workerEpoch,
		now: time.Now, maxBytes: protocolReplayMaxBytes, maxAge: protocolReplayMaxAge,
	}
	lock, err := s.lock()
	if err != nil {
		return nil, err
	}
	defer releaseProtocolLock(lock)
	db, err := s.openDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if err := s.initialize(db); err != nil {
		return nil, fmt.Errorf("protocol replay store: initialize: %w", err)
	}
	return s, nil
}

func (s *ProtocolReplayStore) openDB() (*sql.DB, error) {
	// Create with owner permissions before SQLite opens the file.
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", s.path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	// DELETE journaling avoids a WAL/checkpoint growing independently of retention.
	// Freed pages are reused. The hard page cap bounds the database and rollback
	// journal even when logical retention cannot reclaim an active request.
	_, err = db.Exec(`PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;
		PRAGMA cache_size=-2048; PRAGMA mmap_size=0; PRAGMA max_page_count=32768;
		PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (s *ProtocolReplayStore) initialize(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS identity (
			id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL,
			coordinator TEXT NOT NULL, worker TEXT NOT NULL,
			epoch INTEGER NOT NULL, worker_epoch TEXT NOT NULL);
		CREATE TABLE IF NOT EXISTS sessions (
			id TEXT PRIMARY KEY, sequence INTEGER NOT NULL, touched INTEGER NOT NULL);
		CREATE INDEX IF NOT EXISTS sessions_age ON sessions(touched);
		CREATE TABLE IF NOT EXISTS requests (
			id TEXT PRIMARY KEY, digest TEXT NOT NULL, ready INTEGER NOT NULL,
			started INTEGER NOT NULL);
		CREATE INDEX IF NOT EXISTS requests_age ON requests(started);
		CREATE TABLE IF NOT EXISTS responses (
			id TEXT PRIMARY KEY REFERENCES requests(id) ON DELETE CASCADE,
			body BLOB NOT NULL);
	`)
	if err != nil {
		return err
	}
	var count int
	if err := tx.QueryRow("SELECT count(*) FROM identity").Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		if err := s.importLegacy(tx); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO identity VALUES(1,2,?,?,?,?)",
			s.coordinatorID, s.workerID, s.coordinatorEpoch, s.workerEpoch); err != nil {
			return err
		}
	}
	if err := s.checkIdentity(tx, true); err != nil {
		return err
	}
	if err := s.prune(tx, 4096, 0); err != nil {
		return err
	}
	var metadata int64
	if err := tx.QueryRow(`SELECT
		(SELECT coalesce(sum(length(CAST(id AS BLOB))+32),0) FROM sessions) +
		(SELECT coalesce(sum(length(CAST(id AS BLOB))+length(digest)+32),0) FROM requests)`).Scan(&metadata); err != nil {
		return err
	}
	if metadata > protocolReplayMaxMetadataBytes {
		return errors.New("protocol replay store: migrated metadata exceeds byte budget")
	}
	return tx.Commit()
}

type replayQuerier interface {
	QueryRow(string, ...any) *sql.Row
	Exec(string, ...any) (sql.Result, error)
}

func (s *ProtocolReplayStore) checkIdentity(q replayQuerier, adopt bool) error {
	var version int
	var coordinator, worker, epochName string
	var epoch int64
	if err := q.QueryRow("SELECT version,coordinator,worker,epoch,worker_epoch FROM identity WHERE id=1").
		Scan(&version, &coordinator, &worker, &epoch, &epochName); err != nil {
		return err
	}
	if version != 2 || coordinator != s.coordinatorID || worker != s.workerID ||
		epochName != s.workerEpoch || epoch > s.coordinatorEpoch || (!adopt && epoch != s.coordinatorEpoch) {
		return errors.New("protocol replay store: durable identity or epoch mismatch")
	}
	if adopt && epoch < s.coordinatorEpoch {
		// Keep custody and session fences. Old envelopes are rejected by the
		// authenticated protocol before Begin; this does not alter attempt state.
		_, err := q.Exec("UPDATE identity SET epoch=? WHERE id=1", s.coordinatorEpoch)
		return err
	}
	return nil
}

func (s *ProtocolReplayStore) Begin(peerPrincipal string, envelope workerproto.Envelope, digest string, maxCachedRequests int) (workerproto.ReplayTransaction, error) {
	if peerPrincipal == "" || envelope.RequestID == "" || envelope.SessionID == "" || digest == "" ||
		maxCachedRequests < 1 || envelope.Sequence < 1 || len(peerPrincipal)+len(envelope.RequestID) > 2048 ||
		len(envelope.SessionID) > 1024 || len(digest) > 128 {
		return nil, errors.New("protocol replay store: invalid begin request")
	}
	lock, err := s.lock()
	if err != nil {
		return nil, err
	}
	db, err := s.openDB()
	if err != nil {
		releaseProtocolLock(lock)
		return nil, err
	}
	t := &protocolReplayTransaction{store: s, lock: lock, db: db,
		key: peerPrincipal + "/" + envelope.RequestID, maxCachedRequests: maxCachedRequests}
	var tx *sql.Tx
	fail := func(err error) (workerproto.ReplayTransaction, error) {
		if tx != nil {
			_ = tx.Rollback()
		}
		_ = t.Release()
		return nil, err
	}
	if err := s.checkIdentity(db, false); err != nil {
		return fail(err)
	}
	tx, err = db.Begin()
	if err != nil {
		return fail(err)
	}
	if err := s.prune(tx, maxCachedRequests, 0); err != nil {
		return fail(err)
	}
	if err := tx.Commit(); err != nil {
		return fail(err)
	}
	tx = nil
	var oldDigest string
	var ready bool
	err = db.QueryRow("SELECT digest,ready FROM requests WHERE id=?", t.key).Scan(&oldDigest, &ready)
	if err == nil {
		if oldDigest != digest {
			return fail(replayError(workerproto.ErrorReplay, "request id was reused with different content", envelope.RequestID))
		}
		if ready {
			var raw []byte
			if err := db.QueryRow("SELECT body FROM responses WHERE id=?", t.key).Scan(&raw); err != nil {
				return fail(err)
			}
			if err := decodeProtocolResponse(raw, &t.cached); err != nil {
				return fail(err)
			}
		}
		t.ready = ready
		return t, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fail(err)
	}
	tx, err = db.Begin()
	if err != nil {
		return fail(err)
	}
	defer tx.Rollback()
	if err := s.prune(tx, maxCachedRequests, 0); err != nil {
		return fail(err)
	}
	// The same pending request can resume indefinitely. Other requests can
	// abandon it after the existing two-minute recovery interval; its session
	// sequence remains consumed so it cannot become a fresh execution.
	if _, err := tx.Exec("DELETE FROM requests WHERE ready=0 AND started<?", s.now().Add(-abandonedRequestAge).UnixNano()); err != nil {
		return fail(err)
	}
	var pending int
	if err := tx.QueryRow("SELECT count(*) FROM requests WHERE ready=0").Scan(&pending); err != nil {
		return fail(err)
	}
	if pending != 0 {
		return fail(&workerproto.ProtocolError{Code: workerproto.ErrorBackpressure,
			Message: "another durable request must be resumed first", Retryable: true, RetryAfter: "1s", RequestID: envelope.RequestID})
	}
	var sequence int64
	err = tx.QueryRow("SELECT sequence FROM sessions WHERE id=?", envelope.SessionID).Scan(&sequence)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fail(err)
	}
	if envelope.Sequence != sequence+1 {
		return fail(replayError(workerproto.ErrorReordered, "sequence is not the next durable session value", envelope.RequestID))
	}
	var metadata int64
	if err := tx.QueryRow(`SELECT
		(SELECT coalesce(sum(length(CAST(id AS BLOB))+32),0) FROM sessions) +
		(SELECT coalesce(sum(length(CAST(id AS BLOB))+length(digest)+32),0) FROM requests)`).Scan(&metadata); err != nil {
		return fail(err)
	}
	if metadata+int64(len(t.key)+len(digest)+len(envelope.SessionID)+64) > protocolReplayMaxMetadataBytes {
		return fail(replayError(workerproto.ErrorBackpressure, "replay metadata byte budget exhausted", envelope.RequestID))
	}
	now := s.now().UnixNano()
	if _, err := tx.Exec("INSERT INTO sessions VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET sequence=excluded.sequence,touched=excluded.touched",
		envelope.SessionID, envelope.Sequence, now); err != nil {
		return fail(err)
	}
	if _, err := tx.Exec("INSERT INTO requests VALUES(?,?,0,?)", t.key, digest, now); err != nil {
		return fail(err)
	}
	if err := tx.Commit(); err != nil {
		return fail(err)
	}
	return t, nil
}

func replayError(code workerproto.ErrorCode, message, id string) error {
	return &workerproto.ProtocolError{Code: code, Message: message, RequestID: id}
}

type protocolReplayTransaction struct {
	store             *ProtocolReplayStore
	lock              *os.File
	db                *sql.DB
	key               string
	cached            workerproto.Envelope
	ready             bool
	maxCachedRequests int
}

func (t *protocolReplayTransaction) CachedResponse() (workerproto.Envelope, bool) {
	return t.cached, t.ready
}
func (t *protocolReplayTransaction) Complete(response workerproto.Envelope) error {
	if t.lock == nil {
		return errors.New("protocol replay store: transaction is closed")
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return err
	}
	if len(encoded) > protocolReplayMaxResponseBytes || int64(len(encoded)) > t.store.maxBytes {
		return errors.New("protocol replay store: response exceeds byte budget; pending identity retained")
	}
	tx, err := t.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := t.store.prune(tx, t.maxCachedRequests-1, int64(len(encoded))); err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO responses VALUES(?,?) ON CONFLICT(id) DO UPDATE SET body=excluded.body", t.key, encoded); err != nil {
		return err
	}
	result, err := tx.Exec("UPDATE requests SET ready=1 WHERE id=?", t.key)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return errors.New("protocol replay store: pending request disappeared")
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return t.Release()
}
func (t *protocolReplayTransaction) Release() error {
	if t.lock == nil {
		return nil
	}
	lock := t.lock
	t.lock = nil
	return errors.Join(t.db.Close(), releaseProtocolLock(lock))
}

// prune touches metadata and evicted rows only; it never reads retained bodies.
// It runs while the process lock is held, in the caller's SQL transaction.
func (s *ProtocolReplayStore) prune(tx *sql.Tx, maxCount int, reserve int64) error {
	cutoff := s.now().Add(-s.maxAge).UnixNano()
	if _, err := tx.Exec("DELETE FROM requests WHERE ready=1 AND started<?", cutoff); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM sessions WHERE touched<?", cutoff); err != nil {
		return err
	}
	var total int64
	var count int
	if err := tx.QueryRow("SELECT coalesce(sum(length(body)),0),count(*) FROM responses").Scan(&total, &count); err != nil {
		return err
	}
	for total+reserve > s.maxBytes || count > maxCount {
		var id string
		var size int64
		if err := tx.QueryRow(`SELECT r.id,length(b.body) FROM requests r JOIN responses b ON b.id=r.id
			WHERE r.ready=1 ORDER BY r.started,r.id LIMIT 1`).Scan(&id, &size); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM requests WHERE id=?", id); err != nil {
			return err
		}
		total -= size
		count--
	}
	return nil
}

// importLegacy streams one response at a time. A malformed input rolls back the
// complete migration; the original JSON is retained for evidence, never rewritten
// and never consulted again after the identity transaction commits.
func (s *ProtocolReplayStore) importLegacy(tx *sql.Tx) error {
	file, err := os.Open(filepath.Join(s.root, "protocol-replay.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() > 512<<20 {
		return errors.New("legacy replay exceeds migration input limit")
	}
	d := json.NewDecoder(io.LimitReader(file, 512<<20))
	d.DisallowUnknownFields()
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return errors.New("decode legacy replay: expected object")
	}
	state := protocolReplayState{}
	seen := map[string]bool{}
	requests := map[string]bool{}
	order := []string{}
	now := s.now().UnixNano()
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return errors.New("decode legacy replay: duplicate field")
		}
		seen[key] = true
		switch key {
		case "version":
			err = d.Decode(&state.Version)
		case "coordinatorId":
			err = d.Decode(&state.CoordinatorID)
		case "workerId":
			err = d.Decode(&state.WorkerID)
		case "coordinatorEpoch":
			err = d.Decode(&state.CoordinatorEpoch)
		case "workerEpoch":
			err = d.Decode(&state.WorkerEpoch)
		case "requestOrder":
			err = d.Decode(&order)
		case "sessions", "requests":
			var open json.Token
			open, err = d.Token()
			if err != nil || open != json.Delim('{') {
				return errors.New("decode legacy replay: expected map")
			}
			for d.More() {
				nameToken, e := d.Token()
				if e != nil {
					return e
				}
				name, ok := nameToken.(string)
				if !ok || name == "" || len(name) > 2048 {
					return errors.New("decode legacy replay: invalid identity")
				}
				if key == "sessions" {
					var seq int64
					if e := d.Decode(&seq); e != nil {
						return e
					}
					if seq < 1 {
						return errors.New("decode legacy replay: invalid sequence")
					}
					if _, e := tx.Exec("INSERT INTO sessions VALUES(?,?,?)", name, seq, now); e != nil {
						return e
					}
				} else {
					var record protocolReplayRecord
					if e := d.Decode(&record); e != nil {
						return e
					}
					if record.Digest == "" || len(record.Digest) > 128 || requests[name] || (record.Ready && len(record.Response) == 0) {
						return errors.New("decode legacy replay: invalid request")
					}
					requests[name] = true
					started := record.StartedAt.UnixNano()
					if record.StartedAt.IsZero() {
						started = now
					}
					if _, e := tx.Exec("INSERT INTO requests VALUES(?,?,?,?)", name, record.Digest, record.Ready, started); e != nil {
						return e
					}
					if record.Ready {
						// Oversized old bodies cannot be cached, but their session
						// fences still reject replay. No external effect is repeated.
						if len(record.Response) > protocolReplayMaxResponseBytes {
							if _, e := tx.Exec("DELETE FROM requests WHERE id=?", name); e != nil {
								return e
							}
						} else {
							if _, e := tx.Exec("INSERT INTO responses VALUES(?,?)", name, record.Response); e != nil {
								return e
							}
							if e := s.prune(tx, 4096, 0); e != nil {
								return e
							}
						}
					}
				}
			}
			if _, err = d.Token(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("decode legacy replay: unknown field %q", key)
		}
		if err != nil {
			return fmt.Errorf("decode legacy replay: %w", err)
		}
	}
	if _, err := d.Token(); err != nil {
		return err
	}
	var trailing any
	if err := d.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("decode legacy replay: trailing content")
	}
	if state.Version != protocolReplayVersion || state.CoordinatorID != s.coordinatorID || state.WorkerID != s.workerID ||
		state.WorkerEpoch != s.workerEpoch || state.CoordinatorEpoch > s.coordinatorEpoch || state.CoordinatorEpoch < 1 {
		return errors.New("legacy replay identity or epoch mismatch")
	}
	if len(order) != len(requests) {
		return errors.New("decode legacy replay: invalid request order")
	}
	for _, id := range order {
		if !requests[id] {
			return errors.New("decode legacy replay: duplicate or missing request order")
		}
		delete(requests, id)
	}
	return nil
}

func (s *ProtocolReplayStore) lock() (*os.File, error) {
	lock, err := os.OpenFile(s.lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		lock.Close()
		return nil, err
	}
	return lock, nil
}
func releaseProtocolLock(lock *os.File) error {
	unlockErr := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	return errors.Join(unlockErr, lock.Close())
}
func decodeProtocolResponse(data []byte, response *workerproto.Envelope) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(response); err != nil {
		return fmt.Errorf("protocol replay store: decode response: %w", err)
	}
	var trailing any
	if err := d.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("protocol replay store: trailing response content")
	}
	return nil
}
