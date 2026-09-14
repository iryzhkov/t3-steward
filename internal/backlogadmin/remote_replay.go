package backlogadmin

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
	_ "modernc.org/sqlite"
)

const (
	remoteReplayMaxAge           = 24 * time.Hour
	remoteReplayMaxBytes         = 32 << 20
	remoteReplayMaxResponseBytes = 8 << 20
	remoteReplayMaxRequests      = 4096
	remoteReplayAbandonedAge     = 2 * time.Minute
)

// RemoteReplayStore gives the remote carrier durable replay protection. A
// request id is answered once: repeating it with the same content returns the
// first answer, and repeating it with different content is refused. That is
// what makes "lose the submission response and retry with the same idempotency
// key" produce exactly one workflow run, even when the loss happens below the
// application layer.
//
// Each Begin opens one connection and holds an interprocess lock through the
// handler, because the restricted SSH command is a separate process per
// request. A killed handler leaves a durable pending identity, which the next
// attempt may resume rather than repeat.
type RemoteReplayStore struct {
	path, lockPath string
	coordinatorID  string
	now            func() time.Time
}

// OpenRemoteReplayStore prepares the store beside the coordinator's own state.
func OpenRemoteReplayStore(root, coordinatorID string) (*RemoteReplayStore, error) {
	if root == "" || coordinatorID == "" {
		return nil, errors.New("admin replay store: root and coordinator id are required")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if absolute == string(filepath.Separator) {
		return nil, errors.New("admin replay store: filesystem root is not allowed")
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, err
	}
	store := &RemoteReplayStore{
		path:          filepath.Join(absolute, "admin-replay.sqlite"),
		lockPath:      filepath.Join(absolute, "admin-replay.lock"),
		coordinatorID: coordinatorID, now: time.Now,
	}
	lock, err := store.lock()
	if err != nil {
		return nil, err
	}
	defer releaseAdminLock(lock)
	db, err := store.openDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if err := store.initialize(db); err != nil {
		return nil, fmt.Errorf("admin replay store: initialize: %w", err)
	}
	return store, nil
}

func (s *RemoteReplayStore) openDB() (*sql.DB, error) {
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", s.path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;
		PRAGMA cache_size=-2048; PRAGMA mmap_size=0; PRAGMA max_page_count=32768;
		PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;`); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (s *RemoteReplayStore) initialize(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`
		CREATE TABLE IF NOT EXISTS identity (
			id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, coordinator TEXT NOT NULL);
		CREATE TABLE IF NOT EXISTS requests (
			id TEXT PRIMARY KEY, digest TEXT NOT NULL, ready INTEGER NOT NULL, started INTEGER NOT NULL);
		CREATE INDEX IF NOT EXISTS requests_age ON requests(started);
		CREATE TABLE IF NOT EXISTS responses (
			id TEXT PRIMARY KEY REFERENCES requests(id) ON DELETE CASCADE, body BLOB NOT NULL);
	`); err != nil {
		return err
	}
	var count int
	if err := tx.QueryRow("SELECT count(*) FROM identity").Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		if _, err := tx.Exec("INSERT INTO identity VALUES(1,1,?)", s.coordinatorID); err != nil {
			return err
		}
	}
	if err := s.checkIdentity(tx); err != nil {
		return err
	}
	if err := s.prune(tx, 0); err != nil {
		return err
	}
	return tx.Commit()
}

type adminReplayQuerier interface {
	QueryRow(string, ...any) *sql.Row
}

func (s *RemoteReplayStore) checkIdentity(q adminReplayQuerier) error {
	var version int
	var coordinator string
	if err := q.QueryRow("SELECT version,coordinator FROM identity WHERE id=1").Scan(&version, &coordinator); err != nil {
		return err
	}
	if version != 1 || coordinator != s.coordinatorID {
		return errors.New("admin replay store: durable coordinator identity mismatch")
	}
	return nil
}

// RemoteReplayTransaction owns one request identity until it is completed or
// released.
type RemoteReplayTransaction interface {
	Cached() ([]byte, bool)
	Complete(response []byte) error
	Release() error
}

// Begin claims one request identity. A cached answer is returned as raw bytes,
// which the caller writes back verbatim.
func (s *RemoteReplayStore) Begin(principal, requestID, digest string) (RemoteReplayTransaction, error) {
	if principal == "" || requestID == "" || digest == "" ||
		len(principal)+len(requestID) > 2048 || len(digest) > 128 {
		return nil, errors.New("admin replay store: invalid begin request")
	}
	lock, err := s.lock()
	if err != nil {
		return nil, err
	}
	db, err := s.openDB()
	if err != nil {
		releaseAdminLock(lock)
		return nil, err
	}
	transaction := &remoteReplayTransaction{store: s, lock: lock, db: db, key: principal + "/" + requestID}
	fail := func(err error) (RemoteReplayTransaction, error) {
		_ = transaction.Release()
		return nil, err
	}
	if err := s.checkIdentity(db); err != nil {
		return fail(err)
	}
	// A request whose handler died stops holding its own identity after the
	// recovery interval. This runs before the identity is looked up, so that a
	// caller retrying an abandoned request sees a free identity rather than
	// being told forever that an identical request is in flight.
	if _, err := db.Exec("DELETE FROM requests WHERE ready=0 AND started<?",
		s.now().Add(-remoteReplayAbandonedAge).UnixNano()); err != nil {
		return fail(err)
	}
	var storedDigest string
	var ready bool
	err = db.QueryRow("SELECT digest,ready FROM requests WHERE id=?", transaction.key).Scan(&storedDigest, &ready)
	if err == nil {
		if storedDigest != digest {
			return fail(&workerproto.ProtocolError{Code: workerproto.ErrorReplay,
				Message: "request id was reused with different content", RequestID: requestID})
		}
		if !ready {
			return fail(&workerproto.ProtocolError{Code: workerproto.ErrorBackpressure,
				Message: "an identical request is already in flight", Retryable: true, RetryAfter: "1s", RequestID: requestID})
		}
		if err := db.QueryRow("SELECT body FROM responses WHERE id=?", transaction.key).Scan(&transaction.cached); err != nil {
			return fail(err)
		}
		transaction.ready = true
		return transaction, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fail(err)
	}
	tx, err := db.Begin()
	if err != nil {
		return fail(err)
	}
	defer tx.Rollback()
	if err := s.prune(tx, 0); err != nil {
		return fail(err)
	}
	if _, err := tx.Exec("INSERT INTO requests VALUES(?,?,0,?)", transaction.key, digest, s.now().UnixNano()); err != nil {
		return fail(err)
	}
	if err := tx.Commit(); err != nil {
		return fail(err)
	}
	return transaction, nil
}

type remoteReplayTransaction struct {
	store  *RemoteReplayStore
	lock   *os.File
	db     *sql.DB
	key    string
	cached []byte
	ready  bool
}

func (t *remoteReplayTransaction) Cached() ([]byte, bool) {
	return t.cached, t.ready
}

func (t *remoteReplayTransaction) Complete(response []byte) error {
	if t.lock == nil {
		return errors.New("admin replay store: transaction is closed")
	}
	if len(response) > remoteReplayMaxResponseBytes {
		return errors.New("admin replay store: response exceeds byte budget; pending identity retained")
	}
	tx, err := t.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := t.store.prune(tx, int64(len(response))); err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO responses VALUES(?,?) ON CONFLICT(id) DO UPDATE SET body=excluded.body",
		t.key, response); err != nil {
		return err
	}
	result, err := tx.Exec("UPDATE requests SET ready=1 WHERE id=?", t.key)
	if err != nil {
		return err
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return errors.New("admin replay store: pending request disappeared")
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return t.Release()
}

func (t *remoteReplayTransaction) Release() error {
	if t.lock == nil {
		return nil
	}
	lock := t.lock
	t.lock = nil
	return errors.Join(t.db.Close(), releaseAdminLock(lock))
}

// prune drops expired identities and keeps the cached bodies inside their
// budget. It never reads a retained body.
func (s *RemoteReplayStore) prune(tx *sql.Tx, reserve int64) error {
	cutoff := s.now().Add(-remoteReplayMaxAge).UnixNano()
	if _, err := tx.Exec("DELETE FROM requests WHERE ready=1 AND started<?", cutoff); err != nil {
		return err
	}
	var total int64
	var count int
	if err := tx.QueryRow("SELECT coalesce(sum(length(body)),0),count(*) FROM responses").Scan(&total, &count); err != nil {
		return err
	}
	for total+reserve > remoteReplayMaxBytes || count > remoteReplayMaxRequests {
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

func (s *RemoteReplayStore) lock() (*os.File, error) {
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

func releaseAdminLock(lock *os.File) error {
	return errors.Join(syscall.Flock(int(lock.Fd()), syscall.LOCK_UN), lock.Close())
}
