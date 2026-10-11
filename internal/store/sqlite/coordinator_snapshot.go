package sqlite

import (
	"context"
	"database/sql"
	"reflect"
	"sync"
)

// CoordinatorRecordsSnapshot shares read-only records during one coordinator
// tick. It is passed through context so all existing store interfaces, including
// per-worker exchanges and activation arbitration, use the same snapshot.
type CoordinatorRecordsSnapshot struct {
	mu      sync.Mutex
	active  bool
	loads   int
	entries map[*Store]coordinatorRecordsEntry
}

type coordinatorRecordsEntry struct {
	connection reflect.Value
	changes    int64
	version    int64
	records    CoordinatorRecords
}
type coordinatorRecordsSnapshotKey struct{}

// WithCoordinatorRecordsSnapshot starts a fresh scope; command and admin
// contexts without this value retain independent transactionally consistent loads.
func WithCoordinatorRecordsSnapshot(ctx context.Context) (context.Context, *CoordinatorRecordsSnapshot) {
	snapshot := &CoordinatorRecordsSnapshot{active: true, entries: make(map[*Store]coordinatorRecordsEntry)}
	return context.WithValue(ctx, coordinatorRecordsSnapshotKey{}, snapshot), snapshot
}

// WithoutCoordinatorRecordsSnapshot gives background work its own loads.
func WithoutCoordinatorRecordsSnapshot(ctx context.Context) context.Context {
	return context.WithValue(ctx, coordinatorRecordsSnapshotKey{}, (*CoordinatorRecordsSnapshot)(nil))
}

// Finish releases the snapshot and returns the number of actual record loads.
// Contexts retained by asynchronous work cannot extend the tick's cache lifetime.
func (s *CoordinatorRecordsSnapshot) Finish() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active = false
	s.entries = nil
	return s.loads
}

func (s *Store) loadCoordinatorRecordsSnapshot(ctx context.Context, snapshot *CoordinatorRecordsSnapshot) (CoordinatorRecords, error) {
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if !snapshot.active {
		return s.loadCoordinatorRecords(ctx)
	}
	// Keep the connection and read transaction for both the stamp and the load:
	// data_version detects commits by other connections; total_changes detects
	// writes on this connection (also conservatively invalidating rolled-back writes).
	// The identity prevents reuse if database/sql replaces the connection.
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return CoordinatorRecords{}, err
	}
	defer conn.Close()
	var connection reflect.Value
	if err := conn.Raw(func(raw any) error { connection = reflect.ValueOf(raw); return nil }); err != nil {
		return CoordinatorRecords{}, err
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return CoordinatorRecords{}, err
	}
	defer tx.Rollback()
	var changes, version int64
	if err := tx.QueryRowContext(ctx, "SELECT total_changes()").Scan(&changes); err != nil {
		return CoordinatorRecords{}, err
	}
	if err := tx.QueryRowContext(ctx, "PRAGMA data_version").Scan(&version); err != nil {
		return CoordinatorRecords{}, err
	}
	if cached, ok := snapshot.entries[s]; ok && cached.connection == connection && cached.changes == changes && cached.version == version {
		if err := tx.Commit(); err != nil {
			return CoordinatorRecords{}, err
		}
		return cached.records, nil
	}
	snapshot.loads++
	records, err := loadCoordinatorRecordsTx(ctx, tx)
	if err != nil {
		return CoordinatorRecords{}, err
	}
	if err := tx.Commit(); err != nil {
		return CoordinatorRecords{}, err
	}
	snapshot.entries[s] = coordinatorRecordsEntry{connection: connection, changes: changes, version: version, records: records}
	return records, nil
}
