package sqlite

import (
	"context"
	"database/sql"
	"fmt"
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

// Temporary, connection-local tracking needs no persistent schema migration.
// Trigger updates participate in the writer's transaction, so rollback leaves
// the cached records valid. External commits remain fenced by data_version.
func ensureCoordinatorRecordsStamp(ctx context.Context, conn *sql.Conn) error {
	var exists int
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_temp_master WHERE name = 'coordinator_records_stamp'").Scan(&exists); err != nil {
		return err
	}
	if exists != 0 {
		return nil
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "CREATE TEMP TABLE coordinator_records_stamp (revision INTEGER NOT NULL); INSERT INTO coordinator_records_stamp VALUES (0)"); err != nil {
		return err
	}
	for _, table := range []string{
		"coordinator_workflows", "coordinator_workflow_runs", "coordinator_tasks",
		"coordinator_attempts", "coordinator_assignments", "coordinator_schedules",
		"coordinator_schedule_templates", "coordinator_triggers", "coordinator_quota_pools",
		"coordinator_artifacts", "coordinator_admin_commands",
	} {
		for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
			query := fmt.Sprintf("CREATE TEMP TRIGGER records_stamp_%s_%s AFTER %s ON main.%s BEGIN UPDATE coordinator_records_stamp SET revision = revision + 1; END", table, event, event, table)
			if _, err := tx.ExecContext(ctx, query); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

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
	// data_version detects commits by other connections; temporary triggers track
	// only the tables read by CoordinatorRecords on this connection.
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
	if err := ensureCoordinatorRecordsStamp(ctx, conn); err != nil {
		return CoordinatorRecords{}, err
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return CoordinatorRecords{}, err
	}
	defer tx.Rollback()
	cached := snapshot.entries[s]
	changes, version, valid, err := coordinatorRecordsValidity(ctx, tx, connection, cached)
	if err != nil {
		return CoordinatorRecords{}, err
	}
	if valid {
		if err := tx.Commit(); err != nil {
			return CoordinatorRecords{}, err
		}
		return cached.records, nil
	}
	snapshot.loads++
	records, err := loadCoordinatorRecordsTx(ctx, tx, true)
	if err != nil {
		return CoordinatorRecords{}, err
	}
	if err := tx.Commit(); err != nil {
		return CoordinatorRecords{}, err
	}
	records = s.attachArtifactLoaders(records)
	snapshot.entries[s] = coordinatorRecordsEntry{connection: connection, changes: changes, version: version, records: records}
	return records, nil
}

// coordinatorRecordsValidity shares the connection, records stamp and data-version rule.
func coordinatorRecordsValidity(ctx context.Context, tx *sql.Tx, connection reflect.Value, cached coordinatorRecordsEntry) (changes, version int64, valid bool, err error) {
	if err = tx.QueryRowContext(ctx, "SELECT revision FROM temp.coordinator_records_stamp").Scan(&changes); err != nil {
		return
	}
	if err = tx.QueryRowContext(ctx, "PRAGMA data_version").Scan(&version); err != nil {
		return
	}
	valid = cached.connection == connection && cached.changes == changes && cached.version == version
	return
}

// coordinatorRecordsSnapshotTx reuses read-only tick records in an open transaction.
// Node-wait resolution needs every run, including terminal ones; future narrowing
// of the tick snapshot must preserve this path's complete run history.
func (s *Store) coordinatorRecordsSnapshotTx(ctx context.Context, tx *sql.Tx, connection reflect.Value) (CoordinatorRecords, bool, error) {
	snapshot, _ := ctx.Value(coordinatorRecordsSnapshotKey{}).(*CoordinatorRecordsSnapshot)
	if snapshot == nil {
		return CoordinatorRecords{}, false, nil
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	cached, ok := snapshot.entries[s]
	if !snapshot.active || !ok || cached.connection != connection {
		// A different connection may not have the temporary stamp.
		return CoordinatorRecords{}, false, nil
	}
	_, _, valid, err := coordinatorRecordsValidity(ctx, tx, connection, cached)
	return cached.records, valid, err
}
