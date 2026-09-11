package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestOpenDoesNotCreateOrMigrateSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	if _, err := Open(path); err == nil {
		t.Fatal("read/admin open created a missing database")
	}
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`DROP TABLE coordinator_audit_events`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`DELETE FROM schema_version WHERE version >= 10`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var version int
	if err := store.db.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	var auditTables int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='coordinator_audit_events'`).Scan(&auditTables); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if version != 9 || auditTables != 0 {
		t.Fatalf("plain open changed schema: version=%d audit tables=%d", version, auditTables)
	}

	store, err = OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.db.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != currentSchemaVersion {
		t.Fatalf("explicit migration version = %d", version)
	}
}

func TestCoordinatorOwnershipAdvancesEpochOnlyForOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	first, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	epoch1, err := first.AcquireCoordinator(context.Background(), "coordinator-a")
	if err != nil {
		t.Fatal(err)
	}
	assertNativeAuditEvent(t, first, fmt.Sprintf("coordinator-acquire-epoch:%d", epoch1), "coordinator-a", "acquired")

	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.AcquireCoordinator(context.Background(), "coordinator-b"); !errors.Is(err, ErrCoordinatorOwned) {
		t.Fatalf("second owner error = %v", err)
	}
	epochAfterRefusal, err := second.CoordinatorEpoch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if epochAfterRefusal != epoch1 {
		t.Fatalf("refused owner advanced epoch from %d to %d", epoch1, epochAfterRefusal)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	epoch2, err := restarted.AcquireCoordinator(context.Background(), "coordinator-a")
	if err != nil {
		t.Fatal(err)
	}
	if epoch2 != epoch1+1 {
		t.Fatalf("restart epoch = %d, want %d", epoch2, epoch1+1)
	}
	assertNativeAuditEvent(t, restarted, fmt.Sprintf("coordinator-acquire-epoch:%d", epoch2), "coordinator-a", "acquired")
}

func TestOpenLeavesVersionFixtureUntouched(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE schema_version(version INTEGER NOT NULL); INSERT INTO schema_version VALUES (99)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var version int
	if err := store.db.QueryRow(`SELECT version FROM schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 99 {
		t.Fatalf("version = %d", version)
	}
}
