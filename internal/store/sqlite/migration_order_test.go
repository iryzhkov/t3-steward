package sqlite

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The synthetic versions below are placed above the real registry (which ends
// at V42), so that they never collide with a registered migration.
type testMigration = struct {
	version int
	ddl     string
}

func setTestMigrations(t *testing.T, extra ...testMigration) {
	t.Helper()
	old := versionedMigrations
	versionedMigrations = append(append([]testMigration(nil), old...), extra...)
	t.Cleanup(func() { versionedMigrations = old })
}

// The core receives a supported ceiling so future releases can be simulated
// without changing this binary's production schema version.
func TestMigrationGapsArrivalOrderReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	setTestMigrations(t, testMigration{50, "CREATE TABLE gap_high (id INTEGER);"}, testMigration{52, "CREATE TABLE gap_order (version INTEGER);"})
	if err := s.migrateThroughSupported(52, 52); err != nil {
		t.Fatal(err)
	}
	versionedMigrations = append(versionedMigrations, testMigration{51, "INSERT INTO gap_low VALUES (51); INSERT INTO gap_order VALUES (51);"}, testMigration{49, "CREATE TABLE gap_low (version INTEGER); INSERT INTO gap_order VALUES (49);"})
	if err := s.migrateThroughSupported(48, 52); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM schema_version WHERE version IN (49,51)").Scan(&count); err != nil || count != 0 {
		t.Fatalf("target: count=%d err=%v", count, err)
	}
	if err := s.migrateThroughSupported(52, 52); err != nil {
		t.Fatal(err)
	}
	rows, err := s.db.Query("SELECT version FROM gap_order ORDER BY rowid")
	if err != nil {
		t.Fatal(err)
	}
	var got []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if !reflect.DeepEqual(got, []int{49, 51}) {
		t.Fatalf("order=%v", got)
	}
	before := fixtureSchemaRows(t, s)
	if err := s.migrateThroughSupported(52, 52); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.migrateThroughSupported(52, 52); err != nil {
		t.Fatal(err)
	}
	if after := fixtureSchemaRows(t, s); !reflect.DeepEqual(after, before) {
		t.Fatal("schema changed on replay")
	}
	for _, v := range []int{49, 50, 51, 52} {
		if err := s.db.QueryRow("SELECT COUNT(*) FROM schema_version WHERE version=?", v).Scan(&count); err != nil || count != 1 {
			t.Fatalf("version %d count=%d err=%v", v, count, err)
		}
	}
	if err := s.db.QueryRow("SELECT COUNT(*) FROM gap_order").Scan(&count); err != nil || count != 2 {
		t.Fatalf("replay count=%d err=%v", count, err)
	}
}

func TestMigrationGapsRollbackRetry(t *testing.T) {
	s, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	setTestMigrations(t, testMigration{52, "CREATE TABLE gap_conflict (id INTEGER);"})
	if err := s.migrateThroughSupported(52, 52); err != nil {
		t.Fatal(err)
	}
	versionedMigrations = append(versionedMigrations, testMigration{49, "CREATE TABLE gap_rolled_back (id INTEGER); CREATE TABLE gap_conflict (id INTEGER);"}, testMigration{51, "CREATE TABLE gap_later (id INTEGER);"})
	err = s.migrateThroughSupported(52, 52)
	for _, text := range []string{"49", "52", "already exists"} {
		if err == nil || !strings.Contains(err.Error(), text) {
			t.Fatalf("error=%v want %q", err, text)
		}
	}
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM schema_version WHERE version IN (49,51)").Scan(&count); err != nil || count != 0 {
		t.Fatalf("rows=%d err=%v", count, err)
	}
	if tableExists(t, s, "gap_rolled_back") || tableExists(t, s, "gap_later") {
		t.Fatal("failed transaction or later migration applied")
	}
	versionedMigrations[len(versionedMigrations)-2].ddl = "CREATE TABLE gap_rolled_back (id INTEGER);"
	if err := s.migrateThroughSupported(52, 52); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationGapsLegacyAndUnknown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("DELETE FROM schema_version WHERE version < 37"); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(); err != nil {
		t.Fatal(err)
	}
	pending, err := s.PendingSchemaVersions(context.Background())
	if err != nil || len(pending) != 0 {
		t.Fatalf("legacy pending=%v err=%v", pending, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.db.Exec("INSERT INTO schema_version VALUES (48)"); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(); err == nil || !strings.Contains(err.Error(), "newer than supported") {
		t.Fatalf("newer error=%v", err)
	}
	if err := s.migrateThroughSupported(52, 52); err == nil || !strings.Contains(err.Error(), "48") || !strings.Contains(err.Error(), "unregistered") {
		t.Fatalf("unknown error=%v", err)
	}
	if other, err := OpenMigrated(path); err == nil {
		other.Close()
		t.Fatal("OpenMigrated accepted newer database")
	}
}

func TestMigrationErrorsReachPublicOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.migrateThrough(17); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	setTestMigrations(t)
	for i := range versionedMigrations {
		if versionedMigrations[i].version == 18 {
			versionedMigrations[i].ddl = "CREATE TABLE buckets (id INTEGER);"
		}
	}
	other, err := OpenMigrated(path)
	if err == nil {
		other.Close()
		t.Fatal("OpenMigrated accepted failing migration")
	}
	for _, text := range []string{"18", "17", "already exists"} {
		if !strings.Contains(err.Error(), text) {
			t.Fatalf("error=%v want %q", err, text)
		}
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM schema_version WHERE version>=18").Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed open applied rows: %d %v", count, err)
	}
	if err := s.Migrate(); err == nil || !strings.Contains(err.Error(), "18") {
		t.Fatalf("Migrate error=%v", err)
	}
}

func TestPendingSchemaVersionsReadOnly(t *testing.T) {
	s, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	setTestMigrations(t, testMigration{52, ""}, testMigration{51, ""}, testMigration{49, ""})
	if _, err := s.db.Exec("INSERT INTO schema_version VALUES (52)"); err != nil {
		t.Fatal(err)
	}
	pending, err := s.PendingSchemaVersions(context.Background())
	if err != nil || !reflect.DeepEqual(pending, []int{49, 51}) {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM schema_version WHERE version IN (49,51)").Scan(&count); err != nil || count != 0 {
		t.Fatalf("pending mutated rows: %d %v", count, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.PendingSchemaVersions(ctx); err == nil {
		t.Fatal("ignored canceled context")
	}
}

func TestMigrationRegistrationOrder(t *testing.T) {
	prev := 1
	for _, m := range versionedMigrations {
		if m.version <= prev {
			t.Fatalf("versions not unique and ascending: %d after %d", m.version, prev)
		}
		prev = m.version
	}
	if prev != currentSchemaVersion {
		t.Fatalf("last version=%d supported=%d", prev, currentSchemaVersion)
	}
}
