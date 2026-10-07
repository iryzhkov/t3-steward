package sqlite

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
)

// rc.117 registers V39 (M16-6 continuation checkpoints) and V42 (C1 ownership
// leases). Each was first built on its own, so a coordinator database can
// reach this release at 37, at 42 without 39 (a build of C1 alone) or at 39
// without 42 (a build of M16-6 alone). Every one of them must end with both
// rows and both tables.
func TestRC117RegistryUpgradesEveryPredecessorDatabase(t *testing.T) {
	if currentSchemaVersion != 42 {
		t.Fatalf("currentSchemaVersion = %d, want 42", currentSchemaVersion)
	}
	var registered []int
	for _, migration := range versionedMigrations {
		if migration.version > legacyContiguousSchemaVersion {
			registered = append(registered, migration.version)
		}
	}
	if !reflect.DeepEqual(registered, []int{39, 42}) {
		t.Fatalf("versions above %d = %v, want [39 42]", legacyContiguousSchemaVersion, registered)
	}
	for _, test := range []struct {
		name  string
		extra []int
	}{
		{name: "rc.116 at 37"},
		{name: "C1 alone at 42", extra: []int{42}},
		{name: "M16-6 alone at 39", extra: []int{39}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			s, err := open(path, true)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.migrateThrough(legacyContiguousSchemaVersion); err != nil {
				t.Fatal(err)
			}
			for _, version := range test.extra {
				if err := s.applyVersionedMigration(version, registeredMigration(t, version)); err != nil {
					t.Fatal(err)
				}
			}
			pending, err := s.PendingSchemaVersions(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if want := missingAbove37(test.extra); !reflect.DeepEqual(pending, want) {
				t.Fatalf("pending = %v, want %v", pending, want)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			// The production path: open and migrate to the current registry.
			s, err = OpenMigrated(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			for _, version := range []int{37, 39, 42} {
				var rows int
				if err := s.db.QueryRow("SELECT COUNT(*) FROM schema_version WHERE version = ?", version).Scan(&rows); err != nil || rows != 1 {
					t.Fatalf("V%d rows = %d, %v; want 1", version, rows, err)
				}
			}
			for _, table := range []string{"coordinator_assignment_continuations", "coordinator_leases", "coordinator_lease_receipts"} {
				if !tableExists(t, s, table) {
					t.Fatalf("table %s is missing", table)
				}
			}
			if pending, err := s.PendingSchemaVersions(context.Background()); err != nil || len(pending) != 0 {
				t.Fatalf("pending after upgrade = %v, %v", pending, err)
			}
			if version := schemaVersionOf(t, s); version != 42 {
				t.Fatalf("schema version = %d, want 42", version)
			}
		})
	}
}

func registeredMigration(t *testing.T, version int) string {
	t.Helper()
	for _, migration := range versionedMigrations {
		if migration.version == version {
			return migration.ddl
		}
	}
	t.Fatalf("V%d is not registered", version)
	return ""
}

func missingAbove37(applied []int) []int {
	var missing []int
	for _, version := range []int{39, 42} {
		found := false
		for _, done := range applied {
			found = found || done == version
		}
		if !found {
			missing = append(missing, version)
		}
	}
	return missing
}
