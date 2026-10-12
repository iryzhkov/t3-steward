package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// This test scans the production registry, independently of synthetic fixtures.
func TestMigrationDowngradesRequired(t *testing.T) {
	for _, m := range versionedMigrations {
		if m.version > downgradeRequiredAfter {
			if err := m.validateDowngrade(); err != nil {
				t.Error(err)
			}
		}
	}
}

func TestMigrationDowngradeSchemaParity(t *testing.T) {
	for _, m := range versionedMigrations {
		if m.version > downgradeRequiredAfter {
			t.Run(fmt.Sprint(m.version), func(t *testing.T) {
				if err := checkMigrationDowngrade(t, m); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// No production migration above 42 exists yet. This fixture exercises table,
// index, view, trigger and existing-table changes through the same runner.
func syntheticReversibleMigration() schemaMigration {
	return schemaMigration{
		version: currentSchemaVersion + 1,
		ddl: `
CREATE TABLE downgrade_fixture (id INTEGER PRIMARY KEY, value TEXT);
CREATE INDEX downgrade_fixture_value ON downgrade_fixture(value);
CREATE VIEW downgrade_fixture_view AS SELECT value FROM downgrade_fixture;
CREATE TRIGGER downgrade_fixture_trigger AFTER INSERT ON downgrade_fixture
BEGIN UPDATE downgrade_fixture SET value = 'fixture' WHERE id = NEW.id; END;
ALTER TABLE buckets ADD COLUMN downgrade_fixture TEXT;
`,
		downgradeDDL: `
DROP TRIGGER downgrade_fixture_trigger;
DROP VIEW downgrade_fixture_view;
DROP TABLE downgrade_fixture;
ALTER TABLE buckets DROP COLUMN downgrade_fixture;
`,
	}
}

func TestMigrationDowngradeFramework(t *testing.T) {
	m := syntheticReversibleMigration()
	old := versionedMigrations
	versionedMigrations = append(append([]schemaMigration(nil), old...), m)
	t.Cleanup(func() { versionedMigrations = old })
	t.Run("round trip", func(t *testing.T) {
		if err := checkMigrationDowngrade(t, m); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("missing inverse", func(t *testing.T) {
		missing := m
		missing.downgradeDDL = " \n\t"
		if err := missing.validateDowngrade(); err == nil || !strings.Contains(err.Error(), "lacks a downgrade") {
			t.Fatalf("missing inverse error = %v", err)
		}
	})
	t.Run("incorrect inverse detected", func(t *testing.T) {
		wrong := m
		wrong.downgradeDDL = "SELECT 1;"
		if err := checkMigrationDowngrade(t, wrong); err == nil || !strings.Contains(err.Error(), "schema differs") {
			t.Fatalf("incorrect inverse error = %v", err)
		}
	})
	t.Run("rollback", func(t *testing.T) {
		s := migrationDowngradeFixture(t, m.version, m.version)
		before := normalizedMigrationSchema(t, s)
		broken := m
		broken.downgradeDDL = "DROP VIEW downgrade_fixture_view; DROP TABLE nonexistent_downgrade_table;"
		if err := s.applyMigrationDowngrade(broken); err == nil {
			t.Fatal("invalid SQL succeeded")
		}
		if got := normalizedMigrationSchema(t, s); !reflect.DeepEqual(got, before) {
			t.Fatal("failed downgrade changed schema")
		}
		if version, err := s.SchemaVersion(context.Background()); err != nil || version != m.version {
			t.Fatalf("failed downgrade version=%d error=%v", version, err)
		}
	})
}

// Each side starts with its own empty fixture database and runs the real
// upgrade path. The expected side stops at N-1, never using downgrade SQL.
func checkMigrationDowngrade(t *testing.T, m schemaMigration) error {
	t.Helper()
	if err := m.validateDowngrade(); err != nil {
		return err
	}
	expected := migrationDowngradeFixture(t, m.version-1, m.version)
	actual := migrationDowngradeFixture(t, m.version, m.version)
	if err := actual.applyMigrationDowngrade(m); err != nil {
		return err
	}
	if got, want := normalizedMigrationSchema(t, actual), normalizedMigrationSchema(t, expected); !reflect.DeepEqual(got, want) {
		return fmt.Errorf("downgrade %d schema differs from migration through %d:\ngot %v\nwant %v", m.version, m.version-1, got, want)
	}
	got, err := actual.SchemaVersion(context.Background())
	if err != nil {
		return err
	}
	want, err := expected.SchemaVersion(context.Background())
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("downgrade %d recorded version = %d, want %d", m.version, got, want)
	}
	// The version row must be removed, so the ordinary upgrade can run again.
	if err := actual.migrateThroughSupported(m.version, m.version); err != nil {
		return fmt.Errorf("re-upgrade %d: %w", m.version, err)
	}
	if version, err := actual.SchemaVersion(context.Background()); err != nil || version != m.version {
		return fmt.Errorf("re-upgrade version=%d error=%v", version, err)
	}
	return nil
}

func migrationDowngradeFixture(t *testing.T, target, supported int) *Store {
	t.Helper()
	s, err := open(filepath.Join(t.TempDir(), "state.db"), true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.migrateThroughSupported(target, supported); err != nil {
		t.Fatal(err)
	}
	return s
}

// sqlite_master describes logical schema. Sort by object identity, omit physical
// root pages and trim outer SQL whitespace; preserve literals and all objects,
// including SQLite's implicit indexes, so meaningful differences remain visible.
func normalizedMigrationSchema(t *testing.T, s *Store) []string {
	t.Helper()
	rows, err := s.db.Query("SELECT type, name, tbl_name, coalesce(sql, '') FROM sqlite_master ORDER BY type, name")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var kind, name, table, ddl string
		if err := rows.Scan(&kind, &name, &table, &ddl); err != nil {
			t.Fatal(err)
		}
		result = append(result, fmt.Sprintf("%q|%q|%q|%q", kind, name, table, strings.TrimSpace(ddl)))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}
