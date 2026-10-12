package sqlite

import (
	"fmt"
	"strings"
)

// downgradeRequiredAfter is fixed: increasing the supported version must not
// move the boundary at which migrations require a tested inverse.
const downgradeRequiredAfter = 42

// schemaMigration declares the upgrade and its inverse together. For versions
// above 42, downgradeDDL restores the schema at version-1. Version bookkeeping
// belongs to the framework, not to either SQL body.
type schemaMigration struct {
	version      int
	ddl          string
	downgradeDDL string
}

func (m schemaMigration) validateDowngrade() error {
	if m.version <= downgradeRequiredAfter {
		return fmt.Errorf("schema migration %d predates the downgrade framework", m.version)
	}
	if strings.TrimSpace(m.downgradeDDL) == "" {
		return fmt.Errorf("schema migration %d lacks a downgrade to %d", m.version, m.version-1)
	}
	return nil
}

// applyMigrationDowngrade is intentionally private; there is no operator
// downgrade command. DDL and removal of the version record commit together.
func (s *Store) applyMigrationDowngrade(m schemaMigration) error {
	if err := m.validateDowngrade(); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin schema downgrade %d: %w", m.version, err)
	}
	defer tx.Rollback()
	var version int
	if err := tx.QueryRow("SELECT MAX(version) FROM schema_version").Scan(&version); err != nil {
		return fmt.Errorf("read schema downgrade version: %w", err)
	}
	if version != m.version {
		return fmt.Errorf("cannot downgrade schema migration %d with highest applied version %d", m.version, version)
	}
	if _, err := tx.Exec(m.downgradeDDL); err != nil {
		return fmt.Errorf("apply schema downgrade %d: %w", m.version, err)
	}
	if _, err := tx.Exec("DELETE FROM schema_version WHERE version = ?", m.version); err != nil {
		return fmt.Errorf("remove schema version %d: %w", m.version, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schema downgrade %d: %w", m.version, err)
	}
	return nil
}
