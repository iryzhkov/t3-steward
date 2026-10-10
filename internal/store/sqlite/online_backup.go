package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// BackupOnline writes a coherent SQLite snapshot, including committed WAL data,
// without acquiring coordinator ownership, migrating, or writing the source.
// The destination must not exist. VACUUM INTO holds a SQLite read transaction.
func BackupOnline(ctx context.Context, source, destination string) error {
	info, err := os.Lstat(source)
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("online backup source must be an existing regular database")
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		return errors.New("online backup destination must not exist")
	}
	db, err := sql.Open("sqlite", sqliteFileURL(source)+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return fmt.Errorf("open online backup source: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "VACUUM INTO ?", destination); err != nil {
		return fmt.Errorf("SQLite online backup: %w", err)
	}
	if err := os.Chmod(destination, 0o600); err != nil {
		return err
	}
	file, err := os.OpenFile(destination, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

// SnapshotArtifacts reads the exact immutable object references belonging to
// this database snapshot, rather than a later state of the live catalog.
func (s *Store) SnapshotArtifacts(ctx context.Context) ([]domain.Artifact, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT record FROM coordinator_artifacts ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var artifacts []domain.Artifact
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var artifact domain.Artifact
		if err := json.Unmarshal(raw, &artifact); err != nil {
			return nil, fmt.Errorf("decode snapshot artifact: %w", err)
		}
		artifacts = append(artifacts, artifact)
	}
	return artifacts, rows.Err()
}

// SnapshotCounts provides bounded aggregate evidence for a scratch restore,
// without reconstructing runtime state or taking coordinator ownership.
func (s *Store) SnapshotCounts(ctx context.Context) (map[string]int64, error) {
	counts := make(map[string]int64)
	for _, entry := range []struct{ name, table string }{
		{"workflowRuns", "coordinator_workflow_runs"},
		{"tasks", "coordinator_tasks"},
		{"attempts", "coordinator_attempts"},
		{"assignments", "coordinator_assignments"},
		{"workers", "coordinator_worker_snapshots"},
		{"artifacts", "coordinator_artifacts"},
		{"schedules", "coordinator_schedules"},
		{"auditEvents", "coordinator_audit_events"},
	} {
		var count int64
		if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+entry.table).Scan(&count); err != nil {
			return nil, fmt.Errorf("count snapshot %s: %w", entry.name, err)
		}
		counts[entry.name] = count
	}
	return counts, nil
}
