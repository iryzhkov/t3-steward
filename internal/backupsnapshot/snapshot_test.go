package backupsnapshot

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	storesqlite "github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestSnapshotBackupRestoreDrill(t *testing.T) {
	root := t.TempDir()
	database := filepath.Join(root, "source", "state.db")
	artifacts := filepath.Join(root, "source-artifacts")
	snapshot := filepath.Join(root, "snapshots", "snapshot-1")
	if err := os.MkdirAll(filepath.Dir(database), 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := storesqlite.OpenMigrated(database)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetKV(context.Background(), "snapshot-test", "preserved"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(artifacts, "runs", "run-1"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(artifacts, "runs", "run-1", "result.txt"), []byte("verified artifact\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	manager := Manager{Limits: Limits{MaxFiles: 100, MaxBytes: 1 << 20}, Now: func() time.Time {
		return time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	}}
	manifest, err := manager.Create(context.Background(), database, artifacts, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != FormatVersion || manifest.SchemaVersion != storesqlite.CurrentSchemaVersion() || len(manifest.Files) != 2 {
		t.Fatalf("manifest = %#v", manifest)
	}
	if _, err := manager.Verify(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	restoredDatabase := filepath.Join(root, "restore", "state.db")
	restoredArtifacts := filepath.Join(root, "restore-artifacts")
	if err := os.MkdirAll(filepath.Dir(restoredDatabase), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Restore(context.Background(), snapshot, restoredDatabase, restoredArtifacts); err != nil {
		t.Fatal(err)
	}
	restored, err := storesqlite.Open(restoredDatabase)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	value, ok, err := restored.GetKV(context.Background(), "snapshot-test")
	if err != nil || !ok || value != "preserved" {
		t.Fatalf("restored kv = %q, %v, %v", value, ok, err)
	}
	raw, err := os.ReadFile(filepath.Join(restoredArtifacts, "runs", "run-1", "result.txt"))
	if err != nil || string(raw) != "verified artifact\n" {
		t.Fatalf("restored artifact = %q, %v", raw, err)
	}
	if info, err := os.Stat(restoredDatabase); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("restored database mode = %v, %v", info.Mode().Perm(), err)
	}
}

func TestSnapshotRefusesActiveCoordinatorAndUnsafeFiles(t *testing.T) {
	root, database, artifacts := snapshotFixture(t)
	store, err := storesqlite.OpenMigrated(database)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.AcquireCoordinator(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	manager := Manager{Limits: Limits{MaxFiles: 10, MaxBytes: 1 << 20}}
	if _, err := manager.Create(context.Background(), database, artifacts, filepath.Join(root, "snapshot")); !errors.Is(err, ErrActiveCoordinator) {
		t.Fatalf("active coordinator error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(database, filepath.Join(artifacts, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create(context.Background(), database, artifacts, filepath.Join(root, "snapshot-unsafe")); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("unsafe artifact error = %v", err)
	}
}

func TestSnapshotRefusesCanonicalPathOverlapThroughSymlinks(t *testing.T) {
	t.Run("create destination", func(t *testing.T) {
		root, database, artifacts := snapshotFixture(t)
		alias := filepath.Join(root, "artifact-alias")
		if err := os.Symlink(artifacts, alias); err != nil {
			t.Fatal(err)
		}
		manager := Manager{Limits: Limits{MaxFiles: 10, MaxBytes: 1 << 20}}
		if _, err := manager.Create(context.Background(), database, artifacts, filepath.Join(alias, "snapshot")); err == nil || !strings.Contains(err.Error(), "resolved database, artifact root, and snapshot destination must not overlap") {
			t.Fatalf("canonical create overlap error = %v", err)
		}
	})

	t.Run("restore target", func(t *testing.T) {
		root, database, artifacts := snapshotFixture(t)
		manager := Manager{Limits: Limits{MaxFiles: 10, MaxBytes: 1 << 20}}
		snapshot := filepath.Join(root, "snapshot")
		if _, err := manager.Create(context.Background(), database, artifacts, snapshot); err != nil {
			t.Fatal(err)
		}
		alias := filepath.Join(root, "snapshot-alias")
		if err := os.Symlink(snapshot, alias); err != nil {
			t.Fatal(err)
		}
		restoreParent := filepath.Join(root, "restore")
		if err := os.Mkdir(restoreParent, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := manager.Restore(context.Background(), snapshot, filepath.Join(restoreParent, "state.db"), filepath.Join(alias, "restored-artifacts")); err == nil || !strings.Contains(err.Error(), "resolved snapshot and restore targets must not overlap") {
			t.Fatalf("canonical restore overlap error = %v", err)
		}
	})
}

func TestSnapshotVerifyRefusesCorruptIncompleteAndMismatched(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{name: "corrupt", mutate: func(t *testing.T, snapshot string) {
			path := filepath.Join(snapshot, "artifacts", "artifact.txt")
			if err := os.Chmod(path, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("tampered"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "incomplete", mutate: func(t *testing.T, snapshot string) {
			path := filepath.Join(snapshot, "artifacts", "artifact.txt")
			if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "version mismatch", mutate: func(t *testing.T, snapshot string) {
			path := filepath.Join(snapshot, "manifest.json")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var manifest Manifest
			if err := json.Unmarshal(raw, &manifest); err != nil {
				t.Fatal(err)
			}
			manifest.Version++
			raw, _ = json.Marshal(manifest)
			if err := os.Chmod(path, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "schema mismatch", mutate: func(t *testing.T, snapshot string) {
			path := filepath.Join(snapshot, "manifest.json")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var manifest Manifest
			if err := json.Unmarshal(raw, &manifest); err != nil {
				t.Fatal(err)
			}
			manifest.SchemaVersion--
			raw, _ = json.Marshal(manifest)
			if err := os.Chmod(path, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unexpected", mutate: func(t *testing.T, snapshot string) {
			if err := os.Chmod(snapshot, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(snapshot, "extra"), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, database, artifacts := snapshotFixture(t)
			manager := Manager{Limits: Limits{MaxFiles: 10, MaxBytes: 1 << 20}}
			snapshot := filepath.Join(root, "snapshot")
			if _, err := manager.Create(context.Background(), database, artifacts, snapshot); err != nil {
				t.Fatal(err)
			}
			test.mutate(t, snapshot)
			if _, err := manager.Verify(context.Background(), snapshot); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("verify error = %v", err)
			}
		})
	}
}

func TestSnapshotRestoreRefusesExistingTargetsAndNewerDatabase(t *testing.T) {
	root, database, artifacts := snapshotFixture(t)
	manager := Manager{Limits: Limits{MaxFiles: 10, MaxBytes: 1 << 20}}
	snapshot := filepath.Join(root, "snapshot")
	if _, err := manager.Create(context.Background(), database, artifacts, snapshot); err != nil {
		t.Fatal(err)
	}
	restoreDB := filepath.Join(root, "restore", "state.db")
	if err := os.MkdirAll(filepath.Dir(restoreDB), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(restoreDB, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Restore(context.Background(), snapshot, restoreDB, filepath.Join(root, "restore-artifacts")); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("existing restore target error = %v", err)
	}
}

func snapshotFixture(t *testing.T) (string, string, string) {
	t.Helper()
	root := t.TempDir()
	database := filepath.Join(root, "state", "state.db")
	if err := os.MkdirAll(filepath.Dir(database), 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := storesqlite.OpenMigrated(database)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	artifacts := filepath.Join(root, "artifacts")
	if err := os.MkdirAll(artifacts, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(artifacts, "artifact.txt"), []byte("artifact"), 0o600); err != nil {
		t.Fatal(err)
	}
	return root, database, artifacts
}
