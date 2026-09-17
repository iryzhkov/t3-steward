package backupsnapshot

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	storesqlite "github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// A real coordinator retains thousands of artifact files, and the manifest that
// describes them outgrew the flat megabyte the reader allowed. Create wrote a
// complete snapshot, Verify read a truncated manifest, and the drill failed on
// a snapshot that was in fact intact.
func TestAManifestLargerThanAMegabyteStillVerifies(t *testing.T) {
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
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Each entry costs roughly two hundred bytes of manifest, so seven thousand
	// files - about what this fleet's coordinator holds - is well past a
	// megabyte.
	const files = 7000
	directory := filepath.Join(artifacts, "runs", "run-1")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	for index := range files {
		name := filepath.Join(directory, fmt.Sprintf("artifact-%06d-with-a-realistic-name.json", index))
		if err := os.WriteFile(name, []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	manager := Manager{Limits: Limits{MaxFiles: 1 << 20, MaxBytes: 1 << 30}, Now: func() time.Time {
		return time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	}}
	created, err := manager.Create(context.Background(), database, artifacts, snapshot)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(created.Files) != files+1 {
		t.Fatalf("manifest describes %d files, want %d", len(created.Files), files+1)
	}
	info, err := os.Stat(filepath.Join(snapshot, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() <= 1<<20 {
		t.Fatalf("manifest is %d bytes, which no longer exercises the old bound", info.Size())
	}

	verified, err := manager.Verify(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("verify a snapshot whose manifest exceeds a megabyte: %v", err)
	}
	if len(verified.Files) != len(created.Files) {
		t.Fatalf("verified %d files, created %d", len(verified.Files), len(created.Files))
	}
}
