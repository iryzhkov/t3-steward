package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCoordinatorCloseCheckpointsBeforeReleasingOwnership(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireCoordinator(ctx, "maintenance-coordinator"); err != nil {
		t.Fatal(err)
	}
	// Keep another idle connection open: automatic last-connection SQLite
	// cleanup cannot make this test pass in place of the explicit checkpoint.
	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := store.SetKV(ctx, "shutdown-test", "durable"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path + "-wal")
	if err != nil || info.Size() == 0 {
		t.Fatalf("no WAL to checkpoint: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path + "-wal"); err == nil && info.Size() != 0 {
		t.Fatalf("WAL still has %d bytes", info.Size())
	} else if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
			t.Fatalf("shutdown left %s: %v", suffix, err)
		}
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	value, ok, err := reopened.GetKV(ctx, "shutdown-test")
	if err != nil || !ok || value != "durable" {
		t.Fatalf("lost write: %q %t %v", value, ok, err)
	}
	if _, err := reopened.AcquireCoordinator(ctx, "maintenance-coordinator"); err != nil {
		t.Fatalf("ownership not released: %v", err)
	}
}
