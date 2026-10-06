package sqlitetest

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// The template stands in for a freshly migrated database only if a fresh
// migration produces the same bytes. A schema change that made the result
// depend on when or where it ran would fail here before any fixture used it.
func TestImageMatchesAFreshMigration(t *testing.T) {
	image, err := Image()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "fresh.db")
	fresh, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := fresh.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, image) {
		t.Fatalf("fresh migration (%d bytes) differs from the template (%d bytes)", len(raw), len(image))
	}
}

func TestOpenMigratedSeedsPrivateIndependentCopies(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "nested", "state")
	first, err := OpenMigrated(filepath.Join(dir, "first.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := OpenMigrated(filepath.Join(dir, "second.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	for _, entry := range []struct {
		path string
		mode os.FileMode
	}{{dir, 0o700}, {filepath.Join(dir, "first.db"), 0o600}} {
		info, err := os.Stat(entry.path)
		if err != nil || info.Mode().Perm() != entry.mode {
			t.Fatalf("mode of %s = %v (%v), want %v", entry.path, info.Mode().Perm(), err, entry.mode)
		}
	}
	for _, store := range []*sqlite.Store{first, second} {
		if err := store.IntegrityCheck(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.SetKV(ctx, "isolated", "first"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := second.GetKV(ctx, "isolated"); err != nil || ok {
		t.Fatalf("second copy sees the first copy's write: ok=%v err=%v", ok, err)
	}
}

// An existing database is reopened, never replaced by the template.
func TestOpenMigratedKeepsAnExistingDatabase(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetKV(ctx, "kept", "yes"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if value, ok, err := reopened.GetKV(ctx, "kept"); err != nil || !ok || value != "yes" {
		t.Fatalf("reopened value = %q ok=%v err=%v", value, ok, err)
	}
}
