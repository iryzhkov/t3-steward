package providercontainment

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/testutil"
)

func TestCanonicalRootResolvesSymlinkedParentOnly(t *testing.T) {
	base := testutil.RealTempDir(t)
	target := filepath.Join(base, "target")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	root, err := CanonicalRoot(filepath.Join(link, "contained"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err = privateDirectory(root); err != nil {
		t.Fatalf("canonicalised storage behind a symlinked parent refused: %v", err)
	}
	if err = privateDirectory(filepath.Join(link, "contained")); err == nil {
		t.Fatal("storage named through a symlinked parent accepted")
	} else if want := "supervisor state path contains a symlink: " + link + " is a symlink to " + target; err.Error() != want {
		t.Fatalf("symlinked parent error = %q, want %q", err, want)
	}

	// A symlink at the storage path itself is not resolved, so it stays refused.
	other := filepath.Join(target, "other")
	if err = os.Mkdir(other, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(other, filepath.Join(target, "planted")); err != nil {
		t.Fatal(err)
	}
	planted, err := CanonicalRoot(filepath.Join(link, "planted"))
	if err != nil {
		t.Fatal(err)
	}
	if err = privateDirectory(planted); err == nil {
		t.Fatal("symlinked storage accepted")
	}
	if _, err = CanonicalRoot("/"); err == nil {
		t.Fatal("filesystem root accepted as supervisor storage")
	}
}
