package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRealTempDirHasNoSymlinks(t *testing.T) {
	dir := RealTempDir(t)
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil || resolved != dir {
		t.Fatalf("RealTempDir = %q, resolves to %q (%v)", dir, resolved, err)
	}
}

// TestTempDirIsALinkInTheChild runs only in the child of the rerun below,
// where it proves that the child really has a symlinked TMPDIR.
func TestTempDirIsALinkInTheChild(t *testing.T) {
	if os.Getenv(symlinkedTempDirChild) == "" {
		t.Skip("runs in the rerun child only")
	}
	info, err := os.Lstat(os.Getenv("TMPDIR"))
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("TMPDIR %q is not a symlink: %v", os.Getenv("TMPDIR"), err)
	}
	if dir := RealTempDir(t); filepath.Dir(dir) == os.Getenv("TMPDIR") {
		t.Fatalf("RealTempDir %q kept the link", dir)
	}
}

func TestRerunWithSymlinkedTempDir(t *testing.T) {
	RerunWithSymlinkedTempDir(t, "^Test(RealTempDirHasNoSymlinks|TempDirIsALinkInTheChild)$")
}
