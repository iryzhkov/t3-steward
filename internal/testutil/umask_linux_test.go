//go:build linux

package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrivateTempDirUnderUmasks(t *testing.T) {
	RerunWithUmasks(t, "^TestPrivateTempDirHasNoSymlinks$")
}

func TestPrivateTempDirHasNoSymlinks(t *testing.T) {
	dir := PrivateTempDir(t)
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil || resolved != dir {
		t.Fatalf("private directory = %q, resolves to %q (%v)", dir, resolved, err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("private directory mode = %s, want 0700", info.Mode())
	}
	t.Logf("private directory mode=%04o", info.Mode().Perm())
}
