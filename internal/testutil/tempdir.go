// Package testutil holds helpers that tests in several packages share.
package testutil

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// RealTempDir returns a new t.TempDir with every symbolic link in its path
// resolved. A test whose directory reaches code that refuses symlinks, such as
// a no-follow directory walk or a real-directory check, uses it instead of
// t.TempDir, so that the test still passes on a host whose TMPDIR is itself a
// symlink to a directory on another disk. Production checks are unchanged.
func RealTempDir(t testing.TB) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

// symlinkedTempDirChild is set in the environment of a test binary that
// RerunWithSymlinkedTempDir started, so that the child does not start another.
const symlinkedTempDirChild = "T3_STEWARD_TEST_SYMLINKED_TMPDIR"

// RerunWithSymlinkedTempDir runs the tests of the current package that match
// pattern again, in a child copy of the test binary whose TMPDIR is a symbolic
// link to a real directory, and fails t with the child's output when any of
// them fails. The child skips the calling test itself.
func RerunWithSymlinkedTempDir(t *testing.T, pattern string) {
	t.Helper()
	if os.Getenv(symlinkedTempDirChild) != "" {
		t.Skip("already running with a symlinked TMPDIR")
	}
	if testing.Short() {
		t.Skip("reruns tests in a child process")
	}
	real := filepath.Join(RealTempDir(t), "real")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(filepath.Dir(real), "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run="+pattern, "-test.count=1")
	child.Env = append(os.Environ(), "TMPDIR="+link, symlinkedTempDirChild+"=1")
	output, err := child.CombinedOutput()
	if err != nil {
		t.Fatalf("tests %s failed with TMPDIR a symlink to a real directory: %v\n%s", pattern, err, output)
	}
	if bytes.Contains(output, []byte("no tests to run")) {
		t.Fatalf("pattern %s matched no tests:\n%s", pattern, output)
	}
}
