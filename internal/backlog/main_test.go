package backlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestMain points every Git process the tests start at a global configuration
// that keeps automatic maintenance in the foreground. The worker's own Git
// commands get that setting from gitForegroundMaintenance, but the tests'
// fixture commands (git commit in gitRun) and a receive-pack serving a local
// push (which Git starts without the pushing command's configuration
// environment) would otherwise start a detached git maintenance run that goes
// on writing into a repository's objects directory while the test removes the
// tree, failing the cleanup with "directory not empty". A test that sets its
// own GIT_CONFIG_GLOBAL replaces this file for its duration.
func TestMain(m *testing.M) {
	os.Exit(runWithForegroundGitMaintenance(m))
}

func runWithForegroundGitMaintenance(m *testing.M) int {
	dir, err := os.MkdirTemp("", "backlog-test-gitconfig-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "gitconfig")
	if err := os.WriteFile(path, []byte("[gc]\n\tautoDetach = false\n[maintenance]\n\tautoDetach = false\n"), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := os.Setenv("GIT_CONFIG_GLOBAL", path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return m.Run()
}
