package backlog

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// The installed worker runs with UMask=0077. Git creates checked-out files
// with the inherited umask, so a workspace prepared by the worker lost the
// modes Git records (0644 became 0600, 0755 became 0700) and repository tests
// that check file modes failed only inside campaigns. The checkout must carry
// Git's modes while the workspace root and Steward-private directories stay
// owner-only.
//
// This test changes the process umask, so it must never call t.Parallel.
func TestWorkspacePreparerKeepsGitFileModesUnderOwnerOnlyUmask(t *testing.T) {
	repository := newGitFixture(t)
	writeGitFile(t, repository, "script.sh", "#!/bin/sh\n")
	if err := os.Chmod(filepath.Join(repository, "script.sh"), 0o755); err != nil {
		t.Fatalf("make fixture executable: %v", err)
	}
	writeGitFile(t, repository, ".t3/tracked.txt", "tracked metadata\n")
	gitRun(t, repository, "add", "script.sh", ".t3/tracked.txt")
	gitRun(t, repository, "commit", "-m", "modes")
	commit := gitOutput(t, repository, "rev-parse", "HEAD")

	previous := syscall.Umask(0o077)
	t.Cleanup(func() { syscall.Umask(previous) })

	runsRoot := t.TempDir()
	prepared, err := workspacePreparer(runsRoot, t.TempDir()).Prepare(context.Background(),
		workspaceRequest(repository, commit, workspaceTask("task-id", "modes"), "attempt-1"))
	if err != nil {
		t.Fatalf("prepare workspace: %v", err)
	}
	cleanupImmutable(t, prepared.RootDir)

	for _, want := range []struct {
		path string
		mode os.FileMode
	}{
		{filepath.Join(prepared.WorkspaceDir, "version.txt"), 0o644},
		{filepath.Join(prepared.WorkspaceDir, "script.sh"), 0o755},
		{prepared.WorkspaceDir, 0o700},
		{filepath.Join(prepared.WorkspaceDir, ".t3"), 0o700},
		{prepared.RootDir, 0o700},
	} {
		info, err := os.Lstat(want.path)
		if err != nil {
			t.Fatalf("inspect %s: %v", want.path, err)
		}
		if got := info.Mode().Perm(); got != want.mode {
			t.Errorf("%s mode = %#o, want %#o", want.path, got, want.mode)
		}
	}
}
