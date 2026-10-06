package backlog

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The installed worker runs with UMask=0077. Git creates checked-out files
// with the inherited umask, so a workspace prepared by the worker lost the
// modes Git records (0644 became 0600, 0755 became 0700) and repository tests
// that check file modes failed only inside campaigns. The checkout must carry
// Git's modes while the workspace root and Steward-private directories stay
// owner-only.
func TestWorkspacePreparerKeepsGitFileModesUnderOwnerOnlyUmask(t *testing.T) {
	if runModeTestUnderWorkerUmask(t) {
		return
	}
	repository := newGitFixture(t)
	writeGitFile(t, repository, "script.sh", "#!/bin/sh\n")
	if err := os.Chmod(filepath.Join(repository, "script.sh"), 0o755); err != nil {
		t.Fatalf("make fixture executable: %v", err)
	}
	writeGitFile(t, repository, ".t3/tracked.txt", "tracked metadata\n")
	writeGitFile(t, repository, ".t3-steward/tracked.txt", "tracked identity metadata\n")
	gitRun(t, repository, "add", "script.sh", ".t3/tracked.txt", ".t3-steward/tracked.txt")
	gitRun(t, repository, "commit", "-m", "modes")
	commit := gitOutput(t, repository, "rev-parse", "HEAD")

	runsRoot := t.TempDir()
	prepared, err := workspacePreparer(runsRoot, t.TempDir()).Prepare(context.Background(),
		workspaceRequest(repository, commit, workspaceTask("task-id", "modes"), "attempt-1"))
	if err != nil {
		t.Fatalf("prepare workspace: %v", err)
	}
	cleanupImmutable(t, prepared.RootDir)
	privateFile := filepath.Join(prepared.RootDir, "worker-created.txt")
	if err := os.WriteFile(privateFile, nil, 0o666); err != nil {
		t.Fatal(err)
	}

	for _, want := range []struct {
		path string
		mode os.FileMode
	}{
		{filepath.Join(prepared.WorkspaceDir, "version.txt"), 0o644},
		{filepath.Join(prepared.WorkspaceDir, "script.sh"), 0o755},
		{prepared.WorkspaceDir, 0o700},
		{filepath.Join(prepared.WorkspaceDir, ".t3"), 0o700},
		{filepath.Join(prepared.WorkspaceDir, ".t3-steward"), 0o700},
		{prepared.RootDir, 0o700},
		{prepared.InputsDir, 0o500},
		{prepared.DependenciesDir, 0o500},
		{prepared.PreparationLog, 0o400},
		{privateFile, 0o600},
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

func TestWorkspaceMetadataProtectionRefusesInvalidIdentityDirectory(t *testing.T) {
	for _, kind := range []string{"file", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			workspace := t.TempDir()
			path := filepath.Join(workspace, ".t3-steward")
			target := t.TempDir()
			// This exact mode proves chmod did not follow the symlink, even
			// when the suite itself runs under an owner-only umask.
			if err := os.Chmod(target, 0o755); err != nil {
				t.Fatal(err)
			}
			if kind == "file" {
				if err := os.WriteFile(path, []byte("not a directory"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
			err := exposeWorkspaceInputs(workspace)
			if err == nil || !strings.Contains(err.Error(), "repository .t3-steward is not a real directory") {
				t.Fatalf("invalid identity metadata error = %v", err)
			}
			info, err := os.Stat(target)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != 0o755 {
				t.Fatalf("outside directory mode = %#o, want unchanged 0755", info.Mode().Perm())
			}
		})
	}
}
