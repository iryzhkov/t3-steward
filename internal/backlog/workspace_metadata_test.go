package backlog

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const hostSentinelBytes = "original host bytes"

// hostSentinel creates an owner-writable file outside every workspace, which
// is what a repository author would point a tracked symlink at.
func hostSentinel(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "host-sentinel")
	if err := os.WriteFile(path, []byte(hostSentinelBytes), 0o640); err != nil {
		t.Fatal(err)
	}
	// The exact mode proves nothing chmodded through the link, whatever umask
	// the suite runs under.
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertHostSentinelUnchanged(t *testing.T, path string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != hostSentinelBytes {
		t.Fatalf("host sentinel bytes = %q, want %q", got, hostSentinelBytes)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o640 {
		t.Fatalf("host sentinel mode = %#o, want unchanged 0640", info.Mode().Perm())
	}
}

// R-13: a symlink at .t3/base-commit must not redirect the worker's write to a
// file outside the workspace. This is the audit's TestAuditBaseCommitSymlinkBoundary.
func TestWorkspaceBaseCommitIsNotWrittenThroughASymlink(t *testing.T) {
	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, ".t3"), 0o700); err != nil {
		t.Fatal(err)
	}
	target := hostSentinel(t)
	if err := os.Symlink(target, filepath.Join(workspace, ".t3", "base-commit")); err != nil {
		t.Fatal(err)
	}
	err := writeWorkspaceBaseCommit(workspace, "c05398300a338962a1464cb032277e2235d954c5")
	assertHostSentinelUnchanged(t, target)
	if err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("writer error = %v, want a refusal naming the symbolic link", err)
	}
}

// A repository that tracks .t3/base-commit as a symlink to a host file, or as
// something other than a regular file, fails preparation and leaves the host
// file's bytes and mode alone.
func TestWorkspacePreparerRefusesTrackedBaseCommitThatIsNotARegularFile(t *testing.T) {
	for _, kind := range []string{"symlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			target := hostSentinel(t)
			repository := newGitFixture(t)
			if kind == "symlink" {
				if err := os.MkdirAll(filepath.Join(repository, ".t3"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, filepath.Join(repository, ".t3", "base-commit")); err != nil {
					t.Fatal(err)
				}
			} else {
				writeGitFile(t, repository, ".t3/base-commit/tracked.txt", "tracked\n")
			}
			gitRun(t, repository, "add", ".t3")
			gitRun(t, repository, "commit", "-m", "metadata "+kind)
			commit := gitOutput(t, repository, "rev-parse", "HEAD")

			runsRoot := t.TempDir()
			prepared, err := workspacePreparer(runsRoot, t.TempDir()).Prepare(context.Background(),
				workspaceRequest(repository, commit, workspaceTask("task-id", "metadata"), "attempt-1"))
			if prepared.RootDir != "" {
				cleanupImmutable(t, prepared.RootDir)
			}
			assertHostSentinelUnchanged(t, target)
			want := map[string]string{"symlink": "symbolic link", "directory": "not a regular file"}[kind]
			if err == nil || !strings.Contains(err.Error(), ".t3/base-commit") || !strings.Contains(err.Error(), want) {
				t.Fatalf("prepare error = %v, want a refusal naming .t3/base-commit and %q", err, want)
			}
		})
	}
}

// A workspace without links still records exactly the pinned commit, owner
// read-only, and a tracked regular base-commit file is replaced rather than
// kept with the repository's bytes or mode.
func TestWorkspaceBaseCommitReplacesARegularFile(t *testing.T) {
	const commit = "c05398300a338962a1464cb032277e2235d954c5"
	for _, existing := range []bool{false, true} {
		workspace := t.TempDir()
		if err := os.Mkdir(filepath.Join(workspace, ".t3"), 0o700); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(workspace, ".t3", "base-commit")
		if existing {
			if err := os.WriteFile(path, []byte("repository bytes\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if err := writeWorkspaceBaseCommit(workspace, commit); err != nil {
			t.Fatalf("existing=%t: %v", existing, err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != commit+"\n" {
			t.Fatalf("existing=%t: base commit bytes = %q", existing, raw)
		}
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o400 {
			t.Fatalf("existing=%t: base commit mode = %v, want a regular 0400 file", existing, info.Mode())
		}
		entries, err := os.ReadDir(filepath.Join(workspace, ".t3"))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 {
			t.Fatalf("existing=%t: .t3 holds %d entries, want only base-commit (no staged leftovers)", existing, len(entries))
		}
		if got, err := WorkspaceBaseCommit(workspace); err != nil || got != commit {
			t.Fatalf("existing=%t: WorkspaceBaseCommit = %q, %v", existing, got, err)
		}
	}
}
