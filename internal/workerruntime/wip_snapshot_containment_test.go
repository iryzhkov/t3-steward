package workerruntime

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
)

// A work-in-progress snapshot runs Git on the worker host, outside the
// containment the attempt had. The task controls its repository's
// configuration and attributes, so a filter, a signing program or a work tree
// it names there must not run, or be read, with the worker's authority: here
// each one would write a host-only value into a marker outside the workspace,
// or put a host file into the snapshot. The snapshot still keeps the work, as
// the files are on disk.
func TestSnapshotRunsNoCommandTheTaskConfigured(t *testing.T) {
	const hostOnly = "synthetic-host-only-secret-012345"
	const reversed = "543210-terces-ylno-tsoh-citehtnys"
	leak := `printf '%s' "$RC116_HOST_ONLY" > "$RC116_MARKER"; printf 'transformed-%s' "$(printf '%s' "$RC116_HOST_ONLY" | rev)" >&2`
	for _, tc := range []struct {
		name      string
		configure func(t *testing.T, workspace, hostDir string)
	}{
		{name: "required clean filter", configure: func(t *testing.T, workspace, _ string) {
			writeTestFile(t, filepath.Join(workspace, ".gitattributes"), "a.txt filter=review\n")
			runGitForTest(t, workspace, "config", "filter.review.required", "true")
			runGitForTest(t, workspace, "config", "filter.review.clean", leak+"; exit 1")
		}},
		{name: "process filter named in info/attributes", configure: func(t *testing.T, workspace, _ string) {
			writeTestFile(t, filepath.Join(workspace, ".git", "info", "attributes"), "* filter=review\n")
			runGitForTest(t, workspace, "config", "filter.review.process", "sh -c '"+strings.ReplaceAll(leak, "'", `'\''`)+"; exit 1'")
		}},
		{name: "included configuration", configure: func(t *testing.T, workspace, hostDir string) {
			included := filepath.Join(hostDir, "included.config")
			writeTestFile(t, included, "[filter \"review\"]\n\trequired = true\n\tclean = "+strings.ReplaceAll(leak, `"`, `\"`)+"; exit 1\n")
			writeTestFile(t, filepath.Join(workspace, ".gitattributes"), "a.txt filter=review\n")
			runGitForTest(t, workspace, "config", "include.path", included)
		}},
		{name: "commit signing program", configure: func(t *testing.T, workspace, hostDir string) {
			program := filepath.Join(hostDir, "gpg")
			writeTestFile(t, program, "#!/bin/sh\n"+leak+"\nexit 1\n")
			if err := os.Chmod(program, 0o700); err != nil {
				t.Fatal(err)
			}
			runGitForTest(t, workspace, "config", "commit.gpgSign", "true")
			runGitForTest(t, workspace, "config", "gpg.program", program)
		}},
		{name: "work tree outside the workspace", configure: func(t *testing.T, workspace, hostDir string) {
			writeTestFile(t, filepath.Join(hostDir, "host-file.txt"), hostOnly+"\n")
			runGitForTest(t, workspace, "config", "core.worktree", hostDir)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWIPFixture(t, commitOutputs)
			// The attempt was contained: its package binds a directory resource.
			registration := directoryresource.Registration{WorkerID: "worker-1", ResourceID: "snapshot-directory", Revision: "1", Path: t.TempDir(), Writable: true}
			fd, identity, err := directoryresource.Open(registration)
			if err != nil {
				t.Fatal(err)
			}
			defer fd.Close()
			binding, err := directoryresource.Bind(identity, directoryresource.ReadWrite)
			if err != nil {
				t.Fatal(err)
			}
			f.pkg.Environment.DirectoryBindings = []directoryresource.Binding{binding}

			hostDir := t.TempDir()
			marker := filepath.Join(hostDir, "host-marker")
			t.Setenv("RC116_MARKER", marker)
			t.Setenv("RC116_HOST_ONLY", hostOnly)
			writeTestFile(t, filepath.Join(f.workspace, "a.txt"), "modified\n")
			tc.configure(t, f.workspace, hostDir)

			var logs bytes.Buffer
			f.driver.Log = slog.New(slog.NewTextHandler(&logs, nil))
			custody := testCustodyStore(t, filepath.Join(t.TempDir(), "custody"), func() time.Time { return runtimeTestNow })
			custody.config.SecretScan.StaticCanaries = []string{hostOnly}
			f.driver.Publisher = custody
			retained := f.driver.retainWorkInProgress(context.Background(), f.pkg, f.workspace)

			if raw, err := os.ReadFile(marker); err == nil {
				t.Fatalf("a command the task configured ran on the host and read its environment: %q", raw)
			}
			if strings.Contains(logs.String(), reversed) || strings.Contains(retained, reversed) {
				t.Fatalf("transformed host value escaped the scanner; retained=%q\n%s", retained, logs.String())
			}
			if !strings.HasPrefix(retained, "wip.bundle retained: refs/steward/wip/attempt-1") {
				t.Fatalf("the snapshot was not kept: %q\n%s", retained, logs.String())
			}
			bundle := filepath.Join(f.driver.workspacePath(f.pkg), WorkInProgressBundleFile)
			verify := filepath.Join(t.TempDir(), "verify")
			runGitForTest(t, filepath.Dir(verify), "clone", "-q", f.upstream, verify)
			runGitForTest(t, verify, "fetch", "-q", bundle, "refs/steward/wip/attempt-1:refs/heads/wip")
			if got := runGitForTest(t, verify, "show", "wip:a.txt"); got != "modified" {
				t.Fatalf("a.txt in the snapshot = %q, want the file as it is on disk", got)
			}
			if files := runGitForTest(t, verify, "ls-tree", "-r", "--name-only", "wip"); strings.Contains(files, "host-file") {
				t.Fatalf("the snapshot carries a host file:\n%s", files)
			}
		})
	}
}

// Objects borrowed from another repository, or a .git that points elsewhere,
// would make the worker read objects the task chose from outside the
// workspace into the bundle, so such a workspace is not snapshotted.
func TestSnapshotRefusesARepositoryOutsideTheWorkspace(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(t *testing.T, f wipFixture)
		want      string
	}{
		{name: "alternates", want: "borrows objects", configure: func(t *testing.T, f wipFixture) {
			writeTestFile(t, filepath.Join(f.workspace, ".git", "objects", "info", "alternates"), filepath.Join(f.upstream, ".git", "objects")+"\n")
		}},
		{name: "git file", want: "not a directory", configure: func(t *testing.T, f wipFixture) {
			moved := filepath.Join(t.TempDir(), "moved.git")
			if err := os.Rename(filepath.Join(f.workspace, ".git"), moved); err != nil {
				t.Fatal(err)
			}
			writeTestFile(t, filepath.Join(f.workspace, ".git"), "gitdir: "+moved+"\n")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWIPFixture(t, commitOutputs)
			writeTestFile(t, filepath.Join(f.workspace, "a.txt"), "modified\n")
			tc.configure(t, f)
			summary, err := f.driver.SnapshotWorkInProgress(context.Background(), f.pkg, f.workspace)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("snapshot = %q, %v; want a refusal saying %q", summary, err, tc.want)
			}
			if _, err := os.Stat(filepath.Join(f.driver.workspacePath(f.pkg), WorkInProgressBundleFile)); err == nil {
				t.Fatal("a bundle was kept")
			}
		})
	}
}
