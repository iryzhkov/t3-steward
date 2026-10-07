package workerruntime

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const foreignSecret = "foreign-host-only-secret-9f8e7d"

// foreignRepository is a repository outside the workspace whose one commit
// holds a synthetic secret, with its objects loose or, when packed, in a
// pack.
func foreignRepository(t *testing.T, packed bool) (string, string) {
	t.Helper()
	foreign := filepath.Join(t.TempDir(), "foreign")
	runGitForTest(t, filepath.Dir(foreign), "init", "-q", "-b", "main", foreign)
	writeTestFile(t, filepath.Join(foreign, "secret.txt"), foreignSecret+"\n")
	runGitForTest(t, foreign, "add", "secret.txt")
	runGitForTest(t, foreign, "commit", "-q", "-m", "foreign")
	if packed {
		runGitForTest(t, foreign, "repack", "-adq")
	}
	return foreign, runGitForTest(t, foreign, "rev-parse", "HEAD")
}

// bundleCarriesForeignSecret reports whether the retained bundle holds the
// foreign repository's secret.
func bundleCarriesForeignSecret(t *testing.T, f wipFixture, foreignHead string) bool {
	t.Helper()
	bundle := filepath.Join(f.driver.workspacePath(f.pkg), WorkInProgressBundleFile)
	if _, err := os.Stat(bundle); err != nil {
		return false
	}
	verify := filepath.Join(t.TempDir(), "verify")
	runGitForTest(t, filepath.Dir(verify), "init", "-q", verify)
	runGitForTest(t, verify, "fetch", "-q", bundle, "refs/steward/wip/attempt-1:refs/heads/wip")
	show := exec.Command("git", "-C", verify, "show", foreignHead+":secret.txt")
	out, _ := show.CombinedOutput()
	return strings.Contains(string(out), foreignSecret)
}

// The snapshot reads only the workspace's own objects: a link inside
// .git/objects to another repository's loose objects or pack, which the task
// can create without being able to read its target, is never followed, and
// an alternate the task writes while the snapshot runs is never read.
func TestSnapshotReadsNoObjectFromOutsideTheWorkspace(t *testing.T) {
	for _, tc := range []struct {
		name      string
		packed    bool
		configure func(t *testing.T, f wipFixture, foreign, foreignHead string)
	}{
		{name: "linked loose objects", configure: func(t *testing.T, f wipFixture, foreign, _ string) {
			objects := filepath.Join(foreign, ".git", "objects")
			dirs, _ := os.ReadDir(objects)
			for _, dir := range dirs {
				if len(dir.Name()) != 2 {
					continue
				}
				target := filepath.Join(f.workspace, ".git", "objects", dir.Name())
				if err := os.MkdirAll(target, 0o755); err != nil {
					t.Fatal(err)
				}
				files, _ := os.ReadDir(filepath.Join(objects, dir.Name()))
				for _, file := range files {
					if err := os.Symlink(filepath.Join(objects, dir.Name(), file.Name()), filepath.Join(target, file.Name())); err != nil && !os.IsExist(err) {
						t.Fatal(err)
					}
				}
			}
		}},
		{name: "linked pack", packed: true, configure: func(t *testing.T, f wipFixture, foreign, _ string) {
			packs, _ := filepath.Glob(filepath.Join(foreign, ".git", "objects", "pack", "pack-*"))
			for _, pack := range packs {
				if err := os.Symlink(pack, filepath.Join(f.workspace, ".git", "objects", "pack", filepath.Base(pack))); err != nil {
					t.Fatal(err)
				}
			}
		}},
		{name: "alternate written during the snapshot", packed: true, configure: func(t *testing.T, f wipFixture, foreign, _ string) {
			// A task process still running writes the alternate when the
			// snapshot's first git command starts, after any check.
			real, err := exec.LookPath("git")
			if err != nil {
				t.Fatal(err)
			}
			bin := t.TempDir()
			once := filepath.Join(bin, "done")
			script := "#!/bin/sh\nif [ ! -e " + once + " ]; then : > " + once + "; printf '%s\\n' " + filepath.Join(foreign, ".git", "objects") +
				" > " + filepath.Join(f.workspace, ".git", "objects", "info", "alternates") + "; fi\nexec " + real + " \"$@\"\n"
			writeTestFile(t, filepath.Join(bin, "git"), script)
			if err := os.Chmod(filepath.Join(bin, "git"), 0o700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWIPFixture(t, commitOutputs)
			foreign, foreignHead := foreignRepository(t, tc.packed)
			tc.configure(t, f, foreign, foreignHead)
			writeTestFile(t, filepath.Join(f.workspace, ".git", "HEAD"), foreignHead+"\n")
			writeTestFile(t, filepath.Join(f.workspace, "a.txt"), "modified\n")
			summary, err := f.driver.SnapshotWorkInProgress(context.Background(), f.pkg, f.workspace)
			if bundleCarriesForeignSecret(t, f, foreignHead) {
				t.Fatalf("the snapshot carried another repository's objects: %q %v", summary, err)
			}
		})
	}
}

// Ignore rules the snapshot cannot read fail it, rather than letting the
// files they ignore into the bundle.
func TestSnapshotFailsOnAnUnreadableExcludeFile(t *testing.T) {
	f := newWIPFixture(t, commitOutputs)
	writeTestFile(t, filepath.Join(f.workspace, ".git", "info", "exclude"), strings.Repeat("#\n", 9<<20)+"ignored.env\n")
	writeTestFile(t, filepath.Join(f.workspace, "ignored.env"), "ignored\n")
	writeTestFile(t, filepath.Join(f.workspace, "a.txt"), "modified\n")
	summary, err := f.driver.SnapshotWorkInProgress(context.Background(), f.pkg, f.workspace)
	if err == nil || !strings.Contains(err.Error(), "info/exclude") {
		t.Fatalf("snapshot = %q, %v; want a failure naming info/exclude", summary, err)
	}
}

// The snapshot's view of a clean tree matches Git's in the workspace: an
// unborn branch with nothing written, a file a filter checked out (as Git LFS
// does), and a sparse checkout whose other paths are not on disk.
func TestSnapshotSeesTheCleanTreeTheTaskSees(t *testing.T) {
	t.Run("unborn branch", func(t *testing.T) {
		f := newWIPFixture(t, commitOutputs)
		runGitForTest(t, f.workspace, "checkout", "-q", "--orphan", "fresh")
		runGitForTest(t, f.workspace, "rm", "-rq", "--cached", ".")
		for _, name := range []string{"a.txt", "b.txt"} {
			if err := os.Remove(filepath.Join(f.workspace, name)); err != nil {
				t.Fatal(err)
			}
		}
		summary, err := f.driver.SnapshotWorkInProgress(context.Background(), f.pkg, f.workspace)
		if err != nil || !strings.Contains(summary, "clean") {
			t.Fatalf("snapshot = %q, %v", summary, err)
		}
	})
	t.Run("filtered file", func(t *testing.T) {
		f := newWIPFixture(t, commitOutputs)
		runGitForTest(t, f.workspace, "config", "filter.x.clean", "sed s/^SMUDGED-//")
		runGitForTest(t, f.workspace, "config", "filter.x.smudge", "sed s/^/SMUDGED-/")
		writeTestFile(t, filepath.Join(f.workspace, ".gitattributes"), "*.bin filter=x\n")
		writeTestFile(t, filepath.Join(f.workspace, "p.bin"), "pointer\n")
		runGitForTest(t, f.workspace, "add", ".gitattributes", "p.bin")
		runGitForTest(t, f.workspace, "commit", "-q", "-m", "filtered")
		if err := os.Remove(filepath.Join(f.workspace, "p.bin")); err != nil {
			t.Fatal(err)
		}
		runGitForTest(t, f.workspace, "checkout", "--", "p.bin")
		// The file was checked out a while ago, and Git in the workspace has
		// recorded that it is unchanged since; a file written in the same
		// instant as the index is racy and read again by any Git.
		past := time.Now().Add(-time.Minute)
		if err := os.Chtimes(filepath.Join(f.workspace, "p.bin"), past, past); err != nil {
			t.Fatal(err)
		}
		runGitForTest(t, f.workspace, "update-index", "-q", "--really-refresh")
		if status := runGitForTest(t, f.workspace, "status", "--porcelain", "--", ".", ":(exclude).t3"); status != "" {
			t.Fatalf("setup is not clean: %q", status)
		}
		summary, err := f.driver.SnapshotWorkInProgress(context.Background(), f.pkg, f.workspace)
		if err != nil || !strings.Contains(summary, "clean") {
			t.Fatalf("snapshot = %q, %v", summary, err)
		}
	})
	t.Run("sparse checkout", func(t *testing.T) {
		f := newWIPFixture(t, commitOutputs)
		writeTestFile(t, filepath.Join(f.workspace, "sub", "s.txt"), "sparse\n")
		runGitForTest(t, f.workspace, "add", "sub/s.txt")
		runGitForTest(t, f.workspace, "commit", "-q", "-m", "sparse")
		runGitForTest(t, f.workspace, "sparse-checkout", "set", "--no-cone", "/a.txt", "/b.txt")
		if _, err := os.Stat(filepath.Join(f.workspace, "sub", "s.txt")); err == nil {
			t.Fatal("sub/s.txt is still on disk")
		}
		writeTestFile(t, filepath.Join(f.workspace, "a.txt"), "modified\n")
		head := runGitForTest(t, f.workspace, "rev-parse", "HEAD")
		if _, err := f.driver.SnapshotWorkInProgress(context.Background(), f.pkg, f.workspace); err != nil {
			t.Fatal(err)
		}
		bundle := filepath.Join(f.driver.workspacePath(f.pkg), WorkInProgressBundleFile)
		verify := filepath.Join(t.TempDir(), "verify")
		runGitForTest(t, filepath.Dir(verify), "clone", "-q", f.upstream, verify)
		runGitForTest(t, verify, "fetch", "-q", bundle, "refs/steward/wip/attempt-1:refs/heads/wip")
		if changed := runGitForTest(t, verify, "diff", "--name-only", head, "wip"); changed != "a.txt" {
			t.Fatalf("the snapshot changes %q, want only a.txt", changed)
		}
	})
	t.Run("sha256", func(t *testing.T) {
		f := newWIPFixture(t, commitOutputs)
		if err := os.RemoveAll(f.workspace); err != nil {
			t.Fatal(err)
		}
		runGitForTest(t, filepath.Dir(f.workspace), "init", "-q", "--object-format=sha256", "-b", "main", f.workspace)
		writeTestFile(t, filepath.Join(f.workspace, "a.txt"), "one\n")
		runGitForTest(t, f.workspace, "add", "a.txt")
		runGitForTest(t, f.workspace, "commit", "-q", "-m", "base")
		writeTestFile(t, filepath.Join(f.workspace, "a.txt"), "two\n")
		summary, err := f.driver.SnapshotWorkInProgress(context.Background(), f.pkg, f.workspace)
		if err != nil || !strings.Contains(summary, "wip.bundle retained") {
			t.Fatalf("snapshot = %q, %v", summary, err)
		}
	})
}
