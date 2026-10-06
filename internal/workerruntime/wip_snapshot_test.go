package workerruntime

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func runGitForTest(t *testing.T, dir string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=test", "-c", "user.email=test@example.com", "-c", "commit.gpgsign=false"}, args...)...)
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	out, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// wipFixture is a prepared attempt workspace cloned from an upstream that
// holds only the base commit, the way a task workspace is cloned from its
// project, with the attempt's own commit on top of the base.
type wipFixture struct {
	driver    *LocalDriver
	pkg       workerproto.ExecutionPackage
	upstream  string
	workspace string
	base      string
}

func newWIPFixture(t *testing.T, outputs []domain.ArtifactDeclaration) wipFixture {
	t.Helper()
	root := t.TempDir()
	upstream := filepath.Join(root, "upstream")
	runGitForTest(t, root, "init", "-q", "-b", "main", upstream)
	writeTestFile(t, filepath.Join(upstream, "a.txt"), "one\n")
	runGitForTest(t, upstream, "add", "a.txt")
	runGitForTest(t, upstream, "commit", "-q", "-m", "base")
	base := runGitForTest(t, upstream, "rev-parse", "HEAD")

	pkg := testPackage()
	pkg.Outputs = outputs
	driver := &LocalDriver{
		Config: LocalDriverConfig{ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs")},
		Now:    func() time.Time { return runtimeTestNow },
	}
	workspace := filepath.Join(driver.workspacePath(pkg), "workspace")
	if err := os.MkdirAll(filepath.Dir(workspace), 0o700); err != nil {
		t.Fatal(err)
	}
	runGitForTest(t, root, "clone", "-q", upstream, workspace)
	// Preparation records the pin under .t3, which is never part of the work.
	writeTestFile(t, filepath.Join(workspace, ".t3", "base-commit"), base+"\n")
	writeTestFile(t, filepath.Join(workspace, "b.txt"), "two\n")
	runGitForTest(t, workspace, "add", "b.txt")
	runGitForTest(t, workspace, "commit", "-q", "-m", "attempt commit")
	return wipFixture{driver: driver, pkg: pkg, upstream: upstream, workspace: workspace, base: base}
}

var commitOutputs = []domain.ArtifactDeclaration{{Name: "implementation", Commit: &domain.CommitOutput{}}, {Name: "handoff.md"}}

// Uncommitted work in a task that declares a commit is kept as a bundle of a
// snapshot commit on a private ref. The bundle verifies against the base
// alone and carries the attempt's commits plus every uncommitted change, and
// the task's own index and working tree are left exactly as they were.
func TestWorkInProgressSnapshotBundlesUncommittedChanges(t *testing.T) {
	f := newWIPFixture(t, commitOutputs)
	writeTestFile(t, filepath.Join(f.workspace, "a.txt"), "one\nchanged\n")
	writeTestFile(t, filepath.Join(f.workspace, "c.txt"), "new\n")
	head := runGitForTest(t, f.workspace, "rev-parse", "HEAD")
	status := runGitForTest(t, f.workspace, "status", "--porcelain")

	summary, err := f.driver.SnapshotWorkInProgress(context.Background(), f.pkg, f.workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(summary, "wip.bundle") || !strings.Contains(summary, "refs/steward/wip/attempt-1") {
		t.Fatalf("summary = %q", summary)
	}
	bundle := filepath.Join(f.driver.workspacePath(f.pkg), WorkInProgressBundleFile)
	if _, err := os.Stat(bundle); err != nil {
		t.Fatalf("bundle not retained: %v", err)
	}
	if got := runGitForTest(t, f.workspace, "rev-parse", "HEAD"); got != head {
		t.Fatalf("HEAD moved from %s to %s", head, got)
	}
	if got := runGitForTest(t, f.workspace, "status", "--porcelain"); got != status {
		t.Fatalf("status changed:\nbefore %q\nafter  %q", status, got)
	}

	verify := filepath.Join(t.TempDir(), "verify")
	runGitForTest(t, filepath.Dir(verify), "clone", "-q", f.upstream, verify)
	runGitForTest(t, verify, "bundle", "verify", bundle)
	runGitForTest(t, verify, "fetch", "-q", bundle, "refs/steward/wip/attempt-1:refs/heads/wip")
	for path, want := range map[string]string{"a.txt": "one\nchanged", "b.txt": "two", "c.txt": "new"} {
		if got := runGitForTest(t, verify, "show", "wip:"+path); got != want {
			t.Fatalf("%s in the snapshot = %q, want %q", path, got, want)
		}
	}
	if files := runGitForTest(t, verify, "ls-tree", "-r", "--name-only", "wip"); strings.Contains(files, ".t3") {
		t.Fatalf("the snapshot carries the runtime's .t3 directory:\n%s", files)
	}
	if parent := runGitForTest(t, verify, "rev-parse", "wip^"); parent != head {
		t.Fatalf("snapshot parent = %s, want the attempt HEAD %s", parent, head)
	}
}

// A clean tree, apart from the runtime's own .t3 directory, has nothing to
// recover and is not snapshotted.
func TestWorkInProgressSnapshotSkipsACleanTree(t *testing.T) {
	f := newWIPFixture(t, commitOutputs)
	summary, err := f.driver.SnapshotWorkInProgress(context.Background(), f.pkg, f.workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(summary, "clean") {
		t.Fatalf("summary = %q", summary)
	}
	if _, err := os.Stat(filepath.Join(f.driver.workspacePath(f.pkg), WorkInProgressBundleFile)); !os.IsNotExist(err) {
		t.Fatalf("a clean tree was bundled: %v", err)
	}
	if refs := runGitForTest(t, f.workspace, "for-each-ref", "refs/steward"); refs != "" {
		t.Fatalf("a clean tree left a snapshot ref: %s", refs)
	}
}

// A task that declares no commit has no commit to recover, so its tree is not
// snapshotted however dirty it is.
func TestWorkInProgressSnapshotNeedsADeclaredCommit(t *testing.T) {
	f := newWIPFixture(t, []domain.ArtifactDeclaration{{Name: "handoff.md"}})
	writeTestFile(t, filepath.Join(f.workspace, "c.txt"), "new\n")
	summary, err := f.driver.SnapshotWorkInProgress(context.Background(), f.pkg, f.workspace)
	if err != nil {
		t.Fatal(err)
	}
	if summary != "" {
		t.Fatalf("summary = %q, want none", summary)
	}
	if _, err := os.Stat(filepath.Join(f.driver.workspacePath(f.pkg), WorkInProgressBundleFile)); !os.IsNotExist(err) {
		t.Fatalf("bundled without a declared commit: %v", err)
	}
}

// The failed result of a live-commands failure carries the bundle, so the
// work is recoverable from the coordinator; any other failure does not.
func TestLiveCommandsFailureUploadsTheWorkInProgressBundle(t *testing.T) {
	f := newWIPFixture(t, commitOutputs)
	writeTestFile(t, filepath.Join(f.workspace, "c.txt"), "new\n")
	if _, err := f.driver.SnapshotWorkInProgress(context.Background(), f.pkg, f.workspace); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		failure string
		want    bool
	}{
		{"preparation failed 3 times", false},
		{LiveCommandsFailure + ": 1 background command still running after 2 nudges: pid 1 sleep", true},
	} {
		custody := testCustodyStore(t, filepath.Join(t.TempDir(), "custody"), func() time.Time { return runtimeTestNow })
		f.driver.Publisher = custody
		f.driver.T3 = &recordingT3{thread: &domain.Thread{ID: "thread-1", TurnID: "turn-3", TurnState: "completed"}, archive: []byte("{}")}
		if err := f.driver.CollectFailure(context.Background(), f.pkg, f.workspace, tc.failure); err != nil {
			t.Fatal(err)
		}
		pending, err := custody.PendingUploads()
		if err != nil || len(pending) != 1 {
			t.Fatalf("pending uploads = %d, %v", len(pending), err)
		}
		found := false
		for _, object := range pending[0].Manifest.Objects {
			if object.Path == "results/"+backlog.WorkInProgressBundleName {
				found = true
				if object.ID != backlog.WorkInProgressBundleID("attempt-1") || object.Kind != string(domain.ArtifactGitState) || object.MediaType != backlog.CommitBundleMediaType {
					t.Fatalf("bundle object = %+v", object)
				}
			}
		}
		if found != tc.want {
			t.Fatalf("failure %q: bundle uploaded = %v, want %v", tc.failure, found, tc.want)
		}
	}
}
