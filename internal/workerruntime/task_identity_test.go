package workerruntime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The worker writes the identity record into the prepared workspace, private,
// and carrying identity only. It is what lets a task name itself without the
// provider having to pass anything through.
func TestWorkerWritesAPrivateIdentityRecordCarryingNoAuthority(t *testing.T) {
	workspace := t.TempDir()
	driver := &LocalDriver{}
	pkg := testPackage()
	if err := driver.writeTaskIdentity(pkg, workspace); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(workspace, filepath.FromSlash(domain.TaskIdentityFile))
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("identity record mode = %v, want a regular 0600 file", info.Mode())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	values, err := domain.ParseTaskIdentityFile(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	if values[domain.TaskWaitEnvAttemptID] != pkg.Identity.AttemptID ||
		values[domain.TaskWaitEnvThreadID] != pkg.Identity.ThreadID {
		t.Fatalf("identity record = %+v", values)
	}
	// Nothing in the record can be used to claim an authority the agent was not
	// given. The dispatch token is the one that could be, so it must be absent.
	if pkg.Identity.DispatchToken == "" {
		t.Fatal("the fixture has no dispatch token, so its absence proves nothing")
	}
	if strings.Contains(string(raw), pkg.Identity.DispatchToken) {
		t.Fatal("the identity record carries the dispatch token")
	}
	for _, forbidden := range []string{"token", "secret", "lease", "credential"} {
		if strings.Contains(strings.ToLower(string(raw)), forbidden) {
			t.Fatalf("the identity record mentions %q", forbidden)
		}
	}
	// Rewriting it, as a resumed attempt does, keeps it private.
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := driver.writeTaskIdentity(pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if info, err = os.Lstat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("rewritten identity record mode = %v (%v)", info.Mode(), err)
	}
}

// Collect itself removes the record, before anything is captured from the
// workspace. Asserting it through Collect rather than through the helper is
// what makes the test fail if the call is ever dropped.
func TestCollectRemovesTheIdentityRecordBeforeCapturing(t *testing.T) {
	workspace := t.TempDir()
	driver := &LocalDriver{Config: LocalDriverConfig{DryRun: true}}
	pkg := testPackage()
	if err := driver.writeTaskIdentity(pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if err := driver.Collect(context.Background(), pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(workspace, domain.TaskIdentityDir)); !os.IsNotExist(err) {
		t.Fatalf("Collect left the identity record in the workspace: %v", err)
	}
}

// A collection that defers leaves the identity record in place, because the
// turn it deferred on has not ended: it may still park itself, and the turn
// that resumes after the wake has to be able to name itself. The record leaves
// only when a collection actually goes through.
func TestDeferredCollectionKeepsTheIdentityRecordForTheResumedTurn(t *testing.T) {
	ctx := context.Background()
	workspace := t.TempDir()
	pkg := testPackage()
	control := &recordingT3{thread: &domain.Thread{ID: pkg.Identity.ThreadID, Running: true}}
	publisher := &recordingPublisher{}
	driver := &LocalDriver{
		T3: control, Publisher: publisher, Now: func() time.Time { return runtimeTestNow },
		Finalizer: backlog.AttemptFinalizer{
			StorageRoot: filepath.Join(artifactTestRoot(t), "artifacts"),
			Processes:   &countingProcessRunner{}, Now: func() time.Time { return runtimeTestNow },
			NewID: func(string) string { return "verification-1" },
		},
	}
	if err := driver.writeTaskIdentity(pkg, workspace); err != nil {
		t.Fatal(err)
	}

	err := driver.Collect(ctx, pkg, workspace)
	if err == nil || !strings.Contains(err.Error(), "result collection deferred") {
		t.Fatalf("collection of a running turn = %v, want a deferral", err)
	}
	if len(publisher.results) != 0 {
		t.Fatalf("a deferred collection published a result: %+v", publisher.results)
	}
	raw, err := os.ReadFile(filepath.Join(workspace, filepath.FromSlash(domain.TaskIdentityFile)))
	if err != nil {
		t.Fatalf("the deferred collection destroyed the identity record: %v", err)
	}
	values, err := domain.ParseTaskIdentityFile(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	if values[domain.TaskWaitEnvAttemptID] != pkg.Identity.AttemptID ||
		values[domain.TaskWaitEnvThreadID] != pkg.Identity.ThreadID {
		t.Fatalf("the surviving record does not name this attempt: %+v", values)
	}

	// The turn really ends. Now the collection goes through, and the record
	// leaves with it.
	control.thread.Running = false
	control.thread.TurnState = "completed"
	control.message = "done"
	control.archive = []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed",` +
		`"startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},` +
		`"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`)
	if err := driver.Collect(ctx, pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if len(publisher.results) != 1 {
		t.Fatalf("the completed turn was not collected once: %+v", publisher.results)
	}
	if _, err := os.Lstat(filepath.Join(workspace, domain.TaskIdentityDir)); !os.IsNotExist(err) {
		t.Fatalf("a completed collection left the identity record behind: %v", err)
	}
}

// The record lives inside the task's worktree, and these tasks commit and push.
// It is excluded as it is written, because removing it when outputs are
// collected is far too late: by then `git add -A` has already committed it into
// the project repository and everything downstream of it.
func TestIdentityRecordIsExcludedFromGitWhenItIsWritten(t *testing.T) {
	workspace := t.TempDir()
	gitDir := filepath.Join(workspace, ".git")
	if err := os.MkdirAll(gitDir, 0o700); err != nil {
		t.Fatal(err)
	}
	driver := &LocalDriver{}
	if err := driver.writeTaskIdentity(testPackage(), workspace); err != nil {
		t.Fatal(err)
	}
	// A .gitignore of "*" inside the directory ignores the directory's whole
	// content, including itself, in any repository layout.
	ignore, err := os.ReadFile(filepath.Join(workspace, domain.TaskIdentityDir, ".gitignore"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(ignore), "*") {
		t.Fatalf("the identity directory does not ignore its own content: %q", ignore)
	}
	exclude, err := os.ReadFile(filepath.Join(gitDir, "info", "exclude"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(exclude), "/"+domain.TaskIdentityDir+"/") {
		t.Fatalf("the repository does not exclude the identity directory: %q", exclude)
	}
	// Rewriting, as a resumed attempt does, must not duplicate the entry.
	if err := driver.writeTaskIdentity(testPackage(), workspace); err != nil {
		t.Fatal(err)
	}
	again, err := os.ReadFile(filepath.Join(gitDir, "info", "exclude"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(again), domain.TaskIdentityDir) != 1 {
		t.Fatalf("the exclusion was appended twice: %q", again)
	}
}

// A linked worktree keeps its git directory elsewhere, behind a gitdir pointer.
func TestIdentityRecordIsExcludedThroughALinkedWorktreePointer(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "worktree")
	gitDir := filepath.Join(root, "repository", ".git", "worktrees", "one")
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(gitDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, ".git"), []byte("gitdir: "+gitDir+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := (&LocalDriver{}).writeTaskIdentity(testPackage(), workspace); err != nil {
		t.Fatal(err)
	}
	exclude, err := os.ReadFile(filepath.Join(gitDir, "info", "exclude"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(exclude), "/"+domain.TaskIdentityDir+"/") {
		t.Fatalf("a linked worktree does not exclude the identity directory: %q", exclude)
	}
}

// The record leaves the workspace before anything is captured from it, so it
// cannot reach a declared output, a git-state artifact or an archived tree.
func TestIdentityRecordIsRemovedBeforeAnythingIsCollected(t *testing.T) {
	workspace := t.TempDir()
	driver := &LocalDriver{}
	if err := driver.writeTaskIdentity(testPackage(), workspace); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "result.txt"), []byte("kept"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := driver.removeTaskIdentity(workspace); err != nil {
		t.Fatal(err)
	}
	var remaining []string
	if err := filepath.Walk(workspace, func(path string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if strings.Contains(path, domain.TaskIdentityDir) {
			remaining = append(remaining, path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("the identity record survived collection: %v", remaining)
	}
	if _, err := os.Stat(filepath.Join(workspace, "result.txt")); err != nil {
		t.Fatalf("removal took the attempt's own files with it: %v", err)
	}
	// Removing it again is not an error: collection can be retried.
	if err := driver.removeTaskIdentity(workspace); err != nil {
		t.Fatal(err)
	}
}

// artifactTestRoot is a temporary directory whose captured artifact trees can
// still be removed: a published capture is made read-only on purpose, and the
// default cleanup cannot delete through a read-only directory.
func artifactTestRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	t.Cleanup(func() {
		_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err == nil && entry.IsDir() {
				_ = os.Chmod(path, 0o700)
			}
			return nil
		})
	})
	return root
}

func TestTaskIdentityFileRendersAndParsesTheSixNames(t *testing.T) {
	values := testPackage().Identity.TaskEnvironment()
	content, err := domain.RenderTaskIdentityFile(values)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := domain.ParseTaskIdentityFile(content)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range domain.TaskWaitEnvironmentNames() {
		if parsed[name] != values[name] {
			t.Fatalf("%s round-tripped as %q, want %q", name, parsed[name], values[name])
		}
	}
	incomplete := values
	delete(incomplete, domain.TaskWaitEnvThreadID)
	if _, err := domain.RenderTaskIdentityFile(incomplete); err == nil {
		t.Fatal("an incomplete identity was rendered")
	}
}
