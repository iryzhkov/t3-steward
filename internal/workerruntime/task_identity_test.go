package workerruntime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
