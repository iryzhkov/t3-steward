package workerruntime

import (
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
