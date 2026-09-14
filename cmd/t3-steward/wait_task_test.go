package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func injectedIdentity() map[string]string {
	return map[string]string{
		domain.TaskWaitEnvWorkflowRunID:   "run-1",
		domain.TaskWaitEnvTaskID:          "task-1",
		domain.TaskWaitEnvAttemptID:       "attempt-1",
		domain.TaskWaitEnvAttemptRevision: "7",
		domain.TaskWaitEnvAssignmentID:    "assignment-1",
		domain.TaskWaitEnvThreadID:        "thread-1",
	}
}

// writeIdentityFile puts a worker-written identity record in a directory and
// makes it the working directory, the way a task's shell runs inside its
// prepared workspace.
func writeIdentityFile(t *testing.T, values map[string]string, mode os.FileMode) string {
	t.Helper()
	root := t.TempDir()
	directory := filepath.Join(root, domain.TaskIdentityDir)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	content, err := domain.RenderTaskIdentityFile(values)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, filepath.FromSlash(domain.TaskIdentityFile))
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	return root
}

func noEnvironment(string) string { return "" }

// With no environment at all, the identity comes from the record the worker
// wrote into the workspace. This is the path that needs nothing from T3.
func TestTaskIdentityResolvesFromTheWorkspaceFile(t *testing.T) {
	writeIdentityFile(t, injectedIdentity(), 0o600)
	identity, err := resolveTaskIdentity(noEnvironment)
	if err != nil {
		t.Fatal(err)
	}
	if identity.AttemptID != "attempt-1" || identity.AttemptRevision != 7 ||
		identity.ThreadID != "thread-1" || identity.WorkflowRunID != "run-1" ||
		identity.TaskID != "task-1" || identity.AssignmentID != "assignment-1" {
		t.Fatalf("identity = %+v", identity)
	}
}

// A task's shell is usually somewhere below the workspace root, so the record
// is found the way a tool finds the repository it is inside.
func TestTaskIdentityFileIsFoundFromASubdirectory(t *testing.T) {
	root := writeIdentityFile(t, injectedIdentity(), 0o600)
	nested := filepath.Join(root, "src", "deep")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Chdir(nested)
	if _, err := resolveTaskIdentity(noEnvironment); err != nil {
		t.Fatal(err)
	}
}

// The injected environment wins when both are present: a sandbox sets it for
// exactly one execution, while a workspace can be reached from elsewhere.
func TestInjectedEnvironmentWinsOverTheWorkspaceFile(t *testing.T) {
	fileValues := injectedIdentity()
	fileValues[domain.TaskWaitEnvAttemptID] = "attempt-from-file"
	fileValues[domain.TaskWaitEnvThreadID] = "thread-from-file"
	writeIdentityFile(t, fileValues, 0o600)
	environment := injectedIdentity()
	identity, err := resolveTaskIdentity(func(key string) string { return environment[key] })
	if err != nil {
		t.Fatal(err)
	}
	if identity.AttemptID != "attempt-1" || identity.ThreadID != "thread-1" {
		t.Fatalf("the workspace file overrode the injected environment: %+v", identity)
	}
}

// The record decides which attempt a command speaks for, so anything that is
// not a private regular file owned by this user is refused rather than read.
func TestTaskIdentityFileIsRefusedWhenItIsNotPrivate(t *testing.T) {
	writeIdentityFile(t, injectedIdentity(), 0o644)
	_, err := resolveTaskIdentity(noEnvironment)
	if err == nil {
		t.Fatal("a world-readable identity record was accepted")
	}
	if errors.Is(err, errNotInsideTask) {
		t.Fatalf("a refused record was reported as absence: %v", err)
	}
	if !strings.Contains(err.Error(), "0600") {
		t.Fatalf("the refusal does not say what is wrong: %v", err)
	}
}

func TestTaskIdentityFileIsRefusedWhenItIsNotARegularFile(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, domain.TaskIdentityDir), 0o700); err != nil {
		t.Fatal(err)
	}
	decoy := filepath.Join(root, "decoy.env")
	if err := os.WriteFile(decoy, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(decoy, filepath.Join(root, filepath.FromSlash(domain.TaskIdentityFile))); err != nil {
		t.Skipf("symlinks are unavailable: %v", err)
	}
	t.Chdir(root)
	_, err := resolveTaskIdentity(noEnvironment)
	if err == nil || errors.Is(err, errNotInsideTask) {
		t.Fatalf("a symlinked identity record was accepted: %v", err)
	}
	if !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("the refusal does not say what is wrong: %v", err)
	}
}

// A record that is not a closed set of the six identity keys is refused: a
// reader that tolerates extra keys is a reader that can be fed something else.
func TestTaskIdentityFileRefusesUnknownKeys(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, domain.TaskIdentityDir), 0o700); err != nil {
		t.Fatal(err)
	}
	content, err := domain.RenderTaskIdentityFile(injectedIdentity())
	if err != nil {
		t.Fatal(err)
	}
	content += "T3_STEWARD_DISPATCH_TOKEN=secret\n"
	if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(domain.TaskIdentityFile)), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	if _, err := resolveTaskIdentity(noEnvironment); err == nil {
		t.Fatal("an identity record carrying an unknown key was accepted")
	}
}

func TestTaskIdentityResolvesFromTheInjectedEnvironment(t *testing.T) {
	values := injectedIdentity()
	identity, err := resolveTaskIdentity(func(key string) string { return values[key] })
	if err != nil {
		t.Fatal(err)
	}
	if identity.AttemptID != "attempt-1" || identity.AttemptRevision != 7 || identity.ThreadID != "thread-1" {
		t.Fatalf("identity = %+v", identity)
	}
}

// --task current outside a task is an error that says so, rather than quietly
// registering an interactive wait that parks nothing.
func TestTaskCurrentOutsideATaskSaysSo(t *testing.T) {
	_, err := resolveTaskIdentity(func(string) string { return "" })
	if !errors.Is(err, errNotInsideTask) {
		t.Fatalf("err = %v", err)
	}
	for _, want := range []string{"--task current", "only valid inside a t3-steward task", "wait add --"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the error does not mention %q: %s", want, err)
		}
	}
	// A partial identity is not a task either.
	values := injectedIdentity()
	delete(values, domain.TaskWaitEnvAttemptRevision)
	if _, err := resolveTaskIdentity(func(key string) string { return values[key] }); !errors.Is(err, errNotInsideTask) {
		t.Fatalf("a partial identity was accepted: %v", err)
	}
}

// --task current is routed to the task-bound path; --task <run>/<task> and a
// plain shell check are not.
func TestCurrentTaskWaitRouting(t *testing.T) {
	for _, args := range [][]string{
		{"--task", "current", "--", "true"},
		{"--task=current", "--", "true"},
		{"--name", "ci", "--task", "current", "--", "gh", "run", "view"},
	} {
		if !currentTaskWaitArgs(args) {
			t.Fatalf("%v was not routed to the task-bound path", args)
		}
	}
	for _, args := range [][]string{
		{"--task", "run-1/implement"},
		{"--task=run-1/implement"},
		{"--run", "run-1"},
		{"--", "true"},
		{"--name", "current", "--", "true"},
	} {
		if currentTaskWaitArgs(args) {
			t.Fatalf("%v was misrouted to the task-bound path", args)
		}
	}
}

// A provider-local session ID is an input to resolution and never a thread ID,
// and several providers in the environment name their candidates.
func TestCallerSessionNamesConflictingProviders(t *testing.T) {
	_, err := callerSession(func(key string) string {
		switch key {
		case "CLAUDE_CODE_SESSION_ID":
			return "claude-session"
		case "CODEX_THREAD_ID":
			return "codex-session"
		}
		return ""
	})
	if err == nil {
		t.Fatal("conflicting provider sessions accepted")
	}
	for _, want := range []string{"CLAUDE_CODE_SESSION_ID=claude-session", "CODEX_THREAD_ID=codex-session", "--thread"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the ambiguity does not name %q: %s", want, err)
		}
	}
	// One provider, whichever it is, resolves.
	for _, key := range providerSessionKeys {
		got, err := callerSession(func(k string) string {
			if k == key {
				return "session-123"
			}
			return ""
		})
		if err != nil || got != "session-123" {
			t.Fatalf("%s: %q %v", key, got, err)
		}
	}
	// The same session exported under two names is one session, not a conflict.
	got, err := callerSession(func(string) string { return "same" })
	if err != nil || got != "same" {
		t.Fatalf("one session under several names: %q %v", got, err)
	}
}

// The injected canonical thread wins over provider session resolution, and is
// the only value used as a thread ID.
func TestResolveThreadPrefersTheInjectedCanonicalThread(t *testing.T) {
	for name, value := range injectedIdentity() {
		t.Setenv(name, value)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "claude-session")
	got, err := resolveThread(config.Config{}, "")
	if err != nil || got != "thread-1" {
		t.Fatalf("%q %v", got, err)
	}
	// A partial injection is not a task, so it falls through to the provider
	// session rather than waking a thread named by half an identity.
	t.Setenv(domain.TaskWaitEnvAttemptID, "")
	if _, err := resolveThread(config.Config{}, ""); err == nil {
		t.Fatal("a partial identity resolved a thread")
	}
	if got, err := resolveThread(config.Config{}, "explicit"); err != nil || got != "explicit" {
		t.Fatalf("an explicit --thread was not honoured: %q %v", got, err)
	}
}

// Help distinguishes the two kinds of wait by what they wake and what they can
// change, and answers the questions an agent has to be able to answer before
// it parks a task.
func TestWaitHelpDistinguishesTaskBoundFromInteractive(t *testing.T) {
	for _, want := range []string{
		"TASK-BOUND WAIT", "INTERACTIVE WAIT", "MUTATING", "NON-MUTATING",
		"waiting-external", "Exit codes", "--json", "--request-id",
		"task-bound waits are refused", "maximum duration",
		"t3-steward wait add --task current",
	} {
		if !strings.Contains(waitUsage, want) {
			t.Fatalf("wait help does not explain %q", want)
		}
	}
}
