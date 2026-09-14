package main

import (
	"errors"
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
	t.Setenv(domain.TaskWaitEnvThreadID, "thread-1")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "claude-session")
	got, err := resolveThread(config.Config{}, "")
	if err != nil || got != "thread-1" {
		t.Fatalf("%q %v", got, err)
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
