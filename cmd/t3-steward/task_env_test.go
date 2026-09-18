package main

import (
	"context"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// F-4: the documented request-id pattern used $T3_STEWARD_ATTEMPT_REVISION,
// a variable that is never injected (identity comes from .t3-steward/task.env
// and send_thread_environment is off), so every documented park produced an
// id ending in "-". The default now derives from the resolved identity, a
// truncated id is warned about, and "task env" prints the identity for a
// custom id.
func TestTaskWaitRequestIDDefaultsToParkAttemptRevision(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	if err := cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--", "false"}); err != nil {
		t.Fatal(err)
	}
	records, err := store.ListTaskWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].RequestID != "park-attempt-1-7" {
		t.Fatalf("registered waits = %+v, want one with request id park-attempt-1-7", records)
	}
}

func TestTaskWaitRequestIDWarnsWhenItLooksTruncated(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	var registerErr error
	stderr := captureStderr(t, func() {
		_ = captureStdout(t, func() {
			registerErr = cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "ci-", "--", "false"})
		})
	})
	if registerErr != nil {
		t.Fatalf("a request id ending in - was refused: %v", registerErr)
	}
	if !strings.Contains(stderr, "--request-id \"ci-\" ends in \"-\"") || !strings.Contains(stderr, "empty shell variable") {
		t.Fatalf("stderr does not warn about the truncated request id: %q", stderr)
	}
	records, err := store.ListTaskWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].RequestID != "ci-" {
		t.Fatalf("registered waits = %+v, want one with request id ci-", records)
	}
}

func TestTaskEnvPrintsTheIdentity(t *testing.T) {
	writeIdentityFile(t, injectedIdentity(), 0o600)
	var err error
	output := captureStdout(t, func() { err = run([]string{"task", "env"}) })
	if err != nil {
		t.Fatal(err)
	}
	want := "export T3_STEWARD_WORKFLOW_RUN_ID=run-1\n" +
		"export T3_STEWARD_TASK_ID=task-1\n" +
		"export T3_STEWARD_ATTEMPT_ID=attempt-1\n" +
		"export T3_STEWARD_ATTEMPT_REVISION=7\n" +
		"export T3_STEWARD_ASSIGNMENT_ID=assignment-1\n" +
		"export T3_STEWARD_THREAD_ID=thread-1\n"
	if output != want {
		t.Fatalf("task env printed %q, want %q", output, want)
	}
	for name, value := range map[string]string{
		"revision": "7", "attempt": "attempt-1", "task": "task-1", "run": "run-1",
		"assignment": "assignment-1", "thread": "thread-1", domain.TaskWaitEnvTaskID: "task-1",
	} {
		output := captureStdout(t, func() { err = run([]string{"task", "env", "--get", name}) })
		if err != nil {
			t.Fatalf("--get %s: %v", name, err)
		}
		if output != value+"\n" {
			t.Errorf("--get %s printed %q, want %q", name, output, value+"\n")
		}
	}
	if err := run([]string{"task", "env", "--get", "colour"}); err == nil {
		t.Fatal("--get accepted an unknown name")
	}
}

func TestTaskEnvFailsOutsideATask(t *testing.T) {
	t.Chdir(t.TempDir())
	var err error
	output := captureStdout(t, func() { err = run([]string{"task", "env"}) })
	if err == nil || !strings.Contains(err.Error(), domain.TaskIdentityFile) {
		t.Fatalf("task env outside a task: err=%v", err)
	}
	if output != "" {
		t.Fatalf("task env printed %q without an identity", output)
	}
}

func TestHelpNoLongerNamesTheAttemptRevisionVariable(t *testing.T) {
	if strings.Contains(waitUsage, "$T3_STEWARD_ATTEMPT_REVISION") {
		t.Fatal("wait help still tells agents to interpolate $T3_STEWARD_ATTEMPT_REVISION")
	}
	if !strings.Contains(waitUsage, "t3-steward task env --get revision") {
		t.Fatal("wait help does not point at t3-steward task env for a custom request id")
	}
	if !strings.Contains(usage, "  task ") {
		t.Fatal("root usage does not list the task command")
	}
}
