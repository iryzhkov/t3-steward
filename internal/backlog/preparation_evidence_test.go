package backlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Every preparation attempt of one attempt ID keeps its own evidence file. The
// attempt ID does not change between retries, so a name built from it alone let
// the second attempt collide with the first and lose the original cause.
func TestPreparationRetriesKeepEveryAttemptsEvidence(t *testing.T) {
	runsRoot := t.TempDir()
	missingRepository := filepath.Join(t.TempDir(), "absent.git")
	task := workspaceTask("task-id", "task")
	parent := filepath.Join(runsRoot, "run-1", "task-id")

	// The first attempt fails before the repository exists, which is the
	// failure that explains the whole sequence.
	first := workspaceRequest(missingRepository, "main", task, "attempt-1")
	_, err := workspacePreparer(runsRoot, "").Prepare(context.Background(), first)
	assertRetainedPreparationLog(t, err, parent, "attempt-1", 1)

	// Later attempts of the same attempt ID fail differently, and each keeps
	// its own file rather than colliding with the evidence before it.
	repository := newGitFixture(t)
	for ordinal := 2; ordinal <= 3; ordinal++ {
		request := workspaceRequest(repository, "refs/heads/never-created", task, "attempt-1")
		_, err := workspacePreparer(runsRoot, "").Prepare(context.Background(), request)
		assertRetainedPreparationLog(t, err, parent, "attempt-1", ordinal)
	}

	firstLog := readTestFile(t, parent, PreparationLogName("attempt-1", 1))
	if !strings.Contains(firstLog, "clone --mirror") {
		t.Fatalf("first preparation evidence lost its cause:\n%s", firstLog)
	}
	for ordinal := 2; ordinal <= 3; ordinal++ {
		retry := readTestFile(t, parent, PreparationLogName("attempt-1", ordinal))
		if !strings.Contains(retry, "rev-parse --verify refs/heads/never-created") {
			t.Fatalf("preparation evidence %d is not its own attempt:\n%s", ordinal, retry)
		}
		if retry == firstLog {
			t.Fatalf("preparation evidence %d repeats the first attempt", ordinal)
		}
	}
}

// A failure to retain the log is reported after the failure that caused the
// preparation to fail, never instead of it.
func TestRetainFailureDoesNotMaskThePreparationCause(t *testing.T) {
	runsRoot := t.TempDir()
	repository := newGitFixture(t)
	task := workspaceTask("task-id", "task")
	parent := filepath.Join(runsRoot, "run-1", "task-id")
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	// Occupy every evidence ordinal so that retention cannot succeed.
	for ordinal := 1; ordinal <= MaxRetainedPreparationLogs; ordinal++ {
		path := filepath.Join(parent, PreparationLogName("attempt-1", ordinal))
		if err := os.WriteFile(path, []byte("occupied\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	request := workspaceRequest(repository, "refs/heads/never-created", task, "attempt-1")
	_, err := workspacePreparer(runsRoot, "").Prepare(context.Background(), request)
	var preparationErr *PreparationError
	if !errors.As(err, &preparationErr) {
		t.Fatalf("error = %v, want a preparation error", err)
	}
	message := err.Error()
	cause := strings.Index(message, `resolve ref "refs/heads/never-created"`)
	retain := strings.Index(message, "retain preparation log")
	if cause < 0 || retain < 0 || cause > retain {
		t.Fatalf("error = %q, want the causal failure first and the retention failure after it", message)
	}
	if preparationErr.LogPath != "" {
		t.Fatalf("log path = %q, want none when retention failed", preparationErr.LogPath)
	}
}

func assertRetainedPreparationLog(t *testing.T, err error, parent, attemptID string, ordinal int) {
	t.Helper()
	var preparationErr *PreparationError
	if !errors.As(err, &preparationErr) {
		t.Fatalf("error = %v, want a preparation error", err)
	}
	want := filepath.Join(parent, PreparationLogName(attemptID, ordinal))
	if preparationErr.LogPath != want {
		t.Fatalf("retained log = %q, want %q", preparationErr.LogPath, want)
	}
	if _, statErr := os.Stat(want); statErr != nil {
		t.Fatalf("retained log %d: %v", ordinal, statErr)
	}
}

func TestWorkflowPreparationRetriesKeepEveryAttemptsEvidence(t *testing.T) {
	runsRoot := t.TempDir()
	parent := filepath.Join(runsRoot, "run-1")
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "preparation.log")
	if err := os.WriteFile(source, []byte("staged evidence\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	manager := &WorkflowWorkspaceManager{Preparer: WorkspacePreparer{RunsRoot: runsRoot}}
	request := WorkspacePreparation{
		WorkflowRunID: "run-1",
		Task:          workspaceTask("task-id", "task"),
		Attempt:       domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-id"},
	}
	for ordinal := 1; ordinal <= 3; ordinal++ {
		cause := &PreparationError{Err: fmt.Errorf("clone task workspace %d", ordinal), LogPath: source}
		err := manager.retainInitialFailure(request, cause)
		assertRetainedPreparationLog(t, err, parent, "attempt-1", ordinal)
		if !strings.Contains(err.Error(), fmt.Sprintf("clone task workspace %d", ordinal)) {
			t.Fatalf("error = %v, want the causal failure of attempt %d", err, ordinal)
		}
	}
}
