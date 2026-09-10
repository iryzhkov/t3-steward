package backlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestWorkflowWorkspaceManagerSharesMutationRunsSetupOnceAndSwitchesDependencies(t *testing.T) {
	repository := newGitFixture(t)
	runsRoot := t.TempDir()
	storage := t.TempDir()
	manager := workflowWorkspaceManager(runsRoot, storage)

	firstTask := workspaceTask("task-1", "first")
	first := workflowWorkspaceRequest(repository, firstTask, "attempt-1")
	first.InputArtifacts = []domain.Artifact{storedInputArtifact(t, storage, "run-1", "plan.md", "immutable plan")}
	first.Environment.Setup.Commands = []string{"printf once > setup-runs.txt"}

	firstPrepared, err := manager.Prepare(context.Background(), "worker-a", first)
	if err != nil {
		t.Fatalf("prepare first workflow task: %v", err)
	}
	cleanupImmutable(t, firstPrepared.RootDir)
	if err := os.WriteFile(filepath.Join(firstPrepared.WorkspaceDir, "mutation.txt"), []byte("from first"), 0o644); err != nil {
		t.Fatalf("mutate shared checkout: %v", err)
	}
	if err := manager.Release(first.Attempt.ID, EnvironmentReleaseTerminal); err != nil {
		t.Fatalf("complete first task: %v", err)
	}

	secondTask := workspaceTask("task-2", "second")
	secondTask.Needs = []string{"first"}
	secondTask.DependencyInputs = map[string][]string{"first": {"result.txt"}}
	second := workflowWorkspaceRequest(repository, secondTask, "attempt-2")
	second.Environment.Setup = first.Environment.Setup
	second.InputArtifacts = append([]domain.Artifact(nil), first.InputArtifacts...)
	second.DependencyTasks = []domain.Task{firstTask, secondTask}
	second.DependencyArtifacts = []domain.Artifact{
		storedOutputArtifact(t, storage, firstTask.ID, first.Attempt.ID, "result.txt", "dependency result"),
	}

	secondPrepared, err := manager.Prepare(context.Background(), "worker-a", second)
	if err != nil {
		t.Fatalf("prepare second workflow task: %v", err)
	}
	if secondPrepared.WorkspaceDir != firstPrepared.WorkspaceDir {
		t.Fatalf("workspace changed between tasks: %q != %q", secondPrepared.WorkspaceDir, firstPrepared.WorkspaceDir)
	}
	if got := readTestFile(t, secondPrepared.WorkspaceDir, "mutation.txt"); got != "from first" {
		t.Fatalf("shared mutation = %q, want first task mutation", got)
	}
	if got := readTestFile(t, secondPrepared.WorkspaceDir, "setup-runs.txt"); got != "once" {
		t.Fatalf("setup output = %q, want exactly one setup run", got)
	}
	if got := readTestFile(t, secondPrepared.DependenciesDir, "first/result.txt"); got != "dependency result" {
		t.Fatalf("dependency view = %q", got)
	}
	if got := readTestFile(t, secondPrepared.InputsDir, "plan.md"); got != "immutable plan" {
		t.Fatalf("workflow input = %q", got)
	}
	assertReadOnly(t, filepath.Join(secondPrepared.InputsDir, "plan.md"))
	assertReadOnly(t, filepath.Join(secondPrepared.DependenciesDir, "first", "result.txt"))
	assertReadOnly(t, filepath.Join(secondPrepared.RootDir, "environment.json"))
	if target, err := filepath.EvalSymlinks(filepath.Join(secondPrepared.WorkspaceDir, ".t3", "dependencies")); err != nil || target != secondPrepared.DependenciesDir {
		t.Fatalf("active dependency view = %q, %v; want %q", target, err, secondPrepared.DependenciesDir)
	}
	if err := manager.Release(second.Attempt.ID, EnvironmentReleaseTerminal); err != nil {
		t.Fatalf("complete second task: %v", err)
	}

	changed := workflowWorkspaceRequest(repository, workspaceTask("task-3", "third"), "attempt-3")
	changed.InputArtifacts = append([]domain.Artifact(nil), first.InputArtifacts...)
	changed.Environment.Setup.Commands = []string{"printf changed > setup-runs.txt"}
	if _, err := manager.Prepare(context.Background(), "worker-a", changed); err == nil || !strings.Contains(err.Error(), "preparation contract changed") {
		t.Fatalf("changed workflow contract error = %v", err)
	}
}

func TestWorkflowWorkspaceManagerSetupFailureRetainsLogAndReleasesReservation(t *testing.T) {
	repository := newGitFixture(t)
	runsRoot := t.TempDir()
	manager := workflowWorkspaceManager(runsRoot, "")
	task := workspaceTask("task-1", "first")
	request := workflowWorkspaceRequest(repository, task, "attempt-1")
	request.Environment.Setup.Commands = []string{"printf broken >&2; exit 7"}

	_, err := manager.Prepare(context.Background(), "worker-a", request)
	var preparationErr *PreparationError
	if !errors.As(err, &preparationErr) || preparationErr.LogPath == "" {
		t.Fatalf("error = %v, want retained PreparationError", err)
	}
	if log := readAbsoluteTestFile(t, preparationErr.LogPath); !strings.Contains(log, "broken") || !strings.Contains(log, "exit status 7") {
		t.Fatalf("failure log = %q", log)
	}
	if _, ok := manager.Environments.Reservation(request.Attempt.ID); ok {
		t.Fatal("failed preparation retained its reservation")
	}
	if _, statErr := os.Stat(manager.workflowRoot(request.WorkflowRunID)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed workflow workspace remains: %v", statErr)
	}
}

func TestWorkflowWorkspaceManagerPauseResumeRetryAndWorkerPin(t *testing.T) {
	repository := newGitFixture(t)
	manager := workflowWorkspaceManager(t.TempDir(), "")
	task := workspaceTask("task-1", "first")
	request := workflowWorkspaceRequest(repository, task, "attempt-1")

	prepared, err := manager.Prepare(context.Background(), "worker-a", request)
	if err != nil {
		t.Fatalf("prepare workflow: %v", err)
	}
	cleanupImmutable(t, prepared.RootDir)
	if err := os.WriteFile(filepath.Join(prepared.WorkspaceDir, "checkpoint.txt"), []byte("kept"), 0o644); err != nil {
		t.Fatalf("write checkpoint mutation: %v", err)
	}
	if err := manager.Release(request.Attempt.ID, EnvironmentPauseReleaseResources); err != nil {
		t.Fatalf("pause workflow attempt: %v", err)
	}
	resumed, err := manager.Prepare(context.Background(), "worker-a", request)
	if err != nil {
		t.Fatalf("resume workflow attempt: %v", err)
	}
	if got := readTestFile(t, resumed.WorkspaceDir, "checkpoint.txt"); got != "kept" {
		t.Fatalf("resumed mutation = %q", got)
	}

	otherWorker := request
	otherWorker.Attempt.ID = "attempt-other"
	if _, err := manager.Prepare(context.Background(), "worker-b", otherWorker); err == nil || !strings.Contains(err.Error(), "pinned to worker") {
		t.Fatalf("worker mismatch error = %v", err)
	}
	if err := manager.Release(request.Attempt.ID, EnvironmentReleaseTerminal); err != nil {
		t.Fatalf("complete resumed attempt: %v", err)
	}

	retry := request
	retry.Attempt.ID = "attempt-2"
	retry.Attempt.Number = 2
	retried, err := manager.Prepare(context.Background(), "worker-a", retry)
	if err != nil {
		t.Fatalf("prepare retry: %v", err)
	}
	if retried.WorkspaceDir != prepared.WorkspaceDir {
		t.Fatalf("retry workspace = %q, want retained %q", retried.WorkspaceDir, prepared.WorkspaceDir)
	}
	if got := readTestFile(t, retried.WorkspaceDir, "checkpoint.txt"); got != "kept" {
		t.Fatalf("retry mutation = %q", got)
	}
}

func TestWorkflowWorkspaceManagerCleanupRequiresTerminalDecision(t *testing.T) {
	repository := newGitFixture(t)
	runsRoot := t.TempDir()
	manager := workflowWorkspaceManager(runsRoot, "")
	if err := manager.CleanupWorkflow("../escape", WorkflowWorkspaceRetain); err == nil || !strings.Contains(err.Error(), "safe path component") {
		t.Fatalf("unsafe cleanup error = %v", err)
	}
	request := workflowWorkspaceRequest(repository, workspaceTask("task-1", "first"), "attempt-1")
	prepared, err := manager.Prepare(context.Background(), "worker-a", request)
	if err != nil {
		t.Fatalf("prepare workflow: %v", err)
	}
	cleanupImmutable(t, prepared.RootDir)
	if err := manager.CleanupWorkflow(request.WorkflowRunID, WorkflowWorkspaceRemove); err == nil {
		t.Fatal("cleanup succeeded with an active attempt")
	}
	if _, err := os.Stat(prepared.RootDir); err != nil {
		t.Fatalf("active workspace was removed: %v", err)
	}
	if err := manager.Release(request.Attempt.ID, EnvironmentReleaseTerminal); err != nil {
		t.Fatalf("complete attempt: %v", err)
	}
	if err := manager.CleanupWorkflow(request.WorkflowRunID, WorkflowWorkspaceRemove); err != nil {
		t.Fatalf("remove terminal workflow workspace: %v", err)
	}
	if _, err := os.Stat(prepared.RootDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workflow workspace remains after removal: %v", err)
	}

	retainedRequest := workflowWorkspaceRequest(repository, workspaceTask("task-2", "second"), "attempt-2")
	retainedRequest.WorkflowRunID = "run-2"
	retainedRequest.Attempt.WorkflowRunID = "run-2"
	retained, err := manager.Prepare(context.Background(), "worker-a", retainedRequest)
	if err != nil {
		t.Fatalf("prepare retained workflow: %v", err)
	}
	cleanupImmutable(t, retained.RootDir)
	if err := manager.Release(retainedRequest.Attempt.ID, EnvironmentReleaseTerminal); err != nil {
		t.Fatalf("complete retained workflow: %v", err)
	}
	if err := manager.CleanupWorkflow(retainedRequest.WorkflowRunID, WorkflowWorkspaceRetain); err != nil {
		t.Fatalf("retain terminal workflow workspace: %v", err)
	}
	if _, err := os.Stat(retained.RootDir); err != nil {
		t.Fatalf("retained workspace missing: %v", err)
	}
}

func workflowWorkspaceManager(runsRoot, storageRoot string) *WorkflowWorkspaceManager {
	return &WorkflowWorkspaceManager{
		Preparer:     workspacePreparer(runsRoot, storageRoot),
		Environments: NewEnvironmentCoordinator(),
	}
}

func workflowWorkspaceRequest(repository string, task domain.Task, attemptID string) WorkspacePreparation {
	request := workspaceRequest(repository, "main", task, attemptID)
	request.Environment.Scope = EnvironmentScopeWorkflow
	return request
}
