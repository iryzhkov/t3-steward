package backlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	// Compare resolved paths: on macOS the temporary directory itself is behind
	// a symlink, so the link target and the recorded directory differ in prefix.
	wantDependencies, err := filepath.EvalSymlinks(secondPrepared.DependenciesDir)
	if err != nil {
		t.Fatal(err)
	}
	if target, err := filepath.EvalSymlinks(filepath.Join(secondPrepared.WorkspaceDir, ".t3", "dependencies")); err != nil || target != wantDependencies {
		t.Fatalf("active dependency view = %q, %v; want %q", target, err, wantDependencies)
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
	if err := manager.CleanupWorkflow(context.Background(), "../escape", WorkflowWorkspaceRetain); err == nil || !strings.Contains(err.Error(), "safe path component") {
		t.Fatalf("unsafe cleanup error = %v", err)
	}
	request := workflowWorkspaceRequest(repository, workspaceTask("task-1", "first"), "attempt-1")
	prepared, err := manager.Prepare(context.Background(), "worker-a", request)
	if err != nil {
		t.Fatalf("prepare workflow: %v", err)
	}
	cleanupImmutable(t, prepared.RootDir)
	if err := manager.CleanupWorkflow(context.Background(), request.WorkflowRunID, WorkflowWorkspaceRemove); err == nil {
		t.Fatal("cleanup succeeded with an active attempt")
	}
	if _, err := os.Stat(prepared.RootDir); err != nil {
		t.Fatalf("active workspace was removed: %v", err)
	}
	if err := manager.Release(request.Attempt.ID, EnvironmentReleaseTerminal); err != nil {
		t.Fatalf("complete attempt: %v", err)
	}
	if err := manager.CleanupWorkflow(context.Background(), request.WorkflowRunID, WorkflowWorkspaceRemove); err != nil {
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
	if err := manager.CleanupWorkflow(context.Background(), retainedRequest.WorkflowRunID, WorkflowWorkspaceRetain); err != nil {
		t.Fatalf("retain terminal workflow workspace: %v", err)
	}
	if _, err := os.Stat(retained.RootDir); err != nil {
		t.Fatalf("retained workspace missing: %v", err)
	}
}

func TestWorkflowWorkspaceManagersSerializePublicationAndCleanStaleStage(t *testing.T) {
	repository := newGitFixture(t)
	runsRoot := t.TempDir()
	runDir := filepath.Join(runsRoot, "run-1")
	stale := filepath.Join(runDir, ".workflow-prepare-orphan")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatalf("create stale workflow stage: %v", err)
	}
	firstManager := workflowWorkspaceManager(runsRoot, "")
	secondManager := workflowWorkspaceManager(runsRoot, "")
	request := workflowWorkspaceRequest(repository, workspaceTask("task-1", "first"), "attempt-1")
	request.Environment.Setup.Commands = []string{"printf once > setup-once.txt"}

	start := make(chan struct{})
	results := make(chan PreparedWorkspace, 2)
	errs := make(chan error, 2)
	for _, manager := range []*WorkflowWorkspaceManager{firstManager, secondManager} {
		go func(manager *WorkflowWorkspaceManager) {
			<-start
			prepared, err := manager.Prepare(context.Background(), "worker-a", request)
			results <- prepared
			errs <- err
		}(manager)
	}
	close(start)
	firstPrepared := <-results
	secondPrepared := <-results
	cleanupImmutable(t, firstPrepared.RootDir)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent workflow preparation: %v", err)
		}
	}
	if firstPrepared.WorkspaceDir != secondPrepared.WorkspaceDir {
		t.Fatalf("published workspaces differ: %q != %q", firstPrepared.WorkspaceDir, secondPrepared.WorkspaceDir)
	}
	if got := readTestFile(t, firstPrepared.WorkspaceDir, "setup-once.txt"); got != "once" {
		t.Fatalf("setup output = %q, want one publication", got)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale workflow stage remains: %v", err)
	}
}

func TestWorkflowWorkspaceReconcileRetainedExpiryAndActiveProtection(t *testing.T) {
	repository := newGitFixture(t)
	runsRoot := t.TempDir()
	manager := workflowWorkspaceManager(runsRoot, "")
	retainedAt := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	manager.Now = func() time.Time { return retainedAt }

	retainedRequest := workflowWorkspaceRequest(repository, workspaceTask("task-1", "retained"), "attempt-1")
	retained, err := manager.Prepare(context.Background(), "worker-a", retainedRequest)
	if err != nil {
		t.Fatalf("prepare retained workflow: %v", err)
	}
	cleanupImmutable(t, retained.RootDir)
	if err := manager.Release(retainedRequest.Attempt.ID, EnvironmentReleaseTerminal); err != nil {
		t.Fatalf("complete retained workflow: %v", err)
	}
	if err := manager.CleanupWorkflow(context.Background(), retainedRequest.WorkflowRunID, WorkflowWorkspaceRetain); err != nil {
		t.Fatalf("retain workflow: %v", err)
	}
	marker, err := readWorkflowRetention(retained.RootDir)
	if err != nil || !marker.RetainedAt.Equal(retainedAt) {
		t.Fatalf("retention marker = %#v, %v", marker, err)
	}
	staleMarker := filepath.Join(retained.RootDir, "retention.json.next")
	if err := os.WriteFile(staleMarker, []byte("interrupted"), 0o644); err != nil {
		t.Fatalf("write stale retention marker: %v", err)
	}
	if result, err := manager.Reconcile(context.Background(), retainedAt.Add(-time.Minute)); err != nil || len(result.RemovedWorkflowRunIDs) != 0 {
		t.Fatalf("early reconciliation = %#v, %v", result, err)
	}
	if _, err := os.Stat(staleMarker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale retention marker remains: %v", err)
	}

	activeRequest := workflowWorkspaceRequest(repository, workspaceTask("task-2", "active"), "attempt-2")
	activeRequest.WorkflowRunID = "run-2"
	activeRequest.Attempt.WorkflowRunID = "run-2"
	active, err := manager.Prepare(context.Background(), "worker-a", activeRequest)
	if err != nil {
		t.Fatalf("prepare active workflow: %v", err)
	}
	cleanupImmutable(t, active.RootDir)
	if err := writeWorkflowRetention(active.RootDir, workflowRetention{RetainedAt: retainedAt.Add(-time.Hour)}); err != nil {
		t.Fatalf("write active retention marker: %v", err)
	}

	result, err := manager.Reconcile(context.Background(), retainedAt.Add(time.Minute))
	if err != nil {
		t.Fatalf("reconcile retained workflows: %v", err)
	}
	if len(result.RemovedWorkflowRunIDs) != 1 || result.RemovedWorkflowRunIDs[0] != "run-1" {
		t.Fatalf("removed workflows = %v, want run-1", result.RemovedWorkflowRunIDs)
	}
	if _, err := os.Stat(retained.RootDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired retained workflow remains: %v", err)
	}
	if _, err := os.Stat(active.RootDir); err != nil {
		t.Fatalf("active workflow was removed: %v", err)
	}
}

func TestWorkflowWorkspaceReconcileFailsClosedOnInvalidRetentionMarker(t *testing.T) {
	runsRoot := t.TempDir()
	manager := workflowWorkspaceManager(runsRoot, "")
	rootDir := manager.workflowRoot("run-invalid")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("create invalid retained workflow: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rootDir, "retention.json"), []byte("{"), 0o644); err != nil {
		t.Fatalf("write invalid retention marker: %v", err)
	}
	_, err := manager.Reconcile(context.Background(), time.Now())
	if err == nil || !strings.Contains(err.Error(), "decode workflow retention marker") {
		t.Fatalf("reconciliation error = %v", err)
	}
	if _, statErr := os.Stat(rootDir); statErr != nil {
		t.Fatalf("invalid retained workflow was removed: %v", statErr)
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
