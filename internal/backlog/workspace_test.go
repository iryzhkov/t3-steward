package backlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type testProcessRunner struct{}

func (testProcessRunner) Run(ctx context.Context, request ProcessRequest) (ProcessResult, error) {
	output, err := runLoggedCommandOutput(ctx, request.Log, request.Dir, request.Program, request.Args...)
	result := ProcessResult{Output: string(output)}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		result.ExitCode = exitError.ExitCode()
		return result, &ProcessExitError{ExitCode: result.ExitCode, Err: err}
	}
	return result, err
}

func TestWorkspacePreparerPinsCommitAndMaterializesInputs(t *testing.T) {
	repository := newGitFixture(t)
	firstCommit := gitOutput(t, repository, "rev-parse", "HEAD")
	writeGitFile(t, repository, "version.txt", "second\n")
	gitRun(t, repository, "add", "version.txt")
	gitRun(t, repository, "commit", "-m", "second")

	runsRoot := t.TempDir()
	storage := t.TempDir()
	input := storedInputArtifact(t, storage, "run-1", "input.txt", "static input")
	dependency := storedOutputArtifact(t, storage, "producer-id", "producer-attempt", "result.txt", "dependency")
	task := workspaceTask("task-id", "consumer")
	task.Needs = []string{"producer"}
	task.DependencyInputs = map[string][]string{"producer": {"result.txt"}}
	request := workspaceRequest(repository, firstCommit, task, "attempt-1")
	request.InputArtifacts = []domain.Artifact{input}
	request.DependencyTasks = []domain.Task{{ID: "producer-id", Name: "producer"}, task}
	request.DependencyArtifacts = []domain.Artifact{dependency}
	request.Environment.Setup.Commands = []string{
		"test -f .t3/inputs/input.txt",
		"test -f .t3/dependencies/producer/result.txt",
		"printf prepared > setup.txt",
	}

	prepared, err := workspacePreparer(runsRoot, storage).Prepare(context.Background(), request)
	if err != nil {
		t.Fatalf("prepare workspace: %v", err)
	}
	cleanupImmutable(t, prepared.RootDir)
	if prepared.Commit != firstCommit {
		t.Fatalf("pinned commit = %q, want %q", prepared.Commit, firstCommit)
	}
	if got := strings.TrimSpace(readTestFile(t, prepared.WorkspaceDir, "version.txt")); got != "first" {
		t.Fatalf("workspace version = %q, want first commit", got)
	}
	if got := readTestFile(t, prepared.WorkspaceDir, "setup.txt"); got != "prepared" {
		t.Fatalf("setup output = %q", got)
	}
	if got := readTestFile(t, prepared.InputsDir, "input.txt"); got != "static input" {
		t.Fatalf("static input = %q", got)
	}
	if got := readTestFile(t, prepared.DependenciesDir, "producer/result.txt"); got != "dependency" {
		t.Fatalf("dependency = %q", got)
	}
	for _, link := range []struct {
		name string
		want string
	}{{"inputs", "../../inputs"}, {"dependencies", "../../dependencies"}} {
		target, err := os.Readlink(filepath.Join(prepared.WorkspaceDir, ".t3", link.name))
		if err != nil {
			t.Fatalf("read %s link: %v", link.name, err)
		}
		if target != link.want {
			t.Fatalf("%s link = %q, want %q", link.name, target, link.want)
		}
	}
	if _, err := os.Stat(filepath.Join(prepared.WorkspaceDir, ".git", "objects", "info", "alternates")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace shares cache object store: %v", err)
	}
	log := readTestFile(t, prepared.RootDir, "preparation.log")
	if !strings.Contains(log, "rev-parse --verify "+firstCommit+"^{commit}") ||
		!strings.Contains(log, "printf prepared > setup.txt") {
		t.Fatalf("preparation log missing commands:\n%s", log)
	}
	assertReadOnly(t, prepared.PreparationLog)
	assertReadOnly(t, filepath.Join(prepared.InputsDir, "input.txt"))
	assertReadOnly(t, filepath.Join(prepared.DependenciesDir, "producer", "result.txt"))
}

func TestWorkspacePreparerReusesCacheWithIndependentClones(t *testing.T) {
	repository := newGitFixture(t)
	runsRoot := t.TempDir()
	cacheRoot := t.TempDir()
	preparer := WorkspacePreparer{
		RunsRoot:  runsRoot,
		Cache:     LocalRepositoryCache{Root: cacheRoot},
		Processes: testProcessRunner{},
	}
	task := workspaceTask("task-id", "task")

	first, err := preparer.Prepare(context.Background(), workspaceRequest(repository, "main", task, "attempt-1"))
	if err != nil {
		t.Fatalf("prepare first workspace: %v", err)
	}
	cleanupImmutable(t, first.RootDir)
	if first.CacheReused {
		t.Fatal("first preparation unexpectedly reused cache")
	}

	writeGitFile(t, repository, "version.txt", "second\n")
	gitRun(t, repository, "add", "version.txt")
	gitRun(t, repository, "commit", "-m", "second")
	secondCommit := gitOutput(t, repository, "rev-parse", "HEAD")
	second, err := preparer.Prepare(context.Background(), workspaceRequest(repository, "main", task, "attempt-2"))
	if err != nil {
		t.Fatalf("prepare second workspace: %v", err)
	}
	cleanupImmutable(t, second.RootDir)
	if !second.CacheReused {
		t.Fatal("second preparation did not reuse cache")
	}
	if second.Commit != secondCommit {
		t.Fatalf("second commit = %q, want refreshed %q", second.Commit, secondCommit)
	}
	if first.Commit == second.Commit {
		t.Fatal("cache refresh did not observe the new commit")
	}
	if err := os.WriteFile(filepath.Join(first.WorkspaceDir, "isolated.txt"), []byte("first"), 0o644); err != nil {
		t.Fatalf("mutate first workspace: %v", err)
	}
	if _, err := os.Stat(filepath.Join(second.WorkspaceDir, "isolated.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("second workspace observed first mutation: %v", err)
	}
	firstObjects := filepath.Join(gitOutput(t, first.WorkspaceDir, "rev-parse", "--absolute-git-dir"), "objects")
	secondObjects := filepath.Join(gitOutput(t, second.WorkspaceDir, "rev-parse", "--absolute-git-dir"), "objects")
	if firstObjects == secondObjects {
		t.Fatalf("workspaces share object directory %q", firstObjects)
	}
	entries, err := os.ReadDir(cacheRoot)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("cache entries = %d, want one mirror", len(entries))
	}
}

func TestWorkspacePreparerRetainsLogAndCleansSetupFailure(t *testing.T) {
	repository := newGitFixture(t)
	runsRoot := t.TempDir()
	task := workspaceTask("task-id", "task")
	request := workspaceRequest(repository, "main", task, "attempt-1")
	request.Environment.Setup.Commands = []string{"printf broken >&2; exit 9"}

	_, err := workspacePreparer(runsRoot, "").Prepare(context.Background(), request)
	var preparationErr *PreparationError
	if !errors.As(err, &preparationErr) {
		t.Fatalf("error = %v, want PreparationError", err)
	}
	if preparationErr.LogPath == "" {
		t.Fatal("failure did not retain preparation log")
	}
	t.Cleanup(func() { _ = os.Remove(preparationErr.LogPath) })
	if log := readAbsoluteTestFile(t, preparationErr.LogPath); !strings.Contains(log, "broken") || !strings.Contains(log, "exit status 9") {
		t.Fatalf("failure log = %q", log)
	}
	attemptDir := filepath.Join(runsRoot, "run-1", "task-id", "attempt-1")
	if _, statErr := os.Stat(attemptDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("incomplete attempt directory remains: %v", statErr)
	}
	assertNoPreparationStages(t, filepath.Dir(attemptDir))
}

func TestWorkspacePreparerTimesOutAndCleansSetup(t *testing.T) {
	repository := newGitFixture(t)
	runsRoot := t.TempDir()
	task := workspaceTask("task-id", "task")
	request := workspaceRequest(repository, "main", task, "attempt-1")
	request.Environment.Setup.Commands = []string{"sleep 5"}
	request.Environment.Setup.Timeout = 50 * time.Millisecond

	started := time.Now()
	_, err := workspacePreparer(runsRoot, "").Prepare(context.Background(), request)
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("setup timeout took %s", elapsed)
	}
	var preparationErr *PreparationError
	if !errors.As(err, &preparationErr) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline PreparationError", err)
	}
	if preparationErr.LogPath == "" {
		t.Fatal("timeout did not retain preparation log")
	}
	t.Cleanup(func() { _ = os.Remove(preparationErr.LogPath) })
	attemptDir := filepath.Join(runsRoot, "run-1", "task-id", "attempt-1")
	if _, statErr := os.Stat(attemptDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("timed-out attempt directory remains: %v", statErr)
	}
	assertNoPreparationStages(t, filepath.Dir(attemptDir))
}

func TestWorkspacePreparerCleansInputChecksumFailure(t *testing.T) {
	repository := newGitFixture(t)
	runsRoot := t.TempDir()
	storage := t.TempDir()
	task := workspaceTask("task-id", "task")
	request := workspaceRequest(repository, "main", task, "attempt-1")
	input := storedInputArtifact(t, storage, "run-1", "input.txt", "content")
	input.SHA256 = strings.Repeat("0", 64)
	request.InputArtifacts = []domain.Artifact{input}

	_, err := workspacePreparer(runsRoot, storage).Prepare(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("error = %v, want checksum mismatch", err)
	}
	var preparationErr *PreparationError
	if !errors.As(err, &preparationErr) || preparationErr.LogPath == "" {
		t.Fatalf("error = %v, want retained failure log", err)
	}
	t.Cleanup(func() { _ = os.Remove(preparationErr.LogPath) })
	attemptDir := filepath.Join(runsRoot, "run-1", "task-id", "attempt-1")
	if _, statErr := os.Stat(attemptDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed attempt directory remains: %v", statErr)
	}
	assertNoPreparationStages(t, filepath.Dir(attemptDir))
}

func TestWorkspacePreparerRejectsCrossRunInput(t *testing.T) {
	repository := newGitFixture(t)
	runsRoot := t.TempDir()
	storage := t.TempDir()
	task := workspaceTask("task-id", "task")
	request := workspaceRequest(repository, "main", task, "attempt-1")
	input := storedInputArtifact(t, storage, "another-run", "input.txt", "content")
	request.InputArtifacts = []domain.Artifact{input}

	_, err := workspacePreparer(runsRoot, storage).Prepare(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), `belongs to run "another-run", want "run-1"`) {
		t.Fatalf("error = %v, want cross-run rejection", err)
	}
	var preparationErr *PreparationError
	if !errors.As(err, &preparationErr) || preparationErr.LogPath == "" {
		t.Fatalf("error = %v, want retained failure log", err)
	}
	t.Cleanup(func() { _ = os.Remove(preparationErr.LogPath) })
	attemptDir := filepath.Join(runsRoot, "run-1", "task-id", "attempt-1")
	if _, statErr := os.Stat(attemptDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed attempt directory remains: %v", statErr)
	}
}

func TestWorkspacePreparerRejectsWorkflowScope(t *testing.T) {
	task := workspaceTask("task-id", "task")
	request := workspaceRequest("repository", "main", task, "attempt-1")
	request.Environment.Scope = EnvironmentScopeWorkflow
	_, err := workspacePreparer(t.TempDir(), "").Prepare(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "not supported by per-attempt preparation") {
		t.Fatalf("error = %v, want workflow scope rejection", err)
	}
}

func workspacePreparer(runsRoot, storageRoot string) WorkspacePreparer {
	return WorkspacePreparer{
		RunsRoot: runsRoot, StorageRoot: storageRoot,
		Cache:     LocalRepositoryCache{Root: filepath.Join(runsRoot, "cache")},
		Processes: testProcessRunner{},
	}
}

func workspaceRequest(repository, ref string, task domain.Task, attemptID string) WorkspacePreparation {
	return WorkspacePreparation{
		WorkflowRunID: "run-1",
		Task:          task,
		Attempt: domain.Attempt{
			ID: attemptID, WorkflowRunID: "run-1", TaskID: task.ID, Number: 1,
		},
		Environment: ResolvedEnvironment{
			ProjectName: "project", Repository: repository, Ref: ref,
			Scope: EnvironmentScopeTask,
			Setup: SetupProfile{Name: "test", Commands: []string{"true"}, Timeout: time.Minute},
		},
	}
}

func workspaceTask(id, name string) domain.Task {
	return domain.Task{ID: id, WorkflowID: "workflow-1", Name: name}
}

func storedInputArtifact(t *testing.T, storage, runID, name, content string) domain.Artifact {
	t.Helper()
	storagePath := filepath.ToSlash(filepath.Join("workflows", "workflow-1", "files", name))
	writeTestFile(t, storage, storagePath, content)
	sum := sha256.Sum256([]byte(content))
	return domain.Artifact{
		ID: "input-" + name, WorkflowRunID: runID, Kind: domain.ArtifactInput,
		Name: name, MediaType: mediaType(name), Size: int64(len(content)),
		SHA256: hex.EncodeToString(sum[:]), StoragePath: storagePath,
		Producer: "submission", CreatedAt: dagTestTime,
	}
}

func newGitFixture(t *testing.T) string {
	t.Helper()
	repository := t.TempDir()
	gitRun(t, repository, "init", "--initial-branch=main")
	gitRun(t, repository, "config", "user.name", "Test User")
	gitRun(t, repository, "config", "user.email", "test@example.test")
	writeGitFile(t, repository, "version.txt", "first\n")
	gitRun(t, repository, "add", "version.txt")
	gitRun(t, repository, "commit", "-m", "first")
	return repository
}

func writeGitFile(t *testing.T, repository, relative, content string) {
	t.Helper()
	path := filepath.Join(repository, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create Git fixture directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write Git fixture: %v", err)
	}
}

func gitRun(t *testing.T, repository string, args ...string) {
	t.Helper()
	_ = gitOutput(t, repository, args...)
}

func gitOutput(t *testing.T, repository string, args ...string) string {
	t.Helper()
	commandArgs := append([]string{"-C", repository}, args...)
	command := exec.Command("git", commandArgs...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}

func readTestFile(t *testing.T, root, relative string) string {
	t.Helper()
	return readAbsoluteTestFile(t, filepath.Join(root, filepath.FromSlash(relative)))
}

func readAbsoluteTestFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(content)
}

func assertNoPreparationStages(t *testing.T, parent string) {
	t.Helper()
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatalf("read preparation parent: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".prepare-") {
			t.Fatalf("preparation stage remains: %s", entry.Name())
		}
	}
}
