package backlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// CachedRepository is a bare repository prepared as the source of an independent clone.
type CachedRepository struct {
	Path   string
	Reused bool
}

// RepositoryCache prepares and refreshes a clone source without sharing its object
// store with the eventual task checkout.
type RepositoryCache interface {
	Prepare(context.Context, string, io.Writer) (CachedRepository, error)
}

// LocalRepositoryCache maintains one bare mirror per canonical repository.
type LocalRepositoryCache struct {
	Root      string
	GitBinary string
}

// Prepare creates or refreshes the repository's bare mirror.
func (c LocalRepositoryCache) Prepare(ctx context.Context, repository string, log io.Writer) (CachedRepository, error) {
	if c.Root == "" {
		return CachedRepository{}, errors.New("repository cache root is required")
	}
	if repository == "" {
		return CachedRepository{}, errors.New("repository is required")
	}
	if err := os.MkdirAll(c.Root, 0o755); err != nil {
		return CachedRepository{}, fmt.Errorf("create repository cache: %w", err)
	}
	sum := sha256.Sum256([]byte(repository))
	cacheID := fmt.Sprintf("%x", sum)
	lock, err := acquireFileLock(ctx, c.Root, "repository:"+repository)
	if err != nil {
		return CachedRepository{}, fmt.Errorf("lock repository cache: %w", err)
	}
	defer lock.Close()

	stagePrefix := ".clone-" + cacheID + "-"
	if err := removeStageDirectories(c.Root, stagePrefix); err != nil {
		return CachedRepository{}, fmt.Errorf("reconcile repository cache: %w", err)
	}
	cachePath := filepath.Join(c.Root, cacheID+".git")
	info, err := os.Stat(cachePath)
	if err == nil {
		if !info.IsDir() {
			return CachedRepository{}, fmt.Errorf("repository cache path %q is not a directory", cachePath)
		}
		if err := runLoggedCommand(ctx, log, "", c.git(), "--git-dir", cachePath, "remote", "update", "--prune"); err != nil {
			return CachedRepository{}, fmt.Errorf("refresh repository cache: %w", err)
		}
		return CachedRepository{Path: cachePath, Reused: true}, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return CachedRepository{}, fmt.Errorf("inspect repository cache: %w", err)
	}

	stageRoot, err := os.MkdirTemp(c.Root, stagePrefix)
	if err != nil {
		return CachedRepository{}, fmt.Errorf("stage repository cache: %w", err)
	}
	defer removeIngestedTree(stageRoot)
	stagePath := filepath.Join(stageRoot, "repository.git")
	if err := runLoggedCommand(ctx, log, "", c.git(), "clone", "--mirror", "--", repository, stagePath); err != nil {
		return CachedRepository{}, fmt.Errorf("clone repository cache: %w", err)
	}
	if err := os.Rename(stagePath, cachePath); err != nil {
		return CachedRepository{}, fmt.Errorf("publish repository cache: %w", err)
	}
	return CachedRepository{Path: cachePath}, nil
}

func (c LocalRepositoryCache) git() string {
	if c.GitBinary != "" {
		return c.GitBinary
	}
	return "git"
}

func removeStageDirectories(root, prefix string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() {
			return fmt.Errorf("staging path %q is not a real directory", filepath.Join(root, entry.Name()))
		}
		if err := removeIngestedTree(filepath.Join(root, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

// WorkspacePreparation identifies one isolated task attempt and all immutable inputs.
type WorkspacePreparation struct {
	WorkflowRunID       string
	Task                domain.Task
	Attempt             domain.Attempt
	Environment         ResolvedEnvironment
	InputArtifacts      []domain.Artifact
	DependencyTasks     []domain.Task
	DependencyArtifacts []domain.Artifact
}

// PreparedWorkspace is the published run directory and pinned source revision.
type PreparedWorkspace struct {
	RootDir         string
	WorkspaceDir    string
	InputsDir       string
	DependenciesDir string
	PreparationLog  string
	Commit          string
	CacheReused     bool
}

// PreparationError reports an operational preparation failure and, when setup
// had begun, the retained log outside the removed incomplete environment.
type PreparationError struct {
	Err     error
	LogPath string
}

func (e *PreparationError) Error() string {
	if e.LogPath == "" {
		return "prepare workspace: " + e.Err.Error()
	}
	return fmt.Sprintf("prepare workspace: %v (log: %s)", e.Err, e.LogPath)
}

func (e *PreparationError) Unwrap() error { return e.Err }

// WorkspacePreparer builds clean per-attempt Git environments without contacting T3.
type WorkspacePreparer struct {
	RunsRoot    string
	StorageRoot string
	Cache       RepositoryCache
	GitBinary   string
	Processes   ProcessRunner
}

// Prepare resolves a ref once, creates an independent checkout, materializes
// immutable inputs, runs setup under its timeout, and atomically publishes the run.
func (p WorkspacePreparer) Prepare(ctx context.Context, request WorkspacePreparation) (PreparedWorkspace, error) {
	if err := p.validate(request); err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: err}
	}
	parent := filepath.Join(p.RunsRoot, request.WorkflowRunID, request.Task.ID)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("create run parent: %w", err)}
	}
	finalDir := filepath.Join(parent, request.Attempt.ID)
	if _, err := os.Lstat(finalDir); err == nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("attempt directory %q already exists", finalDir)}
	} else if !errors.Is(err, os.ErrNotExist) {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("inspect attempt directory: %w", err)}
	}

	stageDir, err := os.MkdirTemp(parent, ".prepare-")
	if err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("create preparation stage: %w", err)}
	}
	published := false
	defer func() {
		if !published {
			removeIngestedTree(stageDir)
		}
	}()

	logPath := filepath.Join(stageDir, "preparation.log")
	logFile, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("create preparation log: %w", err)}
	}
	fail := func(cause error) (PreparedWorkspace, error) {
		_ = logFile.Close()
		retained := filepath.Join(parent, request.Attempt.ID+".preparation.log")
		if copyErr := copyFileExclusive(logPath, retained, 0o600); copyErr != nil {
			cause = fmt.Errorf("%w (retain preparation log: %v)", cause, copyErr)
			retained = ""
		}
		return PreparedWorkspace{}, &PreparationError{Err: cause, LogPath: retained}
	}

	cached, err := p.Cache.Prepare(ctx, request.Environment.Repository, logFile)
	if err != nil {
		return fail(fmt.Errorf("prepare repository cache: %w", err))
	}
	commitBytes, err := runLoggedCommandOutput(ctx, logFile, "", p.git(), "--git-dir", cached.Path, "rev-parse", "--verify", request.Environment.Ref+"^{commit}")
	if err != nil {
		return fail(fmt.Errorf("resolve ref %q: %w", request.Environment.Ref, err))
	}
	commit := strings.TrimSpace(string(commitBytes))
	if !validGitObjectID(commit) {
		return fail(fmt.Errorf("resolve ref %q: Git returned invalid commit %q", request.Environment.Ref, commit))
	}

	workspaceDir := filepath.Join(stageDir, "workspace")
	if err := runLoggedCommand(ctx, logFile, "", p.git(), "clone", "--no-local", "--no-checkout", "--", cached.Path, workspaceDir); err != nil {
		return fail(fmt.Errorf("clone task workspace: %w", err))
	}
	if err := runLoggedCommand(ctx, logFile, "", p.git(), "-C", workspaceDir, "checkout", "--detach", commit); err != nil {
		return fail(fmt.Errorf("checkout pinned commit %s: %w", commit, err))
	}
	head, err := runLoggedCommandOutput(ctx, logFile, "", p.git(), "-C", workspaceDir, "rev-parse", "HEAD")
	if err != nil {
		return fail(fmt.Errorf("verify checkout: %w", err))
	}
	if strings.TrimSpace(string(head)) != commit {
		return fail(fmt.Errorf("verify checkout: HEAD changed from pinned commit %s", commit))
	}
	if err := p.materializeInputs(stageDir, request); err != nil {
		return fail(err)
	}
	if err := p.materializeDependencyView(stageDir, request); err != nil {
		return fail(err)
	}
	if err := exposeWorkspaceInputs(workspaceDir); err != nil {
		return fail(err)
	}

	setupCtx, cancel := context.WithTimeout(ctx, request.Environment.Setup.Timeout)
	defer cancel()
	for index, command := range request.Environment.Setup.Commands {
		processID := fmt.Sprintf("setup-%s-%s-%s-%d", request.WorkflowRunID, request.Task.ID, request.Attempt.ID, index)
		_, err := p.processRunner().Run(setupCtx, ProcessRequest{
			ID: processID, Dir: workspaceDir, Program: "/bin/sh", Args: []string{"-c", command}, Log: logFile,
		})
		if err != nil {
			if setupCtx.Err() != nil {
				return fail(fmt.Errorf("setup command %q: %w", command, setupCtx.Err()))
			}
			return fail(fmt.Errorf("setup command %q: %w", command, err))
		}
	}
	if err := logFile.Close(); err != nil {
		return fail(fmt.Errorf("close preparation log: %w", err))
	}
	if err := os.Chmod(logPath, 0o444); err != nil {
		return fail(fmt.Errorf("protect preparation log: %w", err))
	}
	if err := os.Rename(stageDir, finalDir); err != nil {
		return fail(fmt.Errorf("publish prepared workspace: %w", err))
	}
	published = true
	return PreparedWorkspace{
		RootDir: finalDir, WorkspaceDir: filepath.Join(finalDir, "workspace"),
		InputsDir: filepath.Join(finalDir, "inputs"), DependenciesDir: filepath.Join(finalDir, "dependencies"),
		PreparationLog: filepath.Join(finalDir, "preparation.log"),
		Commit:         commit, CacheReused: cached.Reused,
	}, nil
}

func (p WorkspacePreparer) validate(request WorkspacePreparation) error {
	if p.RunsRoot == "" {
		return errors.New("runs root is required")
	}
	if p.Cache == nil {
		return errors.New("repository cache is required")
	}
	if request.WorkflowRunID == "" || request.Task.ID == "" || request.Attempt.ID == "" {
		return errors.New("workflow run, task, and attempt IDs are required")
	}
	for label, value := range map[string]string{
		"workflow run ID": request.WorkflowRunID,
		"task ID":         request.Task.ID,
		"attempt ID":      request.Attempt.ID,
	} {
		if !safePathComponent(value) {
			return fmt.Errorf("%s %q is not a safe path component", label, value)
		}
	}
	if request.Attempt.WorkflowRunID != request.WorkflowRunID || request.Attempt.TaskID != request.Task.ID {
		return errors.New("attempt does not belong to the requested workflow run and task")
	}
	if request.Environment.Scope != EnvironmentScopeTask {
		return fmt.Errorf("environment scope %q is not supported by per-attempt preparation", request.Environment.Scope)
	}
	if request.Environment.Repository == "" {
		return errors.New("repository is required")
	}
	if err := validateGitRef(request.Environment.Ref); err != nil {
		return fmt.Errorf("environment ref: %w", err)
	}
	if request.Environment.Setup.Timeout <= 0 {
		return errors.New("setup timeout must be positive")
	}
	for _, command := range request.Environment.Setup.Commands {
		if strings.TrimSpace(command) == "" {
			return errors.New("setup command must not be empty")
		}
	}
	if (len(request.InputArtifacts) != 0 || len(request.DependencyArtifacts) != 0) && p.StorageRoot == "" {
		return errors.New("artifact storage root is required")
	}
	return nil
}

func (p WorkspacePreparer) materializeInputs(stageDir string, request WorkspacePreparation) error {
	inputsDir := filepath.Join(stageDir, "inputs")
	if err := os.Mkdir(inputsDir, 0o755); err != nil {
		return fmt.Errorf("create input directory: %w", err)
	}
	if len(request.InputArtifacts) == 0 {
		return os.Chmod(inputsDir, 0o555)
	}
	storage, err := os.OpenRoot(p.StorageRoot)
	if err != nil {
		return fmt.Errorf("open artifact storage: %w", err)
	}
	defer storage.Close()
	seen := make(map[string]struct{}, len(request.InputArtifacts))
	for _, artifact := range request.InputArtifacts {
		if artifact.Kind != domain.ArtifactInput {
			return fmt.Errorf("materialize input %q: artifact kind is %q", artifact.Name, artifact.Kind)
		}
		if artifact.WorkflowRunID != request.WorkflowRunID {
			return fmt.Errorf("materialize input %q: artifact belongs to run %q, want %q", artifact.Name, artifact.WorkflowRunID, request.WorkflowRunID)
		}
		if err := validateRelativePath(artifact.Name, false); err != nil {
			return fmt.Errorf("materialize input %q: %w", artifact.Name, err)
		}
		if _, duplicate := seen[artifact.Name]; duplicate {
			return fmt.Errorf("materialize input %q: duplicate artifact name", artifact.Name)
		}
		seen[artifact.Name] = struct{}{}
		resolved, err := safeBundleFile(p.StorageRoot, filepath.FromSlash(artifact.StoragePath))
		if err != nil {
			return fmt.Errorf("materialize input %q: %w", artifact.Name, err)
		}
		source, err := filepath.Rel(p.StorageRoot, resolved)
		if err != nil {
			return fmt.Errorf("materialize input %q: %w", artifact.Name, err)
		}
		file, err := copyIngestedFile(storage, source, filepath.Join(inputsDir, filepath.FromSlash(artifact.Name)), artifact.Name, "")
		if err != nil {
			return fmt.Errorf("materialize input %q: %w", artifact.Name, err)
		}
		if file.size != artifact.Size || !strings.EqualFold(file.sha256, artifact.SHA256) {
			return fmt.Errorf("materialize input %q: checksum mismatch", artifact.Name)
		}
	}
	if err := makeIngestedTreeImmutable(inputsDir); err != nil {
		return fmt.Errorf("protect input directory: %w", err)
	}
	return nil
}

func (p WorkspacePreparer) materializeDependencyView(stageDir string, request WorkspacePreparation) error {
	dependenciesDir := filepath.Join(stageDir, "dependencies")
	if len(request.Task.DependencyInputs) == 0 {
		if err := os.Mkdir(dependenciesDir, 0o555); err != nil {
			return fmt.Errorf("create dependency directory: %w", err)
		}
		return nil
	}
	scratch := filepath.Join(stageDir, ".dependency-view")
	if err := os.Mkdir(scratch, 0o755); err != nil {
		return fmt.Errorf("create dependency staging workspace: %w", err)
	}
	_, err := MaterializeDependencies(
		scratch, p.StorageRoot, request.WorkflowRunID,
		request.Task, request.DependencyTasks, request.DependencyArtifacts,
	)
	if err != nil {
		return err
	}
	materializedDir := filepath.Join(scratch, ".t3", "dependencies")
	if err := os.Chmod(materializedDir, 0o755); err != nil {
		return fmt.Errorf("make dependency view movable: %w", err)
	}
	if err := os.Rename(materializedDir, dependenciesDir); err != nil {
		return fmt.Errorf("publish dependency view: %w", err)
	}
	if err := makeIngestedTreeImmutable(dependenciesDir); err != nil {
		return fmt.Errorf("protect dependency view: %w", err)
	}
	if err := removeIngestedTree(scratch); err != nil {
		return fmt.Errorf("remove dependency staging workspace: %w", err)
	}
	return nil
}

func exposeWorkspaceInputs(workspaceDir string) error {
	metadataDir := filepath.Join(workspaceDir, ".t3")
	if info, err := os.Lstat(metadataDir); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return errors.New("prepare workspace: repository .t3 is not a real directory")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("prepare workspace: inspect repository .t3: %w", err)
	} else if err := os.Mkdir(metadataDir, 0o755); err != nil {
		return fmt.Errorf("prepare workspace: create .t3: %w", err)
	}
	for name, target := range map[string]string{
		"inputs": "../../inputs", "dependencies": "../../dependencies",
	} {
		if err := os.Symlink(target, filepath.Join(metadataDir, name)); err != nil {
			return fmt.Errorf("prepare workspace: expose %s: %w", name, err)
		}
	}
	return nil
}

func (p WorkspacePreparer) git() string {
	if p.GitBinary != "" {
		return p.GitBinary
	}
	return "git"
}

func (p WorkspacePreparer) processRunner() ProcessRunner {
	if p.Processes != nil {
		return p.Processes
	}
	return SystemdScopeRunner{}
}

func runLoggedCommand(ctx context.Context, log io.Writer, dir, program string, args ...string) error {
	_, err := runLoggedCommandOutput(ctx, log, dir, program, args...)
	return err
}

func runLoggedCommandOutput(ctx context.Context, log io.Writer, dir, program string, args ...string) ([]byte, error) {
	if log == nil {
		log = io.Discard
	}
	fmt.Fprintf(log, "$ %s %s\n", program, strings.Join(args, " "))
	command := exec.CommandContext(ctx, program, args...)
	command.Dir = dir
	output, err := command.CombinedOutput()
	if len(output) != 0 {
		_, _ = log.Write(output)
		if output[len(output)-1] != '\n' {
			_, _ = io.WriteString(log, "\n")
		}
	}
	if err != nil {
		fmt.Fprintf(log, "! %v\n", err)
	}
	return output, err
}

func copyFileExclusive(source, destination string, mode os.FileMode) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func validGitObjectID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
