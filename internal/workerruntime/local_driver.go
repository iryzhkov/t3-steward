package workerruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type ArtifactSource interface {
	OpenArtifact(context.Context, workerproto.ArtifactObject) (io.ReadCloser, error)
}

type PublishedResult struct {
	Finalized     backlog.FinalizedAttempt
	FinalMessage  string
	ThreadArchive []byte
}

type ArtifactPublisher interface {
	PublishResult(context.Context, workerproto.ExecutionPackage, PublishedResult) error
	PublishCheckpoint(context.Context, workerproto.ExecutionPackage, string, []byte) (*domain.CheckpointMetadata, error)
}

type T3Control interface {
	ListThreads(context.Context) ([]domain.Thread, error)
	GetThread(context.Context, string) (*domain.Thread, error)
	ResolveProjectID(context.Context, string) (string, error)
	EnsureProject(context.Context, t3control.ManagedProject) (string, error)
	CreateAndStartThread(context.Context, t3control.NewThreadInput) (string, error)
	StopThread(context.Context, domain.Thread, t3control.StopMode) error
	SettleThread(context.Context, string, string) error
	WaitStopped(context.Context, string, time.Duration) (*domain.Thread, bool, error)
	WarnThread(context.Context, domain.Thread, domain.Warning) error
	ResumeThread(context.Context, domain.Thread, string) error
	LastAssistantMessage(context.Context, string) (string, error)
	ExportThread(context.Context, string) ([]byte, error)
}

type LocalDriverConfig struct {
	CatalogRevision string
	ArtifactRoot    string
	RunsRoot        string
	StopTimeout     time.Duration
	// PreflightFreshness bounds how long a preflight receipt may be reused for
	// an unchanged identity. Zero uses DefaultPreflightFreshness.
	PreflightFreshness time.Duration
	RetainWorkspaces   bool
	DryRun             bool
}

type LocalDriver struct {
	Config      LocalDriverConfig
	Catalog     *backlog.ProjectCatalog
	Workspace   backlog.WorkspacePreparer
	Finalizer   backlog.AttemptFinalizer
	Source      ArtifactSource
	Publisher   ArtifactPublisher
	Credentials CredentialChecker
	// Preflight runs declared preflight steps inside the attempt environment.
	// It is the same process abstraction verification uses, and a separate
	// entry point because preflight runs before the provider session exists.
	Preflight backlog.PreflightRunner
	T3        T3Control
	ScopedT3  ExecutionT3Provider
	scoped    bool
	Now       func() time.Time
}

func NewLocalDriver(driver LocalDriver) (*LocalDriver, error) {
	if driver.Config.CatalogRevision == "" || driver.Config.ArtifactRoot == "" || driver.Config.RunsRoot == "" {
		return nil, errors.New("worker local driver: catalog revision, artifact root, and runs root are required")
	}
	if driver.Catalog == nil || driver.Source == nil || driver.Publisher == nil {
		return nil, errors.New("worker local driver: catalog, artifact source, and publisher are required")
	}
	if !driver.Config.DryRun && driver.T3 == nil {
		return nil, errors.New("worker local driver: T3 control is required outside no-effects mode")
	}
	if driver.Config.StopTimeout <= 0 {
		driver.Config.StopTimeout = 30 * time.Second
	}
	if driver.Now == nil {
		driver.Now = time.Now
	}
	artifactRoot, err := safeLocalRoot(driver.Config.ArtifactRoot)
	if err != nil {
		return nil, fmt.Errorf("worker local driver: artifact root: %w", err)
	}
	runsRoot, err := safeLocalRoot(driver.Config.RunsRoot)
	if err != nil {
		return nil, fmt.Errorf("worker local driver: runs root: %w", err)
	}
	if err := ensureRealDirectory(filepath.Join(artifactRoot, "objects")); err != nil {
		return nil, fmt.Errorf("worker local driver: artifact objects: %w", err)
	}
	driver.Config.ArtifactRoot = artifactRoot
	driver.Config.RunsRoot = runsRoot
	driver.Workspace.RunsRoot = runsRoot
	driver.Workspace.StorageRoot = artifactRoot
	driver.Finalizer.StorageRoot = artifactRoot
	return &driver, nil
}

func (d *LocalDriver) Prepare(ctx context.Context, pkg workerproto.ExecutionPackage) (string, error) {
	environment, task, attempt, err := d.executionRecords(pkg)
	if err != nil {
		return "", err
	}
	if d.containedManager(pkg) != nil && (len(environment.Setup.Commands) != 0 || len(environment.RequiredCredentials) != 0) {
		return "", errors.New("contained preparation requires an empty setup profile and no host credentials")
	}
	if pkg.Environment.CatalogRevision != d.Config.CatalogRevision {
		return "", errors.New("execution package catalog revision is stale")
	}
	if len(environment.RequiredCredentials) > 0 {
		if d.Credentials == nil {
			return "", errors.New("prepare workspace: required credential resolver is unavailable")
		}
		if err := d.Credentials.Require(ctx, environment.RequiredCredentials); err != nil {
			return "", err
		}
	}
	// Workspace publication precedes the durable server launch. Recovery must
	// reuse that publication, including when the readiness reply was lost.
	if manager := d.containedManager(pkg); manager != nil {
		workspace := filepath.Join(d.workspacePath(pkg), "workspace")
		if info, err := os.Lstat(workspace); err == nil {
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return "", errors.New("contained workspace is not a real directory")
			}
			if err := manager.PrepareExecution(ctx, pkg, workspace); err != nil {
				return "", err
			}
			return workspace, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
	}
	inputs := make([]domain.Artifact, 0, len(pkg.StaticInputs))
	dependencyTasks := make([]domain.Task, 0, len(pkg.Dependencies))
	dependencyArtifacts := make([]domain.Artifact, 0)
	objects := append([]workerproto.ArtifactObject{pkg.Prompt}, pkg.StaticInputs...)
	for _, dependency := range pkg.Dependencies {
		objects = append(objects, dependency.Artifacts...)
	}
	for _, object := range objects {
		if _, err := d.cacheArtifact(ctx, object, pkg.Limits.MaxArtifactBytes); err != nil {
			return "", err
		}
	}
	for _, object := range pkg.StaticInputs {
		// The package namespaces static objects under inputs/; the preparer
		// already supplies that directory. Preserve the relative artifact name,
		// including nested paths, and tolerate older unprefixed packages.
		name := strings.TrimPrefix(filepath.ToSlash(object.Path), "inputs/")
		inputs = append(inputs, d.domainArtifact(pkg, object, domain.ArtifactInput, name, "submission"))
	}
	for _, dependency := range pkg.Dependencies {
		dependencyTask := domain.Task{ID: dependency.TaskID, WorkflowID: pkg.Identity.WorkflowID, Name: dependency.TaskID}
		names := make([]string, 0, len(dependency.Artifacts))
		for _, object := range dependency.Artifacts {
			parts := strings.Split(filepath.ToSlash(object.Path), "/")
			if len(parts) < 3 || parts[0] != "dependencies" {
				return "", fmt.Errorf("prepare workspace: invalid dependency object path %q", object.Path)
			}
			name := strings.Join(parts[2:], "/")
			names = append(names, name)
			artifact := d.domainArtifact(pkg, object, domain.ArtifactOutput, name, dependency.TaskID)
			artifact.TaskID = dependency.TaskID
			artifact.AttemptID = ""
			dependencyArtifacts = append(dependencyArtifacts, artifact)
		}
		task.Needs = append(task.Needs, dependency.TaskID)
		if task.DependencyInputs == nil {
			task.DependencyInputs = make(map[string][]string)
		}
		task.DependencyInputs[dependency.TaskID] = names
		dependencyTasks = append(dependencyTasks, dependencyTask)
	}
	if d.Config.DryRun {
		path := d.workspacePath(pkg)
		if err := os.MkdirAll(filepath.Join(path, "workspace"), 0o700); err != nil {
			return "", fmt.Errorf("worker no-effects preparation: %w", err)
		}
		workspace := filepath.Join(path, "workspace")
		if err := d.writeTaskIdentity(pkg, workspace); err != nil {
			return "", err
		}
		return workspace, nil
	}
	if pkg.Identity.AssignmentEpoch > 1 {
		attempt.ID = fmt.Sprintf("%s-e%d", attempt.ID, pkg.Identity.AssignmentEpoch)
	}
	prepared, err := d.Workspace.Prepare(ctx, backlog.WorkspacePreparation{
		WorkflowRunID: pkg.Identity.WorkflowRunID, Task: task, Attempt: attempt,
		Environment: environment, InputArtifacts: inputs, DependencyTasks: dependencyTasks,
		DependencyArtifacts: dependencyArtifacts,
	})
	if err != nil {
		return "", err
	}
	// The identity record goes in with the workspace, which is the one moment
	// the real directory is certainly present and certainly ours. Dispatch
	// always follows preparation, so it is there before any thread exists, and
	// a scoped execution sees it through the same directory under its own mount
	// rather than needing a second copy written against a mapped path.
	if err := d.writeTaskIdentity(pkg, prepared.WorkspaceDir); err != nil {
		return "", err
	}
	if manager := d.containedManager(pkg); manager != nil {
		if err := manager.PrepareExecution(ctx, pkg, prepared.WorkspaceDir); err != nil {
			return "", err
		}
	}
	return prepared.WorkspaceDir, nil
}

func (d *LocalDriver) InspectWorkspace(ctx context.Context, pkg workerproto.ExecutionPackage) (string, bool, error) {
	workspace := filepath.Join(d.workspacePath(pkg), "workspace")
	info, err := os.Lstat(workspace)
	if errors.Is(err, os.ErrNotExist) {
		return workspace, false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("inspect prepared workspace: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", false, errors.New("prepared workspace is not a real directory")
	}
	if manager := d.containedManager(pkg); manager != nil {
		if _, err := manager.load(pkg); errors.Is(err, os.ErrNotExist) {
			return workspace, false, nil
		} else if err != nil {
			return workspace, false, err
		}
		if _, err := manager.Attach(ctx, pkg); err != nil {
			return workspace, false, err
		}
	}
	return workspace, true, nil
}

// BeginObservationPass drops observations retained by a previous reconciliation.
// Persistent workers reuse the driver, but each pass must see current T3 state.
func (d *LocalDriver) BeginObservationPass() {
	if scoped, ok := d.ScopedT3.(interface{ BeginObservationPass() }); ok {
		scoped.BeginObservationPass()
	}
	if cache, ok := d.T3.(interface{ invalidate() }); ok {
		cache.invalidate()
	}
}

func (d *LocalDriver) ObserveThread(ctx context.Context, pkg workerproto.ExecutionPackage) (backlog.DispatchThreadState, error) {
	if scoped, err := d.scopedDriver(ctx, pkg); err != nil {
		return "", err
	} else if scoped != nil {
		return scoped.ObserveThread(ctx, pkg)
	}
	if d.Config.DryRun {
		state, err := os.ReadFile(d.noEffectsThreadPath(pkg))
		if errors.Is(err, os.ErrNotExist) {
			return backlog.DispatchThreadMissing, nil
		}
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(string(state)) == "active" {
			return backlog.DispatchThreadActive, nil
		}
		return backlog.DispatchThreadStopped, nil
	}
	thread, err := d.T3.GetThread(ctx, pkg.Identity.ThreadID)
	if err != nil {
		return "", err
	}
	if thread == nil {
		return backlog.DispatchThreadMissing, nil
	}
	if !workerThreadTerminal(*thread) {
		return backlog.DispatchThreadActive, nil
	}
	return backlog.DispatchThreadStopped, nil
}

// A newly accepted T3 start may be visible before its turn and session.
// Only positive terminal evidence permits collection or skipping containment.
func workerThreadTerminal(thread domain.Thread) bool {
	if thread.Running || thread.BackgroundWork == "working" {
		return false
	}
	if thread.Settled() {
		return true
	}
	switch thread.TurnState {
	case "completed", "interrupted", "error":
		return true
	default:
		return false
	}
}

// writeTaskIdentity records the attempt's identity inside the prepared
// workspace, before any thread is dispatched, so an agent can name itself when
// it registers a task-bound wait.
//
// It is written 0600 and holds identity only. A workspace that is archived must
// not carry it, which is why Collect removes it before outputs are captured.
func (d *LocalDriver) writeTaskIdentity(pkg workerproto.ExecutionPackage, workspace string) error {
	if workspace == "" {
		return errors.New("task identity needs a prepared workspace")
	}
	content, err := domain.RenderTaskIdentityFile(pkg.Identity.TaskEnvironment())
	if err != nil {
		return err
	}
	directory := filepath.Join(workspace, domain.TaskIdentityDir)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create task identity directory: %w", err)
	}
	path := filepath.Join(workspace, filepath.FromSlash(domain.TaskIdentityFile))
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return fmt.Errorf("write task identity: %w", err)
	}
	// WriteFile leaves an existing file's mode alone, and a resumed attempt
	// rewrites this one, so the mode is asserted rather than assumed.
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("restrict task identity: %w", err)
	}
	return excludeTaskIdentityFromGit(workspace, directory)
}

// excludeTaskIdentityFromGit keeps the identity record out of the project's
// history.
//
// The record lives inside the task's worktree and these tasks commit and push.
// Removing it when outputs are collected is far too late: by then it has
// already been committed by `git add -A` and pushed into the project repository
// and every checkout downstream of it. So it is excluded at the moment it is
// written, twice over. The self-ignoring .gitignore works in any layout and
// travels with the directory; the repository's own exclude file covers a tool
// that reads only that.
func excludeTaskIdentityFromGit(workspace, directory string) error {
	if err := os.WriteFile(filepath.Join(directory, ".gitignore"), []byte("# Steward task identity. Never commit this.\n*\n"), 0o600); err != nil {
		return fmt.Errorf("exclude task identity: %w", err)
	}
	gitDir, err := resolveGitDir(workspace)
	if err != nil || gitDir == "" {
		// Not a repository, or one whose layout we do not recognise. The
		// self-ignoring file above is still in place.
		return nil
	}
	info := filepath.Join(gitDir, "info")
	if err := os.MkdirAll(info, 0o700); err != nil {
		return nil
	}
	exclude := filepath.Join(info, "exclude")
	line := "/" + domain.TaskIdentityDir + "/"
	current, err := os.ReadFile(exclude)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil
	}
	for _, existing := range strings.Split(string(current), "\n") {
		if strings.TrimSpace(existing) == line {
			return nil
		}
	}
	updated := string(current)
	if updated != "" && !strings.HasSuffix(updated, "\n") {
		updated += "\n"
	}
	updated += line + "\n"
	if err := os.WriteFile(exclude, []byte(updated), 0o600); err != nil {
		return nil
	}
	return nil
}

// resolveGitDir finds a worktree's git directory, following the gitdir pointer
// a linked worktree uses instead of a real .git directory.
func resolveGitDir(workspace string) (string, error) {
	marker := filepath.Join(workspace, ".git")
	info, err := os.Lstat(marker)
	if err != nil {
		return "", err
	}
	if info.IsDir() {
		return marker, nil
	}
	if !info.Mode().IsRegular() {
		return "", nil
	}
	raw, err := readBoundedRegularFile(marker, 4096)
	if err != nil {
		return "", err
	}
	pointer := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(string(raw)), "gitdir:"))
	if pointer == "" || pointer == strings.TrimSpace(string(raw)) {
		return "", nil
	}
	if !filepath.IsAbs(pointer) {
		pointer = filepath.Join(workspace, pointer)
	}
	return filepath.Clean(pointer), nil
}

// removeTaskIdentity deletes the identity record before anything is captured
// from the workspace. A parked turn never reaches here, so the file survives
// for the turn that resumes after the wake.
func (d *LocalDriver) removeTaskIdentity(workspace string) error {
	if workspace == "" {
		return nil
	}
	if err := os.RemoveAll(filepath.Join(workspace, domain.TaskIdentityDir)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove task identity: %w", err)
	}
	return nil
}

func (d *LocalDriver) CreateThread(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) error {
	if scoped, err := d.scopedDriver(ctx, pkg); err != nil {
		return err
	} else if scoped != nil {
		return scoped.CreateThread(ctx, pkg, workspace)
	}
	if d.Config.DryRun {
		return os.WriteFile(d.noEffectsThreadPath(pkg), []byte("active\n"), 0o600)
	}
	promptArtifact, err := d.readCachedArtifact(pkg.Prompt, pkg.Limits.MaxArtifactBytes)
	if err != nil {
		return err
	}
	// Preflight runs here: the workspace is prepared, inputs are materialized,
	// and no provider session exists yet. A blocking outcome returns before the
	// session is created, so a require-pass failure means no session at all.
	prompt, err := d.runPreflight(ctx, pkg, workspace, string(promptArtifact))
	if err != nil {
		return err
	}
	var projectID string
	if d.scoped {
		workspace = "/workspace"
		projectID, err = d.T3.EnsureProject(ctx, t3control.ManagedProject{Key: pkg.Identity.ThreadID, Title: "Steward: " + pkg.Environment.Project, WorkspaceRoot: "/workspace"})
	} else if pkg.Environment.T3Project != "" {
		projectID, err = d.T3.ResolveProjectID(ctx, pkg.Environment.T3Project)
	} else {
		encodedKey, keyErr := json.Marshal([]string{pkg.CoordinatorID, pkg.WorkerID, pkg.Environment.Project})
		if keyErr != nil {
			return keyErr
		}
		key := string(encodedKey)
		sum := sha256.Sum256(encodedKey)
		projectID, err = d.T3.EnsureProject(ctx, t3control.ManagedProject{
			Key: key, Title: "Steward: " + pkg.Environment.Project,
			WorkspaceRoot: filepath.Join(d.Config.RunsRoot, ".projects", hex.EncodeToString(sum[:])),
		})
	}
	if err != nil {
		return err
	}
	selection := map[string]any{"instanceId": pkg.Route.ProviderInstanceID, "model": pkg.Route.Model}
	if len(pkg.Route.Options) != 0 {
		keys := make([]string, 0, len(pkg.Route.Options))
		for key := range pkg.Route.Options {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		options := make([]any, 0, len(keys))
		for _, key := range keys {
			options = append(options, map[string]any{"id": key, "value": pkg.Route.Options[key]})
		}
		selection["options"] = options
	}
	threadID, err := d.T3.CreateAndStartThread(ctx, t3control.NewThreadInput{
		ThreadID: pkg.Identity.ThreadID, DispatchToken: pkg.Identity.DispatchToken,
		ProjectID: projectID, Title: pkg.Identity.TaskID,
		ModelSelection: selection, RuntimeMode: "full-access", InteractionMode: "default",
		WorktreePath: workspace, Prompt: prompt,
		Environment: pkg.Identity.TaskEnvironment(),
	})
	if threadID != "" && threadID != pkg.Identity.ThreadID {
		return errors.New("T3 returned a different deterministic thread identity")
	}
	return err
}

func (d *LocalDriver) StopThread(ctx context.Context, pkg workerproto.ExecutionPackage) error {
	if manager := d.containedManager(pkg); manager != nil {
		return manager.Quiesce(ctx, pkg, true)
	}
	if scoped, err := d.scopedDriver(ctx, pkg); err != nil {
		return err
	} else if scoped != nil {
		return scoped.StopThread(ctx, pkg)
	}
	if d.Config.DryRun {
		path := d.noEffectsThreadPath(pkg)
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return os.WriteFile(path, []byte("stopped\n"), 0o600)
	}
	thread, err := d.T3.GetThread(ctx, pkg.Identity.ThreadID)
	if err != nil {
		return err
	}
	if thread == nil {
		return nil
	}
	if !workerThreadTerminal(*thread) {
		if err := d.T3.StopThread(ctx, *thread, t3control.StopSession); err != nil {
			return err
		}
		_, stopped, err := d.T3.WaitStopped(ctx, pkg.Identity.ThreadID, d.Config.StopTimeout)
		if err != nil {
			return err
		}
		if !stopped {
			return errors.New("T3 thread did not stop before the containment deadline")
		}
	}
	if err := d.T3.SettleThread(ctx, pkg.Identity.ThreadID, pkg.Identity.DispatchToken); err != nil {
		return fmt.Errorf("settle stopped T3 thread: %w", err)
	}
	return nil
}

func (d *LocalDriver) Collect(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) error {
	// The identity record leaves before anything is captured from the
	// workspace, so it cannot reach a declared output, a git-state artifact or
	// an archived tree. It runs on every driver, scoped or not: the path may
	// already be gone, and removing nothing is cheaper than reasoning about
	// which driver in the chain owns the real one. A parked turn never reaches
	// Collect, so the record survives for the turn that resumes after the wake.
	if err := d.removeTaskIdentity(workspace); err != nil {
		return err
	}
	if !d.scoped {
		if manager := d.containedManager(pkg); manager != nil {
			if err := manager.Quiesce(ctx, pkg, false); err != nil {
				return err
			}
		}
	}
	if scoped, err := d.scopedDriver(ctx, pkg); err != nil {
		return err
	} else if scoped != nil {
		return scoped.Collect(ctx, pkg, workspace)
	}
	if d.Config.DryRun {
		return nil
	}
	thread, err := d.T3.GetThread(ctx, pkg.Identity.ThreadID)
	if err != nil {
		return fmt.Errorf("collect thread state: %w", err)
	}
	message, archive := "", []byte("{}")
	if thread != nil && !workerThreadTerminal(*thread) {
		return errors.New("T3 turn is not yet terminal; result collection deferred")
	}
	if thread != nil {
		// T3 marks the turn completed slightly before the final assistant
		// message is projected. Briefly retry an empty summary; completion
		// itself is established by the structured thread archive.
		for reads := 0; ; reads++ {
			if message, err = d.T3.LastAssistantMessage(ctx, pkg.Identity.ThreadID); err != nil {
				return fmt.Errorf("collect final message: %w", err)
			}
			if strings.TrimSpace(message) != "" || reads >= 3 {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
		if archive, err = d.T3.ExportThread(ctx, pkg.Identity.ThreadID); err != nil {
			return fmt.Errorf("collect thread archive: %w", err)
		}
	}
	failure, err := backlog.ResultCompletionFailure(archive, pkg.Identity.ThreadID, message)
	if err != nil {
		return err
	}
	task, attempt := packageRecords(pkg, d.Now().UTC())
	// Preflight logs are captured with the attempt's own outputs, in the same
	// pass, because the capture tree is sealed before it is published.
	extras, err := d.preflightFinalizationArtifacts(pkg)
	if err != nil {
		return fmt.Errorf("collect preflight custody: %w", err)
	}
	// A task that declares a commit reports the pin its workspace started
	// from. Its own HEAD has moved by now, so the pin is read back from the
	// record preparation left in the workspace.
	baseCommit, err := backlog.WorkspaceBaseCommit(workspace)
	if err != nil {
		return err
	}
	finalized, err := d.Finalizer.Finalize(ctx, backlog.AttemptFinalization{
		Task: task, Attempt: attempt, WorkspaceDir: workspace, ExplicitSuccess: failure == "",
		Extra: extras, Repository: pkg.Environment.Repository, BaseCommit: baseCommit,
	})
	if err != nil {
		return err
	}
	if err := d.Publisher.PublishResult(ctx, pkg, PublishedResult{
		Finalized: finalized, FinalMessage: message, ThreadArchive: archive,
	}); err != nil {
		return fmt.Errorf("publish result custody: %w", err)
	}
	if thread == nil {
		return nil
	}
	// Result custody is durable before this external effect. The dispatch token
	// was recorded with the assignment and derives a stable T3 command ID. A
	// settlement that cannot be proven yet is retried later; the result stands.
	if err := d.T3.SettleThread(ctx, pkg.Identity.ThreadID, pkg.Identity.DispatchToken); err != nil {
		return fmt.Errorf("%w: %v", ErrSettleUnproven, err)
	}
	return nil
}

// ErrSettleUnproven reports that the result was published but T3 has not yet
// projected the thread settlement. The runtime retries settlement on later
// reconcile passes without repeating collection.
var ErrSettleUnproven = errors.New("T3 settlement unproven")

// Settle retries the idempotent T3 settlement for a collected attempt.
func (d *LocalDriver) Settle(ctx context.Context, pkg workerproto.ExecutionPackage) error {
	if scoped, err := d.scopedDriver(ctx, pkg); err != nil {
		return err
	} else if scoped != nil {
		return scoped.Settle(ctx, pkg)
	}
	if d.Config.DryRun {
		return nil
	}
	thread, err := d.T3.GetThread(ctx, pkg.Identity.ThreadID)
	if err != nil {
		return err
	}
	if thread == nil || thread.SettledAt != nil {
		return nil
	}
	return d.T3.SettleThread(ctx, pkg.Identity.ThreadID, pkg.Identity.DispatchToken)
}

// CollectFailure publishes a terminal failed result for an attempt that has no
// collectable T3 outcome. The final message carries the failure marker and
// reason so the coordinator records why the attempt failed.
func (d *LocalDriver) CollectFailure(ctx context.Context, pkg workerproto.ExecutionPackage, workspace, failure string) error {
	if !d.scoped {
		if manager := d.containedManager(pkg); manager != nil {
			if err := manager.Quiesce(ctx, pkg, true); err != nil {
				return err
			}
		}
	}
	if scoped, err := d.scopedDriver(ctx, pkg); err != nil {
		return err
	} else if scoped != nil {
		return scoped.CollectFailure(ctx, pkg, workspace, failure)
	}
	if d.Config.DryRun {
		return nil
	}
	if strings.TrimSpace(failure) == "" {
		failure = "attempt failed on the worker"
	}
	message := FailedMarker + "\n" + failure + "\n"
	archive := []byte("{}")
	thread, err := d.T3.GetThread(ctx, pkg.Identity.ThreadID)
	if err == nil && thread != nil {
		if exported, exportErr := d.T3.ExportThread(ctx, pkg.Identity.ThreadID); exportErr == nil && len(exported) != 0 {
			archive = exported
		}
	}
	finalized := backlog.FinalizedAttempt{Completion: backlog.CompletionResult{Failure: failure}}
	if err := d.Publisher.PublishResult(ctx, pkg, PublishedResult{
		Finalized: finalized, FinalMessage: message, ThreadArchive: archive,
	}); err != nil {
		return fmt.Errorf("publish failed result custody: %w", err)
	}
	if err == nil && thread != nil {
		if settleErr := d.T3.SettleThread(ctx, pkg.Identity.ThreadID, pkg.Identity.DispatchToken); settleErr != nil {
			return fmt.Errorf("settle failed T3 thread: %w", settleErr)
		}
	}
	return nil
}

func (d *LocalDriver) Cleanup(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) error {
	if manager := d.containedManager(pkg); manager != nil {
		if err := manager.Quiesce(ctx, pkg, true); err != nil {
			return err
		}
	}
	if d.Config.RetainWorkspaces {
		return nil
	}
	root := d.workspacePath(pkg)
	expected := filepath.Join(root, "workspace")
	if filepath.Clean(workspace) != expected {
		return errors.New("refusing to clean workspace outside deterministic attempt root")
	}
	if err := stageAndRemoveTree(root); err != nil {
		return fmt.Errorf("remove workspace: %w", err)
	}
	resultRoot := filepath.Join(
		d.Config.ArtifactRoot, "runs", pkg.Identity.WorkflowRunID,
		pkg.Identity.TaskID, pkg.Identity.AttemptID,
	)
	if err := stageAndRemoveTree(resultRoot); err != nil {
		return fmt.Errorf("remove uploaded result staging: %w", err)
	}
	return nil
}

func (d *LocalDriver) Warn(ctx context.Context, pkg workerproto.ExecutionPackage, command domain.ThrottleCommand) error {
	if scoped, err := d.scopedDriver(ctx, pkg); err != nil {
		return err
	} else if scoped != nil {
		return scoped.Warn(ctx, pkg, command)
	}
	if d.Config.DryRun {
		return nil
	}
	thread, err := d.requiredThread(ctx, pkg.Identity.ThreadID)
	if err != nil {
		return err
	}
	return d.T3.WarnThread(ctx, thread, domain.Warning{Kind: domain.ActionWarn, Text: command.Reason})
}

func (d *LocalDriver) Checkpoint(ctx context.Context, pkg workerproto.ExecutionPackage, command domain.ThrottleCommand) (*domain.CheckpointMetadata, error) {
	if scoped, err := d.scopedDriver(ctx, pkg); err != nil {
		return nil, err
	} else if scoped != nil {
		return scoped.Checkpoint(ctx, pkg, command)
	}
	if d.Config.DryRun {
		data := []byte("no-external-effects checkpoint\n")
		return d.Publisher.PublishCheckpoint(ctx, pkg, ".t3/checkpoint.md", data)
	}
	thread, err := d.requiredThread(ctx, pkg.Identity.ThreadID)
	if err != nil {
		return nil, err
	}
	text := command.Reason + "\nWrite .t3/checkpoint.md, leave the workspace consistent, and end this turn."
	if err := d.T3.WarnThread(ctx, thread, domain.Warning{Kind: domain.ActionDrain, Text: text}); err != nil {
		return nil, err
	}
	_, stopped, err := d.T3.WaitStopped(ctx, pkg.Identity.ThreadID, d.Config.StopTimeout)
	if err != nil {
		return nil, err
	}
	if !stopped {
		return nil, errors.New("checkpoint turn did not stop before deadline")
	}
	path := filepath.Join(d.workspacePath(pkg), "workspace", ".t3", "checkpoint.md")
	data, err := readBoundedRegularFile(path, pkg.Limits.MaxArtifactBytes)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	return d.Publisher.PublishCheckpoint(ctx, pkg, ".t3/checkpoint.md", data)
}

func (d *LocalDriver) Resume(ctx context.Context, pkg workerproto.ExecutionPackage, command domain.ThrottleCommand) error {
	if scoped, err := d.scopedDriver(ctx, pkg); err != nil {
		return err
	} else if scoped != nil {
		return scoped.Resume(ctx, pkg, command)
	}
	if d.Config.DryRun {
		return os.WriteFile(d.noEffectsThreadPath(pkg), []byte("active\n"), 0o600)
	}
	thread, err := d.requiredThread(ctx, pkg.Identity.ThreadID)
	if err != nil {
		return err
	}
	prompt := "Continue the backlog task from the retained checkpoint. Reconcile the workspace first. Throttle recovery: " + command.Reason
	return d.T3.ResumeThread(ctx, thread, prompt)
}

func (d *LocalDriver) requiredThread(ctx context.Context, id string) (domain.Thread, error) {
	thread, err := d.T3.GetThread(ctx, id)
	if err != nil {
		return domain.Thread{}, err
	}
	if thread == nil {
		return domain.Thread{}, errors.New("deterministic T3 thread is missing")
	}
	return *thread, nil
}

// packageRecords rebuilds the task and attempt an execution package describes.
// It needs no catalog, so collection keeps working after configuration changes.
func packageRecords(pkg workerproto.ExecutionPackage, now time.Time) (domain.Task, domain.Attempt) {
	task := domain.Task{
		ID: pkg.Identity.TaskID, WorkflowID: pkg.Identity.WorkflowID, Name: pkg.Identity.TaskID,
		Class: pkg.Class, Outputs: pkg.Outputs, Verification: pkg.Verification,
		DirectoryBindings: pkg.Environment.DirectoryBindings,
		Routes:            []domain.ProviderRoute{pkg.Route}, ResourceLocks: append([]string(nil), pkg.Environment.ResourceLocks...),
		MaxTurns: pkg.Limits.MaxTurns, NotBefore: pkg.NotBefore, Deadline: pkg.Deadline, ExpiresAt: pkg.ExpiresAt,
	}
	attempt := domain.Attempt{
		ID: pkg.Identity.AttemptID, WorkflowRunID: pkg.Identity.WorkflowRunID,
		TaskID: pkg.Identity.TaskID, Number: 1, Progress: domain.ProgressReady,
		Control: domain.ControlPreparing, Revision: 1, AssignmentID: pkg.Identity.AssignmentID,
		ThreadID: pkg.Identity.ThreadID, UpdatedAt: now,
	}
	return task, attempt
}

func (d *LocalDriver) executionRecords(pkg workerproto.ExecutionPackage) (backlog.ResolvedEnvironment, domain.Task, domain.Attempt, error) {
	if len(pkg.Environment.DirectoryBindings) > 0 {
		manager := d.containedManager(pkg)
		if manager == nil || manager.Profile == nil || pkg.Environment.Type != backlog.EnvironmentFresh || pkg.Route.ProviderInstanceID != "opencode" {
			return backlog.ResolvedEnvironment{}, domain.Task{}, domain.Attempt{}, errors.New("directory execution requires an integrated provider containment backend")
		}
	}
	workspaceType := pkg.Environment.Type
	if workspaceType == "" {
		workspaceType = backlog.EnvironmentGit
	}
	workflow := domain.Workflow{
		ID: pkg.Identity.WorkflowID, Project: pkg.Environment.Project,
		Environment: domain.ExecutionEnvironment{Type: workspaceType, Scope: pkg.Environment.Scope, Ref: pkg.Environment.Ref},
	}
	task, attempt := packageRecords(pkg, d.Now().UTC())
	environment, err := d.Catalog.Resolve(workflow, task)
	if err != nil {
		return backlog.ResolvedEnvironment{}, domain.Task{}, domain.Attempt{}, err
	}
	if environment.Repository != pkg.Environment.Repository || environment.Ref != pkg.Environment.Ref ||
		environment.Scope != pkg.Environment.Scope || environment.T3ProjectTemplate != pkg.Environment.T3Project ||
		environment.Setup.Name != pkg.Environment.SetupProfile ||
		!slices.Equal(environment.ResourceLocks, pkg.Environment.ResourceLocks) ||
		!slices.Equal(environment.RequiredCredentials, pkg.Environment.RequiredCredentials) {
		return backlog.ResolvedEnvironment{}, domain.Task{}, domain.Attempt{}, errors.New("execution package does not match resolved catalog environment")
	}
	return environment, task, attempt, nil
}

func (d *LocalDriver) cacheArtifact(ctx context.Context, object workerproto.ArtifactObject, maxBytes int64) (string, error) {
	path := d.objectPath(object)
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("cached artifact %q is not a regular file", object.ID)
		}
		data, err := os.Open(path)
		if err != nil {
			return "", err
		}
		defer data.Close()
		if err := workerproto.VerifyArtifact(data, object, maxBytes); err != nil {
			return "", fmt.Errorf("cached artifact %q is corrupt: %w", object.ID, err)
		}
		return path, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	directory := filepath.Dir(path)
	if err := ensureRealDirectory(directory); err != nil {
		return "", err
	}
	source, err := d.Source.OpenArtifact(ctx, object)
	if err != nil {
		return "", fmt.Errorf("download artifact %q: %w", object.ID, err)
	}
	defer source.Close()
	stage, err := os.CreateTemp(directory, ".download-*")
	if err != nil {
		return "", err
	}
	stagePath := stage.Name()
	defer os.Remove(stagePath)
	if err := workerproto.VerifyArtifact(io.TeeReader(source, stage), object, maxBytes); err != nil {
		stage.Close()
		return "", fmt.Errorf("verify downloaded artifact %q: %w", object.ID, err)
	}
	if err := stage.Sync(); err != nil {
		stage.Close()
		return "", err
	}
	if err := stage.Chmod(0o400); err != nil {
		stage.Close()
		return "", err
	}
	if err := stage.Close(); err != nil {
		return "", err
	}
	if err := os.Link(stagePath, path); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return "", err
		}
		existing, openErr := os.Open(path)
		if openErr != nil {
			return "", openErr
		}
		defer existing.Close()
		if verifyErr := workerproto.VerifyArtifact(existing, object, maxBytes); verifyErr != nil {
			return "", fmt.Errorf("concurrent cached artifact %q is corrupt: %w", object.ID, verifyErr)
		}
	}
	return path, nil
}

func (d *LocalDriver) readCachedArtifact(object workerproto.ArtifactObject, maxBytes int64) ([]byte, error) {
	path := d.objectPath(object)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("cached artifact is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var data strings.Builder
	if err := workerproto.VerifyArtifact(io.TeeReader(file, &data), object, maxBytes); err != nil {
		return nil, err
	}
	return []byte(data.String()), nil
}

func (d *LocalDriver) domainArtifact(pkg workerproto.ExecutionPackage, object workerproto.ArtifactObject, kind domain.ArtifactKind, name, producer string) domain.Artifact {
	return domain.Artifact{
		ID: object.ID, WorkflowRunID: pkg.Identity.WorkflowRunID, TaskID: pkg.Identity.TaskID,
		AttemptID: pkg.Identity.AttemptID, Kind: kind, Name: filepath.ToSlash(name),
		MediaType: object.MediaType, Size: object.Size, SHA256: object.SHA256,
		StoragePath: filepath.ToSlash(filepath.Join("objects", strings.ToLower(object.SHA256))),
		Producer:    producer, CreatedAt: pkg.CreatedAt,
	}
}

func (d *LocalDriver) objectPath(object workerproto.ArtifactObject) string {
	return filepath.Join(d.Config.ArtifactRoot, "objects", strings.ToLower(object.SHA256))
}

func (d *LocalDriver) workspacePath(pkg workerproto.ExecutionPackage) string {
	attemptDir := pkg.Identity.AttemptID
	if pkg.Identity.AssignmentEpoch > 1 {
		attemptDir = fmt.Sprintf("%s-e%d", attemptDir, pkg.Identity.AssignmentEpoch)
	}
	return filepath.Join(d.Config.RunsRoot, pkg.Identity.WorkflowRunID, pkg.Identity.TaskID, attemptDir)
}

func (d *LocalDriver) noEffectsThreadPath(pkg workerproto.ExecutionPackage) string {
	return filepath.Join(d.workspacePath(pkg), ".no-effects-thread")
}

func ensureRealDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("path is not a real directory")
	}
	return nil
}

func safeLocalRoot(root string) (string, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if filepath.Clean(absolute) == string(filepath.Separator) {
		return "", errors.New("filesystem root is not allowed")
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return "", err
	}
	info, err := os.Lstat(absolute)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("root must be a real directory")
	}
	return absolute, nil
}

func stageAndRemoveTree(root string) error {
	stage := root + ".cleanup"
	if info, err := os.Lstat(stage); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("cleanup staging path is not a real directory")
		}
		if err := removeReadOnlyTree(stage); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	info, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("cleanup target is not a real directory")
	}
	if err := os.Rename(root, stage); err != nil {
		return err
	}
	return removeReadOnlyTree(stage)
}

func removeReadOnlyTree(root string) error {
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if entry.IsDir() {
			return os.Chmod(path, 0o700)
		}
		return os.Chmod(path, 0o600)
	}); err != nil {
		return err
	}
	return os.RemoveAll(root)
}

func readBoundedRegularFile(path string, maxBytes int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > maxBytes {
		return nil, errors.New("file is not a bounded regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(io.LimitReader(file, maxBytes+1))
}

// FailedMarker is the final-message line a worker writes for a terminal
// failure it produced itself. The coordinator records the following lines as
// the failure reason.
const FailedMarker = "BACKLOG STATUS: failed"

func checkpointMetadata(path string, data []byte, now time.Time) domain.CheckpointMetadata {
	sum := sha256.Sum256(data)
	return domain.CheckpointMetadata{
		Path: path, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(data)), CapturedAt: now.UTC(),
	}
}
