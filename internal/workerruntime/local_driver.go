package workerruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	GetThread(context.Context, string) (*domain.Thread, error)
	ResolveProjectID(context.Context, string) (string, error)
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
	CatalogRevision  string
	ArtifactRoot     string
	RunsRoot         string
	StopTimeout      time.Duration
	RetainWorkspaces bool
	DryRun           bool
}

type LocalDriver struct {
	Config      LocalDriverConfig
	Catalog     *backlog.ProjectCatalog
	Workspace   backlog.WorkspacePreparer
	Finalizer   backlog.AttemptFinalizer
	Source      ArtifactSource
	Publisher   ArtifactPublisher
	Credentials CredentialChecker
	T3          T3Control
	Now         func() time.Time
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
	if len(environment.RequiredCredentials) > 0 {
		if d.Credentials == nil {
			return "", errors.New("prepare workspace: required credential resolver is unavailable")
		}
		if err := d.Credentials.Require(ctx, environment.RequiredCredentials); err != nil {
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
		inputs = append(inputs, d.domainArtifact(pkg, object, domain.ArtifactInput, object.Path, "submission"))
	}
	for _, dependency := range pkg.Dependencies {
		dependencyTask := domain.Task{ID: dependency.TaskID, WorkflowID: pkg.Identity.WorkflowID, Name: dependency.TaskID}
		names := make([]string, 0, len(dependency.Artifacts))
		for _, object := range dependency.Artifacts {
			name := filepath.Base(filepath.FromSlash(object.Path))
			names = append(names, name)
			dependencyArtifacts = append(dependencyArtifacts, d.domainArtifact(pkg, object, domain.ArtifactOutput, name, dependency.TaskID))
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
		return filepath.Join(path, "workspace"), nil
	}
	prepared, err := d.Workspace.Prepare(ctx, backlog.WorkspacePreparation{
		WorkflowRunID: pkg.Identity.WorkflowRunID, Task: task, Attempt: attempt,
		Environment: environment, InputArtifacts: inputs, DependencyTasks: dependencyTasks,
		DependencyArtifacts: dependencyArtifacts,
	})
	if err != nil {
		return "", err
	}
	return prepared.WorkspaceDir, nil
}

func (d *LocalDriver) InspectWorkspace(_ context.Context, pkg workerproto.ExecutionPackage) (string, bool, error) {
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
	return workspace, true, nil
}

func (d *LocalDriver) ObserveThread(ctx context.Context, pkg workerproto.ExecutionPackage) (backlog.DispatchThreadState, error) {
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
	if thread.Running {
		return backlog.DispatchThreadActive, nil
	}
	return backlog.DispatchThreadStopped, nil
}

func (d *LocalDriver) CreateThread(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) error {
	if d.Config.DryRun {
		return os.WriteFile(d.noEffectsThreadPath(pkg), []byte("active\n"), 0o600)
	}
	prompt, err := d.readCachedArtifact(pkg.Prompt, pkg.Limits.MaxArtifactBytes)
	if err != nil {
		return err
	}
	projectID, err := d.T3.ResolveProjectID(ctx, pkg.Environment.T3Project)
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
		WorktreePath: workspace, Prompt: string(prompt),
	})
	if threadID != "" && threadID != pkg.Identity.ThreadID {
		return errors.New("T3 returned a different deterministic thread identity")
	}
	return err
}

func (d *LocalDriver) StopThread(ctx context.Context, pkg workerproto.ExecutionPackage) error {
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
	if thread.Running {
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
	if d.Config.DryRun {
		return nil
	}
	message, err := d.T3.LastAssistantMessage(ctx, pkg.Identity.ThreadID)
	if err != nil {
		return fmt.Errorf("collect final message: %w", err)
	}
	archive, err := d.T3.ExportThread(ctx, pkg.Identity.ThreadID)
	if err != nil {
		return fmt.Errorf("collect thread archive: %w", err)
	}
	_, task, attempt, err := d.executionRecords(pkg)
	if err != nil {
		return err
	}
	finalized, err := d.Finalizer.Finalize(ctx, backlog.AttemptFinalization{
		Task: task, Attempt: attempt, WorkspaceDir: workspace, ExplicitSuccess: hasDoneMarker(message),
	})
	if err != nil {
		return err
	}
	if err := d.Publisher.PublishResult(ctx, pkg, PublishedResult{
		Finalized: finalized, FinalMessage: message, ThreadArchive: archive,
	}); err != nil {
		return fmt.Errorf("publish result custody: %w", err)
	}
	// Result custody is durable before this external effect. The dispatch token
	// was recorded with the assignment and derives a stable T3 command ID.
	if err := d.T3.SettleThread(ctx, pkg.Identity.ThreadID, pkg.Identity.DispatchToken); err != nil {
		return fmt.Errorf("settle completed T3 thread: %w", err)
	}
	return nil
}

func (d *LocalDriver) Cleanup(_ context.Context, pkg workerproto.ExecutionPackage, workspace string) error {
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

func (d *LocalDriver) executionRecords(pkg workerproto.ExecutionPackage) (backlog.ResolvedEnvironment, domain.Task, domain.Attempt, error) {
	if pkg.Environment.CatalogRevision != d.Config.CatalogRevision {
		return backlog.ResolvedEnvironment{}, domain.Task{}, domain.Attempt{}, errors.New("execution package catalog revision is stale")
	}
	workflow := domain.Workflow{
		ID: pkg.Identity.WorkflowID, Project: pkg.Environment.Project,
		Environment: domain.ExecutionEnvironment{Type: backlog.EnvironmentGit, Scope: pkg.Environment.Scope, Ref: pkg.Environment.Ref},
	}
	task := domain.Task{
		ID: pkg.Identity.TaskID, WorkflowID: pkg.Identity.WorkflowID, Name: pkg.Identity.TaskID,
		Class: pkg.Class, Outputs: pkg.Outputs, Verification: pkg.Verification,
		Routes: []domain.ProviderRoute{pkg.Route}, ResourceLocks: append([]string(nil), pkg.Environment.ResourceLocks...),
		MaxTurns: pkg.Limits.MaxTurns, NotBefore: pkg.NotBefore, Deadline: pkg.Deadline, ExpiresAt: pkg.ExpiresAt,
	}
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
	attempt := domain.Attempt{
		ID: pkg.Identity.AttemptID, WorkflowRunID: pkg.Identity.WorkflowRunID,
		TaskID: pkg.Identity.TaskID, Number: 1, Progress: domain.ProgressReady,
		Control: domain.ControlPreparing, Revision: 1, AssignmentID: pkg.Identity.AssignmentID,
		ThreadID: pkg.Identity.ThreadID, UpdatedAt: d.Now().UTC(),
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
	return filepath.Join(d.Config.RunsRoot, pkg.Identity.WorkflowRunID, pkg.Identity.TaskID, pkg.Identity.AttemptID)
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

func hasDoneMarker(message string) bool {
	for _, line := range strings.Split(message, "\n") {
		if strings.EqualFold(strings.TrimSpace(line), "BACKLOG STATUS: done") {
			return true
		}
	}
	return false
}

func checkpointMetadata(path string, data []byte, now time.Time) domain.CheckpointMetadata {
	sum := sha256.Sum256(data)
	return domain.CheckpointMetadata{
		Path: path, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(data)), CapturedAt: now.UTC(),
	}
}
