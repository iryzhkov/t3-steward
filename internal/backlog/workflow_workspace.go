package backlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const workflowWorkspaceDirectory = "workflow"

type WorkflowWorkspaceRetention string

const (
	WorkflowWorkspaceRetain WorkflowWorkspaceRetention = "retain"
	WorkflowWorkspaceRemove WorkflowWorkspaceRetention = "remove"
)

type workflowWorkspaceMetadata struct {
	Repository       string   `json:"repository"`
	Ref              string   `json:"ref"`
	Commit           string   `json:"commit"`
	SetupName        string   `json:"setup_name"`
	SetupCommands    []string `json:"setup_commands"`
	SetupTimeout     int64    `json:"setup_timeout_ns"`
	InputFingerprint []string `json:"input_fingerprint"`
}

// WorkflowWorkspaceManager reserves and prepares one mutable checkout per workflow run.
// Its mutex protects filesystem publication; EnvironmentCoordinator separately owns
// worker placement, checkout mutation, and declared resource locks.
type WorkflowWorkspaceManager struct {
	Preparer     WorkspacePreparer
	Environments *EnvironmentCoordinator

	mu sync.Mutex
}

func (m *WorkflowWorkspaceManager) Prepare(ctx context.Context, workerID string, request WorkspacePreparation) (prepared PreparedWorkspace, err error) {
	if m == nil || m.Environments == nil {
		return PreparedWorkspace{}, &PreparationError{Err: errors.New("workflow workspace manager and environment coordinator are required")}
	}
	if request.Environment.Scope != EnvironmentScopeWorkflow {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("environment scope %q is not workflow-scoped", request.Environment.Scope)}
	}
	validation := request
	validation.Environment.Scope = EnvironmentScopeTask
	if validateErr := m.Preparer.validate(validation); validateErr != nil {
		return PreparedWorkspace{}, &PreparationError{Err: validateErr}
	}

	reservation, err := m.Environments.Reserve(EnvironmentReservationRequest{
		WorkflowRunID: request.WorkflowRunID,
		TaskID:        request.Task.ID,
		AttemptID:     request.Attempt.ID,
		WorkerID:      workerID,
		Scope:         EnvironmentScopeWorkflow,
		ResourceLocks: request.Environment.ResourceLocks,
	})
	if err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: err}
	}
	succeeded := false
	defer func() {
		if !succeeded {
			_ = m.Environments.Release(reservation.AttemptID, EnvironmentReleaseTerminal)
		}
	}()

	m.mu.Lock()
	defer m.mu.Unlock()

	rootDir := m.workflowRoot(request.WorkflowRunID)
	if _, statErr := os.Lstat(rootDir); errors.Is(statErr, os.ErrNotExist) {
		prepared, err = m.prepareInitial(ctx, request, rootDir)
	} else if statErr != nil {
		err = &PreparationError{Err: fmt.Errorf("inspect workflow workspace: %w", statErr)}
	} else {
		prepared, err = m.prepareExisting(request, rootDir)
	}
	if err != nil {
		return PreparedWorkspace{}, err
	}
	succeeded = true
	return prepared, nil
}

func (m *WorkflowWorkspaceManager) Release(attemptID string, policy EnvironmentReleasePolicy) error {
	if m == nil || m.Environments == nil {
		return errors.New("workflow workspace manager and environment coordinator are required")
	}
	return m.Environments.Release(attemptID, policy)
}

func (m *WorkflowWorkspaceManager) CleanupWorkflow(workflowRunID string, retention WorkflowWorkspaceRetention) error {
	if m == nil || m.Environments == nil {
		return errors.New("workflow workspace manager and environment coordinator are required")
	}
	if retention != WorkflowWorkspaceRetain && retention != WorkflowWorkspaceRemove {
		return fmt.Errorf("invalid workflow workspace retention %q", retention)
	}
	if !safePathComponent(workflowRunID) {
		return fmt.Errorf("workflow run ID %q is not a safe path component", workflowRunID)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Environments.CleanupWorkflow(workflowRunID); err != nil {
		return err
	}
	if retention == WorkflowWorkspaceRemove {
		if err := removeIngestedTree(m.workflowRoot(workflowRunID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove workflow workspace: %w", err)
		}
	}
	return nil
}

func (m *WorkflowWorkspaceManager) workflowRoot(workflowRunID string) string {
	return filepath.Join(m.Preparer.RunsRoot, workflowRunID, workflowWorkspaceDirectory)
}

func (m *WorkflowWorkspaceManager) prepareInitial(ctx context.Context, request WorkspacePreparation, finalDir string) (PreparedWorkspace, error) {
	if err := os.MkdirAll(m.Preparer.RunsRoot, 0o755); err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("create runs root: %w", err)}
	}
	stageRoot, err := os.MkdirTemp(m.Preparer.RunsRoot, ".workflow-prepare-")
	if err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("create workflow preparation stage: %w", err)}
	}
	defer removeIngestedTree(stageRoot)

	initial := request
	initial.Environment.Scope = EnvironmentScopeTask
	preparer := m.Preparer
	preparer.RunsRoot = stageRoot
	prepared, err := preparer.Prepare(ctx, initial)
	if err != nil {
		return PreparedWorkspace{}, m.retainInitialFailure(request, err)
	}

	viewDir := workflowAttemptView(prepared.RootDir, request.Task.ID, request.Attempt.ID)
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("create initial task view: %w", err)}
	}
	viewDependencies := filepath.Join(viewDir, "dependencies")
	if err := os.Chmod(prepared.DependenciesDir, 0o755); err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("make initial dependency view movable: %w", err)}
	}
	if err := os.Rename(prepared.DependenciesDir, viewDependencies); err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("move initial dependency view: %w", err)}
	}
	if err := makeIngestedTreeImmutable(viewDependencies); err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("protect initial dependency view: %w", err)}
	}
	if err := switchDependencyView(prepared.WorkspaceDir, viewDependencies); err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: err}
	}
	metadata := expectedWorkflowMetadata(request, prepared.Commit)
	if err := writeWorkflowMetadata(prepared.RootDir, metadata); err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: err}
	}

	if err := os.MkdirAll(filepath.Dir(finalDir), 0o755); err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("create workflow run directory: %w", err)}
	}
	if err := os.Rename(prepared.RootDir, finalDir); err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("publish workflow workspace: %w", err)}
	}
	return preparedWorkflow(finalDir, request, metadata.Commit, prepared.CacheReused), nil
}

func (m *WorkflowWorkspaceManager) prepareExisting(request WorkspacePreparation, rootDir string) (PreparedWorkspace, error) {
	metadata, err := readWorkflowMetadata(rootDir)
	if err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: err}
	}
	expected := expectedWorkflowMetadata(request, metadata.Commit)
	if !sameWorkflowMetadata(metadata, expected) {
		return PreparedWorkspace{}, &PreparationError{Err: errors.New("workflow workspace preparation contract changed")}
	}

	viewDir := workflowAttemptView(rootDir, request.Task.ID, request.Attempt.ID)
	dependenciesDir := filepath.Join(viewDir, "dependencies")
	if info, statErr := os.Stat(dependenciesDir); statErr == nil && info.IsDir() {
		if err := switchDependencyView(filepath.Join(rootDir, "workspace"), dependenciesDir); err != nil {
			return PreparedWorkspace{}, &PreparationError{Err: err}
		}
		return preparedWorkflow(rootDir, request, metadata.Commit, true), nil
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("inspect task dependency view: %w", statErr)}
	}

	stageDir, err := os.MkdirTemp(rootDir, ".dependencies-")
	if err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("stage task dependency view: %w", err)}
	}
	defer removeIngestedTree(stageDir)
	if err := m.Preparer.materializeDependencyView(stageDir, request); err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: err}
	}
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("create task view: %w", err)}
	}
	stagedDependencies := filepath.Join(stageDir, "dependencies")
	if err := os.Chmod(stagedDependencies, 0o755); err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("make task dependency view movable: %w", err)}
	}
	if err := os.Rename(stagedDependencies, dependenciesDir); err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("publish task dependency view: %w", err)}
	}
	if err := makeIngestedTreeImmutable(dependenciesDir); err != nil {
		return PreparedWorkspace{}, &PreparationError{Err: fmt.Errorf("protect task dependency view: %w", err)}
	}
	if err := switchDependencyView(filepath.Join(rootDir, "workspace"), dependenciesDir); err != nil {
		_ = removeIngestedTree(viewDir)
		return PreparedWorkspace{}, &PreparationError{Err: err}
	}
	return preparedWorkflow(rootDir, request, metadata.Commit, true), nil
}

func (m *WorkflowWorkspaceManager) retainInitialFailure(request WorkspacePreparation, cause error) error {
	var preparationErr *PreparationError
	if !errors.As(cause, &preparationErr) || preparationErr.LogPath == "" {
		return cause
	}
	parent := filepath.Join(m.Preparer.RunsRoot, request.WorkflowRunID)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return &PreparationError{Err: fmt.Errorf("%w (create retained log directory: %v)", preparationErr.Err, err)}
	}
	retained := filepath.Join(parent, request.Attempt.ID+".preparation.log")
	if err := copyFileExclusive(preparationErr.LogPath, retained, 0o444); err != nil {
		return &PreparationError{Err: fmt.Errorf("%w (retain workflow preparation log: %v)", preparationErr.Err, err)}
	}
	return &PreparationError{Err: preparationErr.Err, LogPath: retained}
}

func workflowAttemptView(rootDir, taskID, attemptID string) string {
	return filepath.Join(rootDir, "tasks", taskID, attemptID)
}

func preparedWorkflow(rootDir string, request WorkspacePreparation, commit string, cacheReused bool) PreparedWorkspace {
	return PreparedWorkspace{
		RootDir:         rootDir,
		WorkspaceDir:    filepath.Join(rootDir, "workspace"),
		InputsDir:       filepath.Join(rootDir, "inputs"),
		DependenciesDir: filepath.Join(workflowAttemptView(rootDir, request.Task.ID, request.Attempt.ID), "dependencies"),
		PreparationLog:  filepath.Join(rootDir, "preparation.log"),
		Commit:          commit,
		CacheReused:     cacheReused,
	}
}

func switchDependencyView(workspaceDir, dependenciesDir string) error {
	metadataDir := filepath.Join(workspaceDir, ".t3")
	target, err := filepath.Rel(metadataDir, dependenciesDir)
	if err != nil {
		return fmt.Errorf("resolve dependency view link: %w", err)
	}
	link := filepath.Join(metadataDir, "dependencies")
	staged := link + ".next"
	if err := os.Remove(staged); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale dependency link: %w", err)
	}
	if err := os.Symlink(filepath.ToSlash(target), staged); err != nil {
		return fmt.Errorf("stage dependency view link: %w", err)
	}
	if err := os.Rename(staged, link); err != nil {
		_ = os.Remove(staged)
		return fmt.Errorf("activate dependency view link: %w", err)
	}
	return nil
}

func expectedWorkflowMetadata(request WorkspacePreparation, commit string) workflowWorkspaceMetadata {
	fingerprint := make([]string, 0, len(request.InputArtifacts))
	for _, artifact := range request.InputArtifacts {
		fingerprint = append(fingerprint, fmt.Sprintf("%s\x00%s\x00%d\x00%s\x00%s", artifact.ID, artifact.Name, artifact.Size, artifact.SHA256, artifact.StoragePath))
	}
	sort.Strings(fingerprint)
	return workflowWorkspaceMetadata{
		Repository: request.Environment.Repository, Ref: request.Environment.Ref, Commit: commit,
		SetupName: request.Environment.Setup.Name, SetupCommands: append([]string(nil), request.Environment.Setup.Commands...),
		SetupTimeout: int64(request.Environment.Setup.Timeout), InputFingerprint: fingerprint,
	}
}

func sameWorkflowMetadata(left, right workflowWorkspaceMetadata) bool {
	if left.Repository != right.Repository || left.Ref != right.Ref || left.Commit != right.Commit ||
		left.SetupName != right.SetupName || left.SetupTimeout != right.SetupTimeout ||
		len(left.SetupCommands) != len(right.SetupCommands) || len(left.InputFingerprint) != len(right.InputFingerprint) {
		return false
	}
	for index := range left.SetupCommands {
		if left.SetupCommands[index] != right.SetupCommands[index] {
			return false
		}
	}
	for index := range left.InputFingerprint {
		if left.InputFingerprint[index] != right.InputFingerprint[index] {
			return false
		}
	}
	return true
}

func writeWorkflowMetadata(rootDir string, metadata workflowWorkspaceMetadata) error {
	raw, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("encode workflow workspace metadata: %w", err)
	}
	path := filepath.Join(rootDir, "environment.json")
	if err := os.WriteFile(path, append(raw, '\n'), 0o444); err != nil {
		return fmt.Errorf("write workflow workspace metadata: %w", err)
	}
	return nil
}

func readWorkflowMetadata(rootDir string) (workflowWorkspaceMetadata, error) {
	raw, err := os.ReadFile(filepath.Join(rootDir, "environment.json"))
	if err != nil {
		return workflowWorkspaceMetadata{}, fmt.Errorf("read workflow workspace metadata: %w", err)
	}
	var metadata workflowWorkspaceMetadata
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return workflowWorkspaceMetadata{}, fmt.Errorf("decode workflow workspace metadata: %w", err)
	}
	if !validGitObjectID(metadata.Commit) {
		return workflowWorkspaceMetadata{}, errors.New("workflow workspace metadata has invalid commit")
	}
	return metadata, nil
}
