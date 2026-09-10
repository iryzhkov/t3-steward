package backlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// AttemptFinalization describes a completed agent turn whose declared outputs and
// verification commands must be reconciled before DAG completion.
type AttemptFinalization struct {
	Task            domain.Task
	Attempt         domain.Attempt
	WorkspaceDir    string
	ExplicitSuccess bool
}

// VerificationReport is the immutable result of one declared verification command.
type VerificationReport struct {
	Command     string    `json:"command"`
	ExitCode    int       `json:"exitCode"`
	Output      string    `json:"output"`
	StartedAt   time.Time `json:"startedAt"`
	CompletedAt time.Time `json:"completedAt"`
}

// FinalizedAttempt contains artifacts ready for coordinator persistence and the
// strict completion result to apply to the DAG transition engine.
type FinalizedAttempt struct {
	Completion CompletionResult
	Artifacts  []domain.Artifact
	StorageDir string
}

// AttemptFinalizer captures declared outputs and verification reports in
// coordinator-owned storage. It does not persist metadata or mutate DAG state.
type AttemptFinalizer struct {
	StorageRoot string
	Now         func() time.Time
	NewID       func(kind string) string
}

// Finalize runs verification, captures immutable artifacts, and returns a strict
// completion result. Declared-output or command failures are task failures, while
// unsafe paths and storage failures are operational errors.
func (f AttemptFinalizer) Finalize(ctx context.Context, request AttemptFinalization) (FinalizedAttempt, error) {
	if err := f.validateRequest(request); err != nil {
		return FinalizedAttempt{}, fmt.Errorf("finalize attempt: %w", err)
	}
	result := FinalizedAttempt{Completion: CompletionResult{ExplicitSuccess: request.ExplicitSuccess}}
	if !request.ExplicitSuccess {
		result.Completion.Failure = "missing explicit success"
		return result, nil
	}

	reports := make([]VerificationReport, 0, len(request.Task.Verification))
	failures := make([]string, 0)
	for _, command := range request.Task.Verification {
		report, err := f.runVerification(ctx, request.WorkspaceDir, command)
		if err != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt verification %q: %w", command, err)
		}
		reports = append(reports, report)
		if report.ExitCode != 0 {
			failures = append(failures, fmt.Sprintf("verification command failed (%d): %s", report.ExitCode, command))
			break
		}
	}

	workspaceRoot, err := os.OpenRoot(request.WorkspaceDir)
	if err != nil {
		return FinalizedAttempt{}, fmt.Errorf("finalize attempt: open workspace: %w", err)
	}
	defer workspaceRoot.Close()

	type outputSource struct {
		declaration domain.ArtifactDeclaration
		relative    string
	}
	outputs := make([]outputSource, 0, len(request.Task.Outputs))
	for _, declaration := range request.Task.Outputs {
		resolved, resolveErr := safeBundleFile(request.WorkspaceDir, declaration.Name)
		if resolveErr != nil {
			if errors.Is(resolveErr, os.ErrNotExist) {
				failures = append(failures, "missing declared output: "+declaration.Name)
				continue
			}
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt output %q: %w", declaration.Name, resolveErr)
		}
		relative, relativeErr := filepath.Rel(request.WorkspaceDir, resolved)
		if relativeErr != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt output %q: %w", declaration.Name, relativeErr)
		}
		outputs = append(outputs, outputSource{declaration: declaration, relative: relative})
	}

	runRoot := filepath.Join(f.StorageRoot, "runs", request.Attempt.WorkflowRunID, request.Task.ID)
	if err := os.MkdirAll(runRoot, 0o755); err != nil {
		return FinalizedAttempt{}, fmt.Errorf("finalize attempt: create storage: %w", err)
	}
	stageDir, err := os.MkdirTemp(runRoot, ".finalize-")
	if err != nil {
		return FinalizedAttempt{}, fmt.Errorf("finalize attempt: create staging directory: %w", err)
	}
	keepStage := false
	defer func() {
		if !keepStage {
			removeIngestedTree(stageDir)
		}
	}()

	now := f.now()
	artifacts := make([]domain.Artifact, 0, len(outputs)+len(reports))
	for _, output := range outputs {
		artifactName := filepath.ToSlash(output.declaration.Name)
		storagePath := filepath.ToSlash(filepath.Join(
			"runs", request.Attempt.WorkflowRunID, request.Task.ID, request.Attempt.ID,
			"artifacts", "outputs", output.declaration.Name,
		))
		file, copyErr := copyIngestedFile(
			workspaceRoot,
			output.relative,
			filepath.Join(stageDir, "artifacts", "outputs", output.declaration.Name),
			output.declaration.Name,
			storagePath,
		)
		if copyErr != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt output %q: %w", output.declaration.Name, copyErr)
		}
		media := output.declaration.MediaType
		if media == "" {
			media = mediaType(output.declaration.Name)
		}
		artifacts = append(artifacts, domain.Artifact{
			ID: f.newID("artifact"), WorkflowRunID: request.Attempt.WorkflowRunID,
			TaskID: request.Task.ID, AttemptID: request.Attempt.ID,
			Kind: domain.ArtifactOutput, Name: artifactName, MediaType: media,
			Size: file.size, SHA256: file.sha256, StoragePath: file.storagePath,
			Producer: request.Task.Name, CreatedAt: now,
		})
	}
	for index, report := range reports {
		name := fmt.Sprintf("verification/%03d.json", index+1)
		raw, marshalErr := json.MarshalIndent(report, "", "  ")
		if marshalErr != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt report %d: %w", index+1, marshalErr)
		}
		raw = append(raw, '\n')
		storagePath := filepath.ToSlash(filepath.Join(
			"runs", request.Attempt.WorkflowRunID, request.Task.ID, request.Attempt.ID,
			"artifacts", name,
		))
		file, writeErr := writeIngestedFile(
			bytes.NewReader(raw),
			filepath.Join(stageDir, "artifacts", filepath.FromSlash(name)),
			name,
			storagePath,
		)
		if writeErr != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt report %d: %w", index+1, writeErr)
		}
		artifacts = append(artifacts, domain.Artifact{
			ID: f.newID("artifact"), WorkflowRunID: request.Attempt.WorkflowRunID,
			TaskID: request.Task.ID, AttemptID: request.Attempt.ID,
			Kind: domain.ArtifactVerification, Name: name, MediaType: "application/json",
			Size: file.size, SHA256: file.sha256, StoragePath: file.storagePath,
			Producer: "verification", CreatedAt: now,
		})
	}

	finalDir := filepath.Join(runRoot, request.Attempt.ID)
	if len(artifacts) != 0 {
		if err := makeIngestedTreeImmutable(stageDir); err != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt: protect staged artifacts: %w", err)
		}
		if err := os.Rename(stageDir, finalDir); err != nil {
			return FinalizedAttempt{}, fmt.Errorf("finalize attempt: publish artifacts: %w", err)
		}
		keepStage = true
		result.StorageDir = finalDir
	}
	result.Artifacts = artifacts
	result.Completion.VerificationPassed = len(failures) == 0
	if len(failures) != 0 {
		result.Completion.Failure = strings.Join(failures, "; ")
	}
	return result, nil
}

func (f AttemptFinalizer) validateRequest(request AttemptFinalization) error {
	if f.StorageRoot == "" {
		return errors.New("storage root is required")
	}
	if request.WorkspaceDir == "" {
		return errors.New("workspace directory is required")
	}
	if request.Task.ID == "" || request.Task.Name == "" {
		return errors.New("task ID and name are required")
	}
	if request.Attempt.ID == "" || request.Attempt.WorkflowRunID == "" {
		return errors.New("attempt ID and workflow run ID are required")
	}
	if request.Attempt.TaskID != request.Task.ID {
		return fmt.Errorf("attempt task %q does not match task %q", request.Attempt.TaskID, request.Task.ID)
	}
	for label, value := range map[string]string{
		"workflow run ID": request.Attempt.WorkflowRunID,
		"task ID":         request.Task.ID,
		"attempt ID":      request.Attempt.ID,
	} {
		if !safePathComponent(value) {
			return fmt.Errorf("%s %q is not a safe storage component", label, value)
		}
	}
	for _, output := range request.Task.Outputs {
		if err := validateRelativePath(output.Name, false); err != nil {
			return fmt.Errorf("output %q: %w", output.Name, err)
		}
	}
	for _, command := range request.Task.Verification {
		if strings.TrimSpace(command) == "" {
			return errors.New("verification command must not be empty")
		}
	}
	return nil
}

func (f AttemptFinalizer) runVerification(ctx context.Context, workspace, command string) (VerificationReport, error) {
	started := f.now()
	process := exec.CommandContext(ctx, "/bin/sh", "-c", command)
	process.Dir = workspace
	output, err := process.CombinedOutput()
	completed := f.now()
	report := VerificationReport{
		Command: command, ExitCode: 0, Output: string(output),
		StartedAt: started, CompletedAt: completed,
	}
	if err == nil {
		return report, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return VerificationReport{}, ctxErr
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		report.ExitCode = exitError.ExitCode()
		return report, nil
	}
	return VerificationReport{}, err
}

func (f AttemptFinalizer) now() time.Time {
	if f.Now != nil {
		return f.Now().UTC()
	}
	return time.Now().UTC()
}

func (f AttemptFinalizer) newID(kind string) string {
	if f.NewID != nil {
		return f.NewID(kind)
	}
	return kind + "-" + uuid.NewString()
}

// MaterializeDependencies copies the selected immutable output artifacts into
// .t3/dependencies/<producer>/ and verifies their size and checksum before publish.
func MaterializeDependencies(workspaceDir, storageRoot, workflowRunID string, task domain.Task, tasks []domain.Task, artifacts []domain.Artifact) ([]string, error) {
	if len(task.DependencyInputs) == 0 {
		return nil, nil
	}
	if workspaceDir == "" || storageRoot == "" {
		return nil, errors.New("materialize dependencies: workspace and storage roots are required")
	}
	if !safePathComponent(workflowRunID) {
		return nil, fmt.Errorf("materialize dependencies: workflow run ID %q is not a safe storage component", workflowRunID)
	}
	taskByName := make(map[string]domain.Task, len(tasks))
	for _, candidate := range tasks {
		if _, exists := taskByName[candidate.Name]; exists {
			return nil, fmt.Errorf("materialize dependencies: duplicate task name %q", candidate.Name)
		}
		taskByName[candidate.Name] = candidate
	}
	type artifactKey struct {
		taskID string
		name   string
	}
	artifactByKey := make(map[artifactKey]domain.Artifact, len(artifacts))
	for _, artifact := range artifacts {
		if artifact.Kind != domain.ArtifactOutput {
			continue
		}
		key := artifactKey{taskID: artifact.TaskID, name: artifact.Name}
		if _, exists := artifactByKey[key]; exists {
			return nil, fmt.Errorf("materialize dependencies: duplicate output %q for task %q", artifact.Name, artifact.TaskID)
		}
		artifactByKey[key] = artifact
	}

	type selectedArtifact struct {
		producer string
		artifact domain.Artifact
		source   string
	}
	var selected []selectedArtifact
	directDependencies := make(map[string]struct{}, len(task.Needs))
	for _, dependency := range task.Needs {
		directDependencies[dependency] = struct{}{}
	}
	producers := make([]string, 0, len(task.DependencyInputs))
	for producer := range task.DependencyInputs {
		if _, direct := directDependencies[producer]; !direct {
			return nil, fmt.Errorf("materialize dependencies: producer %q is not a direct dependency", producer)
		}
		producers = append(producers, producer)
	}
	sort.Strings(producers)
	for _, producer := range producers {
		producerTask, exists := taskByName[producer]
		if !exists {
			return nil, fmt.Errorf("materialize dependencies: missing producer task %q", producer)
		}
		if !safePathComponent(producer) {
			return nil, fmt.Errorf("materialize dependencies: producer %q is not a safe path component", producer)
		}
		names := append([]string(nil), task.DependencyInputs[producer]...)
		sort.Strings(names)
		for _, name := range names {
			if err := validateRelativePath(name, false); err != nil {
				return nil, fmt.Errorf("materialize dependencies: artifact %q: %w", name, err)
			}
			artifact, exists := artifactByKey[artifactKey{taskID: producerTask.ID, name: filepath.ToSlash(name)}]
			if !exists {
				return nil, fmt.Errorf("materialize dependencies: missing output %q from %q", name, producer)
			}
			if artifact.WorkflowRunID != workflowRunID {
				return nil, fmt.Errorf("materialize dependencies: output %q from %q belongs to run %q, want %q", name, producer, artifact.WorkflowRunID, workflowRunID)
			}
			resolved, err := safeBundleFile(storageRoot, filepath.FromSlash(artifact.StoragePath))
			if err != nil {
				return nil, fmt.Errorf("materialize dependencies: open output %q from %q: %w", name, producer, err)
			}
			relative, err := filepath.Rel(storageRoot, resolved)
			if err != nil {
				return nil, fmt.Errorf("materialize dependencies: output %q from %q: %w", name, producer, err)
			}
			selected = append(selected, selectedArtifact{producer: producer, artifact: artifact, source: relative})
		}
	}

	storage, err := os.OpenRoot(storageRoot)
	if err != nil {
		return nil, fmt.Errorf("materialize dependencies: open storage: %w", err)
	}
	defer storage.Close()
	t3Dir := filepath.Join(workspaceDir, ".t3")
	if info, statErr := os.Lstat(t3Dir); statErr == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return nil, errors.New("materialize dependencies: .t3 is not a real directory")
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("materialize dependencies: inspect task metadata directory: %w", statErr)
	} else if err := os.Mkdir(t3Dir, 0o755); err != nil {
		return nil, fmt.Errorf("materialize dependencies: create task metadata directory: %w", err)
	}
	stageDir, err := os.MkdirTemp(t3Dir, ".dependencies-")
	if err != nil {
		return nil, fmt.Errorf("materialize dependencies: create staging directory: %w", err)
	}
	keepStage := false
	defer func() {
		if !keepStage {
			removeIngestedTree(stageDir)
		}
	}()

	materialized := make([]string, 0, len(selected))
	for _, item := range selected {
		destination := filepath.Join(stageDir, item.producer, filepath.FromSlash(item.artifact.Name))
		file, err := copyIngestedFile(storage, item.source, destination, item.artifact.Name, "")
		if err != nil {
			return nil, fmt.Errorf("materialize dependencies: copy %q from %q: %w", item.artifact.Name, item.producer, err)
		}
		if file.size != item.artifact.Size || !strings.EqualFold(file.sha256, item.artifact.SHA256) {
			return nil, fmt.Errorf("materialize dependencies: checksum mismatch for %q from %q", item.artifact.Name, item.producer)
		}
		materialized = append(materialized, filepath.ToSlash(filepath.Join(".t3", "dependencies", item.producer, item.artifact.Name)))
	}
	if err := makeIngestedTreeImmutable(stageDir); err != nil {
		return nil, fmt.Errorf("materialize dependencies: protect staged files: %w", err)
	}
	finalDir := filepath.Join(t3Dir, "dependencies")
	if err := os.Rename(stageDir, finalDir); err != nil {
		return nil, fmt.Errorf("materialize dependencies: publish files: %w", err)
	}
	keepStage = true
	return materialized, nil
}

func safePathComponent(value string) bool {
	return value != "" && value != "." && value != ".." &&
		filepath.Base(value) == value && !strings.ContainsAny(value, `/\\`)
}
