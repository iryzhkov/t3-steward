package backlog

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type ExecutionPackageRecordStore interface {
	LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
}

// CoordinatorOfferBuilder resolves a committed assignment into the immutable
// package understood by the selected worker.
type CoordinatorOfferBuilder struct {
	Store               ExecutionPackageRecordStore
	Catalog             *ProjectCatalog
	CatalogRevision     string
	CoordinatorID       string
	CoordinatorEpoch    int64
	VerificationTimeout time.Duration
	MaxArtifactBytes    int64
	MaxTotalBytes       int64
}

func (b CoordinatorOfferBuilder) BuildAssignmentOffer(
	ctx context.Context,
	assignment domain.Assignment,
	expiresAt time.Time,
) (workerproto.AssignmentOffer, error) {
	if b.Store == nil || b.Catalog == nil || b.CatalogRevision == "" || b.CoordinatorID == "" ||
		b.CoordinatorEpoch < 1 || b.VerificationTimeout <= 0 || b.MaxArtifactBytes <= 0 ||
		b.MaxTotalBytes < b.MaxArtifactBytes {
		return workerproto.AssignmentOffer{}, errors.New("execution package builder: complete authority, catalog, and limits are required")
	}
	if assignment.ID == "" || assignment.State != domain.AssignmentOffered ||
		assignment.WorkerID == "" || assignment.WorkerEpoch == "" ||
		assignment.ThreadID == "" || assignment.CreatedAt.IsZero() ||
		!expiresAt.After(assignment.CreatedAt) || expiresAt.After(assignment.LeaseExpiresAt) {
		return workerproto.AssignmentOffer{}, errors.New("execution package builder: invalid offered assignment or expiry")
	}
	records, err := b.Store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return workerproto.AssignmentOffer{}, err
	}
	state, err := resolveExecutionPackageState(records, assignment)
	if err != nil {
		return workerproto.AssignmentOffer{}, err
	}
	environment, err := b.Catalog.Resolve(state.workflow, state.task)
	if err != nil {
		return workerproto.AssignmentOffer{}, err
	}
	if environment.Setup.Timeout <= 0 {
		return workerproto.AssignmentOffer{}, errors.New("execution package builder: setup profile timeout is required")
	}
	promptArtifact, err := requiredArtifact(state.artifacts, state.task.PromptArtifactID, state.run.ID)
	if err != nil {
		return workerproto.AssignmentOffer{}, fmt.Errorf("execution package builder: prompt: %w", err)
	}
	if promptArtifact.Kind != domain.ArtifactInput || promptArtifact.TaskID != state.task.ID {
		return workerproto.AssignmentOffer{}, errors.New("execution package builder: prompt artifact ownership is invalid")
	}
	prompt, err := packageArtifact(promptArtifact, "prompt/"+filepath.ToSlash(promptArtifact.Name), "prompt")
	if err != nil {
		return workerproto.AssignmentOffer{}, fmt.Errorf("execution package builder: prompt: %w", err)
	}
	staticInputs := make([]workerproto.ArtifactObject, 0, len(state.task.InputArtifactIDs))
	seenInputs := make(map[string]struct{}, len(state.task.InputArtifactIDs))
	for _, id := range state.task.InputArtifactIDs {
		if _, duplicate := seenInputs[id]; duplicate {
			return workerproto.AssignmentOffer{}, fmt.Errorf("execution package builder: repeated static input %q", id)
		}
		seenInputs[id] = struct{}{}
		artifact, err := requiredArtifact(state.artifacts, id, state.run.ID)
		if err != nil {
			return workerproto.AssignmentOffer{}, fmt.Errorf("execution package builder: static input: %w", err)
		}
		if artifact.Kind != domain.ArtifactInput || (artifact.TaskID != "" && artifact.TaskID != state.task.ID) {
			return workerproto.AssignmentOffer{}, fmt.Errorf("execution package builder: static input %q ownership is invalid", id)
		}
		object, err := packageArtifact(artifact, "inputs/"+filepath.ToSlash(artifact.Name), "input")
		if err != nil {
			return workerproto.AssignmentOffer{}, fmt.Errorf("execution package builder: static input: %w", err)
		}
		staticInputs = append(staticInputs, object)
	}
	dependencies, err := packageDependencies(state.task, state.tasks, state.artifacts, state.run.ID)
	if err != nil {
		return workerproto.AssignmentOffer{}, fmt.Errorf("execution package builder: dependencies: %w", err)
	}
	pkg := workerproto.ExecutionPackage{
		Version:          workerproto.ExecutionPackageVersion,
		ID:               stableCoordinatorID("package", assignment.ID),
		CoordinatorID:    b.CoordinatorID,
		CoordinatorEpoch: b.CoordinatorEpoch,
		WorkerID:         assignment.WorkerID,
		WorkerEpoch:      assignment.WorkerEpoch,
		Identity: workerproto.ExecutionIdentity{
			WorkflowID: state.workflow.ID, WorkflowRunID: state.run.ID, TaskID: state.task.ID,
			AttemptID: assignment.AttemptID, AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
			DispatchToken: assignment.DispatchToken, ThreadID: assignment.ThreadID,
		},
		Class:        state.task.Class,
		Prompt:       prompt,
		StaticInputs: staticInputs,
		Dependencies: dependencies,
		Route:        cloneProviderRoute(assignment.Route),
		Environment: workerproto.EnvironmentReference{
			CatalogRevision: b.CatalogRevision, Project: environment.ProjectName,
			Repository: environment.Repository, Ref: environment.Ref, Scope: environment.Scope,
			SetupProfile: environment.Setup.Name, T3Project: environment.T3ProjectTemplate,
			ResourceLocks:       append([]string(nil), environment.ResourceLocks...),
			RequiredCredentials: append([]string(nil), environment.RequiredCredentials...),
		},
		Verification: append([]string(nil), state.task.Verification...),
		Outputs:      append([]domain.ArtifactDeclaration(nil), state.task.Outputs...),
		NotBefore:    cloneTime(state.task.NotBefore),
		Deadline:     cloneTime(state.task.Deadline),
		ExpiresAt:    cloneTime(state.task.ExpiresAt),
		Limits: workerproto.ExecutionLimits{
			MaxTurns: state.task.MaxTurns, PrepareTimeout: environment.Setup.Timeout,
			VerificationTimeout: b.VerificationTimeout,
			MaxArtifactBytes:    b.MaxArtifactBytes, MaxTotalBytes: b.MaxTotalBytes,
		},
		CreatedAt: assignment.CreatedAt,
	}
	manifest, err := workerproto.BuildExecutionPackageManifest(pkg)
	if err != nil {
		return workerproto.AssignmentOffer{}, err
	}
	return workerproto.AssignmentOffer{Assignment: assignment, Package: manifest, ExpiresAt: expiresAt}, nil
}

type executionPackageState struct {
	workflow  domain.Workflow
	run       domain.WorkflowRun
	task      domain.Task
	attempt   domain.Attempt
	tasks     []domain.Task
	artifacts map[string]domain.Artifact
}

func resolveExecutionPackageState(records sqlite.CoordinatorRecords, assignment domain.Assignment) (executionPackageState, error) {
	var state executionPackageState
	var durableAssignment domain.Assignment
	var foundAssignment bool
	for _, candidate := range records.Assignments {
		if candidate.ID != assignment.ID {
			continue
		}
		if foundAssignment {
			return state, fmt.Errorf("execution package builder: duplicate assignment %q", assignment.ID)
		}
		durableAssignment, foundAssignment = candidate, true
	}
	comparison := assignment
	comparison.LeaseExpiresAt = durableAssignment.LeaseExpiresAt
	if !foundAssignment || durableAssignment.State != domain.AssignmentOffered ||
		!reflect.DeepEqual(durableAssignment, comparison) {
		return state, fmt.Errorf("execution package builder: assignment %q does not match its durable offer", assignment.ID)
	}
	var foundAttempt bool
	for _, attempt := range records.Attempts {
		if attempt.ID != assignment.AttemptID {
			continue
		}
		if foundAttempt {
			return state, fmt.Errorf("execution package builder: duplicate attempt %q", attempt.ID)
		}
		state.attempt, foundAttempt = attempt, true
	}
	if !foundAttempt || state.attempt.AssignmentID != assignment.ID {
		return state, fmt.Errorf("execution package builder: assignment %q is not attached to attempt %q", assignment.ID, assignment.AttemptID)
	}
	var foundTask bool
	for _, task := range records.Tasks {
		if task.ID == state.attempt.TaskID {
			if foundTask {
				return state, fmt.Errorf("execution package builder: duplicate task %q", task.ID)
			}
			state.task, foundTask = task, true
		}
	}
	if !foundTask {
		return state, fmt.Errorf("execution package builder: missing task %q", state.attempt.TaskID)
	}
	var foundRun bool
	for _, run := range records.WorkflowRuns {
		if run.ID == state.attempt.WorkflowRunID {
			if foundRun {
				return state, fmt.Errorf("execution package builder: duplicate run %q", run.ID)
			}
			state.run, foundRun = run, true
		}
	}
	if !foundRun {
		return state, fmt.Errorf("execution package builder: missing run %q", state.attempt.WorkflowRunID)
	}
	var foundWorkflow bool
	for _, workflow := range records.Workflows {
		if workflow.ID == state.run.WorkflowID {
			if foundWorkflow {
				return state, fmt.Errorf("execution package builder: duplicate workflow %q", workflow.ID)
			}
			state.workflow, foundWorkflow = workflow, true
		}
	}
	if !foundWorkflow || state.task.WorkflowID != state.workflow.ID {
		return state, errors.New("execution package builder: workflow, run, and task links are inconsistent")
	}
	state.tasks = make([]domain.Task, 0, len(state.workflow.TaskIDs))
	for _, task := range records.Tasks {
		if task.WorkflowID == state.workflow.ID {
			state.tasks = append(state.tasks, task)
		}
	}
	state.artifacts = make(map[string]domain.Artifact, len(records.Artifacts))
	for _, artifact := range records.Artifacts {
		if artifact.ID == "" {
			return state, errors.New("execution package builder: artifact has no identity")
		}
		if _, duplicate := state.artifacts[artifact.ID]; duplicate {
			return state, fmt.Errorf("execution package builder: duplicate artifact %q", artifact.ID)
		}
		state.artifacts[artifact.ID] = artifact
	}
	return state, nil
}

func requiredArtifact(artifacts map[string]domain.Artifact, id, runID string) (domain.Artifact, error) {
	artifact, exists := artifacts[id]
	if !exists || id == "" {
		return domain.Artifact{}, fmt.Errorf("missing artifact %q", id)
	}
	if artifact.WorkflowRunID != runID {
		return domain.Artifact{}, fmt.Errorf("artifact %q belongs to run %q", id, artifact.WorkflowRunID)
	}
	return artifact, nil
}

func packageArtifact(artifact domain.Artifact, path, kind string) (workerproto.ArtifactObject, error) {
	if artifact.ID == "" || artifact.Name == "" || artifact.MediaType == "" ||
		artifact.Size <= 0 || artifact.SHA256 == "" {
		return workerproto.ArtifactObject{}, fmt.Errorf("artifact %q has incomplete immutable metadata", artifact.ID)
	}
	return workerproto.ArtifactObject{
		ID: artifact.ID, Path: path, Kind: kind, MediaType: artifact.MediaType,
		Size: artifact.Size, SHA256: artifact.SHA256,
	}, nil
}

func packageDependencies(
	task domain.Task,
	tasks []domain.Task,
	artifacts map[string]domain.Artifact,
	runID string,
) ([]workerproto.DependencyInput, error) {
	taskByName := make(map[string]domain.Task, len(tasks))
	for _, candidate := range tasks {
		if _, duplicate := taskByName[candidate.Name]; duplicate {
			return nil, fmt.Errorf("duplicate task name %q", candidate.Name)
		}
		taskByName[candidate.Name] = candidate
	}
	outputs := make(map[string]domain.Artifact)
	for _, artifact := range artifacts {
		if artifact.WorkflowRunID != runID || artifact.Kind != domain.ArtifactOutput {
			continue
		}
		key := artifact.TaskID + "\x00" + artifact.Name
		if _, duplicate := outputs[key]; duplicate {
			return nil, fmt.Errorf("duplicate output %q for task %q", artifact.Name, artifact.TaskID)
		}
		outputs[key] = artifact
	}
	direct := make(map[string]struct{}, len(task.Needs))
	for _, name := range task.Needs {
		direct[name] = struct{}{}
	}
	producers := make([]string, 0, len(task.DependencyInputs))
	for producer := range task.DependencyInputs {
		if _, allowed := direct[producer]; !allowed {
			return nil, fmt.Errorf("producer %q is not a direct dependency", producer)
		}
		producers = append(producers, producer)
	}
	sort.Strings(producers)
	result := make([]workerproto.DependencyInput, 0, len(producers))
	for _, producer := range producers {
		producerTask, exists := taskByName[producer]
		if !exists {
			return nil, fmt.Errorf("missing producer task %q", producer)
		}
		names := append([]string(nil), task.DependencyInputs[producer]...)
		sort.Strings(names)
		dependency := workerproto.DependencyInput{TaskID: producerTask.ID}
		for _, name := range names {
			artifact, exists := outputs[producerTask.ID+"\x00"+filepath.ToSlash(name)]
			if !exists {
				return nil, fmt.Errorf("missing output %q from %q", name, producer)
			}
			object, err := packageArtifact(
				artifact,
				"dependencies/"+producer+"/"+filepath.ToSlash(artifact.Name),
				"dependency",
			)
			if err != nil {
				return nil, err
			}
			dependency.Artifacts = append(dependency.Artifacts, object)
		}
		result = append(result, dependency)
	}
	return result, nil
}
