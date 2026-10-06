package backlog

import (
	"path/filepath"
	"slices"
	"sort"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// attemptWorkers maps each attempt to the worker its current assignment
// placed it on, which is the worker whose campaign ref store holds what the
// attempt published.
func attemptWorkers(records sqlite.CoordinatorRecords) map[string]string {
	workers := make(map[string]string, len(records.Assignments))
	assignments := make(map[string]string, len(records.Assignments))
	for _, assignment := range records.Assignments {
		assignments[assignment.ID] = assignment.WorkerID
	}
	for _, attempt := range records.Attempts {
		if worker := assignments[attempt.AssignmentID]; worker != "" {
			workers[attempt.ID] = worker
		}
	}
	return workers
}

// packageCommitBundles selects the retained bundles of the declared commits
// this task consumes that were produced on a worker other than workerID.
//
// A commit produced on the consuming worker is already in that worker's
// campaign ref store, so its bundle is not delivered at all. A commit with no
// retained bundle is left out as well; the consuming worker then refuses it
// with the reason its provenance record gives, which is more precise than any
// refusal here could be.
func packageCommitBundles(
	task domain.Task,
	tasks []domain.Task,
	artifacts map[string]domain.Artifact,
	runID, workerID string,
	producedOn map[string]string,
) ([]workerproto.CommitBundleInput, error) {
	if len(task.DependencyInputs) == 0 {
		return nil, nil
	}
	taskByName := make(map[string]domain.Task, len(tasks))
	for _, candidate := range tasks {
		taskByName[candidate.Name] = candidate
	}
	bundles := make(map[string]domain.Artifact)
	for _, artifact := range artifacts {
		if artifact.WorkflowRunID == runID && artifact.Kind == domain.ArtifactGitState {
			bundles[artifact.TaskID+"\x00"+artifact.Name] = artifact
		}
	}
	producers := make([]string, 0, len(task.DependencyInputs))
	for producer := range task.DependencyInputs {
		producers = append(producers, producer)
	}
	sort.Strings(producers)
	var result []workerproto.CommitBundleInput
	for _, producer := range producers {
		producerTask, exists := taskByName[producer]
		if !exists {
			continue
		}
		names := task.DependencyInputs[producer]
		for _, declaration := range producerTask.Outputs {
			if declaration.Commit == nil || !slices.ContainsFunc(names, func(name string) bool {
				return filepath.ToSlash(name) == declaration.Name
			}) {
				continue
			}
			bundle, retained := bundles[producerTask.ID+"\x00"+CommitBundleArtifactName(declaration.Name)]
			if !retained || producedOn[bundle.AttemptID] == workerID {
				continue
			}
			object, err := packageArtifact(bundle, workerproto.CommitBundlePath(producerTask.ID, declaration.Name), "commit-bundle")
			if err != nil {
				return nil, err
			}
			result = append(result, workerproto.CommitBundleInput{TaskID: producerTask.ID, Name: declaration.Name, Bundle: object})
		}
	}
	return result, nil
}

// declaresCommit reports whether a package's task declares a commit output.
func declaresCommit(pkg workerproto.ExecutionPackage) bool {
	return slices.ContainsFunc(pkg.Outputs, func(output domain.ArtifactDeclaration) bool { return output.Commit != nil })
}
