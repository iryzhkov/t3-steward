package backlog

import (
	"fmt"
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

// commitBundleBudget is what an execution package may still spend on commit
// bundles once every other input is counted.
type commitBundleBudget struct {
	// Remaining is the package's total byte limit less its other inputs.
	Remaining int64
	// MaxTotalBytes and MaxArtifactBytes are the package's limits, named in
	// the reason a bundle is not carried.
	MaxTotalBytes    int64
	MaxArtifactBytes int64
}

// packageCommitBundles selects the retained bundles of the declared commits
// this task consumes that were produced on a worker other than workerID: the
// commits of its direct dependencies in this run, and the commits it carried
// from another run, which a rerun and an external dependency both do.
//
// Every bundle is bound to the exact attempt whose provenance record the task
// receives: for a direct dependency, the attempt that produced the record in
// this run; for a carried input, the source attempt it pinned. A bundle of any
// other attempt of the same task is never selected.
//
// A commit produced on the consuming worker is already in that worker's
// campaign ref store, so its bundle is not delivered at all. A commit with no
// retained bundle is left out as well; the consuming worker then refuses it
// with the reason its provenance record gives, which is more precise than any
// refusal here could be. A bundle that the package cannot carry within its
// limits is named with the reason instead of being delivered, so that the offer
// is still made and the consuming worker refuses that commit specifically, and
// only if it does not already hold it.
func packageCommitBundles(
	task domain.Task,
	tasks []domain.Task,
	artifacts map[string]domain.Artifact,
	runID, workerID string,
	producedOn map[string]string,
	budget commitBundleBudget,
) ([]workerproto.CommitBundleInput, error) {
	if len(task.DependencyInputs) == 0 && len(task.CarriedInputs) == 0 {
		return nil, nil
	}
	type selected struct {
		input  workerproto.CommitBundleInput
		bundle domain.Artifact
	}
	var candidates []selected
	bundles := make(map[string]domain.Artifact)
	records := make(map[string]domain.Artifact)
	for _, artifact := range artifacts {
		switch {
		case artifact.Kind == domain.ArtifactGitState:
			bundles[artifact.WorkflowRunID+"\x00"+artifact.TaskID+"\x00"+artifact.AttemptID+"\x00"+artifact.Name] = artifact
		case artifact.Kind == domain.ArtifactOutput && artifact.WorkflowRunID == runID:
			records[artifact.TaskID+"\x00"+artifact.Name] = artifact
		}
	}
	consider := func(sourceRun, sourceTask, sourceAttempt, name string) {
		bundle, retained := bundles[sourceRun+"\x00"+sourceTask+"\x00"+sourceAttempt+"\x00"+CommitBundleArtifactName(name)]
		if !retained || producedOn[bundle.AttemptID] == workerID {
			return
		}
		candidates = append(candidates, selected{
			input:  workerproto.CommitBundleInput{WorkflowRunID: sourceRun, TaskID: sourceTask, Name: name},
			bundle: bundle,
		})
	}

	taskByName := make(map[string]domain.Task, len(tasks))
	for _, candidate := range tasks {
		taskByName[candidate.Name] = candidate
	}
	producers := make([]string, 0, len(task.DependencyInputs))
	for producer := range task.DependencyInputs {
		producers = append(producers, producer)
	}
	sort.Strings(producers)
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
			record, recorded := records[producerTask.ID+"\x00"+declaration.Name]
			if !recorded {
				continue
			}
			consider(runID, producerTask.ID, record.AttemptID, declaration.Name)
		}
	}

	// A carried input without its source attempt predates the binding; there
	// is no attempt to select a bundle of, so none is delivered and a consumer
	// elsewhere is refused by the provenance check, never handed a guess.
	carried := append([]domain.CarriedInput(nil), task.CarriedInputs...)
	sort.Slice(carried, func(i, j int) bool {
		left, right := carried[i], carried[j]
		if left.SourceRunID != right.SourceRunID {
			return left.SourceRunID < right.SourceRunID
		}
		if left.ProducerTaskID != right.ProducerTaskID {
			return left.ProducerTaskID < right.ProducerTaskID
		}
		return left.Name < right.Name
	})
	for _, input := range carried {
		if input.SourceRunID == "" || input.SourceAttemptID == "" {
			continue
		}
		consider(input.SourceRunID, input.ProducerTaskID, input.SourceAttemptID, filepath.ToSlash(input.Name))
	}

	result := make([]workerproto.CommitBundleInput, 0, len(candidates))
	remaining := budget.Remaining
	for _, candidate := range candidates {
		input, bundle := candidate.input, candidate.bundle
		switch {
		case bundle.Size > budget.MaxArtifactBytes:
			input.Omitted = fmt.Sprintf("its bundle is %d bytes, which exceeds the execution package's per-artifact limit of %d bytes",
				bundle.Size, budget.MaxArtifactBytes)
		case bundle.Size > remaining:
			input.Omitted = fmt.Sprintf("its bundle is %d bytes, and with this task's other inputs the execution package would exceed its total byte limit of %d bytes",
				bundle.Size, budget.MaxTotalBytes)
		default:
			object, err := packageArtifact(bundle, workerproto.CommitBundlePath(input.WorkflowRunID, input.TaskID, input.Name), "commit-bundle")
			if err != nil {
				return nil, err
			}
			input.Bundle = &object
			remaining -= bundle.Size
		}
		result = append(result, input)
	}
	return result, nil
}

// declaresCommit reports whether a package's task declares a commit output.
func declaresCommit(pkg workerproto.ExecutionPackage) bool {
	return slices.ContainsFunc(pkg.Outputs, func(output domain.ArtifactDeclaration) bool { return output.Commit != nil })
}
