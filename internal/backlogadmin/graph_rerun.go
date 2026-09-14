package backlogadmin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// rerunGraph builds the new run a rerun creates. The source run is only read.
//
// The shape of the new graph is the rerun subtree and nothing else: a reused
// ancestor is not a node here, because a node that is already finished would
// have to be given a fabricated successful attempt, and a fabricated outcome is
// exactly the evidence this command exists to protect. What the subtree needed
// from those ancestors travels instead as carried inputs, which name the source
// artifacts and copy no content.
func (s *Service) rerunGraph(
	ctx context.Context,
	p Principal,
	r domain.GraphAmendment,
	records sqlite.CoordinatorRecords,
	source domain.WorkflowRun,
	workflow domain.Workflow,
) (domain.GraphAmendmentResult, error) {
	var result domain.GraphAmendmentResult
	writer, ok := s.reader.(interface {
		CommitGraphRerun(context.Context, sqlite.GraphCommit) (domain.GraphAmendmentResult, error)
	})
	if !ok || s.artifactOpen == nil {
		return result, errors.New("rerun storage or artifact custody unavailable")
	}
	scope, err := domain.PlanRerun(source, records.Tasks, records.Attempts, r.TaskID)
	if err != nil {
		return result, err
	}
	reused := map[string]domain.Task{}
	for _, task := range scope.Reuse {
		reused[task.Name] = task
	}
	// Work on a deep copy: the source definitions must survive this function
	// byte for byte, whatever happens to the candidate.
	raw, err := json.Marshal(scope.Rerun)
	if err != nil {
		return result, err
	}
	var tasks []domain.Task
	if err = json.Unmarshal(raw, &tasks); err != nil {
		return result, err
	}
	runID := "run:rerun:" + r.ID
	idMap := map[string]string{}
	for index, task := range tasks {
		idMap[task.ID] = fmt.Sprintf("task:rerun:%s:%d", r.ID, index)
	}
	builder := rerunReferences{service: s, request: r, source: source, runID: runID, records: records}
	for index := range tasks {
		task := &tasks[index]
		sourceTaskID := task.ID
		task.ID = idMap[sourceTaskID]
		task.RunID = runID
		task.DefinitionRevision = 1
		if task.PromptArtifactID, err = builder.reference(ctx, task.PromptArtifactID, task.ID); err != nil {
			return result, err
		}
		for position, id := range task.InputArtifactIDs {
			if task.InputArtifactIDs[position], err = builder.reference(ctx, id, task.ID); err != nil {
				return result, err
			}
		}
		if err = builder.detach(ctx, task, reused); err != nil {
			return result, err
		}
		if err = s.graphValidator(workflow, *task); err != nil {
			return result, fmt.Errorf("validate task %s: %w", task.Name, err)
		}
	}
	provenance := domain.RerunProvenance{
		SourceRunID:     source.ID,
		SourceTaskID:    scope.From.ID,
		SourceAttemptID: scope.FromAttempt.ID,
		IdempotencyKey:  r.ID,
		Reason:          r.Reason,
	}
	return writer.CommitGraphRerun(ctx, sqlite.GraphCommit{
		Request: r, Actor: p.ID, Before: source, Tasks: tasks,
		Inputs: builder.inputs, Rerun: &provenance, Now: s.now().UTC(),
	})
}

// rerunReferences turns source artifacts into references the new run owns.
type rerunReferences struct {
	service *Service
	request domain.GraphAmendment
	source  domain.WorkflowRun
	runID   string
	records sqlite.CoordinatorRecords
	inputs  []domain.Artifact
	mapped  map[string]string
}

// detach removes the edges that pointed at reused ancestors and replaces the
// artifacts they supplied with carried references.
//
// Artifacts produced inside the rerun subtree are never considered here. They
// stay under the source run as evidence of what failed, and they are not
// visible as an input anywhere in the new run.
func (b *rerunReferences) detach(ctx context.Context, task *domain.Task, reused map[string]domain.Task) error {
	var keptNeeds []string
	for _, need := range task.Needs {
		ancestor, isReused := reused[need]
		if !isReused {
			keptNeeds = append(keptNeeds, need)
			continue
		}
		names := append([]string(nil), task.DependencyInputs[need]...)
		slices.Sort(names)
		delete(task.DependencyInputs, need)
		for _, name := range names {
			artifact, found := b.output(ancestor.ID, name)
			if !found {
				return fmt.Errorf(
					"run %s has no retained output %q from %s, so the rerun would start %s without an input it declares",
					b.source.ID, name, need, task.Name)
			}
			referenced, err := b.reference(ctx, artifact.ID, task.ID)
			if err != nil {
				return err
			}
			task.CarriedInputs = append(task.CarriedInputs, domain.CarriedInput{
				Producer: need, ProducerTaskID: ancestor.ID, Name: name, ArtifactID: referenced,
			})
		}
	}
	task.Needs = keptNeeds
	if len(task.DependencyInputs) == 0 {
		task.DependencyInputs = nil
	}
	return nil
}

// output finds a retained output artifact of a source task by declared name.
func (b *rerunReferences) output(taskID, name string) (domain.Artifact, bool) {
	for _, artifact := range b.records.Artifacts {
		if artifact.WorkflowRunID == b.source.ID && artifact.TaskID == taskID &&
			artifact.Kind == domain.ArtifactOutput && artifact.Name == name {
			return artifact, true
		}
	}
	return domain.Artifact{}, false
}

// reference makes a source artifact usable as an input of the new run.
//
// It records new metadata pointing at the same content address rather than
// copying bytes, and it opens the content first: a rerun that started a task
// whose input had already been pruned would fail later, further from the cause,
// with a message about a missing file rather than about a missing artifact.
func (b *rerunReferences) reference(ctx context.Context, artifactID, taskID string) (string, error) {
	if artifactID == "" {
		return "", errors.New("rerun input artifact is missing")
	}
	if b.mapped == nil {
		b.mapped = map[string]string{}
	}
	key := artifactID + "\x00" + taskID
	if existing := b.mapped[key]; existing != "" {
		return existing, nil
	}
	artifact, content, err := b.service.artifactOpen(ctx, artifactID)
	if err != nil {
		return "", fmt.Errorf(
			"artifact %s is no longer retrievable, so the rerun is refused rather than started without it: %w",
			artifactID, err)
	}
	// The configured opener verifies the immutable size and hash while the
	// content is read, so draining it is the retrievability check.
	_, readErr := io.Copy(io.Discard, content)
	closeErr := content.Close()
	if readErr != nil {
		return "", fmt.Errorf("artifact %s is no longer retrievable: %w", artifactID, readErr)
	}
	if closeErr != nil {
		return "", closeErr
	}
	if artifact.WorkflowRunID != b.source.ID {
		return "", fmt.Errorf("rerun input %s is outside the source run's custody", artifactID)
	}
	referenced := fmt.Sprintf("input:rerun:%s:%d", b.request.ID, len(b.inputs))
	artifact.ID = referenced
	artifact.WorkflowRunID = b.runID
	artifact.AttemptID = ""
	artifact.TaskID = taskID
	// Kind, Producer and StoragePath are preserved as far as the new run's
	// custody rules allow: the content address is the reference.
	artifact.Kind = domain.ArtifactInput
	b.inputs = append(b.inputs, artifact)
	b.mapped[key] = referenced
	return referenced, nil
}
