package backlogadmin

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func (s *Service) cloneGraph(ctx context.Context, p Principal, r domain.GraphAmendment, records sqlite.CoordinatorRecords, source domain.WorkflowRun, workflow domain.Workflow) (domain.GraphAmendmentResult, error) {
	var result domain.GraphAmendmentResult
	writer, ok := s.reader.(interface {
		CommitGraphClone(context.Context, sqlite.GraphCommit) (domain.GraphAmendmentResult, error)
	})
	if !ok || s.artifactOpen == nil {
		return result, errors.New("clone storage or artifact custody unavailable")
	}
	raw, err := json.Marshal(domain.TasksForRun(source, records.Tasks))
	if err != nil {
		return result, err
	}
	var tasks []domain.Task
	if err = json.Unmarshal(raw, &tasks); err != nil {
		return result, err
	}
	runID := domain.CloneRunID(r.ID)
	idMap := map[string]string{}
	for i, task := range tasks {
		idMap[task.ID] = domain.CloneTaskID(r.ID, i)
	}
	artifactMap := map[string]string{}
	var inputs []domain.Artifact
	now := s.now().UTC()
	copyInput := func(id string) (string, error) {
		if copied := artifactMap[id]; copied != "" {
			return copied, nil
		}
		a, content, err := s.artifactOpen(ctx, id)
		if err != nil {
			return "", err
		}
		// The configured opener verifies immutable size/hash before returning content.
		_, readErr := io.Copy(io.Discard, content)
		closeErr := content.Close()
		if readErr != nil {
			return "", readErr
		}
		if closeErr != nil {
			return "", closeErr
		}
		if a.Kind != domain.ArtifactInput || a.WorkflowRunID != source.ID {
			return "", errors.New("clone input is outside source custody")
		}
		copied := domain.CloneInputID(r.ID, len(inputs))
		a.ID = copied
		a.WorkflowRunID = runID
		a.AttemptID = ""
		if a.TaskID != "" {
			mapped := idMap[a.TaskID]
			if mapped == "" {
				return "", errors.New("clone input task unavailable")
			}
			a.TaskID = mapped
		}
		// Preserve Producer and StoragePath so verified submission objects continue
		// resolving under the submission root.
		a.CreatedAt = now
		inputs = append(inputs, a)
		artifactMap[id] = copied
		return copied, nil
	}
	for i := range tasks {
		t := &tasks[i]
		t.ID = idMap[t.ID]
		t.RunID = runID
		t.DefinitionRevision = 1
		t.PromptArtifactID, err = copyInput(t.PromptArtifactID)
		if err != nil {
			return result, err
		}
		for j, id := range t.InputArtifactIDs {
			t.InputArtifactIDs[j], err = copyInput(id)
			if err != nil {
				return result, err
			}
		}
		if t.ReviewRequirements != nil {
			t.ReviewRequirements.Criteria.ArtifactID = artifactMap[t.ReviewRequirements.Criteria.ArtifactID]
			t.ReviewRequirements.Criteria.RunID = runID
			t.ReviewRequirements.Criteria.TaskID = t.ID
		}
		if err = s.graphValidator(workflow, *t); err != nil {
			return result, err
		}
	}
	// idMap is the only place the source-to-clone task identity is known, so it
	// travels to the store, which needs it to move inherited gate definitions
	// onto the cloned tasks.
	if err = validateDerivedGraphIDs("clone", tasks, inputs); err != nil {
		return result, err
	}
	return writer.CommitGraphClone(ctx, sqlite.GraphCommit{Request: r, Actor: p.ID, Before: source, Tasks: tasks, Inputs: inputs, TaskIDRemap: idMap, Now: now})
}
