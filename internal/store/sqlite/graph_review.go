package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"reflect"
)

func validateReferencedTaskReviewTx(ctx context.Context, tx *sql.Tx, source []domain.Task, task domain.Task, remap map[string]string, inputs map[string]domain.Artifact, runID string) error {
	hasReview := task.ReviewRequirements != nil
	for _, s := range source {
		hasReview = hasReview || s.ReviewRequirements != nil
	}
	if !hasReview {
		return nil
	}
	var original domain.Task
	found := false
	for _, s := range source {
		if remap[s.ID] == task.ID {
			original = s
			found = true
		}
	}
	if !found {
		return fmt.Errorf("review declaration source task missing")
	}
	old, current := original.ReviewRequirements, task.ReviewRequirements
	if old == nil && current == nil {
		return nil
	}
	if old == nil || current == nil {
		return fmt.Errorf("clone/rerun cannot add or remove immutable review_requirements")
	}
	expected := domain.CloneTaskReview(old)
	expected.Criteria.RunID = runID
	expected.Criteria.TaskID = task.ID
	expected.Criteria.ArtifactID = current.Criteria.ArtifactID
	if !reflect.DeepEqual(expected, current) {
		return fmt.Errorf("clone/rerun review_requirements changed")
	}
	before, err := loadReviewJSONTx[domain.Artifact](ctx, tx, "SELECT record FROM coordinator_artifacts WHERE id=?", old.Criteria.ArtifactID)
	if err != nil {
		return err
	}
	after, ok := inputs[current.Criteria.ArtifactID]
	if !ok || after.WorkflowRunID != runID || after.Kind != domain.ArtifactInput || after.Producer != "submission" || (after.TaskID != "" && after.TaskID != task.ID) || after.AttemptID != "" || after.Name != old.Criteria.Name || after.Size != old.Criteria.Size || after.SHA256 != old.Criteria.SHA256 || after.StoragePath != before.StoragePath || before.Name != after.Name || before.Size != after.Size || before.SHA256 != after.SHA256 {
		return fmt.Errorf("clone/rerun review criteria reference lost retained source custody")
	}
	return nil
}
