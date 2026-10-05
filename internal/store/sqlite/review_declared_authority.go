package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/review"
	"slices"
)

func (s *Store) GetFrozenReviewAuthority(ctx context.Context, runID, taskID string) (review.FrozenAuthority, bool, error) {
	var raw []byte
	if err := s.db.QueryRowContext(ctx, "SELECT record FROM coordinator_review_authorities WHERE run_id=? AND task_id=?", runID, taskID).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return review.FrozenAuthority{}, false, nil
		}
		return review.FrozenAuthority{}, false, err
	}
	var frozen review.FrozenAuthority
	if err := json.Unmarshal(raw, &frozen); err != nil {
		return frozen, true, err
	}
	canonical, err := frozen.Canonical()
	if err == nil && (canonical.Parent.RunID != runID || canonical.Parent.TaskID != taskID) {
		return review.FrozenAuthority{}, true, ErrReviewAuthorityIdentity
	}
	return canonical, true, err
}
func validateDeclaredAuthorityTx(ctx context.Context, tx *sql.Tx, f review.FrozenAuthority) error {
	fail := func() error {
		return fmt.Errorf("declared review authority: immutable task, criteria or assignment changed")
	}
	projection, err := loadWorkflowProjectionTx(ctx, tx, f.Parent.RunID)
	if err != nil {
		return err
	}
	workflow, err := loadReviewJSONTx[domain.Workflow](ctx, tx, "SELECT record FROM coordinator_workflows WHERE id=?", projection.Run.WorkflowID)
	if err != nil {
		return err
	}
	var task domain.Task
	for _, t := range projection.Tasks {
		if t.ID == f.Parent.TaskID {
			task = t
		}
	}
	d := task.ReviewRequirements
	if d == nil || d.Version != 1 || f.DeclarationDigest == "" || f.DeclarationDigest != domain.TaskDigest(task) || workflow.Environment.Type != "git" || workflow.Environment.Ref != f.Parent.BaseCommit {
		return fail()
	}
	if d.Risk != f.Requirements.Risk || d.RequiredReviewers != f.Requirements.RequiredReviewers || d.MinProviderFamilies != f.Requirements.MinProviderFamilies || d.RoundLimit != f.Requirements.RoundLimit || len(d.Members) != len(f.Requirements.Members) {
		return fail()
	}
	for _, m := range d.Members {
		found := false
		for _, frozen := range f.Requirements.Members {
			if m.ID == frozen.ID && m.Role == frozen.Role && m.Route == frozen.Route && m.Required == frozen.Required {
				found = true
			}
		}
		if !found {
			return fail()
		}
	}
	var retained struct {
		InputManifest pinnedinput.Manifest
		PolicyDigest  string
	}
	if err := json.Unmarshal(f.AdmissionProvenance, &retained); err != nil {
		return fail()
	}
	if workflow.InputManifest == nil || retained.PolicyDigest != f.Requirements.PolicyDigest {
		return fail()
	}
	canonical, err := pinnedinput.NewManifest(workflow.InputManifest.Entries)
	if err != nil || canonical.Digest != workflow.InputManifest.Digest || retained.InputManifest.Digest != canonical.Digest {
		return fail()
	}
	criteria := d.Criteria
	if projection.Run.Graph == nil {
		workflowIDs := append([]string(nil), workflow.InputArtifactIDs...)
		runIDs := append([]string(nil), projection.Run.InputArtifactIDs...)
		slices.Sort(workflowIDs)
		slices.Sort(runIDs)
		if !slices.Equal(workflowIDs, runIDs) || !slices.Contains(workflowIDs, criteria.ArtifactID) {
			return fail()
		}
		for i := 1; i < len(workflowIDs); i++ {
			if workflowIDs[i] == workflowIDs[i-1] {
				return fail()
			}
		}
	}
	for i, id := range task.InputArtifactIDs {
		if id == "" {
			return fail()
		}
		for _, prior := range task.InputArtifactIDs[:i] {
			if prior == id {
				return fail()
			}
		}
	}
	matched := false
	for _, pin := range canonical.Entries {
		if pin.Name == criteria.Name && pin.SHA256 == criteria.SHA256 && pin.Size == criteria.Size {
			matched = true
		}
	}
	if !matched {
		return fail()
	}
	a, err := loadReviewJSONTx[domain.Artifact](ctx, tx, "SELECT record FROM coordinator_artifacts WHERE id=?", criteria.ArtifactID)
	if err != nil {
		return err
	}
	if criteria.RunID != projection.Run.ID || criteria.WorkflowID != workflow.ID || criteria.TaskID != task.ID || a.WorkflowRunID != criteria.RunID || (a.TaskID != "" && a.TaskID != task.ID) || a.AttemptID != "" || a.Kind != domain.ArtifactInput || a.Producer != "submission" || a.Name != criteria.Name || a.Size != criteria.Size || a.SHA256 != criteria.SHA256 || !slices.Contains(task.InputArtifactIDs, a.ID) || a.SHA256 != f.Requirements.CriteriaDigest {
		return fail()
	}
	assignment, err := loadAssignmentTx(ctx, tx, f.Parent.AssignmentID)
	if err != nil {
		return err
	}
	if assignment.TaskDigest != domain.TaskDigest(task) || assignment.TaskRevision != task.DefinitionRevision || assignment.GraphRevision != projection.Run.GraphRevision {
		return fail()
	}
	return nil
}
