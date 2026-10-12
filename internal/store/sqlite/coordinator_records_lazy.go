package sqlite

import (
	"context"
	"database/sql"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// IndexAssignmentsByRun preserves assignment order and includes activation work.
// Projection still selects only assignments belonging to its declared attempts.
func IndexAssignmentsByRun(attempts []domain.Attempt, assignments []domain.Assignment) map[string][]domain.Assignment {
	runs := make(map[string]string, len(attempts))
	for _, attempt := range attempts {
		runs[attempt.ID] = attempt.WorkflowRunID
	}
	index := make(map[string][]domain.Assignment)
	for _, assignment := range assignments {
		if run, ok := runs[assignment.AttemptID]; ok {
			index[run] = append(index[run], assignment)
		}
	}
	return index
}

// WithRunArtifacts fills only requested runs omitted from a tick snapshot.
// Ordinary command snapshots already contain all artifacts. Returned slices and
// omission metadata belong to the caller, never to the shared tick cache.
func (r CoordinatorRecords) WithRunArtifacts(ctx context.Context, runIDs ...string) (CoordinatorRecords, error) {
	if r.loadRunArtifacts == nil {
		return r, nil
	}
	omitted := make(map[string]bool, len(r.omittedArtifactRuns))
	for id, missing := range r.omittedArtifactRuns {
		omitted[id] = missing
	}
	artifacts := append([]domain.Artifact(nil), r.Artifacts...)
	seen := make(map[string]bool, len(artifacts))
	for _, artifact := range artifacts {
		seen[artifact.ID] = true
	}
	for _, id := range runIDs {
		if !omitted[id] {
			continue
		}
		loaded, err := r.loadRunArtifacts(ctx, id)
		if err != nil {
			return CoordinatorRecords{}, err
		}
		for _, artifact := range loaded {
			if !seen[artifact.ID] {
				artifacts = append(artifacts, artifact)
				seen[artifact.ID] = true
			}
		}
		delete(omitted, id)
	}
	r.Artifacts, r.omittedArtifactRuns = artifacts, omitted
	return r, nil
}

// WithArtifactIDs loads missing immutable metadata, including inputs retained
// under a workflow's original, now terminal submission run.
func (r CoordinatorRecords) WithArtifactIDs(ctx context.Context, ids ...string) (CoordinatorRecords, error) {
	if r.loadArtifacts == nil {
		return r, nil
	}
	seen := make(map[string]bool, len(r.Artifacts))
	for _, a := range r.Artifacts {
		seen[a.ID] = true
	}
	var missing []string
	for _, id := range ids {
		if id != "" && !seen[id] {
			missing = append(missing, id)
			seen[id] = true
		}
	}
	if len(missing) == 0 {
		return r, nil
	}
	loaded, err := r.loadArtifacts(ctx, missing)
	if err != nil {
		return CoordinatorRecords{}, err
	}
	r.Artifacts = append(append([]domain.Artifact(nil), r.Artifacts...), loaded...)
	return r, nil
}

// WithCommitArtifactPresence checks retention without loading terminal output
// documents. The declared keys use taskID + NUL + outputName.
func (r CoordinatorRecords) WithCommitArtifactPresence(ctx context.Context, runID string, declared map[string]bool) (CoordinatorRecords, error) {
	if r.loadCommitArtifactPresence == nil {
		return r, nil
	}
	retained, err := r.loadCommitArtifactPresence(ctx, runID, declared)
	if err != nil {
		return CoordinatorRecords{}, err
	}
	checked := make(map[string]bool, len(r.CommitArtifactsRetained)+1)
	for id, present := range r.CommitArtifactsRetained {
		checked[id] = present
	}
	checked[runID] = retained
	r.CommitArtifactsRetained = checked
	return r, nil
}

func (s *Store) loadCommitArtifactPresence(ctx context.Context, runID string, declared map[string]bool) (bool, error) {
	var predicates []string
	args := []any{runID}
	for key, enabled := range declared {
		if !enabled {
			continue
		}
		task, name, ok := strings.Cut(key, "\x00")
		if !ok {
			continue
		}
		predicates = append(predicates, "(task_id = ? AND json_extract(record, '$.name') = ?)")
		args = append(args, task, name)
	}
	if len(predicates) == 0 {
		return false, nil
	}
	query := "SELECT EXISTS(SELECT 1 FROM coordinator_artifacts WHERE workflow_run_id = ? AND json_extract(record, '$.kind') = 'output' AND (" + strings.Join(predicates, " OR ") + "))"
	var retained bool
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&retained)
	return retained, err
}

func (s *Store) loadRunArtifacts(ctx context.Context, runID string) ([]domain.Artifact, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	artifacts, err := loadJSONQuery[domain.Artifact](ctx, tx, "coordinator_artifacts",
		"SELECT record FROM coordinator_artifacts WHERE workflow_run_id = ? ORDER BY id", runID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return artifacts, nil
}
