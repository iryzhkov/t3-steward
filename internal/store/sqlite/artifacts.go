package sqlite

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var (
	ErrStaleArtifactPublication = errors.New("stale artifact publication")
	ErrArtifactConflict         = errors.New("artifact publication conflicts with immutable metadata")
)

// CommitArtifactPublication atomically fences immutable artifact metadata to
// the current coordinator, assignment, worker process, and attempt revision.
func (s *Store) CommitArtifactPublication(ctx context.Context, publication domain.ArtifactPublication) (domain.Artifact, error) {
	artifact := publication.Artifact
	if err := validateArtifactPublication(publication); err != nil {
		return domain.Artifact{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Artifact{}, fmt.Errorf("begin artifact publication: %w", err)
	}
	defer tx.Rollback()

	existing, found, err := loadArtifactTx(ctx, tx, artifact.ID)
	if err != nil {
		return domain.Artifact{}, err
	}
	if found {
		if !sameArtifact(existing, artifact) {
			return domain.Artifact{}, fmt.Errorf("%w: %s", ErrArtifactConflict, artifact.ID)
		}
		return existing, nil
	}
	if err := requireCoordinatorEpoch(ctx, tx, publication.CoordinatorEpoch); err != nil {
		return domain.Artifact{}, fmt.Errorf("%w: %v", ErrStaleArtifactPublication, err)
	}
	assignment, err := loadAssignmentTx(ctx, tx, publication.AssignmentID)
	if err != nil {
		return domain.Artifact{}, fmt.Errorf("%w: load assignment: %v", ErrStaleArtifactPublication, err)
	}
	if assignment.AttemptID != artifact.AttemptID ||
		assignment.WorkerID != publication.WorkerID ||
		assignment.WorkerEpoch != publication.WorkerEpoch ||
		assignment.Epoch != publication.AssignmentEpoch ||
		assignment.State != domain.AssignmentClaimed {
		return domain.Artifact{}, fmt.Errorf("%w: assignment identity or state changed", ErrStaleArtifactPublication)
	}
	attempt, err := loadAttemptTx(ctx, tx, artifact.AttemptID)
	if err != nil {
		return domain.Artifact{}, fmt.Errorf("%w: load attempt: %v", ErrStaleArtifactPublication, err)
	}
	if attempt.WorkflowRunID != artifact.WorkflowRunID || attempt.TaskID != artifact.TaskID ||
		attempt.AssignmentID != assignment.ID || attempt.Revision != publication.AttemptRevision {
		return domain.Artifact{}, fmt.Errorf("%w: attempt identity or revision changed", ErrStaleArtifactPublication)
	}
	raw, err := json.Marshal(artifact)
	if err != nil {
		return domain.Artifact{}, fmt.Errorf("encode artifact %q: %w", artifact.ID, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO coordinator_artifacts(
		id, workflow_run_id, task_id, attempt_id, sha256, record
	) VALUES (?, ?, ?, ?, ?, ?)`,
		artifact.ID, artifact.WorkflowRunID, artifact.TaskID, artifact.AttemptID, artifact.SHA256, string(raw)); err != nil {
		return domain.Artifact{}, fmt.Errorf("insert artifact %q: %w", artifact.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return domain.Artifact{}, fmt.Errorf("commit artifact %q: %w", artifact.ID, err)
	}
	return artifact, nil
}

// LoadArtifacts returns immutable metadata for the requested IDs in request order.
func (s *Store) LoadArtifacts(ctx context.Context, ids []string) ([]domain.Artifact, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin artifact load: %w", err)
	}
	defer tx.Rollback()
	result := make([]domain.Artifact, 0, len(ids))
	for _, id := range ids {
		artifact, found, err := loadArtifactTx(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("artifact %q not found", id)
		}
		result = append(result, artifact)
	}
	return result, nil
}

// PruneArtifacts removes expired metadata and returns the deleted records.
// Protected workflow runs are retained regardless of age.
func (s *Store) PruneArtifacts(ctx context.Context, before time.Time, protectedRunIDs []string) ([]domain.Artifact, error) {
	records, err := s.LoadCoordinatorRecords(ctx)
	if err != nil {
		return nil, err
	}
	protected := make(map[string]struct{}, len(protectedRunIDs))
	for _, id := range protectedRunIDs {
		protected[id] = struct{}{}
	}
	var expired []domain.Artifact
	for _, artifact := range records.Artifacts {
		if _, keep := protected[artifact.WorkflowRunID]; !keep &&
			!artifact.CreatedAt.IsZero() && artifact.CreatedAt.Before(before) {
			expired = append(expired, artifact)
		}
	}
	if len(expired) == 0 {
		return nil, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin artifact prune: %w", err)
	}
	defer tx.Rollback()
	for _, artifact := range expired {
		if _, err := tx.ExecContext(ctx, `DELETE FROM coordinator_artifacts WHERE id = ?`, artifact.ID); err != nil {
			return nil, fmt.Errorf("delete artifact %q: %w", artifact.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit artifact prune: %w", err)
	}
	return expired, nil
}

// ArtifactStoragePathReferenced reports whether retained metadata still owns a blob.
func (s *Store) ArtifactStoragePathReferenced(ctx context.Context, storagePath string) (bool, error) {
	records, err := s.LoadCoordinatorRecords(ctx)
	if err != nil {
		return false, err
	}
	for _, artifact := range records.Artifacts {
		if artifact.StoragePath == storagePath {
			return true, nil
		}
	}
	return false, nil
}

func validateArtifactPublication(publication domain.ArtifactPublication) error {
	artifact := publication.Artifact
	if publication.CoordinatorEpoch < 1 || publication.AssignmentEpoch < 1 || publication.AttemptRevision < 1 {
		return errors.New("artifact publication epochs and attempt revision must be positive")
	}
	for label, value := range map[string]string{
		"worker ID": publication.WorkerID, "worker epoch": publication.WorkerEpoch,
		"assignment ID": publication.AssignmentID, "artifact ID": artifact.ID,
		"workflow run ID": artifact.WorkflowRunID, "task ID": artifact.TaskID,
		"attempt ID": artifact.AttemptID, "artifact name": artifact.Name,
		"media type": artifact.MediaType, "sha256": artifact.SHA256,
		"storage path": artifact.StoragePath, "producer": artifact.Producer,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", label)
		}
	}
	if artifact.Size < 0 || len(artifact.SHA256) != 64 || artifact.CreatedAt.IsZero() {
		return errors.New("artifact size, sha256, and creation time are invalid")
	}
	if _, err := hex.DecodeString(artifact.SHA256); err != nil {
		return errors.New("artifact sha256 is not hexadecimal")
	}
	hash := strings.ToLower(artifact.SHA256)
	wantStoragePath := path.Join("objects", hash[:2], hash)
	if artifact.StoragePath != wantStoragePath {
		return fmt.Errorf("artifact storage path %q does not match content address %q", artifact.StoragePath, wantStoragePath)
	}
	if path.Clean(artifact.Name) != artifact.Name || strings.HasPrefix(artifact.Name, "/") ||
		artifact.Name == "." || strings.HasPrefix(artifact.Name, "../") {
		return errors.New("artifact name is not a safe relative path")
	}
	switch artifact.Kind {
	case domain.ArtifactInput, domain.ArtifactOutput, domain.ArtifactCheckpoint,
		domain.ArtifactLog, domain.ArtifactSummary, domain.ArtifactGitState,
		domain.ArtifactVerification:
	default:
		return fmt.Errorf("artifact kind %q is invalid", artifact.Kind)
	}
	return nil
}

func loadArtifactTx(ctx context.Context, tx *sql.Tx, id string) (domain.Artifact, bool, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT record FROM coordinator_artifacts WHERE id = ?`, id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Artifact{}, false, nil
	}
	if err != nil {
		return domain.Artifact{}, false, fmt.Errorf("load artifact %q: %w", id, err)
	}
	var artifact domain.Artifact
	if err := json.Unmarshal([]byte(raw), &artifact); err != nil {
		return domain.Artifact{}, false, fmt.Errorf("decode artifact %q: %w", id, err)
	}
	return artifact, true, nil
}

func sameArtifact(left, right domain.Artifact) bool {
	left.CreatedAt = left.CreatedAt.UTC()
	right.CreatedAt = right.CreatedAt.UTC()
	return left == right
}
