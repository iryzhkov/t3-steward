package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var ErrActivationEvidenceConflict = errors.New("activation evidence conflicts with immutable snapshot")

type ActivationEvidencePublication struct {
	CoordinatorEpoch int64
	ActivationID     string
	RunID            string
	ActivationEpoch  int64
	GraphRevision    int64
	Artifact         domain.Artifact
}

// LoadActivationEvidence returns the frozen artifact for one activation.
func (s *Store) LoadActivationEvidence(ctx context.Context, runID, activationID string) (domain.Artifact, bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return domain.Artifact{}, false, err
	}
	defer tx.Rollback()
	artifact, found, err := loadActivationEvidenceTx(ctx, tx, runID, activationID)
	if err != nil {
		return domain.Artifact{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Artifact{}, false, err
	}
	return artifact, found, nil
}

// EnsureActivationEvidence freezes the first artifact for an activation.
// Replays return that exact row even if mutable supervision state later advances.
func (s *Store) EnsureActivationEvidence(ctx context.Context, request ActivationEvidencePublication) (domain.Artifact, bool, error) {
	if request.ActivationID == "" || request.RunID == "" || request.ActivationEpoch <= 0 ||
		request.GraphRevision <= 0 || request.Artifact.ID == "" ||
		request.Artifact.WorkflowRunID != request.RunID ||
		request.Artifact.AttemptID == "" ||
		request.Artifact.Name != "supervision-evidence-"+request.ActivationID+".json" ||
		request.Artifact.MediaType != "application/vnd.t3-steward.supervision-evidence.v1+json" ||
		request.Artifact.Producer != "coordinator/supervision" {
		return domain.Artifact{}, false, errors.New("activation evidence publication has incomplete identity")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Artifact{}, false, fmt.Errorf("begin activation evidence publication: %w", err)
	}
	defer tx.Rollback()

	if err := requireCoordinatorEpoch(ctx, tx, request.CoordinatorEpoch); err != nil {
		return domain.Artifact{}, false, err
	}
	run, err := loadActivationRunTx(ctx, tx, request.RunID)
	if err != nil {
		return domain.Artifact{}, false, err
	}
	activations, err := loadSupervisionActivationsTx(ctx, tx, request.RunID)
	if err != nil {
		return domain.Artifact{}, false, err
	}
	var live domain.Activation
	for _, activation := range activations {
		if activation.ID == request.ActivationID {
			live = activation
			break
		}
	}
	if live.ID == "" || live.RunID != request.RunID || live.Epoch != request.ActivationEpoch {
		return domain.Artifact{}, false, fmt.Errorf("%w: activation identity or epoch changed", ErrActivationEvidenceConflict)
	}
	if err := requireActivationEvidenceAttemptTx(ctx, tx, request); err != nil {
		return domain.Artifact{}, false, err
	}
	existing, found, err := loadActivationEvidenceTx(ctx, tx, request.RunID, request.ActivationID)
	if err != nil {
		return domain.Artifact{}, false, err
	}
	if found {
		if existing.ID != request.Artifact.ID || existing.SHA256 != request.Artifact.SHA256 ||
			existing.AttemptID != request.Artifact.AttemptID || existing.TaskID != request.Artifact.TaskID {
			return domain.Artifact{}, false, fmt.Errorf("%w: activation %s retained %s/%s, proposed %s/%s",
				ErrActivationEvidenceConflict, request.ActivationID,
				existing.ID, existing.SHA256, request.Artifact.ID, request.Artifact.SHA256)
		}
		if err := tx.Commit(); err != nil {
			return domain.Artifact{}, false, err
		}
		return existing, false, nil
	}
	if run.GraphRevision != request.GraphRevision {
		return domain.Artifact{}, false, fmt.Errorf("%w: run graph revision is %d, snapshot names %d",
			ErrActivationEvidenceConflict, run.GraphRevision, request.GraphRevision)
	}
	raw, err := json.Marshal(request.Artifact)
	if err != nil {
		return domain.Artifact{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO coordinator_artifacts(
		id, workflow_run_id, task_id, attempt_id, sha256, record
	) VALUES (?, ?, ?, ?, ?, ?)`,
		request.Artifact.ID, request.Artifact.WorkflowRunID, request.Artifact.TaskID,
		request.Artifact.AttemptID, request.Artifact.SHA256, string(raw)); err != nil {
		return domain.Artifact{}, false, fmt.Errorf("insert activation evidence %q: %w", request.Artifact.ID, err)
	}
	if _, err := insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
		ID:   "activation-evidence:" + request.ActivationID,
		Kind: "activation-evidence-frozen", WorkflowRunID: request.RunID,
		AttemptID: request.ActivationID, TargetType: domain.AuditTargetArtifact,
		TargetID: request.Artifact.ID, Actor: "coordinator",
		Reason:    "immutable supervision evidence frozen before dispatch",
		CreatedAt: request.Artifact.CreatedAt,
		Detail: nativeAuditDetail{
			CoordinatorEpoch:    request.CoordinatorEpoch,
			Revision:            request.GraphRevision,
			IdempotencyIdentity: request.ActivationID,
			Outcome:             "frozen",
		},
	}); err != nil {
		return domain.Artifact{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Artifact{}, false, err
	}
	return request.Artifact, true, nil
}

func requireActivationEvidenceAttemptTx(ctx context.Context, tx *sql.Tx, request ActivationEvidencePublication) error {
	var runID, taskID string
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT workflow_run_id, task_id, record
		FROM coordinator_attempts WHERE id = ?`, request.Artifact.AttemptID).Scan(&runID, &taskID, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: activation attempt %q does not exist",
			ErrActivationEvidenceConflict, request.Artifact.AttemptID)
	}
	if err != nil {
		return fmt.Errorf("load activation evidence attempt %q: %w", request.Artifact.AttemptID, err)
	}
	var attempt domain.Attempt
	if err := json.Unmarshal(raw, &attempt); err != nil {
		return fmt.Errorf("decode activation evidence attempt %q: %w", request.Artifact.AttemptID, err)
	}
	if runID != request.RunID || attempt.WorkflowRunID != request.RunID ||
		taskID != request.Artifact.TaskID || attempt.TaskID != request.Artifact.TaskID ||
		attempt.ID != request.Artifact.AttemptID ||
		attempt.SupervisionActivationID != request.ActivationID ||
		attempt.SupervisionActivationEpoch != request.ActivationEpoch ||
		!attempt.IsSupervisionActivation() {
		return fmt.Errorf("%w: artifact task/attempt is not the live supervision activation attempt",
			ErrActivationEvidenceConflict)
	}
	return nil
}

func loadActivationEvidenceTx(ctx context.Context, tx *sql.Tx, runID, activationID string) (domain.Artifact, bool, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT record FROM coordinator_artifacts
		WHERE workflow_run_id = ?
		  AND json_extract(record, '$.producer') = 'coordinator/supervision'
		  AND json_extract(record, '$.name') = ?
		ORDER BY id LIMIT 1`, runID, "supervision-evidence-"+activationID+".json").Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Artifact{}, false, nil
	}
	if err != nil {
		return domain.Artifact{}, false, err
	}
	var artifact domain.Artifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		return domain.Artifact{}, false, err
	}
	if artifact.Producer != "coordinator/supervision" {
		return domain.Artifact{}, false, fmt.Errorf("%w: activation identity is occupied by producer %q",
			ErrActivationEvidenceConflict, artifact.Producer)
	}
	return artifact, true, nil
}
