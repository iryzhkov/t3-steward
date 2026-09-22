package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const coordinatorMigrationV21 = `
CREATE TABLE IF NOT EXISTS coordinator_recovery_supplements (
	operation_id TEXT PRIMARY KEY,
	run_id TEXT NOT NULL,
	incident_id TEXT NOT NULL,
	attempt_id TEXT NOT NULL UNIQUE,
	record TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS coordinator_recovery_supplements_incident
	ON coordinator_recovery_supplements(run_id, incident_id);
`

func (s *Store) CommitRecoveryRetry(ctx context.Context, request domain.RecoveryRetryRequest) (domain.RecoveryRetryReceipt, error) {
	if strings.TrimSpace(request.OperationID) == "" || strings.TrimSpace(request.RunID) == "" ||
		strings.TrimSpace(request.IncidentID) == "" || request.ExpectedIncidentRevision < 1 || request.GraphRevision < 1 ||
		strings.TrimSpace(request.ActivationID) == "" || request.ActivationEpoch < 1 ||
		strings.TrimSpace(request.Principal) == "" || strings.TrimSpace(request.SourceAttemptID) == "" ||
		request.SourceAttemptRevision < 1 || request.RequestedAt.IsZero() ||
		strings.TrimSpace(request.InstructionArtifact.ArtifactID) == "" || strings.TrimSpace(request.InstructionArtifact.Digest) == "" ||
		strings.TrimSpace(request.Diagnostic.FailureFingerprint) == "" ||
		strings.TrimSpace(request.Diagnostic.EvidenceFingerprint) == "" ||
		strings.TrimSpace(request.Diagnostic.StrategyFingerprint) == "" {
		return domain.RecoveryRetryReceipt{}, errors.New("recovery retry request is incomplete")
	}
	digest, err := supervisionPayloadDigest(request)
	if err != nil {
		return domain.RecoveryRetryReceipt{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.RecoveryRetryReceipt{}, err
	}
	defer tx.Rollback()
	if raw, found, err := lookupSupervisionReceiptTx(ctx, tx, "recovery-retry", request.OperationID, digest); err != nil {
		return domain.RecoveryRetryReceipt{}, err
	} else if found {
		var receipt domain.RecoveryRetryReceipt
		if err := json.Unmarshal(raw, &receipt); err != nil {
			return domain.RecoveryRetryReceipt{}, err
		}
		return receipt, tx.Commit()
	}

	incident, err := loadRecoveryRetryIncidentTx(ctx, tx, request)
	if err != nil {
		return domain.RecoveryRetryReceipt{}, err
	}
	now := s.now().UTC()
	if incident.Recovery.AttemptsUsed >= incident.Recovery.AttemptBudget {
		return domain.RecoveryRetryReceipt{}, errors.New("recovery attempt budget exhausted")
	}
	if !now.Before(incident.Recovery.Deadline.UTC()) {
		return domain.RecoveryRetryReceipt{}, errors.New("recovery incident deadline reached")
	}
	canonicalStrategy := domain.RecoveryStrategyFingerprint(request.InstructionArtifact, request.CheckpointArtifacts)
	if request.Diagnostic.StrategyFingerprint != canonicalStrategy {
		return domain.RecoveryRetryReceipt{}, errors.New("recovery strategy fingerprint is not bound to retained instruction content")
	}
	if request.Diagnostic.FailureFingerprint != incident.Recovery.Diagnostic.FailureFingerprint ||
		request.Diagnostic.EvidenceFingerprint != incident.Recovery.Diagnostic.EvidenceFingerprint {
		return domain.RecoveryRetryReceipt{}, errors.New("recovery diagnosis is not bound to the current durable failure evidence")
	}
	if canonicalStrategy == incident.Recovery.Diagnostic.StrategyFingerprint {
		return domain.RecoveryRetryReceipt{}, errors.New("recovery retry needs substantively changed retained strategy content")
	}
	if err := authorizeRecoveryRetryTx(ctx, tx, request, now); err != nil {
		return domain.RecoveryRetryReceipt{}, err
	}
	source, err := loadAttemptTx(ctx, tx, request.SourceAttemptID)
	if err != nil {
		return domain.RecoveryRetryReceipt{}, err
	}
	currentAttemptID := incident.Recovery.CurrentAttemptID
	if currentAttemptID == "" {
		currentAttemptID = incident.SourceAttemptID
	}
	if source.WorkflowRunID != request.RunID || source.ID != currentAttemptID ||
		source.Revision != request.SourceAttemptRevision || source.Progress != domain.ProgressFailed {
		return domain.RecoveryRetryReceipt{}, fmt.Errorf("%w: recovery source attempt changed", ErrSupervisionRequestConflict)
	}
	var latestID string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM coordinator_attempts
		WHERE workflow_run_id = ? AND task_id = ? ORDER BY number DESC LIMIT 1`, request.RunID, source.TaskID).Scan(&latestID); err != nil || latestID != source.ID {
		return domain.RecoveryRetryReceipt{}, fmt.Errorf("%w: recovery source is no longer latest", ErrSupervisionRequestConflict)
	}
	if err := requireRecoveryArtifactTx(ctx, tx, request.RunID, request.InstructionArtifact); err != nil {
		return domain.RecoveryRetryReceipt{}, err
	}
	for _, artifact := range request.CheckpointArtifacts {
		if err := requireRecoveryArtifactTx(ctx, tx, request.RunID, artifact); err != nil {
			return domain.RecoveryRetryReceipt{}, err
		}
	}
	attemptID := "attempt:recovery:" + digest[:24]
	progress, err := recoveryRetryProgressTx(ctx, tx, request.RunID, source.TaskID)
	if err != nil {
		return domain.RecoveryRetryReceipt{}, err
	}
	retry := domain.Attempt{ID: attemptID, WorkflowRunID: request.RunID, TaskID: source.TaskID, Number: source.Number + 1,
		Progress: progress, Control: domain.ControlUnassigned, Revision: 1, UpdatedAt: now}
	if err := insertAdminAttemptTx(ctx, tx, retry); err != nil {
		return domain.RecoveryRetryReceipt{}, err
	}
	var runRaw []byte
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_workflow_runs WHERE id = ?", request.RunID).Scan(&runRaw); err != nil {
		return domain.RecoveryRetryReceipt{}, err
	}
	var run domain.WorkflowRun
	if err := json.Unmarshal(runRaw, &run); err != nil {
		return domain.RecoveryRetryReceipt{}, err
	}
	if run.Progress.Terminal() {
		return domain.RecoveryRetryReceipt{}, errors.New("recovery retry cannot reopen a terminal run")
	}
	run.Progress, run.Revision, run.UpdatedAt, run.CompletedAt = domain.ProgressQueued, run.Revision+1, now, nil
	if err := updateAdminWorkflowRunTx(ctx, tx, run); err != nil {
		return domain.RecoveryRetryReceipt{}, err
	}
	supplement := domain.RepairAttemptSupplement{OperationID: request.OperationID, IncidentID: request.IncidentID,
		SourceAttemptID: source.ID, AttemptID: attemptID, InstructionArtifact: request.InstructionArtifact,
		CheckpointArtifacts: request.CheckpointArtifacts, Diagnostic: request.Diagnostic, CreatedAt: now}
	supplementRaw, _ := json.Marshal(supplement)
	if _, err := tx.ExecContext(ctx, `INSERT INTO coordinator_recovery_supplements(operation_id, run_id, incident_id, attempt_id, record)
		VALUES (?, ?, ?, ?, ?)`, request.OperationID, request.RunID, request.IncidentID, attemptID, supplementRaw); err != nil {
		return domain.RecoveryRetryReceipt{}, err
	}
	incident.Revision++
	incident.Recovery.AttemptsUsed++
	incident.Recovery.Diagnostic = request.Diagnostic
	incident.Recovery.CurrentAttemptID = attemptID
	incident.Recovery.State = domain.RecoveryRecovering
	incident.Recovery.NextAction = domain.RecoveryNoAction
	incident.Recovery.LastProgressAt = now
	// Accepting the custody-backed proposal is the repair activation's durable
	// decision. Acknowledge the immutable failure event it repaired in this same
	// transaction, so activation lifecycle reconciliation cannot later classify
	// the successful repair turn as an empty decision and escalate the incident.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO coordinator_supervision_inbox_ack(event_id, run_id, purpose, acknowledged_at)
		SELECT id, run_id, ?, ? FROM coordinator_supervision_inbox
		WHERE run_id = ? AND json_extract(record, '$.incidentId') = ?
			AND json_extract(record, '$.attemptId') = ?
		ON CONFLICT(event_id, purpose) DO NOTHING`,
		string(domain.RecoveryActivationRepair), now.Format(time.RFC3339Nano), request.RunID, request.IncidentID, request.SourceAttemptID); err != nil {
		return domain.RecoveryRetryReceipt{}, fmt.Errorf("acknowledge repaired recovery event: %w", err)
	}
	if err := saveSupervisionIncidentTx(ctx, tx, incident); err != nil {
		return domain.RecoveryRetryReceipt{}, err
	}
	receipt := domain.RecoveryRetryReceipt{OperationID: request.OperationID, IncidentID: request.IncidentID,
		AttemptID: attemptID, AttemptNumber: retry.Number, CommittedAt: now}
	if err := recordSupervisionReceiptTx(ctx, tx, "recovery-retry", request.RunID, request.OperationID, digest, receipt, now); err != nil {
		return domain.RecoveryRetryReceipt{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.RecoveryRetryReceipt{}, err
	}
	return receipt, nil
}

func loadRecoveryRetryIncidentTx(ctx context.Context, tx *sql.Tx, request domain.RecoveryRetryRequest) (domain.ReviewIncident, error) {
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_supervision_incidents WHERE id = ? AND run_id = ?", request.IncidentID, request.RunID).Scan(&raw); err != nil {
		return domain.ReviewIncident{}, err
	}
	var incident domain.ReviewIncident
	if err := json.Unmarshal(raw, &incident); err != nil {
		return domain.ReviewIncident{}, err
	}
	if incident.Revision != request.ExpectedIncidentRevision || incident.State != domain.IncidentOpen || incident.Recovery == nil ||
		incident.Recovery.Contract != domain.RecoveryContractV1 || incident.Recovery.Owner.Role != domain.RecoveryRoleRepairExecutor {
		return domain.ReviewIncident{}, fmt.Errorf("%w: recovery incident changed", ErrSupervisionRequestConflict)
	}
	return incident, nil
}

func authorizeRecoveryRetryTx(ctx context.Context, tx *sql.Tx, request domain.RecoveryRetryRequest, now time.Time) error {
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_supervision_activations WHERE id = ? AND run_id = ?",
		request.ActivationID, request.RunID).Scan(&raw); err != nil {
		return err
	}
	var activation domain.Activation
	if err := json.Unmarshal(raw, &activation); err != nil {
		return err
	}
	var supervisionRaw, runRaw []byte
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_supervision WHERE run_id = ?", request.RunID).Scan(&supervisionRaw); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_workflow_runs WHERE id = ?", request.RunID).Scan(&runRaw); err != nil {
		return err
	}
	var supervision domain.SupervisionRecord
	var run domain.WorkflowRun
	if json.Unmarshal(supervisionRaw, &supervision) != nil || json.Unmarshal(runRaw, &run) != nil {
		return errors.New("repair executor authority state is invalid")
	}
	recovery := supervision.Config.Recovery
	liveLease := activation.State == domain.ActivationActive && activation.LeaseToken != "" && activation.LeaseExpiresAt != nil &&
		now.Before(activation.LeaseExpiresAt.UTC()) && !domain.ActivationPastDeadline(activation, now)
	if activation.Purpose != domain.RecoveryActivationRepair || activation.IncidentID != request.IncidentID ||
		activation.ID != request.ActivationID || !liveLease || activation.GraphRevision != request.GraphRevision || activation.GraphRevision != run.GraphRevision ||
		activation.Principal != request.Principal || activation.Epoch != request.ActivationEpoch ||
		activation.Epoch != supervision.ActivationEpoch || recovery == nil ||
		activation.Principal == "" {
		return errors.New("repair executor authority is stale or out of scope")
	}
	return nil
}

func recoveryRetryProgressTx(ctx context.Context, tx *sql.Tx, runID, taskID string) (domain.ProgressState, error) {
	var taskRaw []byte
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_tasks WHERE id = ?", taskID).Scan(&taskRaw); err != nil {
		return "", err
	}
	var task domain.Task
	if err := json.Unmarshal(taskRaw, &task); err != nil {
		return "", err
	}
	for _, need := range task.Needs {
		var dependencyID string
		if err := tx.QueryRowContext(ctx, "SELECT id FROM coordinator_tasks WHERE workflow_id = ? AND name = ?", task.WorkflowID, need).Scan(&dependencyID); err != nil {
			return domain.ProgressBlocked, nil
		}
		var succeeded int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM coordinator_attempts WHERE workflow_run_id = ? AND task_id = ? AND json_extract(record, '$.progress') = ?",
			runID, dependencyID, string(domain.ProgressSucceeded)).Scan(&succeeded); err != nil {
			return "", err
		}
		if succeeded == 0 {
			return domain.ProgressBlocked, nil
		}
	}
	return domain.ProgressReady, nil
}

func requireRecoveryArtifactTx(ctx context.Context, tx *sql.Tx, runID string, digest domain.ArtifactDigest) error {
	var stored string
	err := tx.QueryRowContext(ctx, "SELECT sha256 FROM coordinator_artifacts WHERE id = ? AND workflow_run_id = ?",
		digest.ArtifactID, runID).Scan(&stored)
	if err != nil || stored != digest.Digest {
		return fmt.Errorf("recovery artifact %q is unavailable or changed", digest.ArtifactID)
	}
	return nil
}
