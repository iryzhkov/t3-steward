package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// V20 is a table-free compatibility fence: recovery-v1 is encoded in existing
// JSON records, and older binaries must refuse a store that may contain it.
const coordinatorMigrationV20 = `SELECT 1;`

type RecoveryIncidentRequest struct {
	RunID, IncidentID, EventID, SourceTaskID, SourceAttemptID, Reason string
	ExpectedGraphRevision, SourceAttemptRevision                      int64
	RecoveryConfig                                                    domain.RecoveryConfig
	Recovery                                                          domain.RecoveryIncident
	EventRecord                                                       json.RawMessage
	OpenedAt                                                          time.Time
}

func (s *Store) OpenRecoveryIncident(ctx context.Context, request RecoveryIncidentRequest) (SupervisionDecision, bool, error) {
	if strings.TrimSpace(request.RunID) == "" || strings.TrimSpace(request.IncidentID) == "" ||
		strings.TrimSpace(request.EventID) == "" || strings.TrimSpace(request.SourceTaskID) == "" ||
		strings.TrimSpace(request.SourceAttemptID) == "" || strings.TrimSpace(request.Reason) == "" || len(request.EventRecord) == 0 {
		return SupervisionDecision{}, false, errors.New("a recovery incident needs run, incident, event, task, attempt, reason and event record")
	}
	if request.ExpectedGraphRevision < 1 || request.SourceAttemptRevision < 1 || request.RecoveryConfig.Validate() != nil {
		return SupervisionDecision{}, false, errors.New("a recovery incident needs graph, source-attempt and recovery-config fences")
	}
	if request.Recovery.Contract != domain.RecoveryContractV1 || request.Recovery.Purpose != domain.RecoveryActivationRepair ||
		request.Recovery.Owner.Role != domain.RecoveryRoleRepairExecutor || request.Recovery.State != domain.RecoveryPendingDispatch ||
		request.Recovery.NextAction != domain.RecoveryDispatchRepair {
		return SupervisionDecision{}, false, errors.New("a recovery incident needs the recovery-v1 repair-executor pending-dispatch contract")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SupervisionDecision{}, false, fmt.Errorf("begin recovery incident: %w", err)
	}
	defer tx.Rollback()
	if err := validateRecoverySourceTx(ctx, tx, request); err != nil {
		return SupervisionDecision{}, false, err
	}
	var raw []byte
	err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_supervision_incidents WHERE id = ? AND run_id = ?", request.IncidentID, request.RunID).Scan(&raw)
	if err == nil {
		var existing domain.ReviewIncident
		if err := json.Unmarshal(raw, &existing); err != nil {
			return SupervisionDecision{}, false, err
		}
		if existing.SourceEventID != request.EventID || existing.SourceTaskID != request.SourceTaskID || existing.SourceAttemptID != request.SourceAttemptID || existing.Recovery == nil ||
			existing.Recovery.Diagnostic.FailureFingerprint != request.Recovery.Diagnostic.FailureFingerprint ||
			existing.Recovery.Diagnostic.EvidenceFingerprint != request.Recovery.Diagnostic.EvidenceFingerprint {
			return SupervisionDecision{}, false, fmt.Errorf("%w: recovery incident %q changed identity", ErrSupervisionRequestConflict, request.IncidentID)
		}
		written, err := appendRecoveryInboxTx(ctx, tx, request)
		if err != nil {
			return SupervisionDecision{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return SupervisionDecision{}, false, err
		}
		return SupervisionDecision{Incident: &existing}, written, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return SupervisionDecision{}, false, err
	}
	openedAt := request.OpenedAt.UTC()
	if openedAt.IsZero() {
		openedAt = s.now().UTC()
	}
	incident := domain.ReviewIncident{
		ID: request.IncidentID, RunID: request.RunID, SourceEventID: request.EventID,
		SourceTaskID: request.SourceTaskID, SourceAttemptID: request.SourceAttemptID,
		Revision: 1, RequiredDisposition: domain.DispositionOperatorAction, State: domain.IncidentOpen,
		Reason: request.Reason, OpenedAt: openedAt, Recovery: &request.Recovery,
	}
	if err := saveSupervisionIncidentTx(ctx, tx, incident); err != nil {
		return SupervisionDecision{}, false, err
	}
	if _, err := appendRecoveryInboxTx(ctx, tx, request); err != nil {
		return SupervisionDecision{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return SupervisionDecision{}, false, fmt.Errorf("commit recovery incident: %w", err)
	}
	return SupervisionDecision{Incident: &incident}, true, nil
}

func validateRecoverySourceTx(ctx context.Context, tx *sql.Tx, request RecoveryIncidentRequest) error {
	var runRaw, supervisionRaw, attemptRaw []byte
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_workflow_runs WHERE id = ?", request.RunID).Scan(&runRaw); err != nil {
		return fmt.Errorf("%w: recovery run %q is unavailable", ErrSupervisionRequestConflict, request.RunID)
	}
	var run domain.WorkflowRun
	if json.Unmarshal(runRaw, &run) != nil || run.ID != request.RunID || run.GraphRevision != request.ExpectedGraphRevision || run.Progress.Terminal() {
		return fmt.Errorf("%w: recovery run %q changed", ErrSupervisionRequestConflict, request.RunID)
	}
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_supervision WHERE run_id = ?", request.RunID).Scan(&supervisionRaw); err != nil {
		return fmt.Errorf("%w: recovery contract for run %q is unavailable", ErrSupervisionRequestConflict, request.RunID)
	}
	var supervision domain.SupervisionRecord
	if json.Unmarshal(supervisionRaw, &supervision) != nil || supervision.Config.Recovery == nil ||
		!reflect.DeepEqual(*supervision.Config.Recovery, request.RecoveryConfig) {
		return fmt.Errorf("%w: recovery contract for run %q changed", ErrSupervisionRequestConflict, request.RunID)
	}
	if err := tx.QueryRowContext(ctx, `SELECT record FROM coordinator_attempts
		WHERE workflow_run_id = ? AND task_id = ? ORDER BY number DESC LIMIT 1`,
		request.RunID, request.SourceTaskID).Scan(&attemptRaw); err != nil {
		return fmt.Errorf("%w: latest recovery attempt is unavailable", ErrSupervisionRequestConflict)
	}
	var attempt domain.Attempt
	if json.Unmarshal(attemptRaw, &attempt) != nil || attempt.ID != request.SourceAttemptID ||
		attempt.Revision != request.SourceAttemptRevision || attempt.Progress != domain.ProgressFailed {
		return fmt.Errorf("%w: recovery source attempt changed", ErrSupervisionRequestConflict)
	}
	return nil
}

func appendRecoveryInboxTx(ctx context.Context, tx *sql.Tx, request RecoveryIncidentRequest) (bool, error) {
	var existingRun string
	var existingRecord []byte
	err := tx.QueryRowContext(ctx, "SELECT run_id, record FROM coordinator_supervision_inbox WHERE id = ?", request.EventID).Scan(&existingRun, &existingRecord)
	if err == nil {
		if existingRun != request.RunID || !recoveryEventEqual(existingRecord, request.EventRecord) {
			return false, fmt.Errorf("%w: recovery event %q changed identity", ErrSupervisionRequestConflict, request.EventID)
		}
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	var high sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT MAX(sequence) FROM coordinator_supervision_inbox WHERE run_id = ?", request.RunID).Scan(&high); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO coordinator_supervision_inbox(id, run_id, sequence, consumed, record) VALUES (?, ?, ?, 0, ?)", request.EventID, request.RunID, high.Int64+1, []byte(request.EventRecord)); err != nil {
		return false, fmt.Errorf("append recovery event %q: %w", request.EventID, err)
	}
	return true, nil
}

func recoveryEventEqual(left, right []byte) bool {
	var a, b any
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil {
		return string(left) == string(right)
	}
	deleteVolatileRecoveryEventFields(a)
	deleteVolatileRecoveryEventFields(b)
	leftCanonical, _ := json.Marshal(a)
	rightCanonical, _ := json.Marshal(b)
	return string(leftCanonical) == string(rightCanonical)
}

func deleteVolatileRecoveryEventFields(value any) {
	object, ok := value.(map[string]any)
	if !ok {
		return
	}
	delete(object, "occurredAt")
	delete(object, "sequence")
}
