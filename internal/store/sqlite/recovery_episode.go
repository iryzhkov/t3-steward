package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type RecoveryAttemptFailureRequest struct {
	RunID, IncidentID, EventID, AttemptID, Reason                    string
	ExpectedIncidentRevision, ExpectedGraphRevision, AttemptRevision int64
	FailureFingerprint, EvidenceFingerprint                          string
	EventRecord                                                      json.RawMessage
	ObservedAt                                                       time.Time
}

// RecoveryEpisodeForAttempt resolves a retry attempt through its immutable
// supplement. The original failed attempt is resolved through the incident.
func (s *Store) RecoveryEpisodeForAttempt(ctx context.Context, runID, attemptID string) (domain.ReviewIncident, bool, error) {
	var incidentID string
	err := s.db.QueryRowContext(ctx, `SELECT incident_id FROM coordinator_recovery_supplements
		WHERE run_id = ? AND attempt_id = ?`, runID, attemptID).Scan(&incidentID)
	if errors.Is(err, sql.ErrNoRows) {
		rows, queryErr := s.db.QueryContext(ctx, "SELECT record FROM coordinator_supervision_incidents WHERE run_id = ?", runID)
		if queryErr != nil {
			return domain.ReviewIncident{}, false, queryErr
		}
		defer rows.Close()
		for rows.Next() {
			var raw []byte
			var incident domain.ReviewIncident
			if rows.Scan(&raw) == nil && json.Unmarshal(raw, &incident) == nil && incident.Recovery != nil &&
				(incident.SourceAttemptID == attemptID || incident.Recovery.CurrentAttemptID == attemptID) {
				return incident, true, nil
			}
		}
		return domain.ReviewIncident{}, false, rows.Err()
	}
	if err != nil {
		return domain.ReviewIncident{}, false, err
	}
	var raw []byte
	if err := s.db.QueryRowContext(ctx,
		"SELECT record FROM coordinator_supervision_incidents WHERE id = ? AND run_id = ?", incidentID, runID).Scan(&raw); err != nil {
		return domain.ReviewIncident{}, false, err
	}
	var incident domain.ReviewIncident
	if err := json.Unmarshal(raw, &incident); err != nil {
		return domain.ReviewIncident{}, false, err
	}
	return incident, true, nil
}

// RecordRecoveryAttemptFailure advances an existing episode after its current
// retry fails. Root evidence, owner, deadline and budget remain unchanged.
func (s *Store) RecordRecoveryAttemptFailure(ctx context.Context, request RecoveryAttemptFailureRequest) (domain.ReviewIncident, error) {
	if request.RunID == "" || request.IncidentID == "" || request.EventID == "" || request.AttemptID == "" ||
		request.Reason == "" || request.ExpectedIncidentRevision < 1 || request.ExpectedGraphRevision < 1 ||
		request.AttemptRevision < 1 || request.FailureFingerprint == "" || request.EvidenceFingerprint == "" ||
		len(request.EventRecord) == 0 {
		return domain.ReviewIncident{}, errors.New("recovery failure observation is incomplete")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.ReviewIncident{}, err
	}
	defer tx.Rollback()
	var incidentRaw, runRaw, attemptRaw []byte
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_supervision_incidents WHERE id = ? AND run_id = ?",
		request.IncidentID, request.RunID).Scan(&incidentRaw); err != nil {
		return domain.ReviewIncident{}, err
	}
	var incident domain.ReviewIncident
	if json.Unmarshal(incidentRaw, &incident) != nil || incident.Recovery == nil ||
		incident.Revision != request.ExpectedIncidentRevision || incident.Recovery.CurrentAttemptID != request.AttemptID {
		return domain.ReviewIncident{}, fmt.Errorf("%w: recovery episode changed", ErrSupervisionRequestConflict)
	}
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_workflow_runs WHERE id = ?", request.RunID).Scan(&runRaw); err != nil {
		return domain.ReviewIncident{}, err
	}
	var run domain.WorkflowRun
	if json.Unmarshal(runRaw, &run) != nil || run.GraphRevision != request.ExpectedGraphRevision || run.Progress.Terminal() {
		return domain.ReviewIncident{}, fmt.Errorf("%w: recovery run changed", ErrSupervisionRequestConflict)
	}
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_attempts WHERE id = ?", request.AttemptID).Scan(&attemptRaw); err != nil {
		return domain.ReviewIncident{}, err
	}
	var attempt domain.Attempt
	if json.Unmarshal(attemptRaw, &attempt) != nil || attempt.WorkflowRunID != request.RunID ||
		attempt.Revision != request.AttemptRevision || attempt.Progress != domain.ProgressFailed {
		return domain.ReviewIncident{}, fmt.Errorf("%w: recovery attempt changed", ErrSupervisionRequestConflict)
	}
	var latestID string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM coordinator_attempts WHERE workflow_run_id = ? AND task_id = ?
		ORDER BY number DESC LIMIT 1`, request.RunID, attempt.TaskID).Scan(&latestID); err != nil || latestID != attempt.ID {
		return domain.ReviewIncident{}, fmt.Errorf("%w: recovery attempt is no longer latest", ErrSupervisionRequestConflict)
	}

	now := s.now().UTC()
	incident.Revision++
	incident.Recovery.Diagnostic.FailureFingerprint = request.FailureFingerprint
	incident.Recovery.Diagnostic.EvidenceFingerprint = request.EvidenceFingerprint
	incident.Recovery.LastProgressAt = now
	exhausted := incident.Recovery.AttemptsUsed >= incident.Recovery.AttemptBudget || !now.Before(incident.Recovery.Deadline.UTC())
	if exhausted {
		incident.Recovery.State = domain.RecoveryNeedsHuman
		incident.Recovery.NextAction = domain.RecoveryEscalateHuman
		incident.Recovery.ExhaustionReason = fmt.Sprintf("recovery episode exhausted after %d of %d attempts: %s",
			incident.Recovery.AttemptsUsed, incident.Recovery.AttemptBudget, strings.TrimSpace(request.Reason))
		if err := appendRecoveryExhaustionOutboxTx(ctx, tx, incident, request.EventID, now); err != nil {
			return domain.ReviewIncident{}, err
		}
	} else {
		incident.Recovery.State = domain.RecoveryPendingDispatch
		incident.Recovery.NextAction = domain.RecoveryDispatchRepair
		if err := appendRecoveryEpisodeEventTx(ctx, tx, request.RunID, request.EventID, request.EventRecord); err != nil {
			return domain.ReviewIncident{}, err
		}
	}
	if err := saveSupervisionIncidentTx(ctx, tx, incident); err != nil {
		return domain.ReviewIncident{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.ReviewIncident{}, err
	}
	return incident, nil
}

type RecoveryAttemptSuccessRequest struct {
	RunID, IncidentID, AttemptID                    string
	ExpectedIncidentRevision, ExpectedGraphRevision int64
	AttemptRevision                                 int64
}

// ResolveRecoveryEpisode closes only the recovery incident owned by the latest
// successful retry. Any independent gate-review incidents remain untouched.
func (s *Store) ResolveRecoveryEpisode(ctx context.Context, request RecoveryAttemptSuccessRequest) (domain.ReviewIncident, error) {
	if request.RunID == "" || request.IncidentID == "" || request.AttemptID == "" ||
		request.ExpectedIncidentRevision < 1 || request.ExpectedGraphRevision < 1 || request.AttemptRevision < 1 {
		return domain.ReviewIncident{}, errors.New("recovery success observation is incomplete")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.ReviewIncident{}, err
	}
	defer tx.Rollback()
	var incidentRaw, runRaw, attemptRaw []byte
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_supervision_incidents WHERE id = ? AND run_id = ?",
		request.IncidentID, request.RunID).Scan(&incidentRaw); err != nil {
		return domain.ReviewIncident{}, err
	}
	var incident domain.ReviewIncident
	if json.Unmarshal(incidentRaw, &incident) != nil || incident.Recovery == nil ||
		incident.Revision != request.ExpectedIncidentRevision || incident.Recovery.CurrentAttemptID != request.AttemptID {
		return domain.ReviewIncident{}, fmt.Errorf("%w: recovery episode changed", ErrSupervisionRequestConflict)
	}
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_workflow_runs WHERE id = ?", request.RunID).Scan(&runRaw); err != nil {
		return domain.ReviewIncident{}, err
	}
	var run domain.WorkflowRun
	if json.Unmarshal(runRaw, &run) != nil || run.GraphRevision != request.ExpectedGraphRevision || run.Progress.Terminal() {
		return domain.ReviewIncident{}, fmt.Errorf("%w: recovery run changed", ErrSupervisionRequestConflict)
	}
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_attempts WHERE id = ?", request.AttemptID).Scan(&attemptRaw); err != nil {
		return domain.ReviewIncident{}, err
	}
	var attempt domain.Attempt
	if json.Unmarshal(attemptRaw, &attempt) != nil || attempt.WorkflowRunID != request.RunID ||
		attempt.Revision != request.AttemptRevision || attempt.Progress != domain.ProgressSucceeded {
		return domain.ReviewIncident{}, fmt.Errorf("%w: recovery attempt changed", ErrSupervisionRequestConflict)
	}
	var latestID string
	if err := tx.QueryRowContext(ctx, "SELECT id FROM coordinator_attempts WHERE workflow_run_id = ? AND task_id = ? ORDER BY number DESC LIMIT 1",
		request.RunID, attempt.TaskID).Scan(&latestID); err != nil || latestID != attempt.ID {
		return domain.ReviewIncident{}, fmt.Errorf("%w: recovery attempt is no longer latest", ErrSupervisionRequestConflict)
	}
	if incident.Recovery.State == domain.RecoveryResolved && incident.State == domain.IncidentResolved {
		return incident, nil
	}
	if incident.Recovery.State == domain.RecoveryNeedsHuman || incident.State != domain.IncidentOpen {
		return domain.ReviewIncident{}, fmt.Errorf("%w: recovery episode is not resolvable", ErrSupervisionRequestConflict)
	}
	incident.Revision++
	incident.State = domain.IncidentResolved
	incident.Recovery.State = domain.RecoveryResolved
	incident.Recovery.NextAction = domain.RecoveryNoAction
	incident.Recovery.LastProgressAt = s.now().UTC()
	if err := saveSupervisionIncidentTx(ctx, tx, incident); err != nil {
		return domain.ReviewIncident{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.ReviewIncident{}, err
	}
	return incident, nil
}

func appendRecoveryEpisodeEventTx(ctx context.Context, tx *sql.Tx, runID, eventID string, record []byte) error {
	var existingRun string
	var existing []byte
	err := tx.QueryRowContext(ctx, "SELECT run_id, record FROM coordinator_supervision_inbox WHERE id = ?", eventID).Scan(&existingRun, &existing)
	if err == nil {
		if existingRun != runID || !recoveryEventEqual(existing, record) {
			return fmt.Errorf("%w: recovery event %q changed identity", ErrSupervisionRequestConflict, eventID)
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var high sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT MAX(sequence) FROM coordinator_supervision_inbox WHERE run_id = ?", runID).Scan(&high); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO coordinator_supervision_inbox(id, run_id, sequence, consumed, record) VALUES (?, ?, ?, 0, ?)",
		eventID, runID, high.Int64+1, record)
	return err
}

func appendRecoveryExhaustionOutboxTx(ctx context.Context, tx *sql.Tx, incident domain.ReviewIncident, eventID string, now time.Time) error {
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_supervision WHERE run_id = ?", incident.RunID).Scan(&raw); err != nil {
		return err
	}
	var supervision domain.SupervisionRecord
	if err := json.Unmarshal(raw, &supervision); err != nil {
		return err
	}
	threadID := strings.TrimSpace(supervision.Config.Escalation.ThreadID)
	if !supervision.Config.Escalation.NotifyThread || threadID == "" {
		return nil
	}
	sum := sha256.Sum256([]byte("supervision-escalation\x00" + incident.RunID + "\x00" + incident.ID))
	id := fmt.Sprintf("supervision-escalation-%x", sum[:16])
	entry := map[string]any{
		"id": id, "runId": incident.RunID, "kind": "escalation", "dedupeKey": incident.ID,
		"incidentId": incident.ID, "epoch": supervision.ActivationEpoch, "threadId": threadID,
		"reason": incident.Recovery.ExhaustionReason, "eventIds": []string{eventID},
		"delivery": "pending", "createdAt": now,
	}
	entryRaw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO coordinator_supervision_outbox(id, run_id, activation_id, delivery_state, record)
		VALUES (?, ?, '', 'pending', ?) ON CONFLICT(id) DO NOTHING`, id, incident.RunID, entryRaw)
	return err
}
