package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// validateCollectorResumeIntentTx closes custody and stop-observation races
// between source grant issuance and durable intent commit. Exact replay is
// handled before this function and never dispatches a second native effect.
func validateCollectorResumeIntentTx(ctx context.Context, tx *sql.Tx, record domain.ThrottleAttemptRecord, now time.Time) error {
	command := record.Command
	grant := command.QuotaRecovery
	if grant == nil || command.Kind != domain.ThrottleCommandResume || record.Delivery != domain.ThrottleDeliveryPending {
		return nil
	}
	if grant.CoverageDigest == "" || grant.QuotaPoolID != command.QuotaPoolID || grant.AccountID == "" ||
		!grant.ExpiresAt.After(now) || command.CreatedAt.After(now) || !grant.Provenance.AcquiredAt.After(grant.StoppedAt) {
		return fmt.Errorf("collector recovery authority expired or malformed before commit")
	}
	quota, err := scanJSONRows[domain.BucketState](ctx, tx, "SELECT state FROM buckets ORDER BY key")
	if err != nil {
		return err
	}
	origins, err := loadAdminWorkerSnapshotsTx(ctx, tx)
	if err != nil {
		return err
	}
	if grant.EvidenceDigest == "" || grant.EvidenceDigest != domain.QuotaRecoveryEvidenceDigest(grant.AccountID, grant.EvidenceScope, quota, origins) {
		return fmt.Errorf("collector recovery source evidence changed before intent commit")
	}
	var attemptRaw, assignmentRaw, snapshotRaw []byte
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_attempts WHERE id = ?", record.AttemptID).Scan(&attemptRaw); err != nil {
		return err
	}
	var attempt domain.Attempt
	if err := json.Unmarshal(attemptRaw, &attempt); err != nil {
		return err
	}
	if attempt.Progress.Terminal() || attempt.AssignmentID != command.AssignmentID ||
		(attempt.Control != domain.ControlPaused && attempt.Control != domain.ControlPausedUncheckpointed) {
		return fmt.Errorf("collector recovery attempt no longer paused under this custody")
	}
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_assignments WHERE id = ?", command.AssignmentID).Scan(&assignmentRaw); err != nil {
		return err
	}
	var assignment domain.Assignment
	if err := json.Unmarshal(assignmentRaw, &assignment); err != nil {
		return err
	}
	if assignment.AttemptID != attempt.ID || assignment.WorkerID != command.WorkerID || assignment.Epoch != command.AssignmentEpoch ||
		assignment.State != domain.AssignmentClaimed || !assignment.LeaseExpiresAt.After(now) ||
		assignment.ThreadID != command.ThreadID || !reflect.DeepEqual(assignment.Route, command.Route) {
		return fmt.Errorf("collector recovery assignment lease or placement changed before commit")
	}
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_worker_snapshots WHERE worker_id = ?", command.WorkerID).Scan(&snapshotRaw); err != nil {
		return err
	}
	var snapshot domain.WorkerSnapshot
	if err := json.Unmarshal(snapshotRaw, &snapshot); err != nil {
		return err
	}
	var epoch int64
	if err := tx.QueryRowContext(ctx, "SELECT epoch FROM coordinator_runtime WHERE id = 1").Scan(&epoch); err != nil {
		return err
	}
	if snapshot.WorkerEpoch != assignment.WorkerEpoch || snapshot.CoordinatorEpoch != epoch || !snapshot.Connected || !snapshot.ValidUntil.After(now) {
		return fmt.Errorf("collector recovery worker observation expired or changed before commit")
	}
	matched := false
	for _, observation := range snapshot.Assignments {
		if observation.AssignmentID == assignment.ID && observation.AssignmentEpoch == assignment.Epoch &&
			observation.ThreadID == command.ThreadID && observation.WorkspacePath == command.WorkspacePath && observation.Control == domain.ControlPaused {
			if grant.StopCommandID != "" {
				matched = true
			} else if stop := observation.QuotaStop; stop != nil &&
				stop.ObservedSequence == grant.StopObservedSequence && stop.ObservedSequence > 0 && stop.ObservedSequence <= snapshot.Sequence && stop.StoppedAt.Equal(grant.StoppedAt) {
				matched = true
			}
		}
	}
	if !matched {
		return fmt.Errorf("collector recovery durable stop observation changed before commit")
	}
	if grant.StopCommandID != "" {
		var priorRaw []byte
		if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_throttle_attempts WHERE directive_id = ? AND attempt_id = ?", record.DirectiveID, record.AttemptID).Scan(&priorRaw); err != nil {
			return err
		}
		var prior domain.ThrottleAttemptRecord
		if err := json.Unmarshal(priorRaw, &prior); err != nil {
			return err
		}
		history, err := scanJSONRows[domain.ThrottleAttemptRecord](ctx, tx, "SELECT record FROM coordinator_throttle_attempts ORDER BY directive_id, attempt_id")
		if err != nil {
			return err
		}
		if domain.QuotaStopSuperseded(prior, history) {
			return fmt.Errorf("collector recovery stop was superseded before commit")
		}
		stop, accepted := prior.AcceptedQuotaStop()
		_, proposedAccepted := record.AcceptedQuotaStop()
		if !accepted || !proposedAccepted || stop.Command.ID != grant.StopCommandID || !stop.AcknowledgedAt.Equal(grant.StoppedAt) ||
			!reflect.DeepEqual(stop.Command.BucketEpochs, command.BucketEpochs) || !reflect.DeepEqual(record.StopReceipt, stop) {
			return fmt.Errorf("collector recovery stop acknowledgement changed before commit")
		}
	}
	return nil
}
