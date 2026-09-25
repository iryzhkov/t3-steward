package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// CheckpointImportRejection names a worker checkpoint upload that the
// coordinator discards because it can never be imported. The run, task and
// attempt are empty when the assignment the upload names is unknown.
type CheckpointImportRejection struct {
	ManifestID       string
	ArtifactID       string
	WorkerID         string
	WorkerEpoch      string
	AssignmentID     string
	AssignmentEpoch  int64
	WorkflowRunID    string
	TaskID           string
	AttemptID        string
	CoordinatorEpoch int64
	Reason           string
	RejectedAt       time.Time
}

// CheckpointImportRejectionEventID is the idempotent audit identity of a
// rejected upload. A worker may offer the same upload again when its
// acknowledgement was lost, and that replay must not add a second event.
func CheckpointImportRejectionEventID(manifestID string) string {
	return "checkpoint-import-rejected:" + manifestID
}

// RecordCheckpointImportRejection writes the audit event for a discarded
// checkpoint upload, so that `backlog events` and diagnose show why a
// checkpoint the worker took never reached the coordinator. The discard used
// to be visible only as an error line in the coordinator's journal. A replay
// of the same upload returns the event already recorded, whatever its time
// or reason, because the first rejection is the one that decided its fate.
func (s *Store) RecordCheckpointImportRejection(ctx context.Context, rejection CheckpointImportRejection) (domain.AuditEvent, error) {
	if rejection.ManifestID == "" || rejection.ArtifactID == "" || rejection.WorkerID == "" || rejection.RejectedAt.IsZero() {
		return domain.AuditEvent{}, errors.New("checkpoint import rejection requires manifest, artifact, worker and time")
	}
	reason := strings.TrimSpace(rejection.Reason)
	if reason == "" {
		reason = "checkpoint import rejected"
	}
	id := CheckpointImportRejectionEventID(rejection.ManifestID)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.AuditEvent{}, fmt.Errorf("begin checkpoint import rejection: %w", err)
	}
	defer tx.Rollback()
	event, found, err := loadAuditEventTx(ctx, tx, id)
	if err != nil {
		return domain.AuditEvent{}, err
	}
	if !found {
		event, err = insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
			ID: id, Kind: "checkpoint-import-rejected",
			WorkflowRunID: rejection.WorkflowRunID, TaskID: rejection.TaskID, AttemptID: rejection.AttemptID,
			TargetType: domain.AuditTargetArtifact, TargetID: rejection.ArtifactID,
			Actor: "coordinator", Reason: reason, CreatedAt: rejection.RejectedAt.UTC(),
			Detail: nativeAuditDetail{
				CoordinatorEpoch: rejection.CoordinatorEpoch, WorkerEpoch: rejection.WorkerEpoch,
				AssignmentEpoch:     rejection.AssignmentEpoch,
				IdempotencyIdentity: rejection.ManifestID, Outcome: "discarded",
			},
		})
		if err != nil {
			return domain.AuditEvent{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return domain.AuditEvent{}, fmt.Errorf("commit checkpoint import rejection: %w", err)
	}
	return event, nil
}
