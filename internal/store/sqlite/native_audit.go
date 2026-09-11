package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// nativeAuditDetail is deliberately an allowlist. Capability tokens,
// credentials, paths, arbitrary worker detail, and complete domain records must
// never be placed in production audit events.
type nativeAuditDetail struct {
	CoordinatorEpoch    int64  `json:"coordinatorEpoch,omitempty"`
	WorkerEpoch         string `json:"workerEpoch,omitempty"`
	AssignmentEpoch     int64  `json:"assignmentEpoch,omitempty"`
	WorkerSequence      int64  `json:"workerSequence,omitempty"`
	ExpectedRevision    int64  `json:"expectedRevision,omitempty"`
	Revision            int64  `json:"revision,omitempty"`
	IdempotencyIdentity string `json:"idempotencyIdentity"`
	Outcome             string `json:"outcome"`
}

type nativeAuditInput struct {
	ID, Kind                         string
	WorkflowRunID, TaskID, AttemptID string
	TargetType                       domain.AdminTargetType
	TargetID, Actor, Reason          string
	CreatedAt                        time.Time
	Detail                           nativeAuditDetail
}

func insertNativeAuditEventTx(ctx context.Context, tx *sql.Tx, input nativeAuditInput) (domain.AuditEvent, error) {
	for label, value := range map[string]string{
		"id": input.ID, "kind": input.Kind, "target id": input.TargetID,
		"actor": input.Actor, "reason": input.Reason,
		"idempotency identity": input.Detail.IdempotencyIdentity, "outcome": input.Detail.Outcome,
	} {
		if value == "" || strings.TrimSpace(value) != value {
			return domain.AuditEvent{}, fmt.Errorf("native audit %s must be nonempty and trimmed", label)
		}
	}
	if input.TargetType == "" || input.CreatedAt.IsZero() {
		return domain.AuditEvent{}, fmt.Errorf("native audit target type and time are required")
	}
	detail, err := json.Marshal(input.Detail)
	if err != nil {
		return domain.AuditEvent{}, fmt.Errorf("encode native audit detail: %w", err)
	}
	return insertAuditEventTx(ctx, tx, domain.AuditEvent{
		ID: input.ID, Kind: input.Kind, WorkflowRunID: input.WorkflowRunID,
		TaskID: input.TaskID, AttemptID: input.AttemptID,
		TargetType: input.TargetType, TargetID: input.TargetID,
		Actor: input.Actor, Reason: input.Reason, CreatedAt: input.CreatedAt, Detail: detail,
	})
}

func assignmentAuditContext(ctx context.Context, tx *sql.Tx, assignment domain.Assignment) (domain.Attempt, error) {
	attempt, err := loadAttemptTx(ctx, tx, assignment.AttemptID)
	if err != nil {
		return domain.Attempt{}, fmt.Errorf("load assignment %q audit context: %w", assignment.ID, err)
	}
	return attempt, nil
}

func requireNativeAuditEventTx(ctx context.Context, tx *sql.Tx, id string) error {
	if _, found, err := loadAuditEventTx(ctx, tx, id); err != nil {
		return err
	} else if !found {
		return fmt.Errorf("production transition is missing native audit event %q", id)
	}
	return nil
}
