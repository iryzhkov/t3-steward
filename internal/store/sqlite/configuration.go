package sqlite

import (
	"context"
	"time"
)

func (s *Store) RecordCoordinatorConfiguration(ctx context.Context, epoch int64, digest string, at time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := requireCoordinatorEpoch(ctx, tx, epoch); err != nil {
		return err
	}
	if _, err := insertNativeAuditEventTx(ctx, tx, nativeAuditInput{ID: "configuration:" + digest + ":" + at.UTC().Format(time.RFC3339Nano), Kind: "configuration-applied", TargetType: "configuration", TargetID: digest, Actor: "coordinator", Reason: "validated effective configuration activated", CreatedAt: at, Detail: nativeAuditDetail{IdempotencyIdentity: digest + ":" + at.UTC().Format(time.RFC3339Nano), Revision: epoch, Outcome: digest}}); err != nil {
		return err
	}
	return tx.Commit()
}
