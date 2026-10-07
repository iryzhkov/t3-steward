package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// recoverQuotaAdmissionSubmissionTx recognizes the durable admission receipt as
// the authoritative acceptance decision. The files were published before that
// records transaction, so replay must preserve them and all subsequent progress.
func recoverQuotaAdmissionSubmissionTx(ctx context.Context, tx *sql.Tx, record domain.SubmissionRecord) (domain.SubmissionRecord, error) {
	if record.RegisterOnly || record.RunID == "" {
		return record, nil
	}
	var raw string
	err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_workflow_runs WHERE id=? AND workflow_id=?", record.RunID, record.WorkflowID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return record, nil
	}
	if err != nil {
		return record, fmt.Errorf("load admitted submission run: %w", err)
	}
	var run domain.WorkflowRun
	if err := json.Unmarshal([]byte(raw), &run); err != nil {
		return record, err
	}
	if run.QuotaAdmission == nil {
		return record, nil
	}
	if run.QuotaAdmission.AdmittedAt.IsZero() {
		return record, fmt.Errorf("admitted submission run has no admission time")
	}
	return completePendingSubmissionTx(ctx, tx, record, run.QuotaAdmission.AdmittedAt)
}

// QuotaAdmissionSnapshot is the same merged quota truth used by quota waits,
// together with durable pending demand read in the admission transaction.
type QuotaAdmissionSnapshot struct {
	Records    CoordinatorRecords
	States     []domain.BucketState
	StaleAfter time.Duration
}

// QuotaAdmissionGate must perform only pure calculations: the store connection
// is held by the transaction, so calling store methods from a gate deadlocks.
type QuotaAdmissionGate func(QuotaAdmissionSnapshot) (*domain.QuotaAdmissionReceipt, error)

func (s *Store) quotaAdmissionSnapshotTx(ctx context.Context, tx *sql.Tx) (QuotaAdmissionSnapshot, error) {
	records, err := nodeStateRecordsTx(ctx, tx, s.quotaStaleAfter)
	if err != nil {
		return QuotaAdmissionSnapshot{}, err
	}
	records.Tasks = domain.TasksWithGraphAdditions(records.WorkflowRuns, records.Tasks)
	return QuotaAdmissionSnapshot{Records: records.CoordinatorRecords, States: records.Buckets, StaleAfter: records.QuotaStaleAfter}, nil
}

// CheckQuotaAdmission runs the inexpensive pre-stage check. Its success is not
// authorization to write: SaveAdmittedCoordinatorRecords must check again.
func (s *Store) CheckQuotaAdmission(ctx context.Context, gate QuotaAdmissionGate) (*domain.QuotaAdmissionReceipt, error) {
	if gate == nil {
		return nil, fmt.Errorf("quota admission gate is required")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin quota admission check: %w", err)
	}
	defer tx.Rollback()
	snapshot, err := s.quotaAdmissionSnapshotTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	receipt, err := gate(snapshot)
	if err != nil {
		return receipt, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit quota admission check: %w", err)
	}
	return receipt, nil
}

// SaveAdmittedCoordinatorRecords serializes the decisive gate with insertion.
// The store's single connection ensures another submission observes this
// commit's demand before it can pass its gate.
func (s *Store) SaveAdmittedCoordinatorRecords(ctx context.Context, records CoordinatorRecords, gate QuotaAdmissionGate) error {
	if gate == nil {
		return fmt.Errorf("quota admission gate is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin admitted coordinator save: %w", err)
	}
	defer tx.Rollback()
	snapshot, err := s.quotaAdmissionSnapshotTx(ctx, tx)
	if err != nil {
		return err
	}
	receipt, err := gate(snapshot)
	if err != nil {
		return err
	}
	if receipt == nil {
		return fmt.Errorf("quota admission gate returned no receipt")
	}
	// Do not mutate caller-owned slices: a failed save must not publish a receipt.
	records.WorkflowRuns = append([]domain.WorkflowRun(nil), records.WorkflowRuns...)
	for i := range records.WorkflowRuns {
		records.WorkflowRuns[i].QuotaAdmission = receipt
	}
	if err := s.saveCoordinatorRecordsTx(ctx, tx, records); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit admitted coordinator records: %w", err)
	}
	return nil
}
