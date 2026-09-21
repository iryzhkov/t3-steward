package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ErrExecutorCapacity reports an authoritative store refusal to overbook a
// configured executor pool.
var (
	ErrExecutorCapacity         = errors.New("executor capacity exhausted")
	ErrExecutorCapacityEvidence = errors.New("executor capacity evidence unavailable")
)

func requireExecutorSlotTx(ctx context.Context, tx *sql.Tx, workerID string, now time.Time) error {
	snapshot, exists, err := loadWorkerSnapshotTx(ctx, tx, workerID)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w for worker %q: no worker snapshot", ErrExecutorCapacityEvidence, workerID)
	}
	if snapshot.Inventory.Allocatable.ExecutorSlots < 1 {
		// An explicit zero preserves historical ungoverned behavior.
		return nil
	}
	if now.IsZero() || !snapshot.ValidUntil.After(now.UTC()) {
		return fmt.Errorf("%w for worker %q: snapshot is stale", ErrExecutorCapacityEvidence, workerID)
	}
	assignments, err := loadJSON[domain.Assignment](ctx, tx, "coordinator_assignments")
	if err != nil {
		return err
	}
	used := 0
	for _, assignment := range assignments {
		if assignment.WorkerID != workerID {
			continue
		}
		attempt, err := loadAttemptTx(ctx, tx, assignment.AttemptID)
		if err != nil {
			return err
		}
		if domain.AssignmentOwnsExecutorCapacity(attempt, assignment) {
			used++
		}
	}
	if used >= snapshot.Inventory.Allocatable.ExecutorSlots {
		return fmt.Errorf("%w on worker %q: %d of %d slots reserved",
			ErrExecutorCapacity, workerID, used, snapshot.Inventory.Allocatable.ExecutorSlots)
	}
	return nil
}

// ExecutorSlotAvailable is the read-only early-placement view. Transactional
// commit checks remain authoritative.
func (s *Store) ExecutorSlotAvailable(ctx context.Context, workerID string, now time.Time) (bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	err = requireExecutorSlotTx(ctx, tx, workerID, now)
	if errors.Is(err, ErrExecutorCapacity) {
		return false, nil
	}
	return err == nil, err
}
