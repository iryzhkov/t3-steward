package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Executor capacity errors are sentinels so callers can distinguish a full
// governed pool from missing evidence.
var (
	ErrExecutorCapacity         = errors.New("executor capacity exhausted")
	ErrExecutorCapacityEvidence = errors.New("executor capacity evidence unavailable")
)

func requireExecutorCapacityTx(
	ctx context.Context,
	tx *sql.Tx,
	workerID string,
	now time.Time,
	expectedWorkerEpoch string,
	demand domain.ResourceDemand,
	demandKnown bool,
) error {
	snapshot, exists, err := loadWorkerSnapshotTx(ctx, tx, workerID)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w for worker %q: no worker snapshot", ErrExecutorCapacityEvidence, workerID)
	}
	if expectedWorkerEpoch != "" && snapshot.WorkerEpoch != expectedWorkerEpoch {
		return fmt.Errorf("%w for worker %q: snapshot epoch %q replaced assignment epoch %q",
			ErrExecutorCapacityEvidence, workerID, snapshot.WorkerEpoch, expectedWorkerEpoch)
	}
	if now.IsZero() || !snapshot.ValidUntil.After(now.UTC()) {
		return fmt.Errorf("%w for worker %q: snapshot is stale", ErrExecutorCapacityEvidence, workerID)
	}
	allocatable := snapshot.Inventory.Allocatable
	if allocatable.ExecutorSlots < 1 {
		// Fresh, explicit zero preserves historical ungoverned behavior.
		return nil
	}
	sizedGoverned := allocatable.CPUUnits > 0 || allocatable.MemoryMB > 0 || allocatable.ScratchMB > 0
	if !demandKnown && (sizedGoverned || snapshot.Inventory.CPUClass.Valid()) {
		return fmt.Errorf("%w for worker %q: assignment demand is not frozen", ErrExecutorCapacityEvidence, workerID)
	}
	if demandKnown {
		if err := demand.Validate(); err != nil {
			return fmt.Errorf("%w for worker %q: invalid assignment demand: %v", ErrExecutorCapacityEvidence, workerID, err)
		}
		if demand.MinCPUClass != "" &&
			(!snapshot.Inventory.CPUClass.Valid() || snapshot.Inventory.CPUClass.Compare(demand.MinCPUClass) < 0) {
			return fmt.Errorf("%w on worker %q: cpu class %q is below minimum %q",
				ErrExecutorCapacity, workerID, snapshot.Inventory.CPUClass, demand.MinCPUClass)
		}
	}

	assignments, err := loadJSON[domain.Assignment](ctx, tx, "coordinator_assignments")
	if err != nil {
		return err
	}
	usedSlots := 0
	var used domain.ResourceDemand
	for _, assignment := range assignments {
		if assignment.WorkerID != workerID {
			continue
		}
		attempt, err := loadAttemptTx(ctx, tx, assignment.AttemptID)
		if err != nil {
			return err
		}
		if !domain.AssignmentOwnsExecutorCapacity(attempt, assignment) {
			continue
		}
		usedSlots++
		ownerDemand, known := domain.AssignmentExecutorDemand(attempt, assignment)
		if !known {
			if sizedGoverned {
				return fmt.Errorf("%w for worker %q: live assignment %q has no frozen demand",
					ErrExecutorCapacityEvidence, workerID, assignment.ID)
			}
			continue
		}
		used.CPUUnits += ownerDemand.CPUUnits
		used.MemoryMB += ownerDemand.MemoryMB
		used.ScratchMB += ownerDemand.ScratchMB
	}
	switch {
	case usedSlots >= allocatable.ExecutorSlots:
		return fmt.Errorf("%w on worker %q: %d of %d slots reserved",
			ErrExecutorCapacity, workerID, usedSlots, allocatable.ExecutorSlots)
	case used.CPUUnits+demand.CPUUnits > allocatable.CPUUnits:
		return fmt.Errorf("%w on worker %q: cpu demand %v exceeds %v",
			ErrExecutorCapacity, workerID, used.CPUUnits+demand.CPUUnits, allocatable.CPUUnits)
	case used.MemoryMB+demand.MemoryMB > allocatable.MemoryMB:
		return fmt.Errorf("%w on worker %q: memory demand %d exceeds %d",
			ErrExecutorCapacity, workerID, used.MemoryMB+demand.MemoryMB, allocatable.MemoryMB)
	case used.ScratchMB+demand.ScratchMB > allocatable.ScratchMB:
		return fmt.Errorf("%w on worker %q: scratch demand %d exceeds %d",
			ErrExecutorCapacity, workerID, used.ScratchMB+demand.ScratchMB, allocatable.ScratchMB)
	}
	return nil
}

// ExecutorSlotAvailable is the read-only early-placement view for a slot-only
// activation. Transactional commit checks remain authoritative.
func (s *Store) ExecutorSlotAvailable(ctx context.Context, workerID string, now time.Time) (bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	err = requireExecutorCapacityTx(ctx, tx, workerID, now, "", domain.ResourceDemand{}, true)
	if errors.Is(err, ErrExecutorCapacity) {
		return false, nil
	}
	return err == nil, err
}
