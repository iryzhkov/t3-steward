package backlog

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// RepairStore is the persistence needed by the startup repair pass.
type RepairStore interface {
	LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
	SaveCoordinatorRecords(context.Context, sqlite.CoordinatorRecords) error
}

// RepairReport lists the durable rows the startup pass corrected.
type RepairReport struct {
	ThreadIdentitiesAllocated   []string
	OrphanedAssignmentsReleased []string
}

// RepairCoordinatorState fixes rows written by earlier binaries that the
// current invariants would otherwise reject on every cycle:
//
//   - an offered assignment without a deterministic thread identity gets one;
//   - an offered or claimed assignment row that its attempt no longer
//     references (the attempt was recovered or released elsewhere) is marked
//     released so the attempt can be planned again.
//
// Unknown assignments are never touched: they resolve through fresh worker
// observations or explicit recovery.
func RepairCoordinatorState(ctx context.Context, store RepairStore, now time.Time) (RepairReport, error) {
	var report RepairReport
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return report, fmt.Errorf("load records for repair: %w", err)
	}
	attemptByID := make(map[string]domain.Attempt, len(records.Attempts))
	for _, attempt := range records.Attempts {
		attemptByID[attempt.ID] = attempt
	}
	var repaired []domain.Assignment
	for _, assignment := range records.Assignments {
		changed := false
		if assignment.State == domain.AssignmentOffered && assignment.ThreadID == "" {
			if assignment.Epoch <= 1 {
				assignment.ThreadID = stableCoordinatorID("thread", assignment.ID)
			} else {
				assignment.ThreadID = fmt.Sprintf("thread-%s-e%d", assignment.ID, assignment.Epoch)
			}
			report.ThreadIdentitiesAllocated = append(report.ThreadIdentitiesAllocated, assignment.ID)
			changed = true
		}
		if assignment.State == domain.AssignmentOffered || assignment.State == domain.AssignmentClaimed {
			attempt, ok := attemptByID[assignment.AttemptID]
			// An offer for an attempt that already ended (cancelled or skipped
			// while it waited) can never be claimed; release it so the worker
			// stops being offered work the coordinator will reject.
			if ok && assignment.State == domain.AssignmentOffered && attempt.Progress.Terminal() {
				ok = false
			}
			if !ok || attempt.AssignmentID != assignment.ID {
				assignment.State = domain.AssignmentReleased
				assignment.LeaseExpiresAt = time.Time{}
				report.OrphanedAssignmentsReleased = append(report.OrphanedAssignmentsReleased, assignment.ID)
				changed = true
			}
		}
		if changed {
			assignment.UpdatedAt = now
			repaired = append(repaired, assignment)
		}
	}
	if len(repaired) == 0 {
		return report, nil
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: repaired}); err != nil {
		return report, fmt.Errorf("persist repaired assignments: %w", err)
	}
	slog.Info("coordinator state repaired",
		"thread_identities", len(report.ThreadIdentitiesAllocated),
		"orphaned_assignments_released", len(report.OrphanedAssignmentsReleased))
	return report, nil
}
