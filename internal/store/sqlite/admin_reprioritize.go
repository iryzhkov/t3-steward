package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// applyRunReprioritizeTx derives the entire selection from one transactional
// projection. It never projects lifecycle, admission, supervision or sink state.
func applyRunReprioritizeTx(ctx context.Context, tx *sql.Tx, command domain.AdminCommand, now time.Time) (*domain.ReprioritizeReceipt, string, error) {
	importance, err := domain.DecodeReprioritizePayload(command.Payload)
	if err != nil {
		return nil, err.Error(), nil
	}
	snapshot, err := loadWorkflowProjectionTx(ctx, tx, command.TargetID)
	if err != nil {
		return nil, "", err
	}
	run := snapshot.Run
	if run.Progress.Terminal() || run.CompletedAt != nil || run.Sink != nil && run.Sink.Progress.Terminal() {
		return nil, "run is final; submit a new workflow", nil
	}
	failure, err := refuseCommandOnClosedRunTx(ctx, tx, run.ID, "")
	if err != nil || failure != "" {
		return nil, failure, err
	}
	if run.Supervision != nil && run.Supervision.ClosedByCancel != nil {
		return nil, "run supervision is closed; submit a new workflow", nil
	}
	receipt := &domain.ReprioritizeReceipt{Importance: importance, Changed: []domain.ReprioritizeTaskRecord{}, Skipped: []domain.ReprioritizeTaskRecord{}}
	sort.Slice(snapshot.Tasks, func(i, j int) bool { return snapshot.Tasks[i].ID < snapshot.Tasks[j].ID })
	for _, task := range snapshot.Tasks {
		var current *domain.Attempt
		history := false
		for i := range snapshot.Attempts {
			attempt := &snapshot.Attempts[i]
			if attempt.WorkflowRunID != run.ID || attempt.TaskID != task.ID {
				continue
			}
			if current == nil || attempt.Number > current.Number || attempt.Number == current.Number && attempt.ID > current.ID {
				current = attempt
			}
			if attempt.StartedAt != nil || attempt.ThreadID != "" || attempt.AssignmentID != "" {
				history = true
			}
			for _, assignment := range snapshot.Assignments {
				if assignment.AttemptID == attempt.ID {
					history = true
				}
			}
		}
		record := domain.ReprioritizeTaskRecord{TaskID: task.ID, TaskName: task.Name}
		record.Reason = reprioritizeSkipReason(current, history)
		if record.Reason != "" {
			receipt.Skipped = append(receipt.Skipped, record)
			continue
		}
		next := *current
		next.AdminImportance = &importance
		next.Revision++
		next.UpdatedAt = now.UTC()
		if err := updateAdminAttemptTx(ctx, tx, next, current.Revision); err != nil {
			return nil, "", err
		}
		receipt.Changed = append(receipt.Changed, record)
	}
	run.Revision++
	run.UpdatedAt = now.UTC()
	if err := updateAdminWorkflowRunTx(ctx, tx, run); err != nil {
		return nil, "", fmt.Errorf("reprioritize run: %w", err)
	}
	return receipt, "", nil
}

// Fixed precedence: missing, any historical execution/assignment, retry,
// terminal, control, progress. Delays and holds remain unchanged; an otherwise
// eligible delayed ready/blocked attempt receives only the override.
func reprioritizeSkipReason(current *domain.Attempt, history bool) string {
	if current == nil {
		return "missing-attempt"
	}
	if history {
		return "history"
	}
	if current.Number != 1 || current.AutomaticRetry != nil {
		return "retry"
	}
	if current.Progress.Terminal() || current.CompletedAt != nil {
		return "terminal"
	}
	if current.Control != domain.ControlUnassigned {
		return "control"
	}
	if current.Progress != domain.ProgressReady && current.Progress != domain.ProgressBlocked {
		return "progress"
	}
	return ""
}
