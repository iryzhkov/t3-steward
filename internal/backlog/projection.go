package backlog

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// ProjectionStore is the persistence needed to publish DAG progress.
type ProjectionStore interface {
	LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
	SaveCoordinatorRecords(context.Context, sqlite.CoordinatorRecords) error
}

// ProjectionReport lists what one projection pass changed.
type ProjectionReport struct {
	Runs     []string
	Attempts []string
}

// ProjectWorkflowRuns publishes the derived DAG state of every nonterminal
// workflow run: dependents of a succeeded task become ready, and the run
// progress follows its tasks to a terminal state. Turn outcomes only update
// the finished attempt itself, so without this pass the durable run stays
// "queued" forever and operators cannot tell finished work from stalled work.
func ProjectWorkflowRuns(ctx context.Context, store ProjectionStore, now time.Time) (ProjectionReport, error) {
	var report ProjectionReport
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return report, fmt.Errorf("load records for projection: %w", err)
	}
	tasksByWorkflow := make(map[string][]domain.Task)
	for _, task := range records.Tasks {
		tasksByWorkflow[task.WorkflowID] = append(tasksByWorkflow[task.WorkflowID], task)
	}
	attemptsByRun := make(map[string][]domain.Attempt)
	attemptByID := make(map[string]domain.Attempt, len(records.Attempts))
	for _, attempt := range records.Attempts {
		attemptsByRun[attempt.WorkflowRunID] = append(attemptsByRun[attempt.WorkflowRunID], attempt)
		attemptByID[attempt.ID] = attempt
	}
	var changedRuns []domain.WorkflowRun
	var changedAttempts []domain.Attempt
	for _, run := range records.WorkflowRuns {
		if run.Progress.Terminal() {
			continue
		}
		execution, err := NewDAGExecution(DAGState{
			Run: run, Tasks: tasksByWorkflow[run.WorkflowID], Attempts: attemptsByRun[run.ID],
		})
		if err != nil {
			slog.Warn("workflow run projection skipped", "run", run.ID, "error", err)
			continue
		}
		state := execution.Snapshot()
		for _, projected := range state.Attempts {
			current, ok := attemptByID[projected.ID]
			if !ok || current.AssignmentID != "" || current.Progress.Terminal() {
				continue
			}
			if current.Progress == projected.Progress && current.Failure == projected.Failure {
				continue
			}
			current.Progress = projected.Progress
			current.Failure = projected.Failure
			current.Revision++
			current.UpdatedAt = now
			changedAttempts = append(changedAttempts, current)
			report.Attempts = append(report.Attempts, current.ID)
		}
		if state.Run.Progress == run.Progress {
			continue
		}
		run.Progress = state.Run.Progress
		run.Revision++
		run.UpdatedAt = now
		if run.Progress.Terminal() {
			completed := now
			run.CompletedAt = &completed
		} else {
			run.CompletedAt = nil
		}
		changedRuns = append(changedRuns, run)
		report.Runs = append(report.Runs, run.ID)
	}
	if len(changedRuns) == 0 && len(changedAttempts) == 0 {
		return report, nil
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: changedRuns, Attempts: changedAttempts}); err != nil {
		return report, fmt.Errorf("persist workflow projection: %w", err)
	}
	return report, nil
}
