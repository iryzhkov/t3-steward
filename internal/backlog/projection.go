package backlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// ProjectionStore publishes one run against its complete run-local read set.
type ProjectionStore interface {
	LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
	CommitWorkflowProjection(context.Context, sqlite.WorkflowProjectionSnapshot, domain.WorkflowRun, []domain.Attempt, time.Time) error
}

type ProjectionReport struct {
	Runs     []string
	Attempts []string
}

// ProjectWorkflowRuns advances dependencies and the coordinator-owned sink.
// It also backfills sink metadata on old imported runs. Quota planning is not a
// prerequisite: a stopped run can settle while an unrelated bucket is broken.
func ProjectWorkflowRuns(ctx context.Context, store ProjectionStore, now time.Time) (ProjectionReport, error) {
	var report ProjectionReport
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return report, fmt.Errorf("load records for projection: %w", err)
	}
	for _, run := range records.WorkflowRuns {
		if run.Sink != nil && run.Sink.Progress.Terminal() {
			continue
		}
		before := sqlite.WorkflowProjectionSnapshot{Run: run}
		owned := make(map[string]bool)
		for _, task := range records.Tasks {
			if task.WorkflowID == run.WorkflowID {
				before.Tasks = append(before.Tasks, task)
			}
		}
		for _, attempt := range records.Attempts {
			if attempt.WorkflowRunID == run.ID {
				before.Attempts = append(before.Attempts, attempt)
				owned[attempt.ID] = true
			}
		}
		for _, assignment := range records.Assignments {
			if owned[assignment.AttemptID] {
				before.Assignments = append(before.Assignments, assignment)
			}
		}
		external := ResolveExternalNodes(before.Tasks, records.WorkflowRuns, records.Tasks, records.Attempts, records.Assignments)
		for _, task := range before.Tasks {
			for _, ref := range task.ExternalNeeds {
				before.Dependencies = append(before.Dependencies, external[ref.String()])
			}
		}
		execution, err := NewDAGExecution(DAGState{Run: run, Tasks: before.Tasks, Attempts: before.Attempts, External: external})
		if err != nil {
			slog.Warn("workflow run projection skipped", "run", run.ID, "error", err)
			continue
		}
		state := execution.Snapshot()
		finalizeBlockedSinkPredecessors(&state, before.Assignments, now)
		projectedByID := make(map[string]domain.Attempt, len(state.Attempts))
		for _, attempt := range state.Attempts {
			projectedByID[attempt.ID] = attempt
		}
		updated := append([]domain.Attempt(nil), before.Attempts...)
		changedIDs := []string{}
		for index, current := range updated {
			projected := projectedByID[current.ID]
			if current.AssignmentID != "" || current.Progress.Terminal() ||
				(current.Progress == projected.Progress && current.Failure == projected.Failure) {
				continue
			}
			current.Progress = projected.Progress
			current.Failure = projected.Failure
			if current.Progress == domain.ProgressSkipped {
				current.Control = domain.ControlStopped
				completed := now.UTC()
				current.CompletedAt = &completed
			}
			current.Revision++
			current.UpdatedAt = now.UTC()
			updated[index] = current
			changedIDs = append(changedIDs, current.ID)
		}
		projected, err := domain.ProjectRunSink(run, before.Tasks, updated, before.Assignments, now)
		if err != nil {
			slog.Warn("workflow sink projection skipped", "run", run.ID, "error", err)
			continue
		}
		if !projected.Sink.Progress.Terminal() {
			projected.Progress = state.Run.Progress
			if projected.Progress.Terminal() {
				projected.Progress = domain.ProgressActive
			}
			projected.CompletedAt = nil
		}
		oldSink, _ := json.Marshal(run.Sink)
		newSink, _ := json.Marshal(projected.Sink)
		if len(changedIDs) == 0 && run.Progress == projected.Progress && run.GraphRevision == projected.GraphRevision &&
			string(oldSink) == string(newSink) && sameCompletionTime(run.CompletedAt, projected.CompletedAt) {
			continue
		}
		projected.Revision = run.Revision + 1
		projected.UpdatedAt = now.UTC()
		if err := store.CommitWorkflowProjection(ctx, before, projected, updated, now); err != nil {
			if errors.Is(err, sqlite.ErrStaleWorkflowProjection) {
				continue
			}
			return report, fmt.Errorf("persist workflow projection %q: %w", run.ID, err)
		}
		report.Runs = append(report.Runs, run.ID)
		report.Attempts = append(report.Attempts, changedIDs...)
	}
	return report, nil
}

func sameCompletionTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

// If no branch can still make progress, exhausted non-success dependencies
// terminalize their blocked descendants. While another branch is active/ready,
// keep blocked descendants intact so an accepted retry can still unblock them.
// The final skip and sink publication share the same optimistic transaction.
func finalizeBlockedSinkPredecessors(state *DAGState, assignments []domain.Assignment, now time.Time) {
	if !domain.RunExecutionsQuiescent(state.Run.ID, state.Attempts, assignments) {
		return
	}
	latest := make(map[string]int)
	for index, attempt := range state.Attempts {
		if prior, ok := latest[attempt.TaskID]; !ok || state.Attempts[prior].Number < attempt.Number {
			latest[attempt.TaskID] = index
		}
	}
	for _, index := range latest {
		attempt := state.Attempts[index]
		if !attempt.Progress.Terminal() && attempt.Progress != domain.ProgressBlocked {
			return
		}
	}
	byName := make(map[string]domain.Task)
	for _, task := range state.Tasks {
		byName[task.Name] = task
	}
	for changed := true; changed; {
		changed = false
		for _, task := range state.Tasks {
			index, ok := latest[task.ID]
			if !ok {
				continue
			}
			attempt := &state.Attempts[index]
			if attempt.Progress != domain.ProgressBlocked || attempt.AssignmentID != "" {
				continue
			}
			var failed []string
			for _, name := range task.Needs {
				dependency, ok := byName[name]
				if !ok {
					continue
				}
				dependencyIndex, ok := latest[dependency.ID]
				if !ok {
					continue
				}
				progress := state.Attempts[dependencyIndex].Progress
				if progress.Terminal() && progress != domain.ProgressSucceeded {
					failed = append(failed, name)
				}
			}
			for _, ref := range task.ExternalNeeds {
				if obs, ok := state.External[ref.String()]; ok && obs.ExitCode == 2 {
					failed = append(failed, ref.String())
				}
			}
			if len(failed) == 0 {
				continue
			}
			slices.Sort(failed)
			attempt.Progress = domain.ProgressSkipped
			attempt.Control = domain.ControlStopped
			attempt.Failure = fmt.Sprintf("terminal dependency prevented execution: %v", failed)
			attempt.CompletedAt = timePointer(now.UTC())
			changed = true
		}
	}
}
