package domain

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

// SinkTaskName is reserved for the coordinator-created terminal task.
const SinkTaskName = "__sink"

// SinkTask is a run-local task, stored with its owning run rather than in the
// immutable workflow's executable task definitions. It deliberately cannot name
// a worker, route or attempt.
type SinkTask struct {
	ID            string        `json:"id"`
	Name          string        `json:"name"`
	GraphRevision int64         `json:"graphRevision"`
	Needs         []string      `json:"needs"`
	Progress      ProgressState `json:"progress"`
	Result        *SinkResult   `json:"result,omitempty"`
	CompletedAt   *time.Time    `json:"completedAt,omitempty"`
}

type SinkResult struct {
	FailedTaskIDs    []string `json:"failedTaskIds"`
	CancelledTaskIDs []string `json:"cancelledTaskIds"`
	SkippedTaskIDs   []string `json:"skippedTaskIds"`
}

func SinkTaskID(runID string) string { return "sink:" + runID }

// RunExecutionsQuiescent includes old attempts: a retry never erases custody of
// an earlier execution whose stop is still unproven.
func RunExecutionsQuiescent(runID string, attempts []Attempt, assignments []Assignment) bool {
	byID := make(map[string]Assignment, len(assignments))
	owned := make(map[string]bool, len(attempts))
	for _, assignment := range assignments {
		byID[assignment.ID] = assignment
	}
	for _, attempt := range attempts {
		if attempt.WorkflowRunID != runID {
			continue
		}
		owned[attempt.ID] = true
		if attempt.Control != "" && attempt.Control != ControlStopped && attempt.Control != ControlUnassigned {
			return false
		}
		if attempt.AssignmentID != "" {
			assignment, ok := byID[attempt.AssignmentID]
			if !ok || assignment.AttemptID != attempt.ID ||
				(assignment.State != AssignmentCompleted && assignment.State != AssignmentReleased) {
				return false
			}
		}
	}
	for _, assignment := range assignments {
		if owned[assignment.AttemptID] && assignment.State != AssignmentCompleted && assignment.State != AssignmentReleased {
			return false
		}
	}
	return true
}

func CloneSink(sink *SinkTask) *SinkTask {
	if sink == nil {
		return nil
	}
	copy := *sink
	copy.Needs = append([]string{}, sink.Needs...)
	if sink.Result != nil {
		result := *sink.Result
		result.FailedTaskIDs = append([]string{}, result.FailedTaskIDs...)
		result.CancelledTaskIDs = append([]string{}, result.CancelledTaskIDs...)
		result.SkippedTaskIDs = append([]string{}, result.SkippedTaskIDs...)
		copy.Result = &result
	}
	if sink.CompletedAt != nil {
		value := *sink.CompletedAt
		copy.CompletedAt = &value
	}
	return &copy
}

// BindRunSink creates or rebinds the implicit terminal task for a graph revision.
// Callers commit this result with the graph change. It never changes a final sink.
func BindRunSink(run WorkflowRun, tasks []Task) (WorkflowRun, error) {
	if run.ID == "" || run.WorkflowID == "" {
		return run, errors.New("sink requires run and workflow identity")
	}
	if run.GraphRevision == 0 {
		run.GraphRevision = 1
	}
	if run.GraphRevision < 1 {
		return run, errors.New("sink requires a positive graph revision")
	}
	needs := make([]string, 0, len(tasks))
	names := make(map[string]bool, len(tasks))
	for _, task := range tasks {
		if task.WorkflowID != run.WorkflowID || task.ID == "" || task.Name == "" || task.Name == SinkTaskName || task.ID == SinkTaskID(run.ID) {
			return run, fmt.Errorf("invalid or reserved sink predecessor %q", task.ID)
		}
		if names[task.Name] {
			return run, fmt.Errorf("duplicate sink predecessor name %q", task.Name)
		}
		names[task.Name] = true
		needs = append(needs, task.ID)
	}
	slices.Sort(needs)
	if len(slices.Compact(append([]string(nil), needs...))) != len(needs) {
		return run, errors.New("duplicate sink predecessor ID")
	}
	previous := run.Sink
	if previous != nil {
		if previous.ID != SinkTaskID(run.ID) || previous.Name != SinkTaskName || previous.GraphRevision < 1 {
			return run, errors.New("sink identity is inconsistent with its run")
		}
		if run.GraphRevision < previous.GraphRevision {
			return run, errors.New("sink graph revision moved backwards")
		}
		changed := !slices.Equal(needs, previous.Needs)
		if changed && run.GraphRevision == previous.GraphRevision {
			return run, errors.New("sink dependencies changed without a graph revision")
		}
		if previous.Progress.Terminal() {
			if run.GraphRevision != previous.GraphRevision || changed {
				return run, errors.New("completed sink is immutable; submit a new run")
			}
			run.Sink = CloneSink(previous)
			return run, nil
		}
	}
	run.Sink = &SinkTask{ID: SinkTaskID(run.ID), Name: SinkTaskName, GraphRevision: run.GraphRevision, Needs: needs, Progress: ProgressBlocked}
	return run, nil
}

// ProjectRunSink aggregates current task outcomes only after every execution in
// the run is proven quiescent. Missing assignment evidence is not containment.
// Failed earlier attempts do not poison a task that subsequently succeeded.
func ProjectRunSink(run WorkflowRun, tasks []Task, attempts []Attempt, assignments []Assignment, now time.Time) (WorkflowRun, error) {
	run, err := BindRunSink(run, tasks)
	if err != nil || run.Sink.Progress.Terminal() {
		return run, err
	}
	if !RunExecutionsQuiescent(run.ID, attempts, assignments) {
		return run, nil
	}
	current := make(map[string]Attempt, len(tasks))
	for _, attempt := range attempts {
		if attempt.WorkflowRunID != run.ID {
			continue
		}
		if prior, ok := current[attempt.TaskID]; !ok || prior.Number < attempt.Number {
			current[attempt.TaskID] = attempt
		}
	}
	result := &SinkResult{FailedTaskIDs: []string{}, CancelledTaskIDs: []string{}, SkippedTaskIDs: []string{}}
	allSucceeded := true
	for _, id := range run.Sink.Needs {
		attempt, ok := current[id]
		if !ok || !attempt.Progress.Terminal() {
			return run, nil
		}
		switch attempt.Progress {
		case ProgressFailed:
			result.FailedTaskIDs = append(result.FailedTaskIDs, id)
		case ProgressCancelled:
			result.CancelledTaskIDs = append(result.CancelledTaskIDs, id)
		case ProgressSkipped:
			result.SkippedTaskIDs = append(result.SkippedTaskIDs, id)
		}
		allSucceeded = allSucceeded && attempt.Progress == ProgressSucceeded
	}
	if now.IsZero() {
		return run, errors.New("sink settlement requires a timestamp")
	}
	completed := now.UTC()
	run.Sink.Progress = ProgressSucceeded
	if len(result.FailedTaskIDs) > 0 {
		run.Sink.Progress = ProgressFailed
	}
	run.Sink.Result = result
	run.Sink.CompletedAt = &completed
	switch {
	case len(result.FailedTaskIDs) > 0:
		run.Progress = ProgressFailed
	case len(result.CancelledTaskIDs) > 0:
		run.Progress = ProgressCancelled
	case allSucceeded:
		run.Progress = ProgressSucceeded
	default:
		run.Progress = ProgressSkipped
	}
	run.CompletedAt = &completed
	return run, nil
}
