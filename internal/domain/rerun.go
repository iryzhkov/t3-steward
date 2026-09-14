package domain

import (
	"errors"
	"fmt"
	"sort"
)

// RerunProvenance records what a run is a second attempt at. It is written
// once, with the new run's first graph definition, and it is the only link
// between the two runs: the source run itself is never touched, because a run
// that pretends it did not fail is a run nobody can learn from.
type RerunProvenance struct {
	SourceRunID     string `json:"sourceRunId"`
	SourceTaskID    string `json:"sourceTaskId"`
	SourceAttemptID string `json:"sourceAttemptId,omitempty"`
	IdempotencyKey  string `json:"idempotencyKey"`
	Reason          string `json:"reason,omitempty"`
}

// CarriedInput is one output artifact of a source run that a rerun carries
// over by reference.
//
// It exists because the producer is not a node of the new graph. An ordinary
// dependency input names a task in the same run and is resolved through it; a
// carried input names the producer only so that the file keeps arriving at
// .t3/dependencies/<producer>/<name>, which is where the task's prompt was
// written to expect it. ArtifactID names a reference artifact in the new run
// whose content address is the source artifact's, so no content is copied.
type CarriedInput struct {
	// Producer is the source producer's manifest task name, which is the
	// directory component the coordinator writes into the package.
	Producer string `json:"producer"`
	// ProducerTaskID is the source producer's durable task ID. The worker
	// names the dependency directory from it, so keeping the source value is
	// what makes the file land where it landed in the source run.
	ProducerTaskID string `json:"producerTaskId"`
	Name           string `json:"name"`
	ArtifactID     string `json:"artifactId"`
}

// RerunScope is the explicit division of a source run into the part a rerun
// repeats and the part it reuses. The named task and every descendant of it
// are rerun; everything else is reused, and a reused task that did not succeed
// is a refusal rather than a silent second failure.
type RerunScope struct {
	// From is the source task the rerun was asked to start from.
	From Task
	// FromAttempt is the last attempt of From in the source run, recorded as
	// provenance so the new run names the exact failure it answers.
	FromAttempt Attempt
	// Rerun holds From and every descendant of it, in source identity and in
	// a stable order.
	Rerun []Task
	// Reuse holds every other task of the source run. Their outputs are
	// carried over by reference; they are not executed again.
	Reuse []Task
}

// RerunNames reports the manifest names of the tasks a scope reruns.
func (s RerunScope) RerunNames() []string { return taskNames(s.Rerun) }

// ReuseNames reports the manifest names of the tasks a scope reuses.
func (s RerunScope) ReuseNames() []string { return taskNames(s.Reuse) }

func taskNames(tasks []Task) []string {
	names := make([]string, 0, len(tasks))
	for _, task := range tasks {
		names = append(names, task.Name)
	}
	return names
}

// ErrRerunSourceLive refuses a rerun of a run that is still executing. The
// scope of a rerun is read from which tasks succeeded, and that answer is not
// stable while the run can still change it.
var ErrRerunSourceLive = errors.New(
	"the source run has not finished; a rerun scope read from a live run would be a guess. " +
		"Wait for it, or cancel it with \"t3-steward campaign cancel <run>/<task> --reason TEXT\"")

// PlanRerun divides a terminal source run at one task.
//
// It resolves from by task ID or by manifest name, collects the reachable
// descendants, and refuses when a task that would be reused did not succeed:
// starting a task whose input never existed is the failure this command is
// supposed to prevent, not a case it is supposed to handle.
func PlanRerun(run WorkflowRun, templates []Task, attempts []Attempt, from string) (RerunScope, error) {
	var scope RerunScope
	if from == "" {
		return scope, errors.New("a rerun needs the task to start from")
	}
	if !run.Progress.Terminal() {
		return scope, ErrRerunSourceLive
	}
	tasks := TasksForRun(run, templates)
	if len(tasks) == 0 {
		return scope, fmt.Errorf("workflow run %s has no task definitions to rerun", run.ID)
	}
	last := lastAttempts(run, attempts)
	start := -1
	for index, task := range tasks {
		if task.ID == from || task.Name == from {
			if start >= 0 {
				return scope, fmt.Errorf("%q names more than one task in run %s", from, run.ID)
			}
			start = index
		}
	}
	if start < 0 {
		return scope, fmt.Errorf("run %s has no task %q", run.ID, from)
	}
	scope.From = tasks[start]
	scope.FromAttempt = last[scope.From.ID]
	selected := map[string]bool{scope.From.Name: true}
	// A descendant is any task that reaches the named one through needs. The
	// walk repeats until nothing new is selected, so the order of the task
	// list does not decide the answer.
	for changed := true; changed; {
		changed = false
		for _, task := range tasks {
			if selected[task.Name] {
				continue
			}
			for _, need := range task.Needs {
				if selected[need] {
					selected[task.Name] = true
					changed = true
					break
				}
			}
		}
	}
	var unusable []string
	for _, task := range tasks {
		if selected[task.Name] {
			scope.Rerun = append(scope.Rerun, task)
			continue
		}
		if last[task.ID].Progress != ProgressSucceeded {
			unusable = append(unusable, fmt.Sprintf("%s is %s", task.Name, reusableState(last[task.ID])))
			continue
		}
		scope.Reuse = append(scope.Reuse, task)
	}
	if len(unusable) > 0 {
		sort.Strings(unusable)
		return RerunScope{}, fmt.Errorf(
			"a rerun from %s would reuse %d task(s) that did not succeed: %v. "+
				"Rerun from a task they all descend from instead",
			scope.From.Name, len(unusable), unusable)
	}
	return scope, nil
}

func reusableState(attempt Attempt) string {
	if attempt.Progress == "" {
		return "never attempted"
	}
	return string(attempt.Progress)
}

// lastAttempts indexes the highest-numbered attempt of every task in a run.
func lastAttempts(run WorkflowRun, attempts []Attempt) map[string]Attempt {
	last := map[string]Attempt{}
	for _, attempt := range attempts {
		if attempt.WorkflowRunID != run.ID {
			continue
		}
		if current, ok := last[attempt.TaskID]; ok && current.Number >= attempt.Number {
			continue
		}
		last[attempt.TaskID] = attempt
	}
	return last
}
