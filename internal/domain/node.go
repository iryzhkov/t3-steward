package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// NodeRef names retained coordinator metadata, never a transport-side command.
type NodeRef struct {
	RunID  string `json:"runId"`
	TaskID string `json:"taskId"`
}

func ParseNodeRef(value string) (NodeRef, error) {
	run, task, ok := strings.Cut(value, "/")
	if !ok || run == "" || task == "" || strings.Contains(task, "/") {
		return NodeRef{}, errors.New("node must be <run>/<task>")
	}
	return NodeRef{RunID: run, TaskID: task}, nil
}
func (n NodeRef) String() string { return n.RunID + "/" + n.TaskID }

type NodeObservation struct {
	Target          NodeRef       `json:"target"`
	RunRevision     int64         `json:"runRevision"`
	AttemptID       string        `json:"attemptId,omitempty"`
	AttemptRevision int64         `json:"attemptRevision,omitempty"`
	Progress        ProgressState `json:"progress"`
	ExitCode        int           `json:"exitCode"`
	Reason          string        `json:"reason"`
}

// ResolveNode is shared by native waits and success dependencies. Missing state
// fails explicitly. Cancellation overrides a sink's literal no-failures aggregate.
func ResolveNode(ref NodeRef, runs []WorkflowRun, tasks []Task, attempts []Attempt, assignments []Assignment) (NodeObservation, error) {
	out := NodeObservation{Target: ref, ExitCode: 1, Reason: "pending"}
	var run *WorkflowRun
	for i := range runs {
		if runs[i].ID == ref.RunID {
			run = &runs[i]
			break
		}
	}
	if run == nil {
		return out, fmt.Errorf("run %q is unavailable", ref.RunID)
	}
	out.RunRevision = run.Revision
	if run.Sink != nil && (ref.TaskID == SinkTaskName || ref.TaskID == run.Sink.ID) {
		out.Target.TaskID = run.Sink.ID
		out.Progress = run.Sink.Progress
		if !run.Sink.Progress.Terminal() {
			return out, nil
		}
		if run.Progress == ProgressCancelled || run.Progress == ProgressSkipped {
			out.Progress = run.Progress
		}
	} else {
		var task *Task
		for i := range tasks {
			if tasks[i].WorkflowID == run.WorkflowID && (tasks[i].ID == ref.TaskID || tasks[i].Name == ref.TaskID) {
				task = &tasks[i]
				break
			}
		}
		if task == nil {
			return out, fmt.Errorf("task %q is unavailable in run %q", ref.TaskID, ref.RunID)
		}
		out.Target.TaskID = task.ID
		var latest *Attempt
		var owned []Attempt
		for i := range attempts {
			a := &attempts[i]
			if a.WorkflowRunID != run.ID || a.TaskID != task.ID {
				continue
			}
			owned = append(owned, *a)
			if latest == nil || a.Number > latest.Number {
				latest = a
			}
		}
		if latest == nil {
			return out, nil
		}
		out.Progress = latest.Progress
		out.AttemptID = latest.ID
		out.AttemptRevision = latest.Revision
		if !latest.Progress.Terminal() || !RunExecutionsQuiescent(run.ID, owned, assignments) {
			return out, nil
		}
		if run.Progress == ProgressCancelled {
			out.Progress = ProgressCancelled
		}
	}
	if out.Progress == ProgressSucceeded {
		out.ExitCode = 0
		out.Reason = "succeeded"
	} else {
		out.ExitCode = 2
		out.Reason = string(out.Progress)
	}
	return out, nil
}

type NodeWaitRequest struct {
	ID       string        `json:"id"`
	ThreadID string        `json:"threadId"`
	Name     string        `json:"name"`
	Target   NodeRef       `json:"target"`
	Timeout  time.Duration `json:"timeout"`
}
type NodeWait struct {
	Registration       NodeWaitRequest  `json:"registration"`
	Request            NodeWaitRequest  `json:"request"`
	Actor              string           `json:"actor"`
	Host               string           `json:"host"`
	RegisteredRevision int64            `json:"registeredRevision"`
	CreatedAt          time.Time        `json:"createdAt"`
	Deadline           time.Time        `json:"deadline"`
	Observation        *NodeObservation `json:"observation,omitempty"`
	SettledAt          *time.Time       `json:"settledAt,omitempty"`
	DeliveryID         string           `json:"deliveryId"`
	// pending, held, sending, recovery-required, delivered or cancelled.
	Delivery    string     `json:"delivery"`
	DeliveredAt *time.Time `json:"deliveredAt,omitempty"`
}
