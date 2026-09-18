package domain

import (
	"errors"
	"fmt"
	"strconv"
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
	// ExitCode is the check-protocol reading of the observation: 0 met, 1 not
	// yet, 2 settled against the waiter. Success dependencies read it.
	ExitCode int    `json:"exitCode"`
	Reason   string `json:"reason"`
	// Outcome is the wake outcome of a settled observation. Empty in records
	// written before outcomes existed; NodeObservationOutcome derives it.
	Outcome TaskWaitOutcome `json:"outcome,omitempty"`
	// Fields are extra kind-specific pairs for the wake trailer, such as the
	// failed task list of a terminal run or the pause reason of a paused one.
	Fields map[string]string `json:"fields,omitempty"`
}

// ResolveNodeState is ResolveNode for a node wait with a --state: it reports
// the requested state as met, still pending, or unreachable, and carries the
// evidence the wake trailer needs. ExitCode keeps the check-protocol reading
// (0 met, 1 pending, 2 settled against the waiter); Outcome is the wake
// outcome, which for --state terminal is met on any terminal progress except
// cancelled, with the failed task list in Fields.
func ResolveNodeState(ref NodeRef, state NodeWaitState, runs []WorkflowRun, tasks []Task, attempts []Attempt, assignments []Assignment, workers []WorkerSnapshot) (NodeObservation, error) {
	if state == "" {
		state = NodeStateTerminal
	}
	if _, err := ParseNodeWaitState(string(state)); err != nil {
		return NodeObservation{Target: ref, ExitCode: 1, Reason: "pending"}, err
	}
	out, err := ResolveNode(ref, runs, tasks, attempts, assignments)
	if err != nil {
		return out, err
	}
	var run *WorkflowRun
	for i := range runs {
		if runs[i].ID == ref.RunID {
			run = &runs[i]
		}
	}
	isSink := run != nil && run.Sink != nil && out.Target.TaskID == run.Sink.ID
	if isSink && !state.Terminal() {
		return out, fmt.Errorf("--state %s needs a task: a run's sink has no attempt to be %s; name <run>/<task>, or wait for the run with --state terminal or succeeded", state, state)
	}
	out.Fields = map[string]string{}
	if run != nil && run.Sink != nil && run.Sink.Progress.Terminal() && run.Sink.Result != nil && len(run.Sink.Result.FailedTaskIDs) != 0 {
		out.Fields["failed"] = strings.Join(run.Sink.Result.FailedTaskIDs, ",")
	}
	terminal := out.ExitCode != 1
	switch state {
	case NodeStateTerminal, NodeStateSucceeded:
		if !terminal {
			return out, nil
		}
		switch {
		case out.Progress == ProgressCancelled:
			out.Outcome = TaskWaitCancelled
		case out.Progress == ProgressSucceeded || state == NodeStateTerminal:
			out.Outcome = TaskWaitMet
		default:
			out.Outcome = TaskWaitFailed
		}
		return out, nil
	}
	// An attempt state. The latest attempt is the one ResolveNode reported.
	var latest *Attempt
	for i := range attempts {
		if attempts[i].ID == out.AttemptID {
			latest = &attempts[i]
		}
	}
	if terminal || (latest != nil && latest.Progress.Terminal()) {
		out.ExitCode = 2
		out.Outcome = TaskWaitFailed
		out.Reason = fmt.Sprintf("the node reached %s before it was %s", out.Progress, state)
		return out, nil
	}
	if latest == nil {
		out.ExitCode, out.Reason = 1, "pending: no attempt yet"
		return out, nil
	}
	out.Fields["control"] = string(latest.Control)
	met := false
	switch state {
	case NodeStateWaitingExternal:
		met = latest.Progress == ProgressWaitingExternal || latest.Control == ControlWaitingExternal
	case NodeStateActive:
		met = latest.Progress == ProgressActive && latest.Control.HoldsProviderSlot()
	case NodeStatePaused:
		met = latest.Control == ControlPaused || latest.Control == ControlPausedUncheckpointed
		if reason := attemptPauseReason(*latest, assignments, workers); reason != "" {
			out.Fields["pauseReason"] = reason
			met = true
		}
	}
	if !met {
		out.ExitCode, out.Reason = 1, fmt.Sprintf("pending: attempt is %s/%s", latest.Progress, latest.Control)
		return out, nil
	}
	out.ExitCode, out.Outcome = 0, TaskWaitMet
	out.Reason = fmt.Sprintf("attempt is %s/%s", latest.Progress, latest.Control)
	return out, nil
}

// attemptPauseReason is the quota pause the assigned worker last reported for
// the attempt, or empty.
func attemptPauseReason(attempt Attempt, assignments []Assignment, workers []WorkerSnapshot) string {
	var assignment *Assignment
	for i := range assignments {
		if assignments[i].ID == attempt.AssignmentID && assignments[i].AttemptID == attempt.ID {
			assignment = &assignments[i]
		}
	}
	if assignment == nil {
		return ""
	}
	for _, worker := range workers {
		if worker.WorkerID != assignment.WorkerID {
			continue
		}
		for _, observed := range worker.Assignments {
			if observed.AssignmentID == assignment.ID && observed.AssignmentEpoch == assignment.Epoch && observed.Journal != nil {
				return observed.Journal.PauseReason
			}
		}
	}
	return ""
}

// NodeTrailerFields are the wake trailer pairs of a node observation: run,
// task, attempt, revision, progress, the observation's own fields (failed,
// control, pauseReason) and, for a terminal run, the result verb.
func NodeTrailerFields(o NodeObservation) map[string]string {
	fields := make(map[string]string, len(o.Fields)+6)
	for key, value := range o.Fields {
		fields[key] = value
	}
	fields["run"] = o.Target.RunID
	fields["task"] = o.Target.TaskID
	fields["attempt"] = o.AttemptID
	fields["revision"] = strconv.FormatInt(o.RunRevision, 10)
	fields["progress"] = string(o.Progress)
	if o.Progress.Terminal() {
		fields["result"] = "t3-steward result " + o.Target.RunID
	}
	return fields
}

// NodeObservationOutcome is the wake outcome of a settled observation, derived
// from the exit code and reason for records that predate the Outcome field.
func NodeObservationOutcome(o NodeObservation) TaskWaitOutcome {
	if o.Outcome != "" {
		return o.Outcome
	}
	switch {
	case o.ExitCode == 0:
		return TaskWaitMet
	case o.Reason == "timed out":
		return TaskWaitTimedOut
	case o.Progress == ProgressCancelled:
		return TaskWaitCancelled
	default:
		return TaskWaitFailed
	}
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
		tasks = TasksForRun(*run, tasks)
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
	// State is the node state waited for; empty is terminal.
	State NodeWaitState `json:"state,omitempty"`
	// OrTimeout makes the deadline a normal outcome, as it does for a task
	// wait: the observation still says timed-out, with exit 0 and the
	// or-timeout trailer pair, rather than the failure form.
	OrTimeout bool `json:"orTimeout,omitempty"`
	// Quota makes this a quota wait: Target is empty and the condition is
	// settled from the pool's merged bucket observations.
	Quota *QuotaWaitCondition `json:"quota,omitempty"`
	// Group and Wake compose interactive waits of the coordinator kinds the
	// way local waits compose: with Wake all, the thread is woken once, when
	// every member of the group on that thread has settled.
	Group string   `json:"group,omitempty"`
	Wake  WakeMode `json:"wake,omitempty"`
}

// Kind is the wait kind of a coordinator-settled interactive wait.
func (r NodeWaitRequest) Kind() WaitKind {
	if r.Quota != nil {
		return WaitKindQuota
	}
	return WaitKindNode
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
