package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// TaskWaitEnvironment names the execution identity a running task process is
// given so that it can register a wait against itself. The full execution
// identity otherwise stops at the worker: the agent inside the thread has never
// been told which attempt it is, so "wait for this and park me" was not
// expressible from inside a task.
//
// Exactly these six variables are injected, and exactly these six are added to
// the contained execution allowlist. The list is closed on purpose. The
// allowlist is a security boundary, and every extra name in it is another way
// for host state to reach an agent process.
const (
	TaskWaitEnvWorkflowRunID   = "T3_STEWARD_WORKFLOW_RUN_ID"
	TaskWaitEnvTaskID          = "T3_STEWARD_TASK_ID"
	TaskWaitEnvAttemptID       = "T3_STEWARD_ATTEMPT_ID"
	TaskWaitEnvAttemptRevision = "T3_STEWARD_ATTEMPT_REVISION"
	TaskWaitEnvAssignmentID    = "T3_STEWARD_ASSIGNMENT_ID"
	TaskWaitEnvThreadID        = "T3_STEWARD_THREAD_ID"
)

// TaskWaitEnvironmentNames lists the injected variables in a stable order.
func TaskWaitEnvironmentNames() []string {
	return []string{
		TaskWaitEnvWorkflowRunID, TaskWaitEnvTaskID, TaskWaitEnvAttemptID,
		TaskWaitEnvAttemptRevision, TaskWaitEnvAssignmentID, TaskWaitEnvThreadID,
	}
}

// WakeMode decides when a parked attempt resumes once it holds several waits.
type WakeMode string

const (
	// WakeEach resumes the attempt as soon as any one of its waits settles.
	WakeEach WakeMode = "each"
	// WakeAll resumes the attempt only once every live wait has settled.
	WakeAll WakeMode = "all"
)

// TaskWaitOutcome is the immutable settlement of one task-bound wait.
type TaskWaitOutcome string

const (
	TaskWaitMet       TaskWaitOutcome = "met"
	TaskWaitFailed    TaskWaitOutcome = "failed"
	TaskWaitTimedOut  TaskWaitOutcome = "timed-out"
	TaskWaitCancelled TaskWaitOutcome = "cancelled"
)

// TaskWaitResult is the structured evidence a woken task receives. Silence is
// not an outcome: a failed or timed-out wait says which wait it was, what the
// condition reported, and how long it ran, so the agent can either handle it or
// produce an honest final failure.
type TaskWaitResult struct {
	Outcome    TaskWaitOutcome `json:"outcome"`
	ExitCode   int             `json:"exitCode"`
	Reason     string          `json:"reason,omitempty"`
	Output     string          `json:"output,omitempty"`
	RanFor     time.Duration   `json:"ranFor"`
	ObservedAt time.Time       `json:"observedAt"`
}

// TaskWait is the coordinator-owned record that binds one external condition to
// one logical attempt. Registering it is the evidence that the current turn is
// parking rather than completing: the worker does not collect, the coordinator
// does not verify, and neither dependents nor the run sink settle while it is
// live.
type TaskWait struct {
	ID               string        `json:"id"`
	WorkflowRunID    string        `json:"workflowRunId"`
	TaskID           string        `json:"taskId"`
	AttemptID        string        `json:"attemptId"`
	ExpectedRevision uint64        `json:"expectedRevision"`
	ThreadID         string        `json:"threadId"`
	Wake             WakeMode      `json:"wake"`
	MaxDuration      time.Duration `json:"maxDuration"`
	RequestID        string        `json:"requestId"`

	// Name and Condition describe what is being waited for, for the operator
	// reading a queue and for the wake message the agent receives.
	Name      string `json:"name,omitempty"`
	Condition string `json:"condition,omitempty"`

	// RegisteredRevision is the attempt revision this registration produced. It
	// is the fence a later wake is checked against.
	RegisteredRevision int64           `json:"registeredRevision"`
	RegisteredAt       time.Time       `json:"registeredAt"`
	Deadline           time.Time       `json:"deadline"`
	Result             *TaskWaitResult `json:"result,omitempty"`
	SettledAt          *time.Time      `json:"settledAt,omitempty"`
	WokenAt            *time.Time      `json:"wokenAt,omitempty"`

	// Delivery is empty until the attempt is resumed, then "pending" until the
	// wake message reaches the thread and "delivered" afterwards. The resumed
	// attempt is committed before the message is sent, so a lost response
	// retries the message and never the resumption.
	Delivery    string     `json:"delivery,omitempty"`
	DeliveredAt *time.Time `json:"deliveredAt,omitempty"`
}

// Live reports whether this wait still parks its attempt. A settled wait whose
// wake has not been applied yet is not live: its condition is decided, and the
// attempt is about to resume.
func (w TaskWait) Live() bool { return w.SettledAt == nil }

// Settled reports whether the wait has an immutable outcome.
func (w TaskWait) Settled() bool { return w.SettledAt != nil }

// Woken reports whether this wait's settlement has already resumed its attempt.
// One wake per wait, and one resumed turn per wake, is what makes coordinator
// restart, worker restart and duplicate delivery idempotent.
func (w TaskWait) Woken() bool { return w.WokenAt != nil }

// TaskWaitRegistration is the request to park an attempt on a condition.
type TaskWaitRegistration struct {
	RequestID        string        `json:"requestId"`
	WorkflowRunID    string        `json:"workflowRunId"`
	TaskID           string        `json:"taskId"`
	AttemptID        string        `json:"attemptId"`
	ExpectedRevision uint64        `json:"expectedRevision"`
	ThreadID         string        `json:"threadId"`
	Wake             WakeMode      `json:"wake"`
	MaxDuration      time.Duration `json:"maxDuration"`
	Name             string        `json:"name,omitempty"`
	Condition        string        `json:"condition,omitempty"`
}

// MaxTaskWaitDuration bounds any single task-bound wait. Directory writer
// bindings have no deadline by design and are held across a wait, so the bound
// on how long the fleet can be blocked has to come from the wait itself.
const MaxTaskWaitDuration = 30 * 24 * time.Hour

// ErrTaskWaitTerminalAttempt is the refusal a terminal attempt gives a new
// task-bound wait. It is what stops a thread that has already lost its task
// authority from quietly acquiring a new reason to keep working.
var ErrTaskWaitTerminalAttempt = errors.New("attempt is terminal")

// TaskWaitTerminalRefusal is the exact message a terminal attempt refuses with.
func TaskWaitTerminalRefusal(progress ProgressState) error {
	return fmt.Errorf("%w (%s); task-bound waits are refused", ErrTaskWaitTerminalAttempt, progress)
}

// ErrTaskWaitStaleRevision reports a registration that named an attempt
// revision the attempt has since moved past. It is refused rather than applied
// to whatever the attempt has become.
var ErrTaskWaitStaleRevision = errors.New("task-bound wait names a stale attempt revision")

// ErrTaskWaitReplayChanged reports a repeated request ID whose registration
// differs from the one already committed.
var ErrTaskWaitReplayChanged = errors.New("task-bound wait request ID replay changed the registration")

// Validate checks a registration before any store is touched.
func (r TaskWaitRegistration) Validate() error {
	switch {
	case strings.TrimSpace(r.RequestID) == "" || len(r.RequestID) > 128:
		return errors.New("task-bound wait needs a request ID of at most 128 bytes")
	case r.WorkflowRunID == "" || r.TaskID == "" || r.AttemptID == "":
		return errors.New("task-bound wait needs a workflow run, task and attempt")
	case r.ThreadID == "":
		return errors.New("task-bound wait needs the canonical T3 thread")
	case r.Wake != WakeEach && r.Wake != WakeAll:
		return fmt.Errorf("task-bound wait wake mode %q must be each or all", r.Wake)
	case r.MaxDuration <= 0 || r.MaxDuration > MaxTaskWaitDuration:
		return fmt.Errorf("task-bound wait needs a maximum duration above zero and at most %s", MaxTaskWaitDuration)
	case len(r.Name) > 1000 || len(r.Condition) > 4000:
		return errors.New("task-bound wait name or condition is too long")
	}
	return nil
}

// TaskWaitReconciliationKind names why a lifecycle contradiction was recorded.
type TaskWaitReconciliationKind string

const (
	// TaskWaitReconciliationDoneWhileWaiting is a done marker observed while a
	// live task-bound wait exists. It is refused, never silently honoured: it is
	// exactly the contradiction that verified a task against outputs it had not
	// written yet.
	TaskWaitReconciliationDoneWhileWaiting TaskWaitReconciliationKind = "done-while-waiting"
	// TaskWaitReconciliationAuthorityRevoked records that a thread's task
	// authority was invalidated before its task was marked terminal.
	TaskWaitReconciliationAuthorityRevoked TaskWaitReconciliationKind = "authority-revoked"
	// TaskWaitReconciliationExpired records a wait the coordinator timed out.
	TaskWaitReconciliationExpired TaskWaitReconciliationKind = "expired"
)

// TaskWaitReconciliation is the durable record of a lifecycle contradiction.
// The failure this whole record exists to prevent was invisible precisely
// because nothing durable was written when the two stories disagreed.
type TaskWaitReconciliation struct {
	ID         string                     `json:"id"`
	Kind       TaskWaitReconciliationKind `json:"kind"`
	AttemptID  string                     `json:"attemptId"`
	WaitID     string                     `json:"waitId,omitempty"`
	ThreadID   string                     `json:"threadId,omitempty"`
	Detail     string                     `json:"detail"`
	ObservedAt time.Time                  `json:"observedAt"`
}

// TaskWaitWakeContext is the evidence handed to the resumed turn.
type TaskWaitWakeContext struct {
	AttemptID string     `json:"attemptId"`
	ThreadID  string     `json:"threadId"`
	Waits     []TaskWait `json:"waits"`
}

// Prompt renders the wake message that starts the resumed turn.
func (c TaskWaitWakeContext) Prompt() string {
	var builder strings.Builder
	builder.WriteString("The steward is waking this task: its registered wait has settled.\n\n")
	for _, w := range c.Waits {
		name := w.Name
		if name == "" {
			name = w.ID
		}
		if w.Result == nil {
			fmt.Fprintf(&builder, "- %s: still pending\n", name)
			continue
		}
		fmt.Fprintf(&builder, "- %s: %s (exit %d) after %s", name, w.Result.Outcome, w.Result.ExitCode, w.Result.RanFor.Round(time.Second))
		if w.Result.Reason != "" {
			fmt.Fprintf(&builder, ": %s", w.Result.Reason)
		}
		builder.WriteString("\n")
		if output := strings.TrimSpace(w.Result.Output); output != "" {
			fmt.Fprintf(&builder, "\n%s\n\n", output)
		}
	}
	builder.WriteString("\nContinue the task. Write every declared output before ending the turn; ")
	builder.WriteString("the outputs collected are the ones present when the turn ends with no live wait.\n")
	return builder.String()
}
