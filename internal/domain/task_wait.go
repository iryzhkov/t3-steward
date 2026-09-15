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

// TaskIdentityDir and TaskIdentityFile name the worker-written record of the
// same six variables, inside the prepared workspace.
//
// It is the primary mechanism, not a fallback. Passing an environment through
// the provider would make identity depend on a field of the T3 create command
// that no tested release verifies; a file the worker writes into a workspace it
// already owns depends on nothing outside this repository, and can be tested
// without a live provider.
const (
	TaskIdentityDir  = ".t3-steward"
	TaskIdentityFile = TaskIdentityDir + "/task.env"
)

// RenderTaskIdentityFile writes the six identity variables in the order
// TaskWaitEnvironmentNames gives, as KEY=value lines.
//
// It carries identity and nothing else. No dispatch token, no credential and no
// lease: an agent reading it learns which attempt it is, never how to claim an
// authority it was not given.
func RenderTaskIdentityFile(values map[string]string) (string, error) {
	var builder strings.Builder
	builder.WriteString("# Written by t3-steward. Identity only: this file grants nothing.\n")
	for _, name := range TaskWaitEnvironmentNames() {
		value, ok := values[name]
		if !ok || value == "" {
			return "", fmt.Errorf("task identity file is missing %s", name)
		}
		if strings.ContainsAny(value, "\n\r\x00") {
			return "", fmt.Errorf("task identity value for %s contains a line break", name)
		}
		fmt.Fprintf(&builder, "%s=%s\n", name, value)
	}
	return builder.String(), nil
}

// ParseTaskIdentityFile reads the rendered form back. Unknown keys are refused
// rather than ignored: this file is a closed identity record, and a reader that
// silently tolerates extra keys is a reader that can be fed something else.
func ParseTaskIdentityFile(content string) (map[string]string, error) {
	allowed := make(map[string]bool, 6)
	for _, name := range TaskWaitEnvironmentNames() {
		allowed[name] = true
	}
	values := make(map[string]string, 6)
	for number, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, found := strings.Cut(line, "=")
		if !found || !allowed[name] {
			return nil, fmt.Errorf("task identity file line %d is not a known identity assignment", number+1)
		}
		if _, duplicate := values[name]; duplicate {
			return nil, fmt.Errorf("task identity file repeats %s", name)
		}
		values[name] = value
	}
	for name := range allowed {
		if values[name] == "" {
			return nil, fmt.Errorf("task identity file is missing %s", name)
		}
	}
	return values, nil
}

// WakeMode decides when a parked attempt resumes once it holds several waits.
type WakeMode string

const (
	// WakeEach resumes the attempt as soon as any one of its waits settles.
	WakeEach WakeMode = "each"
	// WakeAll resumes the attempt only once every live wait has settled.
	WakeAll WakeMode = "all"
)

// The wake condition is evaluated over all of one attempt's waits, not over a
// named group: an attempt is parked or it is not, and there is nothing for a
// second group on the same attempt to mean.
//
// Mixing the two modes on one attempt is defined rather than refused: any each
// wait that settles wakes the attempt, and the all waits still live at that
// moment are carried into the resumed turn unsettled. An agent that registers
// one urgent check alongside a set it wants complete gets the urgent answer
// when it arrives, which is the only reading under which each still means each.

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
	ID            string `json:"id"`
	WorkflowRunID string `json:"workflowRunId"`
	TaskID        string `json:"taskId"`
	AttemptID     string `json:"attemptId"`
	// IssuedRevision is the attempt revision the task was told by its execution
	// package. It is evidence of what the task was given, not a fence.
	//
	// It cannot be a fence. The coordinator stamps it when the package is built
	// and then advances the attempt itself, at least when the worker claims the
	// assignment and again when it reports the thread running, so the number in
	// the task's hands is already behind before the turn starts. Fencing on it
	// meant no task-bound wait could ever be registered. What the registration
	// is fenced on instead is the attempt's own live turn; see RegisterTaskWait.
	IssuedRevision int64         `json:"issuedRevision"`
	ThreadID       string        `json:"threadId"`
	Wake           WakeMode      `json:"wake"`
	MaxDuration    time.Duration `json:"maxDuration"`
	RequestID      string        `json:"requestId"`

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

	// Delivery is empty until the wake is committed, then pending, held,
	// sending, recovery-required, delivered or abandoned. The resumed attempt
	// is committed before the message is sent, so a lost response retries the
	// message and never the resumption.
	//
	// sending is durable before the send, so a crash on either side of it
	// leaves evidence that a message may exist. Only observing DeliveryID in
	// the thread resolves that; absence is not proof of non-delivery and never
	// authorizes a second send.
	Delivery    string     `json:"delivery,omitempty"`
	DeliveredAt *time.Time `json:"deliveredAt,omitempty"`
	// DeliveryID is the stable external identity of this wake. It is derived
	// once, from the wait, so a retry sends the same command rather than a new
	// one that would start a second turn.
	DeliveryID string `json:"deliveryId,omitempty"`
	// WakeRevision is the attempt revision the wake was committed against. A
	// delivery is refused if the attempt has moved past it: the turn that would
	// have received the message is gone.
	WakeRevision int64 `json:"wakeRevision,omitempty"`
	// Resumption records whether this wake resumed a parked attempt or merely
	// carries evidence to one that was already running.
	Resumption bool `json:"resumption,omitempty"`
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

// Parking reports whether this wait still holds its attempt parked.
//
// It is deliberately wider than Live. A wait stops holding its attempt when the
// outcome reaches the thread, not when the condition settles. In between, the
// turn that parked has ended and the turn that will read the outcome has not
// started, so the attempt is parked with no live wait and no running thread.
// Reading Live there says "not parked" about an attempt that is about to run
// again, and whoever acts on that answer collects a task mid-park.
//
// The two questions are separate on purpose. Live answers "is the condition
// still undecided", which is what refusing a completion marker turns on.
// Parking answers "will this attempt run again", which is what deciding to
// collect it turns on.
func (w TaskWait) Parking() bool {
	switch {
	case w.Live():
		return true
	case !w.Woken():
		// Settled, and the coordinator has not yet decided what the settlement
		// does to the attempt. Until it has, the park stands.
		return true
	case !w.Resumption:
		// This wake carries evidence to a turn that is already running. It
		// holds nothing, and never did.
		return false
	default:
		// A resumption wake holds the attempt until its message has reached the
		// thread, or until it is abandoned because no turn can receive it.
		return w.Delivery != "delivered" && w.Delivery != "abandoned"
	}
}

// TaskWaitRegistration is the request to park an attempt on a condition.
type TaskWaitRegistration struct {
	RequestID     string `json:"requestId"`
	WorkflowRunID string `json:"workflowRunId"`
	TaskID        string `json:"taskId"`
	AttemptID     string `json:"attemptId"`
	// IssuedRevision is the attempt revision the task read from its own
	// identity. It is recorded rather than fenced on; see TaskWait.
	IssuedRevision int64         `json:"issuedRevision"`
	ThreadID       string        `json:"threadId"`
	Wake           WakeMode      `json:"wake"`
	MaxDuration    time.Duration `json:"maxDuration"`
	Name           string        `json:"name,omitempty"`
	Condition      string        `json:"condition,omitempty"`
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

// ErrTaskWaitStaleRevision reports a registration that lost a race: the attempt
// changed between the moment the registration read it and the moment it tried
// to commit. It is refused rather than applied to whatever the attempt has
// since become.
var ErrTaskWaitStaleRevision = errors.New("task-bound wait names a stale attempt revision")

// ErrTaskWaitTurnNotLive reports a registration whose attempt has no live turn
// to park: it has been released, paused, drained, or has finished its turn and
// is being verified.
//
// This is the refusal that means "the attempt moved on underneath you". It is a
// statement about the attempt's own state, so it stays true however long the
// task took to ask, which a revision handed out at dispatch never could.
var ErrTaskWaitTurnNotLive = errors.New("task-bound wait names an attempt with no live turn")

// ErrTaskWaitForeignThread reports a registration from a thread that is not the
// one the attempt is running on. A thread may only park the attempt it is
// executing.
var ErrTaskWaitForeignThread = errors.New("task-bound wait names an attempt running on another thread")

// ErrTaskWaitReplayChanged reports a repeated request ID whose registration
// differs from the one already committed.
var ErrTaskWaitReplayChanged = errors.New("task-bound wait request ID replay changed the registration")

// ErrTaskWaitReplaySettled reports a repeated request ID whose wait has already
// settled, so replaying it cannot park anything.
//
// Returning the settled record would be the original failure rebuilt: the agent
// would be told it is parked, end its turn, and the worker would collect
// outputs it had not written. A request ID identifies one park, not a standing
// permission to park again.
var ErrTaskWaitReplaySettled = errors.New("task-bound wait has already settled; a repeated request ID cannot park the attempt again")

// ErrTaskWaitReplayNotParked reports a repeated request ID whose wait is live
// but whose attempt is no longer parked on it. The two disagree, so neither is
// reported as fact.
var ErrTaskWaitReplayNotParked = errors.New("task-bound wait is live but its attempt is not parked")

// Validate checks a registration before any store is touched.
func (r TaskWaitRegistration) Validate() error {
	switch {
	case strings.TrimSpace(r.RequestID) == "" || len(r.RequestID) > 128:
		return errors.New("task-bound wait needs a request ID of at most 128 bytes")
	case r.WorkflowRunID == "" || r.TaskID == "" || r.AttemptID == "":
		return errors.New("task-bound wait needs a workflow run, task and attempt")
	case r.IssuedRevision < 0:
		return errors.New("task-bound wait needs a non-negative issued attempt revision")
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
	// TaskWaitReconciliationUndelivered records a settled wait whose outcome
	// could not reach any turn, because the attempt became terminal or moved
	// past the revision the wake was bound to. Silence is not an outcome, so
	// when the agent cannot be told, the record says so instead.
	TaskWaitReconciliationUndelivered TaskWaitReconciliationKind = "undelivered"
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
	AttemptID string `json:"attemptId"`
	ThreadID  string `json:"threadId"`
	// AttemptRevision is the revision this wake belongs to. Delivery is
	// refused once the attempt has moved past it: the turn that would have
	// received the message is gone.
	AttemptRevision int64      `json:"attemptRevision"`
	Waits           []TaskWait `json:"waits"`
}

// Resumption reports whether this wake resumed a parked attempt. A wake that
// did not is evidence delivered to a turn that is already running.
func (c TaskWaitWakeContext) Resumption() bool {
	for _, wait := range c.Waits {
		if wait.Resumption {
			return true
		}
	}
	return false
}

// Prompt renders the wake message that starts the resumed turn.
func (c TaskWaitWakeContext) Prompt() string {
	var builder strings.Builder
	if c.Resumption() {
		builder.WriteString("The steward is waking this task: its registered wait has settled.\n\n")
	} else {
		// The turn is already running. Saying so matters: the agent is being
		// handed evidence mid-turn, not being restarted, and a wait that
		// settled without anyone being told is the silence this design refuses.
		builder.WriteString("A registered wait for this task has settled while the task is already running.\n\n")
	}
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
	builder.WriteString("the outputs collected are the ones present when a turn ends with nothing parking this task.\n")
	return builder.String()
}
