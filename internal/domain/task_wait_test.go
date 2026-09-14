package domain

import (
	"errors"
	"testing"
	"time"
)

// The run sink must not settle while any attempt is parked. Giving a waiting
// attempt a quiescent control state is the observed bug wearing a different
// name: a run result would be published while its thread was still to be woken.
func TestSinkCannotSettleWhileATaskIsParked(t *testing.T) {
	now := time.Date(2026, 9, 14, 17, 30, 44, 0, time.UTC)
	run := WorkflowRun{ID: "r", WorkflowID: "w", Revision: 1, Progress: ProgressActive}
	tasks := []Task{{ID: "t", Name: "t", WorkflowID: "w"}}
	assignment := Assignment{ID: "assign-1", AttemptID: "a1", State: AssignmentClaimed}
	parked := Attempt{
		ID: "a1", WorkflowRunID: "r", TaskID: "t", Number: 1, AssignmentID: "assign-1",
		Progress: ProgressWaitingExternal, Control: ControlWaitingExternal,
	}
	if RunExecutionsQuiescent("r", []Attempt{parked}, []Assignment{assignment}) {
		t.Fatal("a parked attempt was reported quiescent")
	}
	projected, err := ProjectRunSink(run, tasks, []Attempt{parked}, []Assignment{assignment}, now)
	if err != nil {
		t.Fatal(err)
	}
	if projected.Sink.Progress.Terminal() || projected.Sink.Result != nil {
		t.Fatalf("the sink settled while a task was parked: %+v", projected.Sink)
	}

	// The same run settles once the parked attempt has genuinely finished.
	finished := parked
	finished.Progress = ProgressSucceeded
	finished.Control = ControlStopped
	settled := assignment
	settled.State = AssignmentCompleted
	projected, err = ProjectRunSink(run, tasks, []Attempt{finished}, []Assignment{settled}, now)
	if err != nil || projected.Sink.Progress != ProgressSucceeded {
		t.Fatalf("the sink did not settle after the task finished: %+v %v", projected.Sink, err)
	}
}

// waiting-external is a live state: not terminal, holding no provider slot, and
// distinct from needs-input, which means a human owes an answer.
func TestWaitingExternalIsLiveAndHoldsNoProviderSlot(t *testing.T) {
	if ProgressWaitingExternal.Terminal() {
		t.Fatal("a parked attempt was reported terminal")
	}
	if ControlWaitingExternal.HoldsProviderSlot() {
		t.Fatal("a parked attempt holds a provider slot")
	}
	if ProgressWaitingExternal == ProgressNeedsInput {
		t.Fatal("waiting-external was collapsed into needs-input")
	}
}

func TestTaskWaitRegistrationValidation(t *testing.T) {
	valid := TaskWaitRegistration{
		RequestID: "req-1", WorkflowRunID: "r", TaskID: "t", AttemptID: "a1",
		ThreadID: "thread-1", Wake: WakeEach, MaxDuration: time.Hour,
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*TaskWaitRegistration){
		"no request ID":   func(r *TaskWaitRegistration) { r.RequestID = "" },
		"no attempt":      func(r *TaskWaitRegistration) { r.AttemptID = "" },
		"no thread":       func(r *TaskWaitRegistration) { r.ThreadID = "" },
		"unknown wake":    func(r *TaskWaitRegistration) { r.Wake = "sometimes" },
		"no bound":        func(r *TaskWaitRegistration) { r.MaxDuration = 0 },
		"unbounded":       func(r *TaskWaitRegistration) { r.MaxDuration = MaxTaskWaitDuration + time.Hour },
		"no workflow run": func(r *TaskWaitRegistration) { r.WorkflowRunID = "" },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := valid
			mutate(&invalid)
			if err := invalid.Validate(); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestTaskWaitTerminalRefusalNamesTheOutcome(t *testing.T) {
	err := TaskWaitTerminalRefusal(ProgressSucceeded)
	if !errors.Is(err, ErrTaskWaitTerminalAttempt) {
		t.Fatal("the refusal is not recognisable")
	}
	if err.Error() != "attempt is terminal (succeeded); task-bound waits are refused" {
		t.Fatalf("refusal message is %q", err.Error())
	}
}

func TestTaskEnvironmentNamesAreExactlySix(t *testing.T) {
	names := TaskWaitEnvironmentNames()
	if len(names) != 6 {
		t.Fatalf("the injected environment is %d names, not six: %v", len(names), names)
	}
	seen := map[string]bool{}
	for _, name := range names {
		if seen[name] {
			t.Fatalf("%q is listed twice", name)
		}
		seen[name] = true
	}
	for _, want := range []string{
		"T3_STEWARD_WORKFLOW_RUN_ID", "T3_STEWARD_TASK_ID", "T3_STEWARD_ATTEMPT_ID",
		"T3_STEWARD_ATTEMPT_REVISION", "T3_STEWARD_ASSIGNMENT_ID", "T3_STEWARD_THREAD_ID",
	} {
		if !seen[want] {
			t.Fatalf("%q is not injected", want)
		}
	}
}
