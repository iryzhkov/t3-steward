package domain

import (
	"strings"
	"testing"
	"time"
)

// nodeStateFixture is one run with one task and one attempt in the given
// progress and control, plus the worker's view of the assignment.
func nodeStateFixture(t *testing.T, progress ProgressState, control ControlState, pauseReason string) ([]WorkflowRun, []Task, []Attempt, []Assignment, []WorkerSnapshot) {
	t.Helper()
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	tasks := []Task{{ID: "t", Name: "build", WorkflowID: "w"}}
	run, err := BindRunSink(WorkflowRun{ID: "r", WorkflowID: "w", Revision: 3, Progress: ProgressActive, CreatedAt: now, UpdatedAt: now}, tasks)
	if err != nil {
		t.Fatal(err)
	}
	attempts := []Attempt{{ID: "a", WorkflowRunID: "r", TaskID: "t", Number: 1, Revision: 5, Progress: progress, Control: control, AssignmentID: "as"}}
	assignments := []Assignment{{ID: "as", AttemptID: "a", WorkerID: "worker", Epoch: 2, State: AssignmentClaimed}}
	if progress.Terminal() {
		assignments[0].State = AssignmentCompleted
		run, err = ProjectRunSink(run, tasks, attempts, assignments, now)
		if err != nil {
			t.Fatal(err)
		}
	}
	workers := []WorkerSnapshot{{WorkerID: "worker", Assignments: []WorkerAssignmentObservation{{
		AssignmentID: "as", AssignmentEpoch: 2, Journal: &WorkerJournalExcerpt{Phase: "running", PauseReason: pauseReason},
	}}}}
	return []WorkflowRun{run}, tasks, attempts, assignments, workers
}

// Each --state value against the attempt states it can observe.
func TestResolveNodeStateReportsEachState(t *testing.T) {
	cases := []struct {
		name         string
		state        NodeWaitState
		progress     ProgressState
		control      ControlState
		pauseReason  string
		want         TaskWaitOutcome // empty means still pending
		field, value string
	}{
		{"terminal on success", NodeStateTerminal, ProgressSucceeded, ControlStopped, "", TaskWaitMet, "progress", "succeeded"},
		{"terminal on failure is met with the failed list", NodeStateTerminal, ProgressFailed, ControlStopped, "", TaskWaitMet, "failed", "t"},
		{"terminal on cancellation", NodeStateTerminal, ProgressCancelled, ControlStopped, "", TaskWaitCancelled, "progress", "cancelled"},
		{"terminal while active is pending", NodeStateTerminal, ProgressActive, ControlRunning, "", "", "", ""},
		{"succeeded on success", NodeStateSucceeded, ProgressSucceeded, ControlStopped, "", TaskWaitMet, "progress", "succeeded"},
		{"succeeded on failure is failed", NodeStateSucceeded, ProgressFailed, ControlStopped, "", TaskWaitFailed, "failed", "t"},
		{"succeeded on cancellation", NodeStateSucceeded, ProgressCancelled, ControlStopped, "", TaskWaitCancelled, "", ""},
		{"paused by control", NodeStatePaused, ProgressActive, ControlPaused, "", TaskWaitMet, "control", "paused"},
		{"paused by worker evidence", NodeStatePaused, ProgressActive, ControlRunning, "claudeAgent/claude/seven_day at 97%", TaskWaitMet, "pauseReason", "claudeAgent/claude/seven_day at 97%"},
		{"paused while running is pending", NodeStatePaused, ProgressActive, ControlRunning, "", "", "", ""},
		{"paused after success is failed", NodeStatePaused, ProgressSucceeded, ControlStopped, "", TaskWaitFailed, "progress", "succeeded"},
		{"waiting-external when parked", NodeStateWaitingExternal, ProgressWaitingExternal, ControlWaitingExternal, "", TaskWaitMet, "progress", "waiting-external"},
		{"waiting-external while running is pending", NodeStateWaitingExternal, ProgressActive, ControlRunning, "", "", "", ""},
		{"active when running", NodeStateActive, ProgressActive, ControlRunning, "", TaskWaitMet, "control", "running"},
		{"active while queued is pending", NodeStateActive, ProgressQueued, ControlUnassigned, "", "", "", ""},
		{"active after failure is failed", NodeStateActive, ProgressFailed, ControlStopped, "", TaskWaitFailed, "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			runs, tasks, attempts, assignments, workers := nodeStateFixture(t, c.progress, c.control, c.pauseReason)
			obs, err := ResolveNodeState(NodeRef{RunID: "r", TaskID: "build"}, c.state, runs, tasks, attempts, assignments, workers)
			if err != nil {
				t.Fatal(err)
			}
			if obs.Outcome != c.want {
				t.Fatalf("outcome = %q (%s), want %q", obs.Outcome, obs.Reason, c.want)
			}
			if (obs.ExitCode == 1) != (c.want == "") {
				t.Fatalf("exit = %d for outcome %q", obs.ExitCode, c.want)
			}
			if c.field != "" {
				fields := NodeTrailerFields(obs)
				if fields[c.field] != c.value {
					t.Fatalf("%s = %q, want %q (fields %v)", c.field, fields[c.field], c.value, fields)
				}
			}
			if obs.AttemptID != "a" || obs.RunRevision == 0 {
				t.Fatalf("observation lacks attempt or revision: %+v", obs)
			}
		})
	}
}

// A sink has no attempt to be paused or active; the run-level states are
// terminal and succeeded. The default state is terminal.
func TestResolveNodeStateOnASinkAndTheDefault(t *testing.T) {
	runs, tasks, attempts, assignments, workers := nodeStateFixture(t, ProgressFailed, ControlStopped, "")
	sink := NodeRef{RunID: "r", TaskID: SinkTaskName}
	obs, err := ResolveNodeState(sink, "", runs, tasks, attempts, assignments, workers)
	if err != nil || obs.Outcome != TaskWaitMet {
		t.Fatalf("default state on a failed run: %+v err=%v", obs, err)
	}
	fields := NodeTrailerFields(obs)
	if fields["failed"] != "t" || fields["result"] != "t3-steward task result r" || fields["run"] != "r" {
		t.Fatalf("fields = %v", fields)
	}
	if _, err := ResolveNodeState(sink, NodeStatePaused, runs, tasks, attempts, assignments, workers); err == nil || !strings.Contains(err.Error(), "sink") {
		t.Fatalf("a sink accepted --state paused: %v", err)
	}
	if _, err := ParseNodeWaitState("finished"); err == nil {
		t.Fatal("an unknown state was accepted")
	}
	if state, err := ParseNodeWaitState(""); err != nil || state != NodeStateTerminal {
		t.Fatalf("the default state is %q, %v", state, err)
	}
}
