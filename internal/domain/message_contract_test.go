package domain

import (
	"strings"
	"testing"
)

func TestWakeMessageContractTaskOutcomesAndAskException(t *testing.T) {
	for _, outcome := range []TaskWaitOutcome{TaskWaitMet, TaskWaitFailed, TaskWaitCancelled, TaskWaitGaveUp, TaskWaitTimedOut} {
		for _, expected := range []bool{false, true} {
			w := TaskWait{ID: "w", Name: "review", OrTimeout: expected, Result: &TaskWaitResult{Outcome: outcome, ExitCode: 0, Output: "REJECT"}}
			text := (TaskWaitWakeContext{Waits: []TaskWait{w}}).Prompt()
			if !strings.Contains(text, "inspect") || !strings.Contains(text, "review ACCEPT") || !strings.Contains(text, "Cancellation and pause") {
				t.Fatal("unconditional task wake", text)
			}
			if !strings.Contains(text, "REJECT") {
				t.Fatal("lost verdict")
			}
			if outcome == TaskWaitTimedOut && expected && !strings.Contains(text, "a normal outcome") {
				t.Fatal("lost expected deadline")
			}
		}
	}
	w := TaskWait{ID: "ask", Ask: &AskRequest{}, Result: &TaskWaitResult{Outcome: TaskWaitCancelled}}
	text := (TaskWaitWakeContext{Waits: []TaskWait{w}}).Prompt()
	if strings.Contains(text, "Write every declared output") || strings.Contains(text, "Continue only unfinished work") {
		t.Fatal("unanswered ask exception overwritten", text)
	}
}
func TestWakeMessageContractTerminalFailureIsMet(t *testing.T) {
	runs, tasks, attempts, assignments, workers := nodeStateFixture(t, ProgressFailed, ControlStopped, "")
	obs, err := ResolveNodeState(NodeRef{RunID: "r", TaskID: SinkTaskName}, NodeStateTerminal, runs, tasks, attempts, assignments, workers)
	if err != nil || obs.Outcome != TaskWaitMet || obs.Progress != ProgressFailed {
		t.Fatal(obs, err)
	}
	result := TaskWaitResult{Outcome: obs.Outcome, ExitCode: obs.ExitCode, Reason: obs.Reason, Fields: NodeTrailerFields(obs)}
	text := (TaskWaitWakeContext{Waits: []TaskWait{{ID: "terminal", Result: &result}}}).Prompt()
	if !strings.Contains(text, "does not establish task success") || result.Fields["failed"] != "t" {
		t.Fatal("failed terminal confused with success", text)
	}
}
