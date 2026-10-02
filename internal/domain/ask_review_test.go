package domain

import (
	"strings"
	"testing"
	"time"
)

// Review findings 1 and 6: an approver ask takes no default, and a default
// obeys the rules an answer obeys.
func TestAskDefaultsFollowTheAnswerRules(t *testing.T) {
	base := AskRequest{Question: "Pick", Options: []string{"alpha", "beta"}, OnDeadline: AskDeadlineDefault, Default: []string{"alpha"}}
	approver := base
	approver.Requires = AskRequiresApprover
	if err := approver.Validate(); err == nil {
		t.Fatal("an approver ask with a default was accepted")
	}
	repeated := base
	repeated.Multi, repeated.Default = true, []string{"alpha", "alpha"}
	if err := repeated.Validate(); err == nil {
		t.Fatal("a repeated default was accepted")
	}
}

// Review finding 3: an ask that ended without an answer gives the final
// instruction itself; the wake does not also tell the task to write its
// outputs.
func TestAnUnansweredAskWakeEndsWithItsOwnInstruction(t *testing.T) {
	for _, outcome := range []TaskWaitOutcome{TaskWaitTimedOut, TaskWaitCancelled} {
		wake := TaskWaitWakeContext{Waits: []TaskWait{{
			ID: "tw-ask", Kind: WaitKindAsk, Resumption: true,
			Ask:    &AskRequest{Question: "Pick", Options: []string{"a", "b"}, OnDeadline: AskDeadlineFail},
			Result: &TaskWaitResult{Outcome: outcome, RanFor: time.Minute},
		}}}
		if prompt := wake.Prompt(); strings.Contains(prompt, "Write every declared output") {
			t.Fatalf("%s wake also says to write the outputs:\n%s", outcome, prompt)
		}
	}
}

func TestTruncateUTF8KeepsCharactersWhole(t *testing.T) {
	if got := TruncateUTF8("aé", 2); got != "a" {
		t.Fatalf("TruncateUTF8 = %q", got)
	}
}
