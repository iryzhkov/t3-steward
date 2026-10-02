package domain

import (
	"errors"
	"fmt"
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

// Review finding 11 (its coordinator half): a refusal the coordinator will
// repeat is recognisable, also as the bare message the admin transport
// carries, and a transport failure is not one.
func TestAskAnswerRefusalsAreRecognisable(t *testing.T) {
	for _, err := range []error{
		fmt.Errorf("%w: %q is not one of the options", ErrAskAnswerRefused, "gamma"),
		errors.New("coordinator rejected: ask answer refused: a T3 answer must come from the ask's own relay thread"),
		fmt.Errorf("%w: an answer given in T3 is refused", ErrAskApproverRequired),
	} {
		if !IsAskAnswerRefusal(err) {
			t.Errorf("not recognised as a refusal: %v", err)
		}
	}
	if IsAskAnswerRefusal(errors.New("coordinator-exchange: no coordinator answered")) {
		t.Error("a transport failure was read as a refusal")
	}
}

func TestTruncateUTF8KeepsCharactersWhole(t *testing.T) {
	if got := TruncateUTF8("aé", 2); got != "a" {
		t.Fatalf("TruncateUTF8 = %q", got)
	}
}
