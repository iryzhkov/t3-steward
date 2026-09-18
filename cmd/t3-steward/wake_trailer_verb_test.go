package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The wake trailer of a terminal node wait hands the woken agent a command
// line, so the verb it names has to be a verb this binary dispatches. The
// assertion is made against the dispatcher and not against a pasted literal:
// the trailer's own words are routed through dispatch, which means a later
// rename of the verb cannot leave the trailer behind without a failing test.
//
// --help is appended to the verb so that the routing is exercised without the
// command reaching a coordinator, loading a configuration or writing a file.
func TestTheTerminalWakeTrailerNamesAVerbTheCLIDispatches(t *testing.T) {
	const runID = "run-2f1c9ade"
	fields := domain.NodeTrailerFields(domain.NodeObservation{
		Target:   domain.NodeRef{RunID: runID, TaskID: "sink:" + runID},
		Progress: domain.ProgressSucceeded,
	})
	command := fields["result"]
	if command == "" {
		t.Fatal("a terminal node observation produced no result= pair for the wake trailer")
	}
	words := strings.Fields(command)
	if len(words) < 3 || words[0] != "t3-steward" || words[len(words)-1] != runID {
		t.Fatalf("result = %q, want a command of the form \"t3-steward <verb> %s\"", command, runID)
	}
	verb := words[1 : len(words)-1]
	if err := dispatch(append(append([]string{}, verb...), "--help")); err != nil {
		t.Fatalf("the wake trailer tells a woken agent to run %q, but dispatching %q reaches no command: %v",
			command, strings.Join(verb, " "), err)
	}
}

// The guard above is only worth having if the dispatcher really does refuse a
// verb it does not route, in both the shapes the trailer could take: a single
// top-level word, and a word inside the task family.
func TestTheDispatcherRefusesAVerbItDoesNotRoute(t *testing.T) {
	if err := dispatch([]string{"result"}); !errors.Is(err, errUnknownCommand) {
		t.Fatalf("a top-level verb that does not exist: error = %v, want errUnknownCommand", err)
	}
	if err := dispatch([]string{"task", "collect"}); !errors.Is(err, errUnknownTaskCommand) {
		t.Fatalf("a task verb that does not exist: error = %v, want errUnknownTaskCommand", err)
	}
}
