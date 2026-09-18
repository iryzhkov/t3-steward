package main

import (
	"strings"
	"testing"
)

// A progress filter that names no known state is refused with the states it
// could have named. "invalid progress" alone sent an operator to the source to
// find out that the state they wanted is spelled active.
func TestProgressFilterRefusalListsTheValidValues(t *testing.T) {
	_, err := parseWorkflowFilters([]string{"--progress", "running"})
	if err == nil {
		t.Fatal("--progress running was accepted")
	}
	want := `invalid progress "running"; valid values: queued, blocked, ready, active, needs-input, waiting-external, verifying, succeeded, failed, cancelled, skipped`
	if err.Error() != want {
		t.Fatalf("error = %q\nwant    %q", err, want)
	}
}

// The verbs that only take a show form say so, and when the caller supplied
// what looks like the identifier without the show word, the refusal spells out
// the command they probably meant.
func TestShowOnlyVerbsSuggestTheShowForm(t *testing.T) {
	tests := []struct {
		args []string
		want string
	}{
		{[]string{"command", "admin-a9d88bf818bde6b032d6baaf"},
			"usage: backlog command show <command>; did you mean: backlog command show admin-a9d88bf818bde6b032d6baaf?"},
		{[]string{"command"}, "usage: backlog command show <command>"},
		{[]string{"command", "show"}, "usage: backlog command show <command>"},
		{[]string{"command", "show", "a", "b"}, "usage: backlog command show <command>"},
		{[]string{"task", "run-1/task-1"},
			"usage: backlog task show <workflow-run>/<task>; did you mean: backlog task show run-1/task-1?"},
		{[]string{"task"}, "usage: backlog task show <workflow-run>/<task>"},
		{[]string{"artifact", "artifact-1"},
			"usage: backlog artifact show <artifact>; did you mean: backlog artifact show artifact-1?"},
		{[]string{"artifact"}, "usage: backlog artifact show <artifact>"},
	}
	for _, test := range tests {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			_, _, err := parseBacklogAdminQuery(test.args)
			if err == nil {
				t.Fatalf("%v was accepted", test.args)
			}
			if err.Error() != test.want {
				t.Fatalf("error = %q\nwant    %q", err, test.want)
			}
		})
	}
}
