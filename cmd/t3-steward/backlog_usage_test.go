package main

import (
	"strings"
	"testing"
)

// F-13: every revision-fenced control the parser accepts is named in the
// usage; rewake was added to the parser without a usage line.
func TestBacklogUsageNamesEveryMutation(t *testing.T) {
	for _, verb := range []string{"start", "resume", "cancel", "retry", "skip", "delay", "pause", "rewake"} {
		if !isBacklogMutation(verb) {
			t.Fatalf("%s is not a backlog mutation", verb)
		}
		if !strings.Contains(backlogUsage, verb) {
			t.Errorf("backlog usage does not document %q", verb)
		}
	}
	if !strings.Contains(backlogUsage, "rewake <workflow-run>/<task> --reason TEXT") {
		t.Errorf("backlog usage does not show the rewake form")
	}
}
