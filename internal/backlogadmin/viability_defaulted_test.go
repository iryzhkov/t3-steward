package backlogadmin

import (
	"context"
	"strings"
	"testing"
)

// A project the coordinator loaded with default local bindings is reported on
// every candidate as an informational detail. It is not a reason: the project
// runs, and a note that turned a ready fleet into accepted_waiting would be a
// blocker wearing an informational label.
func TestViabilityNotesADefaultedProjectBindingWithoutBlocking(t *testing.T) {
	v := viabilityView(t, nil)
	settings := viabilityCatalog(t)
	settings.DefaultedProjects = []string{"t3-steward"}
	matrix := v.viability(context.Background(), settings, ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}})
	if matrix.Outcome != ViabilityReady {
		t.Fatalf("outcome = %s, want ready; a defaulted binding must not block", matrix.Outcome)
	}
	candidate := matrix.Tasks[0].Candidates[0]
	found := false
	for _, note := range candidate.Unchecked {
		if strings.HasPrefix(note, ReasonProjectBindingDefaulted+":") && strings.Contains(note, "t3-steward") {
			found = true
		}
	}
	if !found {
		t.Fatalf("candidate does not carry the %s detail: %+v", ReasonProjectBindingDefaulted, candidate.Unchecked)
	}
	if len(candidate.Reasons) != 0 {
		t.Fatalf("the detail leaked into the reasons: %+v", candidate.Reasons)
	}
	if PermanentViabilityReason(ReasonProjectBindingDefaulted) {
		t.Fatal("the detail is in the permanent reason set")
	}
	// A project with an explicit binding carries no such note.
	settings.DefaultedProjects = nil
	matrix = v.viability(context.Background(), settings, ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}})
	for _, note := range matrix.Tasks[0].Candidates[0].Unchecked {
		if strings.Contains(note, ReasonProjectBindingDefaulted) {
			t.Fatalf("an explicitly bound project was reported as defaulted: %q", note)
		}
	}
}
