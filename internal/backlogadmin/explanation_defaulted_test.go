package backlogadmin

import (
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// explain carries the same project-binding-defaulted detail the readiness
// matrix does, and it changes nothing about eligibility: the detail is not a
// blocker, so a task on a defaulted project is explained exactly as it would
// be on an explicitly bound one, plus one sentence.
func TestExplanationNotesADefaultedProjectBindingWithoutBlocking(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	records := sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow-1", Project: "home-assistant-config", TaskIDs: []string{"task-1"}}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1"}},
		Tasks:        []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "rebuild", Class: domain.TaskClassSurplus}},
	}
	bound := newView(records, nil, nil, RuntimeInfo{}, now)
	reference, ok := bound.explanation("run-1", "task-1")
	if !ok {
		t.Fatal("task not explained")
	}
	if len(reference.Details) != 0 {
		t.Fatalf("an explicitly bound project carries a detail: %v", reference.Details)
	}

	defaulted := newView(records, nil, nil, RuntimeInfo{}, now)
	defaulted.defaultedProjects = []string{"home-assistant-config"}
	explanation, ok := defaulted.explanation("run-1", "task-1")
	if !ok {
		t.Fatal("task not explained")
	}
	if explanation.Eligible != reference.Eligible || len(explanation.Blockers) != len(reference.Blockers) ||
		explanation.Summary != reference.Summary {
		t.Fatalf("the detail changed the verdict: %+v vs %+v", explanation, reference)
	}
	if len(explanation.Details) != 1 || !strings.HasPrefix(explanation.Details[0], ReasonProjectBindingDefaulted+":") ||
		!strings.Contains(explanation.Details[0], "home-assistant-config") {
		t.Fatalf("details = %v", explanation.Details)
	}
	for _, blocker := range explanation.Blockers {
		if blocker.Code == ReasonProjectBindingDefaulted {
			t.Fatal("the detail leaked into the blockers")
		}
	}

	// A task on another project is untouched.
	other := sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow-2", Project: "t3-steward", TaskIDs: []string{"task-2"}}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-2", WorkflowID: "workflow-2"}},
		Tasks:        []domain.Task{{ID: "task-2", WorkflowID: "workflow-2", Name: "build"}},
	}
	v := newView(other, nil, nil, RuntimeInfo{}, now)
	v.defaultedProjects = []string{"home-assistant-config"}
	if explanation, _ := v.explanation("run-2", "task-2"); len(explanation.Details) != 0 {
		t.Fatalf("an unrelated project carries the detail: %v", explanation.Details)
	}
}

// The service wires the configured list into every query's view, so the
// explanation reached over the admin transport carries the detail too.
func TestExplanationQueryCarriesDefaultedProjects(t *testing.T) {
	store := openAdminTestStore(t)
	seedAdminTestStore(t, store)
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if service.viabilitySettings.DefaultedProjects != nil {
		t.Fatal("a fresh service reports defaulted projects")
	}
	view, err := service.loadView(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if view.defaultedProjects != nil {
		t.Fatal("the loaded view reports defaulted projects before any were configured")
	}
}
