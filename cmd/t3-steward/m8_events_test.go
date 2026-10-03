package main

import (
	"bytes"
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
)

func TestM8EventsFiltersNamesAndAttemptEventsWithOlderCoordinator(t *testing.T) {
	var out bytes.Buffer
	service := &fakeAdminQueryService{response: backlogadmin.Response{
		Kind: backlogadmin.QueryEvents,
		Task: &backlogadmin.TaskDetail{Task: domain.Task{ID: "task-review", Name: "review"}},
		Events: []backlogadmin.Event{
			{ID: "run-event", Kind: "workflow-run-created"},
			{ID: "review-event", TaskID: "task-review", AttemptID: "a1", Kind: "attempt-active"},
			{ID: "review-assignment", AttemptID: "a1", Kind: "assignment-active"},
			{ID: "review-old-event", TaskID: "task-review", AttemptID: "old", Kind: "attempt-failed"},
			{ID: "review-old-assignment", AttemptID: "old", Kind: "assignment-released"},
			{ID: "implement-event", TaskID: "task-implement", AttemptID: "a2", Kind: "attempt-active"},
		},
	}}
	cli := backlogAdminCLI{service: service, stdout: &out}
	if err := cli.runBacklog(context.Background(), []string{"events", "run-1/review", "--json"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"review-event", "review-assignment", "review-old-assignment"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %s: %s", want, out.String())
		}
	}
	for _, unwanted := range []string{"run-event", "implement-event"} {
		if strings.Contains(out.String(), unwanted) {
			t.Errorf("unrelated %s: %s", unwanted, out.String())
		}
	}
}
