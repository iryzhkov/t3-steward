package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// campaign show prints why a failed task failed under the task's line, so a
// refused turn start (or any other failure) is readable without a second
// "task show" per task.
func TestCampaignShowNamesAFailedTasksReason(t *testing.T) {
	const reason = "T3 refused to start the provider turn: ProviderValidationError: Expected a value with a length of at most 120000"
	detail := backlogadmin.WorkflowDetail{
		Summary: backlogadmin.WorkflowSummary{Run: domain.WorkflowRun{ID: "run-1", Progress: domain.ProgressFailed}},
		Tasks: []backlogadmin.TaskDetail{
			{Task: domain.Task{ID: "task-1", Name: "review"}, Attempt: &domain.Attempt{ID: "attempt-1", Progress: domain.ProgressFailed, Failure: reason}},
			{Task: domain.Task{ID: "task-2", Name: "build"}, Attempt: &domain.Attempt{ID: "attempt-2", Progress: domain.ProgressSucceeded}},
		},
	}
	var out bytes.Buffer
	renderWorkflow(&out, &detail)
	text := out.String()
	review := strings.Index(text, "  review (task-1)")
	failure := strings.Index(text, "    failure: "+reason+"\n")
	build := strings.Index(text, "  build (task-2)")
	if review < 0 || failure < 0 || build < 0 || !(review < failure && failure < build) {
		t.Fatalf("the failure is not printed under its task:\n%s", text)
	}
	if strings.Count(text, "failure:") != 1 {
		t.Fatalf("a task without a failure printed one:\n%s", text)
	}
	// campaign show --json prints the coordinator's response, whose attempt
	// record carries the same reason.
	encoded, err := json.Marshal(backlogadmin.Response{Version: backlogadmin.Version, Kind: backlogadmin.QueryWorkflow, Workflow: &detail})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"failure":"`+reason+`"`) {
		t.Fatalf("the JSON answer does not carry the failure: %s", encoded)
	}
}
