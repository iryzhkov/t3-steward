package main

import (
	"context"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// "campaign explain <run>" names no task, and explain is about one task. It is
// refused with the run's tasks and their state, so the caller can choose one,
// rather than forwarded to a verb that answered with a bare format error. A
// run/task target is forwarded as it always was.
func TestCampaignExplainOfARunListsItsTasks(t *testing.T) {
	var forwarded [][]string
	cli := campaignCLI{
		admin: func(args []string) error {
			forwarded = append(forwarded, args)
			return nil
		},
		detail: func(_ context.Context, run string) (backlogadmin.WorkflowDetail, error) {
			return backlogadmin.WorkflowDetail{Tasks: []backlogadmin.TaskDetail{
				{Task: domain.Task{ID: "task-1", Name: "draft"}, Attempt: &domain.Attempt{ID: "a-1", Progress: domain.ProgressSucceeded}},
				{Task: domain.Task{ID: "task-2", Name: "review"}},
				{Task: domain.Task{ID: "sink", Name: "__sink"}, Sink: &domain.SinkTask{}},
			}}, nil
		},
	}
	err := cli.run(context.Background(), []string{"explain", "run-1"})
	if err == nil {
		t.Fatal("campaign explain <run> was accepted")
	}
	want := "campaign explain needs <run>/<task>, such as run-1/<task>; run run-1 has tasks draft (succeeded), review (queued)"
	if err.Error() != want {
		t.Fatalf("error = %q\nwant    %q", err, want)
	}
	if len(forwarded) != 0 {
		t.Fatalf("a refused explain was forwarded: %v", forwarded)
	}
	if err := cli.run(context.Background(), []string{"explain", "run-1/review", "--json"}); err != nil {
		t.Fatal(err)
	}
	if len(forwarded) != 1 || strings.Join(forwarded[0], " ") != "explain run-1/review --json" {
		t.Fatalf("forwarded = %v", forwarded)
	}
}
