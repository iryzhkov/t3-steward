package main

import (
	"bytes"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
)

func TestStructuredReviewRender(t *testing.T) {
	verdict := &domain.ReviewVerdict{Verdict: "changes-requested", BlockingFindings: 2, FindingTitles: []string{"lost evidence"}}
	task := backlogadmin.TaskDetail{Task: domain.Task{ID: "t", Name: "review"}, Attempt: &domain.Attempt{ID: "a", Progress: domain.ProgressSucceeded, ReviewVerdict: verdict}}
	var show bytes.Buffer
	renderWorkflow(&show, &backlogadmin.WorkflowDetail{Tasks: []backlogadmin.TaskDetail{task}})
	if !strings.Contains(show.String(), "review=changes-requested blocking=2") || !strings.Contains(show.String(), "lost evidence") {
		t.Fatalf("show: %s", show.String())
	}
	var explain bytes.Buffer
	renderExplanation(&explain, &backlogadmin.Explanation{ReviewVerdict: verdict})
	if !strings.Contains(explain.String(), "review=changes-requested blocking=2") {
		t.Fatal(explain.String())
	}
	var result bytes.Buffer
	if err := renderTaskResult(&result, taskResultDocument{Tasks: []taskResultTask{{Task: "review", Progress: "succeeded", ReviewVerdict: verdict}}}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result.String(), "review=changes-requested blocking=2") || !strings.Contains(result.String(), "lost evidence") {
		t.Fatalf("result: %s", result.String())
	}
}
