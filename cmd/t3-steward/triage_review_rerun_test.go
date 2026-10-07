package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A recorded rerun resolves only the source run/task action, regardless of the
// rerun's current state or the ordering of summaries. Other failures still need
// attention, including a new exhausted attempt in the rerun itself.
func TestTriageReviewRoundLimitAfterRerun(t *testing.T) {
	for _, progress := range []domain.ProgressState{domain.ProgressActive, domain.ProgressFailed, domain.ProgressCancelled, domain.ProgressSucceeded} {
		for _, asJSON := range []bool{false, true} {
			t.Run(string(progress)+"/"+map[bool]string{false: "text", true: "json"}[asJSON], func(t *testing.T) {
				now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
				gate := &domain.ReviewCompletionGate{Code: domain.ReviewGateRoundLimitExhausted, RoundsUsed: 2, RoundLimit: 2}
				task := func(id string) backlogadmin.TaskDetail {
					return backlogadmin.TaskDetail{Task: domain.Task{ID: id}, Attempt: &domain.Attempt{Progress: domain.ProgressFailed, UpdatedAt: now, ReviewGate: gate}}
				}
				runs := []backlogadmin.WorkflowSummary{
					{Run: domain.WorkflowRun{ID: "run-limit", Progress: domain.ProgressFailed, UpdatedAt: now}, Progress: backlogadmin.Progress{Failed: 2}},
					{Run: domain.WorkflowRun{ID: "run-other", Progress: domain.ProgressFailed, UpdatedAt: now}, Progress: backlogadmin.Progress{Failed: 1}},
					{Run: domain.WorkflowRun{ID: "run-rerun", Progress: progress, UpdatedAt: now, Graph: &domain.GraphDefinition{RerunOf: &domain.RerunProvenance{SourceRunID: "run-limit", SourceTaskID: "build"}}}, Progress: backlogadmin.Progress{Failed: 1}},
				}
				sources := triageSources{
					query: func(_ context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
						response := backlogadmin.Response{GeneratedAt: now}
						switch q.Kind {
						case backlogadmin.QueryWorkflows:
							response.Workflows = runs
						case backlogadmin.QueryWorkflow:
							tasks := []backlogadmin.TaskDetail{task("build")}
							if q.WorkflowRunID == "run-limit" {
								tasks = append(tasks, task("second"))
							}
							response.Workflow = &backlogadmin.WorkflowDetail{Tasks: tasks}
						}
						return response, nil
					},
					nodeWait: func(context.Context, backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
						return backlogadmin.NodeWaitResponse{}, nil
					},
				}
				var out bytes.Buffer
				if err := runTriage(context.Background(), sources, triageOptions{asJSON: asJSON}, &out); err != nil {
					t.Fatal(err)
				}
				if strings.Contains(out.String(), "t3-steward campaign rerun run-limit --from build") {
					t.Fatalf("resolved action still offers duplicate rerun: %s", out.String())
				}
				if asJSON {
					var report triageReport
					if err := json.Unmarshal(out.Bytes(), &report); err != nil {
						t.Fatal(err)
					}
					found := map[string]bool{}
					for _, item := range report.Items {
						if item.Kind == "review-round-limit" {
							found[item.Subject] = true
						}
					}
					if found["run-limit/build"] || len(found) != 3 || !found["run-limit/second"] || !found["run-other/build"] || !found["run-rerun/build"] {
						t.Fatalf("round-limit actions = %#v", found)
					}
				} else {
					if strings.Contains(out.String(), "ACTION review-round-limit run-limit/build") {
						t.Fatal(out.String())
					}
					for _, want := range []string{"ACTION review-round-limit run-limit/second", "ACTION review-round-limit run-other/build", "ACTION review-round-limit run-rerun/build"} {
						if !strings.Contains(out.String(), want) {
							t.Fatalf("missing %q in %s", want, out.String())
						}
					}
				}
			})
		}
	}
}
