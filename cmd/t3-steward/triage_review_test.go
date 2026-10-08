package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestTriageReviewRoundLimitTextAndJSON(t *testing.T) {
	for _, progress := range []domain.ProgressState{domain.ProgressActive, domain.ProgressFailed, domain.ProgressCancelled} {
		for _, asJSON := range []bool{false, true} {
			t.Run(string(progress)+"/"+map[bool]string{false: "text", true: "json"}[asJSON], func(t *testing.T) {
				now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
				gate := new(domain.ReviewCompletionGate)
				if err := json.Unmarshal([]byte(`{"code":"review-round-limit-exhausted","roundsUsed":2,"roundLimit":2}`), gate); err != nil {
					t.Fatal(err)
				}
				tasks := []backlogadmin.TaskDetail{
					{Task: domain.Task{ID: "build"}, Attempt: &domain.Attempt{Progress: domain.ProgressFailed, UpdatedAt: now, ReviewGate: gate}},
					{Task: domain.Task{ID: "second"}, Attempt: &domain.Attempt{Progress: domain.ProgressFailed, UpdatedAt: now, ReviewGate: gate}},
					{Task: domain.Task{ID: "other"}, Attempt: &domain.Attempt{Progress: domain.ProgressFailed, ReviewGate: &domain.ReviewCompletionGate{Code: domain.ReviewGateDirtyTree}}},
					{Task: domain.Task{ID: "retried"}, Attempt: &domain.Attempt{Progress: domain.ProgressActive, ReviewGate: gate}},
					{Task: domain.Task{ID: "no-attempt"}},
				}
				reads := 0
				sources := triageSources{
					query: func(_ context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
						response := backlogadmin.Response{GeneratedAt: now}
						switch q.Kind {
						case backlogadmin.QueryWorkflows:
							response.Workflows = []backlogadmin.WorkflowSummary{{Run: domain.WorkflowRun{ID: "run-limit", Progress: progress, UpdatedAt: now}, Progress: backlogadmin.Progress{Failed: 2}}}
						case backlogadmin.QueryWorkflow:
							reads++
							if q.WorkflowRunID != "run-limit" {
								t.Fatalf("run query = %q", q.WorkflowRunID)
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
				if reads != 1 {
					t.Fatalf("detail reads = %d, want 1; output %s", reads, out.String())
				}
				if asJSON {
					var report triageReport
					if err := json.Unmarshal(out.Bytes(), &report); err != nil {
						t.Fatal(err)
					}
					if len(report.Items) != 2 {
						t.Fatalf("items = %#v", report.Items)
					}
					item := report.Items[0]
					if item.Kind != "review-round-limit" || item.Severity != "action" || item.Run != "run-limit" || item.Subject != "run-limit/build" {
						t.Fatalf("item = %#v", item)
					}
					if !strings.Contains(item.Summary, "2 of 2") {
						t.Fatalf("summary = %s", item.Summary)
					}
					if len(item.Commands) != 2 || item.Commands[1].Run != "t3-steward campaign rerun run-limit --from build" {
						t.Fatalf("commands = %#v", item.Commands)
					}
				} else {
					for _, want := range []string{"ACTION review-round-limit run-limit/build", "ACTION review-round-limit run-limit/second", "2 of 2", "t3-steward campaign rerun run-limit --from build"} {
						if !strings.Contains(out.String(), want) {
							t.Fatalf("missing %q in %s", want, out.String())
						}
					}
					if strings.Contains(out.String(), "Nothing needs an operator") {
						t.Fatal(out.String())
					}
				}
			})
		}
	}
}

func TestTriageReviewRoundLimitUnreadDetail(t *testing.T) {
	for _, missing := range []bool{false, true} {
		sources := triageSources{
			query: func(_ context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
				if q.Kind == backlogadmin.QueryWorkflows {
					return backlogadmin.Response{Workflows: []backlogadmin.WorkflowSummary{{Run: domain.WorkflowRun{ID: "failed", Progress: domain.ProgressFailed}}}}, nil
				}
				if q.Kind == backlogadmin.QueryWorkflow && !missing {
					return backlogadmin.Response{}, errors.New("detail unavailable")
				}
				return backlogadmin.Response{}, nil
			},
			nodeWait: func(context.Context, backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
				return backlogadmin.NodeWaitResponse{}, nil
			},
		}
		var out bytes.Buffer
		if err := runTriage(context.Background(), sources, triageOptions{}, &out); err == nil {
			t.Fatalf("missing=%v: succeeded: %s", missing, out.String())
		}
		if !strings.Contains(out.String(), "NOT READ: review round limits") || strings.Contains(out.String(), "Nothing needs an operator") {
			t.Fatal(out.String())
		}
	}
}
