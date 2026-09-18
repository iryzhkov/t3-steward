package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A task that stopped waiting used to leave no trace in the run's text: the
// live wait vanished and nothing said how it ended. The settled wait is now
// printed with its outcome, exit code and reason.
func TestRenderWorkflowPrintsSettledWaitsWithTheirOutcome(t *testing.T) {
	settledAt := time.Date(2026, 9, 18, 11, 0, 0, 0, time.UTC)
	detail := backlogadmin.WorkflowDetail{
		Summary: backlogadmin.WorkflowSummary{
			Run:      domain.WorkflowRun{ID: "run-1", Progress: domain.ProgressActive},
			Workflow: domain.Workflow{ID: "workflow-1", Name: "workflow"},
		},
		Tasks: []backlogadmin.TaskDetail{{
			Task:    domain.Task{ID: "task-1", Name: "alpha"},
			Attempt: &domain.Attempt{ID: "attempt-1", Progress: domain.ProgressActive},
		}},
		TaskWaits: []backlogadmin.TaskWaitDetail{
			{
				ID: "tw-live", TaskID: "task-1", AttemptID: "attempt-1", Name: "ci",
				Condition: "gh run view", Deadline: settledAt.Add(time.Hour),
			},
			{
				ID: "tw-settled", TaskID: "task-1", AttemptID: "attempt-1", Name: "review",
				Condition: "gh pr view", Outcome: string(domain.TaskWaitFailed), ExitCode: 2,
				Reason: "the check failed", SettledAt: &settledAt,
			},
		},
	}
	var out bytes.Buffer
	renderWorkflow(&out, &detail)
	text := out.String()
	for _, want := range []string{
		`wait tw-live "ci": gh run view`,
		`wait tw-settled "review": settled failed exit=2`,
		"the check failed",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("the run text does not contain %q:\n%s", want, text)
		}
	}
}

// "which worker could run this" was answerable only from the JSON: the text
// dropped the projects and the provider routes entirely (F-20).
func TestRenderWorkersPrintsProjectsAndRoutes(t *testing.T) {
	workers := []backlogadmin.Worker{{
		State: "observed", Enrolled: true, Health: string(domain.WorkerHealthReady),
		Snapshot: domain.WorkerSnapshot{
			WorkerID: "omarchy-pc", Connected: true,
			Inventory: domain.WorkerInventory{
				CatalogRevision: "catalog-1", AcceptBacklog: true,
				Projects: []domain.WorkerProjectInventory{
					{Name: "steward", Available: true},
					{Name: "homelab", Available: false},
				},
				Providers: []domain.WorkerProviderInventory{{
					InstanceID: "t3-primary", QuotaPoolID: "pool-claude",
					Available: true, Models: []string{"opus", "claude-haiku-4-5"},
				}},
			},
		},
	}}
	var out bytes.Buffer
	if err := renderWorkers(&out, workers); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"WORKER", "omarchy-pc offers:", "PROJECTS", "ROUTES (instance/model@pool)",
		"steward", "homelab (unavailable)",
		"t3-primary/claude-haiku-4-5@pool-claude", "t3-primary/opus@pool-claude",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("the workers text does not contain %q:\n%s", want, text)
		}
	}
}
