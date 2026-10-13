package backlogadmin

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

func TestRunReprioritizeChangesRelativePlannerOrder(t *testing.T) {
	ctx := context.Background()
	store, path, service := reprioritizeSubmissionFixture(t)
	records, err := store.LoadCoordinatorRecords(ctx)
	reprioritizeRequireNoError(t, err)
	for i := range records.Workflows {
		records.Workflows[i].Project = "project"
		records.Workflows[i].Environment = domain.ExecutionEnvironment{Scope: backlog.EnvironmentScopeTask}
	}
	for i := range records.Tasks {
		records.Tasks[i].Importance = 2
	}
	for i := range records.Attempts {
		records.Attempts[i].Progress, records.Attempts[i].Control = domain.ProgressReady, domain.ControlUnassigned
	}
	records.Workflows = append(records.Workflows, domain.Workflow{ID: "workflow-2", Version: 2, Name: "other", Project: "project", Class: domain.TaskClassRequired,
		TaskIDs: []string{"task-2"}, Environment: domain.ExecutionEnvironment{Scope: backlog.EnvironmentScopeTask}})
	records.WorkflowRuns = append(records.WorkflowRuns, domain.WorkflowRun{ID: "run-2", WorkflowID: "workflow-2", Progress: domain.ProgressReady, Revision: 1, CreatedAt: adminTestNow, UpdatedAt: adminTestNow})
	records.Tasks = append(records.Tasks, domain.Task{ID: "task-2", WorkflowID: "workflow-2", Name: "other", Class: domain.TaskClassRequired, Importance: 3})
	records.Attempts = append(records.Attempts, domain.Attempt{ID: "attempt-2", WorkflowRunID: "run-2", TaskID: "task-2", Number: 1, Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, UpdatedAt: adminTestNow})
	reprioritizeRequireNoError(t, store.SaveCoordinatorRecords(ctx, records))
	before, err := store.LoadCoordinatorRecords(ctx)
	reprioritizeRequireNoError(t, err)
	plan := func(loaded sqlite.CoordinatorRecords, winner, loser string, importance int) {
		t.Helper()
		_, input := planningExplainFixture()
		input.Workflows, input.QuotaPools = nil, nil
		input.Ordering.Attempts = make(map[string]backlog.PlanningAttemptOrdering)
		for _, run := range loaded.WorkflowRuns {
			state := backlog.DAGState{Run: run}
			var workflow domain.Workflow
			for _, wf := range loaded.Workflows {
				if wf.ID == run.WorkflowID {
					workflow = wf
				}
			}
			for _, task := range loaded.Tasks {
				if task.WorkflowID == run.WorkflowID {
					state.Tasks = append(state.Tasks, task)
					for _, attempt := range loaded.Attempts {
						if attempt.WorkflowRunID == run.ID && attempt.TaskID == task.ID {
							state.Attempts = append(state.Attempts, attempt)
							input.Ordering.Attempts[attempt.ID] = backlog.PlanningAttemptOrdering{ReadySince: adminTestNow}
						}
					}
				}
			}
			input.Workflows = append(input.Workflows, backlog.PlanningWorkflow{Workflow: workflow, State: state})
		}
		got, err := backlog.BuildPlan(input)
		reprioritizeRequireNoError(t, err)
		positions := map[string]int{}
		for i, decision := range got.Decisions {
			positions[decision.AttemptID] = i
			if decision.AttemptID == winner && (!decision.Proposed || decision.Order.Importance != importance || !strings.Contains(decision.Order.Reason, fmt.Sprintf("importance %d;", importance))) {
				t.Fatalf("winner decision: %#v", decision)
			}
		}
		win, winOK := positions[winner]
		lose, loseOK := positions[loser]
		if !winOK || !loseOK || win >= lose {
			t.Fatalf("relative order: %v, want %s before %s", positions, winner, loser)
		}
	}
	plan(before, "attempt-2", "attempt-1", 3)
	request := reprioritizeSubmissionRequest()
	for _, run := range before.WorkflowRuns {
		if run.ID == request.WorkflowRunID {
			request.ExpectedRevision = run.Revision
		}
	}
	submitted, err := service.Mutate(ctx, request)
	reprioritizeRequireNoError(t, err)
	if submitted.Command.ID != request.ID || submitted.Command.State != domain.AdminCommandPending {
		t.Fatalf("submission: %#v", submitted)
	}
	report, err := service.ExecutePendingCommands(ctx)
	reprioritizeRequireNoError(t, err)
	applied := false
	for _, decision := range report.Decisions {
		applied = applied || decision.Command.ID == request.ID && decision.Command.State == domain.AdminCommandApplied
	}
	if !applied {
		t.Fatalf("application: %#v", report)
	}
	reprioritizeRequireNoError(t, store.Close())
	store, err = sqlitetest.OpenMigrated(path)
	reprioritizeRequireNoError(t, err)
	defer store.Close()
	after, err := store.LoadCoordinatorRecords(ctx)
	reprioritizeRequireNoError(t, err)
	for _, attempt := range after.Attempts {
		if attempt.ID == "attempt-1" && (attempt.AdminImportance == nil || *attempt.AdminImportance != 5) || attempt.ID == "attempt-2" && attempt.AdminImportance != nil {
			t.Fatalf("reloaded override: %#v", attempt)
		}
	}
	definitions := func(r sqlite.CoordinatorRecords) any {
		workflows, tasks := map[string]domain.Workflow{}, map[string]domain.Task{}
		for _, wf := range r.Workflows {
			workflows[wf.ID] = wf
		}
		for _, task := range r.Tasks {
			tasks[task.ID] = task
		}
		return []any{workflows, tasks}
	}
	if !reflect.DeepEqual(definitions(before), definitions(after)) {
		t.Fatal("persisted definitions changed")
	}
	plan(after, "attempt-1", "attempt-2", 5)
}
