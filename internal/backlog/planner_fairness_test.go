package backlog

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestBuildPlanOrdersByDeadlineThenFairness(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(older, newer *domain.Task, input *PlanInput)
		wantTaskID string
		wantRisk   bool
	}{
		{
			name: "deadline risk precedes accumulated fairness",
			configure: func(older, newer *domain.Task, input *PlanInput) {
				older.Importance = 10
				input.Ordering.Attempts["older-1"] = PlanningAttemptOrdering{
					ReadySince: plannerTestTime.Add(-24 * time.Hour), PriorDeferrals: 12,
				}
				newer.Class = domain.TaskClassRequired
				deadline := plannerTestTime.Add(30 * time.Minute)
				newer.Deadline = &deadline
			},
			wantTaskID: "task-newer",
			wantRisk:   true,
		},
		{
			name: "importance precedes fairness",
			configure: func(older, newer *domain.Task, input *PlanInput) {
				older.Importance = 2
				newer.Importance = 5
				input.Ordering.Attempts["older-1"] = PlanningAttemptOrdering{
					ReadySince: plannerTestTime.Add(-24 * time.Hour), PriorDeferrals: 12,
				}
			},
			wantTaskID: "task-newer",
		},
		{
			name: "repeated deferrals prevent starvation",
			configure: func(older, newer *domain.Task, input *PlanInput) {
				input.Ordering.Attempts["newer-1"] = PlanningAttemptOrdering{
					ReadySince: plannerTestTime, PriorDeferrals: 3,
				}
			},
			wantTaskID: "task-newer",
		},
		{
			name: "older ready task wins after equal deferrals",
			configure: func(older, newer *domain.Task, input *PlanInput) {
				input.Ordering.Attempts["older-1"] = PlanningAttemptOrdering{
					ReadySince: plannerTestTime.Add(-time.Hour),
				}
			},
			wantTaskID: "task-older",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			older := testTask("older")
			newer := testTask("newer")
			older.ResourceLocks = []string{"shared"}
			newer.ResourceLocks = []string{"shared"}
			input := plannerInput([]domain.Task{newer, older}, []domain.WorkerInventory{plannerWorker("worker")})
			test.configure(&older, &newer, &input)
			input.Workflows[0].State.Tasks = []domain.Task{newer, older}

			got, err := BuildPlan(input)
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}
			if len(got.Proposals) != 1 || got.Proposals[0].TaskID != test.wantTaskID {
				t.Fatalf("proposals = %#v, want only %q", got.Proposals, test.wantTaskID)
			}
			winner := plannerDecision(t, got, strings.TrimPrefix(test.wantTaskID, "task-"))
			if winner.Order.DeadlineRisk != test.wantRisk {
				t.Fatalf("winner order = %#v, deadline risk want %v", winner.Order, test.wantRisk)
			}
			if winner.Order.Reason == "" || !strings.Contains(winner.Order.Reason, "workflow round") {
				t.Fatalf("winner ordering reason = %q", winner.Order.Reason)
			}
		})
	}
}

func TestBuildPlanRotatesEqualPriorityWorkflowsDeterministically(t *testing.T) {
	runA := fairnessWorkflow("run-a", "alpha", "beta")
	runB := fairnessWorkflow("run-b", "gamma")
	first := fairnessPlanInput(runA, runB)
	second := fairnessPlanInput(runB, runA)
	second.Workflows[1].State.Tasks[0], second.Workflows[1].State.Tasks[1] =
		second.Workflows[1].State.Tasks[1], second.Workflows[1].State.Tasks[0]

	got, err := BuildPlan(first)
	if err != nil {
		t.Fatalf("BuildPlan(first): %v", err)
	}
	reordered, err := BuildPlan(second)
	if err != nil {
		t.Fatalf("BuildPlan(second): %v", err)
	}
	if !reflect.DeepEqual(got, reordered) {
		t.Fatalf("plan depends on workflow or task input order:\nfirst: %#v\nsecond: %#v", got, reordered)
	}
	want := []string{"run-a/alpha", "run-b/gamma", "run-a/beta"}
	if order := proposalOrder(got); !reflect.DeepEqual(order, want) {
		t.Fatalf("proposal order = %v, want %v", order, want)
	}
	if got.Decisions[0].Order.WorkflowRound != 0 ||
		got.Decisions[1].Order.WorkflowRound != 0 ||
		got.Decisions[2].Order.WorkflowRound != 1 {
		t.Fatalf("workflow rounds = %d, %d, %d, want 0, 0, 1",
			got.Decisions[0].Order.WorkflowRound,
			got.Decisions[1].Order.WorkflowRound,
			got.Decisions[2].Order.WorkflowRound)
	}
}

func TestBuildPlanFairnessInteractsWithRouteQuotaAndResourceContention(t *testing.T) {
	deadline := plannerTestTime.Add(15 * time.Minute)
	urgent := testTask("urgent")
	urgent.Class = domain.TaskClassRequired
	urgent.Deadline = &deadline
	urgent.ResourceLocks = []string{"shared"}
	deferred := testTask("deferred")
	deferred.ResourceLocks = []string{"shared"}
	normal := testTask("normal")
	normal.ResourceLocks = []string{"shared"}
	input := plannerInput([]domain.Task{normal, deferred, urgent}, []domain.WorkerInventory{plannerWorker("worker")})
	input.Ordering.Attempts["deferred-1"] = PlanningAttemptOrdering{
		ReadySince: plannerTestTime.Add(-time.Hour), PriorDeferrals: 5,
	}
	input.Constraints = []PlanningConstraint{planningConstraintFunc(func(candidate PlanningCandidate) []PlanningBlocker {
		if candidate.Task.Name == "urgent" {
			return []PlanningBlocker{{Code: "quota", Detail: "urgent route quota is unavailable"}}
		}
		return nil
	})}

	got, err := BuildPlan(input)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if len(got.Proposals) != 1 || got.Proposals[0].TaskID != deferred.ID {
		t.Fatalf("proposals = %#v, want deferred fallback", got.Proposals)
	}
	urgentDecision := plannerDecision(t, got, "urgent")
	if !urgentDecision.Order.DeadlineRisk ||
		!hasPlanningBlocker(urgentDecision.Blockers, PlanningBlockerCandidatePolicy, "") ||
		len(urgentDecision.Candidates) != 1 ||
		!hasPlanningBlocker(urgentDecision.Candidates[0].Blockers, "quota", "") {
		t.Fatalf("urgent decision = %#v, want deadline-first quota exclusion", urgentDecision)
	}
	deferredDecision := plannerDecision(t, got, "deferred")
	if !deferredDecision.Proposed || deferredDecision.Order.PriorDeferrals != 5 {
		t.Fatalf("deferred decision = %#v, want fairness fallback proposal", deferredDecision)
	}
	normalDecision := plannerDecision(t, got, "normal")
	if !hasPlanningOwnershipBlocker(normalDecision.Blockers, PlanningBlockerResource, "shared", deferredDecision.AttemptID) {
		t.Fatalf("normal decision = %#v, want resource contention after deferred proposal", normalDecision)
	}
}

func TestBuildPlanRejectsInvalidOrderingHistory(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*PlanInput)
		wantError string
	}{
		{
			name: "missing attempt",
			mutate: func(input *PlanInput) {
				delete(input.Ordering.Attempts, "alpha-1")
			},
			wantError: "missing ordering history",
		},
		{
			name: "future ready time",
			mutate: func(input *PlanInput) {
				input.Ordering.Attempts["alpha-1"] = PlanningAttemptOrdering{
					ReadySince: plannerTestTime.Add(time.Second),
				}
			},
			wantError: "ready time is in the future",
		},
		{
			name: "negative deferrals",
			mutate: func(input *PlanInput) {
				input.Ordering.Attempts["alpha-1"] = PlanningAttemptOrdering{
					ReadySince: plannerTestTime, PriorDeferrals: -1,
				}
			},
			wantError: "prior deferrals must not be negative",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := plannerInput([]domain.Task{testTask("alpha")}, []domain.WorkerInventory{plannerWorker("worker")})
			test.mutate(&input)
			if _, err := BuildPlan(input); err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("BuildPlan error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

func fairnessWorkflow(runID string, names ...string) PlanningWorkflow {
	workflowID := "workflow-" + runID
	state := DAGState{
		Run: domain.WorkflowRun{
			ID: runID, WorkflowID: workflowID, Progress: domain.ProgressQueued,
			Revision: 1, CreatedAt: plannerTestTime, UpdatedAt: plannerTestTime,
		},
	}
	for _, name := range names {
		taskID := runID + "-" + name
		state.Tasks = append(state.Tasks, domain.Task{ID: taskID, WorkflowID: workflowID, Name: name})
		state.Attempts = append(state.Attempts, domain.Attempt{
			ID: runID + "-" + name + "-1", WorkflowRunID: runID, TaskID: taskID, Number: 1,
			Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, UpdatedAt: plannerTestTime,
		})
	}
	return PlanningWorkflow{
		Workflow: domain.Workflow{
			ID: workflowID, Project: "project",
			Environment: domain.ExecutionEnvironment{Scope: EnvironmentScopeTask},
		},
		State: state,
	}
}

func fairnessPlanInput(workflows ...PlanningWorkflow) PlanInput {
	input := plannerInput(nil, []domain.WorkerInventory{plannerWorker("worker")})
	input.Workflows = workflows
	input.Ordering.Attempts = make(map[string]PlanningAttemptOrdering)
	for _, workflow := range workflows {
		for _, attempt := range workflow.State.Attempts {
			input.Ordering.Attempts[attempt.ID] = PlanningAttemptOrdering{ReadySince: plannerTestTime}
		}
	}
	return input
}

func proposalOrder(plan Plan) []string {
	result := make([]string, 0, len(plan.Proposals))
	for _, proposal := range plan.Proposals {
		for _, decision := range plan.Decisions {
			if decision.WorkflowRunID == proposal.WorkflowRunID && decision.TaskID == proposal.TaskID {
				result = append(result, proposal.WorkflowRunID+"/"+decision.TaskName)
				break
			}
		}
	}
	return result
}
