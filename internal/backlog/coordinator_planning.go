package backlog

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// CoordinatorPlanningStateInput is one coordinator-owned snapshot used to
// assemble deterministic planner input without producing external effects.
type CoordinatorPlanningStateInput struct {
	Now                    time.Time
	CoordinatorEpoch       int64
	Workflows              []domain.Workflow
	WorkflowRuns           []domain.WorkflowRun
	Tasks                  []domain.Task
	Attempts               []domain.Attempt
	Assignments            []domain.Assignment
	WorkerSnapshots        []domain.WorkerSnapshot
	QuotaPools             []domain.QuotaPool
	QuotaWindows           []QuotaWindowBudget
	MaxWorkerSnapshotAge   time.Duration
	MaxQuotaObservationAge time.Duration
	DeadlineRiskWindow     time.Duration
	CheckpointMargin       time.Duration
}

// BuildCoordinatorPlanInput reconstructs DAGs, ownership, cold-start route
// estimates, and the hard quota admission constraint from durable state.
func BuildCoordinatorPlanInput(input CoordinatorPlanningStateInput) (PlanInput, error) {
	if input.Now.IsZero() {
		return PlanInput{}, fmt.Errorf("coordinator planning current time is required")
	}
	if input.CoordinatorEpoch <= 0 {
		return PlanInput{}, fmt.Errorf("coordinator planning epoch must be positive")
	}
	if input.MaxWorkerSnapshotAge <= 0 || input.MaxQuotaObservationAge <= 0 ||
		input.DeadlineRiskWindow <= 0 || input.CheckpointMargin < 0 {
		return PlanInput{}, fmt.Errorf("coordinator planning freshness and timing values are invalid")
	}

	workflowByID := make(map[string]domain.Workflow, len(input.Workflows))
	for _, workflow := range input.Workflows {
		if workflow.ID == "" {
			return PlanInput{}, fmt.Errorf("coordinator planning workflow identity is required")
		}
		if _, duplicate := workflowByID[workflow.ID]; duplicate {
			return PlanInput{}, fmt.Errorf("coordinator planning repeats workflow %q", workflow.ID)
		}
		workflowByID[workflow.ID] = workflow
	}
	taskByID := make(map[string]domain.Task, len(input.Tasks))
	for _, task := range input.Tasks {
		if task.ID == "" || task.WorkflowID == "" {
			return PlanInput{}, fmt.Errorf("coordinator planning task identity is required")
		}
		if _, duplicate := taskByID[task.ID]; duplicate {
			return PlanInput{}, fmt.Errorf("coordinator planning repeats task %q", task.ID)
		}
		if _, exists := workflowByID[task.WorkflowID]; !exists {
			return PlanInput{}, fmt.Errorf("task %q names unknown workflow %q", task.ID, task.WorkflowID)
		}
		taskByID[task.ID] = task
	}
	attemptsByRun := make(map[string][]domain.Attempt)
	attemptByID := make(map[string]domain.Attempt, len(input.Attempts))
	runByID := make(map[string]domain.WorkflowRun, len(input.WorkflowRuns))
	for _, run := range input.WorkflowRuns {
		if run.ID == "" || run.WorkflowID == "" {
			return PlanInput{}, fmt.Errorf("coordinator planning workflow-run identity is required")
		}
		if _, duplicate := runByID[run.ID]; duplicate {
			return PlanInput{}, fmt.Errorf("coordinator planning repeats workflow run %q", run.ID)
		}
		if _, exists := workflowByID[run.WorkflowID]; !exists {
			return PlanInput{}, fmt.Errorf("workflow run %q names unknown workflow %q", run.ID, run.WorkflowID)
		}
		runByID[run.ID] = run
	}
	for _, attempt := range input.Attempts {
		if attempt.ID == "" || attempt.WorkflowRunID == "" || attempt.TaskID == "" {
			return PlanInput{}, fmt.Errorf("coordinator planning attempt identity is required")
		}
		if _, duplicate := attemptByID[attempt.ID]; duplicate {
			return PlanInput{}, fmt.Errorf("coordinator planning repeats attempt %q", attempt.ID)
		}
		run, exists := runByID[attempt.WorkflowRunID]
		if !exists {
			return PlanInput{}, fmt.Errorf("attempt %q names unknown workflow run %q", attempt.ID, attempt.WorkflowRunID)
		}
		task, exists := domain.TaskForAttempt(attempt, input.WorkflowRuns, input.Tasks)
		if !exists || task.WorkflowID != run.WorkflowID {
			return PlanInput{}, fmt.Errorf("attempt %q names a task outside workflow run %q", attempt.ID, run.ID)
		}
		attemptByID[attempt.ID] = attempt
		attemptsByRun[attempt.WorkflowRunID] = append(attemptsByRun[attempt.WorkflowRunID], attempt)
	}

	planningWorkflows := make([]PlanningWorkflow, 0, len(input.WorkflowRuns))
	ordering := make(map[string]PlanningAttemptOrdering, len(input.Attempts))
	for _, run := range input.WorkflowRuns {
		workflow := workflowByID[run.WorkflowID]
		state := DAGState{
			Run:      run,
			Tasks:    domain.TasksForRun(run, input.Tasks),
			Attempts: append([]domain.Attempt(nil), attemptsByRun[run.ID]...),
		}
		state.External = ResolveExternalNodes(state.Tasks, input.WorkflowRuns, input.Tasks, input.Attempts, input.Assignments)
		execution, err := NewDAGExecution(state)
		if err != nil {
			// A corrupt run is excluded from planning; every other run keeps
			// being scheduled.
			slog.Warn("coordinator planning skipped a workflow run", "run", run.ID, "error", err)
			continue
		}
		state = execution.Snapshot()
		planningWorkflows = append(planningWorkflows, PlanningWorkflow{Workflow: workflow, State: state})
		for _, attempt := range state.Attempts {
			readySince := attempt.UpdatedAt
			if readySince.IsZero() {
				readySince = run.CreatedAt
			}
			if readySince.IsZero() {
				readySince = input.Now
			}
			ordering[attempt.ID] = PlanningAttemptOrdering{ReadySince: readySince}
		}
	}
	sort.Slice(planningWorkflows, func(i, j int) bool {
		return planningWorkflows[i].State.Run.ID < planningWorkflows[j].State.Run.ID
	})

	resourceOwners := make(map[string]string)
	checkoutOwners := make(map[string]string)
	assignmentsByID := make(map[string]domain.Assignment, len(input.Assignments))
	for _, assignment := range input.Assignments {
		if assignment.ID == "" || assignment.AttemptID == "" {
			return PlanInput{}, fmt.Errorf("coordinator planning assignment identity is required")
		}
		if _, duplicate := assignmentsByID[assignment.ID]; duplicate {
			return PlanInput{}, fmt.Errorf("coordinator planning repeats assignment %q", assignment.ID)
		}
		assignmentsByID[assignment.ID] = assignment
	}
	for _, attempt := range input.Attempts {
		if attempt.Progress.Terminal() || attempt.AssignmentID == "" {
			continue
		}
		assignment, exists := assignmentsByID[attempt.AssignmentID]
		if !exists || assignment.AttemptID != attempt.ID {
			// The attempt keeps its assignment reference, so the planner
			// reports it as already assigned instead of planning it twice.
			slog.Warn("coordinator planning found an attempt without its assignment", "attempt", attempt.ID, "assignment", attempt.AssignmentID)
			continue
		}
		if assignment.State == domain.AssignmentReleased || assignment.State == domain.AssignmentCompleted {
			// A worker assignment is settled before its result artifact is
			// verified and imported. It no longer owns planning resources.
			if attempt.Progress != domain.ProgressVerifying {
				slog.Warn("coordinator planning found a nonterminal attempt on a settled assignment", "attempt", attempt.ID, "assignment", assignment.ID)
			}
			continue
		}
		task, _ := domain.TaskForAttempt(attempt, input.WorkflowRuns, input.Tasks)
		for _, resource := range task.ResourceLocks {
			resource = strings.TrimSpace(resource)
			if resource == "" {
				continue
			}
			if owner := resourceOwners[resource]; owner != "" && owner != attempt.ID {
				slog.Warn("resource lock is held by more than one attempt; first owner kept", "resource", resource, "owner", owner, "attempt", attempt.ID)
				continue
			}
			resourceOwners[resource] = attempt.ID
		}
		run := runByID[attempt.WorkflowRunID]
		if workflowByID[run.WorkflowID].Environment.Scope == EnvironmentScopeWorkflow {
			if owner := checkoutOwners[run.ID]; owner != "" && owner != attempt.ID {
				slog.Warn("workflow checkout is held by more than one attempt; first owner kept", "run", run.ID, "owner", owner, "attempt", attempt.ID)
				continue
			}
			checkoutOwners[run.ID] = attempt.ID
		}
	}

	workers := plannerWorkers(input.WorkerSnapshots, input.CoordinatorEpoch, input.Now)
	routeEstimates := make([]RouteEstimate, 0)
	for _, attempt := range input.Attempts {
		task, _ := domain.TaskForAttempt(attempt, input.WorkflowRuns, input.Tasks)
		cost := SeedCost(task.Difficulty)
		if task.EstimatedCost != nil {
			cost = *task.EstimatedCost
		}
		turns := task.MaxTurns
		if turns < 1 {
			turns = 1
		}
		runtime := time.Duration(SeedMinutes(task.Difficulty) * float64(time.Minute) * float64(turns))
		estimate := TaskAdmissionEstimate{
			RemainingCost: cost, ExpectedRuntime: runtime, CheckpointMargin: input.CheckpointMargin,
		}
		for _, worker := range workers {
			for _, route := range task.Routes {
				if route.WorkerID != "" && route.WorkerID != worker.ID {
					continue
				}
				routeEstimates = append(routeEstimates, RouteEstimate{
					AttemptID: attempt.ID, WorkerID: worker.ID,
					ProviderInstanceID: route.ProviderInstanceID, Model: route.Model,
					Options: cloneStringMap(route.Options), Estimate: estimate,
				})
			}
		}
	}
	sort.Slice(routeEstimates, func(i, j int) bool {
		return routeEstimateInputKey(routeEstimates[i]) < routeEstimateInputKey(routeEstimates[j])
	})
	for index := 1; index < len(routeEstimates); index++ {
		if routeEstimateInputKey(routeEstimates[index-1]) == routeEstimateInputKey(routeEstimates[index]) {
			return PlanInput{}, fmt.Errorf("coordinator planning repeats route estimate for attempt %q", routeEstimates[index].AttemptID)
		}
	}
	quotaPolicy, err := NewQuotaAdmissionPolicy(QuotaAdmissionInput{
		Windows: input.QuotaWindows, MaxObservationAge: input.MaxQuotaObservationAge,
	})
	if err != nil {
		return PlanInput{}, fmt.Errorf("coordinator planning quota policy: %w", err)
	}

	return PlanInput{
		Now: input.Now, MaxWorkerSnapshotAge: input.MaxWorkerSnapshotAge,
		Workflows: planningWorkflows, Workers: workers,
		QuotaPools:     append([]domain.QuotaPool(nil), input.QuotaPools...),
		RouteEstimates: routeEstimates, ResourceOwners: resourceOwners,
		WorkflowCheckoutOwners: checkoutOwners,
		Constraints:            []PlanningConstraint{quotaPolicy},
		Ordering: PlanningOrderingInput{
			DeadlineRiskWindow: input.DeadlineRiskWindow, Attempts: ordering,
		},
	}, nil
}
