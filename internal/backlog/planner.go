package backlog

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const (
	PlanningBlockerDependency       = "dependency"
	PlanningBlockerAttemptState     = "attempt-state"
	PlanningBlockerNoEligibleWorker = "no-eligible-worker"
	PlanningBlockerCandidatePolicy  = "candidate-policy"
	PlanningBlockerResource         = "resource-busy"
	PlanningBlockerCheckout         = "workflow-checkout-busy"
)

type PlanningWorkflow struct {
	Workflow domain.Workflow
	State    DAGState
}

type PlanInput struct {
	Now                    time.Time
	MaxWorkerSnapshotAge   time.Duration
	Workflows              []PlanningWorkflow
	Workers                []domain.WorkerInventory
	QuotaPools             []domain.QuotaPool
	RouteEstimates         []RouteEstimate
	ResourceOwners         map[string]string
	WorkflowCheckoutOwners map[string]string
	Constraints            []PlanningConstraint
}

type PlanningCandidate struct {
	WorkflowRunID string
	Task          domain.Task
	Attempt       domain.Attempt
	WorkerID      string
	RouteOrdinal    int
	Route         *domain.ProviderRoute
	Estimate      *TaskAdmissionEstimate
}

// PlanningConstraint is immutable planning policy input. StartPlan returns
// private state for one dry-run evaluation.
type PlanningConstraint interface {
	StartPlan(time.Time) PlanningConstraintSession
}

type PlanningConstraintSession interface {
	Evaluate(PlanningCandidate) []PlanningBlocker
	Reserve(PlanningCandidate)
}

type PlanningBlocker struct {
	Code          string                `json:"code"`
	Detail        string                `json:"detail"`
	WorkerID      string                `json:"workerId,omitempty"`
	Resource      string                `json:"resource,omitempty"`
	OwnerID       string                `json:"ownerId,omitempty"`
	DependsOn     string                `json:"dependsOn,omitempty"`
	ProviderInstanceID string                `json:"providerInstanceId,omitempty"`
	Model              string                `json:"model,omitempty"`
	RouteOrdinal         int                   `json:"routeOrdinal,omitempty"`
	QuotaPoolID        string                `json:"quotaPoolId,omitempty"`
	QuotaWindowID      string                `json:"quotaWindowId,omitempty"`
	Admission     domain.AdmissionState `json:"admission,omitempty"`
	RequiredCost  float64               `json:"requiredCost,omitempty"`
	Available     float64               `json:"available,omitempty"`
	EarliestAt    *time.Time            `json:"earliestAt,omitempty"`
	DeadlineAt    *time.Time            `json:"deadlineAt,omitempty"`
}

type CandidateEvaluation struct {
	WorkerID   string                 `json:"workerId"`
	RouteOrdinal int                    `json:"routeOrdinal,omitempty"`
	Route      *domain.ProviderRoute  `json:"route,omitempty"`
	Estimate   *TaskAdmissionEstimate `json:"estimate,omitempty"`
	Blockers   []PlanningBlocker      `json:"blockers,omitempty"`
}

type TaskPlanningDecision struct {
	WorkflowRunID string                `json:"workflowRunId"`
	TaskID        string                `json:"taskId"`
	TaskName      string                `json:"taskName"`
	AttemptID     string                `json:"attemptId"`
	Progress      domain.ProgressState  `json:"progress"`
	Placement     WorkerPlacement       `json:"placement"`
	Candidates    []CandidateEvaluation `json:"candidates,omitempty"`
	Blockers      []PlanningBlocker     `json:"blockers,omitempty"`
	Proposed      bool                  `json:"proposed"`
}

type ProposedTask struct {
	WorkflowRunID string                 `json:"workflowRunId"`
	TaskID        string                 `json:"taskId"`
	AttemptID     string                 `json:"attemptId"`
	WorkerID      string                 `json:"workerId"`
	Route         *domain.ProviderRoute  `json:"route,omitempty"`
	Estimate      *TaskAdmissionEstimate `json:"estimate,omitempty"`
	ResourceLocks []string               `json:"resourceLocks,omitempty"`
}

type Plan struct {
	Proposals []ProposedTask         `json:"proposals,omitempty"`
	Decisions []TaskPlanningDecision `json:"decisions"`
}

// BuildPlan is a deterministic dry run. It does not mutate DAG projections,
// reservations, worker inventory, or any external state.
func BuildPlan(input PlanInput) (Plan, error) {
	if err := validatePlanInput(input); err != nil {
		return Plan{}, err
	}
	router, err := newProviderRouter(input)
	if err != nil {
		return Plan{}, err
	}
	constraints := make([]PlanningConstraintSession, 0, len(input.Constraints))
	for _, constraint := range input.Constraints {
		session := constraint.StartPlan(input.Now)
		if session == nil {
			return Plan{}, errors.New("planning constraint returned a nil plan session")
		}
		constraints = append(constraints, session)
	}
	workflows := append([]PlanningWorkflow(nil), input.Workflows...)
	sort.Slice(workflows, func(i, j int) bool {
		return workflows[i].State.Run.ID < workflows[j].State.Run.ID
	})
	resourceOwners := cloneStringMap(input.ResourceOwners)
	checkoutOwners := cloneStringMap(input.WorkflowCheckoutOwners)
	result := Plan{Decisions: make([]TaskPlanningDecision, 0)}

	for _, workflow := range workflows {
		execution, err := NewDAGExecution(workflow.State)
		if err != nil {
			return Plan{}, fmt.Errorf("plan workflow run %q: %w", workflow.State.Run.ID, err)
		}
		state := execution.Snapshot()
		tasks := append([]domain.Task(nil), state.Tasks...)
		sort.Slice(tasks, func(i, j int) bool {
			if tasks[i].Name == tasks[j].Name {
				return tasks[i].ID < tasks[j].ID
			}
			return tasks[i].Name < tasks[j].Name
		})
		for _, task := range tasks {
			attempt := currentPlanningAttempt(state.Attempts, task.ID)
			if attempt.Progress.Terminal() {
				continue
			}
			decision, proposal, err := planTask(input, router, constraints, workflow.Workflow, state, task, attempt, resourceOwners, checkoutOwners)
			if err != nil {
				return Plan{}, err
			}
			if proposal != nil {
				decision.Proposed = true
				result.Proposals = append(result.Proposals, *proposal)
				for _, resource := range proposal.ResourceLocks {
					resourceOwners[resource] = proposal.AttemptID
				}
				if workflow.Workflow.Environment.Scope == EnvironmentScopeWorkflow {
					checkoutOwners[state.Run.ID] = proposal.AttemptID
				}
				candidate := PlanningCandidate{
					WorkflowRunID: state.Run.ID,
					Task:          clonePlanningTask(task),
					Attempt:       clonePlanningAttempt(attempt),
					WorkerID:      proposal.WorkerID,
					Route:         cloneProviderRoutePointer(proposal.Route),
					Estimate:      cloneTaskAdmissionEstimatePointer(proposal.Estimate),
				}
				router.Reserve(candidate)
				for _, constraint := range constraints {
					constraint.Reserve(clonePlanningCandidate(candidate))
				}
			}
			result.Decisions = append(result.Decisions, decision)
		}
	}
	return result, nil
}

func planTask(input PlanInput, router *providerRouter, constraints []PlanningConstraintSession, workflow domain.Workflow, state DAGState, task domain.Task, attempt domain.Attempt, resourceOwners, checkoutOwners map[string]string) (TaskPlanningDecision, *ProposedTask, error) {
	placement, err := MatchWorkers(WorkerPlacementRequest{
		Task: task, Project: workflow.Project, Now: input.Now, MaxSnapshotAge: input.MaxWorkerSnapshotAge,
	}, input.Workers)
	if err != nil {
		return TaskPlanningDecision{}, nil, fmt.Errorf("plan task %q: %w", task.Name, err)
	}
	decision := TaskPlanningDecision{
		WorkflowRunID: state.Run.ID, TaskID: task.ID, TaskName: task.Name,
		AttemptID: attempt.ID, Progress: attempt.Progress, Placement: placement,
	}
	decision.Blockers = append(decision.Blockers, progressBlockers(state, task, attempt)...)

	locks, err := planningResourceLocks(task.ResourceLocks)
	if err != nil {
		return TaskPlanningDecision{}, nil, fmt.Errorf("plan task %q: %w", task.Name, err)
	}
	for _, resource := range locks {
		if owner := resourceOwners[resource]; owner != "" && owner != attempt.ID {
			decision.Blockers = append(decision.Blockers, PlanningBlocker{
				Code: PlanningBlockerResource, Resource: resource, OwnerID: owner,
				Detail: fmt.Sprintf("resource %q is held by attempt %q", resource, owner),
			})
		}
	}
	if workflow.Environment.Scope == EnvironmentScopeWorkflow {
		if owner := checkoutOwners[state.Run.ID]; owner != "" && owner != attempt.ID {
			decision.Blockers = append(decision.Blockers, PlanningBlocker{
				Code: PlanningBlockerCheckout, OwnerID: owner,
				Detail: fmt.Sprintf("workflow checkout is held by attempt %q", owner),
			})
		}
	}

	var selected *PlanningCandidate
	for _, routed := range router.Candidates(task, attempt, placement.EligibleWorkerIDs) {
		candidate := clonePlanningCandidate(routed.candidate)
		candidate.WorkflowRunID = state.Run.ID
		evaluation := CandidateEvaluation{
			WorkerID: candidate.WorkerID, RouteOrdinal: candidate.RouteOrdinal,
			Route:    cloneProviderRoutePointer(candidate.Route),
			Estimate: cloneTaskAdmissionEstimatePointer(candidate.Estimate),
			Blockers: append([]PlanningBlocker(nil), routed.blockers...),
		}
		if len(evaluation.Blockers) == 0 {
			for _, constraint := range constraints {
				evaluation.Blockers = append(evaluation.Blockers, constraint.Evaluate(clonePlanningCandidate(candidate))...)
			}
		}
		if err := normalizePlanningBlockers(evaluation.Blockers, candidate.WorkerID); err != nil {
			return TaskPlanningDecision{}, nil, fmt.Errorf("plan task %q candidate worker %q route %d: %w", task.Name, candidate.WorkerID, candidate.RouteOrdinal, err)
		}
		sortPlanningBlockers(evaluation.Blockers)
		decision.Candidates = append(decision.Candidates, evaluation)
		if selected == nil && len(evaluation.Blockers) == 0 {
			copied := clonePlanningCandidate(candidate)
			selected = &copied
		}
	}
	if len(placement.EligibleWorkerIDs) == 0 {
		decision.Blockers = append(decision.Blockers, PlanningBlocker{
			Code: PlanningBlockerNoEligibleWorker, Detail: "no worker satisfies placement requirements",
		})
	} else if selected == nil {
		decision.Blockers = append(decision.Blockers, PlanningBlocker{
			Code: PlanningBlockerCandidatePolicy, Detail: "every placement-eligible worker and provider route is blocked by planning policy",
		})
	}
	sortPlanningBlockers(decision.Blockers)
	if len(decision.Blockers) != 0 {
		return decision, nil, nil
	}
	return decision, &ProposedTask{
		WorkflowRunID: state.Run.ID, TaskID: task.ID, AttemptID: attempt.ID,
		WorkerID: selected.WorkerID, Route: cloneProviderRoutePointer(selected.Route),
		Estimate: cloneTaskAdmissionEstimatePointer(selected.Estimate), ResourceLocks: locks,
	}, nil
}

func validatePlanInput(input PlanInput) error {
	if input.Now.IsZero() {
		return errors.New("plan current time is required")
	}
	if input.MaxWorkerSnapshotAge <= 0 {
		return errors.New("plan maximum worker snapshot age must be positive")
	}
	for _, constraint := range input.Constraints {
		if constraint == nil {
			return errors.New("plan input contains a nil planning constraint")
		}
	}
	seenRuns := make(map[string]struct{}, len(input.Workflows))
	for _, workflow := range input.Workflows {
		if workflow.Workflow.ID == "" || workflow.State.Run.ID == "" {
			return errors.New("plan workflow and run IDs are required")
		}
		if workflow.Workflow.ID != workflow.State.Run.WorkflowID {
			return fmt.Errorf("plan workflow %q does not own run %q", workflow.Workflow.ID, workflow.State.Run.ID)
		}
		if _, duplicate := seenRuns[workflow.State.Run.ID]; duplicate {
			return fmt.Errorf("plan repeats workflow run %q", workflow.State.Run.ID)
		}
		seenRuns[workflow.State.Run.ID] = struct{}{}
	}
	for label, owners := range map[string]map[string]string{
		"resource": input.ResourceOwners, "workflow checkout": input.WorkflowCheckoutOwners,
	} {
		for resource, owner := range owners {
			if strings.TrimSpace(resource) != resource || resource == "" ||
				strings.TrimSpace(owner) != owner || owner == "" {
				return fmt.Errorf("plan %s owners must contain nonempty trimmed keys and values", label)
			}
		}
	}
	return nil
}

func progressBlockers(state DAGState, task domain.Task, attempt domain.Attempt) []PlanningBlocker {
	if attempt.Progress == domain.ProgressReady {
		return nil
	}
	if attempt.Progress == domain.ProgressBlocked {
		succeeded := make(map[string]bool)
		taskIDs := make(map[string]string, len(state.Tasks))
		for _, candidate := range state.Tasks {
			taskIDs[candidate.Name] = candidate.ID
		}
		for _, candidate := range state.Attempts {
			if candidate.Progress == domain.ProgressSucceeded {
				succeeded[candidate.TaskID] = true
			}
		}
		blockers := make([]PlanningBlocker, 0, len(task.Needs))
		for _, dependency := range task.Needs {
			if !succeeded[taskIDs[dependency]] {
				blockers = append(blockers, PlanningBlocker{
					Code: PlanningBlockerDependency, DependsOn: dependency,
					Detail: fmt.Sprintf("waiting for dependency %q", dependency),
				})
			}
		}
		if len(blockers) != 0 {
			return blockers
		}
	}
	return []PlanningBlocker{{
		Code:   PlanningBlockerAttemptState,
		Detail: fmt.Sprintf("attempt progress is %q, want %q", attempt.Progress, domain.ProgressReady),
	}}
}

func currentPlanningAttempt(attempts []domain.Attempt, taskID string) domain.Attempt {
	var current domain.Attempt
	for _, attempt := range attempts {
		if attempt.TaskID == taskID && (current.ID == "" || attempt.Number > current.Number ||
			attempt.Number == current.Number && attempt.ID < current.ID) {
			current = attempt
		}
	}
	return current
}

func planningResourceLocks(resources []string) ([]string, error) {
	locks := append([]string(nil), resources...)
	sort.Strings(locks)
	for index, resource := range locks {
		if strings.TrimSpace(resource) != resource || resource == "" {
			return nil, errors.New("resource locks must be nonempty and trimmed")
		}
		if index > 0 && locks[index-1] == resource {
			return nil, fmt.Errorf("duplicate resource lock %q", resource)
		}
	}
	return locks, nil
}

func normalizePlanningBlockers(blockers []PlanningBlocker, workerID string) error {
	for index := range blockers {
		if strings.TrimSpace(blockers[index].Code) != blockers[index].Code || blockers[index].Code == "" ||
			strings.TrimSpace(blockers[index].Detail) != blockers[index].Detail || blockers[index].Detail == "" {
			return errors.New("planning constraint returned a blocker without a nonempty trimmed code and detail")
		}
		if blockers[index].WorkerID == "" {
			blockers[index].WorkerID = workerID
		}
	}
	return nil
}

func sortPlanningBlockers(blockers []PlanningBlocker) {
	sort.Slice(blockers, func(i, j int) bool {
		left, right := blockers[i], blockers[j]
		leftFields := []string{
			left.Code, left.WorkerID, left.Resource, left.OwnerID, left.DependsOn,
			left.ProviderInstanceID, left.Model, left.QuotaPoolID, left.QuotaWindowID, string(left.Admission),
			planningTimeKey(left.EarliestAt), planningTimeKey(left.DeadlineAt), left.Detail,
		}
		rightFields := []string{
			right.Code, right.WorkerID, right.Resource, right.OwnerID, right.DependsOn,
			right.ProviderInstanceID, right.Model, right.QuotaPoolID, right.QuotaWindowID, string(right.Admission),
			planningTimeKey(right.EarliestAt), planningTimeKey(right.DeadlineAt), right.Detail,
		}
		for index := range leftFields {
			if leftFields[index] != rightFields[index] {
				return leftFields[index] < rightFields[index]
			}
		}
		if left.RouteOrdinal != right.RouteOrdinal {
			return left.RouteOrdinal < right.RouteOrdinal
		}
		if left.RequiredCost != right.RequiredCost {
			return left.RequiredCost < right.RequiredCost
		}
		return left.Available < right.Available
	})
}

func planningTimeKey(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}


func clonePlanningTask(task domain.Task) domain.Task {
	task.Needs = append([]string(nil), task.Needs...)
	task.InputArtifactIDs = append([]string(nil), task.InputArtifactIDs...)
	task.Outputs = append([]domain.ArtifactDeclaration(nil), task.Outputs...)
	task.Verification = append([]string(nil), task.Verification...)
	task.Placement.Hosts = append([]string(nil), task.Placement.Hosts...)
	task.Placement.Capabilities = append([]string(nil), task.Placement.Capabilities...)
	task.ResourceLocks = append([]string(nil), task.ResourceLocks...)
	task.Routes = append([]domain.ProviderRoute(nil), task.Routes...)
	for index := range task.Routes {
		task.Routes[index].Options = cloneStringMap(task.Routes[index].Options)
	}
	if task.DependencyInputs != nil {
		task.DependencyInputs = make(map[string][]string, len(task.DependencyInputs))
		for name, inputs := range task.DependencyInputs {
			task.DependencyInputs[name] = append([]string(nil), inputs...)
		}
	}
	if task.EstimatedCost != nil {
		value := *task.EstimatedCost
		task.EstimatedCost = &value
	}
	task.NotBefore = clonePlanningTime(task.NotBefore)
	task.Deadline = clonePlanningTime(task.Deadline)
	task.ExpiresAt = clonePlanningTime(task.ExpiresAt)
	return task
}

func clonePlanningCandidate(candidate PlanningCandidate) PlanningCandidate {
	candidate.Task = clonePlanningTask(candidate.Task)
	candidate.Attempt = clonePlanningAttempt(candidate.Attempt)
	candidate.Route = cloneProviderRoutePointer(candidate.Route)
	candidate.Estimate = cloneTaskAdmissionEstimatePointer(candidate.Estimate)
	return candidate
}

func clonePlanningAttempt(attempt domain.Attempt) domain.Attempt {
	attempt.StartedAt = clonePlanningTime(attempt.StartedAt)
	attempt.CompletedAt = clonePlanningTime(attempt.CompletedAt)
	return attempt
}

func clonePlanningTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}
