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
	Ordering               PlanningOrderingInput
}

type PlanningOrderingInput struct {
	DeadlineRiskWindow time.Duration
	Attempts           map[string]PlanningAttemptOrdering
}

type PlanningAttemptOrdering struct {
	ReadySince     time.Time
	PriorDeferrals int
}

type PlanningOrder struct {
	DeadlineRisk         bool      `json:"deadlineRisk"`
	DeadlineSlackSeconds *int64    `json:"deadlineSlackSeconds,omitempty"`
	Importance           int       `json:"importance"`
	ReadySince           time.Time `json:"readySince"`
	PriorDeferrals       int       `json:"priorDeferrals"`
	WorkflowRound        int       `json:"workflowRound"`
	Reason               string    `json:"reason"`
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
	Code                     string                `json:"code"`
	Detail                   string                `json:"detail"`
	WorkerID                 string                `json:"workerId,omitempty"`
	Resource                 string                `json:"resource,omitempty"`
	OwnerID                  string                `json:"ownerId,omitempty"`
	DependsOn                string                `json:"dependsOn,omitempty"`
	ProviderInstanceID       string                `json:"providerInstanceId,omitempty"`
	Model                    string                `json:"model,omitempty"`
	RouteOrdinal             int                   `json:"routeOrdinal,omitempty"`
	QuotaPoolID              string                `json:"quotaPoolId,omitempty"`
	QuotaWindowID            string                `json:"quotaWindowId,omitempty"`
	Admission                domain.AdmissionState `json:"admission,omitempty"`
	RequiredCost             float64               `json:"requiredCost,omitempty"`
	Available                float64               `json:"available,omitempty"`
	MaxObservationAgeSeconds float64               `json:"maxObservationAgeSeconds,omitempty"`
	ObservedAt               *time.Time            `json:"observedAt,omitempty"`
	EarliestAt               *time.Time            `json:"earliestAt,omitempty"`
	DeadlineAt               *time.Time            `json:"deadlineAt,omitempty"`
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
	Order         PlanningOrder         `json:"order"`
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
	entries, err := orderedPlanningTasks(input)
	if err != nil {
		return Plan{}, err
	}
	resourceOwners := cloneStringMap(input.ResourceOwners)
	checkoutOwners := cloneStringMap(input.WorkflowCheckoutOwners)
	result := Plan{Decisions: make([]TaskPlanningDecision, 0, len(entries))}

	for _, entry := range entries {
		if entry.attempt.Progress.Terminal() {
			continue
		}
		decision, proposal, err := planTask(input, router, constraints, entry.workflow, entry.state, entry.task, entry.attempt, entry.order, resourceOwners, checkoutOwners)
		if err != nil {
			return Plan{}, err
		}
		if proposal != nil {
			decision.Proposed = true
			result.Proposals = append(result.Proposals, *proposal)
			for _, resource := range proposal.ResourceLocks {
				resourceOwners[resource] = proposal.AttemptID
			}
			if entry.workflow.Environment.Scope == EnvironmentScopeWorkflow {
				checkoutOwners[entry.state.Run.ID] = proposal.AttemptID
			}
			candidate := PlanningCandidate{
				WorkflowRunID: entry.state.Run.ID,
				Task:          clonePlanningTask(entry.task),
				Attempt:       clonePlanningAttempt(entry.attempt),
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
	return result, nil
}

type planningTaskEntry struct {
	workflow domain.Workflow
	state    DAGState
	task     domain.Task
	attempt  domain.Attempt
	order    PlanningOrder
	ready    bool
}

func orderedPlanningTasks(input PlanInput) ([]planningTaskEntry, error) {
	entries := make([]planningTaskEntry, 0)
	byWorkflow := make(map[string][]int, len(input.Workflows))
	for _, workflow := range input.Workflows {
		execution, err := NewDAGExecution(workflow.State)
		if err != nil {
			return nil, fmt.Errorf("plan workflow run %q: %w", workflow.State.Run.ID, err)
		}
		state := execution.Snapshot()
		for _, task := range state.Tasks {
			attempt := currentPlanningAttempt(state.Attempts, task.ID)
			if attempt.Progress.Terminal() {
				continue
			}
			history, ok := input.Ordering.Attempts[attempt.ID]
			if !ok {
				return nil, fmt.Errorf("plan attempt %q is missing ordering history", attempt.ID)
			}
			if history.ReadySince.IsZero() {
				return nil, fmt.Errorf("plan attempt %q ready time is required", attempt.ID)
			}
			if history.ReadySince.After(input.Now) {
				return nil, fmt.Errorf("plan attempt %q ready time is in the future", attempt.ID)
			}
			if history.PriorDeferrals < 0 {
				return nil, fmt.Errorf("plan attempt %q prior deferrals must not be negative", attempt.ID)
			}
			order := planningOrder(input, task, history)
			entries = append(entries, planningTaskEntry{
				workflow: workflow.Workflow,
				state:    state,
				task:     task,
				attempt:  attempt,
				order:    order,
				ready:    attempt.Progress == domain.ProgressReady,
			})
			byWorkflow[state.Run.ID] = append(byWorkflow[state.Run.ID], len(entries)-1)
		}
	}
	for _, indices := range byWorkflow {
		sort.Slice(indices, func(i, j int) bool {
			left, right := entries[indices[i]], entries[indices[j]]
			if comparison := comparePlanningPriority(left, right); comparison != 0 {
				return comparison < 0
			}
			if left.task.Name != right.task.Name {
				return left.task.Name < right.task.Name
			}
			return left.task.ID < right.task.ID
		})
		for round, index := range indices {
			entries[index].order.WorkflowRound = round
			entries[index].order.Reason = planningOrderReason(entries[index].order)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if comparison := comparePlanningPriority(entries[i], entries[j]); comparison != 0 {
			return comparison < 0
		}
		if entries[i].order.WorkflowRound != entries[j].order.WorkflowRound {
			return entries[i].order.WorkflowRound < entries[j].order.WorkflowRound
		}
		if entries[i].state.Run.ID != entries[j].state.Run.ID {
			return entries[i].state.Run.ID < entries[j].state.Run.ID
		}
		if entries[i].task.Name != entries[j].task.Name {
			return entries[i].task.Name < entries[j].task.Name
		}
		return entries[i].task.ID < entries[j].task.ID
	})
	return entries, nil
}

func planningOrder(input PlanInput, task domain.Task, history PlanningAttemptOrdering) PlanningOrder {
	order := PlanningOrder{
		Importance:     task.Importance,
		ReadySince:     history.ReadySince,
		PriorDeferrals: history.PriorDeferrals,
	}
	if task.Deadline != nil {
		slack := int64(task.Deadline.Sub(input.Now) / time.Second)
		order.DeadlineSlackSeconds = &slack
		order.DeadlineRisk = task.Class == domain.TaskClassRequired &&
			task.Deadline.Sub(input.Now) <= input.Ordering.DeadlineRiskWindow
	}
	return order
}

func comparePlanningPriority(left, right planningTaskEntry) int {
	if left.ready != right.ready {
		if left.ready {
			return -1
		}
		return 1
	}
	if left.attempt.AdminForceStart != right.attempt.AdminForceStart {
		if left.attempt.AdminForceStart {
			return -1
		}
		return 1
	}
	if left.order.DeadlineRisk != right.order.DeadlineRisk {
		if left.order.DeadlineRisk {
			return -1
		}
		return 1
	}
	if left.order.DeadlineRisk && *left.order.DeadlineSlackSeconds != *right.order.DeadlineSlackSeconds {
		if *left.order.DeadlineSlackSeconds < *right.order.DeadlineSlackSeconds {
			return -1
		}
		return 1
	}
	if left.order.Importance != right.order.Importance {
		if left.order.Importance > right.order.Importance {
			return -1
		}
		return 1
	}
	if left.order.PriorDeferrals != right.order.PriorDeferrals {
		if left.order.PriorDeferrals > right.order.PriorDeferrals {
			return -1
		}
		return 1
	}
	if !left.order.ReadySince.Equal(right.order.ReadySince) {
		if left.order.ReadySince.Before(right.order.ReadySince) {
			return -1
		}
		return 1
	}
	return 0
}

func planningOrderReason(order PlanningOrder) string {
	prefix := "fairness"
	if order.DeadlineRisk {
		prefix = fmt.Sprintf("deadline risk with %s slack", (time.Duration(*order.DeadlineSlackSeconds) * time.Second).String())
	}
	return fmt.Sprintf("%s; importance %d; prior deferrals %d; ready since %s; workflow round %d",
		prefix, order.Importance, order.PriorDeferrals, order.ReadySince.UTC().Format(time.RFC3339Nano), order.WorkflowRound)
}

func planTask(input PlanInput, router *providerRouter, constraints []PlanningConstraintSession, workflow domain.Workflow, state DAGState, task domain.Task, attempt domain.Attempt, order PlanningOrder, resourceOwners, checkoutOwners map[string]string) (TaskPlanningDecision, *ProposedTask, error) {
	placement, err := MatchWorkers(WorkerPlacementRequest{
		Task: task, Project: workflow.Project, Now: input.Now, MaxSnapshotAge: input.MaxWorkerSnapshotAge,
	}, input.Workers)
	if err != nil {
		return TaskPlanningDecision{}, nil, fmt.Errorf("plan task %q: %w", task.Name, err)
	}
	decision := TaskPlanningDecision{
		WorkflowRunID: state.Run.ID, TaskID: task.ID, TaskName: task.Name,
		AttemptID: attempt.ID, Progress: attempt.Progress, Order: order, Placement: placement,
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
	if input.Ordering.DeadlineRiskWindow <= 0 {
		return errors.New("plan deadline risk window must be positive")
	}
	if input.Ordering.Attempts == nil {
		return errors.New("plan attempt ordering history is required")
	}
	for attemptID := range input.Ordering.Attempts {
		if strings.TrimSpace(attemptID) != attemptID || attemptID == "" {
			return errors.New("plan attempt ordering history contains an invalid attempt ID")
		}
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
			planningTimeKey(left.ObservedAt), planningTimeKey(left.EarliestAt), planningTimeKey(left.DeadlineAt), left.Detail,
		}
		rightFields := []string{
			right.Code, right.WorkerID, right.Resource, right.OwnerID, right.DependsOn,
			right.ProviderInstanceID, right.Model, right.QuotaPoolID, right.QuotaWindowID, string(right.Admission),
			planningTimeKey(right.ObservedAt), planningTimeKey(right.EarliestAt), planningTimeKey(right.DeadlineAt), right.Detail,
		}
		for index := range leftFields {
			if leftFields[index] != rightFields[index] {
				return leftFields[index] < rightFields[index]
			}
		}
		if left.RouteOrdinal != right.RouteOrdinal {
			return left.RouteOrdinal < right.RouteOrdinal
		}
		if left.MaxObservationAgeSeconds != right.MaxObservationAgeSeconds {
			return left.MaxObservationAgeSeconds < right.MaxObservationAgeSeconds
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
