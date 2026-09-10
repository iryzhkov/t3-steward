package backlog

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// QuotaPlanningStateInput is one complete durable coordinator snapshot used to
// reconstruct quota reservations and provider concurrency after a restart.
type QuotaPlanningStateInput struct {
	Tasks           []domain.Task
	Attempts        []domain.Attempt
	Assignments     []domain.Assignment
	ThrottleRecords []domain.ThrottleAttemptRecord
	RouteEstimates  []RouteEstimate
	QuotaPools      []domain.QuotaPool
	QuotaWindows    []QuotaWindowBudget
}

// QuotaPlanningState contains detached, deterministic planner inputs. Pool
// occupancy is reconstructed from attempts that hold slots; paused required
// remainder is copied into every window belonging to the affected pool.
type QuotaPlanningState struct {
	QuotaPools         []domain.QuotaPool
	QuotaWindows       []QuotaWindowBudget
	ResumeReservations []QuotaResumeReservation
}

// DeriveQuotaPlanningState reconstructs planner quota state from canonical
// attempts, assignments, and the latest durable throttle projection. It fails
// closed on incomplete or contradictory identity and estimate records.
func DeriveQuotaPlanningState(input QuotaPlanningStateInput) (QuotaPlanningState, error) {
	pools := append([]domain.QuotaPool(nil), input.QuotaPools...)
	sort.Slice(pools, func(i, j int) bool { return pools[i].ID < pools[j].ID })
	poolByID := make(map[string]int, len(pools))
	for index := range pools {
		pool := &pools[index]
		if strings.TrimSpace(pool.ID) != pool.ID || pool.ID == "" {
			return QuotaPlanningState{}, fmt.Errorf("quota planning pool ID must be nonempty and trimmed")
		}
		if _, duplicate := poolByID[pool.ID]; duplicate {
			return QuotaPlanningState{}, fmt.Errorf("quota planning repeats pool %q", pool.ID)
		}
		if pool.MaxConcurrent <= 0 {
			return QuotaPlanningState{}, fmt.Errorf("quota planning pool %q maximum concurrency must be positive", pool.ID)
		}
		pool.ProviderInstanceIDs = append([]string(nil), pool.ProviderInstanceIDs...)
		pool.Buckets = append([]domain.BucketKey(nil), pool.Buckets...)
		pool.ActiveAssignments = 0
		poolByID[pool.ID] = index
	}

	tasks := make(map[string]domain.Task, len(input.Tasks))
	for _, task := range input.Tasks {
		if task.ID == "" {
			return QuotaPlanningState{}, fmt.Errorf("quota planning task identity is required")
		}
		if _, duplicate := tasks[task.ID]; duplicate {
			return QuotaPlanningState{}, fmt.Errorf("quota planning repeats task %q", task.ID)
		}
		tasks[task.ID] = task
	}

	assignments := make(map[string]domain.Assignment, len(input.Assignments))
	assignmentAttempts := make(map[string]string, len(input.Assignments))
	for _, assignment := range input.Assignments {
		if assignment.ID == "" || assignment.AttemptID == "" {
			return QuotaPlanningState{}, fmt.Errorf("quota planning assignment identity is required")
		}
		if _, duplicate := assignments[assignment.ID]; duplicate {
			return QuotaPlanningState{}, fmt.Errorf("quota planning repeats assignment %q", assignment.ID)
		}
		if previous, duplicate := assignmentAttempts[assignment.AttemptID]; duplicate {
			return QuotaPlanningState{}, fmt.Errorf(
				"quota planning attempt %q has assignments %q and %q",
				assignment.AttemptID, previous, assignment.ID,
			)
		}
		assignments[assignment.ID] = assignment
		assignmentAttempts[assignment.AttemptID] = assignment.ID
	}

	estimates := make(map[string]TaskAdmissionEstimate, len(input.RouteEstimates))
	for _, estimate := range input.RouteEstimates {
		if err := validateRouteEstimate(estimate); err != nil {
			return QuotaPlanningState{}, err
		}
		key := routeEstimateKey(estimate.AttemptID, domain.ProviderRoute{
			WorkerID: estimate.WorkerID, ProviderInstanceID: estimate.ProviderInstanceID,
			Model: estimate.Model, Options: estimate.Options,
		})
		if _, duplicate := estimates[key]; duplicate {
			return QuotaPlanningState{}, fmt.Errorf("quota planning repeats route estimate for attempt %q", estimate.AttemptID)
		}
		estimates[key] = estimate.Estimate
	}

	indexedThrottle, err := indexThrottleAttemptRecords(input.ThrottleRecords)
	if err != nil {
		return QuotaPlanningState{}, err
	}
	latestThrottle := make(map[string]domain.ThrottleAttemptRecord)
	for _, record := range indexedThrottle {
		current, exists := latestThrottle[record.AttemptID]
		if !exists || throttleRecordIsNewer(record, current) {
			latestThrottle[record.AttemptID] = record
		}
	}

	attempts := append([]domain.Attempt(nil), input.Attempts...)
	sort.Slice(attempts, func(i, j int) bool { return attempts[i].ID < attempts[j].ID })
	seenAttempts := make(map[string]struct{}, len(attempts))
	var reservations []QuotaResumeReservation
	remainderByPool := make(map[string]float64)
	for _, attempt := range attempts {
		if attempt.ID == "" {
			return QuotaPlanningState{}, fmt.Errorf("quota planning attempt identity is required")
		}
		if _, duplicate := seenAttempts[attempt.ID]; duplicate {
			return QuotaPlanningState{}, fmt.Errorf("quota planning repeats attempt %q", attempt.ID)
		}
		seenAttempts[attempt.ID] = struct{}{}
		if attempt.Progress.Terminal() {
			if attempt.Control.HoldsProviderSlot() || pausedControl(attempt.Control) {
				return QuotaPlanningState{}, fmt.Errorf("terminal attempt %q has active control %q", attempt.ID, attempt.Control)
			}
			continue
		}
		if !attempt.Control.HoldsProviderSlot() && !pausedControl(attempt.Control) {
			continue
		}
		task, exists := tasks[attempt.TaskID]
		if !exists {
			return QuotaPlanningState{}, fmt.Errorf("attempt %q names unknown task %q", attempt.ID, attempt.TaskID)
		}
		class, valid := planningTaskClass(task.Class)
		if !valid {
			return QuotaPlanningState{}, fmt.Errorf("attempt %q task has invalid class %q", attempt.ID, task.Class)
		}
		assignment, exists := assignments[attempt.AssignmentID]
		if !exists || assignment.AttemptID != attempt.ID {
			return QuotaPlanningState{}, fmt.Errorf("attempt %q has no matching assignment", attempt.ID)
		}
		if assignment.State == domain.AssignmentReleased || assignment.State == domain.AssignmentCompleted {
			return QuotaPlanningState{}, fmt.Errorf("attempt %q uses settled assignment %q", attempt.ID, assignment.ID)
		}
		poolIndex, exists := poolByID[assignment.Route.QuotaPoolID]
		if !exists {
			return QuotaPlanningState{}, fmt.Errorf("attempt %q assignment names unknown pool %q", attempt.ID, assignment.Route.QuotaPoolID)
		}
		if attempt.Control.HoldsProviderSlot() {
			pools[poolIndex].ActiveAssignments++
		}
		if !pausedControl(attempt.Control) && attempt.Control != domain.ControlResuming {
			continue
		}
		record, exists := latestThrottle[attempt.ID]
		if !exists {
			return QuotaPlanningState{}, fmt.Errorf("attempt %q has no durable throttle record", attempt.ID)
		}
		if err := validateThrottlePlanningIdentity(attempt, assignment, record); err != nil {
			return QuotaPlanningState{}, err
		}
		estimate, exists := estimates[routeEstimateKey(attempt.ID, assignment.Route)]
		if !exists {
			return QuotaPlanningState{}, fmt.Errorf("paused attempt %q has no remaining-cost estimate for its fixed route", attempt.ID)
		}
		status := domain.ResumePending
		if attempt.Control == domain.ControlResuming {
			status = domain.ResumeResuming
		}
		reservation := QuotaResumeReservation{
			AttemptID: attempt.ID, TaskID: task.ID, QuotaPoolID: assignment.Route.QuotaPoolID,
			Class: class, Status: status, RemainingCost: estimate.RemainingCost,
			StopEpoch: record.DirectiveID, AttemptRevision: attempt.Revision,
			Deadline: clonePlanningTime(task.Deadline), ExpiresAt: clonePlanningTime(task.ExpiresAt),
		}
		reservations = append(reservations, reservation)
		if class == domain.TaskClassRequired {
			remainderByPool[reservation.QuotaPoolID] += reservation.RemainingCost
		}
	}
	sort.Slice(reservations, func(i, j int) bool { return reservations[i].AttemptID < reservations[j].AttemptID })
	for attemptID := range assignmentAttempts {
		if _, exists := seenAttempts[attemptID]; !exists {
			return QuotaPlanningState{}, fmt.Errorf("quota planning assignment names unknown attempt %q", attemptID)
		}
	}

	windows := append([]QuotaWindowBudget(nil), input.QuotaWindows...)
	sort.Slice(windows, func(i, j int) bool { return quotaWindowKey(windows[i]) < quotaWindowKey(windows[j]) })
	seenWindows := make(map[string]struct{}, len(windows))
	for index := range windows {
		key := quotaWindowKey(windows[index])
		if _, duplicate := seenWindows[key]; duplicate {
			return QuotaPlanningState{}, fmt.Errorf("quota planning repeats window %q", key)
		}
		seenWindows[key] = struct{}{}
		if _, exists := poolByID[windows[index].QuotaPoolID]; !exists {
			return QuotaPlanningState{}, fmt.Errorf("quota window %q names unknown pool %q", key, windows[index].QuotaPoolID)
		}
		windows[index].PausedRequiredWorkRemainder = remainderByPool[windows[index].QuotaPoolID]
	}

	return QuotaPlanningState{QuotaPools: pools, QuotaWindows: windows, ResumeReservations: reservations}, nil
}

func pausedControl(control domain.ControlState) bool {
	return control == domain.ControlPaused || control == domain.ControlPausedUncheckpointed
}

func validateThrottlePlanningIdentity(attempt domain.Attempt, assignment domain.Assignment, record domain.ThrottleAttemptRecord) error {
	if record.Control != attempt.Control {
		return fmt.Errorf("attempt %q control %q contradicts latest throttle control %q", attempt.ID, attempt.Control, record.Control)
	}
	if attempt.AssignmentID != assignment.ID || attempt.ThreadID == "" ||
		attempt.ThreadID != assignment.ThreadID || assignment.WorkerID == "" ||
		assignment.Route.WorkerID != assignment.WorkerID {
		return fmt.Errorf("attempt %q canonical execution identity contradicts its assignment", attempt.ID)
	}
	command := record.Command
	if command.AssignmentID != assignment.ID || command.AssignmentEpoch != assignment.Epoch ||
		command.WorkerID != assignment.WorkerID || command.ThreadID != assignment.ThreadID ||
		!reflect.DeepEqual(command.Route, assignment.Route) ||
		command.QuotaPoolID != assignment.Route.QuotaPoolID {
		return fmt.Errorf("attempt %q throttle execution identity contradicts its assignment", attempt.ID)
	}
	return nil
}
