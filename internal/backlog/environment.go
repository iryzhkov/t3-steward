package backlog

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

type EnvironmentReservationState string

const (
	EnvironmentReservationActive EnvironmentReservationState = "active"
	EnvironmentReservationPaused EnvironmentReservationState = "paused"
)

type EnvironmentReleasePolicy string

const (
	EnvironmentReleaseTerminal       EnvironmentReleasePolicy = "terminal"
	EnvironmentReleaseCanceled       EnvironmentReleasePolicy = "canceled"
	EnvironmentPauseRetainResources  EnvironmentReleasePolicy = "pause-retain-resources"
	EnvironmentPauseReleaseResources EnvironmentReleasePolicy = "pause-release-resources"
)

type EnvironmentConflictKind string

const (
	EnvironmentConflictWorker   EnvironmentConflictKind = "worker-mismatch"
	EnvironmentConflictCheckout EnvironmentConflictKind = "checkout-busy"
	EnvironmentConflictResource EnvironmentConflictKind = "resource-busy"
)

type EnvironmentReservationRequest struct {
	WorkflowRunID string
	TaskID        string
	AttemptID     string
	WorkerID      string
	Scope         string
	ResourceLocks []string
}

type EnvironmentReservation struct {
	WorkflowRunID  string
	TaskID         string
	AttemptID      string
	WorkerID       string
	Scope          string
	ResourceLocks  []string
	State          EnvironmentReservationState
	HoldsResources bool
}

type EnvironmentConflict struct {
	Kind              EnvironmentConflictKind
	WorkflowRunID     string
	Resource          string
	OwnerAttemptID    string
	OwnerWorkerID     string
	RequestedWorkerID string
}

func (e *EnvironmentConflict) Error() string {
	switch e.Kind {
	case EnvironmentConflictWorker:
		return fmt.Sprintf("workflow run %q is pinned to worker %q, not %q", e.WorkflowRunID, e.OwnerWorkerID, e.RequestedWorkerID)
	case EnvironmentConflictCheckout:
		return fmt.Sprintf("workflow checkout %q is held by attempt %q", e.WorkflowRunID, e.OwnerAttemptID)
	case EnvironmentConflictResource:
		return fmt.Sprintf("resource lock %q is held by attempt %q", e.Resource, e.OwnerAttemptID)
	default:
		return "environment reservation conflict"
	}
}

type workflowEnvironmentOwner struct {
	workerID      string
	activeAttempt string
}

type EnvironmentCoordinator struct {
	mu           sync.Mutex
	workflows    map[string]workflowEnvironmentOwner
	reservations map[string]EnvironmentReservation
	locks        map[string]string
}

func NewEnvironmentCoordinator() *EnvironmentCoordinator {
	return &EnvironmentCoordinator{
		workflows:    make(map[string]workflowEnvironmentOwner),
		reservations: make(map[string]EnvironmentReservation),
		locks:        make(map[string]string),
	}
}

func (c *EnvironmentCoordinator) Reserve(request EnvironmentReservationRequest) (EnvironmentReservation, error) {
	if c == nil {
		return EnvironmentReservation{}, errors.New("environment coordinator is required")
	}
	locks, err := validateEnvironmentReservation(request)
	if err != nil {
		return EnvironmentReservation{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.initializeLocked()

	if existing, ok := c.reservations[request.AttemptID]; ok {
		if !sameEnvironmentReservation(existing, request, locks) {
			return EnvironmentReservation{}, fmt.Errorf("attempt %q already has a different environment reservation", request.AttemptID)
		}
		if existing.HoldsResources {
			return cloneEnvironmentReservation(existing), nil
		}
		return c.acquireLocked(existing)
	}

	reservation := EnvironmentReservation{
		WorkflowRunID: request.WorkflowRunID,
		TaskID:        request.TaskID,
		AttemptID:     request.AttemptID,
		WorkerID:      request.WorkerID,
		Scope:         request.Scope,
		ResourceLocks: locks,
		State:         EnvironmentReservationActive,
	}
	return c.acquireLocked(reservation)
}

func (c *EnvironmentCoordinator) Release(attemptID string, policy EnvironmentReleasePolicy) error {
	if c == nil {
		return errors.New("environment coordinator is required")
	}
	if attemptID == "" {
		return errors.New("attempt ID is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	reservation, ok := c.reservations[attemptID]
	if !ok {
		return fmt.Errorf("attempt %q has no environment reservation", attemptID)
	}

	switch policy {
	case EnvironmentPauseRetainResources:
		reservation.State = EnvironmentReservationPaused
		c.reservations[attemptID] = reservation
		return nil
	case EnvironmentPauseReleaseResources:
		c.releaseResourcesLocked(reservation)
		reservation.State = EnvironmentReservationPaused
		reservation.HoldsResources = false
		c.reservations[attemptID] = reservation
		return nil
	case EnvironmentReleaseTerminal, EnvironmentReleaseCanceled:
		c.releaseResourcesLocked(reservation)
		delete(c.reservations, attemptID)
		return nil
	default:
		return fmt.Errorf("invalid environment release policy %q", policy)
	}
}

func (c *EnvironmentCoordinator) CleanupWorkflow(workflowRunID string) error {
	if c == nil {
		return errors.New("environment coordinator is required")
	}
	if workflowRunID == "" {
		return errors.New("workflow run ID is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, reservation := range c.reservations {
		if reservation.WorkflowRunID == workflowRunID {
			return fmt.Errorf("workflow run %q still owns attempt %q", workflowRunID, reservation.AttemptID)
		}
	}
	delete(c.workflows, workflowRunID)
	return nil
}

func (c *EnvironmentCoordinator) Reservation(attemptID string) (EnvironmentReservation, bool) {
	if c == nil {
		return EnvironmentReservation{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	reservation, ok := c.reservations[attemptID]
	return cloneEnvironmentReservation(reservation), ok
}

func (c *EnvironmentCoordinator) initializeLocked() {
	if c.workflows == nil {
		c.workflows = make(map[string]workflowEnvironmentOwner)
	}
	if c.reservations == nil {
		c.reservations = make(map[string]EnvironmentReservation)
	}
	if c.locks == nil {
		c.locks = make(map[string]string)
	}
}

func (c *EnvironmentCoordinator) acquireLocked(reservation EnvironmentReservation) (EnvironmentReservation, error) {
	if reservation.Scope == EnvironmentScopeWorkflow {
		owner, exists := c.workflows[reservation.WorkflowRunID]
		if exists && owner.workerID != reservation.WorkerID {
			return EnvironmentReservation{}, &EnvironmentConflict{
				Kind: EnvironmentConflictWorker, WorkflowRunID: reservation.WorkflowRunID,
				OwnerWorkerID: owner.workerID, RequestedWorkerID: reservation.WorkerID,
			}
		}
		if exists && owner.activeAttempt != "" && owner.activeAttempt != reservation.AttemptID {
			return EnvironmentReservation{}, &EnvironmentConflict{
				Kind: EnvironmentConflictCheckout, WorkflowRunID: reservation.WorkflowRunID,
				OwnerAttemptID: owner.activeAttempt, OwnerWorkerID: owner.workerID,
			}
		}
	}

	for _, resource := range reservation.ResourceLocks {
		if owner, exists := c.locks[resource]; exists && owner != reservation.AttemptID {
			return EnvironmentReservation{}, &EnvironmentConflict{
				Kind: EnvironmentConflictResource, WorkflowRunID: reservation.WorkflowRunID,
				Resource: resource, OwnerAttemptID: owner,
			}
		}
	}

	if reservation.Scope == EnvironmentScopeWorkflow {
		owner := c.workflows[reservation.WorkflowRunID]
		if owner.workerID == "" {
			owner.workerID = reservation.WorkerID
		}
		owner.activeAttempt = reservation.AttemptID
		c.workflows[reservation.WorkflowRunID] = owner
	}
	for _, resource := range reservation.ResourceLocks {
		c.locks[resource] = reservation.AttemptID
	}
	reservation.State = EnvironmentReservationActive
	reservation.HoldsResources = true
	c.reservations[reservation.AttemptID] = reservation
	return cloneEnvironmentReservation(reservation), nil
}

func (c *EnvironmentCoordinator) releaseResourcesLocked(reservation EnvironmentReservation) {
	if !reservation.HoldsResources {
		return
	}
	for _, resource := range reservation.ResourceLocks {
		if c.locks[resource] == reservation.AttemptID {
			delete(c.locks, resource)
		}
	}
	if reservation.Scope == EnvironmentScopeWorkflow {
		owner := c.workflows[reservation.WorkflowRunID]
		if owner.activeAttempt == reservation.AttemptID {
			owner.activeAttempt = ""
			c.workflows[reservation.WorkflowRunID] = owner
		}
	}
}

func validateEnvironmentReservation(request EnvironmentReservationRequest) ([]string, error) {
	for _, field := range []struct {
		label string
		value string
	}{
		{label: "workflow run ID", value: request.WorkflowRunID},
		{label: "task ID", value: request.TaskID},
		{label: "attempt ID", value: request.AttemptID},
		{label: "worker ID", value: request.WorkerID},
	} {
		if strings.TrimSpace(field.value) != field.value || field.value == "" {
			return nil, fmt.Errorf("%s must be nonempty and trimmed", field.label)
		}
	}
	if request.Scope != EnvironmentScopeTask && request.Scope != EnvironmentScopeWorkflow {
		return nil, fmt.Errorf("invalid environment scope %q", request.Scope)
	}
	locks := append([]string(nil), request.ResourceLocks...)
	for _, resource := range locks {
		if strings.TrimSpace(resource) != resource || resource == "" {
			return nil, errors.New("resource locks must be nonempty and trimmed")
		}
	}
	sort.Strings(locks)
	for index := 1; index < len(locks); index++ {
		if locks[index] == locks[index-1] {
			return nil, fmt.Errorf("duplicate resource lock %q", locks[index])
		}
	}
	return locks, nil
}

func sameEnvironmentReservation(existing EnvironmentReservation, request EnvironmentReservationRequest, locks []string) bool {
	if existing.WorkflowRunID != request.WorkflowRunID || existing.TaskID != request.TaskID ||
		existing.AttemptID != request.AttemptID || existing.WorkerID != request.WorkerID ||
		existing.Scope != request.Scope || len(existing.ResourceLocks) != len(locks) {
		return false
	}
	for index := range locks {
		if existing.ResourceLocks[index] != locks[index] {
			return false
		}
	}
	return true
}

func cloneEnvironmentReservation(reservation EnvironmentReservation) EnvironmentReservation {
	reservation.ResourceLocks = append([]string(nil), reservation.ResourceLocks...)
	return reservation
}
