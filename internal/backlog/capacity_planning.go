package backlog

import (
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// PlanningBlockerExecutorCapacity reports that a worker's executor pool cannot
// take this task. It is independent of PlanningBlockerPoolConcurrency, which
// reports the provider-session limit: an attempt needs both.
const PlanningBlockerExecutorCapacity = "executor-capacity"

// CapacityOwner is reconstructed from durable assignment and task records. The
// assignment is the durable reservation, so an attempt holding a live
// assignment holds its worker's capacity, and no separate reservation record
// exists to fall out of step with it.
type CapacityOwner struct {
	AssignmentID string
	AttemptID    string
	WorkerID     string
	Demand       domain.ResourceDemand
}

// capacityOwners derives current capacity ownership from durable state. It
// follows the rule directoryOwners already applies: a settled assignment
// releases, and missing or mismatched assignment evidence is uncertain
// ownership rather than release, so its capacity stays held until reconciled.
func capacityOwners(attempts []domain.Attempt, assignments []domain.Assignment, runs []domain.WorkflowRun, tasks []domain.Task) []CapacityOwner {
	byID := make(map[string]domain.Assignment, len(assignments))
	for _, assignment := range assignments {
		byID[assignment.ID] = assignment
	}
	var owners []CapacityOwner
	for _, attempt := range attempts {
		if attempt.AssignmentID == "" {
			continue
		}
		assignment, exists := byID[attempt.AssignmentID]
		if exists && assignment.AttemptID == attempt.ID &&
			(assignment.State == domain.AssignmentCompleted || assignment.State == domain.AssignmentReleased) {
			continue
		}
		if !exists || assignment.AttemptID != attempt.ID {
			continue
		}
		if attempt.Control == domain.ControlWaitingExternal {
			// A parked attempt keeps its assignment, its workspace and its locks,
			// but releases the executor slot and the CPU, memory and scratch it
			// reserved. A wait on a build or a review lasts minutes to hours, and
			// holding a slot that long on a three-worker fleet turns one parked
			// task into a stalled queue.
			continue
		}
		// An overseer activation is an owner like any other, which is the
		// adopted cost of running it as assigned work: an active review occupies
		// one executor slot on its worker until it ends, bounded by the
		// activation deadline. It names no declared task, so it reserves no CPU,
		// memory or scratch beyond the slot itself.
		task, _ := domain.TaskForAttempt(attempt, runs, tasks)
		owners = append(owners, CapacityOwner{
			AssignmentID: assignment.ID, AttemptID: attempt.ID,
			WorkerID: assignment.WorkerID, Demand: task.ResourceDemand,
		})
	}
	sort.Slice(owners, func(i, j int) bool { return owners[i].AssignmentID < owners[j].AssignmentID })
	return owners
}

// executorPools projects the configured executor capacity of every worker that
// declares slots. A worker without configured slots has no pool and is not
// capacity-governed.
func executorPools(workers []domain.WorkerInventory) []domain.ExecutorPool {
	pools := make([]domain.ExecutorPool, 0, len(workers))
	for _, worker := range workers {
		if worker.Allocatable.ExecutorSlots < 1 {
			continue
		}
		pools = append(pools, domain.ExecutorPool{
			WorkerID: worker.ID, Name: "default", CatalogRevision: worker.CatalogRevision,
			CPUClass: worker.CPUClass, Allocatable: worker.Allocatable,
		})
	}
	sort.Slice(pools, func(i, j int) bool { return pools[i].WorkerID < pools[j].WorkerID })
	return pools
}

// RebuildExecutorRegistry projects durable assignment ownership into a fresh
// registry. It is the coordinator-start path and the per-plan path: both
// rebuild from the same durable records rather than carrying state forward.
//
// An owner whose worker has no configured pool is skipped, because there is no
// capacity to account for. An owner that no longer fits its pool is still
// admitted: shrinking a pool drains future admission and never revokes a slot
// a running attempt already holds.
func RebuildExecutorRegistry(pools []domain.ExecutorPool, owners []CapacityOwner, now func() time.Time) (*ExecutorRegistry, error) {
	registry, err := NewExecutorRegistry(pools, now)
	if err != nil {
		return nil, err
	}
	for _, owner := range owners {
		if err := registry.adopt(owner); err != nil {
			return nil, fmt.Errorf("rebuild executor registry: %w", err)
		}
	}
	return registry, nil
}

// CapacityConstraint is immutable planning policy: the configured pools and
// the capacity durable assignments already hold.
type CapacityConstraint struct {
	pools  []domain.ExecutorPool
	owners []CapacityOwner
}

// NewCapacityConstraint builds the planning constraint from projected pools
// and durable owners.
func NewCapacityConstraint(pools []domain.ExecutorPool, owners []CapacityOwner) *CapacityConstraint {
	return &CapacityConstraint{
		pools:  append([]domain.ExecutorPool(nil), pools...),
		owners: append([]CapacityOwner(nil), owners...),
	}
}

// StartPlan gives one planning pass its own registry, so that proposing a task
// onto a worker reduces what remains available to the next task in the same
// pass instead of letting one pass over-assign a batch.
func (c *CapacityConstraint) StartPlan(now time.Time) PlanningConstraintSession {
	clock := func() time.Time { return now }
	registry, err := RebuildExecutorRegistry(c.pools, c.owners, clock)
	if err != nil {
		// Unusable capacity evidence must not silently admit work, so the
		// session blocks every governed worker instead of ignoring the pools.
		slog.Warn("planning capacity registry could not be rebuilt", "error", err)
		return &capacitySession{failure: err, pools: c.pools}
	}
	return &capacitySession{registry: registry}
}

type capacitySession struct {
	registry *ExecutorRegistry
	failure  error
	pools    []domain.ExecutorPool
}

func (s *capacitySession) Evaluate(candidate PlanningCandidate) []PlanningBlocker {
	if s.failure != nil {
		for _, pool := range s.pools {
			if pool.WorkerID == candidate.WorkerID {
				return []PlanningBlocker{{
					Code: PlanningBlockerExecutorCapacity, Dimension: CapacityDimensionSlots,
					Detail: fmt.Sprintf("executor capacity for worker %q is unknown: %v", candidate.WorkerID, s.failure),
				}}
			}
		}
		return nil
	}
	var blockers []PlanningBlocker
	for _, shortfall := range s.registry.Fits(candidate.WorkerID, candidate.Task.ResourceDemand) {
		blockers = append(blockers, PlanningBlocker{
			Code: PlanningBlockerExecutorCapacity, Dimension: shortfall.Dimension,
			Available: shortfall.Available, Required: shortfall.Required,
			Detail: fmt.Sprintf("worker %q has %v available %s for a requirement of %v",
				candidate.WorkerID, shortfall.Available, shortfall.Dimension, shortfall.Required),
		})
	}
	return blockers
}

func (s *capacitySession) Reserve(candidate PlanningCandidate) {
	if s.failure != nil || !s.registry.Governs(candidate.WorkerID) {
		return
	}
	_, err := s.registry.Reserve(ReservationRequest{
		ReservationID: candidate.Attempt.ID, WorkerID: candidate.WorkerID,
		AttemptID: candidate.Attempt.ID, Kind: ReservationKindAttempt,
		Demand: candidate.Task.ResourceDemand, ProviderAdmissionHeld: true,
	})
	if err != nil {
		// Evaluate refused every candidate this would exceed, so a failure here
		// means the planner proposed past its own constraint. Report it and
		// keep the session's accounting unchanged rather than over-committing.
		slog.Warn("planning reserved executor capacity that was not available",
			"worker", candidate.WorkerID, "attempt", candidate.Attempt.ID, "error", err)
	}
}
