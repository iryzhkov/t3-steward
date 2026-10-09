package backlog

import (
	"fmt"
	"sort"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// PlanningBlockerCapacityDeadlock reports a ready task that is blocked only by
// executor or quota-pool capacity, where every attempt holding that capacity is
// waiting, directly or through other runs, on this task's run or on the run of
// another task stuck the same way. Nothing will release the capacity: the
// holders finish only after the blocked tasks run. Feedback 147 was the second
// shape: N parents held a worker's N slots, each parked on its own child, and
// every child could run only on that worker.
//
// Parked attempts release their capacity, so this should not occur. It is a
// backstop that names the cycle for an operator. The planner takes no action on
// it: cancelling or releasing one of the holders is an operator decision.
const PlanningBlockerCapacityDeadlock = "capacity-deadlock"

// parkedAttemptsReleaseCapacity is the release rule for an attempt parked on a
// task-bound wait: it gives back its executor slot, its quota-pool slot and its
// CPU and memory reservation, and keeps only its workspace. Tests turn it off
// to construct the deadlock the release rule exists to prevent; production code
// never changes it.
var parkedAttemptsReleaseCapacity = true

// attemptHoldsProviderSlot is ControlState.HoldsProviderSlot under the release
// rule above.
func attemptHoldsProviderSlot(control domain.ControlState) bool {
	return control.HoldsProviderSlot() ||
		(!parkedAttemptsReleaseCapacity && control == domain.ControlWaitingExternal)
}

// CapacityDeadlockInput is the durable state the detector reads besides the
// plan: who holds capacity, and who waits on what.
type CapacityDeadlockInput struct {
	WorkflowRuns []domain.WorkflowRun
	Tasks        []domain.Task
	Attempts     []domain.Attempt
	Assignments  []domain.Assignment
	TaskWaits    []domain.TaskWait
}

// PlanHasCapacityOnlyBlocks reports whether any unproposed ready task in the
// plan is blocked by capacity alone, which is the only case the detector
// examines. Callers use it to skip loading wait records on ordinary passes.
func PlanHasCapacityOnlyBlocks(plan Plan) bool {
	for _, decision := range plan.Decisions {
		if _, _, ok := capacityOnlyBlock(decision); ok {
			return true
		}
	}
	return false
}

// AnnotateCapacityDeadlocks adds a capacity-deadlock blocker to every decision
// in a capacity deadlock. It only adds explanation: proposals, decisions' other
// blockers and every record are left as they are.
//
// The deadlocked set is the largest set of capacity-only blocked tasks in which
// every holder of every resource such a task needs waits, directly or through
// other runs, on the run of a task in the set. It is found by starting from
// every capacity-only blocked task and removing, until nothing changes, each
// task one of whose resources has no holder or has a holder that waits on no
// run left in the set: that holder can finish without the set, so its capacity
// will come back.
func AnnotateCapacityDeadlocks(plan *Plan, input CapacityDeadlockInput) {
	if plan == nil {
		return
	}
	workerHolders := make(map[string][]string)
	for _, owner := range capacityOwners(input.Attempts, input.Assignments, input.WorkflowRuns, input.Tasks) {
		workerHolders[owner.WorkerID] = append(workerHolders[owner.WorkerID], owner.AttemptID)
	}
	poolHolders := providerSlotHolders(input.Attempts, input.Assignments)
	waits := newRunWaitGraph(input)

	type resource struct {
		kind, id string
		holders  []string
	}
	blocked := make(map[int][]resource)
	for index, decision := range plan.Decisions {
		workers, pools, ok := capacityOnlyBlock(decision)
		if !ok {
			continue
		}
		var resources []resource
		for _, worker := range workers {
			resources = append(resources, resource{kind: "worker", id: worker, holders: workerHolders[worker]})
		}
		for _, pool := range pools {
			resources = append(resources, resource{kind: "quota pool", id: pool, holders: poolHolders[pool]})
		}
		blocked[index] = resources
	}
	for changed := true; changed; {
		changed = false
		runs := make(map[string]bool, len(blocked))
		for index := range blocked {
			runs[plan.Decisions[index].WorkflowRunID] = true
		}
		for index, resources := range blocked {
			for _, resource := range resources {
				stuck := len(resource.holders) != 0
				for _, holder := range resource.holders {
					stuck = stuck && waits.waitsOnAny(holder, runs)
				}
				if !stuck {
					delete(blocked, index)
					changed = true
					break
				}
			}
		}
	}
	for index, resources := range blocked {
		decision := &plan.Decisions[index]
		var held []string
		owner := ""
		for _, resource := range resources {
			sorted := append([]string(nil), resource.holders...)
			sort.Strings(sorted)
			if owner == "" {
				owner = sorted[0]
			}
			held = append(held, fmt.Sprintf("%s %q is held by %s", resource.kind, resource.id, strings.Join(sorted, ", ")))
		}
		decision.Blockers = append(decision.Blockers, PlanningBlocker{
			Code:    PlanningBlockerCapacityDeadlock,
			OwnerID: owner,
			Detail: fmt.Sprintf("every executor or pool slot this task can use is held by attempts waiting on this run or on other runs blocked the same way: %s; "+
				"they cannot finish before these tasks run, and nothing is changed automatically: cancel one of them or its wait",
				strings.Join(held, "; ")),
		})
		sortPlanningBlockers(decision.Blockers)
	}
}

// capacityOnlyBlock reports the workers and pools whose capacity is the only
// thing keeping a ready, unproposed task from a proposal: every candidate is
// refused for executor capacity or pool concurrency and for nothing else.
func capacityOnlyBlock(decision TaskPlanningDecision) ([]string, []string, bool) {
	if decision.Proposed || decision.Progress != domain.ProgressReady || len(decision.Candidates) == 0 {
		return nil, nil, false
	}
	for _, blocker := range decision.Blockers {
		if blocker.Code != PlanningBlockerCandidatePolicy {
			return nil, nil, false
		}
	}
	workers, pools := map[string]struct{}{}, map[string]struct{}{}
	for _, candidate := range decision.Candidates {
		if len(candidate.Blockers) == 0 {
			return nil, nil, false
		}
		for _, blocker := range candidate.Blockers {
			switch blocker.Code {
			case PlanningBlockerExecutorCapacity:
				if blocker.Dimension != CapacityDimensionSlots && blocker.Dimension != CapacityDimensionCPUUnits &&
					blocker.Dimension != CapacityDimensionMemoryMB && blocker.Dimension != CapacityDimensionScratchMB {
					return nil, nil, false
				}
				workers[firstNonEmpty(blocker.WorkerID, candidate.WorkerID)] = struct{}{}
			case PlanningBlockerPoolConcurrency:
				if blocker.QuotaPoolID == "" {
					return nil, nil, false
				}
				pools[blocker.QuotaPoolID] = struct{}{}
			default:
				return nil, nil, false
			}
		}
	}
	return sortedKeys(workers), sortedKeys(pools), true
}

// providerSlotHolders maps each quota pool to the attempts holding one of its
// concurrency slots, by the rule DeriveQuotaPlanningState counts them with.
func providerSlotHolders(attempts []domain.Attempt, assignments []domain.Assignment) map[string][]string {
	byID := make(map[string]domain.Assignment, len(assignments))
	for _, assignment := range assignments {
		byID[assignment.ID] = assignment
	}
	holders := make(map[string][]string)
	for _, attempt := range attempts {
		if attempt.Progress.Terminal() || attempt.AssignmentID == "" {
			continue
		}
		assignment, found := byID[attempt.AssignmentID]
		if !found || assignment.AttemptID != attempt.ID || assignment.Route.QuotaPoolID == "" ||
			assignment.State == domain.AssignmentCompleted || assignment.State == domain.AssignmentReleased {
			continue
		}
		if attemptHoldsProviderSlot(attempt.Control) || assignment.State == domain.AssignmentUnknown {
			holders[assignment.Route.QuotaPoolID] = append(holders[assignment.Route.QuotaPoolID], attempt.ID)
		}
	}
	return holders
}

// runWaitGraph answers whether an attempt waits, directly or through other
// runs, on a run. An attempt waits on a run when a wait it registered that
// has not resumed it names a node of that run, or when it is parked and that
// run was submitted from it. Waiting is transitive through the attempts of a
// waited-on run.
type runWaitGraph struct {
	direct        map[string]map[string]bool
	attemptsByRun map[string][]string
}

func newRunWaitGraph(input CapacityDeadlockInput) runWaitGraph {
	graph := runWaitGraph{direct: map[string]map[string]bool{}, attemptsByRun: map[string][]string{}}
	add := func(attemptID, runID string) {
		if attemptID == "" || runID == "" {
			return
		}
		if graph.direct[attemptID] == nil {
			graph.direct[attemptID] = map[string]bool{}
		}
		graph.direct[attemptID][runID] = true
	}
	attempts := make(map[string]domain.Attempt, len(input.Attempts))
	for _, attempt := range input.Attempts {
		attempts[attempt.ID] = attempt
		if !attempt.Progress.Terminal() {
			graph.attemptsByRun[attempt.WorkflowRunID] = append(graph.attemptsByRun[attempt.WorkflowRunID], attempt.ID)
		}
	}
	for _, wait := range input.TaskWaits {
		if wait.Woken() || wait.Node == nil {
			continue
		}
		add(wait.AttemptID, wait.Node.Target.RunID)
	}
	for _, run := range input.WorkflowRuns {
		if run.Lineage == nil || run.Progress.Terminal() {
			continue
		}
		parent, found := attempts[run.Lineage.ParentAttemptID]
		if found && (parent.Control == domain.ControlWaitingExternal || parent.Progress == domain.ProgressWaitingExternal) {
			add(parent.ID, run.ID)
		}
	}
	return graph
}

// waitsOnAny reports whether the attempt waits, directly or through other
// runs, on any of the runs.
func (graph runWaitGraph) waitsOnAny(attemptID string, runIDs map[string]bool) bool {
	seenRuns := map[string]bool{}
	seenAttempts := map[string]bool{attemptID: true}
	queue := []string{attemptID}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for target := range graph.direct[current] {
			if runIDs[target] {
				return true
			}
			if seenRuns[target] {
				continue
			}
			seenRuns[target] = true
			for _, next := range graph.attemptsByRun[target] {
				if !seenAttempts[next] {
					seenAttempts[next] = true
					queue = append(queue, next)
				}
			}
		}
	}
	return false
}
