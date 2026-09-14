package backlog

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestPlanningSpreadsABatchAcrossExecutorCapacity(t *testing.T) {
	workers := []domain.WorkerInventory{
		capacityPlanningWorker("normandy", domain.CPUClassLow, 2),
		capacityPlanningWorker("homelab", domain.CPUClassMedium, 4),
	}
	tasks := make([]domain.Task, 0, 7)
	for index := range 7 {
		tasks = append(tasks, testTask(fmt.Sprintf("task%d", index)))
	}
	input := capacityPlanInput(tasks, workers)

	plan, err := BuildPlan(input)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	placed := proposalsByWorker(plan)
	if placed["normandy"] > 2 || placed["homelab"] > 4 {
		t.Fatalf("batch over-assigned a worker: %#v", placed)
	}
	if total := len(plan.Proposals); total != 6 {
		t.Fatalf("proposals = %d, want the six slots the fleet configured", total)
	}
	if placed["normandy"] == 0 || placed["homelab"] == 0 {
		t.Fatalf("batch was not spread across workers: %#v", placed)
	}

	// The seventh task is refused by executor capacity, with the dimension
	// and the numbers that explain it, on every candidate worker.
	var refused int
	for _, decision := range plan.Decisions {
		if decision.Proposed {
			continue
		}
		refused++
		for _, candidate := range decision.Candidates {
			blocker := candidate.Blockers[0]
			if len(candidate.Blockers) != 1 || blocker.Code != PlanningBlockerExecutorCapacity ||
				blocker.Dimension != CapacityDimensionSlots ||
				blocker.Available != 0 || blocker.Required != 1 {
				t.Fatalf("unplaced candidate blockers = %#v", candidate.Blockers)
			}
		}
	}
	if refused != 1 {
		t.Fatalf("unplaced tasks = %d, want exactly one beyond capacity", refused)
	}
}

func TestPlanningReachesTheConfiguredConcurrencyFloors(t *testing.T) {
	for workerID, profile := range config.FleetWorkerProfiles() {
		slots := profile.Executors.Slots
		worker := capacityPlanningWorker(workerID, domain.CPUClass(profile.CPUClass), slots)
		tasks := make([]domain.Task, 0, slots)
		for index := range slots {
			tasks = append(tasks, testTask(fmt.Sprintf("task%d", index)))
		}
		plan, err := BuildPlan(capacityPlanInput(tasks, []domain.WorkerInventory{worker}))
		if err != nil {
			t.Fatalf("BuildPlan for %q: %v", workerID, err)
		}
		if placed := proposalsByWorker(plan)[workerID]; placed != slots {
			t.Fatalf("%s planned %d concurrent attempts, want its configured floor of %d",
				workerID, placed, slots)
		}
	}
}

func TestPlanningChoosesByPreferenceScoreRatherThanWorkerOrder(t *testing.T) {
	// The lexicographically first worker is the high-class one. Light work
	// should still land on the low-class worker, leaving high-class headroom
	// for work that needs it, which first-match selection could never do.
	workers := []domain.WorkerInventory{
		capacityPlanningWorker("aardvark", domain.CPUClassHigh, 4),
		capacityPlanningWorker("zulu", domain.CPUClassLow, 4),
	}
	light := testTask("light")
	light.ResourceDemand = domain.ResourceDemand{MinCPUClass: domain.CPUClassLow}

	plan, err := BuildPlan(capacityPlanInput([]domain.Task{light}, workers))
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if len(plan.Proposals) != 1 || plan.Proposals[0].WorkerID != "zulu" {
		t.Fatalf("proposals = %#v, want the lower-class worker chosen by score", plan.Proposals)
	}
	placement := plan.Proposals[0].Placement
	if placement == nil || placement.SelectedWorkerID != "zulu" || len(placement.Scores) != 2 {
		t.Fatalf("placement explanation = %#v", placement)
	}

	// Build work declares a floor the low-class worker cannot meet, so the
	// same fleet places it on the high-class worker and records why.
	build := testTask("build")
	build.ResourceDemand = domain.ResourceDemand{
		MinCPUClass: domain.CPUClassMedium, PreferredCPUClass: domain.CPUClassHigh,
	}
	buildPlan, err := BuildPlan(capacityPlanInput([]domain.Task{build}, workers))
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if len(buildPlan.Proposals) != 1 || buildPlan.Proposals[0].WorkerID != "aardvark" {
		t.Fatalf("build proposals = %#v, want the high-class worker", buildPlan.Proposals)
	}
	rejections := buildPlan.Proposals[0].Placement.Rejections
	if len(rejections) != 1 || rejections[0].WorkerID != "zulu" ||
		rejections[0].Code != ExclusionCPUClassBelowMinimum {
		t.Fatalf("build placement rejections = %#v", rejections)
	}
}

func TestPlanningIsIdenticalWhenReplannedWithUnchangedInput(t *testing.T) {
	workers := []domain.WorkerInventory{
		capacityPlanningWorker("alpha", domain.CPUClassMedium, 2),
		capacityPlanningWorker("beta", domain.CPUClassMedium, 2),
	}
	tasks := []domain.Task{testTask("one"), testTask("two"), testTask("three")}

	first, err := BuildPlan(capacityPlanInput(tasks, workers))
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	// Equal-scoring workers and a reordered input must still produce the same
	// plan, so a replan of unchanged state proposes exactly the same work.
	reordered := []domain.WorkerInventory{workers[1], workers[0]}
	second, err := BuildPlan(capacityPlanInput([]domain.Task{tasks[2], tasks[0], tasks[1]}, reordered))
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("replan differs:\nfirst:  %#v\nsecond: %#v", first.Proposals, second.Proposals)
	}
}

func TestCapacityOwnershipIsRebuiltFromAssignmentsAndReleasedOnEveryTerminalPath(t *testing.T) {
	pools := []domain.ExecutorPool{capacityPool("homelab", domain.CPUClassMedium, 4)}
	reasons := domain.ResourceReleaseReasons()

	var attempts []domain.Attempt
	var assignments []domain.Assignment
	var tasks []domain.Task
	for index := range reasons {
		name := fmt.Sprintf("task%d", index)
		task := testTask(name)
		tasks = append(tasks, task)
		attempts = append(attempts, domain.Attempt{
			ID: name + "-1", WorkflowRunID: "run", TaskID: task.ID, Number: 1,
			Progress: domain.ProgressActive, AssignmentID: "assignment-" + name,
		})
		assignments = append(assignments, domain.Assignment{
			ID: "assignment-" + name, AttemptID: name + "-1", WorkerID: "homelab",
			State: domain.AssignmentClaimed,
		})
	}
	runs := []domain.WorkflowRun{{ID: "run", WorkflowID: "workflow"}}

	owners := capacityOwners(attempts, assignments, runs, tasks)
	if len(owners) != len(reasons) {
		t.Fatalf("rebuilt owners = %#v, want one per live assignment", owners)
	}
	registry, err := RebuildExecutorRegistry(pools, owners, capacityClock())
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if free := registry.Snapshot("homelab").FreeSlots(); free != 0 {
		t.Fatalf("free slots after rebuild = %d, want the live assignments to hold them all", free)
	}

	for index, reason := range reasons {
		id := fmt.Sprintf("assignment-task%d", index)
		if _, err := registry.Release(id, reason); err != nil {
			t.Fatalf("release %q as %q: %v", id, string(reason), err)
		}
		if _, err := registry.Release(id, reason); err == nil {
			t.Fatalf("second release of %q was accepted", id)
		}
	}
	if outstanding := registry.Outstanding(); len(outstanding) != 0 {
		t.Fatalf("outstanding reservations after every terminal path = %#v", outstanding)
	}
	if free := registry.Snapshot("homelab").FreeSlots(); free != 4 {
		t.Fatalf("free slots after release = %d, want 4", free)
	}

	// The same evidence read from settled assignments holds nothing at all:
	// a terminal assignment is the release, so a coordinator restart rebuilds
	// an empty registry without a second durable record to reconcile.
	settled := append([]domain.Assignment(nil), assignments...)
	settledStates := []domain.AssignmentState{
		domain.AssignmentCompleted, domain.AssignmentReleased,
		domain.AssignmentReleased, domain.AssignmentReleased,
	}
	for index := range settled {
		settled[index].State = settledStates[index]
	}
	if remaining := capacityOwners(attempts, settled, runs, tasks); len(remaining) != 0 {
		t.Fatalf("owners after settlement = %#v, want none", remaining)
	}
	restarted, err := RebuildExecutorRegistry(pools, capacityOwners(attempts, settled, runs, tasks), capacityClock())
	if err != nil {
		t.Fatalf("rebuild after settlement: %v", err)
	}
	if outstanding := restarted.Outstanding(); len(outstanding) != 0 {
		t.Fatalf("restarted registry outstanding = %#v", outstanding)
	}
}

func TestRebuiltCapacityKeepsARunningAttemptAfterAPoolShrink(t *testing.T) {
	owners := []CapacityOwner{
		{AssignmentID: "assignment-1", AttemptID: "attempt-1", WorkerID: "normandy"},
		{AssignmentID: "assignment-2", AttemptID: "attempt-2", WorkerID: "normandy"},
	}
	// The pool now declares one slot although two assignments are live.
	registry, err := RebuildExecutorRegistry(
		[]domain.ExecutorPool{capacityPool("normandy", domain.CPUClassLow, 1)}, owners, capacityClock())
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if outstanding := registry.Outstanding(); len(outstanding) != 2 {
		t.Fatalf("outstanding = %#v, want both live assignments retained", outstanding)
	}
	if free := registry.Snapshot("normandy").FreeSlots(); free != 0 {
		t.Fatalf("free slots = %d, want none until the extra assignment releases", free)
	}
	if shortfalls := registry.Fits("normandy", domain.ResourceDemand{}); len(shortfalls) != 1 ||
		shortfalls[0].Dimension != CapacityDimensionSlots {
		t.Fatalf("shortfalls = %#v, want the shrunk pool to refuse new work", shortfalls)
	}
	if _, err := registry.Release("assignment-2", domain.ResourceReleaseSettled); err != nil {
		t.Fatalf("release: %v", err)
	}
	if outstanding := registry.Outstanding(); len(outstanding) != 1 {
		t.Fatalf("outstanding after release = %#v", outstanding)
	}
}

func TestCapacityConstraintIgnoresUngovernedWorkers(t *testing.T) {
	// A worker that declares no allocatable capacity is not capacity-governed:
	// unconfigured capacity is unknown, not exhausted, so planning behaves as
	// it did before executor pools existed.
	constraint := NewCapacityConstraint(executorPools([]domain.WorkerInventory{plannerWorker("worker-a")}), nil)
	session := constraint.StartPlan(plannerTestTime)
	candidate := PlanningCandidate{WorkerID: "worker-a", Task: testTask("alpha")}
	if blockers := session.Evaluate(candidate); len(blockers) != 0 {
		t.Fatalf("blockers for an ungoverned worker = %#v", blockers)
	}
	session.Reserve(candidate)
	if blockers := session.Evaluate(candidate); len(blockers) != 0 {
		t.Fatalf("blockers after reserving an ungoverned worker = %#v", blockers)
	}
}

func capacityPlanningWorker(id string, class domain.CPUClass, slots int) domain.WorkerInventory {
	worker := plannerWorker(id)
	worker.CPUClass = class
	worker.Allocatable = domain.AllocatableCapacity{ExecutorSlots: slots}
	return worker
}

func capacityPlanInput(tasks []domain.Task, workers []domain.WorkerInventory) PlanInput {
	input := plannerInput(tasks, workers)
	input.Constraints = []PlanningConstraint{NewCapacityConstraint(executorPools(workers), nil)}
	return input
}

func proposalsByWorker(plan Plan) map[string]int {
	placed := make(map[string]int, len(plan.Proposals))
	for _, proposal := range plan.Proposals {
		placed[proposal.WorkerID]++
	}
	return placed
}
