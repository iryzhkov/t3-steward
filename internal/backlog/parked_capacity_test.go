package backlog

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// nestedFleet is durable coordinator state with one-task runs on one worker,
// shaped like the feedback 147 fleet: every task can run only on omarchy-pc.
type nestedFleet struct {
	now     time.Time
	slots   int
	records sqlite.CoordinatorRecords
	waits   []domain.TaskWait
}

const nestedWorker = "omarchy-pc"

func newNestedFleet(slots int) *nestedFleet {
	return &nestedFleet{now: plannerTestTime, slots: slots}
}

// addRun adds a run with one ready task routed to codex/gpt and returns its
// attempt ID. The attempt became ready at readyAt.
func (f *nestedFleet) addRun(id string, class domain.TaskClass, readyAt time.Time, lineage *domain.RunLineage) string {
	task := routingTask(id, "codex", "gpt")
	task.ID, task.WorkflowID, task.Class = "task-"+id, "workflow-"+id, class
	task.Placement.Hosts = []string{nestedWorker}
	f.records.Workflows = append(f.records.Workflows, domain.Workflow{
		ID: task.WorkflowID, Project: "project", Class: class, TaskIDs: []string{task.ID},
		Environment: domain.ExecutionEnvironment{Scope: EnvironmentScopeTask},
	})
	f.records.WorkflowRuns = append(f.records.WorkflowRuns, domain.WorkflowRun{
		ID: "run-" + id, WorkflowID: task.WorkflowID, Progress: domain.ProgressActive,
		Revision: 1, CreatedAt: readyAt, UpdatedAt: readyAt, Lineage: lineage,
	})
	f.records.Tasks = append(f.records.Tasks, task)
	attempt := domain.Attempt{
		ID: id + "-1", WorkflowRunID: "run-" + id, TaskID: task.ID, Number: 1,
		Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, UpdatedAt: readyAt,
	}
	f.records.Attempts = append(f.records.Attempts, attempt)
	return attempt.ID
}

// start assigns an attempt to the worker and marks its turn running.
func (f *nestedFleet) start(attemptID string, at time.Time) {
	for index := range f.records.Attempts {
		attempt := &f.records.Attempts[index]
		if attempt.ID != attemptID {
			continue
		}
		attempt.AssignmentID = "assignment-" + attemptID
		attempt.Progress, attempt.Control = domain.ProgressActive, domain.ControlRunning
		started := at
		attempt.StartedAt = &started
		f.records.Assignments = append(f.records.Assignments, domain.Assignment{
			ID: attempt.AssignmentID, AttemptID: attemptID, WorkerID: nestedWorker, State: domain.AssignmentClaimed,
			Route:          domain.ProviderRoute{WorkerID: nestedWorker, ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "pool"},
			ExecutorDemand: &domain.ResourceDemand{}, CreatedAt: at,
		})
	}
}

func (f *nestedFleet) setControl(attemptID string, progress domain.ProgressState, control domain.ControlState) {
	for index := range f.records.Attempts {
		if f.records.Attempts[index].ID == attemptID {
			f.records.Attempts[index].Progress, f.records.Attempts[index].Control = progress, control
		}
	}
}

// finish ends an attempt and settles its assignment, releasing what it held.
func (f *nestedFleet) finish(attemptID string) {
	f.setControl(attemptID, domain.ProgressSucceeded, domain.ControlStopped)
	for index := range f.records.Assignments {
		if f.records.Assignments[index].AttemptID == attemptID {
			f.records.Assignments[index].State = domain.AssignmentCompleted
		}
	}
}

func (f *nestedFleet) plan(t *testing.T) Plan {
	t.Helper()
	worker := routingWorker(nestedWorker, routingProvider("codex", "pool", true, "gpt"))
	worker.Allocatable = domain.AllocatableCapacity{ExecutorSlots: f.slots}
	window := quotaTestWindow()
	window.ObservedAt = f.now.Add(-time.Minute)
	input, err := BuildCoordinatorPlanInput(CoordinatorPlanningStateInput{
		Now: f.now, CoordinatorEpoch: 4,
		Workflows: f.records.Workflows, WorkflowRuns: f.records.WorkflowRuns, Tasks: f.records.Tasks,
		Attempts: f.records.Attempts, Assignments: f.records.Assignments,
		WorkerSnapshots: []domain.WorkerSnapshot{{
			WorkerID: nestedWorker, WorkerEpoch: "epoch", CoordinatorEpoch: 4, Sequence: 1, Connected: true,
			Inventory: worker, ObservedAt: f.now.Add(-time.Minute), ValidUntil: f.now.Add(time.Hour),
		}},
		QuotaPools:           []domain.QuotaPool{routingPool("pool", 100, 0, "codex")},
		QuotaWindows:         []QuotaWindowBudget{window},
		DisableQuotaChecks:   true,
		MaxWorkerSnapshotAge: time.Hour, MaxQuotaObservationAge: time.Hour,
		DeadlineRiskWindow: time.Hour, CheckpointMargin: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlan(input)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func proposedAttempts(plan Plan) []string {
	var ids []string
	for _, proposal := range plan.Proposals {
		ids = append(ids, proposal.AttemptID)
	}
	return ids
}

func decisionFor(t *testing.T, plan Plan, attemptID string) TaskPlanningDecision {
	t.Helper()
	for _, decision := range plan.Decisions {
		if decision.AttemptID == attemptID {
			return decision
		}
	}
	t.Fatalf("no decision for attempt %q", attemptID)
	return TaskPlanningDecision{}
}

func lineageOf(parentRun, parentAttempt string, started time.Time) *domain.RunLineage {
	return &domain.RunLineage{
		ParentRunID: parentRun, ParentTaskID: "task-" + strings.TrimPrefix(parentRun, "run-"),
		ParentAttemptID: parentAttempt, ParentStartedAt: started, RecordedAt: started,
	}
}

// A child submitted from inside a parked parent is planned before root work of
// the same class that became ready after the parent started, even though the
// root became ready before the child did.
func TestNestedChildIsPlannedBeforeNewerRootOfTheSameClass(t *testing.T) {
	fleet := newNestedFleet(1)
	parentStart := fleet.now.Add(-time.Hour)
	parent := fleet.addRun("parent", domain.TaskClassRequired, parentStart, nil)
	fleet.start(parent, parentStart)
	fleet.setControl(parent, domain.ProgressWaitingExternal, domain.ControlWaitingExternal)
	root := fleet.addRun("root", domain.TaskClassRequired, fleet.now.Add(-20*time.Minute), nil)
	child := fleet.addRun("child", domain.TaskClassRequired, fleet.now.Add(-10*time.Minute),
		lineageOf("run-parent", parent, parentStart))

	plan := fleet.plan(t)
	if got := proposedAttempts(plan); !reflect.DeepEqual(got, []string{child}) {
		t.Fatalf("proposals = %v, want only the nested child %s ahead of the newer root %s", got, child, root)
	}
	order := decisionFor(t, plan, child).Order
	if order.NestedUnder != parent || !order.ReadySince.Equal(parentStart) || !strings.Contains(order.Reason, "nested under live attempt "+parent) {
		t.Fatalf("child order = %+v, want ready since the parent's start and nested under %s", order, parent)
	}

	// Negative: once the parent's turn is over the child is ordinary work, and
	// the root that has waited longer goes first.
	fleet.finish(parent)
	plan = fleet.plan(t)
	if got := proposedAttempts(plan); !reflect.DeepEqual(got, []string{root}) {
		t.Fatalf("proposals = %v, want the older root %s once the parent finished", got, root)
	}
	if order := decisionFor(t, plan, child).Order; order.NestedUnder != "" {
		t.Fatalf("child of a finished parent is still nested: %+v", order)
	}
}

// Root work that was already ready before the parent started keeps its place:
// lineage moves a child ahead of newer work, never ahead of older work.
func TestNestedChildDoesNotOvertakeOlderRoot(t *testing.T) {
	fleet := newNestedFleet(1)
	parentStart := fleet.now.Add(-time.Hour)
	older := fleet.addRun("older", domain.TaskClassRequired, parentStart.Add(-time.Minute), nil)
	parent := fleet.addRun("parent", domain.TaskClassRequired, parentStart, nil)
	fleet.start(parent, parentStart)
	fleet.setControl(parent, domain.ProgressWaitingExternal, domain.ControlWaitingExternal)
	fleet.addRun("child", domain.TaskClassRequired, fleet.now.Add(-time.Minute), lineageOf("run-parent", parent, parentStart))
	if got := proposedAttempts(fleet.plan(t)); !reflect.DeepEqual(got, []string{older}) {
		t.Fatalf("proposals = %v, want the root that was ready before the parent started", got)
	}
}

func TestResolveRunLineageChecksTheClaim(t *testing.T) {
	now := plannerTestTime
	started := now.Add(-time.Hour)
	records := sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{
			{ID: "live", WorkflowRunID: "run", TaskID: "task", Progress: domain.ProgressWaitingExternal,
				Control: domain.ControlWaitingExternal, AssignmentID: "assignment", UpdatedAt: now},
			{ID: "done", WorkflowRunID: "run", TaskID: "task", Progress: domain.ProgressSucceeded,
				Control: domain.ControlStopped, UpdatedAt: now},
		},
		Assignments: []domain.Assignment{{ID: "assignment", AttemptID: "live", CreatedAt: started}},
	}
	lineage, err := ResolveRunLineage(records, domain.SubmissionParent{RunID: "run", TaskID: "task", AttemptID: "live"}, now)
	if err != nil || lineage == nil || lineage.ParentAttemptID != "live" || !lineage.ParentStartedAt.Equal(started) || lineage.Validate() != nil {
		t.Fatalf("live parent lineage = %+v err=%v, want started at the assignment's creation", lineage, err)
	}
	if lineage, err := ResolveRunLineage(records, domain.SubmissionParent{RunID: "run", TaskID: "task", AttemptID: "done"}, now); err != nil || lineage != nil {
		t.Fatalf("finished parent lineage = %+v err=%v, want root work without error", lineage, err)
	}
	for name, parent := range map[string]domain.SubmissionParent{
		"unknown attempt": {RunID: "run", TaskID: "task", AttemptID: "forged"},
		"wrong run":       {RunID: "other", TaskID: "task", AttemptID: "live"},
		"wrong task":      {RunID: "run", TaskID: "other", AttemptID: "live"},
		"incomplete":      {RunID: "run", AttemptID: "live"},
	} {
		if lineage, err := ResolveRunLineage(records, parent, now); err == nil || lineage != nil {
			t.Errorf("%s: lineage = %+v err=%v, want refusal", name, lineage, err)
		}
	}
}

// deadlockFleet is feedback 147 with the release rule switched off: every slot
// of the worker is held by a parent parked on the child it submitted, and each
// child can run only on that worker.
func deadlockFleet(slots int) (*nestedFleet, []string) {
	fleet := newNestedFleet(slots)
	var children []string
	for index := 0; index < slots; index++ {
		name := string(rune('a' + index))
		start := fleet.now.Add(-time.Hour)
		parent := fleet.addRun("parent-"+name, domain.TaskClassRequired, start, nil)
		fleet.start(parent, start)
		fleet.setControl(parent, domain.ProgressWaitingExternal, domain.ControlWaitingExternal)
		child := fleet.addRun("child-"+name, domain.TaskClassRequired, fleet.now.Add(-time.Minute),
			lineageOf("run-parent-"+name, parent, start))
		fleet.waits = append(fleet.waits, domain.TaskWait{
			ID: "wait-" + name, AttemptID: parent, WorkflowRunID: "run-parent-" + name,
			Node: &domain.NodeWaitCondition{Target: domain.NodeRef{RunID: "run-child-" + name}},
		})
		children = append(children, child)
	}
	return fleet, children
}

func (f *nestedFleet) deadlockInput() CapacityDeadlockInput {
	return CapacityDeadlockInput{
		WorkflowRuns: f.records.WorkflowRuns, Tasks: f.records.Tasks, Attempts: f.records.Attempts,
		Assignments: f.records.Assignments, TaskWaits: f.waits,
	}
}

func withParkedCapacityHeld(t *testing.T) {
	t.Helper()
	parkedAttemptsReleaseCapacity = false
	t.Cleanup(func() { parkedAttemptsReleaseCapacity = true })
}

// With the release rule disabled through its seam, the constructed deadlock is
// reported on every child as capacity-deadlock, naming the holders, and the
// detector changes nothing else: no proposal, no other blocker, no record.
func TestCapacityDeadlockIsReportedWithoutAction(t *testing.T) {
	withParkedCapacityHeld(t)
	fleet, children := deadlockFleet(3)
	plan := fleet.plan(t)
	if len(plan.Proposals) != 0 {
		t.Fatalf("held parents still let children run: %v", proposedAttempts(plan))
	}
	if !PlanHasCapacityOnlyBlocks(plan) {
		t.Fatal("children blocked only by executor capacity were not recognised")
	}
	before := clonePlanForComparison(plan)
	records := fleet.deadlockInput()
	AnnotateCapacityDeadlocks(&plan, records)
	if len(plan.Proposals) != len(before.Proposals) || len(plan.Decisions) != len(before.Decisions) {
		t.Fatal("the detector changed the plan's proposals or decisions")
	}
	if !reflect.DeepEqual(records, fleet.deadlockInput()) {
		t.Fatal("the detector changed the records it read")
	}
	for _, child := range children {
		decision := decisionFor(t, plan, child)
		var found *PlanningBlocker
		for index := range decision.Blockers {
			if decision.Blockers[index].Code == PlanningBlockerCapacityDeadlock {
				found = &decision.Blockers[index]
			}
		}
		if found == nil {
			t.Fatalf("child %s blockers = %+v, want %s", child, decision.Blockers, PlanningBlockerCapacityDeadlock)
		}
		if !strings.Contains(found.Detail, nestedWorker) || !strings.Contains(found.Detail, "parent-a-1") ||
			!strings.Contains(found.Detail, "nothing is changed automatically") || found.OwnerID == "" {
			t.Fatalf("deadlock blocker = %+v, want the worker, the holders and no automatic action named", *found)
		}
		if len(decision.Blockers) != len(decisionFor(t, before, child).Blockers)+1 {
			t.Fatalf("the detector changed other blockers of %s", child)
		}
	}
}

// Lineage alone is a wait edge for a parked parent: a parent that parked on a
// shell condition instead of a node wait still waits on its child.
func TestCapacityDeadlockFollowsLineageOfParkedParents(t *testing.T) {
	withParkedCapacityHeld(t)
	fleet, children := deadlockFleet(2)
	fleet.waits = nil
	plan := fleet.plan(t)
	AnnotateCapacityDeadlocks(&plan, fleet.deadlockInput())
	if !hasBlocker(decisionFor(t, plan, children[0]), PlanningBlockerCapacityDeadlock) {
		t.Fatal("parked parents linked only by lineage were not recognised as waiting on the child")
	}
}

// A holder that does not wait on the blocked task's run, directly or through
// other runs, can still finish and free its slot, so nothing is reported.
func TestCapacityDeadlockNeedsEveryHolderWaiting(t *testing.T) {
	withParkedCapacityHeld(t)
	fleet, children := deadlockFleet(2)
	// Parent b now waits on an unrelated run instead of its own child, and is
	// not the child's lineage parent either.
	fleet.waits[1].Node.Target.RunID = "run-unrelated"
	for index := range fleet.records.WorkflowRuns {
		if fleet.records.WorkflowRuns[index].ID == "run-child-b" {
			fleet.records.WorkflowRuns[index].Lineage = nil
		}
	}
	plan := fleet.plan(t)
	AnnotateCapacityDeadlocks(&plan, fleet.deadlockInput())
	for _, child := range children {
		if hasBlocker(decisionFor(t, plan, child), PlanningBlockerCapacityDeadlock) {
			t.Fatalf("child %s reported deadlocked although parent b can still finish", child)
		}
	}
}

// Transitive waiting: parent a waits on run x, whose task waits on the child's
// run. Parent a holds the only slot the child can use.
func TestCapacityDeadlockIsTransitive(t *testing.T) {
	withParkedCapacityHeld(t)
	fleet := newNestedFleet(1)
	start := fleet.now.Add(-time.Hour)
	parent := fleet.addRun("parent", domain.TaskClassRequired, start, nil)
	fleet.start(parent, start)
	fleet.setControl(parent, domain.ProgressWaitingExternal, domain.ControlWaitingExternal)
	middle := fleet.addRun("middle", domain.TaskClassRequired, start, nil)
	fleet.setControl(middle, domain.ProgressWaitingExternal, domain.ControlWaitingExternal)
	child := fleet.addRun("child", domain.TaskClassRequired, fleet.now.Add(-time.Minute), nil)
	fleet.waits = []domain.TaskWait{
		{ID: "outer", AttemptID: parent, Node: &domain.NodeWaitCondition{Target: domain.NodeRef{RunID: "run-middle"}}},
		{ID: "inner", AttemptID: middle, Node: &domain.NodeWaitCondition{Target: domain.NodeRef{RunID: "run-child"}}},
	}
	plan := fleet.plan(t)
	AnnotateCapacityDeadlocks(&plan, fleet.deadlockInput())
	if !hasBlocker(decisionFor(t, plan, child), PlanningBlockerCapacityDeadlock) {
		t.Fatal("a holder waiting on the child's run through another run was not recognised")
	}
}

// With the release rule in force the same fleet has no deadlock: every parked
// parent's slot is free, so every child is proposed.
func TestParkedParentsReleaseTheSlotsTheirChildrenNeed(t *testing.T) {
	fleet, children := deadlockFleet(3)
	plan := fleet.plan(t)
	if got := proposedAttempts(plan); len(got) != len(children) {
		t.Fatalf("proposals = %v, want every child %v", got, children)
	}
	if PlanHasCapacityOnlyBlocks(plan) {
		t.Fatal("a capacity-only block remains with parked parents released")
	}
}

func hasBlocker(decision TaskPlanningDecision, code string) bool {
	for _, blocker := range decision.Blockers {
		if blocker.Code == code {
			return true
		}
	}
	return false
}

func clonePlanForComparison(plan Plan) Plan {
	cloned := Plan{Proposals: append([]ProposedTask(nil), plan.Proposals...)}
	for _, decision := range plan.Decisions {
		decision.Blockers = append([]PlanningBlocker(nil), decision.Blockers...)
		cloned.Decisions = append(cloned.Decisions, decision)
	}
	return cloned
}
