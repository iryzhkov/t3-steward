package campaign

// The core supervised path, verification gate 3 of
// docs/plans/campaign-supervision.md: two producers, a review gate, one
// downstream task and a final-settlement gate.
//
// It submits the checked-in supervised example through the real campaign path
// beside an ordinary unsupervised campaign in the same coordinator, and then
// asserts the four facts the plan names. The downstream task is not offered
// before a valid acceptance. A rejection holds it while the unsupervised
// campaign keeps being scheduled, because supervision of one run never withholds
// another's. An acceptance bound to the evidence the producers actually left
// releases the dispatch. And the final-settlement gate keeps the run nonterminal
// until it is decided, without a dummy agent task existing anywhere.

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type supervisedFixture struct {
	store        *sqlite.Store
	storage      string
	root         string
	supervised   string
	unsupervised string
}

func TestSupervisedCampaignWithholdsDispatchUntilAcceptance(t *testing.T) {
	ctx := context.Background()
	fixture := submitSupervisedCampaign(t)
	store := fixture.store
	operator := domain.Actor{Kind: domain.ActorOperator, Principal: "operator-a"}

	// Ingestion materialized the declared supervision and nothing a decision
	// owns: two gates closed before any producer ran, no hold, no incident, no
	// acceptance. The unsupervised campaign in the same coordinator has no
	// supervision record at all.
	snapshot, err := store.LoadSupervisionSnapshot(ctx, fixture.supervised)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Supervised || len(snapshot.Gates) != 2 || len(snapshot.Holds) != 0 {
		t.Fatalf("materialized supervision = %#v", snapshot)
	}
	for _, gate := range snapshot.Gates {
		if gate.State != domain.GatePendingEvidence || gate.EvidenceSnapshotID != "" {
			t.Fatalf("gate %q starts %q with evidence %q, want closed and unbound",
				gate.Definition.ID, gate.State, gate.EvidenceSnapshotID)
		}
	}
	projection, err := store.LoadSupervisionProjection(ctx, fixture.supervised)
	if err != nil {
		t.Fatal(err)
	}
	if len(projection.Incidents) != 0 || len(projection.Decisions) != 0 {
		t.Fatalf("a fresh supervised run already has %d incidents and %d decisions",
			len(projection.Incidents), len(projection.Decisions))
	}
	if other, err := store.LoadSupervisionSnapshot(ctx, fixture.unsupervised); err != nil || other.Supervised {
		t.Fatalf("the unsupervised campaign reports supervision %#v (err %v)", other, err)
	}

	// The prompt and the rubrics were rewritten from bundle paths to the
	// artifacts this ingestion retained, so a review reads the exact bytes the
	// submission declared.
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run := supervisedRun(t, records, fixture.supervised)
	if run.Supervision == nil {
		t.Fatal("the supervised run carries no declared supervision record")
	}
	retained := map[string]bool{}
	for _, artifact := range records.Artifacts {
		retained[artifact.ID] = true
	}
	if !retained[run.Supervision.Config.PromptArtifactID] {
		t.Fatalf("overseer prompt %q is not a retained artifact", run.Supervision.Config.PromptArtifactID)
	}
	for _, gate := range snapshot.Gates {
		if gate.Definition.RubricArtifactID != "" && !retained[gate.Definition.RubricArtifactID] {
			t.Fatalf("gate %q rubric %q is not a retained artifact",
				gate.Definition.ID, gate.Definition.RubricArtifactID)
		}
	}
	tasks := supervisedTasksByName(records, fixture.supervised)
	synthesis := tasks["synthesis"]
	analysisReview := supervisionGateByName(t, snapshot, "analysis_review")
	if !analysisReview.Definition.Protects(synthesis.ID) {
		t.Fatalf("gate %q protects %v, want the synthesis task id %q",
			analysisReview.Definition.ID, analysisReview.Definition.ProtectedTaskIDs, synthesis.ID)
	}

	// First wave: the two analyses, and the unsupervised campaign's own
	// producers. Synthesis is blocked by its ordinary dependencies, so no gate
	// has to refuse it yet.
	coordinator := backlog.FleetCoordinator{Store: store, Now: func() time.Time { return baselineTime }}
	first := supervisedCommit(ctx, t, coordinator, store, records, baselineTime)
	if len(first) == 0 {
		t.Fatal("the first wave scheduled nothing")
	}
	if first[synthesis.ID] {
		t.Fatal("synthesis was offered before its producers ran")
	}
	for _, name := range []string{"interfaces", "tests"} {
		if !first[tasks[name].ID] {
			t.Fatalf("the first wave did not offer %q: offered %v", name, first)
		}
	}

	// Both producers succeed with their declared outputs in coordinator custody.
	supervisedFinalize(ctx, t, fixture, []string{"interfaces", "tests"}, baselineTime)

	// The coordinator notices the producers and makes the gate reviewable,
	// binding the evidence identity it was made ready against.
	advanced, err := store.AdvanceSupervisionGates(ctx, fixture.supervised, baselineTime.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("advance gates: %v", err)
	}
	if len(advanced) != 1 || advanced[0].Gate.Definition.Name != "analysis_review" {
		t.Fatalf("advanced gates = %#v, want only the analysis review", advanced)
	}
	if advanced[0].Evidence.ID == "" || len(advanced[0].Evidence.Producers) != 2 {
		t.Fatalf("bound evidence = %#v, want both observed producers named", advanced[0].Evidence)
	}

	// A gate that is merely reviewable does not release anything. Synthesis is
	// now dependency-ready and is still withheld, with an explainable reason.
	records = reload(ctx, t, store)
	blockers := supervisedBlockers(t, ctx, store, records, fixture.supervised, synthesis.ID, baselineTime.Add(3*time.Minute))
	if !hasSupervisionCode(blockers, domain.SupervisionBlockerGateAwaitingReview) {
		t.Fatalf("synthesis blockers = %#v, want the awaiting-review gate", blockers)
	}
	if offered := supervisedCommit(ctx, t, coordinator, store, records, baselineTime.Add(3*time.Minute)); offered[synthesis.ID] {
		t.Fatal("synthesis was offered while its gate was only awaiting review")
	}

	// A rejection holds the branch. The unsupervised campaign in the same
	// coordinator keeps being scheduled, because supervision of one run never
	// withholds another run's work.
	state := adminState(ctx, t, store, fixture.supervised)
	reviewable := gateFacts(t, state, analysisReview.Definition.ID)
	if _, err := store.DecideGate(ctx, sqlite.GateDecisionRequest{
		RunID: fixture.supervised, GateID: analysisReview.Definition.ID, RequestID: "decide-reject",
		Actor:                 operator,
		ExpectedGraphRevision: reviewable.Gate.GraphRevision,
		ExpectedGateRevision:  reviewable.Gate.Revision,
		Evidence:              *reviewable.Evidence,
		Outcome:               domain.GateDecisionReject,
		Reason:                "the two analyses disagree about the interface boundary",
		DecidedAt:             baselineTime.Add(4 * time.Minute),
	}); err != nil {
		t.Fatalf("reject the gate: %v", err)
	}
	records = reload(ctx, t, store)
	blockers = supervisedBlockers(t, ctx, store, records, fixture.supervised, synthesis.ID, baselineTime.Add(5*time.Minute))
	if !hasSupervisionCode(blockers, domain.SupervisionBlockerGateHeld) {
		t.Fatalf("synthesis blockers after rejection = %#v, want the held gate", blockers)
	}
	unsupervisedTasks := supervisedTasksByName(records, fixture.unsupervised)
	unrelated := supervisedBlockers(t, ctx, store, records, fixture.unsupervised,
		unsupervisedTasks["join"].ID, baselineTime.Add(5*time.Minute))
	for _, blocker := range unrelated {
		if blocker.SupervisionCode != "" {
			t.Fatalf("the unsupervised campaign was withheld by supervision: %#v", blocker)
		}
	}

	// A held gate cannot be re-decided on the same rejected evidence: an
	// overseer may not wake itself out of its own rejection.
	state = adminState(ctx, t, store, fixture.supervised)
	held := gateFacts(t, state, analysisReview.Definition.ID)
	if _, err := store.DecideGate(ctx, sqlite.GateDecisionRequest{
		RunID: fixture.supervised, GateID: analysisReview.Definition.ID, RequestID: "decide-again",
		Actor: operator, ExpectedGraphRevision: held.Gate.GraphRevision, ExpectedGateRevision: held.Gate.Revision,
		Evidence: *held.Evidence, Outcome: domain.GateDecisionAccept,
		Reason: "changed my mind", DecidedAt: baselineTime.Add(6 * time.Minute),
	}); err == nil {
		t.Fatal("a held gate accepted the same evidence it had already rejected")
	}

	// Operator-authorized reconsideration returns it to review against a fresh
	// snapshot, and an acceptance bound to the evidence the producers actually
	// left releases the dispatch.
	if _, err := store.ReconsiderGate(ctx, sqlite.GateReconsiderRequest{
		RunID: fixture.supervised, GateID: analysisReview.Definition.ID, RequestID: "reconsider",
		Actor: operator, Reason: "the disagreement was a wording difference", At: baselineTime.Add(7 * time.Minute),
	}); err != nil {
		t.Fatalf("reconsider the gate: %v", err)
	}
	state = adminState(ctx, t, store, fixture.supervised)
	ready := gateFacts(t, state, analysisReview.Definition.ID)
	if ready.Gate.State != domain.GateReadyForReview {
		t.Fatalf("reconsidered gate = %q, want ready for review", ready.Gate.State)
	}
	if _, err := store.DecideGate(ctx, sqlite.GateDecisionRequest{
		RunID: fixture.supervised, GateID: analysisReview.Definition.ID, RequestID: "decide-accept",
		Actor: operator, ExpectedGraphRevision: ready.Gate.GraphRevision, ExpectedGateRevision: ready.Gate.Revision,
		Evidence: *ready.Evidence, Outcome: domain.GateDecisionAccept,
		Reason: "both analyses meet the rubric", DecidedAt: baselineTime.Add(8 * time.Minute),
	}); err != nil {
		t.Fatalf("accept the gate: %v", err)
	}
	records = reload(ctx, t, store)
	if offered := supervisedCommit(ctx, t, coordinator, store, records, baselineTime.Add(9*time.Minute)); !offered[synthesis.ID] {
		t.Fatal("synthesis was still withheld after a valid acceptance")
	}

	// The final-settlement gate guards run settlement rather than a downstream
	// task, and no dummy agent task exists to represent it.
	supervisedFinalize(ctx, t, fixture, []string{"synthesis"}, baselineTime.Add(10*time.Minute))
	supervisedCompleteAssignments(ctx, t, store)
	if _, err := store.AdvanceSupervisionGates(ctx, fixture.supervised, baselineTime.Add(11*time.Minute)); err != nil {
		t.Fatalf("advance the final gate: %v", err)
	}
	finalGate := gateFacts(t, adminState(ctx, t, store, fixture.supervised), supervisionGateByName(t,
		supervisionSnapshot(ctx, t, store, fixture.supervised), "final_report").Definition.ID)
	if finalGate.Gate.State != domain.GateReadyForReview {
		t.Fatalf("final gate = %q, want ready for review once synthesis succeeded", finalGate.Gate.State)
	}
	projectionStore := backlog.CoordinatorSupervisionStore{Store: store}
	heldOpen, err := backlog.ProjectWorkflowRuns(ctx, projectionStore, baselineTime.Add(12*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if settled := supervisedRun(t, reload(ctx, t, store), fixture.supervised); settled.Sink.Progress.Terminal() {
		t.Fatal("the run settled while its final-settlement gate was undecided")
	}
	if len(heldOpen.Supervision) != 1 ||
		heldOpen.Supervision[0].Barrier.Reason != domain.SinkBarrierFinalGateUnaccepted {
		t.Fatalf("settlement barrier = %#v, want the unaccepted final gate", heldOpen.Supervision)
	}
	if _, err := store.DecideGate(ctx, sqlite.GateDecisionRequest{
		RunID: fixture.supervised, GateID: finalGate.Gate.Definition.ID, RequestID: "decide-final",
		Actor: operator, ExpectedGraphRevision: finalGate.Gate.GraphRevision,
		ExpectedGateRevision: finalGate.Gate.Revision, Evidence: *finalGate.Evidence,
		Outcome: domain.GateDecisionAccept, Reason: "the synthesis is the report",
		DecidedAt: baselineTime.Add(13 * time.Minute),
	}); err != nil {
		t.Fatalf("accept the final gate: %v", err)
	}
	if _, err := backlog.ProjectWorkflowRuns(ctx, projectionStore, baselineTime.Add(14*time.Minute)); err != nil {
		t.Fatal(err)
	}
	final := reload(ctx, t, store)
	if settled := supervisedRun(t, final, fixture.supervised); !settled.Sink.Progress.Terminal() {
		t.Fatalf("the run did not settle after its final gate was accepted: %#v", settled.Sink)
	}
	for _, task := range final.Tasks {
		if task.Name == "overseer" || task.Name == "analysis_review" || task.Name == "final_report" {
			t.Fatalf("a gate was modelled as task %q; gates are graph-level objects", task.Name)
		}
	}
}

// submitSupervisedCampaign submits the supervised example and an ordinary
// unsupervised campaign into one coordinator, through the real loader, packer
// and ingestion service.
func submitSupervisedCampaign(t *testing.T) supervisedFixture {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	store, err := sqlite.OpenMigrated(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	storage := filepath.Join(root, "storage")
	t.Cleanup(func() { baselineCleanup(t, storage) })
	service := &backlog.SubmissionService{
		StorageRoot: storage, Store: store,
		MaxBytes: DefaultLimits.MaxBytes, MaxFiles: DefaultLimits.MaxFiles,
	}
	submit := func(example, key string) string {
		bundle, err := Prepare(exampleRoot(example), DefaultLimits)
		if err != nil {
			t.Fatalf("prepare the %s example: %v", example, err)
		}
		accepted, err := service.SubmitArchive(ctx, backlog.ArchiveSubmission{
			IdempotencyKey: key, Archive: bytes.NewReader(bundle.Archive),
		})
		if err != nil {
			t.Fatalf("submit the %s campaign: %v", example, err)
		}
		return accepted.Record.RunID
	}
	supervised := submit("supervised-three-node", "supervised-path")
	unsupervised := submit("three-node", "unsupervised-neighbour")
	if err := store.SaveWorkerSnapshot(ctx, baselineSnapshot()); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		QuotaPools: []domain.QuotaPool{baselineQuotaPool(), baselineSupervisedQuotaPool()},
	}); err != nil {
		t.Fatal(err)
	}
	return supervisedFixture{
		store: store, storage: storage, root: root,
		supervised: supervised, unsupervised: unsupervised,
	}
}

// supervisedPlanInput projects every run into planner input, with the
// supervision snapshot of each supervised run resolved from the store. The
// overseer route is reported available: this test drives the decisions as an
// operator, and an unavailable route is a separate case.
func supervisedPlanInput(
	t *testing.T,
	ctx context.Context,
	store *sqlite.Store,
	records sqlite.CoordinatorRecords,
	now time.Time,
) backlog.PlanInput {
	t.Helper()
	input := baselinePlanInput(records, now)
	workflows := make([]backlog.PlanningWorkflow, 0, len(records.WorkflowRuns))
	snapshots := make(map[string]domain.SupervisionSnapshot)
	for _, run := range records.WorkflowRuns {
		var workflow domain.Workflow
		for _, candidate := range records.Workflows {
			if candidate.ID == run.WorkflowID {
				workflow = candidate
			}
		}
		workflows = append(workflows, backlog.PlanningWorkflow{
			Workflow: workflow,
			State: backlog.DAGState{
				Run:      run,
				Tasks:    domain.TasksForRun(run, records.Tasks),
				Attempts: attemptsOfRun(records.Attempts, run.ID),
			},
		})
		snapshot, err := store.LoadSupervisionSnapshot(ctx, run.ID)
		if err != nil {
			t.Fatalf("load supervision snapshot of %q: %v", run.ID, err)
		}
		if snapshot.Supervised {
			snapshot.RouteAvailable = true
			snapshots[run.ID] = snapshot
		}
	}
	input.Workflows = workflows
	input.SupervisionSnapshots = snapshots
	input.QuotaPools = []domain.QuotaPool{baselineQuotaPool(), baselineSupervisedQuotaPool()}
	// Each attempt is estimated on the route its own task declares, because the
	// two campaigns in this coordinator declare different models on purpose.
	tasksByID := make(map[string]domain.Task, len(records.Tasks))
	for _, task := range records.Tasks {
		tasksByID[task.ID] = task
	}
	estimates := make([]backlog.RouteEstimate, 0, len(records.Attempts))
	for _, attempt := range records.Attempts {
		task := tasksByID[attempt.TaskID]
		if len(task.Routes) == 0 {
			continue
		}
		estimates = append(estimates, backlog.RouteEstimate{
			AttemptID: attempt.ID, WorkerID: baselineWorker,
			ProviderInstanceID: task.Routes[0].ProviderInstanceID, Model: task.Routes[0].Model,
			Estimate: backlog.TaskAdmissionEstimate{
				RemainingCost: 10, ExpectedRuntime: 20 * time.Minute, CheckpointMargin: 5 * time.Minute,
			},
		})
	}
	input.RouteEstimates = estimates
	return input
}

func supervisedBlockers(
	t *testing.T,
	ctx context.Context,
	store *sqlite.Store,
	records sqlite.CoordinatorRecords,
	runID, taskID string,
	now time.Time,
) []backlog.PlanningBlocker {
	t.Helper()
	plan, err := backlog.BuildPlan(supervisedPlanInput(t, ctx, store, records, now))
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}
	for _, decision := range plan.Decisions {
		if decision.WorkflowRunID == runID && decision.TaskID == taskID {
			return decision.Blockers
		}
	}
	return nil
}

func hasSupervisionCode(blockers []backlog.PlanningBlocker, code domain.SupervisionBlockerCode) bool {
	for _, blocker := range blockers {
		if blocker.SupervisionCode == code {
			return true
		}
	}
	return false
}

// supervisedCommit commits one planning pass and reports which tasks it offered.
func supervisedCommit(
	ctx context.Context,
	t *testing.T,
	coordinator backlog.FleetCoordinator,
	store *sqlite.Store,
	records sqlite.CoordinatorRecords,
	now time.Time,
) map[string]bool {
	t.Helper()
	report, err := coordinator.PlanAndCommit(ctx, supervisedPlanInput(t, ctx, store, records, now))
	if err != nil {
		t.Fatalf("plan and commit: %v", err)
	}
	offered := make(map[string]bool, len(report.Assignments))
	for _, assignment := range report.Assignments {
		attempt, known := attemptByID(records.Attempts, assignment.AttemptID)
		if !known {
			continue
		}
		offered[attempt.TaskID] = true
		// The worker claims what it was offered, which is what moves the attempt
		// past the start boundary and gives it its own session identity.
		baselineClaim(ctx, t, store, assignment, now)
	}
	return offered
}

// supervisedFinalize runs the named tasks of the supervised campaign to success
// with their declared outputs, exactly as the unsupervised baseline does.
func supervisedFinalize(ctx context.Context, t *testing.T, fixture supervisedFixture, names []string, start time.Time) {
	t.Helper()
	records := reload(ctx, t, fixture.store)
	run := supervisedRun(t, records, fixture.supervised)
	tasks := supervisedTasksByName(records, fixture.supervised)
	execution, err := backlog.NewDAGExecution(backlog.DAGState{
		Run: run, Tasks: domain.TasksForRun(run, records.Tasks),
		Attempts: attemptsOfRun(records.Attempts, run.ID),
	})
	if err != nil {
		t.Fatal(err)
	}
	finalizer := supervisedFinalizer(fixture.storage, names[0])
	var produced []domain.Artifact
	for index, name := range names {
		task := tasks[name]
		workspace := baselineWorkspace(t, fixture.root, "supervised-"+name)
		for _, output := range task.Outputs {
			writeBaselineFile(t, workspace, output.Name, "output of the "+name+" task\n")
		}
		attempt := baselineAttemptsByTask(execution.Snapshot().Attempts)[task.ID]
		finalized, err := finalizer.Finalize(ctx, backlog.AttemptFinalization{
			Task: task, Attempt: attempt, WorkspaceDir: workspace, ExplicitSuccess: true,
		})
		if err != nil {
			t.Fatalf("finalize %s: %v", name, err)
		}
		baselineCleanup(t, finalized.StorageDir)
		if !finalized.Completion.VerificationPassed {
			t.Fatalf("%s verification = %#v", name, finalized.Completion)
		}
		produced = append(produced, finalized.Artifacts...)
		if err := execution.CompleteAttempt(attempt.ID, finalized.Completion,
			start.Add(time.Duration(index+1)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	state := execution.Snapshot()
	if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{state.Run}, Attempts: state.Attempts, Artifacts: produced,
	}); err != nil {
		t.Fatalf("persist completion: %v", err)
	}
}

// supervisedCompleteAssignments retires every live assignment, which is what a
// worker reporting its finished turn eventually does.
func supervisedCompleteAssignments(ctx context.Context, t *testing.T, store *sqlite.Store) {
	t.Helper()
	records := reload(ctx, t, store)
	for index := range records.Assignments {
		records.Assignments[index].State = domain.AssignmentCompleted
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: records.Assignments}); err != nil {
		t.Fatal(err)
	}
}

// supervisedFinalizer mints artifact identities under its own prefix. Two waves
// of the same run finalize separately, and an artifact identity is immutable, so
// a shared counter restarting at one would collide with what the first wave
// already stored.
func supervisedFinalizer(storage, prefix string) backlog.AttemptFinalizer {
	finalized := 0
	return backlog.AttemptFinalizer{
		StorageRoot: storage,
		Processes:   baselineProcessRunner{},
		Now:         func() time.Time { return baselineTime.Add(time.Duration(finalized) * time.Second) },
		NewID: func(kind string) string {
			finalized++
			return fmt.Sprintf("%s-supervised-%s-%d", kind, prefix, finalized)
		},
	}
}

func reload(ctx context.Context, t *testing.T, store *sqlite.Store) sqlite.CoordinatorRecords {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return records
}

func supervisedRun(t *testing.T, records sqlite.CoordinatorRecords, runID string) domain.WorkflowRun {
	t.Helper()
	for _, run := range records.WorkflowRuns {
		if run.ID == runID {
			return run
		}
	}
	t.Fatalf("run %q is not in the coordinator records", runID)
	return domain.WorkflowRun{}
}

func supervisedTasksByName(records sqlite.CoordinatorRecords, runID string) map[string]domain.Task {
	var workflowID string
	for _, run := range records.WorkflowRuns {
		if run.ID == runID {
			workflowID = run.WorkflowID
		}
	}
	tasks := make(map[string]domain.Task)
	for _, task := range records.Tasks {
		if task.WorkflowID == workflowID {
			tasks[task.Name] = task
		}
	}
	return tasks
}

func attemptsOfRun(attempts []domain.Attempt, runID string) []domain.Attempt {
	var owned []domain.Attempt
	for _, attempt := range attempts {
		if attempt.WorkflowRunID == runID {
			owned = append(owned, attempt)
		}
	}
	return owned
}

func supervisionSnapshot(ctx context.Context, t *testing.T, store *sqlite.Store, runID string) domain.SupervisionSnapshot {
	t.Helper()
	snapshot, err := store.LoadSupervisionSnapshot(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func supervisionGateByName(t *testing.T, snapshot domain.SupervisionSnapshot, name string) domain.Gate {
	t.Helper()
	for _, gate := range snapshot.Gates {
		if gate.Definition.Name == name {
			return gate
		}
	}
	t.Fatalf("the run declares no gate %q", name)
	return domain.Gate{}
}

func adminState(ctx context.Context, t *testing.T, store *sqlite.Store, runID string) sqlite.SupervisionAdminState {
	t.Helper()
	state, err := store.LoadSupervisionAdminState(ctx, runID)
	if err != nil {
		t.Fatalf("load supervision state of %q: %v", runID, err)
	}
	return state
}

func gateFacts(t *testing.T, state sqlite.SupervisionAdminState, gateID string) sqlite.SupervisionGateFacts {
	t.Helper()
	for _, gate := range state.Gates {
		if gate.Gate.Definition.ID == gateID {
			if gate.Evidence == nil {
				t.Fatalf("gate %q has no bound evidence", gateID)
			}
			return gate
		}
	}
	t.Fatalf("the run has no gate %q", gateID)
	return sqlite.SupervisionGateFacts{}
}
