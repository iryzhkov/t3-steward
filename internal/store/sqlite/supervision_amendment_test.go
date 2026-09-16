package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The supervision half of a graph change: what an amendment recomputes, what it
// refuses, and what a clone or rerun inherits.

func amendmentTask(id, name string, needs ...string) domain.Task {
	return domain.Task{
		ID: id, WorkflowID: "workflow", Name: name, Class: domain.TaskClassRequired,
		MaxTurns: 1, PromptArtifactID: "prompt:" + id, Needs: needs,
		Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt-5.6-sol"}},
	}
}

func amendmentPrompt(runID string, task domain.Task) domain.Artifact {
	return domain.Artifact{
		ID: task.PromptArtifactID, WorkflowRunID: runID, TaskID: task.ID,
		Kind: domain.ArtifactInput, Name: "prompt.md", MediaType: "text/markdown",
		Size: 1, SHA256: "hash-" + task.ID, StoragePath: "prompt-" + task.ID,
	}
}

// amendmentFixture writes a supervised run over the supplied tasks, one ready
// attempt per task, and a healthy worker. Passing no gates still creates the
// supervision record; passing unsupervised leaves the run with none.
func amendmentFixture(t *testing.T, supervised bool, tasks []domain.Task, gates []domain.Gate) (*Store, domain.WorkflowRun) {
	t.Helper()
	ctx := context.Background()
	store := openFleetTestStore(t)
	store.SetClock(func() time.Time { return fleetTestTime })
	run, err := domain.BindRunSink(domain.WorkflowRun{
		ID: "run-1", WorkflowID: "workflow", GraphRevision: 1, Revision: 1,
		Progress: domain.ProgressQueued, CreatedAt: fleetTestTime, UpdatedAt: fleetTestTime,
	}, tasks)
	if err != nil {
		t.Fatal(err)
	}
	records := CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}, Tasks: tasks}
	for index, task := range tasks {
		records.Artifacts = append(records.Artifacts, amendmentPrompt(run.ID, task))
		records.Attempts = append(records.Attempts, domain.Attempt{
			ID: "attempt-" + task.ID, WorkflowRunID: run.ID, TaskID: task.ID, Number: 1,
			Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
			Revision: 1, UpdatedAt: fleetTestTime,
		})
		_ = index
	}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	saveFleetSnapshot(t, store, fleetSnapshot(1, "worker-epoch-1", 1, true, fleetTestTime.Add(time.Hour)))
	if supervised {
		if _, err := store.PutSupervision(ctx, SupervisionMaterialization{
			Record: domain.SupervisionRecord{RunID: run.ID, Config: supervisionTestConfig()},
			Gates:  gates,
		}); err != nil {
			t.Fatal(err)
		}
	}
	return store, run
}

func amendmentAcceptedGate(id string, observed, protected []string) domain.Gate {
	return domain.Gate{
		Definition: domain.GateDefinition{
			ID: id, Name: id, ObservedTaskIDs: observed, ProtectedTaskIDs: protected,
		},
		RunID: "run-1", State: domain.GateAccepted, GraphRevision: 1,
		EvidenceSnapshotID: "snapshot-1", Revision: 1, UpdatedAt: fleetTestTime,
	}
}

// currentRun reads the run exactly as the amendment transaction will, so the
// commit's Before matches whatever earlier operations left behind.
func currentRun(t *testing.T, store *Store, runID string) domain.WorkflowRun {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range records.WorkflowRuns {
		if run.ID == runID {
			return run
		}
	}
	t.Fatalf("run %q disappeared", runID)
	return domain.WorkflowRun{}
}

// amendmentCommit prepares a commit the way the admin service does: the
// candidate is whatever domain.AmendTasks produces, because the store refuses
// anything else.
func amendmentCommit(t *testing.T, store *Store, run domain.WorkflowRun, request domain.GraphAmendment) GraphCommit {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	taskID, promptID := "task:graph:"+request.ID, "input:graph:"+request.ID
	tasks, err := domain.AmendTasks(request, run, records.Tasks, taskID, promptID)
	if err != nil {
		t.Fatal(err)
	}
	commit := GraphCommit{Request: request, Actor: "operator", Before: run, Tasks: tasks, Now: fleetTestTime}
	if request.Operation == "task-add" {
		commit.Inputs = []domain.Artifact{{
			ID: promptID, WorkflowRunID: run.ID, TaskID: taskID, Kind: domain.ArtifactInput,
			Name: "prompt.md", MediaType: "text/markdown", Size: 1,
			SHA256: "hash-" + taskID, StoragePath: "prompt-" + taskID,
		}}
	}
	return commit
}

func amendmentAddTask(id, name string, needs ...string) domain.GraphAmendment {
	return domain.GraphAmendment{
		ID: id, RunID: "run-1", ExpectedRevision: 1, Operation: "task-add",
		Reason: "add a task", Prompt: "do the work",
		Task: &domain.Task{
			Name: name, Class: domain.TaskClassRequired, MaxTurns: 1, Needs: needs,
			Routes:       []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt-5.6-sol"}},
			Verification: []string{"true"},
		},
	}
}

func amendmentSetTimeout(id, taskID string, timeout time.Duration) domain.GraphAmendment {
	return domain.GraphAmendment{
		ID: id, RunID: "run-1", ExpectedRevision: 1, Operation: "task-set",
		TaskID: taskID, Timeout: &timeout, Reason: "retune the task",
	}
}

func amendmentOffer(t *testing.T, store *Store, assignmentID, taskID string) {
	t.Helper()
	commit := fleetPlanCommit(1, assignmentID, "attempt-"+taskID, "worker-epoch-1", 1)
	assignments, err := store.CommitAssignmentPlan(context.Background(), commit)
	if err != nil {
		t.Fatal(err)
	}
	if len(assignments) != 1 {
		t.Fatalf("task %q was not offered: %#v", taskID, assignments)
	}
}

func supervisionAfter(t *testing.T, store *Store, runID string) SupervisionProjection {
	t.Helper()
	projection, err := store.LoadSupervisionProjection(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	return projection
}

func gateByID(t *testing.T, gates []domain.Gate, id string) domain.Gate {
	t.Helper()
	for _, gate := range gates {
		if gate.Definition.ID == id {
			return gate
		}
	}
	t.Fatalf("gate %q is missing from %#v", id, gates)
	return domain.Gate{}
}

func TestAmendmentWidensBranchHoldToNewDescendant(t *testing.T) {
	ctx := context.Background()
	tasks := []domain.Task{
		amendmentTask("task-1", "producer"),
		amendmentTask("task-2", "consumer", "producer"),
	}
	store, run := amendmentFixture(t, true, tasks, nil)
	if _, err := store.PlaceHold(ctx, HoldRequest{
		RunID: run.ID, HoldID: "hold-1", RequestID: "hold-request-1", Actor: supervisionOperator(),
		Scope:  domain.HoldScope{Kind: domain.HoldScopeBranch, BranchRootTaskID: "task-1"},
		Reason: "review the branch", PlacedAt: fleetTestTime,
	}); err != nil {
		t.Fatal(err)
	}
	request := amendmentAddTask("widen", "reviewer", "consumer")
	if _, err := store.CommitGraphAmendment(ctx, amendmentCommit(t, store, currentRun(t, store, run.ID), request)); err != nil {
		t.Fatal(err)
	}
	projection := supervisionAfter(t, store, run.ID)
	if len(projection.Holds) != 1 {
		t.Fatalf("expected the one hold to survive: %#v", projection.Holds)
	}
	hold := projection.Holds[0]
	added := "task:graph:widen"
	if hold.State != domain.HoldActive || hold.GraphRevision != 2 {
		t.Fatalf("hold did not move onto the amended graph: %#v", hold)
	}
	if !hold.Covers(added) {
		t.Fatalf("new descendant %q escaped the branch hold: %#v", added, hold.ResolvedTaskIDs)
	}
	for _, expected := range []string{"task-1", "task-2"} {
		if !hold.Covers(expected) {
			t.Fatalf("hold lost %q: %#v", expected, hold.ResolvedTaskIDs)
		}
	}
}

func TestAmendmentInvalidatesAcceptanceTouchingGateScope(t *testing.T) {
	tasks := []domain.Task{
		amendmentTask("task-1", "producer"),
		amendmentTask("task-2", "consumer", "producer"),
		amendmentTask("task-3", "unrelated"),
	}
	for _, testCase := range []struct {
		name    string
		request domain.GraphAmendment
	}{
		{name: "observed task retuned", request: amendmentSetTimeout("touch1", "task-1", time.Minute)},
		{name: "protected task retuned", request: amendmentSetTimeout("touch2", "task-2", time.Minute)},
		{name: "descendant of a protected task added", request: amendmentAddTask("touch3", "reviewer", "consumer")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			gate := amendmentAcceptedGate("gate-1", []string{"task-1"}, []string{"task-2"})
			store, run := amendmentFixture(t, true, tasks, []domain.Gate{gate})
			commit := amendmentCommit(t, store, currentRun(t, store, run.ID), testCase.request)
			if _, err := store.CommitGraphAmendment(ctx, commit); err != nil {
				t.Fatal(err)
			}
			projection := supervisionAfter(t, store, run.ID)
			amended := gateByID(t, projection.Gates, "gate-1")
			if amended.State != domain.GatePendingEvidence {
				t.Fatalf("acceptance survived a change to the gate's scope: %#v", amended)
			}
			if amended.EvidenceSnapshotID != "" {
				t.Fatalf("invalidated gate still names evidence %q", amended.EvidenceSnapshotID)
			}
			if amended.GraphRevision != 2 || amended.Revision != 2 {
				t.Fatalf("gate did not move onto the amended graph: %#v", amended)
			}
		})
	}
}

func TestAmendmentCarriesAcceptanceForwardWithRevalidationReceipt(t *testing.T) {
	ctx := context.Background()
	tasks := []domain.Task{
		amendmentTask("task-1", "producer"),
		amendmentTask("task-2", "consumer", "producer"),
		amendmentTask("task-3", "unrelated"),
	}
	gate := amendmentAcceptedGate("gate-1", []string{"task-1"}, []string{"task-2"})
	store, run := amendmentFixture(t, true, tasks, []domain.Gate{gate})
	request := amendmentSetTimeout("carry", "task-3", time.Minute)
	if _, err := store.CommitGraphAmendment(ctx, amendmentCommit(t, store, currentRun(t, store, run.ID), request)); err != nil {
		t.Fatal(err)
	}
	amended := gateByID(t, supervisionAfter(t, store, run.ID).Gates, "gate-1")
	if amended.State != domain.GateAccepted || amended.EvidenceSnapshotID != "snapshot-1" {
		t.Fatalf("acceptance outside the gate's scope was not carried forward: %#v", amended)
	}
	if amended.GraphRevision != 2 {
		t.Fatalf("carried acceptance did not move onto the amended graph: %#v", amended)
	}
	var receipts int
	if err := store.db.QueryRow(
		"SELECT COUNT(*) FROM coordinator_audit_events WHERE kind = ? AND json_extract(record,'$.detail.idempotencyIdentity') = ?",
		"supervision-revalidated", "revalidation:carry:gate-1").Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 {
		t.Fatalf("expected exactly one durable revalidation receipt, found %d", receipts)
	}
}

func TestAmendmentRefusesWhenProtectedSuccessorAlreadyOffered(t *testing.T) {
	ctx := context.Background()
	tasks := []domain.Task{
		amendmentTask("task-1", "producer"),
		amendmentTask("task-2", "consumer", "producer"),
	}
	gate := amendmentAcceptedGate("gate-1", []string{"task-1"}, []string{"task-2"})
	store, run := amendmentFixture(t, true, tasks, []domain.Gate{gate})
	amendmentOffer(t, store, "assignment-1", "task-2")
	request := amendmentSetTimeout("refuse", "task-1", time.Minute)
	_, err := store.CommitGraphAmendment(ctx, amendmentCommit(t, store, currentRun(t, store, run.ID), request))
	if !errors.Is(err, ErrSupervisionAmendmentRefused) {
		t.Fatalf("expected a supervision refusal, got %v", err)
	}
	if !strings.Contains(err.Error(), "gate-1") || !strings.Contains(err.Error(), "task-2") {
		t.Fatalf("refusal names neither the gate nor the protected task: %v", err)
	}
	if after := currentRun(t, store, run.ID); after.GraphRevision != 1 {
		t.Fatalf("refused amendment was committed anyway: %#v", after)
	}
	amended := gateByID(t, supervisionAfter(t, store, run.ID).Gates, "gate-1")
	if amended.State != domain.GateAccepted || amended.Revision != 1 {
		t.Fatalf("refused amendment left the gate changed: %#v", amended)
	}
}

func TestAmendmentRevokesNarrowedOffersInSameTransaction(t *testing.T) {
	ctx := context.Background()
	// The offered task is not edited by the amendment: an edited task with
	// assignment history is refused by the ordinary amendment rules, so the edge
	// that pulls the offered leaf under the hold is added to its parent.
	tasks := []domain.Task{
		amendmentTask("task-1", "root"),
		amendmentTask("task-2", "middle"),
		amendmentTask("task-3", "leaf", "middle"),
	}
	store, run := amendmentFixture(t, true, tasks, nil)
	if _, err := store.PlaceHold(ctx, HoldRequest{
		RunID: run.ID, HoldID: "hold-1", RequestID: "hold-request-1", Actor: supervisionOperator(),
		Scope:  domain.HoldScope{Kind: domain.HoldScopeBranch, BranchRootTaskID: "task-1"},
		Reason: "hold the root branch", PlacedAt: fleetTestTime,
	}); err != nil {
		t.Fatal(err)
	}
	amendmentOffer(t, store, "assignment-1", "task-3")
	request := domain.GraphAmendment{
		ID: "narrow", RunID: run.ID, ExpectedRevision: 1, Operation: "edge-add",
		TaskID: "task-2", Source: "root", Reason: "the middle task needs the root",
	}
	if _, err := store.CommitGraphAmendment(ctx, amendmentCommit(t, store, currentRun(t, store, run.ID), request)); err != nil {
		t.Fatal(err)
	}
	projection := supervisionAfter(t, store, run.ID)
	if len(projection.Holds) != 1 || !projection.Holds[0].Covers("task-3") {
		t.Fatalf("hold did not widen onto the offered leaf: %#v", projection.Holds)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Assignments) != 1 || records.Assignments[0].State != domain.AssignmentReleased {
		t.Fatalf("the narrowed offer was not released: %#v", records.Assignments)
	}
	for _, attempt := range records.Attempts {
		if attempt.TaskID == "task-3" && attempt.AssignmentID != "" {
			t.Fatalf("released attempt is still attached to its assignment: %#v", attempt)
		}
	}
}

func TestAmendmentBumpsSupervisionRevision(t *testing.T) {
	ctx := context.Background()
	tasks := []domain.Task{
		amendmentTask("task-1", "producer"),
		amendmentTask("task-2", "consumer", "producer"),
	}
	store, run := amendmentFixture(t, true, tasks, nil)
	before := supervisionAfter(t, store, run.ID)
	if before.Record == nil {
		t.Fatal("the fixture is not supervised")
	}
	request := amendmentSetTimeout("fence", "task-1", time.Minute)
	if _, err := store.CommitGraphAmendment(ctx, amendmentCommit(t, store, currentRun(t, store, run.ID), request)); err != nil {
		t.Fatal(err)
	}
	after := supervisionAfter(t, store, run.ID)
	if after.Record == nil || after.Record.Revision != before.Record.Revision+1 {
		t.Fatalf("the amendment did not fence decisions formed against the old state: %#v", after.Record)
	}
}

func TestUnsupervisedAmendmentIsUnchanged(t *testing.T) {
	ctx := context.Background()
	tasks := []domain.Task{
		amendmentTask("task-1", "producer"),
		amendmentTask("task-2", "consumer", "producer"),
	}
	store, run := amendmentFixture(t, false, tasks, nil)
	request := amendmentSetTimeout("plain", "task-1", time.Minute)
	result, err := store.CommitGraphAmendment(ctx, amendmentCommit(t, store, currentRun(t, store, run.ID), request))
	if err != nil {
		t.Fatal(err)
	}
	if result.Run.GraphRevision != 2 {
		t.Fatalf("unsupervised amendment did not advance the graph: %#v", result.Run)
	}
	projection := supervisionAfter(t, store, run.ID)
	if projection.Record != nil || len(projection.Gates) != 0 || len(projection.Holds) != 0 {
		t.Fatalf("an unsupervised run grew supervision state: %#v", projection)
	}
	var rows int
	if err := store.db.QueryRow("SELECT COUNT(*) FROM coordinator_supervision").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("an empty supervision record was created for an unsupervised run: %d rows", rows)
	}
}

// clonedTasks rebuilds the source tasks under a new run, the way the admin
// service does, and returns the candidate plus its old-to-new identity map.
func clonedTasks(prefix, runID string, source []domain.Task) ([]domain.Task, []domain.Artifact, map[string]string) {
	remap := make(map[string]string, len(source))
	for index, task := range source {
		remap[task.ID] = prefix + ":task:" + string(rune('a'+index))
	}
	var tasks []domain.Task
	var inputs []domain.Artifact
	for _, task := range source {
		next := task
		next.ID = remap[task.ID]
		next.RunID = runID
		next.DefinitionRevision = 1
		next.PromptArtifactID = prefix + ":input:" + next.ID
		tasks = append(tasks, next)
		inputs = append(inputs, domain.Artifact{
			ID: next.PromptArtifactID, WorkflowRunID: runID, TaskID: next.ID,
			Kind: domain.ArtifactInput, Name: "prompt.md", MediaType: "text/markdown",
			Size: 1, SHA256: "hash-" + task.ID, StoragePath: "prompt-" + task.ID,
		})
	}
	return tasks, inputs, remap
}

func TestCloneInheritsSupervisionConfigurationWithoutAcceptances(t *testing.T) {
	ctx := context.Background()
	sourceTasks := []domain.Task{
		amendmentTask("task-1", "producer"),
		amendmentTask("task-2", "consumer", "producer"),
	}
	gate := amendmentAcceptedGate("gate-1", []string{"task-1"}, []string{"task-2"})
	store, run := amendmentFixture(t, true, sourceTasks, []domain.Gate{gate})
	if _, err := store.PlaceHold(ctx, HoldRequest{
		RunID: run.ID, HoldID: "hold-1", RequestID: "hold-request-1", Actor: supervisionOperator(),
		Scope:  domain.HoldScope{Kind: domain.HoldScopeRun},
		Reason: "hold the source run", PlacedAt: fleetTestTime,
	}); err != nil {
		t.Fatal(err)
	}
	cloneRunID := "run:clone:cloned"
	tasks, inputs, remap := clonedTasks("clone", cloneRunID, sourceTasks)
	result, err := store.CommitGraphClone(ctx, GraphCommit{
		Request: domain.GraphAmendment{
			ID: "cloned", RunID: run.ID, ExpectedRevision: 1, Operation: "clone", Reason: "clone the run",
		},
		Actor: "operator", Before: currentRun(t, store, run.ID), Tasks: tasks, Inputs: inputs,
		TaskIDRemap: remap, Now: fleetTestTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Run.Supervision == nil || result.Run.Supervision.Config.PromptArtifactID != supervisionTestConfig().PromptArtifactID {
		t.Fatalf("the clone did not inherit the supervision configuration: %#v", result.Run.Supervision)
	}
	projection := supervisionAfter(t, store, result.Run.ID)
	if projection.Record == nil {
		t.Fatal("the clone has no supervision record")
	}
	if projection.Record.ActivationEpoch != 1 || projection.Record.Revision != 1 ||
		projection.Record.EventCursor != 0 || projection.Record.ActivationsUsed != 0 {
		t.Fatalf("the clone did not start from a fresh activation epoch: %#v", projection.Record)
	}
	if len(projection.Holds) != 0 || len(projection.Incidents) != 0 || len(projection.Decisions) != 0 {
		t.Fatalf("the clone inherited decision-owned state: %#v", projection)
	}
	if len(projection.Gates) != 1 {
		t.Fatalf("expected the one gate definition to be inherited: %#v", projection.Gates)
	}
	inherited := projection.Gates[0]
	if inherited.State != domain.GatePendingEvidence || inherited.EvidenceSnapshotID != "" {
		t.Fatalf("the clone inherited an acceptance: %#v", inherited)
	}
	if inherited.Revision != 1 || inherited.GraphRevision != result.Run.GraphRevision {
		t.Fatalf("inherited gate is not at the clone's own revisions: %#v", inherited)
	}
	if inherited.Definition.ObservedTaskIDs[0] != remap["task-1"] ||
		inherited.Definition.ProtectedTaskIDs[0] != remap["task-2"] {
		t.Fatalf("the gate was not remapped onto the clone's tasks: %#v", inherited.Definition)
	}
	// The source keeps everything it had.
	source := supervisionAfter(t, store, run.ID)
	if len(source.Holds) != 1 || gateByID(t, source.Gates, "gate-1").State != domain.GateAccepted {
		t.Fatalf("cloning changed the source run's supervision: %#v", source)
	}
}

func TestRerunInheritsSupervisionConfigurationWithoutHoldsOrIncidents(t *testing.T) {
	ctx := context.Background()
	sourceTasks := []domain.Task{
		amendmentTask("task-1", "producer"),
		amendmentTask("task-2", "consumer", "producer"),
	}
	gate := amendmentAcceptedGate("gate-1", []string{"task-1"}, []string{"task-2"})
	store, run := amendmentFixture(t, true, sourceTasks, []domain.Gate{gate})
	terminal := currentRun(t, store, run.ID)
	terminal.Progress = domain.ProgressFailed
	terminal.Revision++
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{terminal}}); err != nil {
		t.Fatal(err)
	}
	seedSupervisionHoldRow(t, store, run.ID, "hold-1")
	seedSupervisionIncidentRow(t, store, run.ID, "incident-1")
	rerunRunID := "run:rerun:again"
	tasks, inputs, remap := clonedTasks("rerun", rerunRunID, sourceTasks)
	result, err := store.CommitGraphRerun(ctx, GraphCommit{
		Request: domain.GraphAmendment{
			ID: "again", RunID: run.ID, ExpectedRevision: 1, Operation: "rerun",
			TaskID: "task-1", Reason: "run it again",
		},
		Actor: "operator", Before: currentRun(t, store, run.ID), Tasks: tasks, Inputs: inputs,
		TaskIDRemap: remap,
		Rerun: &domain.RerunProvenance{
			SourceRunID: run.ID, SourceTaskID: "task-1", SourceAttemptID: "attempt-task-1",
			IdempotencyKey: "again", Reason: "run it again",
		},
		Now: fleetTestTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Run.Supervision == nil || result.Run.Supervision.ActivationEpoch != 1 {
		t.Fatalf("the rerun did not inherit the supervision configuration: %#v", result.Run.Supervision)
	}
	projection := supervisionAfter(t, store, result.Run.ID)
	if len(projection.Holds) != 0 || len(projection.Incidents) != 0 {
		t.Fatalf("the rerun inherited holds or incidents: %#v", projection)
	}
	if len(projection.Gates) != 1 {
		t.Fatalf("expected the one gate definition to be inherited: %#v", projection.Gates)
	}
	inherited := projection.Gates[0]
	if inherited.State != domain.GatePendingEvidence || inherited.EvidenceSnapshotID != "" || inherited.Revision != 1 {
		t.Fatalf("the rerun inherited an acceptance: %#v", inherited)
	}
	if inherited.Definition.ObservedTaskIDs[0] != remap["task-1"] {
		t.Fatalf("the gate was not remapped onto the rerun's tasks: %#v", inherited.Definition)
	}
	source := supervisionAfter(t, store, run.ID)
	if len(source.Holds) != 1 || len(source.Incidents) != 1 {
		t.Fatalf("the rerun changed the source run's supervision: %#v", source)
	}
}

// seedSupervisionHoldRow writes an active hold directly, because the source run
// of a rerun has already settled and no decision may be made on it any more.
func seedSupervisionHoldRow(t *testing.T, store *Store, runID, holdID string) {
	t.Helper()
	hold := domain.Hold{
		ID: holdID, RunID: runID, Scope: domain.HoldScope{Kind: domain.HoldScopeRun},
		Owner: supervisionOperator(), GraphRevision: 1, State: domain.HoldActive,
		Reason: "seeded", CreatedAt: fleetTestTime,
	}
	raw, err := json.Marshal(hold)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(
		"INSERT INTO coordinator_supervision_holds(id,run_id,owner,scope_kind,state,record) VALUES(?,?,?,?,?,?)",
		hold.ID, hold.RunID, "operator:operator-1", string(hold.Scope.Kind), string(hold.State), raw); err != nil {
		t.Fatal(err)
	}
}

func seedSupervisionIncidentRow(t *testing.T, store *Store, runID, incidentID string) {
	t.Helper()
	incident := domain.ReviewIncident{
		ID: incidentID, RunID: runID, State: domain.IncidentOpen, Revision: 1, GateID: "gate-1",
	}
	raw, err := json.Marshal(incident)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(
		"INSERT INTO coordinator_supervision_incidents(id,run_id,state,revision,gate_id,record) VALUES(?,?,?,?,?,?)",
		incident.ID, incident.RunID, string(incident.State), incident.Revision, incident.GateID, raw); err != nil {
		t.Fatal(err)
	}
}
