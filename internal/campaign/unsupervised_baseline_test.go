package campaign

// The unsupervised baseline: a three-node campaign directory submitted through
// the campaign path becomes one workflow, one run and three tasks, each of
// which the coordinator schedules as its own T3 session with its own dispatch
// identity. Dependencies release the join only after both producers succeed,
// the producers' artifacts reach the join through inputs_from, and nothing
// supervisory exists anywhere in the run: there is no fourth task, no fourth
// attempt, no fourth assignment and no fourth thread.
//
// This is the behaviour the optional campaign overseer
// (docs/plans/campaign-supervision.md, verification gate 2) must leave exactly
// as it is when a campaign declares no supervision. The assertions on absence
// are the point: they fail the moment any supervisory record starts being
// created unconditionally.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

const (
	baselineWorker      = "worker-a"
	baselineWorkerEpoch = "worker-epoch-1"
	baselineInstance    = "claudeAgent"
	baselineModel       = "claude-opus-5"
	baselinePool        = "claude-main"
)

var baselineTime = time.Date(2026, time.September, 16, 9, 0, 0, 0, time.UTC)

func TestThreeNodeCampaignRunsAsThreeSessionsWithoutSupervision(t *testing.T) {
	ctx := context.Background()
	fixture := submitBaselineCampaign(t)
	store := fixture.store

	// One workflow and one run, whatever the graph contains. The campaign
	// namespace is a facade over exactly one version-2 submission.
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Workflows) != 1 || len(records.WorkflowRuns) != 1 {
		t.Fatalf("submission created %d workflows and %d runs, want one of each",
			len(records.Workflows), len(records.WorkflowRuns))
	}
	run := records.WorkflowRuns[0]
	if run.Sink == nil {
		t.Fatal("the run has no terminal sink")
	}
	if names := taskNames(records.Tasks); !reflect.DeepEqual(names, []string{"interfaces", "join", "tests"}) {
		t.Fatalf("task names = %v, want the three declared tasks and nothing else", names)
	}
	if len(records.Attempts) != 3 {
		t.Fatalf("attempts = %d, want one per declared task", len(records.Attempts))
	}
	tasks := baselineTasksByName(records.Tasks)
	attempts := baselineAttemptsByTask(records.Attempts)
	join := tasks["join"]
	if !reflect.DeepEqual(join.Needs, []string{"interfaces", "tests"}) ||
		len(join.DependencyInputs) != 2 {
		t.Fatalf("join dependencies = %v inputs = %v", join.Needs, join.DependencyInputs)
	}
	if attempts[join.ID].Progress != domain.ProgressBlocked {
		t.Fatalf("join attempt starts %q, want blocked behind its producers", attempts[join.ID].Progress)
	}

	// First wave: the two producers are planned, each as its own assignment
	// with its own T3 thread and dispatch token. The join is not planned at
	// all, because a blocked attempt is not a candidate.
	coordinator := backlog.FleetCoordinator{Store: store, Now: func() time.Time { return baselineTime }}
	first, err := coordinator.PlanAndCommit(ctx, baselinePlanInput(records, baselineTime))
	if err != nil {
		t.Fatalf("plan the first wave: %v", err)
	}
	if len(first.Assignments) != 2 {
		t.Fatalf("first wave assigned %d attempts, want the two producers", len(first.Assignments))
	}
	for _, assignment := range first.Assignments {
		if assignment.AttemptID == attempts[join.ID].ID {
			t.Fatal("the join was scheduled before its producers succeeded")
		}
	}

	// Each assignment is claimed and dispatched on its own. The worker is sent
	// one dispatch command per task, never one session doing the others' work.
	// Delivery is exercised for this wave; the join's own session is asserted
	// below from its durable assignment, because a second delivery round would
	// reconcile these two finished attempts against a worker that is a fake.
	transport := &baselineTransport{}
	claimed := make(map[string]domain.Assignment, 3)
	for _, assignment := range first.Assignments {
		claimed[assignment.AttemptID] = baselineClaim(ctx, t, store, assignment, baselineTime)
	}
	baselineDeliver(ctx, t, coordinator, transport)
	if dispatched := transport.dispatched(); len(dispatched) != 2 {
		t.Fatalf("first wave dispatched %v, want one command per producer", dispatched)
	}

	// The producers run and succeed. Each writes its own declared output in
	// its own workspace; neither can see the other's.
	records, err = store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	execution, err := backlog.NewDAGExecution(backlog.DAGState{
		Run: records.WorkflowRuns[0], Tasks: records.Tasks, Attempts: records.Attempts,
	})
	if err != nil {
		t.Fatal(err)
	}
	finalizer := baselineFinalizer(fixture.storage)
	var producerArtifacts []domain.Artifact
	for index, name := range []string{"interfaces", "tests"} {
		task := tasks[name]
		workspace := baselineWorkspace(t, fixture.root, name)
		writeBaselineFile(t, workspace, name+".md", "findings from the "+name+" task\n")
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
		producerArtifacts = append(producerArtifacts, finalized.Artifacts...)
		if err := execution.CompleteAttempt(attempt.ID, finalized.Completion,
			baselineTime.Add(time.Duration(index+1)*time.Minute)); err != nil {
			t.Fatal(err)
		}
		// Dependency ordering: one producer is not enough. The join stays
		// blocked until the second one has succeeded too.
		state := execution.Snapshot()
		joinProgress := baselineAttemptsByTask(state.Attempts)[join.ID].Progress
		if index == 0 && joinProgress != domain.ProgressBlocked {
			t.Fatalf("join became %q after one producer, want blocked", joinProgress)
		}
		if index == 1 && joinProgress != domain.ProgressReady {
			t.Fatalf("join is %q after both producers succeeded, want ready", joinProgress)
		}
	}
	state := execution.Snapshot()
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{state.Run}, Attempts: state.Attempts, Artifacts: producerArtifacts,
	}); err != nil {
		t.Fatalf("persist producer completion: %v", err)
	}

	// Second wave: the join is now a candidate, and it is scheduled as a third
	// session with an identity of its own.
	records, err = store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := coordinator.PlanAndCommit(ctx, baselinePlanInput(records, baselineTime.Add(3*time.Minute)))
	if err != nil {
		t.Fatalf("plan the second wave: %v", err)
	}
	if len(second.Assignments) != 1 || second.Assignments[0].AttemptID != attempts[join.ID].ID {
		t.Fatalf("second wave assignments = %#v, want the join only", second.Assignments)
	}
	claimed[second.Assignments[0].AttemptID] = baselineClaim(
		ctx, t, store, second.Assignments[0], baselineTime.Add(3*time.Minute))

	// Three distinct sessions: three assignments, three thread IDs, three
	// dispatch tokens, one attempt each, no sharing anywhere.
	threads := map[string]string{}
	tokens := map[string]string{}
	for attemptID, assignment := range claimed {
		if previous, repeated := threads[assignment.ThreadID]; repeated {
			t.Fatalf("attempts %q and %q share T3 thread %q", previous, attemptID, assignment.ThreadID)
		}
		threads[assignment.ThreadID] = attemptID
		if previous, repeated := tokens[assignment.DispatchToken]; repeated {
			t.Fatalf("attempts %q and %q share dispatch token %q", previous, attemptID, assignment.DispatchToken)
		}
		tokens[assignment.DispatchToken] = attemptID
	}
	if len(threads) != 3 || len(tokens) != 3 {
		t.Fatalf("distinct threads = %d, distinct dispatch tokens = %d, want three of each",
			len(threads), len(tokens))
	}

	// Artifacts cross the dependency edges declared with inputs_from, and
	// arrive under the producing task's name.
	joinWorkspace := baselineWorkspace(t, fixture.root, "join")
	materialized, err := backlog.MaterializeDependencies(
		joinWorkspace, fixture.storage, fixture.runID, join, records.Tasks, producerArtifacts,
	)
	if err != nil {
		t.Fatalf("materialize the join's declared inputs: %v", err)
	}
	baselineCleanup(t, filepath.Join(joinWorkspace, ".t3", "dependencies"))
	want := []string{
		".t3/dependencies/interfaces/interfaces.md",
		".t3/dependencies/tests/tests.md",
	}
	if !reflect.DeepEqual(materialized, want) {
		t.Fatalf("materialized = %#v, want %#v", materialized, want)
	}
	for _, relative := range want {
		content, err := os.ReadFile(filepath.Join(joinWorkspace, filepath.FromSlash(relative)))
		if err != nil {
			t.Fatalf("read %s: %v", relative, err)
		}
		if len(content) == 0 {
			t.Fatalf("%s arrived empty", relative)
		}
	}

	// The join runs, the run settles, and the sink is the only node the
	// coordinator added to the three the campaign declared.
	running, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	execution, err = backlog.NewDAGExecution(backlog.DAGState{
		Run: running.WorkflowRuns[0], Tasks: running.Tasks, Attempts: running.Attempts,
	})
	if err != nil {
		t.Fatal(err)
	}
	joinAttempt := baselineAttemptsByTask(execution.Snapshot().Attempts)[join.ID]
	writeBaselineFile(t, joinWorkspace, "combined.md", "agreed, disagreed, exposed by the join\n")
	finalizedJoin, err := finalizer.Finalize(ctx, backlog.AttemptFinalization{
		Task: join, Attempt: joinAttempt, WorkspaceDir: joinWorkspace, ExplicitSuccess: true,
	})
	if err != nil {
		t.Fatalf("finalize join: %v", err)
	}
	baselineCleanup(t, finalizedJoin.StorageDir)
	if !finalizedJoin.Completion.VerificationPassed {
		t.Fatalf("join verification = %#v", finalizedJoin.Completion)
	}
	if err := execution.CompleteAttempt(joinAttempt.ID, finalizedJoin.Completion, baselineTime.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	state = execution.Snapshot()
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{state.Run}, Attempts: state.Attempts,
		Artifacts: finalizedJoin.Artifacts,
	}); err != nil {
		t.Fatalf("persist join completion: %v", err)
	}
	settled, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for index := range settled.Assignments {
		settled.Assignments[index].State = domain.AssignmentCompleted
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: settled.Assignments}); err != nil {
		t.Fatal(err)
	}
	if _, err := backlog.ProjectWorkflowRuns(ctx, store, baselineTime.Add(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	final, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if final.WorkflowRuns[0].Progress != domain.ProgressSucceeded ||
		final.WorkflowRuns[0].Sink.Progress != domain.ProgressSucceeded {
		t.Fatalf("settled run = %#v", final.WorkflowRuns[0])
	}

	// Nothing supervisory was created. There is no overseer task, attempt,
	// assignment or T3 session, because an unsupervised campaign has none and
	// this is the baseline that must stay true when supervision is added.
	if len(final.Workflows) != 1 || len(final.WorkflowRuns) != 1 {
		t.Fatalf("run count changed during execution: %d workflows, %d runs",
			len(final.Workflows), len(final.WorkflowRuns))
	}
	if names := taskNames(final.Tasks); !reflect.DeepEqual(names, []string{"interfaces", "join", "tests"}) {
		t.Fatalf("tasks after settlement = %v, want only the three declared tasks", names)
	}
	if len(final.Attempts) != 3 || len(final.Assignments) != 3 {
		t.Fatalf("attempts = %d, assignments = %d, want three of each and nothing supervisory",
			len(final.Attempts), len(final.Assignments))
	}
	declared := map[string]bool{}
	for _, task := range final.Tasks {
		declared[task.ID] = true
	}
	finalAttempts := baselineAttemptsByTask(final.Attempts)
	for _, assignment := range final.Assignments {
		attempt, known := attemptByID(final.Attempts, assignment.AttemptID)
		if !known || !declared[attempt.TaskID] {
			t.Fatalf("assignment %q belongs to no declared task", assignment.ID)
		}
		if _, tracked := threads[assignment.ThreadID]; !tracked {
			t.Fatalf("T3 thread %q was created for something other than a declared task", assignment.ThreadID)
		}
	}
	if len(finalAttempts) != 3 {
		t.Fatalf("attempts cover %d tasks, want three", len(finalAttempts))
	}
	for _, assignmentID := range transport.dispatched() {
		if _, known := claimed[assignmentByID(t, final.Assignments, assignmentID).AttemptID]; !known {
			t.Fatalf("a dispatch was delivered for assignment %q, which no declared task owns", assignmentID)
		}
	}
}

type baselineFixture struct {
	store   *sqlite.Store
	storage string
	root    string
	runID   string
}

// submitBaselineCampaign submits the checked-in three-node example through the
// campaign path: the real loader, the real deterministic packer and the real
// ingestion service, exactly as "t3-steward campaign submit" does.
func submitBaselineCampaign(t *testing.T) baselineFixture {
	t.Helper()
	bundle, err := Prepare(exampleRoot("three-node"), DefaultLimits)
	if err != nil {
		t.Fatalf("prepare the three-node example: %v", err)
	}
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
	accepted, err := service.SubmitArchive(context.Background(), backlog.ArchiveSubmission{
		IdempotencyKey: "unsupervised-baseline", Archive: bytes.NewReader(bundle.Archive),
	})
	if err != nil {
		t.Fatalf("submit the three-node campaign: %v", err)
	}
	if err := store.SaveWorkerSnapshot(context.Background(), baselineSnapshot()); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{
		QuotaPools: []domain.QuotaPool{baselineQuotaPool()},
	}); err != nil {
		t.Fatal(err)
	}
	return baselineFixture{store: store, storage: storage, root: root, runID: accepted.Record.RunID}
}

func baselineSnapshot() domain.WorkerSnapshot {
	return domain.WorkerSnapshot{
		WorkerID: baselineWorker, WorkerEpoch: baselineWorkerEpoch,
		CoordinatorEpoch: 1, Sequence: 1, Connected: true,
		Inventory: domain.WorkerInventory{
			ID: baselineWorker, AcceptBacklog: true, Health: domain.WorkerHealthReady,
			CPUClass: domain.CPUClassHigh,
			Projects: []domain.WorkerProjectInventory{{
				Name: "example-project", Available: true, UpdatedAt: baselineTime,
			}},
			Providers: []domain.WorkerProviderInventory{{
				InstanceID: baselineInstance, Models: []string{baselineModel},
				QuotaPoolID: baselinePool, Available: true,
			}},
			ObservedAt: baselineTime,
		},
		ObservedAt: baselineTime,
		ValidUntil: baselineTime.Add(time.Hour),
	}
}

func baselineQuotaPool() domain.QuotaPool {
	return domain.QuotaPool{
		ID: baselinePool, ProviderInstanceIDs: []string{baselineInstance}, MaxConcurrent: 4,
	}
}

// baselinePlanInput projects the ingested records into planner input. Every
// schedulable attempt is offered the campaign's own declared route, so the
// plan the coordinator commits is the campaign's graph and nothing else.
func baselinePlanInput(records sqlite.CoordinatorRecords, now time.Time) backlog.PlanInput {
	ordering := make(map[string]backlog.PlanningAttemptOrdering, len(records.Attempts))
	estimates := make([]backlog.RouteEstimate, 0, len(records.Attempts))
	for _, attempt := range records.Attempts {
		ordering[attempt.ID] = backlog.PlanningAttemptOrdering{ReadySince: now}
		estimates = append(estimates, backlog.RouteEstimate{
			AttemptID: attempt.ID, WorkerID: baselineWorker,
			ProviderInstanceID: baselineInstance, Model: baselineModel,
			Estimate: backlog.TaskAdmissionEstimate{
				RemainingCost: 10, ExpectedRuntime: 20 * time.Minute, CheckpointMargin: 5 * time.Minute,
			},
		})
	}
	return backlog.PlanInput{
		Now:                  now,
		MaxWorkerSnapshotAge: time.Hour,
		Workflows: []backlog.PlanningWorkflow{{
			Workflow: records.Workflows[0],
			State: backlog.DAGState{
				Run: records.WorkflowRuns[0], Tasks: records.Tasks, Attempts: records.Attempts,
			},
		}},
		ResourceOwners:         map[string]string{},
		WorkflowCheckoutOwners: map[string]string{},
		QuotaPools:             []domain.QuotaPool{baselineQuotaPool()},
		RouteEstimates:         estimates,
		Ordering: backlog.PlanningOrderingInput{
			DeadlineRiskWindow: time.Hour,
			Attempts:           ordering,
		},
	}
}

// baselineClaim activates one offered assignment as its worker would, and
// returns the claimed assignment with its durable execution identity.
func baselineClaim(
	ctx context.Context,
	t *testing.T,
	store *sqlite.Store,
	offered domain.Assignment,
	now time.Time,
) domain.Assignment {
	t.Helper()
	claimed, err := store.ClaimAssignment(ctx, domain.AssignmentClaimRequest{
		CoordinatorEpoch: 1,
		WorkerID:         baselineWorker,
		WorkerEpoch:      baselineWorkerEpoch,
		AssignmentID:     offered.ID,
		AssignmentEpoch:  offered.Epoch,
		LeaseToken:       offered.LeaseToken,
		ClaimedAt:        now.Add(time.Second),
		LeaseExpiresAt:   now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("claim assignment %q: %v", offered.ID, err)
	}
	if claimed.ThreadID == "" || claimed.DispatchToken == "" {
		t.Fatalf("assignment %q has no execution identity: %#v", offered.ID, claimed)
	}
	return claimed
}

// baselineDeliver runs the coordinator's worker-command reconciliation until it
// has nothing left to send, which is how an assignment reaches its worker and
// becomes a T3 session.
func baselineDeliver(ctx context.Context, t *testing.T, coordinator backlog.FleetCoordinator, transport *baselineTransport) {
	t.Helper()
	for range 8 {
		report, err := coordinator.ReconcileWorkerCommands(ctx, baselineSnapshot(), transport)
		if err != nil {
			t.Fatalf("reconcile worker commands: %v", err)
		}
		if len(report.Planned) == 0 && len(report.Pending) == 0 {
			return
		}
	}
	t.Fatal("worker command reconciliation did not settle")
}

// baselineTransport is the worker seam: it accepts every command once and
// records it. A second dispatch for one assignment, or a dispatch for an
// assignment no declared task owns, is visible here.
type baselineTransport struct {
	delivered map[string]domain.WorkerCommand
}

func (transport *baselineTransport) DeliverWorkerCommands(
	_ context.Context,
	snapshot domain.WorkerSnapshot,
	commands []domain.WorkerCommand,
) ([]domain.WorkerAcknowledgement, error) {
	if transport.delivered == nil {
		transport.delivered = map[string]domain.WorkerCommand{}
	}
	acknowledgements := make([]domain.WorkerAcknowledgement, 0, len(commands))
	for _, command := range commands {
		transport.delivered[command.ID] = command
		acknowledgements = append(acknowledgements, domain.WorkerAcknowledgement{
			CommandID: command.ID, WorkerID: command.WorkerID,
			WorkerEpoch: command.WorkerEpoch, CoordinatorEpoch: command.CoordinatorEpoch,
			AssignmentID: command.AssignmentID, AssignmentEpoch: command.AssignmentEpoch,
			WorkerSequence: snapshot.Sequence, Accepted: true,
			AcknowledgedAt: baselineTime.Add(10 * time.Minute),
		})
	}
	return acknowledgements, nil
}

// dispatched returns the assignment IDs a dispatch command was delivered for,
// one entry per assignment.
func (transport *baselineTransport) dispatched() []string {
	assignments := map[string]struct{}{}
	for _, command := range transport.delivered {
		if command.Kind == domain.WorkerCommandDispatch {
			assignments[command.AssignmentID] = struct{}{}
		}
	}
	dispatched := make([]string, 0, len(assignments))
	for assignment := range assignments {
		dispatched = append(dispatched, assignment)
	}
	sort.Strings(dispatched)
	return dispatched
}

func assignmentByID(t *testing.T, assignments []domain.Assignment, id string) domain.Assignment {
	t.Helper()
	for _, assignment := range assignments {
		if assignment.ID == id {
			return assignment
		}
	}
	t.Fatalf("assignment %q is not a durable record of this run", id)
	return domain.Assignment{}
}

// baselineProcessRunner runs a task's declared verification commands for real,
// so that "succeeded" here means what it means in production.
type baselineProcessRunner struct{}

func (baselineProcessRunner) Run(ctx context.Context, request backlog.ProcessRequest) (backlog.ProcessResult, error) {
	command := exec.CommandContext(ctx, request.Program, request.Args...)
	command.Dir = request.Dir
	output, err := command.CombinedOutput()
	result := backlog.ProcessResult{Output: string(output)}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		result.ExitCode = exitError.ExitCode()
		return result, &backlog.ProcessExitError{ExitCode: result.ExitCode, Err: err}
	}
	return result, err
}

func baselineFinalizer(storage string) backlog.AttemptFinalizer {
	finalized := 0
	return backlog.AttemptFinalizer{
		StorageRoot: storage,
		Processes:   baselineProcessRunner{},
		Now:         func() time.Time { return baselineTime.Add(time.Duration(finalized) * time.Second) },
		NewID: func(kind string) string {
			finalized++
			return fmt.Sprintf("%s-baseline-%d", kind, finalized)
		},
	}
}

// baselineWorkspace is one task's disposable workspace. The example's producer
// tasks verify a clean working tree, so the workspace is a repository with a
// commit in it, exactly as a real task's checkout would be.
func baselineWorkspace(t *testing.T, root, name string) string {
	t.Helper()
	workspace := filepath.Join(root, "workspace-"+name)
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = workspace
		command.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=baseline", "GIT_AUTHOR_EMAIL=baseline@example.test",
			"GIT_COMMITTER_NAME=baseline", "GIT_COMMITTER_EMAIL=baseline@example.test",
		)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
	}
	git("init", "--quiet", "--initial-branch=main")
	writeBaselineFile(t, workspace, "subject.txt", "the subject under review\n")
	git("add", "subject.txt")
	git("commit", "--quiet", "-m", "subject")
	return workspace
}

func writeBaselineFile(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// baselineCleanup makes a stored tree writable again. Ingestion and artifact
// capture publish immutable directories, which the test framework cannot
// remove on its own.
func baselineCleanup(t *testing.T, root string) {
	t.Helper()
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err == nil && entry.IsDir() {
			_ = os.Chmod(path, 0o700)
		}
		return nil
	})
}

func taskNames(tasks []domain.Task) []string {
	names := make([]string, 0, len(tasks))
	for _, task := range tasks {
		names = append(names, task.Name)
	}
	sort.Strings(names)
	return names
}

func baselineTasksByName(tasks []domain.Task) map[string]domain.Task {
	byName := make(map[string]domain.Task, len(tasks))
	for _, task := range tasks {
		byName[task.Name] = task
	}
	return byName
}

func baselineAttemptsByTask(attempts []domain.Attempt) map[string]domain.Attempt {
	byTask := make(map[string]domain.Attempt, len(attempts))
	for _, attempt := range attempts {
		if current, seen := byTask[attempt.TaskID]; !seen || attempt.Number > current.Number {
			byTask[attempt.TaskID] = attempt
		}
	}
	return byTask
}

func attemptByID(attempts []domain.Attempt, id string) (domain.Attempt, bool) {
	for _, attempt := range attempts {
		if attempt.ID == id {
			return attempt, true
		}
	}
	return domain.Attempt{}, false
}
