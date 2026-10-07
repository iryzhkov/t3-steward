package backlog

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

var (
	reviewGateBase  = strings.Repeat("c", 40)
	reviewGateHeadA = strings.Repeat("a", 40)
	reviewGateHeadB = strings.Repeat("b", 40)
)

const reviewGateArchive = `{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`

// reviewGateFixture is a review-declared task whose attempt is live, so review
// rounds can be opened through the store exactly as the checkpoint operation
// opens them, and is then finished for collection.
type reviewGateFixture struct {
	path       string
	store      *sqlite.Store
	task       domain.Task
	attempt    domain.Attempt
	assignment domain.Assignment
	authority  review.FrozenAuthority
}

func newReviewGateFixture(t *testing.T, declared bool, commit bool) *reviewGateFixture {
	t.Helper()
	ctx := context.Background()
	now := coordinatorTestTime
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	task := testTask("task")
	if declared {
		task.ReviewRequirements = &domain.TaskReviewRequirements{Version: 1, Risk: "routine", RequiredReviewers: 2, MinProviderFamilies: 2, RoundLimit: 3}
	}
	if commit {
		task.Outputs = []domain.ArtifactDeclaration{{Name: "change", Commit: &domain.CommitOutput{}}}
	}
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 3, AssignmentID: "assignment-1", ThreadID: "thread-1", UpdatedAt: now}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentClaimed, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", Route: domain.ProviderRoute{ProviderInstanceID: "codex", Model: "sol"}, CreatedAt: now, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: task.WorkflowID, Project: "repo", Environment: domain.ExecutionEnvironment{Type: "repository", Scope: "repo"}, CreatedAt: now}},
		WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}},
		Tasks:        []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	requirements, err := review.NewRequirements(review.RequirementsSpec{Risk: "routine", CriteriaDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), RequiredReviewers: 2, MinProviderFamilies: 2, Members: []review.MemberRequirement{
		{ID: "one", Role: "independent", Route: "codex/sol", ProviderFamily: "openai", Tier: "executor", Required: true},
		{ID: "two", Role: "independent", Route: "other/model", ProviderFamily: "other", Tier: "executor", Required: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	authority, err := review.NewFrozenAuthority(review.ParentBinding{RunID: attempt.WorkflowRunID, TaskID: task.ID, AttemptID: attempt.ID, ThreadID: attempt.ThreadID, AssignmentID: assignment.ID, AssignmentEpoch: 1, IssuedRevision: attempt.Revision, Repository: "repo", BaseCommit: reviewGateBase, ExecutorRoute: "codex/sol"}, requirements)
	if err != nil {
		t.Fatal(err)
	}
	return &reviewGateFixture{path: path, store: store, task: task, attempt: attempt, assignment: assignment, authority: authority}
}

// openRound opens a checkpoint round on head while the attempt is live, and
// records every member's verdict when one is given.
func (f *reviewGateFixture) openRound(t *testing.T, id, head, verdict string) {
	t.Helper()
	ctx := context.Background()
	if _, err := f.store.FreezeReviewAuthority(ctx, f.authority); err != nil {
		t.Fatal(err)
	}
	cp, err := f.store.AllocateReviewCheckpoint(ctx, f.authority, review.Checkpoint{ID: id, HeadCommit: head, InputDigest: strings.Repeat("e", 64)})
	if err != nil {
		t.Fatal(err)
	}
	if verdict == "" {
		return
	}
	round, err := f.store.GetReviewRound(ctx, cp.RoundID)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range round.Reviewers {
		round, err = f.store.RecordReviewResult(ctx, cp.RoundID, member.ID, round.Revision, review.Result{State: "succeeded", ReviewMD: "Reviewed", VerdictJSON: []byte(`{"schema":"review-verdict/v1","verdict":"` + verdict + `","findings":[],"inputManifestDigest":"` + cp.Checkpoint.InputDigest + `","reviewerRoute":"` + member.Route + `"}`)})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// finishTurn moves the attempt to verification, where a worker result lands.
func (f *reviewGateFixture) finishTurn(t *testing.T) {
	t.Helper()
	attempt, assignment := f.attempt, f.assignment
	attempt.Progress, attempt.Control, attempt.Revision = domain.ProgressVerifying, domain.ControlStopped, attempt.Revision+1
	assignment.State = domain.AssignmentCompleted
	if err := f.store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
}

// result builds the worker upload: the summary and archive every result
// carries, the workspace HEAD evidence when given, and the declared commit's
// provenance when the task declares one.
func (f *reviewGateFixture) result(t *testing.T, head *domain.WorkspaceHead, commit string) (workerproto.ArtifactUploadResponse, resultUploadOpener) {
	t.Helper()
	now := coordinatorTestTime
	data := resultUploadOpener{"final-message-attempt-1": []byte("finished\n"), "thread-archive-attempt-1": []byte(reviewGateArchive)}
	var objects []workerproto.ArtifactObject
	if commit != "" {
		raw, err := MarshalCommitProvenance(CommitProvenance{WorkflowRunID: f.attempt.WorkflowRunID, TaskID: f.task.ID, Name: "change", Repository: "repo", Base: reviewGateBase, Commit: commit, Ref: CampaignRef(f.attempt.WorkflowRunID, f.task.ID, "change"), CreatedAt: now})
		if err != nil {
			t.Fatal(err)
		}
		data["commit-1"] = raw
		objects = append(objects, resultObject("commit-1", "results/change", "output", "application/json", raw))
	}
	objects = append(objects,
		resultObject("final-message-attempt-1", "results/final-message.md", "summary", "text/markdown", data["final-message-attempt-1"]),
		resultObject("thread-archive-attempt-1", "results/thread.json", "log", "application/json", data["thread-archive-attempt-1"]))
	if head != nil {
		raw, err := MarshalWorkspaceHead(*head)
		if err != nil {
			t.Fatal(err)
		}
		id := WorkspaceHeadArtifactID(f.attempt.ID)
		data[id] = raw
		objects = append(objects, resultObject(id, "results/"+WorkspaceHeadArtifactName, string(domain.ArtifactGitState), "application/json", raw))
	}
	manifest := resultManifest(now, f.assignment, objects)
	return workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}, data
}

func reviewGateImporter(t *testing.T, store *sqlite.Store) CoordinatorResultImporter {
	return CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 4096, MaxTotalBytes: 16384, Now: func() time.Time { return coordinatorTestTime.Add(time.Minute) }}
}

func (f *reviewGateFixture) collect(t *testing.T, store *sqlite.Store, head *domain.WorkspaceHead, commit string) domain.Attempt {
	t.Helper()
	response, data := f.result(t, head, commit)
	report, err := reviewGateImporter(t, store).Import(context.Background(), response, data)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Transition) != 1 {
		t.Fatalf("report = %#v", report)
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range records.Attempts {
		if attempt.ID == f.attempt.ID {
			return attempt
		}
	}
	t.Fatal("attempt is gone")
	return domain.Attempt{}
}

func cleanWorkspaceHead(head string) *domain.WorkspaceHead {
	return &domain.WorkspaceHead{Schema: domain.WorkspaceHeadSchema, Head: head}
}

// Criterion 1 and 2: the accepted head equal to the physical HEAD of a clean
// tree completes, the declared commit resolves to that head, and the durable
// attempt carries the decision with both heads.
func TestReviewGateCompletesAtTheAcceptedHead(t *testing.T) {
	f := newReviewGateFixture(t, true, true)
	f.openRound(t, "cp-1", reviewGateHeadA, "accept")
	f.finishTurn(t)
	attempt := f.collect(t, f.store, cleanWorkspaceHead(reviewGateHeadA), reviewGateHeadA)
	gate := attempt.ReviewGate
	if attempt.Progress != domain.ProgressSucceeded || gate == nil || !gate.Passed || gate.Code != domain.ReviewGateAccepted ||
		gate.ReviewedHead != reviewGateHeadA || gate.PhysicalHead != reviewGateHeadA || gate.RoundNumber != 1 || gate.CheckpointID != "cp-1" {
		t.Fatalf("attempt = %+v gate = %+v", attempt, gate)
	}
}

// Criterion 1: every structured failure reason fails the task with its code.
func TestReviewGateFailsWithEachStructuredReason(t *testing.T) {
	dirty := cleanWorkspaceHead(reviewGateHeadA)
	dirty.Dirty, dirty.DirtyPaths = true, []string{"main.go"}
	for _, test := range []struct {
		name    string
		round   func(*testing.T, *reviewGateFixture)
		head    *domain.WorkspaceHead
		commit  string
		code    domain.ReviewGateCode
		details []string
	}{
		{name: "no round", round: func(*testing.T, *reviewGateFixture) {}, head: cleanWorkspaceHead(reviewGateHeadA), code: domain.ReviewGateRequired},
		{name: "pending round", round: func(t *testing.T, f *reviewGateFixture) { f.openRound(t, "cp-1", reviewGateHeadA, "") }, head: cleanWorkspaceHead(reviewGateHeadA), code: domain.ReviewGateNotAccepted, details: []string{"pending"}},
		{name: "rejected round", round: func(t *testing.T, f *reviewGateFixture) { f.openRound(t, "cp-1", reviewGateHeadA, "reject") }, head: cleanWorkspaceHead(reviewGateHeadA), code: domain.ReviewGateNotAccepted, details: []string{"reject"}},
		{name: "head changed", round: func(t *testing.T, f *reviewGateFixture) { f.openRound(t, "cp-1", reviewGateHeadA, "accept") }, head: cleanWorkspaceHead(reviewGateHeadB), code: domain.ReviewGateHeadChanged, details: []string{reviewGateHeadA, reviewGateHeadB}},
		{name: "dirty tree", round: func(t *testing.T, f *reviewGateFixture) { f.openRound(t, "cp-1", reviewGateHeadA, "accept") }, head: dirty, code: domain.ReviewGateDirtyTree, details: []string{"main.go"}},
		{name: "no head evidence", round: func(t *testing.T, f *reviewGateFixture) { f.openRound(t, "cp-1", reviewGateHeadA, "accept") }, code: domain.ReviewGateHeadUnknown},
		{name: "declared commit after review", round: func(t *testing.T, f *reviewGateFixture) { f.openRound(t, "cp-1", reviewGateHeadA, "accept") }, head: cleanWorkspaceHead(reviewGateHeadA), commit: reviewGateHeadB,
			code: domain.ReviewGateHeadChanged, details: []string{`declared commit "change"`, reviewGateHeadB}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newReviewGateFixture(t, true, test.commit != "")
			test.round(t, f)
			f.finishTurn(t)
			attempt := f.collect(t, f.store, test.head, test.commit)
			if attempt.Progress != domain.ProgressFailed || attempt.ReviewGate == nil || attempt.ReviewGate.Passed || attempt.ReviewGate.Code != test.code ||
				!strings.Contains(attempt.Failure, string(test.code)) {
				t.Fatalf("attempt = %+v gate = %+v", attempt, attempt.ReviewGate)
			}
			for _, detail := range test.details {
				if !strings.Contains(attempt.Failure, detail) {
					t.Fatalf("failure %q does not name %q", attempt.Failure, detail)
				}
			}
		})
	}
}

// Criterion 3: a commit after an accepted round invalidates that acceptance
// and asks for a new round; a second accepted round on the new head completes.
func TestReviewGateNewCommitInvalidatesAndASecondRoundCompletes(t *testing.T) {
	invalidated := newReviewGateFixture(t, true, false)
	invalidated.openRound(t, "cp-1", reviewGateHeadA, "accept")
	invalidated.finishTurn(t)
	attempt := invalidated.collect(t, invalidated.store, cleanWorkspaceHead(reviewGateHeadB), "")
	if attempt.Progress != domain.ProgressFailed || attempt.ReviewGate == nil || !attempt.ReviewGate.NewRoundNeeded ||
		!strings.Contains(attempt.Failure, "new review round on "+reviewGateHeadB) {
		t.Fatalf("invalidated attempt = %+v gate = %+v", attempt, attempt.ReviewGate)
	}

	second := newReviewGateFixture(t, true, false)
	second.openRound(t, "cp-1", reviewGateHeadA, "accept")
	second.openRound(t, "cp-2", reviewGateHeadB, "accept")
	second.finishTurn(t)
	attempt = second.collect(t, second.store, cleanWorkspaceHead(reviewGateHeadB), "")
	if attempt.Progress != domain.ProgressSucceeded || attempt.ReviewGate == nil || attempt.ReviewGate.RoundNumber != 2 || attempt.ReviewGate.ReviewedHead != reviewGateHeadB {
		t.Fatalf("second round attempt = %+v gate = %+v", attempt, attempt.ReviewGate)
	}
}

// Criterion 5: a coordinator restart between the round's acceptance and the
// result's collection evaluates against the durable round.
func TestReviewGateEvaluatesAfterACoordinatorRestart(t *testing.T) {
	f := newReviewGateFixture(t, true, false)
	f.openRound(t, "cp-1", reviewGateHeadA, "accept")
	f.finishTurn(t)
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sqlite.OpenMigrated(f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	attempt := f.collect(t, reopened, cleanWorkspaceHead(reviewGateHeadA), "")
	if attempt.Progress != domain.ProgressSucceeded || attempt.ReviewGate == nil || !attempt.ReviewGate.Passed {
		t.Fatalf("attempt after restart = %+v gate = %+v", attempt, attempt.ReviewGate)
	}
}

// Criterion 4: a task without a review declaration is untouched by the gate,
// and it may not carry workspace HEAD evidence it was never asked for.
func TestReviewGateLeavesUndeclaredTasksAlone(t *testing.T) {
	f := newReviewGateFixture(t, false, false)
	f.finishTurn(t)
	attempt := f.collect(t, f.store, nil, "")
	if attempt.Progress != domain.ProgressSucceeded || attempt.ReviewGate != nil {
		t.Fatalf("undeclared attempt = %+v", attempt)
	}

	smuggled := newReviewGateFixture(t, false, false)
	smuggled.finishTurn(t)
	response, data := smuggled.result(t, cleanWorkspaceHead(reviewGateHeadA), "")
	_, err := reviewGateImporter(t, smuggled.store).Import(context.Background(), response, data)
	if !errors.Is(err, ErrResultImportRejected) {
		t.Fatalf("undeclared workspace head evidence was not rejected: %v", err)
	}
}

// A result that has already failed is not judged by the gate: its own failure
// is the reason, and no decision is recorded for a turn that was never
// eligible to complete.
func TestReviewGateDoesNotJudgeAnAlreadyFailedResult(t *testing.T) {
	f := newReviewGateFixture(t, true, true)
	f.finishTurn(t)
	attempt := f.collect(t, f.store, nil, "")
	if attempt.Progress != domain.ProgressFailed || attempt.ReviewGate != nil || !strings.Contains(attempt.Failure, "missing declared output") {
		t.Fatalf("attempt = %+v gate = %+v", attempt, attempt.ReviewGate)
	}
}
