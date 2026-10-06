package backlog

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// The workspace's configuration belongs to the executor. A core.worktree
// setting that points Git at a clean copy elsewhere must not stand in for the
// task workspace the review gate is asked about.
func TestCaptureWorkspaceHeadInspectsTheTaskWorkspaceNotAConfiguredWorktree(t *testing.T) {
	dir, head := workspaceHeadRepository(t)
	shadow := t.TempDir()
	for _, name := range []string{"main.go", "answer.txt", ".t3/base-commit", ".t3-steward/task.env"} {
		target := filepath.Join(shadow, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte("one\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	gitRun(t, dir, "config", "core.worktree", shadow)
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("unreviewed code\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	captured := CaptureWorkspaceHead(context.Background(), "", dir, nil)
	gate := domain.EvaluateReviewCompletionGate(&domain.ReviewRoundHead{HeadCommit: head, Accepted: true, Verdict: "accept"}, &captured, nil)
	if captured.Head != head || !captured.Dirty || !slices.Contains(captured.DirtyPaths, "main.go") || gate.Passed {
		t.Fatalf("redirected worktree completed: capture=%+v gate=%+v", captured, gate)
	}
}

// The same redirection with nothing changed in the task workspace is clean:
// pinning the worktree must not report the shadow copy's differences.
func TestCaptureWorkspaceHeadIgnoresAConfiguredWorktreeWhenTheWorkspaceIsClean(t *testing.T) {
	dir, head := workspaceHeadRepository(t)
	gitRun(t, dir, "config", "core.worktree", t.TempDir())
	if captured := CaptureWorkspaceHead(context.Background(), "", dir, nil); captured.Head != head || captured.Dirty || captured.Error != "" {
		t.Fatalf("clean task workspace reported as %+v", captured)
	}
}

// gatedFinalize finalizes a review-declared attempt whose workspace HEAD is
// the declared commit, with the repository and base the coordinator's copy of
// the commit reference carries, and returns the staged provenance.
func gatedFinalize(t *testing.T, f *reviewGateFixture, refs CampaignRefStore, repository string) CommitProvenance {
	t.Helper()
	finalized, err := (AttemptFinalizer{StorageRoot: t.TempDir(), CampaignRefs: refs, Processes: testProcessRunner{}}).Finalize(context.Background(), AttemptFinalization{
		Task: f.task, Attempt: f.attempt, WorkspaceDir: repository, ExplicitSuccess: true,
		Repository: "repo", BaseCommit: reviewGateBase, ReviewGated: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, finalized.StorageDir)
	if !finalized.Completion.VerificationPassed {
		t.Fatalf("fixture finalization failed: %+v", finalized.Completion)
	}
	return finalizedCommitProvenance(t, finalized, "change")
}

// rejectedReviewGatedOutput finalizes a review-declared attempt at a commit
// after the accepted head, has the coordinator reject it, and returns the
// staged provenance the coordinator retained with the failed result.
func rejectedReviewGatedOutput(t *testing.T) (*reviewGateFixture, CampaignRefStore, CommitProvenance, string, string) {
	t.Helper()
	repository := newGitFixture(t)
	accepted := gitOutput(t, repository, "rev-parse", "HEAD")
	gitRun(t, repository, "commit", "--allow-empty", "-m", "unreviewed later head")
	later := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	f := newReviewGateFixture(t, true, true)
	f.openRound(t, "cp-1", accepted, "accept")
	f.finishTurn(t)
	staged := gatedFinalize(t, f, refs, repository)
	attempt := f.collect(t, f.store, cleanWorkspaceHead(later), later)
	if attempt.Progress != domain.ProgressFailed || attempt.ReviewGate == nil || attempt.ReviewGate.Code != domain.ReviewGateHeadChanged {
		t.Fatalf("gate did not reject: %+v", attempt)
	}
	return f, refs, staged, accepted, later
}

func reviewJudgeOf(f *reviewGateFixture) domain.Task {
	return domain.Task{ID: "judge", Name: "judge", ReviewJudge: true, Needs: []string{f.task.Name},
		DependencyInputs: map[string][]string{f.task.Name: {"change"}}}
}

// A review judge may run after its dependency failed and is given the failed
// result's outputs to inspect. Inspecting a rejected declared commit must not
// make it the producer's campaign output.
func TestReviewGateJudgeInspectingARejectedCommitDoesNotPublishIt(t *testing.T) {
	ctx := context.Background()
	f, refs, staged, accepted, later := rejectedReviewGatedOutput(t)
	records, err := f.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	artifacts := map[string]domain.Artifact{}
	for _, artifact := range records.Artifacts {
		artifacts[artifact.ID] = artifact
	}
	judge := reviewJudgeOf(f)
	dependencies, err := packageDependencies(judge, []domain.Task{f.task, judge}, artifacts, f.attempt.WorkflowRunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(dependencies) != 1 || len(dependencies[0].Artifacts) != 1 {
		t.Fatalf("judge not given the rejected output: %+v", dependencies)
	}
	consumer := t.TempDir()
	gitRun(t, consumer, "init", "-q")
	if err := refs.FetchInto(ctx, consumer, staged, nil); err != nil {
		t.Fatal(err)
	}
	if got, err := refs.Resolve(f.attempt.WorkflowRunID, f.task.ID, "change"); err == nil {
		t.Fatalf("judge inspection published rejected commit %s (accepted %s)", got.Commit, accepted)
	}
	if out := gitOutput(t, filepath.Join(refs.Root, "campaigns.git"), "for-each-ref", "refs/campaigns/"); out != "" {
		t.Fatalf("judge inspection created a campaign ref: %s", out)
	}
	// The judge still sees the commit it was asked to inspect.
	if got := gitOutput(t, consumer, "rev-parse", staged.Ref); got != later {
		t.Fatalf("judge resolved %s, want the rejected %s", got, later)
	}
}

// coordinatorDependency packages the producer's retained output for a
// consumer exactly as the offer builder does, including the acceptance mark.
func coordinatorDependency(t *testing.T, f *reviewGateFixture, consumer domain.Task) workerproto.DependencyInput {
	t.Helper()
	records, err := f.store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	artifacts := map[string]domain.Artifact{}
	for _, artifact := range records.Artifacts {
		artifacts[artifact.ID] = artifact
	}
	succeeded := map[string]struct{}{}
	for _, attempt := range records.Attempts {
		if attempt.Progress == domain.ProgressSucceeded {
			succeeded[attempt.ID] = struct{}{}
		}
	}
	tasks := []domain.Task{f.task, consumer}
	dependencies, err := packageDependencies(consumer, tasks, artifacts, f.attempt.WorkflowRunID)
	if err != nil {
		t.Fatal(err)
	}
	markAcceptedDependencies(dependencies, tasks, artifacts, succeeded)
	if len(dependencies) != 1 || len(dependencies[0].Artifacts) != 1 {
		t.Fatalf("consumer not given the producer's output: %+v", dependencies)
	}
	return dependencies[0]
}

// consumeDependencyCommit runs the worker's dependency resolution for a
// consumer whose dependency view holds the producer's commit reference under
// change, and any further files given, accepted or not as the execution
// package said, and returns the commit the consumer resolved.
func consumeDependencyCommit(t *testing.T, refs CampaignRefStore, runID string, provenance CommitProvenance, dependency workerproto.DependencyInput, extra map[string]CommitProvenance) string {
	t.Helper()
	dependencies := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dependencies, dependency.TaskID), 0o700); err != nil {
		t.Fatal(err)
	}
	files := map[string]CommitProvenance{"change": provenance}
	for name, content := range extra {
		files[name] = content
	}
	for name, content := range files {
		raw, err := MarshalCommitProvenance(content)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dependencies, dependency.TaskID, name), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// The worker names each dependency task by its ID, as LocalDriver does.
	request := WorkspacePreparation{
		WorkflowRunID: runID, Environment: ResolvedEnvironment{Type: EnvironmentGit},
		DependencyTasks: []domain.Task{{ID: dependency.TaskID, Name: dependency.TaskID}},
	}
	if len(dependency.AcceptedCommits) != 0 {
		request.AcceptedCommits = map[string][]string{dependency.TaskID: dependency.AcceptedCommits}
	}
	consumer := t.TempDir()
	gitRun(t, consumer, "init", "-q")
	if err := (WorkspacePreparer{CampaignRefs: refs}).resolveDependencyCommits(context.Background(), dependencies, consumer, request, nil); err != nil {
		t.Fatal(err)
	}
	return gitOutput(t, consumer, "rev-parse", provenance.Ref)
}

// The whole path from the gate to publication: the coordinator marks only an
// accepted result, a judge given the rejected result inspects its commit and
// publishes nothing, and a consumer of the accepted result publishes the
// accepted head.
func TestReviewGatedCommitIsPublishedOnlyThroughAnAcceptedDependency(t *testing.T) {
	t.Run("rejected", func(t *testing.T) {
		f, refs, staged, _, later := rejectedReviewGatedOutput(t)
		dependency := coordinatorDependency(t, f, reviewJudgeOf(f))
		if len(dependency.AcceptedCommits) != 0 {
			t.Fatalf("rejected result marked accepted: %+v", dependency)
		}
		if got := consumeDependencyCommit(t, refs, f.attempt.WorkflowRunID, staged, dependency, nil); got != later {
			t.Fatalf("judge resolved %s, want the rejected %s", got, later)
		}
		if provenance, err := refs.Resolve(f.attempt.WorkflowRunID, f.task.ID, "change"); err == nil {
			t.Fatalf("judge inspection published the rejected commit: %+v", provenance)
		}
	})
	t.Run("accepted", func(t *testing.T) {
		repository := newGitFixture(t)
		accepted := gitOutput(t, repository, "rev-parse", "HEAD")
		refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
		f := newReviewGateFixture(t, true, true)
		f.openRound(t, "cp-1", accepted, "accept")
		f.finishTurn(t)
		staged := gatedFinalize(t, f, refs, repository)
		if attempt := f.collect(t, f.store, cleanWorkspaceHead(accepted), accepted); attempt.Progress != domain.ProgressSucceeded {
			t.Fatalf("gate did not accept: %+v", attempt)
		}
		consumer := domain.Task{ID: "consumer", Name: "consumer", Needs: []string{f.task.Name},
			DependencyInputs: map[string][]string{f.task.Name: {"change"}}}
		dependency := coordinatorDependency(t, f, consumer)
		if !slices.Equal(dependency.AcceptedCommits, []string{"change"}) {
			t.Fatalf("accepted result not marked: %+v", dependency)
		}
		if got := consumeDependencyCommit(t, refs, f.attempt.WorkflowRunID, staged, dependency, nil); got != accepted {
			t.Fatalf("consumer resolved %s, want %s", got, accepted)
		}
		if provenance, err := refs.Resolve(f.attempt.WorkflowRunID, f.task.ID, "change"); err != nil || provenance.Commit != accepted {
			t.Fatalf("published provenance = %+v %v, want %s", provenance, err, accepted)
		}
	})
}

// An accepted retry's ordinary output that holds the commit reference of the
// rejected first attempt must not publish that attempt's commit: only the
// declared commit output the coordinator accepted can.
func TestAcceptedProducersOtherOutputCannotPublishARejectedStaging(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	request := PublishCommitRequest{WorkflowRunID: "run-1", TaskID: "producer", Name: "change",
		Repository: "repo", WorkspaceDir: repository, Base: base}
	gitRun(t, repository, "commit", "--allow-empty", "-m", "rejected")
	rejected, err := refs.Stage(ctx, request, "attempt-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	gitRun(t, repository, "commit", "--allow-empty", "-m", "accepted")
	accepted, err := refs.Stage(ctx, request, "attempt-2", nil)
	if err != nil {
		t.Fatal(err)
	}
	dependency := workerproto.DependencyInput{TaskID: "producer", AcceptedCommits: []string{"change"}}
	// "aaa-notes" sorts first, so it is resolved before the declared output.
	got := consumeDependencyCommit(t, refs, "run-1", accepted, dependency, map[string]CommitProvenance{"aaa-notes": rejected})
	if got != accepted.Commit {
		t.Fatalf("consumer resolved %s, want %s", got, accepted.Commit)
	}
	if provenance, err := refs.Resolve("run-1", "producer", "change"); err != nil || provenance.Commit != accepted.Commit {
		t.Fatalf("published provenance = %+v %v, want %s (rejected %s)", provenance, err, accepted.Commit, rejected.Commit)
	}
}

// The acceptance covers the accepted producer's declared commit output only.
// Any other file in its outputs, a file naming another task or output, or one
// outside its directory of the view, cannot publish a staged commit.
func TestAcceptedDependencyCommitMustBeTheProducersOwn(t *testing.T) {
	dependencies := filepath.Join(t.TempDir(), "dependencies")
	request := WorkspacePreparation{
		DependencyTasks: []domain.Task{{ID: "producer", Name: "producer"}, {ID: "other", Name: "other"}},
		AcceptedCommits: map[string][]string{"producer": {"change"}},
	}
	for _, test := range []struct {
		name, path, task, output string
		want                     bool
	}{
		{"declared commit output", "producer/change", "producer", "change", true},
		{"another output naming the producer", "producer/notes", "producer", "change", false},
		{"another output naming itself", "producer/notes", "producer", "notes", false},
		{"nested output", "producer/nested/change", "producer", "change", false},
		{"names another output", "producer/change", "producer", "other-change", false},
		{"names another task", "producer/change", "other", "change", false},
		{"unaccepted producer", "other/change", "other", "change", false},
		{"outside a producer directory", "change", "producer", "change", false},
		{"another producer's directory", "other/change", "producer", "change", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			provenance := CommitProvenance{TaskID: test.task, Name: test.output}
			if got := acceptedDependencyCommit(dependencies, filepath.Join(dependencies, filepath.FromSlash(test.path)), provenance, request); got != test.want {
				t.Fatalf("accepted = %v, want %v", got, test.want)
			}
		})
	}
}

// The offer builder marks a review-declared producer only when its packaged
// output came from the attempt the coordinator accepted, requires the
// capability that honours the mark, and leaves other packages unchanged.
func TestOfferMarksOnlyAnAcceptedReviewDeclaredProducer(t *testing.T) {
	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	build := func(t *testing.T, declared bool, progress domain.ProgressState, capabilities []string) (workerproto.ExecutionPackage, error) {
		t.Helper()
		records, assignment := packageBuilderFixture(now)
		if declared {
			records.Tasks[0].ReviewRequirements = &domain.TaskReviewRequirements{Version: 1}
		}
		records.Tasks[0].Outputs = []domain.ArtifactDeclaration{{Name: "reports/result.txt", Commit: &domain.CommitOutput{}}}
		records.Attempts = append(records.Attempts, domain.Attempt{
			ID: "producer-attempt", WorkflowRunID: "run-1", TaskID: records.Tasks[0].ID, Number: 1,
			Progress: progress, Control: domain.ControlStopped, Revision: 4, UpdatedAt: now,
		})
		for index := range records.Artifacts {
			if records.Artifacts[index].TaskID == records.Tasks[0].ID {
				records.Artifacts[index].AttemptID = "producer-attempt"
			}
		}
		builder := packageBuilder(t, records)
		if capabilities != nil {
			builder.WorkerCapabilities = map[string][]string{"normandy": capabilities}
		}
		offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
		return offer.Package.Package, err
	}
	capable := []string{workerproto.PackageCapabilityAcceptedDependencies}

	pkg, err := build(t, true, domain.ProgressSucceeded, capable)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(pkg.Dependencies[0].AcceptedCommits, []string{"reports/result.txt"}) || !slices.Contains(pkg.RequiredCapabilities, workerproto.PackageCapabilityAcceptedDependencies) {
		t.Fatalf("accepted review-declared producer not marked: %+v %v", pkg.Dependencies, pkg.RequiredCapabilities)
	}
	if _, err := build(t, true, domain.ProgressSucceeded, []string{}); err == nil || !strings.Contains(err.Error(), workerproto.PackageCapabilityAcceptedDependencies) {
		t.Fatalf("older worker was offered an accepted dependency: %v", err)
	}

	for _, test := range []struct {
		name     string
		declared bool
		progress domain.ProgressState
	}{
		{"rejected review-declared producer", true, domain.ProgressFailed},
		{"producer without review", false, domain.ProgressSucceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			pkg, err := build(t, test.declared, test.progress, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(pkg.Dependencies[0].AcceptedCommits) != 0 || len(pkg.RequiredCapabilities) != 0 {
				t.Fatalf("package changed: %+v %v", pkg.Dependencies, pkg.RequiredCapabilities)
			}
		})
	}
}
