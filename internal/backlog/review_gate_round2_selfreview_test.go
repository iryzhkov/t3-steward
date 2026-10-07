package backlog

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// Fix round 2 self-review: attempt-1 staged a commit the gate rejected, and
// attempt-2's commit was promoted on the same worker. A judge of attempt-1,
// prepared then or again after a restart, inspects attempt-1's staging. The
// campaign ref naming attempt-2's commit is expected, not a conflict.
func TestAJudgeOfARejectedAttemptPreparesAfterALaterPromotion(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	storage := t.TempDir()
	worker := newCommitWorker(t, storage)
	rejected := produceCommit(t, worker, repository, storage, produceOptions{reviewGated: true})

	clone := t.TempDir()
	gitRun(t, clone, "clone", "-q", repository, ".")
	gitRun(t, clone, "checkout", "-q", rejected.base)
	gitRun(t, clone, "-c", "user.name=T", "-c", "user.email=t@example.test", "commit", "-q", "--allow-empty", "-m", "accepted")
	accepted, err := worker.refs.Stage(ctx, PublishCommitRequest{
		WorkflowRunID: "run-1", TaskID: "task-producer", Name: "repair", Repository: rejected.provenance.Repository,
		WorkspaceDir: clone, Base: rejected.base,
	}, "attempt-2", nil)
	if err != nil {
		t.Fatal(err)
	}
	consumer := t.TempDir()
	gitRun(t, consumer, "init", "-q")
	if err := worker.refs.FetchAcceptedInto(ctx, consumer, accepted, nil); err != nil {
		t.Fatal(err)
	}
	prepared, err := consumeReviewed(t, worker, rejected, nil, "attempt-judge", false)
	if err != nil {
		t.Fatalf("judge of the rejected attempt cannot prepare after a later promotion: %v", err)
	}
	if got := gitOutput(t, prepared.WorkspaceDir, "rev-parse", CampaignRef("run-1", "task-producer", "repair")); got != rejected.commit {
		t.Fatalf("judge fetched %s, want %s", got, rejected.commit)
	}
	if provenance, err := worker.refs.Resolve("run-1", "task-producer", "repair"); err != nil || provenance.Commit != accepted.Commit {
		t.Fatalf("published commit = %+v %v, want the accepted %s", provenance, err, accepted.Commit)
	}
	// The accepted consumer of the rejected attempt is still refused.
	if _, err := consumeReviewed(t, worker, rejected, nil, "attempt-wrong", true); err == nil {
		t.Fatal("an accepted consumer of another attempt's staging was prepared")
	}
}

// A promotion that died between its campaign ref and its record leaves a ref
// no record names. Releasing the run removes it with everything else.
func TestReleaseRemovesACampaignRefWhosePromotionCrashed(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	storage := t.TempDir()
	worker := newCommitWorker(t, storage)
	produced := produceCommit(t, worker, repository, storage, produceOptions{reviewGated: true})
	gitDir := filepath.Join(worker.refs.Root, "campaigns.git")
	gitRun(t, gitDir, "update-ref", CampaignRef("run-1", "task-producer", "repair"), produced.commit)
	if err := worker.refs.ReleaseRun(ctx, "run-1", nil); err != nil {
		t.Fatal(err)
	}
	if out := strings.TrimSpace(gitOutput(t, gitDir, "for-each-ref")); out != "" {
		t.Fatalf("release left refs behind: %s", out)
	}
}

// A rerun created before carried inputs were bound to their source carries a
// commit record of another run with no source run. The coordinator cannot
// find its declaration, so it is not marked; the worker still refuses the
// record, as it did before marks, rather than preparing the consumer without
// the commit it was handed.
func TestAnUnboundCarriedCommitRecordIsStillRefused(t *testing.T) {
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1"}},
		Tasks:        []domain.Task{commitProducerTask()},
	}
	legacy := domain.CarriedInput{Producer: "producer", ProducerTaskID: "task-producer", Name: "repair", ArtifactID: "carried"}
	dependencies := []workerproto.DependencyInput{{TaskID: "task-producer", Artifacts: []workerproto.ArtifactObject{{Path: "dependencies/producer/repair"}}}}
	markDependencyCommitOutputs(dependencies, domain.Task{CarriedInputs: []domain.CarriedInput{legacy}}, nil,
		func(c domain.CarriedInput) bool { return carriedCommitDeclared(records, c) })
	if len(dependencies[0].CommitOutputs) != 0 {
		t.Fatalf("an unbound carried input was marked: %v", dependencies[0].CommitOutputs)
	}

	repository := newGitFixture(t)
	storage := t.TempDir()
	produced := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{})
	consumer := newCommitWorker(t, storage)
	task := workspaceTask("task-consumer", "consumer")
	task.Needs = []string{"producer"}
	task.DependencyInputs = map[string][]string{"producer": {"repair"}}
	request := workspaceRequest(produced.repository, "main", task, "attempt-2")
	request.WorkflowRunID, request.Attempt.WorkflowRunID = "run-2", "run-2"
	record := produced.record
	record.WorkflowRunID, record.AttemptID = "run-2", ""
	// As LocalDriver builds it from the unmarked dependency.
	request.DependencyTasks = []domain.Task{{ID: "task-producer", WorkflowID: "workflow-1", Name: "producer"}, task}
	request.DependencyArtifacts = []domain.Artifact{record}
	prepared, err := consumer.preparer.Prepare(context.Background(), request)
	if err == nil {
		cleanupImmutable(t, prepared.RootDir)
		t.Fatal("an unbound carried commit record was skipped, and its consumer prepared without the commit")
	}
	if !strings.Contains(err.Error(), `belongs to run "run-1", want "run-2"`) {
		t.Fatalf("error = %v", err)
	}
}
