package backlog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A commit a downstream task needs is reachable by its campaign ref after the
// repository cache has been pruned. The cache is a cache: pruning it is normal,
// and nothing may depend on what pruning removes.
func TestDownstreamTaskResolvesCommitAfterCachePrune(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	runsRoot := t.TempDir()
	storage := t.TempDir()
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	preparer := workspacePreparer(runsRoot, storage)
	preparer.CampaignRefs = refs

	producer := workspaceTask("task-producer", "producer")
	producerRequest := workspaceRequest(repository, "main", producer, "attempt-1")
	prepared, err := preparer.Prepare(ctx, producerRequest)
	if err != nil {
		t.Fatalf("prepare producer workspace: %v", err)
	}
	cleanupImmutable(t, prepared.RootDir)
	base := prepared.Commit

	// The producing task commits its work in its own checkout.
	gitRun(t, prepared.WorkspaceDir, "config", "user.name", "Test User")
	gitRun(t, prepared.WorkspaceDir, "config", "user.email", "test@example.test")
	writeGitFile(t, prepared.WorkspaceDir, "implementation.txt", "implemented\n")
	gitRun(t, prepared.WorkspaceDir, "add", "implementation.txt")
	gitRun(t, prepared.WorkspaceDir, "commit", "-m", "implementation")
	commit := gitOutput(t, prepared.WorkspaceDir, "rev-parse", "HEAD")

	// The old handoff parked the branch in the shared repository cache. Do the
	// same here, so that the prune below removes something real.
	sum := sha256.Sum256([]byte(repository))
	cachePath := filepath.Join(runsRoot, "cache", fmt.Sprintf("%x.git", sum))
	gitRun(t, prepared.WorkspaceDir, "push", cachePath, "HEAD:refs/heads/handoff")

	producerTask := producer
	producerTask.Outputs = []domain.ArtifactDeclaration{{Name: "handoff", Commit: &domain.CommitOutput{}}}
	finalizer := AttemptFinalizer{StorageRoot: storage, CampaignRefs: refs, Processes: testProcessRunner{}}
	finalized, err := finalizer.Finalize(ctx, AttemptFinalization{
		Task: producerTask, Attempt: producerRequest.Attempt, WorkspaceDir: prepared.WorkspaceDir,
		ExplicitSuccess: true, Repository: repository, BaseCommit: base,
	})
	if err != nil {
		t.Fatalf("finalize producer: %v", err)
	}
	cleanupImmutable(t, finalized.StorageDir)
	if !finalized.Completion.VerificationPassed || len(finalized.Artifacts) != 1 {
		t.Fatalf("finalized producer = %+v", finalized)
	}
	handoff := finalized.Artifacts[0]
	if handoff.Name != "handoff" {
		t.Fatalf("declared commit artifact = %+v", handoff)
	}

	// The provenance report names the producer, the base and the repository.
	provenance, err := refs.Resolve("run-1", "task-producer", "handoff")
	if err != nil {
		t.Fatalf("resolve provenance: %v", err)
	}
	if provenance.Commit != commit || provenance.Base != base ||
		provenance.Repository != repository || provenance.TaskID != "task-producer" ||
		provenance.Ref != "refs/campaigns/run-1/task-producer/handoff" {
		t.Fatalf("provenance = %+v", provenance)
	}
	report := provenance.Report()
	for _, want := range []string{commit, base, repository, "task-producer"} {
		if !strings.Contains(report, want) {
			t.Fatalf("provenance report %q does not name %q", report, want)
		}
	}
	if records, err := refs.List("run-1"); err != nil || len(records) != 1 || records[0].Commit != commit {
		t.Fatalf("campaign commits = %+v, %v", records, err)
	}

	// Prune the repository cache for real, exactly as the next task does when
	// it refreshes the cache before its own clone.
	if _, err := preparer.Cache.Prepare(ctx, repository, nil); err != nil {
		t.Fatalf("refresh repository cache: %v", err)
	}
	if err := exec.Command("git", "--git-dir", cachePath, "rev-parse", "--verify", "--quiet", "refs/heads/handoff").Run(); err == nil {
		t.Fatal("pruning the repository cache kept the parked branch; the test no longer proves anything")
	}

	consumer := workspaceTask("task-consumer", "consumer")
	consumer.Needs = []string{"producer"}
	consumer.DependencyInputs = map[string][]string{"producer": {"handoff"}}
	consumerRequest := workspaceRequest(repository, "main", consumer, "attempt-2")
	consumerRequest.DependencyTasks = []domain.Task{producer, consumer}
	consumerRequest.DependencyArtifacts = []domain.Artifact{handoff}
	consumerRequest.Environment.Setup.Commands = []string{
		"git rev-parse --verify refs/campaigns/run-1/task-producer/handoff",
	}
	consumerWorkspace, err := preparer.Prepare(ctx, consumerRequest)
	if err != nil {
		t.Fatalf("prepare consumer workspace: %v", err)
	}
	cleanupImmutable(t, consumerWorkspace.RootDir)
	resolved := gitOutput(t, consumerWorkspace.WorkspaceDir, "rev-parse", provenance.Ref+"^{commit}")
	if resolved != commit {
		t.Fatalf("consumer resolved %s, want %s", resolved, commit)
	}
	if got := gitOutput(t, consumerWorkspace.WorkspaceDir, "show", commit+":implementation.txt"); got != "implemented" {
		t.Fatalf("consumer content = %q", got)
	}
	record := readTestFile(t, consumerWorkspace.DependenciesDir, "producer/handoff")
	delivered, err := ParseCommitProvenance([]byte(record))
	if err != nil || delivered != provenance {
		t.Fatalf("delivered provenance = %+v, %v", delivered, err)
	}
}

func TestCampaignRefStoreRefusesADifferentCommitAndReleasesARun(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	request := PublishCommitRequest{
		WorkflowRunID: "run-1", TaskID: "task-producer", Name: "handoff",
		Repository: repository, WorkspaceDir: repository, Base: base,
	}
	first, err := refs.Publish(ctx, request, nil)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := refs.Publish(ctx, request, nil); err != nil {
		t.Fatalf("republishing the same commit must be idempotent: %v", err)
	}

	writeGitFile(t, repository, "version.txt", "second\n")
	gitRun(t, repository, "add", "version.txt")
	gitRun(t, repository, "commit", "-m", "second")
	if _, err := refs.Publish(ctx, request, nil); err == nil || !strings.Contains(err.Error(), "already names commit "+first.Commit) {
		t.Fatalf("error = %v, want a refusal to redefine the ref", err)
	}

	if err := refs.ReleaseRun(ctx, "run-1", nil); err != nil {
		t.Fatalf("release run: %v", err)
	}
	if records, err := refs.List("run-1"); err != nil || len(records) != 0 {
		t.Fatalf("campaign commits after release = %+v, %v", records, err)
	}
	if _, err := refs.Resolve("run-1", "task-producer", "handoff"); err == nil {
		t.Fatal("released campaign commit still resolves")
	}
	if err := exec.Command("git", "--git-dir", filepath.Join(refs.Root, "campaigns.git"),
		"rev-parse", "--verify", "--quiet", first.Ref).Run(); err == nil {
		t.Fatal("released campaign ref still exists")
	}
	_ = os.Remove(filepath.Join(refs.Root, "unused"))
}
