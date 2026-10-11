package backlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestThreeRunChainingWithoutPush(t *testing.T) {
	for _, unchanged := range []bool{false, true} {
		t.Run(fmt.Sprintf("unchanged=%v", unchanged), func(t *testing.T) { testThreeRunChainingWithoutPush(t, unchanged) })
	}
}

func testThreeRunChainingWithoutPush(t *testing.T, unchanged bool) {
	ctx := context.Background()
	repository, storage := newGitFixture(t), t.TempDir()
	first := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{})
	consume := func(worker commitWorker, produced producedCommit, run, attempt string) (PreparedWorkspace, WorkspacePreparation, error) {
		sourceRun, sourceTask := produced.provenance.WorkflowRunID, produced.provenance.TaskID
		namespace := externalProducerNamespace(sourceRun, domain.Task{ID: sourceTask, Name: "producer"})
		task := workspaceTask("task-producer", "producer")
		task.Needs = []string{namespace}
		task.DependencyInputs = map[string][]string{namespace: {"repair"}}
		task.Outputs = []domain.ArtifactDeclaration{{Name: "repair", Commit: &domain.CommitOutput{}}}
		request := workspaceRequest(repository, fmt.Sprintf("campaign:%s/producer/repair", sourceRun), task, attempt)
		request.WorkflowRunID, request.Attempt.WorkflowRunID = run, run
		record := produced.record
		record.WorkflowRunID, record.TaskID, record.AttemptID = run, sourceTask, ""
		request.DependencyTasks = []domain.Task{{ID: sourceTask, Name: namespace, Outputs: task.Outputs}}
		request.DependencyArtifacts = []domain.Artifact{record}
		request.DependencySources = map[string]DependencySource{namespace: {WorkflowRunID: sourceRun, TaskID: sourceTask, AttemptID: produced.record.AttemptID}}
		request.CommitBundles = map[string]CommitBundleDelivery{produced.provenance.Ref: produced.delivery(t, nil)}
		prepared, err := worker.preparer.Prepare(ctx, request)
		if err == nil {
			cleanupImmutable(t, prepared.RootDir)
		}
		return prepared, request, err
	}
	secondWorker := newCommitWorker(t, storage)
	second, request, err := consume(secondWorker, first, "run-2", "attempt-2")
	if err != nil {
		t.Fatal(err)
	}
	gitRun(t, second.WorkspaceDir, "config", "user.name", "Test User")
	gitRun(t, second.WorkspaceDir, "config", "user.email", "test@example.test")
	if !unchanged {
		writeGitFile(t, second.WorkspaceDir, "second.txt", "second\n")
		gitRun(t, second.WorkspaceDir, "add", "second.txt")
		gitRun(t, second.WorkspaceDir, "commit", "-m", "second")
	}
	finalizer := AttemptFinalizer{StorageRoot: storage, CampaignRefs: secondWorker.refs, Processes: testProcessRunner{}}
	finalized, err := finalizer.Finalize(ctx, AttemptFinalization{Task: request.Task, Attempt: request.Attempt, WorkspaceDir: second.WorkspaceDir, ExplicitSuccess: true, Repository: repository, BaseCommit: second.Commit, CommitBundles: true})
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, finalized.StorageDir)
	if !finalized.Completion.VerificationPassed {
		t.Fatalf("second failed: %+v", finalized.Completion)
	}
	produced := producedCommit{repository: repository, storage: storage, base: second.Commit, commit: gitOutput(t, second.WorkspaceDir, "rev-parse", "HEAD")}
	for index, artifact := range finalized.Artifacts {
		if artifact.Kind == domain.ArtifactOutput && artifact.Name == "repair" {
			produced.record = artifact
		}
		if artifact.Kind == domain.ArtifactGitState && artifact.Name == CommitBundleArtifactName("repair") {
			produced.bundle = &finalized.Artifacts[index]
		}
	}
	produced.provenance, err = ParseCommitProvenance([]byte(readTestFile(t, storage, produced.record.StoragePath)))
	if err != nil {
		t.Fatal(err)
	}
	if gitOutput(t, repository, "rev-parse", "HEAD") != first.base {
		t.Fatal("unexpected push")
	}
	heads, prerequisites, err := readBundleHeader(filepath.Join(storage, produced.bundle.StoragePath))
	if err != nil {
		t.Fatal(err)
	}
	if len(heads) != 1 || heads[0].commit != produced.commit || len(prerequisites) != 1 || prerequisites[0] != first.base {
		t.Fatalf("bundle must carry unpublished ancestry from public base: heads=%v prerequisites=%v", heads, prerequisites)
	}
	source, err := os.Open(filepath.Join(storage, produced.bundle.StoragePath))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	exported, err := ExportCommitBundle(ctx, produced.provenance, "repair", source, DefaultCommitBundleMaxBytes)
	if err != nil {
		t.Fatalf("second-generation retained commit must export: %v", err)
	}
	defer exported.Close()
	fresh := t.TempDir()
	gitRun(t, fresh, "clone", "--no-local", repository, ".")
	gitRun(t, fresh, "bundle", "verify", exported.Path)
	gitRun(t, fresh, "fetch", exported.Path, "refs/heads/repair:refs/heads/repair")
	if got := gitOutput(t, fresh, "rev-parse", "refs/heads/repair"); got != produced.commit {
		t.Fatalf("export imported %s, want %s", got, produced.commit)
	}
	for _, attempt := range []string{"attempt-3", "attempt-replay"} {
		third, _, err := consume(newCommitWorker(t, storage), produced, "run-3", attempt)
		if err != nil {
			t.Fatalf("third run must start from second run's declared commit without a WIP push: %v", err)
		}
		if third.Commit != produced.commit || gitOutput(t, third.WorkspaceDir, "rev-parse", "HEAD") != produced.commit {
			t.Fatal("third run did not check out exact retained commit")
		}
	}

}
