package backlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestChainedCheckoutRejectsReservedIdentitySymlink(t *testing.T) {
	repository, storage := newGitFixture(t), t.TempDir()
	produced := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{edit: func(t *testing.T, w string) {
		target := t.TempDir()
		if err := os.Symlink(target, filepath.Join(w, domain.TaskIdentityDir)); err != nil {
			t.Fatal(err)
		}
		gitRun(t, w, "add", domain.TaskIdentityDir)
	}})
	namespace := externalProducerNamespace("run-1", domain.Task{ID: "task-producer", Name: "producer"})
	task := workspaceTask("consumer", "consumer")
	task.Needs = []string{namespace}
	task.DependencyInputs = map[string][]string{namespace: {"repair"}}
	request := workspaceRequest(repository, "campaign:run-1/producer/repair", task, "attempt-reserved")
	request.WorkflowRunID, request.Attempt.WorkflowRunID = "run-2", "run-2"
	record := produced.record
	record.WorkflowRunID, record.TaskID, record.AttemptID = "run-2", "task-producer", ""
	request.DependencyTasks = []domain.Task{{ID: "task-producer", Name: namespace, Outputs: []domain.ArtifactDeclaration{{Name: "repair", Commit: &domain.CommitOutput{}}}}}
	request.DependencyArtifacts = []domain.Artifact{record}
	request.DependencySources = map[string]DependencySource{namespace: {WorkflowRunID: "run-1", TaskID: "task-producer", AttemptID: "attempt-1"}}
	request.CommitBundles = map[string]CommitBundleDelivery{produced.provenance.Ref: produced.delivery(t, nil)}
	prepared, err := newCommitWorker(t, storage).preparer.Prepare(context.Background(), request)
	if err == nil {
		cleanupImmutable(t, prepared.RootDir)
		t.Fatal("chained preparation published repository-provided identity symlink")
	}
}
