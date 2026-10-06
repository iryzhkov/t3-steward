package backlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestWorkspacePreparerMaterializesCarriedOnlyDependencies(t *testing.T) {
	repository := newGitFixture(t)
	storage := t.TempDir()
	reference := storedOutputArtifact(t, storage, "producer-id", "attempt-p", "handoff.md", "carried handoff")
	reference.ID = "carried"
	reference.TaskID = "consumer-id"
	reference.AttemptID = ""
	reference.Kind = domain.ArtifactInput
	task := workspaceTask("consumer-id", "consumer")
	task.CarriedInputs = []domain.CarriedInput{{Producer: "producer", ProducerTaskID: "producer-id", Name: "handoff.md", ArtifactID: reference.ID}}
	request := workspaceRequest(repository, gitOutput(t, repository, "rev-parse", "HEAD"), task, "attempt-c")
	request.DependencyArtifacts = []domain.Artifact{reference}
	prepared, err := workspacePreparer(t.TempDir(), storage).Prepare(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, prepared.RootDir)
	got, err := os.ReadFile(filepath.Join(prepared.WorkspaceDir, ".t3", "dependencies", "producer", "handoff.md"))
	if err != nil || string(got) != "carried handoff" {
		t.Fatalf("carried-only input=%q error=%v", got, err)
	}
}
