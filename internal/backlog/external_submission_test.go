package backlog

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func externalSubmissionFixture(t *testing.T) (*SubmissionService, CoordinatorArtifactStore, domain.Artifact, string) {
	t.Helper()
	ctx := context.Background()
	store, root, publication := coordinatorArtifactFixture(t)
	bundles := filepath.Join(filepath.Dir(root), "bundles")
	t.Cleanup(func() { _ = removeIngestedTree(bundles) })
	artifacts := CoordinatorArtifactStore{Root: root, SubmissionRoot: bundles, Catalog: store}
	content := []byte("retained producer output\n")
	setPublicationContent(&publication, content)
	artifact, err := artifacts.Publish(ctx, publication, strings.NewReader(string(content)))
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	records.Workflows = []domain.Workflow{{ID: "workflow-source", Name: "producer"}}
	records.WorkflowRuns = []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-source", Progress: domain.ProgressSucceeded}}
	records.Tasks = []domain.Task{{
		ID: "task-producer", WorkflowID: "workflow-source", Name: "producer",
		Outputs: []domain.ArtifactDeclaration{{Name: artifact.Name, MediaType: artifact.MediaType}},
	}}
	records.Attempts[0].Progress = domain.ProgressSucceeded
	records.Attempts[0].Control = domain.ControlStopped
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	bundle := validBundle(t)
	rewriteBundleManifest(t, bundle, `
version: 2
name: consumer
environment: {project: t3-steward}
tasks:
  inspect:
    prompt_file: prompts/inspect.md
    needs: [run-1/producer]
    inputs_from: {run-1/producer: [result.txt]}
`)
	service := &SubmissionService{
		Store: store, StorageRoot: bundles, Artifacts: &artifacts, MaxBytes: 1 << 20, MaxFiles: 16,
	}
	return service, artifacts, artifact, bundle
}

func TestSubmissionExternalInputsUseCoordinatorArtifactStore(t *testing.T) {
	service, artifacts, source, bundle := externalSubmissionFixture(t)
	ctx := context.Background()
	_, file, err := artifacts.Open(ctx, source.ID)
	if err != nil {
		t.Fatalf("artifact get: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(service.StorageRoot, "objects")); !os.IsNotExist(err) {
		t.Fatalf("bundle root unexpectedly has artifact objects: %v", err)
	}
	result, err := service.SubmitDirectory(ctx, DirectorySubmission{IdempotencyKey: "external-input", BundleDir: bundle})
	if err != nil {
		t.Fatalf("submit consumer: %v", err)
	}
	records, err := service.Store.(coordinatorRecordLoader).LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, artifact := range records.Artifacts {
		if artifact.WorkflowRunID == result.Record.RunID && artifact.Kind == domain.ArtifactInput && artifact.Name == source.Name {
			if artifact.StoragePath != source.StoragePath || artifact.SHA256 != source.SHA256 || artifact.Size != source.Size {
				t.Fatalf("retained input changed source custody: %#v", artifact)
			}
			return
		}
	}
	t.Fatal("consumer did not retain producer output")
}

func TestSubmissionExternalInputsRefuseMissingCoordinatorArtifact(t *testing.T) {
	service, artifacts, source, bundle := externalSubmissionFixture(t)
	if err := os.Remove(filepath.Join(artifacts.Root, filepath.FromSlash(source.StoragePath))); err != nil {
		t.Fatal(err)
	}
	_, err := service.SubmitDirectory(context.Background(), DirectorySubmission{IdempotencyKey: "missing-external-input", BundleDir: bundle})
	if err == nil || !strings.Contains(err.Error(), "is not retrievable") {
		t.Fatalf("missing artifact refusal: %v", err)
	}
}
