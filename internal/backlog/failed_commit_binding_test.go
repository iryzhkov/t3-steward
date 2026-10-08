package backlog

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestFailedCommitOrdinaryDependencyRequiresSourceAttemptBinding(t *testing.T) {
	ctx := context.Background()
	repo := newGitFixture(t)
	base := gitOutput(t, repo, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: t.TempDir()}
	p, err := refs.Publish(ctx, PublishCommitRequest{WorkflowRunID: "run-1", TaskID: "producer", Name: "candidate", Repository: repo, WorkspaceDir: repo, Base: base, FailedAttempt: &FailedCommitAttempt{ID: "failed-attempt", VerificationFailures: []string{"verification command failed (1): false"}}}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	data, err := MarshalCommitProvenance(p)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := t.TempDir()
	if err := os.Mkdir(filepath.Join(dependencies, "ordinary-producer"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dependencies, "ordinary-producer", "ordinary-file.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	preparer := WorkspacePreparer{CampaignRefs: refs}
	request := workspaceRequest(repo, "main", workspaceTask("consumer", "consumer"), "consumer-attempt")
	for _, source := range []DependencySource{
		{},
		{WorkflowRunID: "run-1", TaskID: "producer"},
		{WorkflowRunID: "run-1", TaskID: "producer", AttemptID: "other-attempt"},
		{WorkflowRunID: "run-1", TaskID: "other-producer", AttemptID: "failed-attempt"},
		{WorkflowRunID: "other-run", TaskID: "producer", AttemptID: "failed-attempt"},
	} {
		request.DependencySources = map[string]DependencySource{"ordinary-producer": source}
		if err := preparer.resolveDependencyCommits(ctx, dependencies, repo, request, io.Discard); err == nil {
			t.Fatalf("failed candidate accepted without its exact source binding: %+v", source)
		}
	}
	request.DependencySources = map[string]DependencySource{"ordinary-producer": {WorkflowRunID: "run-1", TaskID: "producer", AttemptID: "failed-attempt"}}
	for _, consumingRun := range []string{"run-1", "rerun"} {
		request.WorkflowRunID = consumingRun
		if err := preparer.resolveDependencyCommits(ctx, dependencies, repo, request, io.Discard); err != nil {
			t.Fatalf("explicit bound reuse in %s refused: %v", consumingRun, err)
		}
	}
}
