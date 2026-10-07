package backlog

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFailedCommitRecordWriteRollback(t *testing.T) {
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
	blocked := filepath.Join(refs.Root, "provenance", "run", "implement", "failed", "attempt", "candidate.json")
	if err := os.MkdirAll(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	task := domain.Task{ID: "implement", Name: "implement", Outputs: []domain.ArtifactDeclaration{{Name: "candidate", Commit: &domain.CommitOutput{}}}, Verification: []string{"exit 7"}}
	request := AttemptFinalization{Task: task, Attempt: domain.Attempt{ID: "attempt", WorkflowRunID: "run", TaskID: task.ID, Number: 1}, WorkspaceDir: repository, ExplicitSuccess: true, Repository: repository, BaseCommit: base, CommitBundles: true}
	result, err := (AttemptFinalizer{StorageRoot: t.TempDir(), CampaignRefs: refs, Processes: testProcessRunner{}}).Finalize(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, result.StorageDir)
	if !strings.Contains(result.Completion.Failure, "declared commit") {
		t.Fatalf("missing storage failure: %+v", result.Completion)
	}
	if err := refs.ReleaseRun(context.Background(), "run", nil); err != nil {
		t.Fatal(err)
	}
	got := gitOutput(t, refs.Root, "--git-dir", filepath.Join(refs.Root, "campaigns.git"), "for-each-ref", "--format=%(refname)", "refs/campaigns-quarantine/run/")
	if got != "" {
		t.Fatalf("quarantine ref survives non-verification publish failure and ReleaseRun: %s; failure=%s", got, result.Completion.Failure)
	}
}
