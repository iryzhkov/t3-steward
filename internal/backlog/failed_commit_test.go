package backlog

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Failed verification may retain a candidate, but never publish an ordinary output.
func TestVerificationFailedCommitRetainedSeparately(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	consumer := t.TempDir()
	gitRun(t, consumer, "clone", "--quiet", repository, ".")
	writeGitFile(t, repository, "change.txt", "candidate\n")
	gitRun(t, repository, "add", "change.txt")
	gitRun(t, repository, "commit", "-m", "candidate")
	candidate := gitOutput(t, repository, "rev-parse", "HEAD")
	storage := t.TempDir()
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
	task := domain.Task{ID: "implement", Name: "implement", Outputs: []domain.ArtifactDeclaration{{Name: "candidate", Commit: &domain.CommitOutput{}}}, Verification: []string{"exit 7"}}
	attempt := domain.Attempt{ID: "attempt-failed", WorkflowRunID: "run-failed", TaskID: task.ID, Number: 1}
	finalizer := AttemptFinalizer{StorageRoot: storage, CampaignRefs: refs, Processes: testProcessRunner{}}
	request := AttemptFinalization{Task: task, Attempt: attempt, WorkspaceDir: repository, ExplicitSuccess: true, Repository: repository, BaseCommit: base, CommitBundles: true, FailedCommits: true}
	result, err := finalizer.Finalize(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, result.StorageDir)
	var record *domain.Artifact
	var bundle *domain.Artifact
	for i := range result.Artifacts {
		a := &result.Artifacts[i]
		if a.Kind == domain.ArtifactOutput {
			t.Fatalf("failed commit became ordinary output: %+v", a)
		}
		if a.Kind == domain.ArtifactGitState && a.MediaType == "application/json" {
			record = a
		}
		if a.MediaType == CommitBundleMediaType {
			bundle = a
		}
	}
	if record == nil || bundle == nil {
		t.Fatalf("missing failed commit record or cross-host bundle: %+v", result.Artifacts)
	}
	p, err := ParseCommitProvenance([]byte(readTestFile(t, storage, record.StoragePath)))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(p)
	if !strings.Contains(string(raw), "attempt-failed") || !strings.Contains(string(raw), "verification command failed (7): exit 7") {
		t.Fatalf("missing failure provenance: %s", raw)
	}
	if p.Commit != candidate || !strings.Contains(p.Ref, "campaigns-quarantine/") {
		t.Fatalf("wrong failed ref: %+v", p)
	}
	if _, err := refs.Resolve(attempt.WorkflowRunID, task.ID, "candidate"); err == nil {
		t.Fatal("failed commit satisfies ordinary Resolve")
	}
	// A host with only the base obtains the candidate using the same transport.
	if other := (CampaignRefStore{}); other.hasCommit(ctx, consumer, candidate, nil) {
		t.Fatal("consumer already contains the candidate")
	}
	delivery := CommitBundleDelivery{SHA256: bundle.SHA256, Size: bundle.Size, Open: func(context.Context) (io.ReadCloser, error) {
		return os.Open(filepath.Join(storage, filepath.FromSlash(bundle.StoragePath)))
	}}
	other := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
	if err := other.Obtain(ctx, consumer, p, &delivery, nil); err != nil {
		t.Fatal(err)
	}
	if err := other.FetchInto(ctx, consumer, p, nil); err != nil {
		t.Fatal(err)
	}
	if got := gitOutput(t, consumer, "rev-parse", p.Ref); got != candidate {
		t.Fatalf("received %s want %s", got, candidate)
	}
	// A retry can publish its own normal ref.
	writeGitFile(t, repository, "change.txt", "retry\n")
	gitRun(t, repository, "add", "change.txt")
	gitRun(t, repository, "commit", "-m", "retry")
	request.Task.Verification = []string{"true"}
	request.Attempt.ID = "attempt-retry"
	retry, err := finalizer.Finalize(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, retry.StorageDir)
	if !retry.Completion.VerificationPassed {
		t.Fatalf("retry failed: %+v", retry.Completion)
	}
	normal, err := refs.Resolve(attempt.WorkflowRunID, task.ID, "candidate")
	if err != nil || normal.Commit == candidate {
		t.Fatalf("retry ordinary ref: %+v %v", normal, err)
	}
	if err := refs.ReleaseRun(ctx, attempt.WorkflowRunID, nil); err != nil {
		t.Fatal(err)
	}
	if records, err := refs.List(attempt.WorkflowRunID); err != nil || len(records) != 0 {
		t.Fatalf("failed refs not released: %+v %v", records, err)
	}
}

func TestVerificationFailureWithUnresolvableCommitRetainsNoCandidates(t *testing.T) {
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
	task := domain.Task{ID: "implement", Name: "implement", Outputs: []domain.ArtifactDeclaration{{Name: "valid", Commit: &domain.CommitOutput{}}, {Name: "invalid", Commit: &domain.CommitOutput{Revision: "does-not-exist"}}}, Verification: []string{"exit 7"}}
	result, err := (AttemptFinalizer{StorageRoot: t.TempDir(), CampaignRefs: refs, Processes: testProcessRunner{}}).Finalize(context.Background(), AttemptFinalization{Task: task, Attempt: domain.Attempt{ID: "attempt", WorkflowRunID: "run", TaskID: task.ID, Number: 1}, WorkspaceDir: repository, ExplicitSuccess: true, Repository: repository, BaseCommit: base, CommitBundles: true, FailedCommits: true})
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, result.StorageDir)
	if records, err := refs.List("run"); err != nil || len(records) != 0 {
		t.Fatalf("resolution failure retained candidates: %+v %v", records, err)
	}
	for _, a := range result.Artifacts {
		if a.Kind == domain.ArtifactGitState {
			t.Fatalf("resolution failure retained artifact %+v", a)
		}
	}
}

func TestVerificationFailureWithMissingFileRetainsNoCommit(t *testing.T) {
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	writeGitFile(t, repository, "change.txt", "candidate\n")
	gitRun(t, repository, "add", "change.txt")
	gitRun(t, repository, "commit", "-m", "candidate")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
	task := domain.Task{ID: "implement", Name: "implement", Outputs: []domain.ArtifactDeclaration{{Name: "candidate", Commit: &domain.CommitOutput{}}, {Name: "missing.txt"}}, Verification: []string{"exit 7"}}
	result, err := (AttemptFinalizer{StorageRoot: t.TempDir(), CampaignRefs: refs, Processes: testProcessRunner{}}).Finalize(context.Background(), AttemptFinalization{Task: task, Attempt: domain.Attempt{ID: "attempt", WorkflowRunID: "run", TaskID: task.ID, Number: 1}, WorkspaceDir: repository, ExplicitSuccess: true, Repository: repository, BaseCommit: base, CommitBundles: true, FailedCommits: true})
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, result.StorageDir)
	if records, err := refs.List("run"); err != nil || len(records) != 0 {
		t.Fatalf("nonverification failure retained commit: %+v %v", records, err)
	}
}
