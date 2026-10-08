package backlog

import (
	"context"
	"io"
	"path/filepath"
	"testing"
)

// rc.117 made a consumer fetch an unpublished commit from its staging, or
// promote it for an accepted consumer. A retained failed candidate is neither
// published nor staged: it lives under its attempt's quarantine ref, and is
// fetched from there by every consumer without ever becoming the task's
// campaign output.
func TestFailedCommitFetchUsesQuarantineRefAndNeverPromotes(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	writeGitFile(t, repository, "change.txt", "candidate\n")
	gitRun(t, repository, "add", "change.txt")
	gitRun(t, repository, "commit", "-m", "candidate")
	candidate := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
	failed, err := refs.Publish(ctx, PublishCommitRequest{
		WorkflowRunID: "run", TaskID: "producer", Name: "candidate", Repository: repository,
		WorkspaceDir: repository, Base: base,
		FailedAttempt: &FailedCommitAttempt{ID: "attempt-1", VerificationFailures: []string{"verification command failed (1): false"}},
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	for _, accepted := range []bool{false, true} {
		consumer := t.TempDir()
		gitRun(t, consumer, "clone", "--quiet", repository, ".")
		fetch := refs.FetchInto
		if accepted {
			fetch = refs.FetchAcceptedInto
		}
		if err := fetch(ctx, consumer, failed, io.Discard); err != nil {
			t.Fatalf("accepted=%t: %v", accepted, err)
		}
		if got := gitOutput(t, consumer, "rev-parse", failed.Ref); got != candidate {
			t.Fatalf("accepted=%t: consumer has %s at %s, want %s", accepted, got, failed.Ref, candidate)
		}
	}
	if _, err := refs.Resolve("run", "producer", "candidate"); err == nil {
		t.Fatal("fetching a failed candidate published it as the task's campaign output")
	}
	gitDir, err := refs.open(ctx, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := refs.head(ctx, gitDir, CampaignRef("run", "producer", "candidate"), io.Discard); err != nil || found {
		t.Fatalf("campaign ref of a failed candidate exists=%t err=%v", found, err)
	}
	// The bundle of a failed candidate names its quarantine ref.
	if ref, err := bundleRef(failed); err != nil || ref != failed.Ref {
		t.Fatalf("bundle ref %q err %v, want %q", ref, err, failed.Ref)
	}
	both := failed
	both.StagedAttempt = "attempt-1"
	if _, err := bundleRef(both); err == nil {
		t.Fatal("a record naming both a failed attempt and a staging was bundled")
	}
}
