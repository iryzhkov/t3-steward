package backlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// rc.117 made ReleaseRun sweep a run's campaign and staging ref namespaces, so
// a ref whose record a crash never wrote is still released. A3's quarantine
// refs live in a third namespace that the sweep, and discardFailedAttempt,
// which worked from records alone, never reached: a failed candidate whose
// record was lost kept its objects in the store forever.
func TestQuarantineRefWithoutRecordIsReleasedAndDiscarded(t *testing.T) {
	ctx := context.Background()
	failed := &FailedCommitAttempt{ID: "attempt", VerificationFailures: []string{"verification command failed (7): exit 7"}}
	publishLost := func(t *testing.T, withOrdinary bool) (CampaignRefStore, string) {
		repository := newGitFixture(t)
		base := gitOutput(t, repository, "rev-parse", "HEAD")
		refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
		if _, err := refs.Publish(ctx, PublishCommitRequest{WorkflowRunID: "run", TaskID: "implement", Name: "candidate", Repository: repository, WorkspaceDir: repository, Base: base, FailedAttempt: failed}, nil); err != nil {
			t.Fatal(err)
		}
		if withOrdinary {
			if _, err := refs.Publish(ctx, PublishCommitRequest{WorkflowRunID: "run", TaskID: "other", Name: "out", Repository: repository, WorkspaceDir: repository, Base: base}, nil); err != nil {
				t.Fatal(err)
			}
		}
		// The record is lost, as a crash between the ref and its record
		// leaves it.
		if err := os.Remove(filepath.Join(refs.Root, "provenance", "run", "implement", "failed", "attempt", "candidate.json")); err != nil {
			t.Fatal(err)
		}
		return refs, filepath.Join(refs.Root, "campaigns.git")
	}
	quarantined := func(t *testing.T, gitDir string) string {
		return gitOutput(t, filepath.Dir(gitDir), "--git-dir", gitDir, "for-each-ref", "--format=%(refname)", "refs/campaigns-quarantine/run/")
	}
	for _, withOrdinary := range []bool{false, true} {
		refs, gitDir := publishLost(t, withOrdinary)
		if quarantined(t, gitDir) == "" {
			t.Fatal("sensitivity: no quarantine ref to release")
		}
		if err := refs.ReleaseRun(ctx, "run", nil); err != nil {
			t.Fatal(err)
		}
		if got := quarantined(t, gitDir); got != "" {
			t.Fatalf("withOrdinary=%t: quarantine ref without record survives ReleaseRun: %s", withOrdinary, got)
		}
		if runs, err := refs.Runs(); err != nil || len(runs) != 0 {
			t.Fatalf("withOrdinary=%t: run still held after release: %v %v", withOrdinary, runs, err)
		}
	}
	refs, gitDir := publishLost(t, false)
	if err := refs.discardFailedAttempt(ctx, "run", "implement", "attempt"); err != nil {
		t.Fatal(err)
	}
	if got := quarantined(t, gitDir); got != "" {
		t.Fatalf("quarantine ref without record survives discardFailedAttempt: %s", got)
	}
}
