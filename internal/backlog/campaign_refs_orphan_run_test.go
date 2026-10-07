package backlog

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// Publish installs a run's ref before it writes the run's first record, so a
// crash between the two on the run's first publication leaves a ref and no run
// directory at all. Runs listed runs from their directories alone, and
// ReleaseRun returned early for a run without one, so such a ref, and the
// commit it pins, was never released.
func TestRunWithOnlyAnOrphanRefIsListedAndReleased(t *testing.T) {
	ctx := context.Background()
	failed := &FailedCommitAttempt{ID: "attempt", VerificationFailures: []string{"verification command failed (7): exit 7"}}
	cases := []struct {
		name      string
		failed    *FailedCommitAttempt
		namespace string
		stage     bool
	}{
		{"quarantine", failed, "refs/campaigns-quarantine/run/", false},
		{"campaign", nil, "refs/campaigns/run/", false},
		{"staged", nil, "refs/campaign-staged/run/", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repository := newGitFixture(t)
			base := gitOutput(t, repository, "rev-parse", "HEAD")
			refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
			request := PublishCommitRequest{WorkflowRunID: "run", TaskID: "implement", Name: "candidate", Repository: repository, WorkspaceDir: repository, Base: base, FailedAttempt: tc.failed}
			var err error
			if tc.stage {
				_, err = refs.Stage(ctx, request, "attempt", nil)
			} else {
				_, err = refs.Publish(ctx, request, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			// The state a crash after the ref and before the run's first
			// record leaves: the ref, and neither run directory nor record.
			for _, kind := range []string{"provenance", "staged"} {
				if err := os.RemoveAll(filepath.Join(refs.Root, kind, "run")); err != nil {
					t.Fatal(err)
				}
			}
			gitDir := filepath.Join(refs.Root, "campaigns.git")
			listed := func() string {
				return gitOutput(t, refs.Root, "--git-dir", gitDir, "for-each-ref", "--format=%(refname)", tc.namespace)
			}
			if listed() == "" {
				t.Fatal("sensitivity: no orphan ref to release")
			}
			runs, err := refs.Runs()
			if err != nil || !slices.Equal(runs, []string{"run"}) {
				// Not fatal: an explicit ReleaseRun must sweep the ref too.
				t.Errorf("Runs with only an orphan ref = %v, %v; want [run]", runs, err)
			}
			if err := refs.ReleaseRun(ctx, "run", nil); err != nil {
				t.Fatal(err)
			}
			if got := listed(); got != "" {
				t.Fatalf("orphan ref survives ReleaseRun: %s", got)
			}
			if runs, err := refs.Runs(); err != nil || len(runs) != 0 {
				t.Fatalf("run still held after release: %v %v", runs, err)
			}
		})
	}
}

// A store that never opened its repository lists no runs and creates nothing.
func TestRunsOfAnUnopenedStoreCreatesNothing(t *testing.T) {
	root := filepath.Join(t.TempDir(), "refs")
	refs := CampaignRefStore{Root: root}
	if runs, err := refs.Runs(); err != nil || len(runs) != 0 {
		t.Fatalf("Runs = %v, %v", runs, err)
	}
	if err := refs.ReleaseRun(context.Background(), "run", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(root); !os.IsNotExist(err) {
		t.Fatalf("store root was created: %v", err)
	}
}
