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

// Reading runs from refs is in addition to their directories. A repository that
// cannot list its refs, as a crash while it was being created can leave it,
// must not hide the runs the directories name, or no run would ever be
// released again.
func TestRunsOfAnUnreadableRepositoryStillListsRunDirectories(t *testing.T) {
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
	for _, dir := range []string{"campaigns.git", filepath.Join("provenance", "old")} {
		if err := os.MkdirAll(filepath.Join(refs.Root, dir), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if runs, err := refs.Runs(); err != nil || !slices.Equal(runs, []string{"old"}) {
		t.Fatalf("Runs = %v, %v; want [old]", runs, err)
	}
}

// Every ref the store writes names a run and something under it. A ref named
// by its run alone is never written and the release sweep, which works under
// the run's namespace, cannot remove it, so listing it would report a release
// on every cycle that never happens.
func TestRunsIgnoresARefNamedByItsRunAlone(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
	if _, err := refs.Publish(ctx, PublishCommitRequest{WorkflowRunID: "run-1", TaskID: "implement", Name: "candidate", Repository: repository, WorkspaceDir: repository, Base: base}, nil); err != nil {
		t.Fatal(err)
	}
	gitDir := filepath.Join(refs.Root, "campaigns.git")
	gitOutput(t, refs.Root, "--git-dir", gitDir, "update-ref", "refs/campaigns/run", base)
	if runs, err := refs.Runs(); err != nil || !slices.Equal(runs, []string{"run-1"}) {
		t.Fatalf("Runs = %v, %v; want [run-1]", runs, err)
	}
}

// Git warns about a broken ref on standard error and still lists the others.
// That warning names the ref, and reading it as a listed ref name made Runs
// list a run nothing could release and made every release of a run holding a
// broken ref fail trying to delete "warning:".
func TestABrokenRefIsNotReadFromGitsWarning(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
	failed := &FailedCommitAttempt{ID: "attempt", VerificationFailures: []string{"verification command failed (7): exit 7"}}
	for _, run := range []string{"good", "held"} {
		if _, err := refs.Publish(ctx, PublishCommitRequest{WorkflowRunID: run, TaskID: "t", Name: "n", Repository: repository, WorkspaceDir: repository, Base: base, FailedAttempt: failed}, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, run := range []string{"crashed", "held"} {
		broken := filepath.Join(refs.Root, "campaigns.git", "refs", "campaigns-quarantine", run, "t", "broken")
		if err := os.MkdirAll(broken, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(broken, "n"), []byte("garbage\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if runs, err := refs.Runs(); err != nil || !slices.Equal(runs, []string{"good", "held"}) {
		t.Errorf("Runs = %v, %v; want [good held]", runs, err)
	}
	if err := refs.ReleaseRun(ctx, "held", nil); err != nil {
		t.Errorf("release of a run holding a broken ref: %v", err)
	}
	if err := refs.discardFailedAttempt(ctx, "crashed", "t", "broken"); err != nil {
		t.Errorf("discard of an attempt holding a broken ref: %v", err)
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
