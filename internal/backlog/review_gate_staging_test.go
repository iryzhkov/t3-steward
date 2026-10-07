package backlog

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// assertFinalizedHeadIsPhysical imports a review-gated finalization's own
// HEAD artifact and commit provenance through the coordinator and requires
// both the artifact and the coordinator's decision to agree with the
// workspace as it is after collection.
func assertFinalizedHeadIsPhysical(t *testing.T, f *reviewGateFixture, finalized FinalizedAttempt, repository, accepted string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(finalized.StorageDir, "artifacts", WorkspaceHeadArtifactName))
	if err != nil {
		t.Fatal(err)
	}
	captured, err := ParseWorkspaceHead(raw)
	if err != nil {
		t.Fatal(err)
	}
	actual := CaptureWorkspaceHead(context.Background(), "", repository, nil)
	if captured.Head != actual.Head || captured.Dirty != actual.Dirty {
		t.Fatalf("finalized HEAD is not the workspace after collection: captured=%+v actual=%+v", captured, actual)
	}
	provenance := finalizedCommitProvenance(t, finalized, "change")
	attempt := f.collect(t, f.store, &captured, provenance.Commit)
	physicalAccepted := actual.Head == accepted && !actual.Dirty && actual.Error == ""
	if succeeded := attempt.Progress == domain.ProgressSucceeded; succeeded != physicalAccepted {
		t.Fatalf("completion disagrees with the workspace: actual=%+v gate=%+v progress=%s", actual, attempt.ReviewGate, attempt.Progress)
	}
}

// installPrePushHook makes the workspace's pre-push hook run script and leave
// a marker, and returns the marker's path.
func installPrePushHook(t *testing.T, repository, script string) string {
	t.Helper()
	marker := filepath.Join(t.TempDir(), "hook-ran")
	hook := gitOutput(t, repository, "rev-parse", "--path-format=absolute", "--git-path", "hooks/pre-push")
	if err := os.MkdirAll(filepath.Dir(hook), 0o700); err != nil {
		t.Fatal(err)
	}
	body := "#!/bin/sh\ntouch '" + marker + "'\n" + script
	if err := os.WriteFile(hook, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	return marker
}

func assertNotExist(t *testing.T, path, what string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("%s: stat %s = %v", what, path, err)
	}
}

// The review's reproduction: staging a declared commit ran the workspace's
// pre-push hook after the HEAD report was captured, so a hook that edits
// tracked source completed against stale clean evidence.
func TestReviewRound4PrePushMutatesAfterCapture(t *testing.T) {
	repository, head := workspaceHeadRepository(t)
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	f := newReviewGateFixture(t, true, true)
	f.openRound(t, "cp-1", head, "accept")
	marker := installPrePushHook(t, repository, "printf 'unreviewed source\\n' > main.go\n")
	f.finishTurn(t)
	finalized := reviewGatedFinalize(t, f, refs, repository, head)
	assertNotExist(t, marker, "staging ran the workspace's pre-push hook")
	assertFinalizedHeadIsPhysical(t, f, finalized, repository, head)
}

// The same hook committing its edit moves HEAD after the capture, while the
// staged provenance still names the accepted commit.
func TestReviewRound4PrePushCommitsAfterCapture(t *testing.T) {
	repository, head := workspaceHeadRepository(t)
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	f := newReviewGateFixture(t, true, true)
	f.openRound(t, "cp-1", head, "accept")
	marker := installPrePushHook(t, repository, "printf 'unreviewed source\\n' > main.go\n"+
		"git -c user.name=t -c user.email=t@example.invalid commit -qam unreviewed\n")
	f.finishTurn(t)
	finalized := reviewGatedFinalize(t, f, refs, repository, head)
	assertNotExist(t, marker, "staging ran the workspace's pre-push hook")
	assertFinalizedHeadIsPhysical(t, f, finalized, repository, head)
}

// Whatever runs in the workspace while declared commits are staged, the HEAD
// report describes the workspace after it. The Git binary here edits tracked
// source after every command, standing in for any code staging may run.
func TestFinalizeCapturesWorkspaceHeadAfterStagingCommits(t *testing.T) {
	repository, head := workspaceHeadRepository(t)
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	wrapper := filepath.Join(t.TempDir(), "git")
	script := "#!/bin/sh\n'" + gitPath + "' \"$@\"\nstatus=$?\nprintf 'unreviewed source\\n' > '" +
		filepath.Join(repository, "main.go") + "'\nexit $status\n"
	if err := os.WriteFile(wrapper, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs"), GitBinary: wrapper}
	f := newReviewGateFixture(t, true, true)
	f.openRound(t, "cp-1", head, "accept")
	f.finishTurn(t)
	finalized := reviewGatedFinalize(t, f, refs, repository, head)
	if actual := CaptureWorkspaceHead(context.Background(), "", repository, nil); !actual.Dirty {
		t.Fatalf("fixture: staging did not change source: %+v", actual)
	}
	assertFinalizedHeadIsPhysical(t, f, finalized, repository, head)
}

// Copying a declared commit into the store runs nothing the producing
// workspace configures: no hook, and no receive-pack command named for the
// store's path.
func TestCampaignRefStoreRunsNoWorkspaceCommand(t *testing.T) {
	for _, mode := range []string{"stage", "publish"} {
		t.Run(mode, func(t *testing.T) {
			repository := newGitFixture(t)
			base := gitOutput(t, repository, "rev-parse", "HEAD")
			refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
			gitDir := filepath.Join(refs.Root, "campaigns.git")
			markers := t.TempDir()
			hooks := filepath.Join(t.TempDir(), "hooks")
			if err := os.MkdirAll(hooks, 0o700); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"pre-push", "reference-transaction", "post-update", "push-to-checkout"} {
				body := "#!/bin/sh\ntouch '" + filepath.Join(markers, name) + "'\n"
				if err := os.WriteFile(filepath.Join(hooks, name), []byte(body), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			gitRun(t, repository, "config", "core.hooksPath", hooks)
			receive := filepath.Join(t.TempDir(), "receive-pack")
			if err := os.WriteFile(receive, []byte("#!/bin/sh\ntouch '"+filepath.Join(markers, "receive-pack")+"'\nexec git-receive-pack \"$@\"\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			gitRun(t, repository, "config", "remote."+gitDir+".receivepack", receive)
			request := PublishCommitRequest{
				WorkflowRunID: "run-1", TaskID: "task-1", Name: "change", Repository: repository,
				WorkspaceDir: repository, Base: base,
			}
			var provenance CommitProvenance
			var err error
			ref := CampaignRef("run-1", "task-1", "change")
			if mode == "stage" {
				provenance, err = refs.Stage(context.Background(), request, "attempt-1", nil)
				ref = StagedCampaignRef("run-1", "task-1", "attempt-1", "change")
			} else {
				provenance, err = refs.Publish(context.Background(), request, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := gitOutput(t, gitDir, "rev-parse", ref); got != base || provenance.Commit != base {
				t.Fatalf("ref %s = %s, provenance %s, want %s", ref, got, provenance.Commit, base)
			}
			if ran, err := os.ReadDir(markers); err != nil || len(ran) != 0 {
				names := make([]string, 0, len(ran))
				for _, entry := range ran {
					names = append(names, entry.Name())
				}
				t.Fatalf("workspace commands ran while copying the commit: %s %v", strings.Join(names, ", "), err)
			}
		})
	}
}

// A declared commit made in a linked worktree, or named from a subdirectory of
// the workspace, is still copied: the store fetches from the repository that
// holds the workspace's objects.
func TestCampaignRefStoreCopiesFromALinkedWorktreeSubdirectory(t *testing.T) {
	repository := newGitFixture(t)
	worktree := filepath.Join(t.TempDir(), "linked")
	gitRun(t, repository, "worktree", "add", "-q", "--detach", worktree)
	gitRun(t, worktree, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "--allow-empty", "-m", "linked only")
	commit := gitOutput(t, worktree, "rev-parse", "HEAD")
	subdirectory := filepath.Join(worktree, "nested")
	if err := os.MkdirAll(subdirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	provenance, err := refs.Stage(context.Background(), PublishCommitRequest{
		WorkflowRunID: "run-1", TaskID: "task-1", Name: "change", Repository: repository,
		WorkspaceDir: subdirectory, Base: commit,
	}, "attempt-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	staged := StagedCampaignRef("run-1", "task-1", "attempt-1", "change")
	if got := gitOutput(t, filepath.Join(refs.Root, "campaigns.git"), "rev-parse", staged); got != commit || provenance.Commit != commit {
		t.Fatalf("staged %s, provenance %s, want %s", got, provenance.Commit, commit)
	}
}
