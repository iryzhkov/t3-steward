package backlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// lazyFetchWorkspace returns a workspace the executor has configured as a
// partial clone whose promisor remote's upload-pack runs payload, with its
// HEAD commit object deleted so that reading HEAD would fetch it, and HEAD.
func lazyFetchWorkspace(t *testing.T, payload string) (string, string) {
	t.Helper()
	source := newGitFixture(t)
	workspace := filepath.Join(t.TempDir(), "workspace")
	gitRun(t, filepath.Dir(workspace), "clone", "-q", "--no-local", source, workspace)
	gitRun(t, workspace, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "--allow-empty", "-m", "work")
	head := gitOutput(t, workspace, "rev-parse", "HEAD")
	if err := os.Remove(filepath.Join(workspace, ".git", "objects", head[:2], head[2:])); err != nil {
		t.Fatal(err)
	}
	gitRun(t, workspace, "config", "core.repositoryformatversion", "1")
	gitRun(t, workspace, "config", "extensions.partialClone", "origin")
	gitRun(t, workspace, "config", "remote.origin.promisor", "true")
	gitRun(t, workspace, "config", "remote.origin.uploadpack", payload+"; git-upload-pack")
	gitRun(t, workspace, "config", "protocol.allow", "always")
	return workspace, head
}

// Reading the workspace's HEAD must not make Git fetch a missing object from
// a remote the executor configured, because that remote's upload-pack is the
// executor's code running as the worker after verification.
func TestCaptureWorkspaceHeadFetchesNoMissingObject(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	workspace, _ := lazyFetchWorkspace(t, "touch '"+marker+"'")
	captured := CaptureWorkspaceHead(context.Background(), "", workspace, nil)
	assertNotExist(t, marker, "capture ran the workspace's promisor upload-pack")
	if captured.Error == "" {
		t.Fatalf("capture with a missing HEAD object did not fail: %+v", captured)
	}
}

func TestCampaignRefStoreFetchesNoMissingWorkspaceObject(t *testing.T) {
	for _, mode := range []string{"stage", "publish"} {
		t.Run(mode, func(t *testing.T) {
			marker := filepath.Join(t.TempDir(), "ran")
			workspace, head := lazyFetchWorkspace(t, "touch '"+marker+"'")
			refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
			request := PublishCommitRequest{
				WorkflowRunID: "run-1", TaskID: "task-1", Name: "change", Repository: workspace,
				WorkspaceDir: workspace, Base: head,
			}
			var err error
			if mode == "stage" {
				_, err = refs.Stage(context.Background(), request, "attempt-1", nil)
			} else {
				_, err = refs.Publish(context.Background(), request, nil)
			}
			assertNotExist(t, marker, "copying the commit ran the workspace's promisor upload-pack")
			if err == nil {
				t.Fatal("a commit whose object is missing was copied")
			}
		})
	}
}

// Git declines to update a ref from a shallow source whose history the store
// lacks, and still exits successfully. That is a failure to keep the declared
// commit, not a success.
func TestCampaignRefStoreRefusesACommitItCouldNotStore(t *testing.T) {
	for _, mode := range []string{"stage", "publish"} {
		t.Run(mode, func(t *testing.T) {
			source := newGitFixture(t)
			gitRun(t, source, "commit", "-q", "--allow-empty", "-m", "second")
			workspace := filepath.Join(t.TempDir(), "workspace")
			gitRun(t, filepath.Dir(workspace), "clone", "-q", "--depth", "1", "file://"+source, workspace)
			base := gitOutput(t, workspace, "rev-parse", "HEAD")
			gitRun(t, workspace, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "--allow-empty", "-m", "work")
			refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
			request := PublishCommitRequest{
				WorkflowRunID: "run-1", TaskID: "task-1", Name: "change", Repository: source,
				WorkspaceDir: workspace, Base: base,
			}
			var err error
			if mode == "stage" {
				_, err = refs.Stage(context.Background(), request, "attempt-1", nil)
			} else {
				_, err = refs.Publish(context.Background(), request, nil)
				if _, resolveErr := refs.Resolve("run-1", "task-1", "change"); resolveErr == nil {
					t.Error("a provenance record names a ref the store does not hold")
				}
			}
			if err == nil {
				t.Fatalf("%s reported success without storing the commit", mode)
			}
		})
	}
}
