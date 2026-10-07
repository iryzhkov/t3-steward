package workerruntime

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func collectWorkspaceHead(t *testing.T, pkg workerproto.ExecutionPackage, workspace string) PublishedResult {
	t.Helper()
	control := &recordingT3{
		thread:  &domain.Thread{ID: pkg.Identity.ThreadID, TurnState: "completed"},
		message: "done",
		archive: []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed",` +
			`"startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},` +
			`"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`),
	}
	publisher := &recordingPublisher{}
	driver := &LocalDriver{
		T3: control, Publisher: publisher, Now: func() time.Time { return runtimeTestNow },
		Finalizer: backlog.AttemptFinalizer{
			StorageRoot: filepath.Join(artifactTestRoot(t), "artifacts"),
			Processes:   &countingProcessRunner{}, Now: func() time.Time { return runtimeTestNow },
			NewID: func(string) string { return "verification-1" },
		},
	}
	if err := driver.Collect(context.Background(), pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if len(publisher.results) != 1 {
		t.Fatalf("published %d results", len(publisher.results))
	}
	return publisher.results[0]
}

func workspaceHeadArtifact(t *testing.T, result PublishedResult) (domain.Artifact, bool) {
	t.Helper()
	for _, artifact := range result.Finalized.Artifacts {
		if artifact.Kind == domain.ArtifactGitState {
			return artifact, true
		}
	}
	return domain.Artifact{}, false
}

// A package that requires the workspace HEAD gets it in its result: the
// physical HEAD and the tracked changes as the turn left them, captured before
// verification runs. A package that does not is collected exactly as before.
func TestCollectRecordsWorkspaceHeadOnlyWhenThePackageRequiresIt(t *testing.T) {
	gitDir, head := makeGitRepository(t)
	workspace := filepath.Dir(gitDir)
	if err := os.WriteFile(filepath.Join(workspace, "README.md"), []byte("changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	pkg := testPackage()
	pkg.Verification = nil
	if _, found := workspaceHeadArtifact(t, collectWorkspaceHead(t, pkg, workspace)); found {
		t.Fatal("a package without the capability published workspace HEAD evidence")
	}

	pkg.RequiredCapabilities = []string{workerproto.PackageCapabilityWorkspaceHead}
	result := collectWorkspaceHead(t, pkg, workspace)
	artifact, found := workspaceHeadArtifact(t, result)
	if !found || artifact.ID != backlog.WorkspaceHeadArtifactID(pkg.Identity.AttemptID) || artifact.Name != backlog.WorkspaceHeadArtifactName || artifact.MediaType != "application/json" {
		t.Fatalf("workspace head artifact = %+v found=%v", artifact, found)
	}
	raw, err := os.ReadFile(filepath.Join(result.Finalized.StorageDir, "artifacts", filepath.FromSlash(backlog.WorkspaceHeadArtifactName)))
	if err != nil {
		t.Fatal(err)
	}
	captured, err := backlog.ParseWorkspaceHead(raw)
	if err != nil {
		t.Fatal(err)
	}
	if captured.Head != head || !captured.Dirty || !slices.Contains(captured.DirtyPaths, "README.md") {
		t.Fatalf("captured = %+v, want head %s dirty README.md", captured, head)
	}
	if out, err := exec.Command("git", "-C", workspace, "status", "--porcelain").Output(); err != nil || !strings.Contains(string(out), "README.md") {
		t.Fatalf("collection changed the workspace: %q %v", out, err)
	}
}
