package workerruntime

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// mutatingVerificationRunner is a verification command that succeeds after
// rewriting a tracked file, as a generator or formatter does.
type mutatingVerificationRunner struct{ workspace string }

func (r *mutatingVerificationRunner) Run(_ context.Context, request backlog.ProcessRequest) (backlog.ProcessResult, error) {
	if err := os.WriteFile(filepath.Join(r.workspace, "README.md"), []byte("verification changed tracked source\n"), 0o600); err != nil {
		return backlog.ProcessResult{}, err
	}
	if request.Log != nil {
		_, _ = request.Log.Write([]byte("ok\n"))
	}
	return backlog.ProcessResult{ExitCode: 0}, nil
}

func (r *mutatingVerificationRunner) Kill(string) error { return nil }

// The workspace HEAD report describes the tree after verification, so source a
// verification command changed is unreviewed work the gate sees.
func TestCollectCapturesWorkspaceHeadAfterVerification(t *testing.T) {
	gitDir, head := makeGitRepository(t)
	workspace := filepath.Dir(gitDir)
	pkg := testPackage()
	pkg.RequiredCapabilities = []string{workerproto.PackageCapabilityWorkspaceHead}
	pkg.Verification = []string{"fixture mutation"}
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
			Processes:   &mutatingVerificationRunner{workspace: workspace}, Now: func() time.Time { return runtimeTestNow },
			NewID: func(string) string { return "verification-1" },
		},
	}
	if err := driver.Collect(context.Background(), pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if len(publisher.results) != 1 {
		t.Fatalf("published %d results", len(publisher.results))
	}
	result := publisher.results[0]
	artifact, found := workspaceHeadArtifact(t, result)
	if !found {
		t.Fatal("missing workspace head evidence")
	}
	raw, err := os.ReadFile(filepath.Join(result.Finalized.StorageDir, "artifacts", filepath.FromSlash(artifact.Name)))
	if err != nil {
		t.Fatal(err)
	}
	captured, err := backlog.ParseWorkspaceHead(raw)
	if err != nil {
		t.Fatal(err)
	}
	gate := domain.EvaluateReviewCompletionGate(&domain.ReviewRoundHead{HeadCommit: head, Accepted: true, Verdict: "accept"}, &captured, nil)
	if !captured.Dirty || !slices.Contains(captured.DirtyPaths, "README.md") || gate.Passed {
		t.Fatalf("completed with an unreviewed verification edit: captured=%+v gate=%+v", captured, gate)
	}
}
