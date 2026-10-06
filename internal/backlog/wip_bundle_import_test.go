package backlog

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// The coordinator takes the work-in-progress bundle of a failed attempt only
// under its fixed identity and only for a task that declares a commit, the
// one kind of task whose unfinished work is a commit to recover.
func TestResultImportAcceptsTheWorkInProgressBundle(t *testing.T) {
	data := []byte("# v2 git bundle\n")
	sum := sha256.Sum256(data)
	bundle := workerproto.ArtifactObject{
		ID: WorkInProgressBundleID("attempt-1"), Path: "results/" + WorkInProgressBundleName,
		Kind: string(domain.ArtifactGitState), MediaType: CommitBundleMediaType,
		Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:]),
	}
	manifest := workerproto.ArtifactTransferManifest{WorkerID: "homelab"}
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1"}
	withCommit := domain.Task{ID: "task-1", Outputs: []domain.ArtifactDeclaration{{Name: "implementation", Commit: &domain.CommitOutput{}}}}
	withoutCommit := domain.Task{ID: "task-1", Outputs: []domain.ArtifactDeclaration{{Name: "handoff.md"}}}
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

	artifact, err := resultArtifact(bundle, manifest, attempt, withCommit, now)
	if err != nil {
		t.Fatalf("bundle refused: %v", err)
	}
	if artifact.Kind != domain.ArtifactGitState || artifact.Name != WorkInProgressBundleName {
		t.Fatalf("artifact = %+v", artifact)
	}
	if _, err := resultArtifact(bundle, manifest, attempt, withoutCommit, now); err == nil {
		t.Fatal("a task without a declared commit published a work-in-progress bundle")
	}
	foreign := bundle
	foreign.ID = WorkInProgressBundleID("attempt-2")
	if _, err := resultArtifact(foreign, manifest, attempt, withCommit, now); err == nil {
		t.Fatal("another attempt's bundle identity was accepted")
	}
}
