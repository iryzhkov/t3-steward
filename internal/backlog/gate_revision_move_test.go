package backlog

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A gate command moved the declared revision to a failing commit after the
// pre-command binding check. The gate attested HEAD's tree, and finalization
// then published the moved revision: an ungated tree under passing evidence.
func TestGateDeclaredRevisionMovedDuringGateIsNotPublished(t *testing.T) {
	for _, revision := range []string{"work", "refs/heads/work"} {
		t.Run(revision, func(t *testing.T) {
			dir := h2GateRepository(t)
			base := gateBindingGit(t, dir, "rev-parse", "HEAD")
			writeTestFile(t, dir, "source.txt", "bad")
			h2Commit(t, dir)
			bad := gateBindingGit(t, dir, "rev-parse", "HEAD")
			gateBindingGit(t, dir, "checkout", "-q", "--detach", base)
			gateBindingGit(t, dir, "branch", "work", base)
			req := h2GateRequest(dir, "moving-revision")
			req.Task.Gate = &domain.TaskGate{Commands: []string{fmt.Sprintf("grep -qx source source.txt && git update-ref refs/heads/work %s", bad)}, Timeout: 5 * time.Second}
			req.Task.Outputs = []domain.ArtifactDeclaration{{Name: "handoff", Commit: &domain.CommitOutput{Revision: revision}}}
			req.Repository, req.BaseCommit = dir, base
			storage := t.TempDir()
			refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
			result, err := (AttemptFinalizer{StorageRoot: storage, CampaignRefs: refs, Processes: &directRunner{}}).Finalize(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			cleanupImmutable(t, result.StorageDir)
			report := h2ReadGate(t, storage, result)
			if report.Passed || result.Completion.VerificationPassed {
				t.Fatalf("gate passed although declared revision moved during it: passed=%v verification=%v", report.Passed, result.Completion.VerificationPassed)
			}
			if report.Failure == nil || !strings.Contains(report.Failure.Reason, "moved during the gate") {
				t.Fatalf("gate failure does not name the moved revision: %+v", report.Failure)
			}
			for _, a := range result.Artifacts {
				if a.Name != "handoff" {
					continue
				}
				var p CommitProvenance
				if err := json.Unmarshal(readStoredArtifact(t, storage, a), &p); err != nil {
					t.Fatal(err)
				}
				t.Fatalf("published declared commit %s after a failed gate (bad=%s)", p.Commit, bad)
			}
		})
	}
}

// Publication is pinned to the gated commit, so a declared revision that
// moves after the gate's own checks still cannot publish another commit.
func TestCampaignRefPublishRefusesCommitOtherThanGated(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	gated := gitOutput(t, repository, "rev-parse", "HEAD")
	gitRun(t, repository, "branch", "work")
	writeGitFile(t, repository, "version.txt", "ungated\n")
	gitRun(t, repository, "commit", "-qam", "ungated")
	gitRun(t, repository, "update-ref", "refs/heads/work", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	request := PublishCommitRequest{
		WorkflowRunID: "run-1", TaskID: "task-producer", Name: "handoff",
		Repository: repository, WorkspaceDir: repository, Base: gated,
		Revision: "work", ExpectedCommit: gated,
	}
	if p, err := refs.Publish(ctx, request, nil); err == nil || !strings.Contains(err.Error(), "not the gated commit "+gated) {
		t.Fatalf("published %+v for a moved revision, err=%v", p, err)
	}
	if _, err := refs.Resolve("run-1", "task-producer", "handoff"); err == nil {
		t.Fatal("refused publication left a provenance record")
	}
	gitRun(t, repository, "update-ref", "refs/heads/work", gated)
	if p, err := refs.Publish(ctx, request, nil); err != nil || p.Commit != gated {
		t.Fatalf("gated revision publish = %+v, %v", p, err)
	}
}
