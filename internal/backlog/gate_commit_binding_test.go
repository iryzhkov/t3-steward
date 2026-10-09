package backlog

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

func gateBindingGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	c := exec.Command("git", args...)
	c.Dir = dir
	out, err := c.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %s %v", args, out, err)
	}
	return strings.TrimSpace(string(out))
}

// gateBindingFinalize gates source.txt == "source" on a task that declares a
// commit at revision, and returns the gate report.
func gateBindingFinalize(t *testing.T, dir, base, revision string) GateReport {
	t.Helper()
	storage := t.TempDir()
	req := h2GateRequest(dir, "attempt-binding")
	req.Task.Gate = &domain.TaskGate{Commands: []string{"grep -qx source source.txt"}, Timeout: testtiming.Bound(5 * time.Second)}
	req.Task.Outputs = []domain.ArtifactDeclaration{{Name: "handoff", Commit: &domain.CommitOutput{Revision: revision}}}
	req.Repository, req.BaseCommit = dir, base
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	result, err := AttemptFinalizer{StorageRoot: storage, CampaignRefs: refs, Processes: &directRunner{}}.Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, result.StorageDir)
	return h2ReadGate(t, storage, result)
}

// A replace ref made the gate attest a passing commit's tree while the
// declared commit published was the failing one.
func TestGateIgnoresReplaceRefs(t *testing.T) {
	dir := h2GateRepository(t)
	base := gateBindingGit(t, dir, "rev-parse", "HEAD")
	writeTestFile(t, dir, "source.txt", "bad")
	h2Commit(t, dir)
	bad := gateBindingGit(t, dir, "rev-parse", "HEAD")
	gateBindingGit(t, dir, "replace", bad, base)
	gateBindingGit(t, dir, "reset", "-q", "--hard")
	report := gateBindingFinalize(t, dir, base, "")
	badTree := gateBindingGit(t, dir, "--no-replace-objects", "rev-parse", bad+"^{tree}")
	if report.Passed || (report.TreeHash != "" && report.TreeHash != badTree) {
		t.Fatalf("gate attested replaced tree %s for commit %s (real tree %s), passed=%v", report.TreeHash, bad, badTree, report.Passed)
	}
}

// A declared commit revision other than HEAD was published without being the
// commit whose tree the gate attested.
func TestGateRequiresDeclaredCommitAtHead(t *testing.T) {
	dir := h2GateRepository(t)
	base := gateBindingGit(t, dir, "rev-parse", "HEAD")
	gateBindingGit(t, dir, "checkout", "-q", "-b", "work")
	writeTestFile(t, dir, "source.txt", "bad")
	h2Commit(t, dir)
	gateBindingGit(t, dir, "checkout", "-q", "--detach", base)
	report := gateBindingFinalize(t, dir, base, "work")
	if report.Passed || report.Failure == nil || !strings.Contains(report.Failure.Reason, "different commit") {
		t.Fatalf("gate passed HEAD while declared revision work differs: %+v", report.Failure)
	}
	// The same revision at HEAD still gates normally.
	gateBindingGit(t, dir, "checkout", "-q", "-B", "work", base)
	if report := gateBindingFinalize(t, dir, base, "work"); !report.Passed {
		t.Fatalf("declared revision at HEAD failed: %+v", report.Failure)
	}
}
