package workerruntime

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func secretGit(t *testing.T, repo string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Fixture", "GIT_AUTHOR_EMAIL=fixture@example.com", "GIT_COMMITTER_NAME=Fixture", "GIT_COMMITTER_EMAIL=fixture@example.com")
	raw, err := cmd.Output()
	if err != nil {
		t.Fatalf("fixture git %s failed", args[0])
	}
	return strings.TrimSpace(string(raw))
}
func secretGitRepo(t *testing.T, content []byte) (string, string, string, string) {
	t.Helper()
	repo := t.TempDir()
	secretGit(t, repo, "init", "-q")
	secretGit(t, repo, "commit", "-q", "--allow-empty", "-m", "base")
	base := secretGit(t, repo, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(repo, "added.bin"), content, 0600); err != nil {
		t.Fatal(err)
	}
	secretGit(t, repo, "add", "added.bin")
	secretGit(t, repo, "commit", "-q", "-m", "fixture")
	head := secretGit(t, repo, "rev-parse", "HEAD")
	bundle := filepath.Join(t.TempDir(), "result.bundle")
	secretGit(t, repo, "bundle", "create", bundle, base+"..HEAD")
	return repo, base, head, bundle
}
func TestSecretScanGitCommitAndCompressedBundle(t *testing.T) {
	secret := "synthetic-git-credential"
	repo, base, head, bundle := secretGitRepo(t, append(bytes.Repeat([]byte{0, 1, 2}, 4096), []byte(secret)...))
	scanner := newResultScanner(SecretScanConfig{}, []string{secret}, nil)
	listing, err := scanGitOutput(context.Background(), repo, "rev-list", "--objects", base+".."+head, "--")
	if err != nil {
		t.Fatal(err)
	}
	for _, err := range []error{scanner.scanGitObjects(context.Background(), repo, "implementation", "commit", string(listing)), scanner.scanBundle(context.Background(), repo, "bundle", bundle)} {
		var finding *SecretScanError
		if !errors.As(err, &finding) || finding.Detector != "canary" || strings.Contains(err.Error(), secret) {
			t.Fatalf("Git credential not refused safely: %v", err)
		}
	}
	// The packed file cannot be scanned solely as raw text.
	raw, err := os.ReadFile(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(secret)) {
		t.Fatal("fixture must require bundle decompression")
	}
}
func TestSecretScanCommittedCanaryAtAdmission(t *testing.T) {
	secret := "synthetic-admission-credential"
	repo, base, head, _ := secretGitRepo(t, []byte(secret))
	pkg := testPackage()
	pkg.Outputs = []domain.ArtifactDeclaration{{Name: "implementation", Commit: &domain.CommitOutput{Revision: "HEAD"}}}
	provenance := backlog.CommitProvenance{Version: backlog.CampaignCommitRecordVersion, WorkflowRunID: "run-1", TaskID: "task-1", Name: "implementation", Repository: repo, Base: base, Commit: head, Ref: backlog.CampaignRef("run-1", "task-1", "implementation"), CreatedAt: runtimeTestNow}
	raw, err := backlog.MarshalCommitProvenance(provenance)
	if err != nil {
		t.Fatal(err)
	}
	finalized := backlog.FinalizedAttempt{StorageDir: t.TempDir()}
	if err := os.MkdirAll(filepath.Join(finalized.StorageDir, "artifacts"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(finalized.StorageDir, "artifacts", "implementation"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	obj := objectForBytes("fixture", "unused", "output", "application/json", raw)
	finalized.Artifacts = []domain.Artifact{{ID: obj.ID, Name: "implementation", Kind: domain.ArtifactOutput, MediaType: obj.MediaType, Size: obj.Size, SHA256: obj.SHA256, WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1", StoragePath: "runs/run-1/task-1/attempt-1/artifacts/implementation"}}
	store := testCustodyStore(t, t.TempDir(), func() time.Time { return runtimeTestNow })
	store.config.SecretScan.StaticCanaries = []string{secret}
	err = store.PublishResult(context.Background(), pkg, PublishedResult{Finalized: finalized, FinalMessage: "done", ThreadArchive: []byte("{}"), WorkspaceDir: repo})
	var finding *SecretScanError
	if !errors.As(err, &finding) || finding.Detector != "canary" {
		t.Fatalf("commit admitted: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(store.config.Root, "outbox"))
	if err != nil || len(entries) != 0 {
		t.Fatal("blocked upload exists", err)
	}
}
func TestSecretScanGitPatternsAndRepositoryAllowlist(t *testing.T) {
	token := "ghp_" + strings.Repeat("B", 36)
	repo, base, head, bundle := secretGitRepo(t, []byte(token))
	listing, err := scanGitOutput(context.Background(), repo, "rev-list", "--objects", base+".."+head, "--")
	if err != nil {
		t.Fatal(err)
	}
	scanner := newResultScanner(SecretScanConfig{}, nil, nil)
	if scanner.scanGitObjects(context.Background(), repo, "implementation", "commit", string(listing)) == nil {
		t.Fatal("commit pattern admitted")
	}
	if scanner.scanBundle(context.Background(), repo, "bundle", bundle) == nil {
		t.Fatal("bundle pattern admitted")
	}
	if err := os.Mkdir(filepath.Join(repo, ".t3"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, ".t3", "secret-scan-allow"), []byte(secretFingerprint(token)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	allow, err := loadSecretAllowlist(repo)
	if err != nil {
		t.Fatal(err)
	}
	scanner = newResultScanner(SecretScanConfig{}, nil, allow)
	if err := scanner.scanGitObjects(context.Background(), repo, "implementation", "commit", string(listing)); err != nil {
		t.Fatal(err)
	}
	if err := scanner.scanBundle(context.Background(), repo, "bundle", bundle); err != nil {
		t.Fatal(err)
	}
}
