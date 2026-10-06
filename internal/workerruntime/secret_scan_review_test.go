package workerruntime

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestSecretScanMixedURLCanary(t *testing.T) {
	secret := "synthetic/key? credential+"
	for _, encoded := range []string{"synthetic%2fkey%3f%20credential%2b", "synthetic%2Fkey%3f+credential%2B", "%73ynthetic/key?%20credential+"} {
		scanner := newResultScanner(SecretScanConfig{}, []string{secret}, nil)
		err := scanner.scan("output", "output", strings.NewReader("safe\n"+encoded))
		var finding *SecretScanError
		if !errors.As(err, &finding) || finding.Detector != "canary" || finding.Offset != 5 {
			t.Fatalf("URL canary escaped: %v", err)
		}
	}
}
func TestSecretScanReusedGitBlob(t *testing.T) {
	repo := t.TempDir()
	secret := "synthetic-reused-credential"
	secretGit(t, repo, "init", "-q")
	if err := os.WriteFile(filepath.Join(repo, "existing"), []byte(secret), 0600); err != nil {
		t.Fatal(err)
	}
	secretGit(t, repo, "add", "existing")
	secretGit(t, repo, "commit", "-q", "-m", "base")
	base := secretGit(t, repo, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(repo, "copied.bin"), []byte(secret), 0600); err != nil {
		t.Fatal(err)
	}
	secretGit(t, repo, "add", "copied.bin")
	secretGit(t, repo, "commit", "-q", "-m", "copy")
	head := secretGit(t, repo, "rev-parse", "HEAD")
	listing, err := scanCommitListing(context.Background(), repo, base, head)
	if err != nil {
		t.Fatal(err)
	}
	scanner := newResultScanner(SecretScanConfig{}, []string{secret}, nil)
	if err := scanner.scanGitObjects(context.Background(), repo, "commit", "commit", string(listing)); err == nil {
		t.Fatal("reused added blob admitted")
	}
	secretGit(t, repo, "rm", "existing", "copied.bin")
	secretGit(t, repo, "commit", "-q", "-m", "remove")
	listing, err = scanCommitListing(context.Background(), repo, base, secretGit(t, repo, "rev-parse", "HEAD"))
	if err != nil {
		t.Fatal(err)
	}
	if err := scanner.scanGitObjects(context.Background(), repo, "commit", "commit", string(listing)); err == nil {
		t.Fatal("removed diff content omitted")
	}
}
func TestSecretScanBindsContentToDeclaredDigest(t *testing.T) {
	store := testCustodyStore(t, t.TempDir(), func() time.Time { return runtimeTestNow })
	secret := "synthetic-hash-bound-secret"
	store.config.SecretScan.StaticCanaries = []string{secret}
	finalized := backlog.FinalizedAttempt{StorageDir: t.TempDir()}
	if err := os.MkdirAll(filepath.Join(finalized.StorageDir, "artifacts"), 0700); err != nil {
		t.Fatal(err)
	}
	// Metadata commits to secret-bearing bytes; the scan sees different clean bytes.
	if err := os.WriteFile(filepath.Join(finalized.StorageDir, "artifacts", "output"), []byte("clean substituted bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	obj := objectForBytes("fixture", "unused", "output", "text/plain", []byte(secret))
	finalized.Artifacts = []domain.Artifact{{ID: obj.ID, Name: "output", Kind: domain.ArtifactOutput, MediaType: obj.MediaType, Size: obj.Size, SHA256: obj.SHA256, WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1", StoragePath: "runs/run-1/task-1/attempt-1/artifacts/output"}}
	err := store.AdmitResult(testPackage(), PublishedResult{Finalized: finalized, FinalMessage: "done", ThreadArchive: []byte("{}")})
	var finding *SecretScanError
	if !errors.As(err, &finding) || finding.Detector != "object-digest" {
		t.Fatalf("unbound scanned bytes: %v", err)
	}
}
func TestSecretScanCredentialSnapshotSurvivesRotationAndRestart(t *testing.T) {
	root := t.TempDir()
	store := testCustodyStore(t, root, func() time.Time { return runtimeTestNow })
	old := "synthetic-old-oauth-credential"
	current := old
	store.config.SecretScan.Canaries = func(context.Context, workerproto.ExecutionPackage) ([]string, error) { return []string{current}, nil }
	pkg := testPackage()
	if err := store.SnapshotSecrets(context.Background(), pkg); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "secret-scans"))
	if err != nil || len(entries) == 0 {
		t.Fatal("snapshot absent", err)
	}
	for _, entry := range entries {
		raw, err := os.ReadFile(filepath.Join(root, "secret-scans", entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte(old)) {
			t.Fatal("snapshot persisted credential value")
		}
	}
	current = "synthetic-new-oauth-credential"
	store = testCustodyStore(t, root, func() time.Time { return runtimeTestNow })
	store.config.SecretScan.Canaries = func(context.Context, workerproto.ExecutionPackage) ([]string, error) { return []string{current}, nil }
	for _, value := range []string{old, current, "synthetic-old-oauth%2dcredential"} {
		err := store.AdmitResult(pkg, PublishedResult{FinalMessage: value, ThreadArchive: []byte("{}")})
		var finding *SecretScanError
		if !errors.As(err, &finding) || finding.Detector != "canary" {
			t.Fatalf("rotated credential escaped: %v", err)
		}
	}
}
