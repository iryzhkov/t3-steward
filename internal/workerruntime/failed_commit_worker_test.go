package workerruntime

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestFailedCommitGitStateCannotHideCommittedSecret(t *testing.T) {
	secret := "synthetic-failed-commit-credential"
	repo, base, head, _ := secretGitRepo(t, []byte(secret))
	pkg := testPackage()
	pkg.Outputs = []domain.ArtifactDeclaration{{Name: "implementation", Commit: &domain.CommitOutput{}}}
	p := backlog.CommitProvenance{WorkflowRunID: pkg.Identity.WorkflowRunID, TaskID: pkg.Identity.TaskID, Name: "implementation", Repository: repo, Base: base, Commit: head, Ref: backlog.FailedCampaignRef(pkg.Identity.WorkflowRunID, pkg.Identity.TaskID, pkg.Identity.AttemptID, "implementation"), FailedAttempt: &backlog.FailedCommitAttempt{ID: pkg.Identity.AttemptID, VerificationFailures: []string{"verification command failed (7): exit 7"}}}
	raw, err := backlog.MarshalCommitProvenance(p)
	if err != nil {
		t.Fatal(err)
	}
	name := backlog.FailedCommitArtifactName("implementation")
	finalized := backlog.FinalizedAttempt{StorageDir: t.TempDir()}
	if err := os.MkdirAll(filepath.Join(finalized.StorageDir, "artifacts", filepath.Dir(name)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(finalized.StorageDir, "artifacts", name), raw, 0600); err != nil {
		t.Fatal(err)
	}
	obj := objectForBytes("candidate", "unused", "git_state", "application/json", raw)
	artifact := domain.Artifact{ID: obj.ID, Name: name, Kind: domain.ArtifactGitState, MediaType: obj.MediaType, Size: obj.Size, SHA256: obj.SHA256, WorkflowRunID: pkg.Identity.WorkflowRunID, TaskID: pkg.Identity.TaskID, AttemptID: pkg.Identity.AttemptID, StoragePath: filepath.ToSlash(filepath.Join("runs", pkg.Identity.WorkflowRunID, pkg.Identity.TaskID, pkg.Identity.AttemptID, "artifacts", name))}
	finalized.Artifacts = []domain.Artifact{artifact}
	store := testCustodyStore(t, t.TempDir(), func() time.Time { return runtimeTestNow })
	store.config.SecretScan.StaticCanaries = []string{secret}
	err = store.PublishResult(context.Background(), pkg, PublishedResult{Finalized: finalized, FinalMessage: "verify failed", ThreadArchive: []byte("{}"), WorkspaceDir: repo})
	var finding *SecretScanError
	if !errors.As(err, &finding) || finding.Detector != "canary" {
		t.Fatalf("failed commit secret admitted: %v", err)
	}
}

func TestFailedCommitCrossHostDeliveryAndRestore(t *testing.T) {
	source, base, head, bundlePath := secretGitRepo(t, []byte("clean committed content"))
	pkg := testPackage()
	root := t.TempDir()
	t.Cleanup(func() { _ = removeReadOnlyTree(root) })
	consumer := filepath.Join(root, "consumer")
	secretGit(t, source, "branch", "base-only", base)
	if out, err := exec.Command("git", "clone", "--no-local", "--single-branch", "--branch", "base-only", source, consumer).CombinedOutput(); err != nil {
		t.Fatalf("clone: %s %v", out, err)
	}
	producerStore := backlog.CampaignRefStore{Root: filepath.Join(root, "producer-refs")}
	p, err := producerStore.Publish(context.Background(), backlog.PublishCommitRequest{WorkflowRunID: "source-run", TaskID: "producer", Name: "implementation", Repository: pkg.Environment.Repository, WorkspaceDir: source, Base: base, FailedAttempt: &backlog.FailedCommitAttempt{ID: "failed-attempt", VerificationFailures: []string{"verification command failed (7): exit 7"}}}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	secretGit(t, source, "update-ref", p.Ref, head)
	secretGit(t, source, "bundle", "create", bundlePath, p.Ref, "^"+base)
	bundleRaw, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	metadata := objectForBytes("bundle", "unused", "commit-bundle", backlog.CommitBundleMediaType, bundleRaw)
	p.Bundle = &backlog.CommitBundleRecord{Artifact: backlog.CommitBundleArtifactName(p.Name), SHA256: metadata.SHA256, Size: metadata.Size}
	raw, err := backlog.MarshalCommitProvenance(p)
	if err != nil {
		t.Fatal(err)
	}
	record := testArtifact("record", "dependencies/producer/implementation", string(raw))
	bundle := testArtifact("bundle", "commit-bundles/source-run/producer/implementation.bundle", string(bundleRaw))
	bundle.MediaType = backlog.CommitBundleMediaType
	pkg.Dependencies = []workerproto.DependencyInput{{TaskID: "external-producer", Artifacts: []workerproto.ArtifactObject{record}, Provenance: &workerproto.DependencyProvenance{RunID: "source-run", TaskID: "producer", AttemptID: "failed-attempt"}}}
	pkg.CommitBundles = []workerproto.CommitBundleInput{{WorkflowRunID: "source-run", TaskID: "producer", Name: "implementation", Bundle: &bundle}}
	deliveries := commitBundleDeliveries(pkg, func(workerproto.ArtifactObject) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(string(bundleRaw))), nil
	})
	if _, ok := deliveries[p.Ref]; !ok {
		t.Fatalf("failed namespace delivery missing: %s", p.Ref)
	}
	driver := &LocalDriver{Config: LocalDriverConfig{ArtifactRoot: filepath.Join(root, "objects")}, Source: mapArtifactSource{record.ID: raw, bundle.ID: bundleRaw}}
	driver.Workspace.CampaignRefs = backlog.CampaignRefStore{Root: filepath.Join(root, "consumer-refs")}
	if err := os.MkdirAll(filepath.Join(consumer, ".t3"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../dependencies", filepath.Join(consumer, ".t3", "dependencies")); err != nil {
		t.Fatal(err)
	}
	if err := driver.ensureDependencyIntegrity(context.Background(), pkg, consumer); err != nil {
		t.Fatal(err)
	}
	got := secretGit(t, consumer, "rev-parse", "--verify", p.Ref+"^{commit}")
	if got != head {
		t.Fatalf("restored %s want %s", got, head)
	}
	p.FailedAttempt.ID = "other"
	p.Ref = backlog.FailedCampaignRef(p.WorkflowRunID, p.TaskID, "other", p.Name)
	if err := validateDependencyProvenance(pkg, pkg.Dependencies[0], p); err == nil {
		t.Fatal("failed attempt mismatch accepted")
	}
	pkg.Dependencies[0].Provenance = nil
	if err := validateDependencyProvenance(pkg, pkg.Dependencies[0], p); err == nil {
		t.Fatal("ordinary dependency accepted failed candidate")
	}
}
