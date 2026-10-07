package workerruntime

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type failedCommitVerifyProcess struct{}

func (failedCommitVerifyProcess) Run(context.Context, backlog.ProcessRequest) (backlog.ProcessResult, error) {
	return backlog.ProcessResult{ExitCode: 7, Output: "failed verify"}, nil
}
func (failedCommitVerifyProcess) Kill(string) error { return nil }
func TestFailedCommitScanRefusalPreservesOrdinaryFailure(t *testing.T) {
	const secret = "selfreview-failed-candidate-secret"
	for _, mode := range []string{"baseline", "record", "bundle"} {
		t.Run(mode, func(t *testing.T) {
			f := newCollectionFixtureWith(t, 8192, 32768, 20, 20, func(pkg *workerproto.ExecutionPackage, d *LocalDriver, workspace string) {
				pkg.Outputs = append(pkg.Outputs, domain.ArtifactDeclaration{Name: "candidate", MediaType: "application/json", Commit: &domain.CommitOutput{}})
				if mode != "baseline" {
					pkg.RequiredCapabilities = append(pkg.RequiredCapabilities, workerproto.PackageCapabilityFailedCommit)
				}
				if mode == "bundle" {
					pkg.RequiredCapabilities = append(pkg.RequiredCapabilities, workerproto.PackageCapabilityCommitBundle)
				}
				secretGit(t, workspace, "init", "-q")
				secretGit(t, workspace, "commit", "-q", "--allow-empty", "-m", "base")
				base := secretGit(t, workspace, "rev-parse", "HEAD")
				if err := os.WriteFile(filepath.Join(workspace, "committed.txt"), []byte(secret), 0600); err != nil {
					t.Fatal(err)
				}
				secretGit(t, workspace, "add", "committed.txt")
				secretGit(t, workspace, "commit", "-q", "-m", "candidate")
				if err := os.MkdirAll(filepath.Join(workspace, ".t3"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(workspace, ".t3", "base-commit"), []byte(base), 0600); err != nil {
					t.Fatal(err)
				}
				d.Finalizer.Processes = failedCommitVerifyProcess{}
				d.Finalizer.CampaignRefs = backlog.CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
			})
			f.custody.config.SecretScan.StaticCanaries = []string{secret}
			err := f.runtime.collect(context.Background(), "assignment-1")
			if err != nil {
				t.Logf("first collection: %v", err)
				if err = f.runtime.collect(context.Background(), "assignment-1"); err != nil {
					t.Fatal(err)
				}
			}
			pending, err := f.custody.PendingUploadByPurpose("result")
			if err != nil || pending == nil {
				t.Fatalf("ordinary failed result unavailable: %v", err)
			}
			var verification, output bool
			for _, obj := range pending.Manifest.Objects {
				if obj.Kind == string(domain.ArtifactGitState) {
					t.Fatalf("refused candidate still uploaded: %+v", obj)
				}
				verification = verification || obj.Kind == "verification"
				output = output || obj.Path == "results/answer.txt"
			}
			if !verification || !output {
				t.Fatalf("candidate refusal discarded ordinary failed result: verification=%v output=%v failure=%s objects=%+v", verification, output, f.record(t).Failure, pending.Manifest.Objects)
			}
			if strings.Contains(f.record(t).Failure, "secret") {
				t.Log(f.record(t).Failure)
			}
		})
	}
}
