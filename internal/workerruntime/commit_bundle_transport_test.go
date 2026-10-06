package workerruntime

import (
	"context"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	output, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

// A producer that declares two commits retains two bundles, each well within
// the per-artifact limit, and together over the result upload's total. The
// collection must still succeed: the whole upload, including the final message
// and the thread archive added after finalization, stays within the custody
// store's aggregate limit, and the bundle that does not fit is left out rather
// than turning the producer into a permanent collection failure.
func TestCollectionBudgetsCommitBundlesIntoTheResultUpload(t *testing.T) {
	const limit = 10000
	f := newCollectionFixtureWith(t, limit, limit, 100, 200, func(pkg *workerproto.ExecutionPackage, driver *LocalDriver, workspace string) {
		gitIn(t, workspace, "init", "--initial-branch=main")
		gitIn(t, workspace, "config", "user.name", "Test User")
		gitIn(t, workspace, "config", "user.email", "test@example.test")
		if err := os.WriteFile(filepath.Join(workspace, "base.txt"), []byte("base\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		gitIn(t, workspace, "add", "base.txt")
		gitIn(t, workspace, "commit", "-m", "base")
		base := gitIn(t, workspace, "rev-parse", "HEAD")
		if err := os.MkdirAll(filepath.Join(workspace, ".t3"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(workspace, ".t3", "base-commit"), []byte(base+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		noise := make([]byte, 6000)
		rand.New(rand.NewSource(16)).Read(noise)
		if err := os.WriteFile(filepath.Join(workspace, "noise.bin"), noise, 0o600); err != nil {
			t.Fatal(err)
		}
		gitIn(t, workspace, "add", "noise.bin")
		gitIn(t, workspace, "commit", "-m", "noise")
		pkg.Outputs = append(pkg.Outputs,
			domain.ArtifactDeclaration{Name: "repair", MediaType: "application/json", Commit: &domain.CommitOutput{}},
			domain.ArtifactDeclaration{Name: "followup", MediaType: "application/json", Commit: &domain.CommitOutput{}},
		)
		pkg.RequiredCapabilities = append(pkg.RequiredCapabilities, workerproto.PackageCapabilityCommitBundle)
		driver.Finalizer.CampaignRefs = backlog.CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	})
	if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if record := f.record(t); record.Phase != PhaseCompleted {
		t.Fatalf("phase = %s, failure %q; want a completed collection", record.Phase, record.Failure)
	}
	pending, err := f.custody.PendingUploadByPurpose("result")
	if err != nil || pending == nil {
		t.Fatalf("result upload: %+v, %v", pending, err)
	}
	if pending.Manifest.TotalBytes > limit {
		t.Fatalf("result upload is %d bytes, over the aggregate limit of %d", pending.Manifest.TotalBytes, limit)
	}
	var bundles []string
	for _, object := range pending.Manifest.Objects {
		if object.Kind == string(domain.ArtifactGitState) {
			bundles = append(bundles, object.Path)
		}
	}
	if !slices.Equal(bundles, []string{"results/" + backlog.CommitBundleArtifactName("repair")}) {
		t.Fatalf("uploaded bundles = %v, want only the first declared commit's", bundles)
	}
}
