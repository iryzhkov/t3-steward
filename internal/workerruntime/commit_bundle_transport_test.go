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
	f := collectTwoCommitProducer(t, limit, true)
	if record := f.record(t); record.Phase != PhaseCompleted {
		t.Fatalf("phase = %s, failure %q; want a completed collection", record.Phase, record.Failure)
	}
	total, bundles := resultUpload(t, f)
	if total > limit {
		t.Fatalf("result upload is %d bytes, over the aggregate limit of %d", total, limit)
	}
	if !slices.Equal(bundles, []string{"results/" + backlog.CommitBundleArtifactName("repair")}) {
		t.Fatalf("uploaded bundles = %v, want only the first declared commit's", bundles)
	}
}

// Bundle bookkeeping never fails a producer that collects without it. The
// same valid two-commit producer is collected with bundle generation disabled,
// which measures its ordinary result upload, and then with it enabled at
// limits from exactly that size to a little above it, where no bundle fits and
// at most the fixed omission records do. Every enabled collection completes
// within the custody store's aggregate limit.
func TestCollectionWithBundlesCompletesWheneverItCompletesWithout(t *testing.T) {
	plain := collectTwoCommitProducer(t, 10000, false)
	if record := plain.record(t); record.Phase != PhaseCompleted {
		t.Fatalf("phase = %s, failure %q; want the ordinary result collected", record.Phase, record.Failure)
	}
	ordinary, _ := resultUpload(t, plain)
	for _, limit := range []int64{ordinary, ordinary + 32, ordinary + 64, max(ordinary, 1500)} {
		disabled := collectTwoCommitProducer(t, limit, false)
		if record := disabled.record(t); record.Phase != PhaseCompleted {
			t.Fatalf("limit %d without bundles: phase = %s, failure %q", limit, record.Phase, record.Failure)
		}
		enabled := collectTwoCommitProducer(t, limit, true)
		if record := enabled.record(t); record.Phase != PhaseCompleted {
			t.Fatalf("limit %d with bundles: phase = %s, failure %q; the same result collects without bundles in %d bytes",
				limit, record.Phase, record.Failure, ordinary)
		}
		total, bundles := resultUpload(t, enabled)
		if total > limit || len(bundles) != 0 {
			t.Fatalf("limit %d with bundles: upload %d bytes carrying bundles %v", limit, total, bundles)
		}
	}
}

// resultUpload is the size of the collected result upload and the bundles it
// carries.
func resultUpload(t *testing.T, f *collectionFixture) (int64, []string) {
	t.Helper()
	pending, err := f.custody.PendingUploadByPurpose("result")
	if err != nil || pending == nil {
		t.Fatalf("result upload: %+v, %v", pending, err)
	}
	var bundles []string
	for _, object := range pending.Manifest.Objects {
		if object.Kind == string(domain.ArtifactGitState) {
			bundles = append(bundles, object.Path)
		}
	}
	return pending.Manifest.TotalBytes, bundles
}

// collectTwoCommitProducer collects a producer that declared two commits of a
// 6000-byte incompressible change under a custody store whose per-object and
// aggregate limits are both limit, with bundle generation enabled or not.
func collectTwoCommitProducer(t *testing.T, limit int64, bundles bool) *collectionFixture {
	t.Helper()
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
		if bundles {
			pkg.RequiredCapabilities = append(pkg.RequiredCapabilities, workerproto.PackageCapabilityCommitBundle)
		}
		driver.Finalizer.CampaignRefs = backlog.CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	})
	if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
		t.Fatalf("collect: %v", err)
	}
	return f
}
