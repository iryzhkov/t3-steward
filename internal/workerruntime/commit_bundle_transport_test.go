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

// longRepository is an ordinary repository path long enough that the plain
// provenance records come close to a small per-artifact limit.
var longRepository = "/home/repositories/" + strings.Repeat("r", 120)

// The review of round 3 reproduced the same failure at the per-artifact limit:
// with ample aggregate room, an omission code made a provenance record that
// fits the per-artifact limit without bundles exceed it with them. Each
// result object, not only the whole upload, must stay collectable. The
// producer is collected with bundles disabled to find its largest ordinary
// object, and then at a per-artifact limit of exactly that size and at the
// review's 512 bytes, with an aggregate limit of 10000.
func TestCollectionWithBundlesKeepsEveryObjectWithinThePerArtifactLimit(t *testing.T) {
	plain := collectTwoCommitProducerWith(t, 10000, 10000, longRepository, false)
	if record := plain.record(t); record.Phase != PhaseCompleted {
		t.Fatalf("phase = %s, failure %q; want the ordinary result collected", record.Phase, record.Failure)
	}
	_, largest, _ := resultUploadObjects(t, plain)
	if largest > 512 {
		t.Fatalf("largest ordinary object is %d bytes; the review's 512-byte limit expects it to fit", largest)
	}
	for _, objectLimit := range []int64{largest, 512} {
		disabled := collectTwoCommitProducerWith(t, objectLimit, 10000, longRepository, false)
		if record := disabled.record(t); record.Phase != PhaseCompleted {
			t.Fatalf("per-artifact limit %d without bundles: phase = %s, failure %q", objectLimit, record.Phase, record.Failure)
		}
		enabled := collectTwoCommitProducerWith(t, objectLimit, 10000, longRepository, true)
		if record := enabled.record(t); record.Phase != PhaseCompleted {
			t.Fatalf("per-artifact limit %d with bundles: phase = %s, failure %q; the same result collects without bundles",
				objectLimit, record.Phase, record.Failure)
		}
		if _, biggest, bundles := resultUploadObjects(t, enabled); biggest > objectLimit || len(bundles) != 0 {
			t.Fatalf("per-artifact limit %d with bundles: largest object %d bytes, bundles %v", objectLimit, biggest, bundles)
		}
	}
}

// The invariant over a range of limits: wherever the producer collects with
// bundle generation disabled, it collects with it enabled, and the enabled
// upload keeps within both the per-artifact and the aggregate limit. The
// limits run from just below the ordinary result's largest object and total
// to room for both bundles, so the range covers no metadata, omission codes
// only, some bundles and every bundle.
func TestCollectionWithBundlesCollectsWheneverItCollectsWithoutAcrossLimits(t *testing.T) {
	plain := collectTwoCommitProducerWith(t, 10000, 10000, longRepository, false)
	if record := plain.record(t); record.Phase != PhaseCompleted {
		t.Fatalf("phase = %s, failure %q; want the ordinary result collected", record.Phase, record.Failure)
	}
	ordinary, largest, _ := resultUploadObjects(t, plain)
	objectLimits := []int64{largest - 1, largest, largest + 24, largest + 48, largest + 160, 8192, 16384}
	totalLimits := []int64{ordinary - 1, ordinary, ordinary + 48, ordinary + 160, ordinary + 8000, 40000}
	var refused, withoutBundles, someBundles, everyBundle int
	for _, objectLimit := range objectLimits {
		for _, totalLimit := range totalLimits {
			if totalLimit < objectLimit {
				continue
			}
			disabled, err := tryCollectTwoCommitProducer(t, objectLimit, totalLimit, longRepository, false)
			if err != nil || disabled.record(t).Phase != PhaseCompleted {
				refused++
				continue
			}
			enabled, err := tryCollectTwoCommitProducer(t, objectLimit, totalLimit, longRepository, true)
			if err != nil {
				t.Fatalf("limits %d/%d: collects without bundles but not with them: %v", objectLimit, totalLimit, err)
			}
			if record := enabled.record(t); record.Phase != PhaseCompleted {
				t.Fatalf("limits %d/%d: collects without bundles but not with them: phase = %s, failure %q",
					objectLimit, totalLimit, record.Phase, record.Failure)
			}
			total, biggest, bundles := resultUploadObjects(t, enabled)
			if total > totalLimit || biggest > objectLimit {
				t.Fatalf("limits %d/%d: upload %d bytes with a largest object of %d bytes", objectLimit, totalLimit, total, biggest)
			}
			switch len(bundles) {
			case 0:
				withoutBundles++
			case 1:
				someBundles++
			default:
				everyBundle++
			}
		}
	}
	if refused == 0 || withoutBundles == 0 || someBundles == 0 || everyBundle == 0 {
		t.Fatalf("limits covered %d refused, %d without bundles, %d with one, %d with both; want every case",
			refused, withoutBundles, someBundles, everyBundle)
	}
}

// resultUpload is the size of the collected result upload and the bundles it
// carries.
func resultUpload(t *testing.T, f *collectionFixture) (int64, []string) {
	t.Helper()
	total, _, bundles := resultUploadObjects(t, f)
	return total, bundles
}

// resultUploadObjects is the size of the collected result upload, the size of
// its largest object and the bundles it carries.
func resultUploadObjects(t *testing.T, f *collectionFixture) (int64, int64, []string) {
	t.Helper()
	pending, err := f.custody.PendingUploadByPurpose("result")
	if err != nil || pending == nil {
		t.Fatalf("result upload: %+v, %v", pending, err)
	}
	var largest int64
	var bundles []string
	for _, object := range pending.Manifest.Objects {
		largest = max(largest, object.Size)
		if object.Kind == string(domain.ArtifactGitState) {
			bundles = append(bundles, object.Path)
		}
	}
	return pending.Manifest.TotalBytes, largest, bundles
}

// collectTwoCommitProducer collects a producer that declared two commits of a
// 6000-byte incompressible change under a custody store whose per-object and
// aggregate limits are both limit, with bundle generation enabled or not.
func collectTwoCommitProducer(t *testing.T, limit int64, bundles bool) *collectionFixture {
	t.Helper()
	return collectTwoCommitProducerWith(t, limit, limit, "", bundles)
}

// collectTwoCommitProducerWith collects the two-commit producer with the
// package's and the custody store's per-object and aggregate limits set alike,
// and with the package's repository, when given, recorded in its provenance.
func collectTwoCommitProducerWith(t *testing.T, objectLimit, totalLimit int64, repository string, bundles bool) *collectionFixture {
	t.Helper()
	f, err := tryCollectTwoCommitProducer(t, objectLimit, totalLimit, repository, bundles)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	return f
}

// tryCollectTwoCommitProducer is collectTwoCommitProducerWith returning the
// collection's error, such as a permanent collection failure.
func tryCollectTwoCommitProducer(t *testing.T, objectLimit, totalLimit int64, repository string, bundles bool) (*collectionFixture, error) {
	t.Helper()
	f := newCollectionFixtureWith(t, objectLimit, totalLimit, 100, 200, func(pkg *workerproto.ExecutionPackage, driver *LocalDriver, workspace string) {
		pkg.Limits.MaxArtifactBytes, pkg.Limits.MaxTotalBytes = objectLimit, totalLimit
		if repository != "" {
			pkg.Environment.Repository = repository
		}
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
	return f, f.runtime.collect(context.Background(), "assignment-1")
}
