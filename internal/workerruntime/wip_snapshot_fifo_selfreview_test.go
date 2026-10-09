//go:build unix

package workerruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

// awaitReleasing waits at most testtiming.Bound(wait) for done. If it is still pending, the
// FIFO at path is released by opening and closing writers until done
// arrives, and blocked is reported, so no test leaves a reader waiting.
func awaitReleasing[T any](t *testing.T, done <-chan T, path string, wait time.Duration) (value T, blocked bool) {
	t.Helper()
	select {
	case value = <-done:
		return value, false
	case <-time.After(testtiming.Bound(wait)):
	}
	deadline := time.Now().Add(testtiming.Bound(10 * time.Second))
	for {
		select {
		case value = <-done:
			return value, true
		default:
		}
		if writer, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0); err == nil {
			writer.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("still blocked after releasing %s", path)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Self-review, lens 2: the secret scan of a failed result's wip.bundle ran
// Git in the task's repository to borrow the bundle's prerequisites, and Git
// waits on a FIFO the task put at .git/config. The scan reads only the
// workspace's objects now, so the task's configuration is never opened, and
// it still finds what the bundle carries.
func TestSecretScanOfABundleReadsNoTaskConfiguration(t *testing.T) {
	secret := "synthetic-bundle-credential"
	repo, _, _, bundle := secretGitRepo(t, []byte(secret))
	config := filepath.Join(repo, ".git", "config")
	if err := os.Remove(config); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(config, 0o600); err != nil {
		t.Fatal(err)
	}
	scanner := newResultScanner(SecretScanConfig{}, []string{secret}, nil)
	done := make(chan error, 1)
	go func() { done <- scanner.scanBundle(context.Background(), repo, "bundle", bundle) }()
	err, blocked := awaitReleasing(t, done, config, 3*time.Second)
	if blocked {
		t.Fatal("the bundle scan opened the task's .git/config and waited on it")
	}
	var finding *SecretScanError
	if !errors.As(err, &finding) || finding.Detector != "canary" {
		t.Fatalf("err = %v, want the bundle's credential found", err)
	}
}

// The scan's own repository takes the bundle's object format, so a sha256
// workspace's bundle is still decoded and scanned.
func TestSecretScanOfASHA256Bundle(t *testing.T) {
	secret := "synthetic-sha256-credential"
	repo := t.TempDir()
	secretGit(t, repo, "init", "-q", "--object-format=sha256")
	secretGit(t, repo, "commit", "-q", "--allow-empty", "-m", "base")
	base := secretGit(t, repo, "rev-parse", "HEAD")
	writeTestFile(t, filepath.Join(repo, "added.txt"), secret)
	secretGit(t, repo, "add", "added.txt")
	secretGit(t, repo, "commit", "-q", "-m", "fixture")
	bundle := filepath.Join(t.TempDir(), "result.bundle")
	secretGit(t, repo, "bundle", "create", bundle, base+"..HEAD")
	err := newResultScanner(SecretScanConfig{}, []string{secret}, nil).scanBundle(context.Background(), repo, "bundle", bundle)
	var finding *SecretScanError
	if !errors.As(err, &finding) || finding.Detector != "canary" {
		t.Fatalf("err = %v, want the sha256 bundle's credential found", err)
	}
}

// The objects the bundle scan borrows are still the task's, and Git waits on
// a FIFO among them too, so the decode is bounded by its own time whatever
// the caller's context; running out of it refuses the bundle.
func TestSecretScanOfABundleIsBounded(t *testing.T) {
	repo, _, _, bundle := secretGitRepo(t, []byte("harmless"))
	pack := filepath.Join(repo, ".git", "objects", "pack", "pack-"+strings.Repeat("0", 40)+".idx")
	if err := os.MkdirAll(filepath.Dir(pack), 0o700); err != nil {
		t.Fatal(err)
	}
	// Git lists a pack only when its .pack exists, and opens the index when it
	// looks an object up.
	writeTestFile(t, strings.TrimSuffix(pack, ".idx")+".pack", "")
	if err := syscall.Mkfifo(pack, 0o600); err != nil {
		t.Fatal(err)
	}
	scanner := newResultScanner(SecretScanConfig{DecodeTimeout: 300 * time.Millisecond}, nil, nil)
	done := make(chan error, 1)
	go func() { done <- scanner.scanBundle(context.Background(), repo, "bundle", bundle) }()
	err, blocked := awaitReleasing(t, done, pack, 3*time.Second)
	if blocked {
		t.Fatal("the bundle scan waited on a FIFO among the task's objects past its own time")
	}
	var finding *SecretScanError
	if !errors.As(err, &finding) || finding.Detector != "bundle-decode" {
		t.Fatalf("err = %v, want the bundle refused as undecodable", err)
	}
}

// Self-review, lens 2, end to end: a failed attempt's collection snapshots
// its work and scans the bundle before the failure publishes. A FIFO at
// .git/config held it up forever.
func TestCollectFailurePublishesPastAFIFOTaskConfiguration(t *testing.T) {
	f := newWIPFixture(t, commitOutputs)
	writeTestFile(t, filepath.Join(f.workspace, "c.txt"), "new\n")
	custody := &capturingCustody{CustodyStore: testCustodyStore(t, filepath.Join(t.TempDir(), "custody"), func() time.Time { return runtimeTestNow })}
	f.driver.Publisher = custody
	f.driver.T3 = &recordingT3{thread: &domain.Thread{ID: "thread-1", TurnID: "turn-3", TurnState: "completed"}, archive: []byte("{}")}
	config := filepath.Join(f.workspace, ".git", "config")
	if err := os.Remove(config); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(config, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- f.driver.CollectFailure(context.Background(), f.pkg, f.workspace, "attempt failed")
	}()
	err, blocked := awaitReleasing(t, done, config, 10*time.Second)
	if blocked {
		t.Fatal("failure collection waited on the task's .git/config")
	}
	if err != nil {
		t.Fatal(err)
	}
}

// Self-review, lens 1: when Git waiting on the work tree is stopped, the
// error names the step that waited, not a fallback tried after the time was
// already up.
func TestSnapshotTimeoutNamesTheStepThatWaited(t *testing.T) {
	f := newWIPFixture(t, commitOutputs)
	// The steps before staging must finish inside the snapshot's own time,
	// so it is scaled for a loaded race run.
	f.driver.Config.SnapshotTimeout = testtiming.Bound(300 * time.Millisecond)
	writeTestFile(t, filepath.Join(f.workspace, "a.txt"), "changed\n")
	path := filepath.Join(f.workspace, ".gitignore")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err, blocked := snapshotOutcome(t, f, context.Background(), path, 3*time.Second)
	if blocked {
		t.Fatal("snapshot waited past its own time")
	}
	if err == nil || !strings.Contains(err.Error(), "stage snapshot") || strings.Contains(err.Error(), "seed snapshot index") {
		t.Fatalf("err = %v, want the staging step named", err)
	}
}

// Self-review, lens 4: the task's index, up to 1 GiB, was held in memory
// and copied twice more before it was written to the snapshot repository. A
// task can make its index that large with one truncate. It is copied as a
// file now.
func TestSnapshotDoesNotHoldTheTaskIndexInMemory(t *testing.T) {
	f := newWIPFixture(t, commitOutputs)
	writeTestFile(t, filepath.Join(f.workspace, "a.txt"), "changed\n")
	index := filepath.Join(f.workspace, ".git", "index")
	if err := os.Remove(index); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(256 << 20); err != nil {
		t.Fatal(err)
	}
	file.Close()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	summary, err := f.driver.SnapshotWorkInProgress(context.Background(), f.pkg, f.workspace)
	runtime.ReadMemStats(&after)
	// The unusable index gives way to HEAD's tree, as before.
	if err != nil || !strings.HasPrefix(summary, "wip.bundle retained") {
		t.Fatalf("summary = %q, err = %v", summary, err)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 64<<20 {
		t.Fatalf("snapshot allocated %d MiB for a 256 MiB index", allocated>>20)
	}
}
