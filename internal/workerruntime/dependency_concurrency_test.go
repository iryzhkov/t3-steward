package workerruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestDependencyRepairLockCancellationAndSymlink(t *testing.T) {
	workspace := filepath.Join(t.TempDir(), "workspace")
	first, err := lockDependencyRepair(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if second, err := lockDependencyRepair(ctx, workspace); !errors.Is(err, context.DeadlineExceeded) {
		if second != nil {
			second.Close()
		}
		t.Fatalf("held lock: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(filepath.Dir(workspace), ".dependency-repair.lock")
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(filepath.Dir(workspace), "unrelated")
	if err := os.WriteFile(target, []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, lockPath); err != nil {
		t.Fatal(err)
	}
	if file, err := lockDependencyRepair(context.Background(), workspace); err == nil {
		file.Close()
		t.Fatal("accepted task-controlled symlink as repair lock")
	}
	if data, err := os.ReadFile(target); err != nil || string(data) != "unchanged" {
		t.Fatalf("symlink target changed: %q, %v", data, err)
	}
}

func TestConcurrentDependencyRepairPreservesEveryCaller(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { _ = removeReadOnlyTree(root) })
	workspace := filepath.Join(root, "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, ".t3"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../dependencies", filepath.Join(workspace, ".t3", "dependencies")); err != nil {
		t.Fatal(err)
	}
	pkg := testPackage()
	object := testArtifact("handoff", "dependencies/producer/handoff.md", "verified handoff")
	pkg.Dependencies = []workerproto.DependencyInput{{TaskID: "producer", Artifacts: []workerproto.ArtifactObject{object}}}
	driver := &LocalDriver{Config: LocalDriverConfig{ArtifactRoot: filepath.Join(root, "artifacts")}, Source: mapArtifactSource{"handoff": []byte("verified handoff")}}
	if _, err := driver.cacheArtifact(context.Background(), object, pkg.Limits.MaxArtifactBytes); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errors := make(chan error, 32)
	var group sync.WaitGroup
	for range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			errors <- driver.ensureDependencyIntegrity(context.Background(), pkg, workspace)
		}()
	}
	close(start)
	group.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Errorf("concurrent repair failed: %v", err)
		}
	}
	if err := driver.verifyDependencyIntegrity(context.Background(), pkg, workspace); err != nil {
		t.Fatal(err)
	}
}
