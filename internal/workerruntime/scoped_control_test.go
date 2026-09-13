package workerruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type attachFunc func(context.Context, workerproto.ExecutionPackage) (T3Control, error)

func (f attachFunc) Attach(ctx context.Context, pkg workerproto.ExecutionPackage) (T3Control, error) {
	return f(ctx, pkg)
}

func TestDirectoryThreadOperationsNeverFallBackToHost(t *testing.T) {
	ctx := context.Background()
	pkg := testPackage()
	pkg.Environment.DirectoryBindings = []directoryresource.Binding{{}}
	refused := errors.New("scoped execution unavailable")
	calls := 0
	driver := &LocalDriver{ScopedT3: attachFunc(func(context.Context, workerproto.ExecutionPackage) (T3Control, error) { calls++; return nil, refused })}
	// A nil host control would panic if any path fell back after attachment failed.
	ops := []func() error{
		func() error { _, err := driver.ObserveThread(ctx, pkg); return err },
		func() error { return driver.CreateThread(ctx, pkg, "/host/path") },
		func() error { return driver.StopThread(ctx, pkg) },
		func() error { return driver.Collect(ctx, pkg, "/host/path") },
		func() error { return driver.Settle(ctx, pkg) },
		func() error { return driver.CollectFailure(ctx, pkg, "/host/path", "failed") },
		func() error { return driver.Warn(ctx, pkg, domain.ThrottleCommand{}) },
		func() error { _, err := driver.Checkpoint(ctx, pkg, domain.ThrottleCommand{}); return err },
		func() error { return driver.Resume(ctx, pkg, domain.ThrottleCommand{}) },
	}
	for i, op := range ops {
		if err := op(); !errors.Is(err, refused) {
			t.Fatalf("operation %d: %v", i, err)
		}
	}
	if calls != len(ops) {
		t.Fatalf("attachment calls=%d", calls)
	}
	driver.ScopedT3 = nil
	if _, err := driver.ObserveThread(ctx, pkg); err == nil {
		t.Fatal("missing attachment accepted")
	}
	driver.ScopedT3 = attachFunc(func(context.Context, workerproto.ExecutionPackage) (T3Control, error) { return nil, nil })
	if _, err := driver.ObserveThread(ctx, pkg); err == nil {
		t.Fatal("nil scoped control accepted")
	}
}

func TestScopedDispatchMapsPathsAndKeepsHostControlUntouched(t *testing.T) {
	ctx := context.Background()
	pkg := testPackage()
	pkg.Environment.DirectoryBindings = []directoryresource.Binding{{}}
	pkg.Prompt = testArtifact("scoped-prompt", "prompt.md", "scoped prompt")
	host := &recordingT3{resolveErr: errors.New("host control must not be used")}
	scoped := &recordingT3{}
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "objects"), 0700); err != nil {
		t.Fatal(err)
	}
	driver := &LocalDriver{Config: LocalDriverConfig{ArtifactRoot: root, StopTimeout: time.Second}, T3: host,
		Source: mapArtifactSource{pkg.Prompt.ID: []byte("scoped prompt")}, Publisher: &recordingPublisher{}, Now: func() time.Time { return runtimeTestNow },
		ScopedT3: attachFunc(func(_ context.Context, got workerproto.ExecutionPackage) (T3Control, error) {
			if got.Identity.ThreadID != pkg.Identity.ThreadID {
				t.Fatal("wrong execution attachment")
			}
			return scoped, nil
		})}
	if _, err := driver.cacheArtifact(ctx, pkg.Prompt, pkg.Limits.MaxArtifactBytes); err != nil {
		t.Fatal(err)
	}
	if err := driver.CreateThread(ctx, pkg, "/host/owned/workspace"); err != nil {
		t.Fatal(err)
	}
	if len(scoped.created) != 1 || scoped.created[0].WorktreePath != "/workspace" || scoped.created[0].ThreadID != pkg.Identity.ThreadID {
		t.Fatalf("dispatch: %+v", scoped.created)
	}
	if len(scoped.managedProjects) != 1 || scoped.managedProjects[0].WorkspaceRoot != "/workspace" {
		t.Fatalf("project: %+v", scoped.managedProjects)
	}
	if err := driver.Warn(ctx, pkg, domain.ThrottleCommand{Reason: "scoped warning"}); err != nil {
		t.Fatal(err)
	}
	if err := driver.Resume(ctx, pkg, domain.ThrottleCommand{Reason: "scoped resume"}); err != nil {
		t.Fatal(err)
	}
	if err := driver.StopThread(ctx, pkg); err != nil {
		t.Fatal(err)
	}
	if len(scoped.warns) != 1 || len(scoped.resumes) != 1 || scoped.stops != 1 {
		t.Fatal("control operation escaped scope")
	}
	if driver.T3 != host || len(host.created) != 0 || len(host.managedProjects) != 0 || host.stops != 0 {
		t.Fatal("shared driver/control mutated")
	}
	// Thread routing is not permission to prepare a directory execution yet.
	if _, err := driver.Prepare(ctx, pkg); err == nil {
		t.Fatal("directory preparation guard opened prematurely")
	}
}
