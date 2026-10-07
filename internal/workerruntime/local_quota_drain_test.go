package workerruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type quotaDrainControl struct {
	recordingT3
	waits   int
	warnErr error
}

func (c *quotaDrainControl) WaitStopped(context.Context, string, time.Duration) (*domain.Thread, bool, error) {
	c.waits++
	return c.thread, false, errors.New("turn remains active beyond stop timeout")
}

func (c *quotaDrainControl) WarnThread(ctx context.Context, thread domain.Thread, warning domain.Warning) error {
	if c.warnErr != nil {
		return c.warnErr
	}
	return c.recordingT3.WarnThread(ctx, thread, warning)
}

func TestLocalQuotaDrainSendsWithoutWaitingAndKeepsCoordinatorCheckpointBlocking(t *testing.T) {
	pkg := testPackage()
	control := &quotaDrainControl{recordingT3: recordingT3{thread: &domain.Thread{ID: pkg.Identity.ThreadID, Running: true}}}
	publisher := &recordingPublisher{}
	driver := &LocalDriver{Config: LocalDriverConfig{RunsRoot: t.TempDir(), StopTimeout: time.Second}, T3: control, Publisher: publisher}
	command := domain.ThrottleCommand{Kind: domain.ThrottleCommandDrain, Reason: "host bucket draining"}
	if err := driver.RequestQuotaDrain(context.Background(), pkg, command); err != nil {
		t.Fatal(err)
	}
	if control.waits != 0 || len(control.warns) != 1 || publisher.checkpoints != 0 {
		t.Fatalf("non-blocking drain waited or published early: waits=%d warns=%v published=%d", control.waits, control.warns, publisher.checkpoints)
	}
	warning := control.warns[0]
	if warning.Kind != domain.ActionDrain || !strings.Contains(warning.Text, command.Reason) || !strings.Contains(warning.Text, "backlog status: done") || !strings.Contains(warning.Text, "backlog status: continue") {
		t.Fatalf("drain notice omitted runtime completion contract: %+v", warning)
	}
	// The separate coordinator throttle entry point must retain its stop wait.
	if _, err := driver.Checkpoint(context.Background(), pkg, command); err == nil {
		t.Fatal("coordinator checkpoint skipped stop wait")
	}
	if control.waits != 1 || len(control.warns) != 2 {
		t.Fatalf("coordinator checkpoint waits=%d warns=%d", control.waits, len(control.warns))
	}
}

func TestLocalQuotaCheckpointMissingThenAvailable(t *testing.T) {
	pkg := testPackage()
	publisher := &recordingPublisher{}
	driver := &LocalDriver{Config: LocalDriverConfig{RunsRoot: t.TempDir()}, Publisher: publisher}
	checkpoint, err := driver.ReadQuotaCheckpoint(context.Background(), pkg)
	if err != nil || checkpoint != nil || publisher.checkpoints != 0 {
		t.Fatalf("missing checkpoint blocked pause: %v %v publications=%d", checkpoint, err, publisher.checkpoints)
	}
	path := filepath.Join(driver.workspacePath(pkg), "workspace", ".t3", "checkpoint.md")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("saved progress\n"), 0600); err != nil {
		t.Fatal(err)
	}
	checkpoint, err = driver.ReadQuotaCheckpoint(context.Background(), pkg)
	if err != nil || checkpoint == nil || checkpoint.ArtifactID != "checkpoint-1" || publisher.checkpoints != 1 {
		t.Fatalf("stopped checkpoint not published: %v %v publications=%d", checkpoint, err, publisher.checkpoints)
	}
	// A malformed file is not mistaken for absence and never published.
	pkg.Limits.MaxArtifactBytes = 1
	if _, err := driver.ReadQuotaCheckpoint(context.Background(), pkg); err == nil {
		t.Fatal("oversized checkpoint accepted")
	}
	if publisher.checkpoints != 1 {
		t.Fatal("oversized checkpoint published")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "outside.md")
	if err := os.WriteFile(target, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.ReadQuotaCheckpoint(context.Background(), pkg); err == nil {
		t.Fatal("symlink checkpoint accepted")
	}
	if publisher.checkpoints != 1 {
		t.Fatal("symlink checkpoint published")
	}
}

func TestLocalQuotaDrainPropagatesNoticeFailure(t *testing.T) {
	pkg := testPackage()
	refused := errors.New("notice rejected")
	control := &quotaDrainControl{recordingT3: recordingT3{thread: &domain.Thread{ID: pkg.Identity.ThreadID, Running: true}}, warnErr: refused}
	driver := &LocalDriver{T3: control}
	if err := driver.RequestQuotaDrain(context.Background(), pkg, domain.ThrottleCommand{}); !errors.Is(err, refused) {
		t.Fatalf("notice failure=%v", err)
	}
	if control.waits != 0 {
		t.Fatal("failed notice started a stop wait")
	}
	control.warnErr = nil
	control.thread = nil
	if err := driver.RequestQuotaDrain(context.Background(), pkg, domain.ThrottleCommand{}); err == nil {
		t.Fatal("missing thread accepted")
	}
}

func TestLocalQuotaDrainOperationsPreserveScopedAttachmentFence(t *testing.T) {
	pkg := testPackage()
	pkg.Environment.DirectoryBindings = []directoryresource.Binding{{}}
	refused := errors.New("identity-fenced attachment refused")
	for _, tc := range []struct {
		name     string
		provider ExecutionT3Provider
	}{
		{"missing", nil},
		{"refused", attachFunc(func(context.Context, workerproto.ExecutionPackage) (T3Control, error) { return nil, refused })},
		{"nil control", attachFunc(func(context.Context, workerproto.ExecutionPackage) (T3Control, error) { return nil, nil })},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Nil shared T3 and publisher make host fallback observable as a panic.
			driver := &LocalDriver{ScopedT3: tc.provider}
			if err := driver.RequestQuotaDrain(context.Background(), pkg, domain.ThrottleCommand{}); err == nil {
				t.Fatal("unfenced drain accepted")
			}
			if _, err := driver.ReadQuotaCheckpoint(context.Background(), pkg); err == nil {
				t.Fatal("unfenced checkpoint read accepted")
			}
		})
	}
	host := &quotaDrainControl{}
	scoped := &quotaDrainControl{recordingT3: recordingT3{thread: &domain.Thread{ID: pkg.Identity.ThreadID, Running: true}}}
	calls := 0
	driver := &LocalDriver{Config: LocalDriverConfig{RunsRoot: t.TempDir()}, T3: host, Publisher: &recordingPublisher{},
		ScopedT3: attachFunc(func(_ context.Context, got workerproto.ExecutionPackage) (T3Control, error) {
			calls++
			if got.Identity != pkg.Identity {
				t.Fatal("wrong execution identity attached")
			}
			return scoped, nil
		})}
	if err := driver.RequestQuotaDrain(context.Background(), pkg, domain.ThrottleCommand{}); err != nil {
		t.Fatal(err)
	}
	if checkpoint, err := driver.ReadQuotaCheckpoint(context.Background(), pkg); err != nil || checkpoint != nil {
		t.Fatalf("scoped missing checkpoint: %v %v", checkpoint, err)
	}
	if calls != 2 || len(scoped.warns) != 1 || len(host.warns) != 0 || scoped.waits != 0 || host.waits != 0 || driver.T3 != host {
		t.Fatalf("scoped fence violated: calls=%d scoped=%+v host=%+v", calls, scoped, host)
	}
}
