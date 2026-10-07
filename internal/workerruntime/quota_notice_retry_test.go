package workerruntime

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type retryNoticeDriver struct{ *watchdogDrainDriver }

func (d *retryNoticeDriver) StopThread(ctx context.Context, pkg workerproto.ExecutionPackage) error {
	if err := d.fakeDriver.StopThread(ctx, pkg); err != nil {
		return err
	}
	d.state = backlog.DispatchThreadStopped
	return nil
}

func TestQuotaRetryNoticeGetsFullEscalationWindowAfterRestart(t *testing.T) {
	now := runtimeTestNow
	base := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	r := runningRuntime(t, base, guard, &now)
	d := &retryNoticeDriver{&watchdogDrainDriver{fakeDriver: base, state: backlog.DispatchThreadActive,
		noticeErr: fmt.Errorf("request definitely not delivered")}}
	r.driver = d
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	requested := journalRecord(t, r).LocalThrottle.RequestedAt
	reopen := func() {
		r = reopenTestRuntime(t, r.journal.root, base)
		r.config.Now = func() time.Time { return now }
		r.config.Quota = guard
		r.config.PauseEscalation = 30 * time.Second
		r.driver = d
	}
	now = now.Add(time.Minute)
	reopen()
	d.noticeErr = nil
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.notices != 2 || d.stopCalls != 0 {
		t.Fatalf("retry did not send notice first: notices=%d stops=%d", d.notices, d.stopCalls)
	}
	// Reopen again to require the successful send time to be durable.
	reopen()
	now = now.Add(time.Second)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.stopCalls != 0 {
		t.Fatalf("hard stop one second after successful retry: stops=%d escalation=%s", d.stopCalls, r.config.PauseEscalation)
	}
	now = now.Add(29 * time.Second)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	record := journalRecord(t, r)
	if d.stopCalls != 1 || d.notices != 2 || record.Phase != PhaseStopped {
		t.Fatalf("stop at escalation boundary: stops=%d notices=%d phase=%s", d.stopCalls, d.notices, record.Phase)
	}
	if !record.LocalThrottle.RequestedAt.Equal(requested) {
		t.Fatal("retry changed original pause intent time")
	}
}
