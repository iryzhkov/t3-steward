package workerruntime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A drain notice belongs to the quota episode that sent it. After the bucket
// recovers, or when a different bucket governs, the old notice is neither
// delivery nor escalation evidence: the new episode sends its own notice and
// gives the thread the full escalation window before the stop.
func TestQuotaNewEpisodeGetsItsOwnNoticeWindow(t *testing.T) {
	newWindow := domain.BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: "new-window"}
	for _, tc := range []struct {
		name    string
		recover bool
	}{
		{name: "after recovery", recover: true},
		{name: "governing bucket changes", recover: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := runtimeTestNow
			base := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
			guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
			r := runningRuntime(t, base, guard, &now)
			r.config.PauseEscalation = 30 * time.Second
			d := &retryNoticeDriver{&watchdogDrainDriver{fakeDriver: base, state: backlog.DispatchThreadActive}}
			r.driver = d
			if err := r.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if tc.recover {
				guard.pauseNeeded = false
				guard.resumeOK = true
				now = now.Add(time.Hour)
				if err := r.Reconcile(context.Background()); err != nil {
					t.Fatal(err)
				}
				guard.pauseNeeded = true
			} else {
				now = now.Add(time.Hour)
			}
			guard.pause.Bucket = newWindow
			guard.pause.Phase = domain.PhaseDraining
			if err := r.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if d.notices != 2 {
				t.Fatalf("new episode sent no notice of its own: notices=%d", d.notices)
			}
			guard.pause.Phase = domain.PhaseStopped
			if err := r.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if d.stopCalls != 0 {
				t.Fatalf("new quota episode hard-stopped the active thread on the old notice: notices=%d stops=%d", d.notices, d.stopCalls)
			}
			record := attemptRecord(t, r)
			if record.LocalThrottle == nil || record.LocalThrottle.Bucket != newWindow || record.LocalThrottle.RecoveredAt != nil {
				t.Fatalf("pause in force is not the new episode's: %+v", record.LocalThrottle)
			}
			// The new episode still escalates once its own window passes.
			now = now.Add(31 * time.Second)
			if err := r.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if d.stopCalls != 1 || d.notices != 2 {
				t.Fatalf("new episode did not escalate after its window: notices=%d stops=%d", d.notices, d.stopCalls)
			}
		})
	}
}

type erroringQuotaGuard struct {
	*fakeQuotaGuard
	err error
}

func (g *erroringQuotaGuard) PauseRequired(ctx context.Context, route domain.ProviderRoute) (QuotaPause, bool, error) {
	if g.err != nil {
		return QuotaPause{}, false, g.err
	}
	return g.fakeQuotaGuard.PauseRequired(ctx, route)
}

// A recovery no pass observed, here because every read failed while the
// bucket was healthy, still ends the episode once its window has reset: the
// next window of the same bucket sends its own notice.
func TestQuotaEpisodeEndsWhenItsWindowResetsUnobserved(t *testing.T) {
	now := runtimeTestNow
	base := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
	inner := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	guard := &erroringQuotaGuard{fakeQuotaGuard: inner}
	r := runningRuntime(t, base, guard, &now)
	r.config.PauseEscalation = 30 * time.Second
	d := &retryNoticeDriver{&watchdogDrainDriver{fakeDriver: base, state: backlog.DispatchThreadActive}}
	r.driver = d
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	guard.err = errors.New("quota store locked")
	now = now.Add(3 * time.Hour) // past the paused window's reset
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	guard.err = nil
	reset := now.Add(5 * time.Hour)
	inner.pause.ResetsAt = &reset
	inner.pause.ObservedAt = now
	inner.pause.Phase = domain.PhaseDraining
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	inner.pause.Phase = domain.PhaseStopped
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.stopCalls != 0 || d.notices != 2 {
		t.Fatalf("new window of the same bucket reused the old episode's notice: notices=%d stops=%d", d.notices, d.stopCalls)
	}
}

// A stop already decided is not demoted to a fresh notice when another
// stopped bucket takes over: the failed stop is retried.
func TestQuotaDecidedStopSurvivesBucketChange(t *testing.T) {
	now := runtimeTestNow
	base := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	r := runningRuntime(t, base, guard, &now)
	r.config.PauseEscalation = 30 * time.Second
	d := &retryNoticeDriver{&watchdogDrainDriver{fakeDriver: base, state: backlog.DispatchThreadActive}}
	r.driver = d
	if err := r.Reconcile(context.Background()); err != nil { // notice
		t.Fatal(err)
	}
	base.stopErr = errors.New("t3 unreachable")
	now = now.Add(31 * time.Second)
	if err := r.Reconcile(context.Background()); err != nil { // escalated stop fails
		t.Fatal(err)
	}
	if d.stopCalls != 1 {
		t.Fatalf("setup: stops=%d", d.stopCalls)
	}
	base.stopErr = nil
	guard.pause.Bucket = domain.BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: "weekly"}
	now = now.Add(time.Second)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.stopCalls != 2 || d.notices != 1 {
		t.Fatalf("failed stop was not retried after the governing bucket changed: notices=%d stops=%d", d.notices, d.stopCalls)
	}
}

// An undelivered notice is no episode of its own: when the governing bucket
// changes, the request keeps its time and still escalates.
func TestQuotaUndeliveredNoticeEscalatesAcrossBucketChanges(t *testing.T) {
	now := runtimeTestNow
	base := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	r := runningRuntime(t, base, guard, &now)
	r.config.PauseEscalation = 30 * time.Second
	d := &retryNoticeDriver{&watchdogDrainDriver{fakeDriver: base, state: backlog.DispatchThreadActive, noticeErr: errors.New("send failed")}}
	r.driver = d
	other := domain.BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: "weekly"}
	for i := 0; i < 20 && d.stopCalls == 0; i++ {
		guard.pause.Bucket = sevenDay
		if i%2 == 1 {
			guard.pause.Bucket = other
		}
		if err := r.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Minute)
	}
	if d.stopCalls == 0 {
		t.Fatalf("undeliverable notice never escalated across bucket changes: notices=%d stops=%d", d.notices, d.stopCalls)
	}
}

// The same bucket draining again after recovery is a new episode as well.
func TestQuotaSameBucketAfterRecoveryGetsNewNotice(t *testing.T) {
	now := runtimeTestNow
	base := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
	draining := stoppedPause()
	draining.Phase = domain.PhaseDraining
	guard := &fakeQuotaGuard{pause: draining, pauseNeeded: true}
	r := runningRuntime(t, base, guard, &now)
	d := &retryNoticeDriver{&watchdogDrainDriver{fakeDriver: base, state: backlog.DispatchThreadActive}}
	r.driver = d
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	guard.pauseNeeded = false
	now = now.Add(time.Hour)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Recovery keeps the intent so that a late stop of the drained turn is
	// still recognised as the worker's own pause.
	if record := attemptRecord(t, r); record.LocalThrottle == nil || record.LocalThrottle.RecoveredAt == nil {
		t.Fatalf("recovery did not mark the retained intent: %+v", record.LocalThrottle)
	}
	guard.pauseNeeded = true
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.notices != 2 {
		t.Fatalf("second drain episode of the same bucket sent no notice: notices=%d", d.notices)
	}
}
