package workerruntime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A drain notice that can never be delivered must not keep a thread working
// against a stopped bucket forever. The retry is still tried first, so a
// notice that eventually lands gets its full window; only a retry that also
// fails after the escalation window falls through to the stop, which is the
// bound the base had when the notice failed inside Checkpoint.
func TestStoppedBucketUndeliverableNoticeEscalates(t *testing.T) {
	now := runtimeTestNow
	base := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	r := runningRuntime(t, base, guard, &now)
	r.config.PauseEscalation = 30 * time.Second
	d := &retryNoticeDriver{&watchdogDrainDriver{fakeDriver: base, state: backlog.DispatchThreadActive,
		noticeErr: fmt.Errorf("T3 send failed")}}
	r.driver = d
	for pass := 0; pass < 3; pass++ {
		if err := r.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		if pass == 0 && d.stopCalls != 0 {
			t.Fatalf("first contact stopped the thread without a notice window: stops=%d", d.stopCalls)
		}
		now = now.Add(time.Minute)
	}
	record := journalRecord(t, r)
	if d.stopCalls != 1 || d.notices != 2 || record.Phase != PhaseStopped {
		t.Fatalf("undeliverable notice: notices=%d stops=%d phase=%s; want one retry, then the stop", d.notices, d.stopCalls, record.Phase)
	}
	if record.LocalThrottle == nil || record.LocalThrottle.Kind != domain.ThrottleCommandHardStop {
		t.Fatalf("stop recorded as %+v, want the hard stop intent", record.LocalThrottle)
	}
}

// A draining bucket never escalates, however often the notice fails: it
// keeps retrying the notice and leaves the thread working.
func TestDrainingBucketUndeliverableNoticeOnlyRetries(t *testing.T) {
	now := runtimeTestNow
	base := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
	pause := stoppedPause()
	pause.Phase = domain.PhaseDraining
	guard := &fakeQuotaGuard{pause: pause, pauseNeeded: true}
	r := runningRuntime(t, base, guard, &now)
	r.config.PauseEscalation = 30 * time.Second
	d := &retryNoticeDriver{&watchdogDrainDriver{fakeDriver: base, state: backlog.DispatchThreadActive,
		noticeErr: fmt.Errorf("T3 send failed")}}
	r.driver = d
	for pass := 0; pass < 3; pass++ {
		if err := r.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Minute)
	}
	if d.stopCalls != 0 || d.notices != 3 || journalRecord(t, r).Phase != PhaseRunning {
		t.Fatalf("draining bucket: notices=%d stops=%d; want retries only", d.notices, d.stopCalls)
	}
}

// A drain request written by the previous binary went out through the
// blocking Checkpoint, which sent the notice before it waited, and has no
// delivery fields. After an upgrade it counts as sent: no second notice, and
// a stopped bucket escalates from the original request time.
func TestLegacyDrainRecordCountsAsSent(t *testing.T) {
	for _, tc := range []struct {
		name  string
		phase domain.Phase
		stops int
	}{{"stopped bucket escalates", domain.PhaseStopped, 1}, {"draining bucket observes", domain.PhaseDraining, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			now := runtimeTestNow
			base := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
			pause := stoppedPause()
			pause.Phase = tc.phase
			r := runningRuntime(t, base, &fakeQuotaGuard{pause: pause, pauseNeeded: true}, &now)
			r.config.PauseEscalation = 30 * time.Second
			d := &retryNoticeDriver{&watchdogDrainDriver{fakeDriver: base, state: backlog.DispatchThreadActive}}
			r.driver = d
			legacyAt := now.Add(-10 * time.Minute)
			if err := r.journal.update(func(s *journalState) error {
				record := s.Attempts["assignment-1"]
				record.LocalThrottle = &LocalThrottleRequest{Kind: domain.ThrottleCommandDrain, Bucket: sevenDay,
					Phase: domain.PhaseDraining, UsedPercent: 91, Reason: "legacy", RequestedAt: legacyAt}
				s.Attempts["assignment-1"] = record
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := r.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if d.notices != 0 || d.stopCalls != tc.stops {
				t.Fatalf("legacy drain record: notices=%d stops=%d; want no second notice and %d stops", d.notices, d.stopCalls, tc.stops)
			}
		})
	}
}

// Liveness is advisory. A journal that cannot take the liveness write must
// not turn an idle reconcile pass, which the base finished without writing,
// into a failed exchange.
func TestLivenessWriteFailureDoesNotFailReconcile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	root := t.TempDir()
	now := runtimeTestNow
	runtime := newTestRuntimeWithClock(t, root, &fakeDriver{}, func() time.Time { return now })
	if err := os.Chmod(root, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(root, 0o700)
	for pass := 0; pass < 3; pass++ {
		now = now.Add(2 * time.Minute)
		if err := runtime.Reconcile(context.Background()); err != nil {
			t.Fatalf("idle reconcile pass %d failed: %v", pass, err)
		}
	}
}

// Liveness covers a brief lease lapse, not an abandoned assignment: a lease
// that expired longer than OwnershipMaxAge ago returns the thread to the
// watchdog even while the worker process keeps reconciling.
func TestOwnershipLivenessIsBoundedPastTheLease(t *testing.T) {
	now := runtimeTestNow
	for _, tc := range []struct {
		name  string
		lease time.Time
		stale bool
	}{
		{"lapse within the bound", now.Add(-30 * time.Minute), false},
		{"lapse at the bound", now.Add(-OwnershipMaxAge), false},
		{"abandoned lease", now.Add(-OwnershipMaxAge - time.Second), true},
		{"lease expired a day ago", now.Add(-24 * time.Hour), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := AttemptRecord{UpdatedAt: now}
			record.Assignment.LeaseExpiresAt = tc.lease
			if got := ownershipStale(record, now, now); (got != "") != tc.stale {
				t.Fatalf("staleness = %q, want stale %v", got, tc.stale)
			}
		})
	}
}
