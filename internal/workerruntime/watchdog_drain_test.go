package workerruntime

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type watchdogDrainDriver struct {
	*fakeDriver
	state     backlog.DispatchThreadState
	notices   int
	noticeErr error
	complete  bool
}

func (d *watchdogDrainDriver) ObserveThread(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, error) {
	return d.state, nil
}
func (d *watchdogDrainDriver) ObserveThreadTurn(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, string, error) {
	return d.state, "turn-drained", nil
}
func (d *watchdogDrainDriver) Checkpoint(context.Context, workerproto.ExecutionPackage, domain.ThrottleCommand) (*domain.CheckpointMetadata, error) {
	d.checkpointCalls++
	time.Sleep(80 * time.Millisecond)
	return nil, fmt.Errorf("checkpoint turn did not stop before deadline")
}
func (d *watchdogDrainDriver) RequestQuotaDrain(context.Context, workerproto.ExecutionPackage, domain.ThrottleCommand) error {
	d.notices++
	return d.noticeErr
}
func (d *watchdogDrainDriver) ReadQuotaCheckpoint(context.Context, workerproto.ExecutionPackage) (*domain.CheckpointMetadata, error) {
	return nil, nil
}
func (d *watchdogDrainDriver) QuotaPauseCompleted(context.Context, workerproto.ExecutionPackage, string) (bool, error) {
	return d.complete, nil
}

func TestWatchdogDrainDoesNotWaitAcrossAttempts(t *testing.T) {
	now := runtimeTestNow
	base := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	guard.pause.Phase = domain.PhaseDraining
	r := runningRuntime(t, base, guard, &now)
	driver := &watchdogDrainDriver{fakeDriver: base, state: backlog.DispatchThreadActive}
	r.driver = driver
	if err := r.journal.update(func(s *journalState) error {
		original := s.Attempts["assignment-1"]
		for i := 2; i <= 3; i++ {
			rec := original
			rec.Assignment.ID = fmt.Sprintf("assignment-%d", i)
			rec.Package.Package.Identity.AssignmentID = rec.Assignment.ID
			rec.Package.Package.Identity.AttemptID = fmt.Sprintf("attempt-%d", i)
			rec.Package.Package.Identity.ThreadID = fmt.Sprintf("thread-%d", i)
			s.Attempts[rec.Assignment.ID] = rec
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed >= 2*time.Second || driver.notices != 3 || driver.checkpointCalls != 0 {
		t.Fatalf("reconcile waited %s; notices=%d, want 3 without stop waits", elapsed, driver.notices)
	}
	if rec := journalRecord(t, r); rec.Phase != PhaseRunning || rec.LocalThrottle == nil || rec.LocalThrottle.StoppedAt != nil {
		t.Fatalf("request not recorded: %+v", rec)
	}
	driver.state = backlog.DispatchThreadStopped
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec := journalRecord(t, r)
	if rec.Phase != PhaseStopped || rec.LocalThrottle == nil || rec.LocalThrottle.StoppedAt == nil || rec.LocalThrottle.Checkpoint != nil || driver.collectCalls != 0 {
		t.Fatalf("missing checkpoint must still park: %+v", rec)
	}
	guard.pauseNeeded = false
	guard.resumeOK = true
	guard.resumeWhy = "bucket recovered"
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.resumeCalls != 3 || journalRecord(t, r).LocalThrottle != nil {
		t.Fatalf("resumes=%d record=%+v", driver.resumeCalls, journalRecord(t, r))
	}
}

func TestWatchdogStoppedTurnWithoutPauseIsParkedUnlessComplete(t *testing.T) {
	for _, tc := range []struct {
		name               string
		draining, complete bool
	}{
		{"drained", true, false}, {"finished", true, true}, {"ordinary", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := runtimeTestNow
			base := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
			guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: tc.draining}
			r := runningRuntime(t, base, guard, &now)
			driver := &watchdogDrainDriver{fakeDriver: base, state: backlog.DispatchThreadStopped, complete: tc.complete}
			r.driver = driver
			if err := r.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			rec := journalRecord(t, r)
			if tc.draining && !tc.complete {
				if rec.Phase != PhaseStopped || rec.LocalThrottle == nil || rec.LocalThrottle.Reason != "turn ended during a host quota drain" || driver.collectCalls != 0 {
					t.Fatalf("unrecorded drain collected instead of parked: %+v; collects=%d", rec, driver.collectCalls)
				}
				guard.pauseNeeded = false
				guard.resumeOK = true
				guard.resumeWhy = "recovered"
				if err := r.Reconcile(context.Background()); err != nil {
					t.Fatal(err)
				}
				if driver.resumeCalls != 1 || journalRecord(t, r).Phase != PhaseRunning {
					t.Fatalf("not resumed: %+v", journalRecord(t, r))
				}
			} else if driver.collectCalls != 1 || rec.Phase != PhaseCompleted || rec.LocalThrottle != nil {
				t.Fatalf("finished turn not collected: %+v collects=%d", rec, driver.collectCalls)
			}
		})
	}
}

func TestWatchdogDrainRetriesAfterSendFailureAndRestart(t *testing.T) {
	now := runtimeTestNow
	base := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	guard.pause.Phase = domain.PhaseDraining
	r := runningRuntime(t, base, guard, &now)
	d := &watchdogDrainDriver{fakeDriver: base, state: backlog.DispatchThreadActive, noticeErr: fmt.Errorf("send failed")}
	r.driver = d
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec := journalRecord(t, r)
	if d.notices != 1 || rec.LocalThrottle == nil || rec.LocalThrottle.DrainNoticeSent {
		t.Fatalf("failed send lost retry: notices=%d pause=%+v", d.notices, rec.LocalThrottle)
	}
	r = reopenTestRuntime(t, r.journal.root, base)
	r.config.Quota = guard
	r.driver = d
	d.noticeErr = nil
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.notices != 2 || !journalRecord(t, r).LocalThrottle.DrainNoticeSent {
		t.Fatalf("restart did not retry: notices=%d", d.notices)
	}
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.notices != 2 {
		t.Fatalf("successful notice repeated %d times", d.notices)
	}
}

// The base already preserves a durable drain request when its thread later
// stops, even after quota recovery; the incident's old withdrawal path is fixed.
func TestWatchdogRecordedDrainStopsAfterRecovery(t *testing.T) {
	now := runtimeTestNow
	base := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
	guard := &fakeQuotaGuard{}
	r := runningRuntime(t, base, guard, &now)
	d := &watchdogDrainDriver{fakeDriver: base, state: backlog.DispatchThreadStopped}
	r.driver = d
	if err := r.journal.update(func(s *journalState) error {
		rec := s.Attempts["assignment-1"]
		rec.LocalThrottle = &LocalThrottleRequest{Kind: domain.ThrottleCommandDrain, Bucket: sevenDay, Phase: domain.PhaseDraining, RequestedAt: now}
		s.Attempts["assignment-1"] = rec
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec := journalRecord(t, r); rec.Phase != PhaseStopped || rec.LocalThrottle == nil || rec.LocalThrottle.StoppedAt == nil || d.collectCalls != 0 {
		t.Fatalf("recorded pause lost: %+v", rec)
	}
}
