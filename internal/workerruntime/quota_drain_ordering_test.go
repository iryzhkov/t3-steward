package workerruntime

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type delayedTurnLookupDriver struct {
	*watchdogDrainDriver
	turnErr error
}

func (d *delayedTurnLookupDriver) ObserveThreadTurn(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, string, error) {
	if d.turnErr != nil {
		return backlog.DispatchThreadStopped, "", d.turnErr
	}
	return backlog.DispatchThreadStopped, "turn-drained", nil
}

func TestWatchdogTransientTurnLookupDefersCompletedPause(t *testing.T) {
	now := runtimeTestNow
	base := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	r := runningRuntime(t, base, guard, &now)
	d := &delayedTurnLookupDriver{watchdogDrainDriver: &watchdogDrainDriver{fakeDriver: base, state: backlog.DispatchThreadStopped, complete: true}, turnErr: fmt.Errorf("temporary turn lookup failure")}
	r.driver = d
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	guard.pauseNeeded = false
	guard.resumeOK = true
	guard.resumeWhy = "recovered"
	// Failure may last across exchanges or a restart. No provider turn may start
	// before the stopped turn's completion can be checked.
	for range 2 {
		if err := r.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		if d.resumeCalls != 0 || d.collectCalls != 0 {
			t.Fatalf("unknown completed turn resumed/collected: resumes=%d collects=%d", d.resumeCalls, d.collectCalls)
		}
	}
	d.turnErr = nil
	for range 2 {
		if err := r.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if d.resumeCalls != 0 || d.collectCalls != 1 {
		t.Fatalf("completed turn not collected after lookup recovery: resumes=%d collects=%d record=%+v", d.resumeCalls, d.collectCalls, journalRecord(t, r))
	}
}

func TestWatchdogDrainStopsAfterRecoveredActiveObservation(t *testing.T) {
	now := runtimeTestNow
	base := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	r := runningRuntime(t, base, guard, &now)
	d := &watchdogDrainDriver{fakeDriver: base, state: backlog.DispatchThreadActive}
	r.driver = d
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec := journalRecord(t, r); rec.LocalThrottle == nil || !rec.LocalThrottle.DrainNoticeSent {
		t.Fatalf("drain not sent: %+v", rec)
	}
	guard.pauseNeeded = false
	guard.resumeOK = true
	guard.resumeWhy = "bucket recovered"
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	d.state = backlog.DispatchThreadStopped
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec := journalRecord(t, r)
	if rec.LocalThrottle == nil || rec.Phase != PhaseStopped || d.collectCalls != 0 {
		t.Fatalf("queued drain collected after recovery: phase=%s pause=%+v collects=%d", rec.Phase, rec.LocalThrottle, d.collectCalls)
	}
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.resumeCalls != 1 || journalRecord(t, r).LocalThrottle != nil {
		t.Fatalf("late stop not resumed: %+v", journalRecord(t, r))
	}
}
