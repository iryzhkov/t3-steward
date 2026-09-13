package workerruntime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestConfirmedStopRetention(t *testing.T) {
	root := t.TempDir()
	now := runtimeTestNow
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace")}
	runtime := newClaimedRuntimeWithClock(t, root, driver, func() time.Time { return now })
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	stop := testCommand(t, runtime, domain.WorkerCommandStop, "stop")
	if _, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{stop}}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(DefaultRetention - time.Minute)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.cleanupCalls != 0 {
		t.Fatal("removed evidence before retention")
	}
	now = now.Add(2 * time.Minute)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := runtime.journal.snapshot()
	if err != nil || len(state.Attempts) != 0 || driver.cleanupCalls != 1 {
		t.Fatalf("state=%+v cleanup=%d err=%v", state, driver.cleanupCalls, err)
	}
}

func TestExplicitStopReleasesOnlyAfterConfirmedEffectAcrossRestart(t *testing.T) {
	for _, phase := range []Phase{PhaseRunning, PhaseStopped} {
		t.Run(string(phase), func(t *testing.T) {
			root := t.TempDir()
			driver := &fakeDriver{workspace: filepath.Join(root, "workspace"), stopErr: errors.New("lost response"),
				observations: []backlog.DispatchThreadState{backlog.DispatchThreadStopped}}
			runtime := newClaimedRuntime(t, root, driver)
			if err := runtime.markPhase("assignment-1", phase, "", driver.workspace, "thread-1"); err != nil {
				t.Fatal(err)
			}
			stop := testCommand(t, runtime, domain.WorkerCommandStop, "stop")
			if _, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{stop}}); err != nil {
				t.Fatal(err)
			}
			check := func(want domain.AssignmentState) {
				t.Helper()
				snapshot, err := runtime.Snapshot(context.Background())
				if err != nil || len(snapshot.Assignments) != 1 || snapshot.Assignments[0].State != want {
					t.Fatalf("snapshot=%+v err=%v want=%s", snapshot, err, want)
				}
				if want == domain.AssignmentReleased && snapshot.Assignments[0].Control != domain.ControlStopped {
					t.Fatal("released execution reported running")
				}
			}
			check(domain.AssignmentClaimed)
			runtime = reopenTestRuntime(t, root, driver)
			check(domain.AssignmentClaimed)
			driver.stopErr = nil
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			check(domain.AssignmentReleased)
			calls := driver.stopCalls
			runtime = reopenTestRuntime(t, root, driver)
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			check(domain.AssignmentReleased)
			if driver.stopCalls != calls || driver.createCalls != 0 || driver.collectCalls != 0 {
				t.Fatalf("unexpected effects: stops=%d creates=%d collects=%d", driver.stopCalls, driver.createCalls, driver.collectCalls)
			}
		})
	}
}
