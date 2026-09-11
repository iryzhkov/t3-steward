package workerruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestRuntimeRestartReconcilesEveryInFlightBoundary(t *testing.T) {
	t.Run("preparing", func(t *testing.T) {
		root := t.TempDir()
		driver := &fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true}
		runtime := newClaimedRuntime(t, root, driver)
		if err := runtime.markPhase("assignment-1", PhasePreparing, "", "", ""); err != nil {
			t.Fatal(err)
		}
		if err := reopenTestRuntime(t, root, driver).Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		assertPhase(t, runtime, PhasePrepared)
		if driver.prepareCalls != 0 {
			t.Fatalf("preparation repeated despite published workspace: %d", driver.prepareCalls)
		}
	})
	t.Run("dispatching", func(t *testing.T) {
		root := t.TempDir()
		driver := &fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true, observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive}}
		runtime := newClaimedRuntime(t, root, driver)
		if err := runtime.markPhase("assignment-1", PhaseDispatching, "", driver.workspace, ""); err != nil {
			t.Fatal(err)
		}
		if err := reopenTestRuntime(t, root, driver).Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		assertPhase(t, runtime, PhaseRunning)
		if driver.createCalls != 0 {
			t.Fatalf("observed thread was recreated: %d", driver.createCalls)
		}
	})
	t.Run("stopping", func(t *testing.T) {
		root := t.TempDir()
		driver := &fakeDriver{workspace: filepath.Join(root, "workspace")}
		runtime := newClaimedRuntime(t, root, driver)
		if err := runtime.markPhase("assignment-1", PhaseStopping, "", driver.workspace, "thread-1"); err != nil {
			t.Fatal(err)
		}
		if err := reopenTestRuntime(t, root, driver).Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		assertPhase(t, runtime, PhaseStopped)
		if driver.stopCalls != 1 {
			t.Fatalf("stop calls = %d", driver.stopCalls)
		}
	})
	t.Run("collecting", func(t *testing.T) {
		root := t.TempDir()
		driver := &fakeDriver{workspace: filepath.Join(root, "workspace")}
		runtime := newClaimedRuntime(t, root, driver)
		if err := runtime.markPhase("assignment-1", PhaseCollecting, "", driver.workspace, "thread-1"); err != nil {
			t.Fatal(err)
		}
		if err := reopenTestRuntime(t, root, driver).Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		assertPhase(t, runtime, PhaseCompleted)
		if driver.collectCalls != 1 || driver.cleanupCalls != 1 {
			t.Fatalf("collect=%d cleanup=%d", driver.collectCalls, driver.cleanupCalls)
		}
	})
	t.Run("pending checkpoint", func(t *testing.T) {
		root := t.TempDir()
		driver := &fakeDriver{workspace: filepath.Join(root, "workspace")}
		runtime := newClaimedRuntime(t, root, driver)
		if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
			t.Fatal(err)
		}
		command := testThrottle(runtime, domain.ThrottleCommandDrain, "drain-crash")
		if err := runtime.journal.update(func(state *journalState) error {
			record := state.Attempts["assignment-1"]
			record.PendingThrottle = &command
			state.Attempts["assignment-1"] = record
			state.Sequence++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := reopenTestRuntime(t, root, driver).Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		assertPhase(t, runtime, PhaseStopped)
		if driver.checkpointCalls != 1 {
			t.Fatalf("checkpoint calls = %d", driver.checkpointCalls)
		}
	})
}

func TestRuntimeRejectsChangedCommandReplayAndCorruptJournal(t *testing.T) {
	root := t.TempDir()
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace")}
	runtime := newClaimedRuntime(t, root, driver)
	command := testCommand(t, runtime, domain.WorkerCommandPrepare, "prepare")
	if _, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{command}}); err != nil {
		t.Fatal(err)
	}
	changed := command
	changed.Kind = domain.WorkerCommandStop
	if _, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{changed}}); err == nil {
		t.Fatal("changed command replay accepted")
	}

	corruptRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(corruptRoot, "journal.json"), []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenJournal(corruptRoot, "normandy", "worker-1", 9); err == nil {
		t.Fatal("corrupt journal accepted")
	}
}

func TestOpenJournalAdoptsNewerCoordinatorEpoch(t *testing.T) {
	root := t.TempDir()
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace")}
	runtime := newClaimedRuntime(t, root, driver)
	before, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}

	journal, err := OpenJournal(root, "normandy", "worker-1", 10)
	if err != nil {
		t.Fatal(err)
	}
	after, err := journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if after.CoordinatorEpoch != 10 {
		t.Fatalf("coordinator epoch = %d, want 10", after.CoordinatorEpoch)
	}
	if after.Sequence != before.Sequence {
		t.Fatalf("sequence = %d, want %d", after.Sequence, before.Sequence)
	}
	if _, ok := after.Attempts["assignment-1"]; !ok {
		t.Fatal("durable attempt was not preserved")
	}
	if _, err := OpenJournal(root, "normandy", "worker-1", 9); err == nil {
		t.Fatal("stale coordinator epoch was accepted")
	}
}

func assertPhase(t *testing.T, runtime *Runtime, want Phase) {
	t.Helper()
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got := state.Attempts["assignment-1"].Phase; got != want {
		t.Fatalf("phase = %q, want %q", got, want)
	}
}
