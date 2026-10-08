package workerruntime

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A coordinator may report a park for an assignment this worker holds no
// record of, and then omit it. Removing that park must not write a record into
// the journal, or every later read, including a restart, refuses the journal.
func TestParkRemovalForAbsentAttemptKeepsJournalReadable(t *testing.T) {
	root := t.TempDir()
	driver := &fakeDriver{}
	runtime := newTestRuntime(t, root, driver)
	if err := runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ParkedReported: true, Parked: []workerproto.ParkedAssignment{
		{AssignmentID: "gone", AssignmentEpoch: 1, AttemptID: "old", AttemptRevision: 1},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ParkedReported: true}); err != nil {
		t.Fatal(err)
	}
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatalf("removing a park broke the journal: %v", err)
	}
	if _, exists := state.Attempts["gone"]; exists || len(state.Parked) != 0 {
		t.Fatalf("park removal left attempts=%v parked=%v", state.Attempts, state.Parked)
	}
	restarted := reopenTestRuntime(t, root, driver)
	if _, err := restarted.Snapshot(context.Background()); err != nil {
		t.Fatalf("restart after park removal: %v", err)
	}
}

// Pruning an attempt drops the park the coordinator last reported for it, and
// a park reported again for the pruned attempt and then omitted is harmless.
func TestPrunedAttemptDropsItsParkAndALaterOmissionKeepsJournalReadable(t *testing.T) {
	root := t.TempDir()
	now := runtimeTestNow
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace"), observations: []backlog.DispatchThreadState{backlog.DispatchThreadStopped}}
	runtime := newClaimedRuntimeWithClock(t, root, driver, func() time.Time { return now })
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := mustRecord(t, runtime, "assignment-1").Phase; got != PhaseCompleted {
		t.Fatalf("phase = %q, want completed before the delayed park arrives", got)
	}
	// A delayed coordinator statement parks the attempt that already finished.
	if err := runtime.ApplyParkedAssignments(parkedStatement(5)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(DefaultRetention + time.Minute)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if driver.cleanupCalls != 1 || len(state.Attempts) != 0 {
		t.Fatalf("cleanup=%d remaining=%d; want the record pruned", driver.cleanupCalls, len(state.Attempts))
	}
	if _, parked := state.Parked["assignment-1"]; parked {
		t.Fatal("pruning kept the pruned attempt's park")
	}
	// The coordinator still names it, then removes it.
	if err := runtime.ApplyParkedAssignments(parkedStatement(5)); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ParkedReported: true}); err != nil {
		t.Fatal(err)
	}
	if state, err = runtime.journal.snapshot(); err != nil {
		t.Fatalf("omitting the pruned attempt's park broke the journal: %v", err)
	}
	if len(state.Attempts) != 0 {
		t.Fatalf("park removal recreated attempts %v", state.Attempts)
	}
	restarted := reopenTestRuntime(t, root, driver)
	if _, err := restarted.Snapshot(context.Background()); err != nil {
		t.Fatalf("restart after pruned park removal: %v", err)
	}
}

// The journal refuses to publish a state that its own reader would refuse, so a
// faulty update fails alone instead of making the journal unreadable.
func TestJournalRefusesToPublishAnInvalidAttemptRecord(t *testing.T) {
	root := t.TempDir()
	runtime := newClaimedRuntime(t, root, &fakeDriver{})
	before, err := os.ReadFile(runtime.journal.path)
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*journalState){
		"zero record": func(state *journalState) { state.Attempts["gone"] = AttemptRecord{} },
		"empty key": func(state *journalState) {
			state.Attempts[""] = state.Attempts["assignment-1"]
		},
		"foreign package": func(state *journalState) {
			record := state.Attempts["assignment-1"]
			record.Package.Package.Identity.AssignmentID = "other"
			state.Attempts["assignment-1"] = record
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := runtime.journal.update(func(state *journalState) error {
				change(state)
				state.Sequence++
				return nil
			})
			if err == nil {
				t.Fatal("journal published an invalid attempt record")
			}
			after, readErr := os.ReadFile(runtime.journal.path)
			if readErr != nil || !bytes.Equal(before, after) {
				t.Fatalf("refused update changed the journal: %v", readErr)
			}
			if _, err := runtime.journal.snapshot(); err != nil {
				t.Fatalf("journal unreadable after a refused update: %v", err)
			}
		})
	}
}
