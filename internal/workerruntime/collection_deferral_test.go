package workerruntime

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Field defect 2026-10-08: an attempt whose collection deferred on every pass
// for hours was reported as running, and triage said nothing. The worker now
// keeps when the deferral began and why it last happened, and reports both in
// the turn-end note of the attempt's journal excerpt.
func TestADeferredCollectionIsReportedWithItsStartAndReason(t *testing.T) {
	now := runtimeTestNow
	workspace := filepath.Join(t.TempDir(), "workspace")
	driver := &fakeDriver{workspace: workspace, workspaceReady: true, collectErr: errors.New("T3 is unreachable")}
	runtime := newTestRuntimeWithClock(t, t.TempDir(), driver, func() time.Time { return now })
	seedAttempt(t, runtime, collectingRecord(t, workspace))

	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.collectCalls == 0 {
		t.Fatal("the collecting attempt was not collected")
	}
	started := now
	now = now.Add(15 * time.Minute)
	driver.collectErr = errors.New("thread archive could not be decoded (pass 1 of 3)")
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}

	// The note goes only to a coordinator that asked for turn-end notes.
	snapshot, err := runtime.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if journal := snapshot.Assignments[0].Journal; journal == nil || journal.TurnEnd != "" {
		t.Fatalf("a coordinator that did not ask got the note: %+v", journal)
	}
	runtime.reportTurnEnd = true
	snapshot, err = runtime.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	journal := snapshot.Assignments[0].Journal
	if journal == nil || journal.Phase != string(PhaseCollecting) {
		t.Fatalf("journal excerpt %+v", journal)
	}
	since, reason, ok := domain.ParseCollectionDeferredNote(journal.TurnEnd)
	if !ok || !since.Equal(started) || !strings.Contains(reason, "could not be decoded") {
		t.Fatalf("turn-end note %q: since=%s reason=%q ok=%v; want since %s and the latest reason", journal.TurnEnd, since, reason, ok, started)
	}

	// A collection that succeeds leaves collecting, and the note goes with it.
	driver.collectErr = nil
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, err = runtime.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if journal := snapshot.Assignments[0].Journal; journal == nil || journal.Phase == string(PhaseCollecting) ||
		strings.HasPrefix(journal.TurnEnd, "collection deferred") {
		t.Fatalf("after a successful collection: %+v", journal)
	}
}
