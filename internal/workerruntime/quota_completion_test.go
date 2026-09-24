package workerruntime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type quotaCompletionDriver struct {
	*fakeDriver
	turnID   string
	complete bool
}

func (d *quotaCompletionDriver) ObserveThreadTurn(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, string, error) {
	return backlog.DispatchThreadStopped, d.turnID, nil
}

func (d *quotaCompletionDriver) QuotaPauseCompleted(_ context.Context, _ workerproto.ExecutionPackage, stoppedTurnID string) (bool, error) {
	return d.complete && stoppedTurnID == d.turnID, nil
}

func TestCompletedQuotaDrainCollectsWithoutRecovery(t *testing.T) {
	now := runtimeTestNow
	base := &fakeDriver{
		workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped},
	}
	driver := &quotaCompletionDriver{fakeDriver: base, turnID: "turn-drained"}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	runtime := runningRuntime(t, base, guard, &now)
	runtime.driver = driver
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	record := journalRecord(t, runtime)
	if record.LocalThrottle == nil || record.LocalThrottle.StoppedTurnID != "turn-drained" || base.collectCalls != 0 {
		t.Fatalf("initial drain: record=%+v collects=%d", record, base.collectCalls)
	}
	// A stopped turn without an explicit done status remains paused.
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if base.collectCalls != 0 || guard.resumeAsked == 0 {
		t.Fatalf("unfinished drain collected or skipped pause: collects=%d resumeAsked=%d", base.collectCalls, guard.resumeAsked)
	}
	// A later turn cannot claim completion for the drained turn.
	driver.turnID, driver.complete = "turn-user-ping", true
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if base.collectCalls != 0 {
		t.Fatalf("later turn collected drained task: collects=%d", base.collectCalls)
	}
	driver.turnID = "turn-drained"
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	record = journalRecord(t, runtime)
	if base.collectCalls != 1 || base.resumeCalls != 0 || record.LocalThrottle != nil || record.Phase != PhaseCompleted {
		t.Fatalf("completed drain: collects=%d resumes=%d record=%+v", base.collectCalls, base.resumeCalls, record)
	}
}

func TestLocalDriverQuotaPauseCompletionNeedsExactTurnAndReadySession(t *testing.T) {
	archive := []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-drained","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`)
	control := &recordingT3{
		thread:  &domain.Thread{ID: "thread-1", TurnID: "turn-drained", TurnState: "completed"},
		message: "Declared outputs are ready.\nbacklog status: done",
		archive: archive,
	}
	driver := &LocalDriver{T3: control, Now: func() time.Time { return runtimeTestNow }}
	pkg := testPackage()
	pkg.Identity.ThreadID = "thread-1"
	for _, tc := range []struct {
		name    string
		turn    string
		message string
		archive []byte
		want    bool
	}{
		{"complete", "turn-drained", control.message, archive, true},
		{"other turn", "turn-other", control.message, archive, false},
		{"unfinished", "turn-drained", "checkpoint saved\nbacklog status: continue", archive, false},
		{"no marker", "turn-drained", "Done.", archive, false},
		{"quoted marker", "turn-drained", "backlog status: done\nStill working", archive, false},
		{"pending input", "turn-drained", control.message, []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-drained","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null},"hasPendingUserInput":true}}`), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			control.message, control.archive = tc.message, tc.archive
			got, err := driver.QuotaPauseCompleted(context.Background(), pkg, tc.turn)
			if err != nil || got != tc.want {
				t.Fatalf("got=%v err=%v want=%v", got, err, tc.want)
			}
		})
	}
}
