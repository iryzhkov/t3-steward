package workerruntime

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// redactingTurnEndDriver adds the local driver's failure scanner to a
// turn-end driver: it removes secret, or fails when fail is set.
type redactingTurnEndDriver struct {
	*turnEndDriver
	secret string
	fail   bool
}

func (d *redactingTurnEndDriver) RedactFailure(_ context.Context, _ workerproto.ExecutionPackage, text string) (string, error) {
	if d.fail {
		return "", errors.New("credential resolution failed")
	}
	return strings.ReplaceAll(text, d.secret, "[redacted]"), nil
}

// The turn-end note travels to the coordinator in every snapshot that asks for
// it, and it quotes the command lines still running, which can carry a
// credential. It is scanned like a failure reason before it is recorded, and
// withheld when the scanner cannot run; the nudge to the task's own session
// still names the commands.
func TestTurnEndNoteIsRedactedBeforeTheCoordinatorSeesIt(t *testing.T) {
	secret := "synthetic-turn-end-credential"
	for _, tc := range []struct {
		name        string
		unsupported string
		fail        bool
		want        string
	}{
		{name: "waiting note", want: "[redacted]"},
		{name: "unsupported note", unsupported: "process inspection failed reading token=" + secret, want: "[redacted]"},
		{name: "scanner unavailable", fail: true, want: withheldTurnEndNote},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			inner := &turnEndDriver{
				fakeDriver: &fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true},
				state:      backlog.DispatchThreadActive, turn: "turn-1",
				live:        []LiveCommand{{PID: 7, Command: "curl -H 'Authorization: Bearer " + secret + "' https://example.invalid"}},
				unsupported: tc.unsupported,
			}
			driver := &redactingTurnEndDriver{turnEndDriver: inner, secret: secret, fail: tc.fail}
			journal, err := OpenJournal(root, "normandy", "worker-1", 9)
			if err != nil {
				t.Fatal(err)
			}
			config := testConfig(func() time.Time { return runtimeTestNow })
			config.LiveTaskWait = func(context.Context, workerproto.ExecutionPackage) (bool, error) { return false, nil }
			runtime, err := New(config, journal, driver)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
				t.Fatal(err)
			}
			if err := runtime.markPhase("assignment-1", PhaseRunning, "", inner.workspace, "thread-1"); err != nil {
				t.Fatal(err)
			}
			inner.endTurn("turn-1")
			reconcileOnce(t, runtime)
			if err := runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{TurnEndWanted: true}); err != nil {
				t.Fatal(err)
			}
			snapshot, err := runtime.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Assignments) != 1 || snapshot.Assignments[0].Journal == nil {
				t.Fatalf("snapshot assignments = %+v", snapshot.Assignments)
			}
			note := snapshot.Assignments[0].Journal.TurnEnd
			if strings.Contains(note, secret) || !strings.Contains(note, tc.want) {
				t.Fatalf("turn-end note = %q, want it to contain %q and not the credential", note, tc.want)
			}
			if record := mustRecord(t, runtime, "assignment-1"); record.TurnEnd == nil || strings.Contains(record.TurnEnd.Note, secret) {
				t.Fatalf("journal turn-end state = %+v", record.TurnEnd)
			}
		})
	}
}
