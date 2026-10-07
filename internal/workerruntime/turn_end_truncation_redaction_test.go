package workerruntime

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// custodyRedactingTurnEndDriver redacts with the real result secret scan.
type custodyRedactingTurnEndDriver struct {
	*turnEndDriver
	custody *CustodyStore
}

func (d *custodyRedactingTurnEndDriver) RedactFailure(ctx context.Context, pkg workerproto.ExecutionPackage, text string) (string, error) {
	return d.custody.RedactText(ctx, pkg, text)
}

// A credential cut by truncation before the scan no longer matches the
// scanner, so its prefix leaves the worker.
// A0 shortens running command lines for the turn-end note (80 bytes each) and
// the live-commands failure (the process list and the whole reason), and the
// secret scan's redaction matches only whole credentials. A credential across
// the cut must not leave the worker as its first part, so the scan sees each
// whole command line before anything is shortened.
func TestTurnEndShortenedCommandLinesKeepNoCredentialPrefix(t *testing.T) {
	secret := "synthetic-turn-end-credential-" + strings.Repeat("q", 50)
	for _, tc := range []struct {
		name    string
		command string
		fail    bool
	}{
		{name: "turn-end note", command: "deploy --token " + secret},
		{name: "live-command failure", command: strings.Repeat("x", 1600) + " --token " + secret, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			inner := &turnEndDriver{
				fakeDriver: &fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true},
				state:      backlog.DispatchThreadActive, turn: "turn-1",
				live: []LiveCommand{{PID: 7, Command: tc.command}},
			}
			custody := testCustodyStore(t, filepath.Join(root, "custody"), func() time.Time { return runtimeTestNow })
			custody.config.SecretScan.StaticCanaries = []string{secret}
			driver := &custodyRedactingTurnEndDriver{turnEndDriver: inner, custody: custody}
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
			turns := 1
			if tc.fail {
				turns = MaxLiveCommandNudges + 1
			}
			for turn := 1; turn <= turns; turn++ {
				inner.endTurn("turn-" + string(rune('0'+turn)))
				reconcileOnce(t, runtime)
				if turn < turns {
					inner.state = backlog.DispatchThreadActive
					reconcileOnce(t, runtime)
				}
			}
			record := mustRecord(t, runtime, "assignment-1")
			text := record.Failure
			if !tc.fail {
				if record.TurnEnd == nil {
					t.Fatal("no turn-end state")
				}
				text = record.TurnEnd.Note
			}
			if strings.Contains(text, secret[:30]) {
				t.Fatalf("credential prefix leaves the worker: %q", text)
			}
		})
	}
}
