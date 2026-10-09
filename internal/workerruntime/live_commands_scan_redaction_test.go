package workerruntime

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/testutil"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// The /proc scan used to shorten each command line to 200 bytes before the
// secret scan saw it, so a credential across that cut reached the scan as a
// prefix it could not match, and the prefix left the worker in the turn-end
// note and the live-commands failure. The command lines here come from the
// real scan of a synthetic process table, not from a mock that hands over the
// whole line.
func TestScannedCommandLinesAreRedactedBeforeTheyAreShortened(t *testing.T) {
	secret := "synthetic-execution-credential-" + strings.Repeat("q", 50)
	spaced := "synthetic  spaced\tcredential-" + strings.Repeat("r", 40)
	for _, tc := range []struct {
		name   string
		secret string
		// argv is the detached command's arguments; the credential crosses
		// the cut the shown text makes.
		argv []string
		fail bool
	}{
		{name: "turn-end note", secret: spaced, argv: []string{"deploy", strings.Repeat("p", 40) + spaced}},
		{name: "live-command failure", secret: secret, argv: []string{strings.Repeat("p", 150) + secret}, fail: true},
		{name: "live-command failure, credential holding white space", secret: spaced, argv: []string{"deploy", strings.Repeat("p", 150) + spaced}, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			secret := tc.secret
			root := testutil.RealTempDir(t)
			workspace := filepath.Join(root, "workspace")
			if err := os.Mkdir(workspace, 0o700); err != nil {
				t.Fatal(err)
			}
			proc := filepath.Join(root, "proc")
			for _, entry := range []struct {
				pid, parent int
				cwd         string
				argv        []string
			}{
				{1, 0, root, []string{"init"}}, {100, 1, root, []string{"worker"}}, {7, 1, workspace, tc.argv},
			} {
				dir := filepath.Join(proc, fmt.Sprint(entry.pid))
				writeTestFile(t, filepath.Join(dir, "stat"), fmt.Sprintf("%d (process) S %d %d %d", entry.pid, entry.parent, entry.pid, entry.pid))
				writeTestFile(t, filepath.Join(dir, "cmdline"), strings.Join(entry.argv, "\x00")+"\x00")
				if err := os.Symlink(entry.cwd, filepath.Join(dir, "cwd")); err != nil {
					t.Fatal(err)
				}
			}
			report, err := scanLiveCommandsIn(runtime.GOOS, proc, 100, workspace)
			if err != nil {
				t.Fatal(err)
			}
			if runtime.GOOS != "linux" {
				if report.Unsupported != "process inspection is unavailable on "+runtime.GOOS || len(report.Commands) != 0 {
					t.Fatalf("report = %+v, want unsupported process inspection with no commands", report)
				}
				return
			}
			if len(report.Commands) != 1 || report.Commands[0].PID != 7 {
				t.Fatalf("report = %+v", report)
			}
			inner := &turnEndDriver{
				fakeDriver: &fakeDriver{workspace: workspace, workspaceReady: true},
				state:      backlog.DispatchThreadActive, turn: "turn-1",
				live: report.Commands,
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
			var logs bytes.Buffer
			runtime.log = slog.New(slog.NewTextHandler(&logs, nil))
			if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
				t.Fatal(err)
			}
			if err := runtime.markPhase("assignment-1", PhaseRunning, "", workspace, "thread-1"); err != nil {
				t.Fatal(err)
			}
			turns := 1
			if tc.fail {
				turns = MaxLiveCommandNudges + 1
			}
			for turn := 1; turn <= turns; turn++ {
				inner.endTurn(fmt.Sprintf("turn-%d", turn))
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
					t.Fatal("no turn-end note recorded")
				}
				text = record.TurnEnd.Note
			}
			if text == "" {
				t.Fatalf("nothing recorded: %+v", record)
			}
			// The shown text collapses white space, so the prefix is looked for
			// both as the credential holds it and as it would be shown.
			for _, prefix := range []string{secret[:20], strings.Join(strings.Fields(secret), " ")[:20]} {
				if strings.Contains(text, prefix) || strings.Contains(logs.String(), prefix) {
					t.Fatalf("credential prefix %q left the worker:\nrecorded %q\nlogs %q", prefix, text, logs.String())
				}
			}
			if tc.fail && !strings.Contains(text, "pid 7 ") {
				t.Fatalf("failure does not name the command: %q", text)
			}
			if len(text) > 2048 {
				t.Fatalf("recorded text is %d bytes", len(text))
			}
		})
	}
}

// The scan keeps the whole command line; only what is shown is bounded.
func TestDisplayCommandBoundsTheShownLine(t *testing.T) {
	line := "make  check\treview " + strings.Repeat("x", 400)
	shown := displayCommand(line)
	if len(shown) > maxLiveCommandText || !strings.HasPrefix(shown, "make check review x") || !strings.HasSuffix(shown, "...") {
		t.Fatalf("displayCommand = %q", shown)
	}
}
