package workerruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A work-in-progress snapshot that fails quotes Git's standard error, and Git
// names the paths it could not use. When such a path holds an execution
// credential, the failure must not write it into the worker log or the failed
// result: the snapshot error is redacted with the attempt's scanner before it
// is logged or shortened, and withheld when the scanner cannot run.
func TestFailedResultSnapshotErrorIsRedactedBeforeItIsLogged(t *testing.T) {
	secret := "synthetic-execution-credential-0123456789"
	for _, tc := range []struct {
		name        string
		unavailable bool
	}{
		{name: "redacted"},
		{name: "scanner unavailable", unavailable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWIPFixture(t, commitOutputs)
			writeTestFile(t, filepath.Join(f.workspace, "c.txt"), "new\n")
			// A work tree Git cannot open makes status fail with the path in
			// its standard error.
			runGitForTest(t, f.workspace, "config", "core.worktree", filepath.Join(t.TempDir(), secret, "child"))
			if _, err := f.driver.SnapshotWorkInProgress(context.Background(), f.pkg, f.workspace); err == nil || !strings.Contains(err.Error(), secret) {
				t.Fatalf("snapshot error = %v, want one quoting the credential", err)
			}
			var logs bytes.Buffer
			f.driver.Log = slog.New(slog.NewTextHandler(&logs, nil))
			custody := &capturingCustody{CustodyStore: testCustodyStore(t, filepath.Join(t.TempDir(), "custody"), func() time.Time { return runtimeTestNow })}
			custody.config.SecretScan.StaticCanaries = []string{secret}
			if tc.unavailable {
				custody.config.SecretScan.Canaries = func(context.Context, workerproto.ExecutionPackage) ([]string, error) {
					return nil, errors.New("credential source unavailable")
				}
			}
			f.driver.Publisher = custody
			f.driver.T3 = &recordingT3{thread: &domain.Thread{ID: "thread-1", TurnID: "turn-3", TurnState: "completed"}, archive: []byte("{}")}
			collectErr := f.driver.CollectFailure(context.Background(), f.pkg, f.workspace, "preparation failed 3 times")
			if strings.Contains(logs.String(), secret) {
				t.Fatalf("the worker log carries the credential:\n%s", logs.String())
			}
			for _, result := range custody.results {
				if failure := result.Finalized.Completion.Failure; strings.Contains(failure, secret) {
					t.Fatalf("a published failure carries the credential: %q", failure)
				}
			}
			if tc.unavailable {
				if !strings.Contains(logs.String(), "withheld") {
					t.Fatalf("the log does not say the snapshot error was withheld:\n%s", logs.String())
				}
				return
			}
			if collectErr != nil {
				t.Fatal(collectErr)
			}
			last := custody.results[len(custody.results)-1]
			if failure := last.Finalized.Completion.Failure; !strings.Contains(failure, "wip.bundle not retained") || !strings.Contains(failure, "[redacted]") {
				t.Fatalf("the failure does not name the redacted snapshot error: %q", failure)
			}
		})
	}
}

// failingRedactTurnEndDriver is a turn-end driver whose secret scan cannot
// check any text.
type failingRedactTurnEndDriver struct {
	*turnEndDriver
}

func (failingRedactTurnEndDriver) RedactFailure(context.Context, workerproto.ExecutionPackage, string) (string, error) {
	return "", errors.New("credential source unavailable")
}

// The live-commands failure snapshots the work first and quotes a snapshot
// error in its reason and its warning. The error is redacted before the
// warning is logged and before the reason is shortened, so neither the log
// nor the recorded failure carries the credential or a prefix of it; with no
// scanner the error is withheld. However long Git's standard error is, the
// warning quotes a bounded part of it.
func TestLiveCommandsSnapshotErrorIsRedactedBeforeItIsLogged(t *testing.T) {
	secret := "synthetic-execution-credential-" + strings.Repeat("q", 50)
	for _, tc := range []struct {
		name        string
		padding     int
		unavailable bool
	}{
		{name: "redacted", padding: 1960},
		{name: "scanner unavailable", padding: 1960, unavailable: true},
		{name: "huge error", padding: 4 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshotErr := fmt.Errorf("read working tree status: git status: exit status 128: fatal: Invalid path '%s/%s': No such file or directory", strings.Repeat("p", tc.padding), secret)
			root := t.TempDir()
			inner := &turnEndDriver{
				fakeDriver: &fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true},
				state:      backlog.DispatchThreadActive, turn: "turn-1",
				live:        []LiveCommand{{PID: 7, Command: "sleep 300"}},
				snapshotErr: snapshotErr,
			}
			var driver Driver
			if tc.unavailable {
				driver = failingRedactTurnEndDriver{inner}
			} else {
				custody := testCustodyStore(t, filepath.Join(root, "custody"), func() time.Time { return runtimeTestNow })
				custody.config.SecretScan.StaticCanaries = []string{secret}
				driver = &custodyRedactingTurnEndDriver{turnEndDriver: inner, custody: custody}
			}
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
			if err := runtime.markPhase("assignment-1", PhaseRunning, "", inner.workspace, "thread-1"); err != nil {
				t.Fatal(err)
			}
			turns := MaxLiveCommandNudges + 1
			for turn := 1; turn <= turns; turn++ {
				inner.endTurn("turn-" + string(rune('0'+turn)))
				reconcileOnce(t, runtime)
				if turn < turns {
					inner.state = backlog.DispatchThreadActive
					reconcileOnce(t, runtime)
				}
			}
			if inner.snapshots == 0 {
				t.Fatal("the live-commands failure did not snapshot the work")
			}
			record := mustRecord(t, runtime, "assignment-1")
			if record.Phase != PhaseFailed && record.Phase != PhaseCompleted {
				t.Fatalf("phase = %q, want the attempt failed", record.Phase)
			}
			if logs.Len() > 64<<10 {
				t.Fatalf("the worker log quotes an unbounded snapshot error: %d bytes", logs.Len())
			}
			if strings.Contains(logs.String(), secret[:30]) {
				t.Fatalf("the worker log carries the credential:\n%s", logs.String())
			}
			if strings.Contains(record.Failure, secret[:30]) {
				t.Fatalf("the recorded failure carries a credential prefix: %q", record.Failure)
			}
		})
	}
}
