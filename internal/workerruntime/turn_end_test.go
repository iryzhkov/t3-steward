package workerruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// turnEndDriver is a driver whose thread, provider turn and background
// commands the test sets directly, and which records every nudge it is asked
// to send.
type turnEndDriver struct {
	*fakeDriver
	state       backlog.DispatchThreadState
	turn        string
	live        []LiveCommand
	unsupported string
	nudgeErr    error
	nudgeTokens []string
	nudgeTexts  []string
	snapshots   int
}

func (d *turnEndDriver) ObserveThread(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, error) {
	return d.state, nil
}

func (d *turnEndDriver) ObserveThreadTurn(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, string, error) {
	return d.state, d.turn, nil
}

func (d *turnEndDriver) LiveCommands(context.Context, workerproto.ExecutionPackage, string) (LiveCommandReport, error) {
	return LiveCommandReport{Unsupported: d.unsupported, Commands: append([]LiveCommand(nil), d.live...)}, nil
}

func (d *turnEndDriver) NudgeLiveCommands(_ context.Context, _ workerproto.ExecutionPackage, token, text string) error {
	d.nudgeTokens = append(d.nudgeTokens, token)
	d.nudgeTexts = append(d.nudgeTexts, text)
	return d.nudgeErr
}

func (d *turnEndDriver) SnapshotWorkInProgress(context.Context, workerproto.ExecutionPackage, string) (string, error) {
	d.snapshots++
	return "wip.bundle retained (refs/steward/wip/attempt-1)", nil
}

func turnEndRuntime(t *testing.T, root string, driver *turnEndDriver) *Runtime {
	t.Helper()
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
	return runtime
}

// runningTurnEnd is a claimed attempt whose provider turn is running.
func runningTurnEnd(t *testing.T) (string, *turnEndDriver, *Runtime) {
	t.Helper()
	root := t.TempDir()
	driver := &turnEndDriver{
		fakeDriver: &fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true},
		state:      backlog.DispatchThreadActive, turn: "turn-1",
	}
	runtime := turnEndRuntime(t, root, driver)
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	return root, driver, runtime
}

func reconcileOnce(t *testing.T, runtime *Runtime) {
	t.Helper()
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// endTurn ends the current provider turn under a new identity.
func (d *turnEndDriver) endTurn(turn string) {
	d.state, d.turn = backlog.DispatchThreadStopped, turn
}

var backgroundGate = []LiveCommand{
	{PID: 4242, Command: "sh -c GOMAXPROCS=2 make check-review FAST_BASE=1088ab7"},
	{PID: 4243, Command: "sleep 300"},
}

// A turn that ends with nothing running in the background is collected
// exactly as before: no nudge, no snapshot, no note.
func TestTurnEndWithoutLiveCommandsIsCollectedUnchanged(t *testing.T) {
	_, driver, runtime := runningTurnEnd(t)
	driver.endTurn("turn-1")
	reconcileOnce(t, runtime)
	if got := phaseOf(t, runtime, "assignment-1"); got != PhaseCompleted {
		t.Fatalf("phase = %q, want completed", got)
	}
	if driver.collectCalls != 1 || len(driver.nudgeTokens) != 0 || driver.snapshots != 0 {
		t.Fatalf("collect=%d nudges=%d snapshots=%d", driver.collectCalls, len(driver.nudgeTokens), driver.snapshots)
	}
	if record := mustRecord(t, runtime, "assignment-1"); record.TurnEnd != nil {
		t.Fatalf("a clean turn end left turn-end state: %+v", record.TurnEnd)
	}
}

// A turn that ends while its commands still run is not collected. The same
// session is told, once, which commands are running and that it must wait
// for them, and the explanation says what the attempt is waiting for. A
// repeated pass and a worker restart on the same ended turn send nothing more.
func TestTurnEndWithLiveCommandsNudgesOncePerTurn(t *testing.T) {
	root, driver, runtime := runningTurnEnd(t)
	driver.live = backgroundGate
	driver.endTurn("turn-1")
	reconcileOnce(t, runtime)
	if got := phaseOf(t, runtime, "assignment-1"); got == PhaseCompleted || got == PhaseCollecting || got == PhaseFailed {
		t.Fatalf("an attempt with live commands left the stopped phase: %q", got)
	}
	if driver.collectCalls != 0 || driver.collectFailureCalls != 0 {
		t.Fatalf("collected while commands run: collect=%d failure=%d", driver.collectCalls, driver.collectFailureCalls)
	}
	if len(driver.nudgeTokens) != 1 {
		t.Fatalf("nudges = %d, want 1", len(driver.nudgeTokens))
	}
	if !strings.Contains(driver.nudgeTokens[0], "turn-1") || !strings.Contains(driver.nudgeTokens[0], "dispatch-1") {
		t.Fatalf("nudge token %q is not bound to the dispatch and the ended turn", driver.nudgeTokens[0])
	}
	text := driver.nudgeTexts[0]
	for _, want := range []string{"make check-review", "sleep 300", "Wait for each of them to exit"} {
		if !strings.Contains(text, want) {
			t.Fatalf("nudge text does not mention %q:\n%s", want, text)
		}
	}

	reconcileOnce(t, runtime)
	restarted := turnEndRuntime(t, root, driver)
	reconcileOnce(t, restarted)
	if len(driver.nudgeTokens) != 1 || driver.collectCalls != 0 {
		t.Fatalf("the same ended turn was nudged %d times, collected %d times", len(driver.nudgeTokens), driver.collectCalls)
	}

	if err := restarted.ApplyParkedAssignments(workerproto.SnapshotRequest{TurnEndWanted: true}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := restarted.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Assignments) != 1 || snapshot.Assignments[0].Journal == nil {
		t.Fatalf("snapshot assignments = %+v", snapshot.Assignments)
	}
	note := snapshot.Assignments[0].Journal.TurnEnd
	if !strings.HasPrefix(note, "waiting for 2 background commands: ") || !strings.Contains(note, "make check-review") {
		t.Fatalf("turn-end note = %q", note)
	}
	if !slicesContains(snapshot.Inventory.Capabilities, workerproto.CapabilityTurnEndCommands) {
		t.Fatalf("capability %q not advertised: %v", workerproto.CapabilityTurnEndCommands, snapshot.Inventory.Capabilities)
	}

	// A coordinator that did not ask never meets the field.
	if err := restarted.ApplyParkedAssignments(workerproto.SnapshotRequest{}); err != nil {
		t.Fatal(err)
	}
	if snapshot, err = restarted.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if note := snapshot.Assignments[0].Journal.TurnEnd; note != "" {
		t.Fatalf("turn-end note sent unasked: %q", note)
	}
}

func slicesContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// A nudge whose claim is durable but whose send failed, or whose worker died
// before recording the send, is sent again with the same identity, so T3
// recognises a repeat instead of starting a second turn. Once sent it is never
// sent again for that turn.
func TestTurnEndNudgeClaimSurvivesRestartWithTheSameIdentity(t *testing.T) {
	root, driver, runtime := runningTurnEnd(t)
	driver.live = backgroundGate
	driver.nudgeErr = errors.New("T3 unavailable")
	driver.endTurn("turn-1")
	reconcileOnce(t, runtime)
	if len(driver.nudgeTokens) != 1 {
		t.Fatalf("nudge attempts = %d, want 1", len(driver.nudgeTokens))
	}
	if check := mustRecord(t, runtime, "assignment-1").TurnEnd; check == nil || check.Nudges != 1 || check.NudgeSent {
		t.Fatalf("claimed nudge state = %+v", check)
	}

	driver.nudgeErr = nil
	restarted := turnEndRuntime(t, root, driver)
	reconcileOnce(t, restarted)
	reconcileOnce(t, restarted)
	if len(driver.nudgeTokens) != 2 || driver.nudgeTokens[0] != driver.nudgeTokens[1] || driver.nudgeTexts[0] != driver.nudgeTexts[1] {
		t.Fatalf("retried nudge changed identity or repeated: %q", driver.nudgeTokens)
	}
	if check := mustRecord(t, restarted, "assignment-1").TurnEnd; check == nil || check.Nudges != 1 || !check.NudgeSent {
		t.Fatalf("sent nudge state = %+v", check)
	}
}

// The nudged session waits, its commands finish, and its next turn ends
// clean: the attempt is collected normally.
func TestTurnEndCollectsOnceTheNudgedTurnEndsClean(t *testing.T) {
	_, driver, runtime := runningTurnEnd(t)
	driver.live = backgroundGate
	driver.endTurn("turn-1")
	reconcileOnce(t, runtime)
	driver.state = backlog.DispatchThreadActive
	reconcileOnce(t, runtime)
	if got := phaseOf(t, runtime, "assignment-1"); got != PhaseRunning {
		t.Fatalf("the nudged turn did not resume: phase=%q", got)
	}
	driver.live = nil
	driver.endTurn("turn-2")
	reconcileOnce(t, runtime)
	if got := phaseOf(t, runtime, "assignment-1"); got != PhaseCompleted || driver.collectCalls != 1 {
		t.Fatalf("phase=%q collect=%d", got, driver.collectCalls)
	}
	if len(driver.nudgeTokens) != 1 || driver.snapshots != 0 || driver.collectFailureCalls != 0 {
		t.Fatalf("nudges=%d snapshots=%d failures=%d", len(driver.nudgeTokens), driver.snapshots, driver.collectFailureCalls)
	}
}

// Two nudges are the budget. A third turn that ends with commands still
// running fails the attempt with the fixed reason and its evidence, after the
// work in progress has been snapshotted.
func TestTurnEndFailsAfterTwoNudges(t *testing.T) {
	_, driver, runtime := runningTurnEnd(t)
	driver.live = backgroundGate
	for turn := 1; turn <= MaxLiveCommandNudges; turn++ {
		driver.endTurn("turn-" + string(rune('0'+turn)))
		reconcileOnce(t, runtime)
		if len(driver.nudgeTokens) != turn {
			t.Fatalf("after turn %d: nudges = %d", turn, len(driver.nudgeTokens))
		}
		driver.state = backlog.DispatchThreadActive
		reconcileOnce(t, runtime)
	}
	if !strings.Contains(driver.nudgeTexts[1], "2 of 2") {
		t.Fatalf("the last nudge does not say it is the last:\n%s", driver.nudgeTexts[1])
	}
	driver.endTurn("turn-3")
	reconcileOnce(t, runtime)
	if len(driver.nudgeTokens) != MaxLiveCommandNudges {
		t.Fatalf("nudged past the budget: %d", len(driver.nudgeTokens))
	}
	record := mustRecord(t, runtime, "assignment-1")
	if record.Phase != PhaseCompleted || driver.collectFailureCalls != 1 || driver.collectCalls != 0 {
		t.Fatalf("phase=%q failure collections=%d collections=%d", record.Phase, driver.collectFailureCalls, driver.collectCalls)
	}
	if driver.snapshots != 1 {
		t.Fatalf("work in progress snapshots = %d, want 1", driver.snapshots)
	}
	if !strings.HasPrefix(record.Failure, LiveCommandsFailure+": ") {
		t.Fatalf("failure = %q", record.Failure)
	}
	for _, want := range []string{"pid 4242", "make check-review", "pid 4243", "wip.bundle retained"} {
		if !strings.Contains(record.Failure, want) {
			t.Fatalf("failure evidence lacks %q: %s", want, record.Failure)
		}
	}
}

// A host that cannot inspect processes collects the turn as before and says
// that the check did not run, rather than failing a task it cannot judge.
func TestTurnEndWithoutProcessInspectionCollectsWithAWarning(t *testing.T) {
	_, driver, runtime := runningTurnEnd(t)
	driver.unsupported = "process inspection is unavailable on darwin"
	driver.endTurn("turn-1")
	reconcileOnce(t, runtime)
	record := mustRecord(t, runtime, "assignment-1")
	if record.Phase != PhaseCompleted || driver.collectCalls != 1 || len(driver.nudgeTokens) != 0 {
		t.Fatalf("phase=%q collect=%d nudges=%d", record.Phase, driver.collectCalls, len(driver.nudgeTokens))
	}
	if record.TurnEnd == nil || !strings.Contains(record.TurnEnd.Note, "unavailable on darwin") {
		t.Fatalf("no warning recorded: %+v", record.TurnEnd)
	}
}

// The evidence names every declared file output as present or missing.
func TestDeclaredOutputEvidence(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "handoff.md"), []byte("done\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	pkg := testPackage()
	pkg.Outputs = []domain.ArtifactDeclaration{
		{Name: "handoff.md"}, {Name: "verification.log"}, {Name: "implementation", Commit: &domain.CommitOutput{}},
	}
	got := declaredOutputEvidence(pkg, workspace)
	for _, want := range []string{"present: handoff.md", "missing: verification.log", "commit: implementation"} {
		if !strings.Contains(got, want) {
			t.Fatalf("evidence %q lacks %q", got, want)
		}
	}
}
