package workerruntime

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// fakeQuotaGuard scripts the host watchdog's answers.
type fakeQuotaGuard struct {
	pause        QuotaPause
	pauseNeeded  bool
	resumeOK     bool
	resumeWhy    string
	observations []domain.WorkerQuotaObservation
	resumeAsked  int
}

func (g *fakeQuotaGuard) PauseRequired(context.Context, domain.ProviderRoute) (QuotaPause, bool, error) {
	return g.pause, g.pauseNeeded, nil
}

func (g *fakeQuotaGuard) ResumeAllowed(context.Context, LocalThrottleRequest, domain.ProviderRoute) (bool, string, error) {
	g.resumeAsked++
	return g.resumeOK, g.resumeWhy, nil
}

func (g *fakeQuotaGuard) Observations(context.Context) ([]domain.WorkerQuotaObservation, error) {
	return g.observations, nil
}

var sevenDay = domain.BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: "primary"}

func stoppedPause() QuotaPause {
	reset := runtimeTestNow.Add(2 * time.Hour)
	return QuotaPause{Bucket: sevenDay, Phase: domain.PhaseStopped, UsedPercent: 97, ResetsAt: &reset, ObservedAt: runtimeTestNow}
}

func runningRuntime(t *testing.T, driver *fakeDriver, guard QuotaGuard, now *time.Time) *Runtime {
	t.Helper()
	root := t.TempDir()
	runtime := newClaimedRuntimeWithClock(t, root, driver, func() time.Time { return *now })
	runtime.config.Quota = guard
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	return runtime
}

func journalRecord(t *testing.T, runtime *Runtime) AttemptRecord {
	t.Helper()
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return state.Attempts["assignment-1"]
}

// S-3: a watchdog stop of an owned thread is a pause. The worker stops the
// thread itself through the throttle path, reports ControlPaused with the
// bucket as the reason, and collects nothing while the pause is in force,
// even though the thread is observed stopped and the driver would collect.
func TestLocalQuotaStopReportsPausedAndDefersCollection(t *testing.T) {
	now := runtimeTestNow
	// The thread is mid-work when the bucket stops; afterwards it is observed
	// stopped until the worker resumes it.
	driver := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped, backlog.DispatchThreadActive}}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	runtime := runningRuntime(t, driver, guard, &now)
	// The coordinator asks for quota observations, which is what carries the
	// pause reason and thread state in the journal excerpt.
	if err := runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ParkedReported: true, QuotaObservationsWanted: true}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.stopCalls != 1 || driver.collectCalls != 0 || driver.collectFailureCalls != 0 {
		t.Fatalf("stops=%d collects=%d failures=%d", driver.stopCalls, driver.collectCalls, driver.collectFailureCalls)
	}
	record := journalRecord(t, runtime)
	if record.Phase != PhaseStopped || record.LocalThrottle == nil || record.LocalThrottle.Kind != domain.ThrottleCommandHardStop ||
		record.LocalThrottle.Reason != "codex/codex/primary at 97%" || record.LocalThrottle.StoppedAt == nil {
		t.Fatalf("record = %+v throttle=%+v", record, record.LocalThrottle)
	}
	snapshot, err := runtime.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	observed := snapshot.Assignments[0]
	if observed.State != domain.AssignmentClaimed || observed.Control != domain.ControlPaused ||
		observed.Journal == nil || observed.Journal.PauseReason != "codex/codex/primary at 97%" || observed.Journal.ThreadState != "stopped" {
		t.Fatalf("observation = %+v journal=%+v", observed, observed.Journal)
	}
	// More passes while the bucket is stopped: still nothing collected, no
	// second stop, and a commanded collection is refused too.
	now = now.Add(5 * time.Minute)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	collect := testCommand(t, runtime, domain.WorkerCommandCollect, "collect-1")
	acks, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{collect}})
	if err != nil || !acks.Acknowledgements[0].Accepted || !strings.Contains(acks.Acknowledgements[0].Detail, "paused by the quota watchdog") {
		t.Fatalf("collect while paused: acks=%+v err=%v", acks, err)
	}
	if driver.stopCalls != 1 || driver.collectCalls != 0 || guard.resumeAsked == 0 {
		t.Fatalf("stops=%d collects=%d resumeAsked=%d", driver.stopCalls, driver.collectCalls, guard.resumeAsked)
	}
	// The bucket recovers and the attempt is still live: the same worker
	// resumes the thread through the throttle path and the pause ends.
	guard.pauseNeeded = false
	guard.resumeOK, guard.resumeWhy = true, "codex/codex/primary recovered"
	now = now.Add(time.Hour)
	renewLease(t, runtime, now.Add(2*time.Minute))
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	record = journalRecord(t, runtime)
	if driver.resumeCalls != 1 || record.Phase != PhaseRunning || record.LocalThrottle != nil ||
		record.LastLocalThrottle == nil || record.LastLocalThrottle.ResumedAt == nil {
		t.Fatalf("after recovery: resumes=%d record=%+v", driver.resumeCalls, record)
	}
	snapshot, err = runtime.Snapshot(context.Background())
	if err != nil || snapshot.Assignments[0].Control != domain.ControlRunning || snapshot.Assignments[0].Journal.PauseReason != "" {
		t.Fatalf("resumed observation = %+v err=%v", snapshot.Assignments[0], err)
	}
}

// The drain phase asks the thread to checkpoint through the throttle path; a
// thread that does not stop in time keeps running without a second notice,
// and a hard stop follows when the bucket reaches the stop phase.
func TestLocalQuotaDrainThenHardStop(t *testing.T) {
	now := runtimeTestNow
	driver := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive, backlog.DispatchThreadStopped}}
	pause := stoppedPause()
	pause.Phase, pause.UsedPercent = domain.PhaseDraining, 91
	guard := &fakeQuotaGuard{pause: pause, pauseNeeded: true}
	runtime := runningRuntime(t, driver, guard, &now)
	// The fake checkpoint succeeds at once, which is a thread that honoured
	// the drain notice: the pause is in force with a checkpoint.
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	record := journalRecord(t, runtime)
	if driver.checkpointCalls != 1 || driver.stopCalls != 0 || record.Phase != PhaseStopped ||
		record.LocalThrottle == nil || record.LocalThrottle.Kind != domain.ThrottleCommandDrain || record.LocalThrottle.Checkpoint == nil {
		t.Fatalf("checkpoints=%d stops=%d record=%+v", driver.checkpointCalls, driver.stopCalls, record)
	}
	snapshot, err := runtime.Snapshot(context.Background())
	if err != nil || snapshot.Assignments[0].Control != domain.ControlPaused {
		t.Fatalf("drained observation = %+v err=%v", snapshot.Assignments[0], err)
	}
}

// A thread that has already ended its turn cleanly when the bucket is stopped
// is finished work: collecting it spends no quota. It is collected as today,
// never settled, paused and later resumed with a filler turn. A drain notice
// that was already sent is the exception: the thread ending its turn is the
// pause taking effect.
func TestFinishedTurnIsCollectedNotPausedWhileBucketStopped(t *testing.T) {
	now := runtimeTestNow
	driver := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadStopped}}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	runtime := runningRuntime(t, driver, guard, &now)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	record := journalRecord(t, runtime)
	if driver.stopCalls != 0 || driver.checkpointCalls != 0 || driver.collectCalls != 1 || record.LocalThrottle != nil || record.Phase != PhaseCompleted {
		t.Fatalf("stops=%d checkpoints=%d collects=%d record=%+v", driver.stopCalls, driver.checkpointCalls, driver.collectCalls, record)
	}
	if driver.resumeCalls != 0 {
		t.Fatal("a finished thread was resumed")
	}
	// The drained case: the notice went out while the thread was working,
	// and the turn ending afterwards is the pause, not the attempt finishing.
	driver = &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive, backlog.DispatchThreadStopped}}
	driver.checkpointErr = errors.New("checkpoint turn did not stop before deadline")
	pause := stoppedPause()
	pause.Phase, pause.UsedPercent = domain.PhaseDraining, 91
	guard = &fakeQuotaGuard{pause: pause, pauseNeeded: true}
	runtime = runningRuntime(t, driver, guard, &now)
	for range 2 {
		if err := runtime.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	record = journalRecord(t, runtime)
	if driver.checkpointCalls != 1 || driver.collectCalls != 0 || record.Phase != PhaseStopped || record.LocalThrottle == nil || record.LocalThrottle.StoppedAt == nil {
		t.Fatalf("drained: checkpoints=%d collects=%d record=%+v", driver.checkpointCalls, driver.collectCalls, record)
	}
}

// S-16 from the worker's side: a cancelled attempt is never resumed. The
// coordinator's stop command settles the pause through the ordinary stop
// path, and the recovered bucket changes nothing.
func TestLocalQuotaPauseNeverResumesACancelledAttempt(t *testing.T) {
	now := runtimeTestNow
	driver := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped}}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	runtime := runningRuntime(t, driver, guard, &now)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if journalRecord(t, runtime).LocalThrottle == nil {
		t.Fatal("pause not recorded")
	}
	stop := testCommand(t, runtime, domain.WorkerCommandStop, "stop-1")
	if _, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{stop}}); err != nil {
		t.Fatal(err)
	}
	guard.pauseNeeded = false
	guard.resumeOK, guard.resumeWhy = true, "recovered"
	now = now.Add(time.Hour)
	// A restart in between rebuilds the decision from the journal.
	runtime = reopenTestRuntime(t, runtime.journal.root, driver)
	runtime.config.Quota = guard
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := runtime.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if driver.resumeCalls != 0 || snapshot.Assignments[0].State != domain.AssignmentReleased || snapshot.Assignments[0].Control != domain.ControlStopped {
		t.Fatalf("resumes=%d observation=%+v", driver.resumeCalls, snapshot.Assignments[0])
	}
}

// A paused attempt whose lease has expired, or whose bucket has not
// recovered, stays paused; only both conditions together resume it.
func TestLocalQuotaResumeIsGatedOnRecoveryAndLiveness(t *testing.T) {
	now := runtimeTestNow
	driver := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped}}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	runtime := runningRuntime(t, driver, guard, &now)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	guard.pauseNeeded = false
	// Not recovered: no resume.
	guard.resumeOK, guard.resumeWhy = false, "codex/codex/primary has not recovered since the stop"
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.resumeCalls != 0 {
		t.Fatal("resumed before the bucket recovered")
	}
	// Recovered, but the lease the worker holds has expired: the coordinator
	// is out of reach and the pause stays.
	guard.resumeOK, guard.resumeWhy = true, "recovered"
	now = now.Add(3 * time.Minute)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.resumeCalls != 0 {
		t.Fatal("resumed on an expired lease")
	}
	// A renewed lease from the next coordinator reconcile lets it resume.
	renewLease(t, runtime, now.Add(2*time.Minute))
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.resumeCalls != 1 || journalRecord(t, runtime).Phase != PhaseRunning {
		t.Fatalf("resumes=%d phase=%s", driver.resumeCalls, journalRecord(t, runtime).Phase)
	}
}

// The snapshot carries the host's bucket observations only when the
// coordinator asked for them on the exchange, so an older coordinator never
// meets the field.
func TestSnapshotReportsQuotaObservationsWhenAsked(t *testing.T) {
	now := runtimeTestNow
	driver := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace")}
	guard := &fakeQuotaGuard{observations: []domain.WorkerQuotaObservation{{Key: sevenDay, Phase: domain.PhaseStopped, UsedPercent: 97, ObservedAt: now}}}
	root := t.TempDir()
	runtime := newTestRuntimeWithClock(t, root, driver, func() time.Time { return now })
	runtime.config.Quota = guard
	snapshot, err := runtime.Snapshot(context.Background())
	if err != nil || len(snapshot.QuotaObservations) != 0 {
		t.Fatalf("unasked snapshot = %+v err=%v", snapshot.QuotaObservations, err)
	}
	if err := runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ParkedReported: true, QuotaObservationsWanted: true}); err != nil {
		t.Fatal(err)
	}
	snapshot, err = runtime.Snapshot(context.Background())
	if err != nil || len(snapshot.QuotaObservations) != 1 || snapshot.QuotaObservations[0].UsedPercent != 97 {
		t.Fatalf("asked snapshot = %+v err=%v", snapshot.QuotaObservations, err)
	}
	if !containsString(snapshot.Inventory.Capabilities, workerproto.CapabilityQuotaObservations) {
		t.Fatalf("capabilities = %v", snapshot.Inventory.Capabilities)
	}
}

// renewLease is the coordinator's lease renewal as the journal records it.
func renewLease(t *testing.T, runtime *Runtime, until time.Time) {
	t.Helper()
	if err := runtime.journal.update(func(state *journalState) error {
		record := state.Attempts["assignment-1"]
		record.Assignment.LeaseExpiresAt = until
		state.Attempts["assignment-1"] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func containsString(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

// The watchdog's ownership question is answered from the journal on disk:
// a dispatched attempt owns its thread, a released or finished one does not.
func TestJournalThreadOwnershipListsLiveAttemptThreads(t *testing.T) {
	host, projection := catalogHostFixture(t)
	bootstrapWorkerFile(t, host)
	ownership := &JournalThreadOwnership{Home: host.Home}
	if owners, err := ownership.Threads(context.Background()); err != nil || len(owners.Live) != 0 || len(owners.Settled) != 0 {
		t.Fatalf("worker without a catalog knows %v err=%v", owners, err)
	}
	publishCatalog(t, host, "first", CatalogRequest{Projection: projection})
	record := func(id, thread string, phase Phase) AttemptRecord {
		r := AttemptRecord{Phase: phase, ThreadID: thread}
		r.Assignment.ID, r.Assignment.AttemptID = id, "attempt-"+id
		r.Package.Package.Identity.AssignmentID = id
		return r
	}
	released := record("released", "thread-released", PhaseStopped)
	released.StopConfirmed = true
	released.CommandRequests = map[string]domain.WorkerCommand{"stop": {ID: "stop", Kind: domain.WorkerCommandStop}}
	if err := host.service.Exchange.Runtime.journal.update(func(state *journalState) error {
		state.Attempts["running"] = record("running", "thread-running", PhaseRunning)
		state.Attempts["paused"] = record("paused", "thread-paused", PhaseStopped)
		state.Attempts["done"] = record("done", "thread-done", PhaseCompleted)
		state.Attempts["released"] = released
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	owners, err := ownership.Threads(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	owned := owners.Live
	if len(owned) != 2 || owned["thread-running"] != "attempt-running" || owned["thread-paused"] != "attempt-paused" {
		t.Fatalf("owned = %v", owned)
	}
	// The finished and the released attempt's threads are settled: nobody
	// owns them, and an old resume intent for them is cancelled rather than
	// resumed.
	if len(owners.Settled) != 2 || owners.Settled["thread-done"] != "attempt-done" || owners.Settled["thread-released"] != "attempt-released" {
		t.Fatalf("settled = %v", owners.Settled)
	}
	// A host with no worker bootstrap owns nothing and reports no error.
	if owners, err := (&JournalThreadOwnership{Home: t.TempDir()}).Threads(context.Background()); err != nil || len(owners.Live) != 0 || len(owners.Settled) != 0 {
		t.Fatalf("host without a worker: %v %v", owners, err)
	}
}

// Ownership has a freshness bound: only a live worker renews the leases in
// its journal, so a record whose lease has expired no longer owns its thread
// for the watchdog, nor does a lease-less record not updated for an hour. A
// crashed worker's threads are the watchdog's again once the leases lapse.
// The staleness is logged once, not on every tick.
func TestJournalThreadOwnershipExpiresWithTheLease(t *testing.T) {
	host, projection := catalogHostFixture(t)
	bootstrapWorkerFile(t, host)
	publishCatalog(t, host, "first", CatalogRequest{Projection: projection})
	now := runtimeTestNow
	record := func(id string, phase Phase, lease, updated time.Time) AttemptRecord {
		r := AttemptRecord{Phase: phase, ThreadID: "thread-" + id, UpdatedAt: updated}
		r.Assignment.ID, r.Assignment.AttemptID, r.Assignment.LeaseExpiresAt = id, "attempt-"+id, lease
		r.Package.Package.Identity.AssignmentID = id
		return r
	}
	if err := host.service.Exchange.Runtime.journal.update(func(state *journalState) error {
		state.Attempts["live"] = record("live", PhaseRunning, now.Add(time.Minute), now)
		state.Attempts["lapsed"] = record("lapsed", PhaseRunning, now.Add(-time.Second), now.Add(-3*time.Minute))
		state.Attempts["paused-lapsed"] = record("paused-lapsed", PhaseStopped, now.Add(-time.Hour), now.Add(-time.Hour))
		state.Attempts["no-lease-fresh"] = record("no-lease-fresh", PhaseRunning, time.Time{}, now.Add(-30*time.Minute))
		state.Attempts["no-lease-old"] = record("no-lease-old", PhaseRunning, time.Time{}, now.Add(-OwnershipMaxAge-time.Minute))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	ownership := &JournalThreadOwnership{Home: host.Home, Now: func() time.Time { return now }, Log: slog.New(slog.NewTextHandler(&logged, nil))}
	for range 3 {
		owners, err := ownership.Threads(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		owned := owners.Live
		if len(owners.Settled) != 0 || len(owned) != 2 || owned["thread-live"] != "attempt-live" || owned["thread-no-lease-fresh"] != "attempt-no-lease-fresh" {
			t.Fatalf("owned = %v", owned)
		}
	}
	if n := strings.Count(logged.String(), "stale attempt records"); n != 1 {
		t.Fatalf("staleness logged %d times:\n%s", n, logged.String())
	}
	if !strings.Contains(logged.String(), "attempt-lapsed (thread thread-lapsed): assignment lease expired") || !strings.Contains(logged.String(), "attempt-no-lease-old") {
		t.Fatalf("log does not name the stale records:\n%s", logged.String())
	}
	// A renewed lease restores ownership, and the recovery is logged once.
	if err := host.service.Exchange.Runtime.journal.update(func(state *journalState) error {
		for _, id := range []string{"lapsed", "paused-lapsed"} {
			r := state.Attempts[id]
			r.Assignment.LeaseExpiresAt = now.Add(time.Minute)
			state.Attempts[id] = r
		}
		r := state.Attempts["no-lease-old"]
		r.UpdatedAt = now
		state.Attempts["no-lease-old"] = r
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	owners, err := ownership.Threads(context.Background())
	if err != nil || len(owners.Live) != 5 {
		t.Fatalf("owned after renewal = %v err=%v", owners, err)
	}
	if !strings.Contains(logged.String(), "no stale attempt records remain") {
		t.Fatalf("recovery not logged:\n%s", logged.String())
	}
}
