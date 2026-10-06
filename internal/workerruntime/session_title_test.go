package workerruntime

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// titleDriver is a fake driver whose T3 thread has a title Steward can read and
// set. lostResponse applies a title and still reports failure, which is a
// dispatch whose response never arrived.
type titleDriver struct {
	*fakeDriver
	title        string
	found        bool
	readErr      error
	setErr       error
	lostResponse bool
	reads        int
	sets         []string
}

func (d *titleDriver) ThreadTitle(context.Context, workerproto.ExecutionPackage) (string, bool, error) {
	d.reads++
	return d.title, d.found, d.readErr
}

func (d *titleDriver) SetThreadTitle(_ context.Context, _ workerproto.ExecutionPackage, title string) error {
	d.sets = append(d.sets, title)
	if d.lostResponse {
		d.title = title
		return errors.New("T3 dispatch: response lost")
	}
	if d.setErr != nil {
		return d.setErr
	}
	d.title = title
	return nil
}

func displayPackage() workerproto.ExecutionPackage {
	pkg := testPackage()
	pkg.Display = &workerproto.SessionDisplay{WorkflowName: "M16 review authority", TaskName: "implement"}
	pkg.RequiredCapabilities = append(pkg.RequiredCapabilities, workerproto.PackageCapabilitySessionDisplay)
	return pkg
}

func displayOffer(t *testing.T, pkg workerproto.ExecutionPackage) workerproto.AssignmentOffer {
	t.Helper()
	offer := testOffer(t)
	manifest, err := workerproto.BuildExecutionPackageManifest(pkg)
	if err != nil {
		t.Fatal(err)
	}
	offer.Package = manifest
	return offer
}

func sessionState(state workerproto.SessionState, progress *workerproto.CampaignProgress) workerproto.SnapshotRequest {
	return workerproto.SnapshotRequest{ParkedReported: true, SessionStatesReported: true, SessionStates: []workerproto.AssignmentSessionState{{
		AssignmentID: "assignment-1", AssignmentEpoch: 2, AttemptID: "attempt-1", AttemptRevision: 3,
		State: state, Progress: progress,
	}}}
}

// titleRuntime holds a running display attempt on thread-1 whose T3 title is
// still the initial one set at creation.
func titleRuntime(t *testing.T, root string, driver Driver, pkg workerproto.ExecutionPackage) *Runtime {
	t.Helper()
	runtime := signalRuntime(t, root, nil, func() time.Time { return runtimeTestNow })
	runtime.driver = driver
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{displayOffer(t, pkg)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", filepath.Join(root, "workspace"), "thread-1"); err != nil {
		t.Fatal(err)
	}
	return runtime
}

func newTitleDriver(root string, pkg workerproto.ExecutionPackage) *titleDriver {
	return &titleDriver{
		fakeDriver: &fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true},
		title:      workerproto.InitialSessionTitle(pkg), found: true,
	}
}

func applyTitles(t *testing.T, runtime *Runtime, request workerproto.SnapshotRequest) {
	t.Helper()
	if err := runtime.ApplyParkedAssignments(request); err != nil {
		t.Fatal(err)
	}
	runtime.UpdateSessionTitles(context.Background())
}

// The title follows each coordinator-recorded state, carries campaign progress
// when the coordinator has a count, and is set once per change.
func TestSessionTitleFollowsEveryRecordedLifecycleState(t *testing.T) {
	root := t.TempDir()
	pkg := displayPackage()
	driver := newTitleDriver(root, pkg)
	runtime := titleRuntime(t, root, driver, pkg)
	states := []workerproto.SessionState{
		workerproto.SessionStarting, workerproto.SessionRunning, workerproto.SessionWaiting, workerproto.SessionRunning,
		workerproto.SessionCollecting, workerproto.SessionCompleted, workerproto.SessionFailed, workerproto.SessionCancelled,
	}
	for index, state := range states {
		progress := &workerproto.CampaignProgress{Completed: index % 3, Total: 3}
		applyTitles(t, runtime, sessionState(state, progress))
		want := workerproto.SessionTitle(pkg, state, progress)
		if driver.title != want || len(driver.sets) != index+1 {
			t.Fatalf("after %s: title %q (sets %d), want %q", state, driver.title, len(driver.sets), want)
		}
		if record := mustRecord(t, runtime, "assignment-1"); record.SessionTitle == nil || record.SessionTitle.Last != want || record.SessionTitle.Pending != "" {
			t.Fatalf("last title Steward set is not stored: %+v", record.SessionTitle)
		}
		// Repeating the same statement is idempotent and does not even read T3.
		reads := driver.reads
		applyTitles(t, runtime, sessionState(state, progress))
		if len(driver.sets) != index+1 || driver.reads != reads {
			t.Fatalf("a repeated statement updated again: sets=%d reads=%d->%d", len(driver.sets), reads, driver.reads)
		}
	}
	// Without an authoritative count no progress is shown.
	applyTitles(t, runtime, sessionState(workerproto.SessionCancelled, nil))
	if strings.Contains(driver.title, "campaign") {
		t.Fatalf("progress shown without a count: %q", driver.title)
	}
}

// A title that is neither the one Steward last set nor the one it was setting
// is the operator's. Steward records why and never updates that thread again.
func TestSessionTitleStopsAfterAManualRename(t *testing.T) {
	root := t.TempDir()
	pkg := displayPackage()
	driver := newTitleDriver(root, pkg)
	runtime := titleRuntime(t, root, driver, pkg)
	applyTitles(t, runtime, sessionState(workerproto.SessionRunning, nil))
	driver.title = "Igor's review of M16"
	applyTitles(t, runtime, sessionState(workerproto.SessionWaiting, nil))
	applyTitles(t, runtime, sessionState(workerproto.SessionCompleted, nil))
	if driver.title != "Igor's review of M16" || len(driver.sets) != 1 {
		t.Fatalf("a renamed thread was overwritten: %q after %d sets", driver.title, len(driver.sets))
	}
	record := mustRecord(t, runtime, "assignment-1")
	if record.SessionTitle == nil || !record.SessionTitle.Stopped || !strings.Contains(record.SessionTitle.StopReason, "renamed") || record.SessionTitle.StoppedAt == nil {
		t.Fatalf("the stop is not recorded: %+v", record.SessionTitle)
	}
	// The stop is durable: a restarted worker does not resume updating.
	restarted := signalRuntime(t, root, nil, func() time.Time { return runtimeTestNow })
	restarted.driver = driver
	applyTitles(t, restarted, sessionState(workerproto.SessionFailed, nil))
	if len(driver.sets) != 1 {
		t.Fatalf("a restarted worker overwrote a renamed thread: %v", driver.sets)
	}
}

// The manual-rename check also holds before Steward's first update: the
// baseline is the initial title it set when it created the thread.
func TestSessionTitleRespectsARenameOfTheInitialTitle(t *testing.T) {
	root := t.TempDir()
	pkg := displayPackage()
	driver := newTitleDriver(root, pkg)
	driver.title = "renamed before any update"
	runtime := titleRuntime(t, root, driver, pkg)
	applyTitles(t, runtime, sessionState(workerproto.SessionRunning, nil))
	if len(driver.sets) != 0 || !mustRecord(t, runtime, "assignment-1").SessionTitle.Stopped {
		t.Fatalf("the initial title's rename was overwritten: %v", driver.sets)
	}
}

// A failed update is logged and never blocks the exchange; it is retried at the
// next state change rather than on every exchange.
func TestSessionTitleFailureDoesNotBlockAndRetriesAtTheNextStateChange(t *testing.T) {
	root := t.TempDir()
	pkg := displayPackage()
	driver := newTitleDriver(root, pkg)
	driver.observations = []backlog.DispatchThreadState{backlog.DispatchThreadActive, backlog.DispatchThreadActive, backlog.DispatchThreadActive}
	runtime := titleRuntime(t, root, driver, pkg)
	driver.setErr = errors.New("T3 is unavailable")
	if err := runtime.ApplyParkedAssignments(sessionState(workerproto.SessionRunning, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Snapshot(context.Background()); err != nil {
		t.Fatalf("a failed title update blocked the exchange: %v", err)
	}
	if len(driver.sets) != 1 || phaseOf(t, runtime, "assignment-1") != PhaseRunning {
		t.Fatalf("sets=%d phase=%s", len(driver.sets), phaseOf(t, runtime, "assignment-1"))
	}
	if _, err := runtime.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(driver.sets) != 1 {
		t.Fatalf("a failed update was retried without a state change: %v", driver.sets)
	}
	driver.readErr = errors.New("shell snapshot unavailable")
	if err := runtime.ApplyParkedAssignments(sessionState(workerproto.SessionWaiting, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Snapshot(context.Background()); err != nil {
		t.Fatalf("a failed title read blocked the exchange: %v", err)
	}
	driver.readErr, driver.setErr = nil, nil
	applyTitles(t, runtime, sessionState(workerproto.SessionCollecting, nil))
	want := workerproto.SessionTitle(pkg, workerproto.SessionCollecting, nil)
	if driver.title != want || mustRecord(t, runtime, "assignment-1").SessionTitle.Last != want {
		t.Fatalf("the next state change did not retry: %q", driver.title)
	}
}

// A worker that restarts after a dispatch whose response was lost converges on
// the current state: it recognises its own pending title, records it without
// sending it again, and keeps updating.
func TestSessionTitleConvergesAfterRestartWithoutDuplicateUpdates(t *testing.T) {
	root := t.TempDir()
	pkg := displayPackage()
	driver := newTitleDriver(root, pkg)
	driver.lostResponse = true
	runtime := titleRuntime(t, root, driver, pkg)
	applyTitles(t, runtime, sessionState(workerproto.SessionRunning, nil))
	running := workerproto.SessionTitle(pkg, workerproto.SessionRunning, nil)
	if record := mustRecord(t, runtime, "assignment-1"); record.SessionTitle.Pending != running {
		t.Fatalf("the intent was not recorded before the effect: %+v", record.SessionTitle)
	}
	driver.lostResponse = false

	restarted := signalRuntime(t, root, nil, func() time.Time { return runtimeTestNow })
	restarted.driver = driver
	applyTitles(t, restarted, sessionState(workerproto.SessionRunning, nil))
	if len(driver.sets) != 1 {
		t.Fatalf("the restarted worker sent the same title again: %v", driver.sets)
	}
	if record := mustRecord(t, restarted, "assignment-1"); record.SessionTitle.Last != running || record.SessionTitle.Pending != "" {
		t.Fatalf("restart did not converge the record: %+v", record.SessionTitle)
	}

	// The state moved while the worker was down, after a lost response.
	driver.lostResponse = true
	applyTitles(t, restarted, sessionState(workerproto.SessionWaiting, nil))
	driver.lostResponse = false
	again := signalRuntime(t, root, nil, func() time.Time { return runtimeTestNow })
	again.driver = driver
	applyTitles(t, again, sessionState(workerproto.SessionCompleted, nil))
	completed := workerproto.SessionTitle(pkg, workerproto.SessionCompleted, nil)
	if driver.title != completed || len(driver.sets) != 3 {
		t.Fatalf("restart after a lost response did not converge: %q, sets %v", driver.title, driver.sets)
	}
	if record := mustRecord(t, again, "assignment-1"); record.SessionTitle.Stopped {
		t.Fatalf("Steward's own pending title was read as an operator rename: %+v", record.SessionTitle)
	}
}

// Legacy paths keep today's behaviour: no display metadata, no statement from
// the coordinator, a statement about another execution, a thread T3 does not
// hold, or a driver without title support.
func TestSessionTitleLegacyPathsAreUnchanged(t *testing.T) {
	t.Run("no display metadata", func(t *testing.T) {
		root := t.TempDir()
		pkg := testPackage()
		driver := newTitleDriver(root, pkg)
		runtime := titleRuntime(t, root, driver, pkg)
		applyTitles(t, runtime, sessionState(workerproto.SessionRunning, nil))
		if len(driver.sets) != 0 || driver.reads != 0 {
			t.Fatalf("a package without display metadata was retitled: %v", driver.sets)
		}
	})
	t.Run("no coordinator statement", func(t *testing.T) {
		root := t.TempDir()
		pkg := displayPackage()
		driver := newTitleDriver(root, pkg)
		runtime := titleRuntime(t, root, driver, pkg)
		applyTitles(t, runtime, workerproto.SnapshotRequest{ParkedReported: true})
		if len(driver.sets) != 0 {
			t.Fatalf("retitled without a coordinator statement: %v", driver.sets)
		}
	})
	t.Run("another execution", func(t *testing.T) {
		root := t.TempDir()
		pkg := displayPackage()
		driver := newTitleDriver(root, pkg)
		runtime := titleRuntime(t, root, driver, pkg)
		statement := sessionState(workerproto.SessionRunning, nil)
		statement.SessionStates[0].AssignmentEpoch = 1
		applyTitles(t, runtime, statement)
		if len(driver.sets) != 0 {
			t.Fatalf("retitled from a statement about another execution: %v", driver.sets)
		}
	})
	t.Run("thread not yet created", func(t *testing.T) {
		root := t.TempDir()
		pkg := displayPackage()
		driver := newTitleDriver(root, pkg)
		runtime := titleRuntime(t, root, driver, pkg)
		if err := runtime.markPhase("assignment-1", PhaseDispatching, "", filepath.Join(root, "workspace"), "thread-1"); err != nil {
			t.Fatal(err)
		}
		applyTitles(t, runtime, sessionState(workerproto.SessionStarting, nil))
		if len(driver.sets) != 0 || driver.reads != 0 {
			t.Fatal("a thread that may not exist yet was retitled")
		}
		if err := runtime.markPhase("assignment-1", PhaseRunning, "", filepath.Join(root, "workspace"), "thread-1"); err != nil {
			t.Fatal(err)
		}
		driver.found = false
		runtime.UpdateSessionTitles(context.Background())
		if len(driver.sets) != 0 {
			t.Fatal("a thread T3 does not hold was retitled")
		}
	})
	t.Run("driver without title support", func(t *testing.T) {
		root := t.TempDir()
		pkg := displayPackage()
		driver := &fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true, observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive}}
		runtime := titleRuntime(t, root, driver, pkg)
		if err := runtime.ApplyParkedAssignments(sessionState(workerproto.SessionRunning, nil)); err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.Snapshot(context.Background()); err != nil {
			t.Fatal(err)
		}
		if record := mustRecord(t, runtime, "assignment-1"); record.SessionTitle != nil {
			t.Fatalf("a driver without title support recorded a title: %+v", record.SessionTitle)
		}
	})
}

// titledT3 is a T3 control that can set titles, as the production control can.
type titledT3 struct {
	*recordingT3
	updates []string
}

func (c *titledT3) UpdateThreadTitle(_ context.Context, threadID, title string) error {
	c.updates = append(c.updates, threadID+"="+title)
	c.thread.Title = title
	return nil
}

// The local driver retitles through the cached control and invalidates the
// cached listing; it leaves contained executions, no-effects mode and a control
// without the ability alone.
func TestLocalDriverSetsThreadTitlesThroughTheCachedControl(t *testing.T) {
	pkg := displayPackage()
	control := &titledT3{recordingT3: &recordingT3{thread: &domain.Thread{ID: "thread-1", Title: "initial"}}}
	driver := &LocalDriver{T3: NewCachedT3(control)}
	title, found, err := driver.ThreadTitle(context.Background(), pkg)
	if err != nil || !found || title != "initial" {
		t.Fatalf("title = %q, %v, %v", title, found, err)
	}
	if err := driver.SetThreadTitle(context.Background(), pkg, "next"); err != nil {
		t.Fatal(err)
	}
	if title, _, _ := driver.ThreadTitle(context.Background(), pkg); title != "next" || len(control.updates) != 1 || control.updates[0] != "thread-1=next" {
		t.Fatalf("title after update = %q, updates %v", title, control.updates)
	}

	contained := pkg
	contained.Environment.DirectoryBindings = []directoryresource.Binding{{}}
	dryRun := &LocalDriver{T3: control, Config: LocalDriverConfig{DryRun: true}}
	legacy := &LocalDriver{T3: NewCachedT3(control.recordingT3)}
	for name, check := range map[string]func() (bool, error){
		"contained": func() (bool, error) {
			_, found, err := driver.ThreadTitle(context.Background(), contained)
			return found, err
		},
		"dry run": func() (bool, error) {
			_, found, err := dryRun.ThreadTitle(context.Background(), pkg)
			return found, err
		},
		"legacy": func() (bool, error) {
			_, found, err := legacy.ThreadTitle(context.Background(), pkg)
			return found, err
		},
	} {
		if found, err := check(); found || err != nil {
			t.Fatalf("%s: a thread that must not be retitled was offered: %v, %v", name, found, err)
		}
	}
	if err := legacy.SetThreadTitle(context.Background(), pkg, "x"); err == nil || len(control.updates) != 1 {
		t.Fatalf("a control without title support was used: %v", err)
	}
}

// This build advertises that it understands the statement; the coordinator
// sends it to no other.
func TestWorkerAdvertisesSessionTitles(t *testing.T) {
	capabilities := AdvertisedCapabilities(nil)
	found := false
	for _, capability := range capabilities {
		found = found || capability == workerproto.CapabilitySessionTitles
	}
	if !found {
		t.Fatalf("capabilities = %v", capabilities)
	}
}
