//go:build linux

package workerruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/providercontainment"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A contained run the memory limit killed has no T3 outcome left to capture.
// A forced quiesce, which failure collection and cleanup use, must still stop
// its unit and keep the named cause; otherwise the attempt can never end.
func TestForcedQuiesceStopsAContainedRunTheMemoryLimitKilled(t *testing.T) {
	manager, pkg, launch, stopped := oomKilledContainedRun(t)
	if err := manager.Quiesce(context.Background(), pkg, false); err == nil {
		t.Fatal("an unforced quiesce adopted a run whose outcome it cannot capture")
	}
	if err := manager.Quiesce(context.Background(), pkg, true); err != nil {
		t.Fatalf("forced quiesce of a memory-killed run: %v", err)
	}
	if _, err := os.Stat(stopped); err != nil {
		t.Fatalf("the unit was never stopped: %v", err)
	}
	observation, err := manager.Supervisor.Observe(context.Background(), launch)
	if err != nil || !observation.Stopped || observation.Failure != "contained run exceeded its 6000 MB memory reservation" {
		t.Fatalf("stopped observation %+v %v", observation, err)
	}
	retained, err := manager.Attach(context.Background(), pkg)
	if err != nil {
		t.Fatal(err)
	}
	if message, err := retained.LastAssistantMessage(context.Background(), pkg.Identity.ThreadID); err != nil || !strings.Contains(message, "6000 MB memory reservation") {
		t.Fatalf("retained outcome %q %v does not name the reservation", message, err)
	}
}

// containedRunDriver drives the runtime through the real LocalDriver and
// ContainedT3 for the contained package of the fixture.
type containedRunDriver struct {
	fakeDriver
	local *LocalDriver
	pkg   workerproto.ExecutionPackage
}

func (d *containedRunDriver) ObserveThread(ctx context.Context, _ workerproto.ExecutionPackage) (backlog.DispatchThreadState, error) {
	return d.local.ObserveThread(ctx, d.pkg)
}

func (d *containedRunDriver) StopPreparation(ctx context.Context, _ workerproto.ExecutionPackage) error {
	return d.local.StopPreparation(ctx, d.pkg)
}

func (d *containedRunDriver) StopThread(ctx context.Context, _ workerproto.ExecutionPackage) error {
	d.stopCalls++
	return d.local.StopThread(ctx, d.pkg)
}

func (d *containedRunDriver) Collect(ctx context.Context, _ workerproto.ExecutionPackage, workspace string) error {
	d.collectCalls++
	return d.local.Collect(ctx, d.pkg, workspace)
}

func containedRunRuntime(t *testing.T, phase Phase) (*Runtime, *containedRunDriver, ContainedT3, workerproto.ExecutionPackage) {
	t.Helper()
	manager, pkg, _, _ := oomKilledContainedRun(t)
	root := t.TempDir()
	driver := &containedRunDriver{fakeDriver: fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true},
		local: &LocalDriver{ScopedT3: manager}, pkg: pkg}
	runtime := newClaimedRuntime(t, root, &driver.fakeDriver)
	runtime.driver = driver
	if err := runtime.markPhase("assignment-1", phase, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	return runtime, driver, manager, pkg
}

func requireReservationFailure(t *testing.T, runtime *Runtime) {
	t.Helper()
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if record := state.Attempts["assignment-1"]; record.Phase != PhaseFailed || record.Failure != "contained run exceeded its 6000 MB memory reservation" {
		t.Fatalf("attempt is %s with failure %q", record.Phase, record.Failure)
	}
}

// The forced quiesce stops the unit before the failure is durable. A worker
// that dies in between finds a stopped unit with no provider outcome; the
// cause recorded with the unit still decides the failure, instead of the run
// reading as a thread that never started.
func TestStoppedContainedRunStillFailsWithTheReservation(t *testing.T) {
	_, driver, manager, pkg := containedRunRuntime(t, PhaseRunning)
	if err := manager.Quiesce(context.Background(), pkg, true); err != nil {
		t.Fatal(err)
	}
	runtime := reopenTestRuntime(t, filepath.Dir(driver.workspace), &driver.fakeDriver)
	runtime.driver = driver
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireReservationFailure(t, runtime)
}

// A task timeout that stops a run the memory limit already killed still
// reports the memory limit, which is why the run ended.
func TestTimeoutAfterTheMemoryLimitKeepsTheReservationFailure(t *testing.T) {
	runtime, driver, _, _ := containedRunRuntime(t, PhaseRunning)
	if err := runtime.journal.update(func(state *journalState) error {
		record := state.Attempts["assignment-1"]
		record.Package.Package.Timeout = time.Second
		record.Package.Package.CreatedAt = runtimeTestNow.Add(-time.Hour)
		state.Attempts["assignment-1"] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.stopCalls == 0 {
		t.Fatal("the task timeout did not stop the run")
	}
	requireReservationFailure(t, runtime)
}

// A collection that finds the run killed by the memory limit can never
// capture an outcome. It fails the attempt with that cause instead of
// deferring the collection on every pass.
func TestCollectionOfAContainedRunTheMemoryLimitKilledFails(t *testing.T) {
	runtime, driver, _, _ := containedRunRuntime(t, PhaseCollecting)
	deadline := time.Now().Add(testtiming.Bound(10 * time.Second))
	for {
		if err := runtime.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		state, err := runtime.journal.snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if state.Attempts["assignment-1"].Phase != PhaseCollecting || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if driver.collectCalls == 0 {
		t.Fatal("no collection ran")
	}
	requireReservationFailure(t, runtime)
}

// A running attempt whose contained run the memory limit killed ends as a
// failed attempt naming the reservation, after the real forced quiesce has
// stopped its unit, instead of waiting for an observation that never comes.
func TestRunningAttemptWhoseContainedRunTheMemoryLimitKilledFails(t *testing.T) {
	manager, pkg, _, stopped := oomKilledContainedRun(t)
	root := t.TempDir()
	driver := &containedRunDriver{fakeDriver: fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true},
		local: &LocalDriver{ScopedT3: manager}, pkg: pkg}
	runtime := newClaimedRuntime(t, root, &driver.fakeDriver)
	runtime.driver = driver
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if record := state.Attempts["assignment-1"]; record.Phase != PhaseFailed || record.Failure != "contained run exceeded its 6000 MB memory reservation" {
		t.Fatalf("attempt is %s with failure %q", record.Phase, record.Failure)
	}
	if _, err := os.Stat(stopped); err != nil {
		t.Fatalf("the attempt failed before its unit was stopped: %v", err)
	}
	retained, err := manager.Attach(context.Background(), pkg)
	if err != nil {
		t.Fatal(err)
	}
	if message, err := retained.LastAssistantMessage(context.Background(), pkg.Identity.ThreadID); err != nil || !strings.Contains(message, "6000 MB memory reservation") {
		t.Fatalf("retained outcome %q %v does not name the reservation", message, err)
	}
}

// oomKilledContainedRun prepares an attached contained run whose unit systemd
// reports as failed by the memory limit, with a systemctl stand-in on PATH. It
// returns the path the stand-in creates when the unit is stopped.
func oomKilledContainedRun(t *testing.T) (ContainedT3, workerproto.ExecutionPackage, providercontainment.Launch, string) {
	t.Helper()
	root := t.TempDir()
	journal, err := providercontainment.CanonicalRoot(filepath.Join(root, "journal"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(journal, 0700); err != nil {
		t.Fatal(err)
	}
	pkg := testPackage()
	pkg.Environment.DirectoryBindings = []directoryresource.Binding{{}}
	pkg.ResourceDemand = &domain.ResourceDemand{CPUUnits: 2, MemoryMB: 6000}
	manager := ContainedT3{Supervisor: providercontainment.Supervisor{Root: journal, Executable: "/bin/true"}}
	launch := providercontainment.Launch{ExecutionID: pkg.Identity.ThreadID, Spec: providercontainment.Spec{
		WorkerID: pkg.WorkerID, Directories: pkg.Environment.DirectoryBindings,
		Control: &directoryresource.Identity{}, Limits: containedLimits(pkg),
	}}
	key := sha256.Sum256([]byte(launch.Spec.WorkerID + "\x00" + launch.ExecutionID))
	unit := "t3-contained-" + hex.EncodeToString(key[:]) + ".service"
	if err := os.Mkdir(filepath.Join(journal, unit), 0700); err != nil {
		t.Fatal(err)
	}
	intent, err := json.Marshal(struct {
		Launch     providercontainment.Launch `json:"launch"`
		Executable string                     `json:"executable"`
	}{launch, manager.Supervisor.Executable})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(intent)
	digest := hex.EncodeToString(sum[:])
	if err := os.WriteFile(filepath.Join(journal, unit, "intent.json"), intent, 0600); err != nil {
		t.Fatal(err)
	}
	path, err := manager.recordPath(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if err := privateJSON(path+".preparation", containedPreparation{Identity: pkg.Identity, WorkerID: pkg.WorkerID, Launch: launch}); err != nil {
		t.Fatal(err)
	}
	attachment, err := json.Marshal(ContainedAttachment{WorkerID: pkg.WorkerID, Identity: pkg.Identity, Launch: launch, InvocationID: "invocation", EnvironmentID: "environment"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, attachment, 0600); err != nil {
		t.Fatal(err)
	}
	// A systemctl stand-in: the unit failed with the memory limit's result.
	bin := t.TempDir()
	stopped := filepath.Join(bin, "stopped")
	script := "#!/bin/sh\nif [ \"$2\" = stop ]; then : > " + stopped + "; exit 0; fi\n" +
		"printf 'LoadState=loaded\\nDescription=t3-containment:" + digest + "\\nActiveState=failed\\nSubState=failed\\nInvocationID=invocation\\nResult=oom-kill\\nCPUQuotaPerSecUSec=2s\\nMemoryMax=6291456000\\n'\n"
	if err := os.WriteFile(filepath.Join(bin, "systemctl"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	return manager, pkg, launch, stopped
}
