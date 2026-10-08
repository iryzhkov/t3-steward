package workerruntime

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/providercontainment"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

const endedRunFailure = "contained run exceeded its 6000 MB memory reservation"

// endedRunDriver observes through the real LocalDriver and ContainedT3
// attachment path, so the error the runtime sees is exactly the one a
// contained run that systemd ended produces. StopPreparation stands in for the
// forced quiesce that proves custody before a terminal failure.
type endedRunDriver struct {
	fakeDriver
	local     LocalDriver
	stopErr   error
	stops     int
	phaseSeen []Phase
	runtime   **Runtime
}

func (d *endedRunDriver) ObserveThread(ctx context.Context, pkg workerproto.ExecutionPackage) (backlog.DispatchThreadState, error) {
	pkg.Environment.DirectoryBindings = []directoryresource.Binding{{}}
	return d.local.ObserveThread(ctx, pkg)
}

func (d *endedRunDriver) StopPreparation(context.Context, workerproto.ExecutionPackage) error {
	d.stops++
	if d.runtime != nil && *d.runtime != nil {
		if state, err := (*d.runtime).journal.snapshot(); err == nil {
			d.phaseSeen = append(d.phaseSeen, state.Attempts["assignment-1"].Phase)
		}
	}
	return d.stopErr
}

func newEndedRunDriver(root string, obs providercontainment.SupervisorObservation) *endedRunDriver {
	manager := ContainedT3{observe: func(context.Context, providercontainment.Launch) (providercontainment.SupervisorObservation, error) {
		return obs, nil
	}}
	driver := &endedRunDriver{fakeDriver: fakeDriver{workspace: filepath.Join(root, "workspace"), workspaceReady: true}}
	driver.local.ScopedT3 = attachFunc(func(ctx context.Context, _ workerproto.ExecutionPackage) (T3Control, error) {
		return nil, manager.observation(ctx, ContainedAttachment{InvocationID: "invocation"})
	})
	return driver
}

func endedRunRuntime(t *testing.T, root string, driver *endedRunDriver) *Runtime {
	t.Helper()
	runtime := newClaimedRuntime(t, root, &driver.fakeDriver)
	runtime.driver = driver
	return runtime
}

// A contained run that systemd ended with a known cause, such as the memory
// limit killing it, is a definite outcome. Every phase that observes a
// dispatched execution must fail the attempt with that cause once custody is
// proven, so the coordinator collects a failed result naming the reservation;
// observing it as an unavailable thread kept the attempt running for ever.
func TestEndedContainedRunFailsTheAttemptInEveryObservingPhase(t *testing.T) {
	for _, phase := range []Phase{PhaseRunning, PhaseStopped, PhaseWaiting, PhaseUnknown, PhaseDispatching} {
		t.Run(string(phase), func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			driver := newEndedRunDriver(root, providercontainment.SupervisorObservation{State: "failed/failed", Failure: endedRunFailure})
			runtime := endedRunRuntime(t, root, driver)
			driver.runtime = &runtime
			if err := runtime.markPhase("assignment-1", phase, "", driver.workspace, "thread-1"); err != nil {
				t.Fatal(err)
			}
			if err := runtime.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			state, err := runtime.journal.snapshot()
			if err != nil {
				t.Fatal(err)
			}
			record := state.Attempts["assignment-1"]
			if record.Phase != PhaseFailed || !strings.Contains(record.Failure, endedRunFailure) {
				t.Fatalf("ended run left the attempt %s with failure %q", record.Phase, record.Failure)
			}
			if record.WorkspacePath != driver.workspace {
				t.Fatalf("failed attempt lost its workspace %q", record.WorkspacePath)
			}
			if driver.stops != 1 || driver.phaseSeen[0] == PhaseFailed {
				t.Fatalf("custody was not proven before the terminal failure: stops=%d phases=%v", driver.stops, driver.phaseSeen)
			}
			collect := testCommand(t, runtime, domain.WorkerCommandCollect, "collect")
			if _, err := runtime.DeliverCommands(ctx, workerproto.CommandDelivery{Commands: []domain.WorkerCommand{collect}}); err != nil {
				t.Fatal(err)
			}
			state, _ = runtime.journal.snapshot()
			if driver.collectFailureCalls != 1 || state.Attempts["assignment-1"].Phase != PhaseCompleted || driver.collectCalls != 0 {
				t.Fatalf("failed result not published: failures=%d collects=%d phase=%s", driver.collectFailureCalls, driver.collectCalls, state.Attempts["assignment-1"].Phase)
			}
		})
	}
}

// Custody that cannot be proven keeps the attempt where it was, and a worker
// restart re-observes the same ended run and finishes failing it.
func TestEndedContainedRunFailsOnlyAfterCustodyAcrossRestart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	driver := newEndedRunDriver(root, providercontainment.SupervisorObservation{State: "failed/failed", Failure: endedRunFailure})
	driver.stopErr = errors.New("contained process custody unproven")
	runtime := endedRunRuntime(t, root, driver)
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := runtime.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
	}
	state, _ := runtime.journal.snapshot()
	if record := state.Attempts["assignment-1"]; record.Phase != PhaseRunning || driver.stops != 2 {
		t.Fatalf("unproven custody gave phase %s after %d stops", record.Phase, driver.stops)
	}
	driver.stopErr = nil
	runtime = reopenTestRuntime(t, root, &driver.fakeDriver)
	runtime.driver = driver
	if err := runtime.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ = runtime.journal.snapshot()
	if record := state.Attempts["assignment-1"]; record.Phase != PhaseFailed || !strings.Contains(record.Failure, endedRunFailure) {
		t.Fatalf("after restart the attempt is %s with failure %q", record.Phase, record.Failure)
	}
}

// Only a run systemd has ended is definite. An observation that is merely
// unavailable, and a recorded cause on a unit that is still running, keep the
// existing behaviour: the attempt keeps running and is re-observed.
func TestUnendedContainedObservationKeepsTheAttemptRunning(t *testing.T) {
	for name, obs := range map[string]providercontainment.SupervisorObservation{
		"unavailable":             {State: "recovery-required"},
		"cause on a running unit": {State: "active/running", InvocationID: "other", Failure: endedRunFailure},
		"ended without a cause":   {State: "failed/failed"},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			driver := newEndedRunDriver(root, obs)
			runtime := endedRunRuntime(t, root, driver)
			if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
				t.Fatal(err)
			}
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			state, _ := runtime.journal.snapshot()
			if record := state.Attempts["assignment-1"]; record.Phase != PhaseRunning || driver.stops != 0 {
				t.Fatalf("phase %s after %d stops", record.Phase, driver.stops)
			}
		})
	}
}
