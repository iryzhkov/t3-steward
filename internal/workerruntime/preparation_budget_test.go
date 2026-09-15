package workerruntime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// observingDriver reports what the journal said while its preparation ran, so
// a test can see whether the attempt was counted before it ran or after it
// failed.
type observingDriver struct {
	*fakeDriver
	runtime  *Runtime
	observed []int
}

func (d *observingDriver) Prepare(ctx context.Context, pkg workerproto.ExecutionPackage) (string, error) {
	state, err := d.runtime.journal.snapshot()
	if err != nil {
		return "", err
	}
	d.observed = append(d.observed, state.Attempts["assignment-1"].PrepareAttempts)
	return d.fakeDriver.Prepare(ctx, pkg)
}

// A preparation attempt is counted before it runs. A worker that died between
// the driver failing and the journal write used to come back believing no
// attempt had been made: the budget never terminated, and every restart took
// another preparation-log ordinal until retention itself failed.
func TestPreparationIsCountedBeforeItRuns(t *testing.T) {
	driver := &observingDriver{fakeDriver: &fakeDriver{prepareErr: errors.New("clone failed")}}
	runtime := newClaimedRuntime(t, t.TempDir(), driver.fakeDriver)
	driver.runtime = runtime
	runtime.driver = driver

	if err := runtime.prepare(context.Background(), "assignment-1"); err == nil {
		t.Fatal("a failed preparation reported success")
	}
	if len(driver.observed) != 1 || driver.observed[0] != 1 {
		t.Fatalf("the journal said %v while the first preparation ran, want [1]", driver.observed)
	}
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if record := state.Attempts["assignment-1"]; record.PrepareAttempts != 1 || record.FirstPrepareFailure != "clone failed" {
		t.Fatalf("after the failure: attempts %d, first %q", record.PrepareAttempts, record.FirstPrepareFailure)
	}
}

// The budget terminates even when a crash cost the first cause, and the
// terminal reason says the first cause was not retained rather than quoting a
// later error as if it were the first.
func TestPreparationBudgetTerminatesAfterACrashLostTheFirstCause(t *testing.T) {
	ctx := context.Background()
	driver := &fakeDriver{prepareErrs: []error{
		errors.New("second failure"), errors.New("third failure"),
	}}
	runtime := newClaimedRuntime(t, t.TempDir(), driver)
	// What a crash between the driver failing and the journal write leaves: the
	// attempt is counted, its cause is not.
	if err := runtime.setPrepareAttempts("assignment-1", 1); err != nil {
		t.Fatal(err)
	}

	if err := runtime.prepare(ctx, "assignment-1"); err == nil {
		t.Fatal("the second preparation reported success")
	}
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if record := state.Attempts["assignment-1"]; record.PrepareAttempts != 2 || record.FirstPrepareFailure != "" {
		t.Fatalf("a later error was recorded as the first cause: %+v", record)
	}

	// The third attempt exhausts the budget, which is the point: before this the
	// count reset on every crash and the attempt never became terminal.
	if err := runtime.prepare(ctx, "assignment-1"); err != nil {
		t.Fatalf("terminal preparation: %v", err)
	}
	state, err = runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	record := state.Attempts["assignment-1"]
	if record.Phase != PhaseFailed {
		t.Fatalf("phase = %q, want the attempt to be terminal after its budget", record.Phase)
	}
	wantLog := backlog.PreparationLogName("attempt-1", 1)
	for _, want := range []string{
		"preparation failed 3 times", "first error: not retained", wantLog, "last error: third failure",
	} {
		if !strings.Contains(record.Failure, want) {
			t.Fatalf("terminal reason %q does not contain %q", record.Failure, want)
		}
	}
	if strings.Contains(record.Failure, "first error: second failure") {
		t.Fatalf("the terminal reason quotes a later error as the first: %q", record.Failure)
	}
}

// An uncertain contained preparation still spends nothing: the reservation is
// given back, because exhausting a budget on an unknown outcome would turn not
// knowing into a terminal decision.
func TestUncertainContainedPreparationGivesTheReservationBack(t *testing.T) {
	driver := &fakeDriver{prepareErr: fmt.Errorf("%w: lost start reply", ErrContainedCustody)}
	runtime := newClaimedRuntime(t, t.TempDir(), driver)
	for range MaxPrepareAttempts + 2 {
		if err := runtime.prepare(context.Background(), "assignment-1"); !errors.Is(err, ErrContainedCustody) {
			t.Fatalf("prepare: %v", err)
		}
	}
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if record := state.Attempts["assignment-1"]; record.PrepareAttempts != 0 || record.Phase != PhasePreparing {
		t.Fatalf("uncertain preparation spent budget: %+v", record)
	}
}
