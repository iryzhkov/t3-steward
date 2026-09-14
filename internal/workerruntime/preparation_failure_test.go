package workerruntime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestTerminalPreparationReasonQuotesTheFirstCause(t *testing.T) {
	root := t.TempDir()
	driver := &fakeDriver{
		workspace: filepath.Join(root, "workspace"),
		prepareErrs: []error{
			errors.New("prepare repository cache: authentication failed"),
			errors.New(`attempt directory "attempt-1" already exists`),
			errors.New("create preparation stage: no space left on device"),
		},
	}
	runtime := newClaimedRuntime(t, root, driver)
	prepare := testCommand(t, runtime, domain.WorkerCommandPrepare, "prepare")
	if _, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{prepare}}); err != nil {
		t.Fatal(err)
	}
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got := state.Attempts["assignment-1"].FirstPrepareFailure; got != "prepare repository cache: authentication failed" {
		t.Fatalf("first preparation failure = %q, want it recorded after the first attempt", got)
	}

	for i := 0; i < MaxPrepareAttempts; i++ {
		if err := runtime.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	state, err = runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	record := state.Attempts["assignment-1"]
	want := "preparation failed 3 times; " +
		"first error: prepare repository cache: authentication failed; " +
		"last error: create preparation stage: no space left on device"
	if record.Phase != PhaseFailed || record.Failure != want {
		t.Fatalf("terminal record = %+v\nwant failure %q", record, want)
	}
	if driver.prepareCalls != MaxPrepareAttempts {
		t.Fatalf("prepare calls = %d, want %d", driver.prepareCalls, MaxPrepareAttempts)
	}
}
