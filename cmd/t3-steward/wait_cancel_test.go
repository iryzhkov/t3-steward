package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// A task-bound wait is two records: the coordinator's, which parks the
// attempt, and the local check on the worker, which is the only one an
// operator sees in `wait list`. Cancelling the local half must settle the
// coordinator half, or the attempt stays parked on a wait nothing will ever
// settle until its deadline.
func TestLocalCancelOfATaskBoundCheckSettlesTheCoordinatorWait(t *testing.T) {
	ctx := context.Background()
	cfg, coordinator := taskWaitCLIFixture(t)
	if err := cmdTaskWaitAdd(ctx, cfg, []string{
		"--task", "current", "--request-id", "cancel-local", "--name", "cancel-local", "--", "false",
	}); err != nil {
		t.Fatal(err)
	}
	// The fixture keeps the local checks in the coordinator's own state file.
	checks, err := coordinator.ListWaits(ctx, "")
	if err != nil || len(checks) != 1 {
		t.Fatalf("checks=%v err=%v", checks, err)
	}
	local := checks[0]
	if local.TaskWaitID == "" || strings.HasPrefix(local.ID, "tw-") {
		t.Fatalf("local check %q is not bound the way registration binds it: %+v", local.ID, local)
	}

	var cancelErr error
	output := captureStdout(t, func() {
		cancelErr = cmdWaitControl(ctx, cfg, coordinator, "cancel", local.ID)
	})
	if cancelErr != nil {
		t.Fatal(cancelErr)
	}
	for _, want := range []string{local.ID, local.TaskWaitID, "resumes"} {
		if !strings.Contains(output, want) {
			t.Fatalf("the cancel report does not mention %q: %s", want, output)
		}
	}

	waits, err := coordinator.ListTaskWaits(ctx)
	if err != nil || len(waits) != 1 {
		t.Fatalf("waits=%v err=%v", waits, err)
	}
	if waits[0].Result == nil || waits[0].Result.Outcome != domain.TaskWaitCancelled {
		t.Fatalf("the coordinator wait was not settled as cancelled: %+v", waits[0])
	}
	checks, err = coordinator.ListWaits(ctx, "")
	if err != nil || len(checks) != 1 || checks[0].Status != wait.StatusCancelled {
		t.Fatalf("local check after cancel: %+v err=%v", checks, err)
	}
	// The coordinator's next tick resumes the attempt with the cancellation as
	// its outcome; nothing is left in waiting-external.
	if _, err := coordinator.WakeTaskWaits(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	records, err := coordinator.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := records.Attempts[0]; got.Progress == domain.ProgressWaitingExternal {
		t.Fatalf("the attempt is still parked after its only wait was cancelled: %q/%q", got.Progress, got.Control)
	}
}

// The coordinator record is what holds the attempt, so when it cannot be
// reached nothing is cancelled: the local row stays exactly as it was and the
// failure carries the transport class, so the operator retries rather than
// believing the wait is gone.
func TestLocalCancelLeavesTheCheckWaitingWhenTheCoordinatorIsUnreachable(t *testing.T) {
	ctx := context.Background()
	cfg, coordinator, worker := remoteTaskWaitFixture(t)
	if err := cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "unreachable", "--", "false"}); err != nil {
		t.Fatal(err)
	}
	checks, err := worker.ListWaits(ctx, "")
	if err != nil || len(checks) != 1 {
		t.Fatalf("checks=%v err=%v", checks, err)
	}
	// The fixture's carrier is a script on PATH; replace it with one that
	// reaches no coordinator at all.
	carrier, err := exec.LookPath("ssh")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(carrier, []byte("#!/bin/sh\nexit 255\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	err = cmdWaitControl(ctx, cfg, worker, "cancel", checks[0].ID)
	if err == nil {
		t.Fatal("a cancel that reached no coordinator reported success")
	}
	// The carrier dies without answering; the SSH client classifies that as a
	// transport failure (a protocol failure when no frame arrives), never as
	// the generic exit 1 that would read as "no such wait".
	if class := backlogadmin.ClassOf(err); class == "" || backlogadmin.ExitCodeFor(err) == 1 {
		t.Fatalf("the failure is not transport-classified: %q (%v)", class, err)
	}
	checks, err = worker.ListWaits(ctx, "")
	if err != nil || len(checks) != 1 || checks[0].Status != wait.StatusWaiting {
		t.Fatalf("the local check was changed although the coordinator never answered: %+v err=%v", checks, err)
	}
	waits, err := coordinator.ListTaskWaits(ctx)
	if err != nil || len(waits) != 1 || !waits[0].Live() {
		t.Fatalf("coordinator waits: %+v err=%v", waits, err)
	}
}

// The local list is where task-bound checks are visible on a worker, so it
// must say which coordinator wait each one belongs to, in both forms.
func TestLocalWaitListNamesTheTaskWaitAndHasJSON(t *testing.T) {
	ctx := context.Background()
	cfg, coordinator := taskWaitCLIFixture(t)
	if err := cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "listed", "--", "false"}); err != nil {
		t.Fatal(err)
	}
	checks, err := coordinator.ListWaits(ctx, "")
	if err != nil || len(checks) != 1 {
		t.Fatalf("checks=%v err=%v", checks, err)
	}
	var human bytes.Buffer
	if err := cmdWaitList(ctx, cfg, coordinator, []string{"--all"}, &human); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(human.String(), checks[0].TaskWaitID) {
		t.Fatalf("the human list does not name the task wait %s: %s", checks[0].TaskWaitID, human.String())
	}
	var machine bytes.Buffer
	if err := cmdWaitList(ctx, cfg, coordinator, []string{"--all", "--json"}, &machine); err != nil {
		t.Fatal(err)
	}
	var listed []wait.Wait
	if err := json.Unmarshal(machine.Bytes(), &listed); err != nil {
		t.Fatalf("wait list --json is not a JSON list: %v: %s", err, machine.String())
	}
	if len(listed) != 1 || listed[0].TaskWaitID != checks[0].TaskWaitID {
		t.Fatalf("listed=%+v", listed)
	}
}
