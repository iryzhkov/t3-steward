package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
)

// fakeReloadSignaller plays the coordinator: when signalled it writes the
// receipt it was given, stamped at the fake clock, or nothing at all.
type fakeReloadSignaller struct {
	writer   *reloadReceiptWriter
	answer   *backlogadmin.ReloadReceipt
	clock    *time.Time
	signals  []int
	failWith error
}

func (f *fakeReloadSignaller) Signal(pid int) error {
	f.signals = append(f.signals, pid)
	if f.failWith != nil {
		return f.failWith
	}
	if f.answer == nil {
		return nil
	}
	receipt := *f.answer
	receipt.RequestedAt = *f.clock
	receipt.CompletedAt = f.clock.Add(50 * time.Millisecond)
	return f.writer.Write(receipt)
}

func reloadCommandFixture(t *testing.T) (config.Config, *reloadReceiptWriter, *fakeReloadSignaller, func() time.Time) {
	t.Helper()
	cfg := config.Default()
	cfg.StatePath = filepath.Join(t.TempDir(), "state.db")
	writer, err := newReloadReceiptWriter(cfg, "rc.69")
	if err != nil {
		t.Fatal(err)
	}
	remove, err := writeCoordinatorPID(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(remove)
	clock := time.Date(2026, 9, 18, 4, 0, 0, 0, time.UTC)
	signaller := &fakeReloadSignaller{writer: writer, clock: &clock}
	return cfg, writer, signaller, func() time.Time { return clock }
}

// The verb signals the pid the coordinator wrote, waits for a receipt
// requested at or after the signal and prints it; accepted and unchanged exit 0.
func TestCoordinatorReloadPrintsTheReceiptForAcceptedAndUnchanged(t *testing.T) {
	for _, outcome := range []string{backlogadmin.ReloadAccepted, backlogadmin.ReloadUnchanged} {
		t.Run(outcome, func(t *testing.T) {
			cfg, _, signaller, now := reloadCommandFixture(t)
			signaller.answer = &backlogadmin.ReloadReceipt{Outcome: outcome, ConfigurationDigest: "digest-after", PreviousDigest: "digest-before"}
			var out bytes.Buffer
			if err := runCoordinatorReload(context.Background(), cfg, &out, false, time.Second, signaller, now); err != nil {
				t.Fatalf("%s reload failed: %v", outcome, err)
			}
			if len(signaller.signals) != 1 || signaller.signals[0] != os.Getpid() {
				t.Fatalf("signals = %v, want the pid from the pid file (%d)", signaller.signals, os.Getpid())
			}
			if !strings.Contains(out.String(), "reload       "+outcome+" at 2026-09-18T04:00:00Z (digest-after)") {
				t.Fatalf("text output lacks the receipt line:\n%s", out.String())
			}
			out.Reset()
			if err := runCoordinatorReload(context.Background(), cfg, &out, true, time.Second, signaller, now); err != nil {
				t.Fatal(err)
			}
			var document map[string]any
			if err := json.Unmarshal(out.Bytes(), &document); err != nil {
				t.Fatalf("invalid JSON %q: %v", out.String(), err)
			}
			if document["kind"] != "coordinator-reload" || document["outcome"] != outcome || document["configurationDigest"] != "digest-after" || document["release"] != "rc.69" {
				t.Fatalf("document = %v", document)
			}
		})
	}
}

// A rejection prints the receipt with its blockers and exits 8, class
// rejected, so the stage 1 error envelope and exit table apply.
func TestCoordinatorReloadRejectionExitsRejectedWithBlockers(t *testing.T) {
	cfg, _, signaller, now := reloadCommandFixture(t)
	signaller.answer = &backlogadmin.ReloadReceipt{
		Outcome: backlogadmin.ReloadRejected, Error: "worker normandy has retained assignment assignment-1",
		ConfigurationDigest: "digest-before", PreviousDigest: "digest-before",
		Blockers: []backlogadmin.ReloadBlocker{{
			WorkerID: "normandy", AssignmentID: "assignment-1", AttemptID: "attempt-1", Progress: "active", Control: "paused",
			Unblock: "wait for the pause to lift, or t3-steward backlog cancel run-1/implement --reason TEXT",
		}},
	}
	var out bytes.Buffer
	err := runCoordinatorReload(context.Background(), cfg, &out, false, time.Second, signaller, now)
	if backlogadmin.ClassOf(err) != backlogadmin.ClassRejected || backlogadmin.ExitCodeFor(err) != 8 {
		t.Fatalf("class = %q, exit = %d (%v)", backlogadmin.ClassOf(err), backlogadmin.ExitCodeFor(err), err)
	}
	for _, want := range []string{"reload       rejected at", "error: worker normandy has retained assignment assignment-1", "attempt attempt-1 active/paused", "backlog cancel run-1/implement"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("text output lacks %q:\n%s", want, out.String())
		}
	}
	out.Reset()
	err = runCoordinatorReload(context.Background(), cfg, &out, true, time.Second, signaller, now)
	if backlogadmin.ExitCodeFor(err) != 8 {
		t.Fatalf("--json exit = %d (%v)", backlogadmin.ExitCodeFor(err), err)
	}
	// The document is the answer; the envelope must not follow it.
	assertExactlyOneJSONDocument(t, "coordinator reload --json", out.Bytes())
	if reportJSONError([]string{"coordinator", "reload", "--json"}, err) == nil {
		t.Fatal("the rejection was swallowed")
	}
	var document map[string]any
	if err := json.Unmarshal(out.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	blockers, _ := document["blockers"].([]any)
	if len(blockers) != 1 || document["outcome"] != "rejected" {
		t.Fatalf("document = %v", document)
	}
}

// No receipt newer than the signal within --wait is a timeout, exit 6: an
// older receipt is the previous verdict and never counts as this one's.
func TestCoordinatorReloadTimesOutWithoutANewerReceipt(t *testing.T) {
	cfg, writer, signaller, now := reloadCommandFixture(t)
	old := backlogadmin.ReloadReceipt{Outcome: backlogadmin.ReloadAccepted, RequestedAt: now().Add(-time.Hour), CompletedAt: now().Add(-time.Hour), ConfigurationDigest: "d", PreviousDigest: "d"}
	if err := writer.Write(old); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err := runCoordinatorReload(context.Background(), cfg, &out, false, 50*time.Millisecond, signaller, now)
	if backlogadmin.ClassOf(err) != backlogadmin.ClassTimeout || backlogadmin.ExitCodeFor(err) != 6 {
		t.Fatalf("class = %q, exit = %d (%v)", backlogadmin.ClassOf(err), backlogadmin.ExitCodeFor(err), err)
	}
	if !strings.Contains(err.Error(), "reload-receipt.json") || !strings.Contains(err.Error(), "predates the receipt") {
		t.Fatalf("timeout error does not say where to look: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("a timeout printed a receipt:\n%s", out.String())
	}
}

// Without a pid file, or with a process that cannot be signalled, the verb
// reports the coordinator unavailable (exit 5) rather than waiting.
func TestCoordinatorReloadWithoutACoordinatorIsUnavailable(t *testing.T) {
	cfg, _, signaller, now := reloadCommandFixture(t)
	signaller.failWith = os.ErrProcessDone
	var out bytes.Buffer
	err := runCoordinatorReload(context.Background(), cfg, &out, false, time.Second, signaller, now)
	if backlogadmin.ClassOf(err) != backlogadmin.ClassUnavailable || backlogadmin.ExitCodeFor(err) != 5 {
		t.Fatalf("class = %q, exit = %d (%v)", backlogadmin.ClassOf(err), backlogadmin.ExitCodeFor(err), err)
	}
	pidPath, _ := coordinatorPIDPath(cfg)
	if err := os.Remove(pidPath); err != nil {
		t.Fatal(err)
	}
	signaller.signals = nil
	err = runCoordinatorReload(context.Background(), cfg, &out, false, time.Second, signaller, now)
	if backlogadmin.ClassOf(err) != backlogadmin.ClassUnavailable || !strings.Contains(err.Error(), pidPath) {
		t.Fatalf("missing pid file: class = %q (%v)", backlogadmin.ClassOf(err), err)
	}
	if len(signaller.signals) != 0 {
		t.Fatalf("a missing pid file still signalled %v", signaller.signals)
	}
}

// The verb is reachable as "coordinator reload" and refuses what it does not
// understand, the same way "coordinator identity" does.
func TestCoordinatorReloadFlagsAreParsed(t *testing.T) {
	if err := cmdCoordinator(globalFlags{}, []string{"reload", "--no-such-flag"}); err == nil || !strings.Contains(err.Error(), "coordinator reload") {
		t.Fatalf("unknown flag error = %v", err)
	}
	if err := cmdCoordinator(globalFlags{}, []string{"reload", "--wait", "0s"}); err == nil || !strings.Contains(err.Error(), "positive") {
		t.Fatalf("zero wait error = %v", err)
	}
	if err := cmdCoordinator(globalFlags{}, []string{"reload", "extra"}); err == nil || !strings.Contains(err.Error(), "no arguments") {
		t.Fatalf("extra argument error = %v", err)
	}
	output := captureStdout(t, func() {
		if err := cmdCoordinator(globalFlags{}, []string{"reload", "--help"}); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(output, "--wait DURATION") || !strings.Contains(output, "8  rejected") {
		t.Fatalf("help = %q", output)
	}
	output = captureStdout(t, func() {
		if err := cmdCoordinator(globalFlags{}, []string{"help"}); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(output, "reload [--json] [--wait DURATION]") {
		t.Fatalf("coordinator help does not list reload: %q", output)
	}
}
