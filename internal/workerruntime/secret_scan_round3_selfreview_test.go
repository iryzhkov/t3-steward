package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
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

const throttleSecret = "synthetic-throttle-review-credential"

// preparedRunningRuntime is a runtime whose attempt was prepared and is running.
func preparedRunningRuntime(t *testing.T) (*Runtime, *fakeDriver) {
	t.Helper()
	root := t.TempDir()
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace")}
	runtime := newClaimedRuntime(t, root, driver)
	prepare := testCommand(t, runtime, domain.WorkerCommandPrepare, "prepare")
	if _, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{prepare}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	return runtime, driver
}

// A throttle acknowledgement is returned to the coordinator and stored in the
// journal; a driver error quoting a credential must not reach either raw.
func TestSecretScanThrottleAcknowledgementRedacted(t *testing.T) {
	for _, kind := range []domain.ThrottleCommandKind{domain.ThrottleCommandDrain, domain.ThrottleCommandHardStop} {
		t.Run(string(kind), func(t *testing.T) {
			runtime, driver := preparedRunningRuntime(t)
			driver.checkpointErr = errors.New("git push https://x:" + throttleSecret + "@host failed")
			driver.stopErr = driver.checkpointErr
			runtime.driver = redactingDriver{driver, throttleSecret}
			acks, err := runtime.DeliverThrottle(context.Background(), []domain.ThrottleCommand{testThrottle(runtime, kind, "t-1")})
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(acks)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), throttleSecret) || !strings.Contains(string(raw), "[redacted]") {
				t.Fatalf("throttle acknowledgement not redacted: %s", raw)
			}
			state, err := runtime.journal.snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if record, _ := json.Marshal(state.Attempts["assignment-1"]); strings.Contains(string(record), throttleSecret) {
				t.Fatalf("journal record carries the credential: %s", record)
			}
		})
	}
}

// An acknowledgement an earlier release stored raw is redacted when a
// redelivered command replays it.
func TestSecretScanLegacyAcknowledgementReplayRedacted(t *testing.T) {
	runtime, driver := preparedRunningRuntime(t)
	runtime.driver = redactingDriver{driver, throttleSecret}
	throttle := testThrottle(runtime, domain.ThrottleCommandHardStop, "t-legacy")
	command := testCommand(t, runtime, domain.WorkerCommandStop, "stop-legacy")
	raw := "kill helper --token=" + throttleSecret + " failed"
	if err := runtime.journal.update(func(state *journalState) error {
		record := state.Attempts["assignment-1"]
		if record.ThrottleRequests == nil {
			record.ThrottleRequests = map[string]domain.ThrottleCommand{}
		}
		if record.ThrottleResults == nil {
			record.ThrottleResults = map[string]domain.ThrottleAcknowledgement{}
		}
		record.ThrottleRequests[throttle.ID] = throttle
		record.ThrottleResults[throttle.ID] = domain.ThrottleAcknowledgement{CommandID: throttle.ID, AttemptID: throttle.AttemptID, Error: raw, AcknowledgedAt: runtimeTestNow}
		record.CommandRequests[command.ID] = command
		record.CommandResults[command.ID] = domain.WorkerAcknowledgement{CommandID: command.ID, Accepted: true, Detail: raw}
		state.Attempts["assignment-1"] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	throttleAcks, err := runtime.DeliverThrottle(context.Background(), []domain.ThrottleCommand{throttle})
	if err != nil {
		t.Fatal(err)
	}
	commandAcks, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{command}})
	if err != nil {
		t.Fatal(err)
	}
	for _, replay := range []any{throttleAcks, commandAcks} {
		encoded, err := json.Marshal(replay)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded), throttleSecret) || !strings.Contains(string(encoded), "[redacted]") {
			t.Fatalf("replayed acknowledgement not redacted: %s", encoded)
		}
	}
}

// The local quota watchdog logs driver errors from its drain and stop; they
// must not reach the runtime log raw.
func TestSecretScanLocalQuotaLogRedacted(t *testing.T) {
	for _, name := range []string{"drain", "stop"} {
		t.Run(name, func(t *testing.T) {
			now := runtimeTestNow
			driver := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true,
				observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive, backlog.DispatchThreadActive}}
			driver.checkpointErr = errors.New("checkpoint failed: token=" + throttleSecret)
			driver.stopErr = driver.checkpointErr
			runtime := runningRuntime(t, driver, &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}, &now)
			runtime.driver = redactingDriver{driver, throttleSecret}
			var logs bytes.Buffer
			runtime.log = slog.New(slog.NewTextHandler(&logs, nil))
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if name == "stop" {
				runtime.config.PauseEscalation = time.Second
				now = now.Add(time.Minute)
				if err := runtime.Reconcile(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if strings.Contains(logs.String(), throttleSecret) || !strings.Contains(logs.String(), "[redacted]") {
				t.Fatalf("quota watchdog log not redacted: %s", logs.String())
			}
		})
	}
}

// A task timeout whose stop fails logs the driver error; it must not reach the
// runtime log raw.
func TestSecretScanTaskTimeoutLogRedacted(t *testing.T) {
	now := runtimeTestNow
	driver := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true,
		stopErr: errors.New("stop failed: token=" + throttleSecret)}
	runtime := runningRuntime(t, driver, nil, &now)
	runtime.driver = redactingDriver{driver, throttleSecret}
	if err := runtime.journal.update(func(state *journalState) error {
		record := state.Attempts["assignment-1"]
		record.Package.Package.Timeout = time.Minute
		record.Package.Package.CreatedAt = runtimeTestNow.Add(-time.Hour)
		state.Attempts["assignment-1"] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	runtime.log = slog.New(slog.NewTextHandler(&logs, nil))
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.stopCalls == 0 {
		t.Fatalf("stop not attempted; logs: %s", logs.String())
	}
	if strings.Contains(logs.String(), throttleSecret) || !strings.Contains(logs.String(), "[redacted]") {
		t.Fatalf("task timeout log not redacted: %s", logs.String())
	}
}
