package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// taskWaitCLIFixture starts a real coordinator admin socket over a real store
// and points a configuration at both, so that `wait add --task current` runs
// the path a task runs on the fleet: the command parses its own flags, proves
// its check, registers over the transport, and stores the local poll itself.
func taskWaitCLIFixture(t *testing.T) (config.Config, *sqlite.Store) {
	t.Helper()
	// macOS bounds a Unix socket path at 104 bytes, and t.TempDir() carries the
	// test name, so the socket beside the state file needs a short root.
	root, err := os.MkdirTemp("", "t3-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	statePath := filepath.Join(root, "state.db")
	store, err := sqlite.OpenMigrated(statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	run := domain.WorkflowRun{
		ID: "run-1", WorkflowID: "workflow-1", Revision: 1,
		Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now,
	}
	tasks := []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassRequired}}
	run, err = domain.BindRunSink(run, tasks)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{run},
		Tasks:        tasks,
		Attempts: []domain.Attempt{{
			ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1, Revision: 7,
			Progress: domain.ProgressActive, Control: domain.ControlRunning,
			ThreadID: "thread-1", AssignmentID: "assign-1", UpdatedAt: now,
		}},
		Assignments: []domain.Assignment{{
			ID: "assign-1", AttemptID: "attempt-1", WorkerID: "worker", Epoch: 1,
			State: domain.AssignmentClaimed, LeaseToken: "lease", DispatchToken: "dispatch",
			ThreadID: "thread-1", CreatedAt: now, UpdatedAt: now,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(context.Background(), domain.WorkerSnapshot{
		WorkerID: "worker", WorkerEpoch: "worker-epoch-1", CoordinatorEpoch: 1, Sequence: 1,
		Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour),
		Inventory: domain.WorkerInventory{ID: "worker", AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now},
	}); err != nil {
		t.Fatal(err)
	}

	service, err := backlogadmin.New(store, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now })
	listener, err := backlogadmin.ListenLocal(statePath + ".admin.sock")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	server := &backlogadmin.LocalServer{
		Listener: listener, Service: coordinatorLocalService{admin: service}, CoordinatorID: "test-coordinator",
		AllowedUID: uint32(os.Getuid()), MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20,
		MaxSubmissionBytes: 1 << 20, RequestTimeout: 10 * time.Second, MaxConcurrent: 8,
	}
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	cfg := config.Default()
	cfg.StatePath = statePath
	cfg.BacklogV2.Transport = config.V2Transport{Kind: "ssh", RequestTimeout: config.Duration(10 * time.Second)}
	cfg.BacklogV2.MessageLimits = config.V2MessageLimits{MaxBytes: 1 << 20, MaxArtifactBytes: 1 << 20}

	t.Setenv(domain.TaskWaitEnvWorkflowRunID, "run-1")
	t.Setenv(domain.TaskWaitEnvTaskID, "task-1")
	t.Setenv(domain.TaskWaitEnvAttemptID, "attempt-1")
	t.Setenv(domain.TaskWaitEnvAttemptRevision, "7")
	t.Setenv(domain.TaskWaitEnvAssignmentID, "assign-1")
	t.Setenv(domain.TaskWaitEnvThreadID, "thread-1")
	return cfg, store
}

// Two registrations from inside one task, the second on an already-parked
// attempt, must both reach the coordinator as the mode the command was given.
// A wake mode that does not survive the command, the transport or the store
// turns every each wait into an all wait, which is the F6 observation exactly.
func TestTaskWaitRegistrationCarriesTheWakeModeThroughTheRealTransport(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	// The first registration names no wake mode, exactly as a task that takes
	// the documented default does; the second names it. Both mean each.
	if err := cmdTaskWaitAdd(ctx, cfg, []string{
		"--task", "current", "--request-id", "req-1", "--name", "req-1", "--", "false",
	}); err != nil {
		t.Fatalf("registering req-1: %v", err)
	}
	if err := cmdTaskWaitAdd(ctx, cfg, []string{
		"--task", "current", "--wake", "each", "--request-id", "req-2", "--name", "req-2", "--", "false",
	}); err != nil {
		t.Fatalf("registering req-2: %v", err)
	}

	records, err := store.ListTaskWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("the coordinator holds %d task-bound waits, want 2", len(records))
	}
	bound := map[string]bool{}
	for _, record := range records {
		if record.Wake != domain.WakeEach {
			t.Fatalf("wait %s was registered as each and stored as %q", record.ID, record.Wake)
		}
		bound[record.ID] = true
	}
	waits, err := store.ListWaits(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(waits) != 2 {
		t.Fatalf("the host holds %d local checks, want 2", len(waits))
	}
	for _, w := range waits {
		if !bound[w.TaskWaitID] {
			t.Fatalf("local check %s is bound to %q, which is no coordinator record", w.ID, w.TaskWaitID)
		}
	}
	attempt, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Attempts[0].Progress != domain.ProgressWaitingExternal {
		t.Fatalf("the attempt is %q after two registrations", attempt.Attempts[0].Progress)
	}
}

// waitTestControl is the thread seam of the wait runner.
type waitTestControl struct {
	threads  map[string]*domain.Thread
	sends    []string
	observed map[string]bool
}

func (c *waitTestControl) GetThread(_ context.Context, id string) (*domain.Thread, error) {
	return c.threads[id], nil
}

func (c *waitTestControl) ResumeThread(context.Context, domain.Thread, string) error { return nil }

func (c *waitTestControl) SendNodeWake(_ context.Context, _ domain.Thread, messageID, _ string) error {
	c.sends = append(c.sends, messageID)
	if c.observed == nil {
		c.observed = map[string]bool{}
	}
	c.observed[messageID] = true
	return nil
}

func (c *waitTestControl) ObserveNodeWake(_ context.Context, _, messageID string) (bool, error) {
	return c.observed[messageID], nil
}

// The whole fleet path in one process: the task registers two each waits
// through the command and the transport, the steward's own wait runner polls
// them, and one settles. The attempt must resume on that settlement alone.
func TestTwoRegisteredEachWaitsResumeTheAttemptOnOneSettlement(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	for _, request := range []string{"req-1", "req-2"} {
		if err := cmdTaskWaitAdd(ctx, cfg, []string{
			"--task", "current", "--wake", "each", "--request-id", request,
			"--name", request, "--", "false",
		}); err != nil {
			t.Fatalf("registering %s: %v", request, err)
		}
	}
	waits, err := store.ListWaits(ctx, "")
	if err != nil || len(waits) != 2 {
		t.Fatalf("waits=%v err=%v", waits, err)
	}
	settling := waits[1].ID

	control := &waitTestControl{threads: map[string]*domain.Thread{
		"thread-1": {ID: "thread-1", ProviderInstanceID: "claudeAgent"},
	}}
	runner := wait.New(store, control, nil)
	runner.DisableQuotaChecks = true
	now := time.Now().Add(2 * time.Minute)
	runner.SetClock(func() time.Time { return now })
	runner.Exec = func(_ context.Context, w wait.Wait) (string, int, error) {
		if w.ID == settling {
			return "done", 0, nil
		}
		return "not yet", 1, nil
	}
	runner.Tick(ctx, nil, nil)

	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := records.Attempts[0]; got.Progress != domain.ProgressActive || got.Control != domain.ControlResuming {
		t.Fatalf("the attempt did not resume on an each settlement: %q/%q at revision %d",
			got.Progress, got.Control, got.Revision)
	}
	if len(control.sends) != 1 {
		t.Fatalf("the resumed turn was told %d times, want once", len(control.sends))
	}
}
