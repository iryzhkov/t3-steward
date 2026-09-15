package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The schedule client tests speak to the coordinator the way a client host
// does: a signed frame over a restricted SSH command, relayed into the
// coordinator's own socket, arriving at the real service under the
// remote-admin role. Reading the allowlist is not the same as exercising it;
// two operations have already been found unreachable from a client host by
// running them, and both looked correct in the source.
const (
	scheduleClientHelperEnabled   = "T3_STEWARD_TEST_SCHEDULE_CLIENT"
	scheduleClientHelperSocket    = "T3_STEWARD_TEST_SCHEDULE_CLIENT_SOCKET"
	scheduleClientHelperOperation = "T3_STEWARD_TEST_SCHEDULE_CLIENT_OPERATION"
)

const scheduleClientCoordinatorID = "normandy"

func scheduleClientCredentials() backlogadmin.AdminCredentials {
	return backlogadmin.AdminCredentials{
		ClientPrincipal:      "admin:omarchy-pc",
		ClientKeyID:          "admin-key-1",
		ClientSecret:         []byte("0123456789abcdef-client"),
		CoordinatorPrincipal: "coordinator:normandy",
		CoordinatorKeyID:     "coordinator-key-1",
		CoordinatorSecret:    []byte("0123456789abcdef-coord"),
	}
}

// TestScheduleClientHelperProcess is not a test. It is the coordinator side of
// one exchange, re-executed as a child process so the client really talks to a
// separate process, as it does over SSH.
func TestScheduleClientHelperProcess(t *testing.T) {
	if os.Getenv(scheduleClientHelperEnabled) != "1" {
		t.Skip("helper process")
	}
	server, err := backlogadmin.NewRemoteServer(backlogadmin.RemoteServerConfig{
		CoordinatorID: scheduleClientCoordinatorID,
		Clients: map[string]backlogadmin.AdminCredentials{
			scheduleClientCredentials().ClientPrincipal: scheduleClientCredentials(),
		},
		Relay: backlogadmin.LocalClient{
			Path:             os.Getenv(scheduleClientHelperSocket),
			CoordinatorID:    scheduleClientCoordinatorID,
			MaxResponseBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
			RequestTimeout: 30 * time.Second,
		},
		MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	if err := server.Serve(context.Background(), os.Getenv(scheduleClientHelperOperation), os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(4)
	}
	os.Exit(0)
}

type scheduleClientHarness struct {
	client    *backlogadmin.SSHClient
	local     backlogadmin.LocalClient
	statePath string
}

// startScheduleClientCoordinator runs the real coordinator runtime and returns a
// client that reaches it only through the remote carrier.
func startScheduleClientCoordinator(t *testing.T) scheduleClientHarness {
	t.Helper()
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = scheduleClientCoordinatorID
	cfg.BacklogV2.StartupAdmission = "closed"
	cfg.BacklogV2.Scheduling.Interval = config.Duration(10 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := runBacklogV2(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		done <- err
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("coordinator stopped with %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("coordinator did not stop")
		}
	})
	socketPath, err := resolveBacklogV2AdminSocketPath(cfg)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(socketPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("admin socket was not created: %s", socketPath)
		}
		time.Sleep(10 * time.Millisecond)
	}

	factory := func(ctx context.Context, _ string, args ...string) *exec.Cmd {
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=TestScheduleClientHelperProcess")
		command.Env = append(os.Environ(),
			scheduleClientHelperEnabled+"=1",
			scheduleClientHelperSocket+"="+socketPath,
			scheduleClientHelperOperation+"="+args[len(args)-1],
		)
		return command
	}
	client, err := backlogadmin.NewSSHClient(backlogadmin.SSHClientConfig{
		CoordinatorID:    scheduleClientCoordinatorID,
		Address:          "normandy",
		RemoteCommand:    "t3-steward",
		Credentials:      scheduleClientCredentials(),
		RequestTimeout:   60 * time.Second,
		MaxResponseBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
		Factory: factory,
	})
	if err != nil {
		t.Fatal(err)
	}
	return scheduleClientHarness{
		client: client,
		local: backlogadmin.LocalClient{
			Path: socketPath, MaxResponseBytes: int64(cfg.BacklogV2.MessageLimits.MaxBytes),
			MaxArtifactBytes:   int64(cfg.BacklogV2.MessageLimits.MaxArtifactBytes),
			MaxSubmissionBytes: cfg.BacklogV2.MessageLimits.MaxBytes,
			RequestTimeout:     30 * time.Second,
		},
		statePath: cfg.StatePath,
	}
}

// submitScheduleClientWorkflow puts one real version-2 workflow in the
// coordinator, from the client host, so the schedules below name something that
// exists.
func (h scheduleClientHarness) submitWorkflow(t *testing.T) string {
	t.Helper()
	archive := runtimeSubmissionTar(t)
	response, err := h.client.SubmitArchive(context.Background(),
		backlogadmin.LocalSubmissionRequest{IdempotencyKey: "schedule-client-workflow"},
		bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("client submit: %v", err)
	}
	return response.WorkflowID
}

func (h scheduleClientHarness) schedules(t *testing.T) []backlogadmin.Schedule {
	t.Helper()
	response, err := h.client.Query(context.Background(), backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QuerySchedules,
	})
	if err != nil {
		t.Fatalf("client schedules query: %v", err)
	}
	return response.Schedules
}

func (h scheduleClientHarness) schedule(t *testing.T, id string) (backlogadmin.Schedule, bool) {
	t.Helper()
	for _, item := range h.schedules(t) {
		if item.Schedule.ID == id {
			return item, true
		}
	}
	return backlogadmin.Schedule{}, false
}

// awaitClientCommand waits for one command submitted from the client host to
// reach a terminal state and returns it.
func (h scheduleClientHarness) awaitCommand(t *testing.T, commandID string) backlogadmin.Command {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		response, err := h.client.Query(context.Background(), backlogadmin.Query{
			Version: backlogadmin.Version, Kind: backlogadmin.QueryCommands,
		})
		if err != nil {
			t.Fatalf("client command query: %v", err)
		}
		for _, command := range response.Commands {
			if command.ID == commandID && command.State != domain.AdminCommandPending {
				return command
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("command %q never reached a terminal state", commandID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (h scheduleClientHarness) mutate(t *testing.T, mutation backlogadmin.Mutation) backlogadmin.Command {
	t.Helper()
	mutation.Version = backlogadmin.Version
	response, err := h.client.Mutate(context.Background(), mutation)
	if err != nil {
		t.Fatalf("client %s: %v", mutation.Kind, err)
	}
	if response.Command.State == domain.AdminCommandRejected {
		return response.Command
	}
	return h.awaitCommand(t, mutation.ID)
}

// TestScheduleClientCanDefineModifyAndControlSchedules exercises every schedule
// operation from a client host against the real coordinator: creating a new
// definition, modifying an existing one at its current revision, the four
// revision-fenced controls, the three read views, and removal.
func TestScheduleClientCanDefineModifyAndControlSchedules(t *testing.T) {
	harness := startScheduleClientCoordinator(t)
	workflowID := harness.submitWorkflow(t)
	ctx := context.Background()

	create := backlogadmin.LocalScheduleDefinitionRequest{
		RequestID: "client-nightly-1", ID: "nightly", Name: "Nightly",
		WorkflowID: workflowID, Expression: "0 2 * * *", Timezone: "UTC",
		AfterFailure: domain.ScheduleFailureNextCycle, Enabled: true,
		Reason: "client host creates a schedule",
	}
	created, err := harness.client.PutSchedule(ctx, backlogadmin.Principal{}, create)
	if err != nil {
		t.Fatalf("client put (new): %v", err)
	}
	if created.Schedule.ID != "nightly" || created.Schedule.Revision != 1 || created.Replay {
		t.Fatalf("created schedule = %+v", created)
	}
	// The audit actor is the coordinator's own view of the client, never the
	// identity the request claimed.
	if created.Schedule.WorkflowID != workflowID {
		t.Fatalf("created schedule workflow = %q", created.Schedule.WorkflowID)
	}

	// Modification at the current revision.
	modify := create
	modify.RequestID, modify.Expression, modify.ExpectedRevision = "client-nightly-2", "30 3 * * *", 1
	modify.Reason = "client host moves the hour"
	modified, err := harness.client.PutSchedule(ctx, backlogadmin.Principal{}, modify)
	if err != nil {
		t.Fatalf("client put (modify): %v", err)
	}
	if modified.Schedule.Revision != 2 || modified.Schedule.Expression != "30 3 * * *" ||
		modified.Schedule.Version != 2 || modified.Replay {
		t.Fatalf("modified schedule = %+v", modified)
	}

	// Replay of the same request id with the same content returns the original
	// rather than creating a second version.
	replayed, err := harness.client.PutSchedule(ctx, backlogadmin.Principal{}, modify)
	if err != nil {
		t.Fatalf("client put (replay): %v", err)
	}
	if !replayed.Replay || replayed.Schedule != modified.Schedule {
		t.Fatalf("replayed put = %+v, want the original %+v", replayed, modified)
	}

	// A stale expected revision is refused from a client host exactly as it is
	// locally: the fence is the coordinator's, not the caller's.
	stale := modify
	stale.RequestID, stale.ExpectedRevision, stale.Expression = "client-nightly-3", 1, "0 4 * * *"
	_, staleErr := harness.client.PutSchedule(ctx, backlogadmin.Principal{}, stale)
	if staleErr == nil || !strings.Contains(staleErr.Error(), "revision is 2, expected 1") {
		t.Fatalf("stale expected revision from a client host = %v", staleErr)
	}
	if current, _ := harness.schedule(t, "nightly"); current.Schedule.Revision != 2 {
		t.Fatalf("refused modification changed the schedule: %+v", current.Schedule)
	}

	// The three read views.
	if len(harness.schedules(t)) != 1 {
		t.Fatalf("client list = %+v", harness.schedules(t))
	}
	shown, ok := harness.schedule(t, "nightly")
	if !ok || shown.Schedule.Name != "Nightly" {
		t.Fatalf("client show = %+v", shown)
	}
	if shown.Triggers == nil && len(shown.Triggers) != 0 {
		t.Fatalf("client history = %+v", shown.Triggers)
	}

	// The revision-fenced controls, each read-then-fence like the CLI does.
	for _, control := range []struct {
		name    string
		kind    domain.AdminCommandKind
		payload string
	}{
		{name: "disable", kind: domain.AdminCommandDisable},
		{name: "enable", kind: domain.AdminCommandEnable},
		{name: "delay-next", kind: domain.AdminCommandDelayNext, payload: `{"until":"2099-01-01T00:00:00Z"}`},
	} {
		current, _ := harness.schedule(t, "nightly")
		command := harness.mutate(t, backlogadmin.Mutation{
			ID: "client-" + control.name, Kind: control.kind, ScheduleID: "nightly",
			ExpectedRevision: current.Schedule.Revision, Reason: "client host control",
			Payload: []byte(control.payload),
		})
		if command.State != domain.AdminCommandApplied {
			t.Fatalf("client %s = %+v", control.name, command)
		}
	}

	// A manual run from a client host, which is the operation that has to seed a
	// real run rather than an inert row.
	current, _ := harness.schedule(t, "nightly")
	runCommand := harness.mutate(t, backlogadmin.Mutation{
		ID: "client-run", Kind: domain.AdminCommandScheduleRun, ScheduleID: "nightly",
		ExpectedRevision: current.Schedule.Revision, Reason: "client host runs it now",
	})
	if runCommand.State != domain.AdminCommandApplied {
		t.Fatalf("client run = %+v", runCommand)
	}
	ran, _ := harness.schedule(t, "nightly")
	if ran.Schedule.ActiveRunID == "" {
		t.Fatalf("manual run left no active run: %+v", ran.Schedule)
	}
	assertClientRunHasWork(t, harness, ran.Schedule.ActiveRunID)

	// Delete is refused while that run is open.
	refused := harness.mutate(t, backlogadmin.Mutation{
		ID: "client-delete-open", Kind: domain.AdminCommandScheduleDelete, ScheduleID: "nightly",
		ExpectedRevision: ran.Schedule.Revision, Reason: "client host removes a live schedule",
	})
	if refused.State != domain.AdminCommandRejected ||
		!strings.Contains(refused.Failure, "while its run is open") {
		t.Fatalf("delete with an open run = %+v", refused)
	}
	if _, ok := harness.schedule(t, "nightly"); !ok {
		t.Fatal("refused delete removed the schedule anyway")
	}

	// A schedule that never ran deletes cleanly, from the same client host.
	weekly := backlogadmin.LocalScheduleDefinitionRequest{
		RequestID: "client-weekly-1", ID: "weekly", Name: "Weekly",
		WorkflowID: workflowID, Expression: "0 5 * * 0", Timezone: "UTC",
		AfterFailure: domain.ScheduleFailureNextCycle, Enabled: false,
		Reason: "client host creates a second schedule",
	}
	if _, err := harness.client.PutSchedule(ctx, backlogadmin.Principal{}, weekly); err != nil {
		t.Fatalf("client put (weekly): %v", err)
	}
	deleted := harness.mutate(t, backlogadmin.Mutation{
		ID: "client-delete", Kind: domain.AdminCommandScheduleDelete, ScheduleID: "weekly",
		ExpectedRevision: 1, Reason: "client host removes a schedule",
	})
	if deleted.State != domain.AdminCommandApplied {
		t.Fatalf("client delete = %+v", deleted)
	}
	if _, ok := harness.schedule(t, "weekly"); ok {
		t.Fatal("deleted schedule is still listed")
	}

	// The delete is audited and replaying its command id returns the same
	// terminal decision rather than acting again.
	replayDelete, err := harness.client.Mutate(ctx, backlogadmin.Mutation{
		Version: backlogadmin.Version, ID: "client-delete",
		Kind: domain.AdminCommandScheduleDelete, ScheduleID: "weekly",
		ExpectedRevision: 1, Reason: "client host removes a schedule",
	})
	if err != nil {
		t.Fatalf("replayed delete: %v", err)
	}
	if replayDelete.Command.ID != "client-delete" || replayDelete.Command.State != domain.AdminCommandApplied {
		t.Fatalf("replayed delete = %+v", replayDelete.Command)
	}
	if replayDelete.Event.ID == "" || replayDelete.Event.TargetID != "weekly" {
		t.Fatalf("delete audit event = %+v", replayDelete.Event)
	}

	// Negative control. If this harness were somehow granting local-admin
	// authority, every assertion above would pass for the wrong reason.
	//
	// The control is worker enrollment rather than an invented command kind.
	// An unknown kind proves only that the allowlist is consulted; enrollment
	// is a real, implemented operation that this coordinator genuinely performs
	// for a local operator and genuinely refuses to a remote client, because it
	// binds a worker to the coordinator's own identity and epoch. Refusing it
	// therefore distinguishes "restricted from remote-admin" from "not a thing".
	_, enrollErr := harness.client.EnrollWorker(ctx, domain.WorkerEnrollmentRequest{
		ID: "client-enrollment", WorkerID: "normandy", ExpectedRevision: 0,
		CatalogRevision: "catalog-1", Reason: "a client host may not enroll a worker",
	})
	if enrollErr == nil ||
		!strings.Contains(enrollErr.Error(), "not available to the remote-admin role") {
		t.Fatalf("worker enrollment from a client host = %v", enrollErr)
	}
	// The same operation is available to the coordinator's own local peer, which
	// is what makes the refusal above a restriction rather than an absence.
	if err := (localAdminAuthorizer{}).Authorize(ctx,
		backlogadmin.Principal{ID: "local:1000", Roles: []string{backlogadmin.LocalAdminRole}},
		backlogadmin.Action{Kind: "worker-enrollment"},
	); err != nil {
		t.Fatalf("local-admin was refused worker enrollment: %v", err)
	}
}

func assertClientRunHasWork(t *testing.T, harness scheduleClientHarness, runID string) {
	t.Helper()
	response, err := harness.client.Query(context.Background(), backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryWorkflow, WorkflowRunID: runID,
	})
	if err != nil {
		t.Fatalf("client run query: %v", err)
	}
	if response.Workflow == nil {
		t.Fatalf("scheduled run %q is not readable", runID)
	}
	executable := 0
	for _, task := range response.Workflow.Tasks {
		if task.Sink == nil && task.Attempt != nil {
			executable++
		}
	}
	if executable == 0 {
		t.Fatalf("scheduled run %q has no attempts: %+v", runID, response.Workflow.Tasks)
	}
	if len(response.Workflow.Artifacts) == 0 {
		t.Fatalf("scheduled run %q has no rebound artifacts", runID)
	}
}
