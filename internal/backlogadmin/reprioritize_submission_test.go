package backlogadmin

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

func reprioritizeSubmissionFixture(t *testing.T) (*sqlite.Store, string, *Service) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlitetest.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	records := sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "workflow", Class: domain.TaskClassRequired, TaskIDs: []string{"task-1"}}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive, Revision: 3, CreatedAt: adminTestNow, UpdatedAt: adminTestNow}},
		Tasks:        []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassRequired}},
		Attempts:     []domain.Attempt{{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 4, UpdatedAt: adminTestNow}},
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	return store, path, reprioritizeSubmissionService(t, store)
}

func reprioritizeSubmissionService(t *testing.T, store *sqlite.Store) *Service {
	t.Helper()
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return adminTestNow })
	return service
}

func reprioritizeSubmissionRequest() Mutation {
	return Mutation{Version: Version, Principal: Principal{ID: "operator"}, ID: "reprioritize-1",
		Kind: domain.AdminCommandReprioritize, WorkflowRunID: "run-1", ExpectedRevision: 3,
		Reason: "raise importance", Payload: json.RawMessage(`{"importance":5}`)}
}

func reprioritizeSubmissionCommand(request Mutation) domain.AdminCommand {
	return domain.AdminCommand{ID: request.ID, Kind: request.Kind, TargetType: domain.AdminTargetWorkflowRun,
		TargetID: request.WorkflowRunID, ExpectedRevision: request.ExpectedRevision, Reason: request.Reason,
		RequestedBy: request.Principal.ID, Payload: request.Payload, State: domain.AdminCommandPending, CreatedAt: adminTestNow}
}

func TestRunReprioritizePayloadAndTargetValidation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, payload string
		valid         bool
	}{
		{"one", `{"importance":1}`, true}, {"five", `{"importance":5}`, true},
		{"zero", `{"importance":0}`, false}, {"six", `{"importance":6}`, false},
		{"absent", "", false}, {"missing", "{}", false}, {"null", `{"importance":null}`, false},
		{"fraction", `{"importance":1.5}`, false}, {"string", `{"importance":"5"}`, false},
		{"malformed", `{"importance":`, false}, {"null-object", "null", false},
		{"array", "[5]", false}, {"unknown-field", `{"importance":5,"scope":"run"}`, false},
		{"trailing", `{"importance":5} {}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, direct := range []bool{false, true} {
				store, _, service := reprioritizeSubmissionFixture(t)
				request := reprioritizeSubmissionRequest()
				request.Payload = json.RawMessage(tc.payload)
				var err error
				if direct {
					_, err = store.SubmitAdminCommand(context.Background(), reprioritizeSubmissionCommand(request))
				} else {
					_, err = service.Mutate(context.Background(), request)
				}
				if (err == nil) != tc.valid {
					t.Fatalf("direct=%t valid=%t: %v", direct, tc.valid, err)
				}
				loaded, loadErr := store.LoadCoordinatorRecords(context.Background())
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				if !tc.valid && len(loaded.AdminCommands) != 0 {
					t.Fatalf("invalid payload persisted: %#v", loaded.AdminCommands)
				}
			}
		})
	}
	for _, target := range []domain.AdminTargetType{domain.AdminTargetAttempt, domain.AdminTargetSchedule} {
		t.Run(string(target), func(t *testing.T) {
			t.Parallel()
			store, _, service := reprioritizeSubmissionFixture(t)
			request := reprioritizeSubmissionRequest()
			command := reprioritizeSubmissionCommand(request)
			command.TargetType = target
			command.TargetID = "attempt-1"
			if target == domain.AdminTargetAttempt {
				request.TaskID = "task-1"
			} else {
				request.WorkflowRunID, request.ScheduleID = "", "schedule-1"
				command.TargetID = "schedule-1"
			}
			if _, err := service.Mutate(context.Background(), request); !errors.Is(err, ErrInvalidQuery) {
				t.Fatalf("service target: %v", err)
			}
			if _, err := store.SubmitAdminCommand(context.Background(), command); !errors.Is(err, sqlite.ErrInvalidAdminCommand) {
				t.Fatalf("store target: %v", err)
			}
		})
	}
	t.Run("missing-run", func(t *testing.T) {
		t.Parallel()
		store, _, service := reprioritizeSubmissionFixture(t)
		request := reprioritizeSubmissionRequest()
		request.WorkflowRunID = ""
		if _, err := service.Mutate(context.Background(), request); err == nil {
			t.Fatal("service accepted missing run")
		}
		if _, err := store.SubmitAdminCommand(context.Background(), reprioritizeSubmissionCommand(request)); err == nil {
			t.Fatal("store accepted missing run")
		}
	})
	if domain.AdminCommandRunScoped(domain.AdminCommandReprioritize, json.RawMessage(`{"scope":"run"}`)) {
		t.Fatal("reprioritize became a run cancel")
	}
}

func TestRunReprioritizeSubmissionFencesAndReplays(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, path, service := reprioritizeSubmissionFixture(t)
	before, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request := reprioritizeSubmissionRequest()
	submitted, err := service.Mutate(ctx, request)
	if err != nil || submitted.Command.State != domain.AdminCommandPending {
		t.Fatalf("submission: %#v, %v", submitted, err)
	}
	if submitted.Command.TargetType != domain.AdminTargetWorkflowRun || submitted.Command.TargetID != request.WorkflowRunID ||
		submitted.Event.Kind != "admin-command-submitted" || submitted.Event.Actor != request.Principal.ID || submitted.Event.Reason != request.Reason ||
		submitted.Event.TargetType != domain.AdminTargetWorkflowRun || submitted.Event.TargetID != request.WorkflowRunID ||
		submitted.Event.WorkflowRunID != request.WorkflowRunID || submitted.Event.TaskID != "" || submitted.Event.AttemptID != "" {
		t.Fatalf("submission audit/target: %#v", submitted)
	}
	stale := request
	stale.ID, stale.ExpectedRevision = "stale", 2
	rejected, err := service.Mutate(ctx, stale)
	if err != nil || rejected.Command.State != domain.AdminCommandRejected || rejected.CurrentTarget == nil || rejected.CurrentTarget.Revision != 3 {
		t.Fatalf("stale fence: %#v, %v", rejected, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlitetest.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service = reprioritizeSubmissionService(t, store)
	replay, err := service.Mutate(ctx, request)
	if err != nil || !reflect.DeepEqual(replay, submitted) {
		t.Fatalf("replay: %#v, %v", replay, err)
	}
	direct, err := store.SubmitAdminCommand(ctx, reprioritizeSubmissionCommand(request))
	if err != nil || !reflect.DeepEqual(commandDTO(direct.Command), submitted.Command) || !reflect.DeepEqual(auditEventDTO(direct.Event), submitted.Event) {
		t.Fatalf("direct replay: %#v, %v", direct, err)
	}
	for i, change := range []func(*Mutation){
		func(r *Mutation) { r.Payload = json.RawMessage(`{"importance":1}`) },
		func(r *Mutation) { r.Principal.ID = "other" },
		func(r *Mutation) { r.Reason = "other reason" },
	} {
		changed := request
		change(&changed)
		if _, err := service.Mutate(ctx, changed); err == nil {
			t.Fatalf("service replay change %d accepted", i)
		}
		if _, err := store.SubmitAdminCommand(ctx, reprioritizeSubmissionCommand(changed)); !errors.Is(err, sqlite.ErrAdminCommandConflict) {
			t.Fatalf("store replay change %d: %v", i, err)
		}
	}
	loaded, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runs := make(map[string]domain.WorkflowRun)
	attempts := make(map[string]domain.Attempt)
	for _, run := range loaded.WorkflowRuns {
		runs[run.ID] = run
	}
	for _, attempt := range loaded.Attempts {
		attempts[attempt.ID] = attempt
	}
	for _, run := range before.WorkflowRuns {
		if !reflect.DeepEqual(runs[run.ID], run) {
			t.Fatalf("run changed: %#v", runs[run.ID])
		}
	}
	for _, attempt := range before.Attempts {
		if !reflect.DeepEqual(attempts[attempt.ID], attempt) {
			t.Fatalf("attempt changed: %#v", attempts[attempt.ID])
		}
	}
	if len(runs) != len(before.WorkflowRuns) || len(attempts) != len(before.Attempts) || !reflect.DeepEqual(loaded.Workflows, before.Workflows) ||
		len(loaded.AdminCommands) != 2 {
		t.Fatalf("unexpected submission effects: %#v", loaded)
	}
	events, err := store.LoadAuditEvents(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]int)
	for _, event := range events {
		counts[event.ID]++
	}
	if len(events) != 2 || counts[submitted.Event.ID] != 1 || counts[rejected.Event.ID] != 1 {
		t.Fatalf("audit replay duplicated events: %#v", events)
	}
}
