package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type fakeAdminMutationService struct {
	queryResponse  backlogadmin.Response
	queryErr       error
	mutateResponse backlogadmin.MutationResponse
	mutateErr      error
	queries        []backlogadmin.Query
	mutations      []backlogadmin.Mutation
}

func (f *fakeAdminMutationService) Query(_ context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
	f.queries = append(f.queries, query)
	return f.queryResponse, f.queryErr
}

func (f *fakeAdminMutationService) Mutate(_ context.Context, mutation backlogadmin.Mutation) (backlogadmin.MutationResponse, error) {
	f.mutations = append(f.mutations, mutation)
	return f.mutateResponse, f.mutateErr
}

func TestParseBacklogMutations(t *testing.T) {
	tests := []struct {
		command string
		kind    domain.AdminCommandKind
		extra   []string
		payload string
	}{
		{command: "start", kind: domain.AdminCommandStart},
		{command: "delay", kind: domain.AdminCommandDelay, extra: []string{"--until", "2026-09-11T02:00:00-07:00"}, payload: `{"until":"2026-09-11T09:00:00Z"}`},
		{command: "pause", kind: domain.AdminCommandPause, payload: `{"now":false}`},
		{command: "pause", kind: domain.AdminCommandPause, extra: []string{"--now"}, payload: `{"now":true}`},
		{command: "resume", kind: domain.AdminCommandResume},
		{command: "cancel", kind: domain.AdminCommandCancel},
		{command: "retry", kind: domain.AdminCommandRetry},
		{command: "skip", kind: domain.AdminCommandSkip},
	}
	for _, test := range tests {
		name := test.command
		if len(test.extra) > 0 {
			name += strings.Join(test.extra, "_")
		}
		t.Run(name, func(t *testing.T) {
			args := []string{test.command, "run-1/task-1", "--reason", "operator request", "--command-id", "command-1", "--json"}
			args = append(args, test.extra...)
			got, err := parseBacklogMutation(args)
			if err != nil {
				t.Fatal(err)
			}
			if got.kind != test.kind || got.workflowRunID != "run-1" || got.taskID != "task-1" ||
				got.reason != "operator request" || got.commandID != "command-1" || !got.asJSON ||
				string(got.payload) != test.payload {
				t.Fatalf("invocation = %#v", got)
			}
		})
	}
}

func TestParseScheduleMutations(t *testing.T) {
	tests := []struct {
		command string
		kind    domain.AdminCommandKind
		extra   []string
		payload string
	}{
		{command: "run", kind: domain.AdminCommandScheduleRun},
		{command: "delay-next", kind: domain.AdminCommandDelayNext, extra: []string{"--until", "2026-09-11T10:00:00Z"}, payload: `{"until":"2026-09-11T10:00:00Z"}`},
		{command: "enable", kind: domain.AdminCommandEnable},
		{command: "disable", kind: domain.AdminCommandDisable},
	}
	for _, test := range tests {
		t.Run(test.command, func(t *testing.T) {
			args := []string{test.command, "nightly", "--reason", "operator request"}
			args = append(args, test.extra...)
			got, err := parseScheduleMutation(args)
			if err != nil {
				t.Fatal(err)
			}
			if got.kind != test.kind || got.scheduleID != "nightly" || got.reason != "operator request" ||
				string(got.payload) != test.payload {
				t.Fatalf("invocation = %#v", got)
			}
		})
	}
}

func TestMutationParsingRejectsUnsafeOrIncompleteArguments(t *testing.T) {
	tests := []struct {
		schedule bool
		args     []string
	}{
		{args: []string{"pause", "task-only", "--reason", "why"}},
		{args: []string{"pause", "run/task"}},
		{args: []string{"pause", "run/task", "--reason", " padded "}},
		{args: []string{"pause", "run/task", "--reason", "why", "--now", "--now"}},
		{args: []string{"start", "run/task", "--reason", "why", "--now"}},
		{args: []string{"delay", "run/task", "--reason", "why"}},
		{args: []string{"delay", "run/task", "--reason", "why", "--until", "tomorrow"}},
		{args: []string{"retry", "run/task", "--reason", "why", "--until", "2026-09-11T10:00:00Z"}},
		{args: []string{"skip", "run/task", "--reason", "why", "--unknown"}},
		{schedule: true, args: []string{"run", "bad/id", "--reason", "why"}},
		{schedule: true, args: []string{"delay-next", "nightly", "--reason", "why"}},
		{schedule: true, args: []string{"enable", "nightly", "--reason", "why", "--until", "2026-09-11T10:00:00Z"}},
	}
	for _, test := range tests {
		var err error
		if test.schedule {
			_, err = parseScheduleMutation(test.args)
		} else {
			_, err = parseBacklogMutation(test.args)
		}
		if err == nil {
			t.Errorf("parse(%q) succeeded", test.args)
		}
	}
}

func TestBacklogMutationQueriesRevisionThenSubmits(t *testing.T) {
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Revision: 7}
	fake := &fakeAdminMutationService{
		queryResponse: backlogadmin.Response{
			Version: backlogadmin.Version, Kind: backlogadmin.QueryTask,
			Task: &backlogadmin.TaskDetail{Attempt: &attempt},
		},
		mutateResponse: backlogadmin.MutationResponse{
			Version: backlogadmin.Version,
			Command: backlogadmin.Command{
				ID: "command-fixed", Kind: domain.AdminCommandPause, TargetType: domain.AdminTargetAttempt,
				TargetID: "attempt-1", ExpectedRevision: 7, State: domain.AdminCommandPending,
			},
			Event: backlogadmin.Event{ID: "event-1"},
		},
	}
	var out bytes.Buffer
	cli := backlogAdminCLI{
		service: fake, mutator: fake,
		principal: backlogadmin.Principal{ID: "local:1000", Roles: []string{"local-admin"}},
		stdout:    &out, newCommandID: func() (string, error) { return "command-fixed", nil },
	}
	if err := cli.runBacklog(context.Background(), []string{
		"pause", "run-1/task-1", "--now", "--reason", "maintenance", "--json",
	}); err != nil {
		t.Fatal(err)
	}
	if len(fake.queries) != 1 || fake.queries[0].Kind != backlogadmin.QueryTask ||
		fake.queries[0].WorkflowRunID != "run-1" || fake.queries[0].TaskID != "task-1" {
		t.Fatalf("queries = %+v", fake.queries)
	}
	if len(fake.mutations) != 1 {
		t.Fatalf("mutations = %d", len(fake.mutations))
	}
	mutation := fake.mutations[0]
	if mutation.ID != "command-fixed" || mutation.Kind != domain.AdminCommandPause ||
		mutation.ExpectedRevision != 7 || mutation.Reason != "maintenance" ||
		mutation.Principal.ID != "local:1000" || string(mutation.Payload) != `{"now":true}` {
		t.Fatalf("mutation = %+v", mutation)
	}
	var response backlogadmin.MutationResponse
	if err := json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatalf("invalid JSON %q: %v", out.String(), err)
	}
	if response.Command.ID != "command-fixed" {
		t.Fatalf("response = %+v", response)
	}
}

func TestScheduleMutationUsesCurrentRevisionAndReplayID(t *testing.T) {
	fake := &fakeAdminMutationService{
		queryResponse: backlogadmin.Response{
			Version: backlogadmin.Version, Kind: backlogadmin.QuerySchedules,
			Schedules: []backlogadmin.Schedule{{Schedule: domain.Schedule{ID: "nightly", Revision: 4}}},
		},
		mutateResponse: backlogadmin.MutationResponse{
			Version: backlogadmin.Version,
			Command: backlogadmin.Command{
				ID: "replay-1", Kind: domain.AdminCommandDisable, TargetType: domain.AdminTargetSchedule,
				TargetID: "nightly", ExpectedRevision: 4, State: domain.AdminCommandPending,
			},
			Event: backlogadmin.Event{ID: "event-1"},
		},
	}
	var out bytes.Buffer
	cli := backlogAdminCLI{
		service: fake, mutator: fake,
		principal: backlogadmin.Principal{ID: "operator", Roles: []string{"local-admin"}}, stdout: &out,
		newCommandID: func() (string, error) { return "", errors.New("must not generate") },
	}
	if err := cli.runSchedules(context.Background(), []string{
		"disable", "nightly", "--reason", "maintenance", "--command-id", "replay-1",
	}); err != nil {
		t.Fatal(err)
	}
	if len(fake.mutations) != 1 || fake.mutations[0].ScheduleID != "nightly" ||
		fake.mutations[0].ExpectedRevision != 4 || fake.mutations[0].ID != "replay-1" {
		t.Fatalf("mutations = %+v", fake.mutations)
	}
	for _, want := range []string{"command: replay-1", "state: pending", "event: event-1"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output %q does not contain %q", out.String(), want)
		}
	}
}

func TestMutationRendersDurableStaleDecision(t *testing.T) {
	attempt := domain.Attempt{ID: "attempt-1", Revision: 5}
	fake := &fakeAdminMutationService{
		queryResponse: backlogadmin.Response{Kind: backlogadmin.QueryTask, Task: &backlogadmin.TaskDetail{Attempt: &attempt}},
		mutateResponse: backlogadmin.MutationResponse{
			Command: backlogadmin.Command{
				ID: "stale-1", Kind: domain.AdminCommandRetry, TargetType: domain.AdminTargetAttempt,
				TargetID: "attempt-1", State: domain.AdminCommandRejected, Failure: "stale target revision",
			},
			Event:         backlogadmin.Event{ID: "event-stale"},
			CurrentTarget: &domain.AdminTargetSnapshot{Type: domain.AdminTargetAttempt, ID: "attempt-1", Revision: 6},
		},
	}
	var out bytes.Buffer
	cli := backlogAdminCLI{
		service: fake, mutator: fake, principal: backlogadmin.Principal{ID: "operator"},
		stdout: &out, newCommandID: func() (string, error) { return "stale-1", nil },
	}
	if err := cli.runBacklogMutation(context.Background(), []string{"retry", "run/task", "--reason", "try again"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"state: rejected", "failure: stale target revision", "current revision: 6"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output %q does not contain %q", out.String(), want)
		}
	}
}

func TestMutationStopsBeforeSubmitOnLookupOrIDFailure(t *testing.T) {
	t.Run("no attempt", func(t *testing.T) {
		fake := &fakeAdminMutationService{queryResponse: backlogadmin.Response{
			Kind: backlogadmin.QueryTask, Task: &backlogadmin.TaskDetail{},
		}}
		cli := backlogAdminCLI{service: fake, mutator: fake, principal: backlogadmin.Principal{ID: "operator"}}
		err := cli.runBacklogMutation(context.Background(), []string{"cancel", "run/task", "--reason", "stop"})
		if err == nil || !strings.Contains(err.Error(), "no attempt") || len(fake.mutations) != 0 {
			t.Fatalf("error = %v, mutations = %d", err, len(fake.mutations))
		}
	})
	t.Run("schedule missing", func(t *testing.T) {
		fake := &fakeAdminMutationService{queryResponse: backlogadmin.Response{Kind: backlogadmin.QuerySchedules}}
		cli := backlogAdminCLI{service: fake, mutator: fake, principal: backlogadmin.Principal{ID: "operator"}}
		err := cli.runScheduleMutation(context.Background(), []string{"enable", "missing", "--reason", "turn on"})
		if !errors.Is(err, backlogadmin.ErrNotFound) || len(fake.mutations) != 0 {
			t.Fatalf("error = %v, mutations = %d", err, len(fake.mutations))
		}
	})
	t.Run("id generation", func(t *testing.T) {
		attempt := domain.Attempt{Revision: 1}
		fake := &fakeAdminMutationService{queryResponse: backlogadmin.Response{
			Kind: backlogadmin.QueryTask, Task: &backlogadmin.TaskDetail{Attempt: &attempt},
		}}
		cli := backlogAdminCLI{
			service: fake, mutator: fake, principal: backlogadmin.Principal{ID: "operator"},
			newCommandID: func() (string, error) { return "", errors.New("entropy unavailable") },
		}
		err := cli.runBacklogMutation(context.Background(), []string{"skip", "run/task", "--reason", "not needed"})
		if err == nil || !strings.Contains(err.Error(), "entropy unavailable") || len(fake.mutations) != 0 {
			t.Fatalf("error = %v, mutations = %d", err, len(fake.mutations))
		}
	})
}

func TestNewAdminCommandID(t *testing.T) {
	first, err := newAdminCommandID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := newAdminCommandID()
	if err != nil {
		t.Fatal(err)
	}
	if first == second || !strings.HasPrefix(first, "admin-") || len(first) != len("admin-")+24 {
		t.Fatalf("ids = %q, %q", first, second)
	}
}

func TestBacklogMutationSubmitsAndReplaysWithoutExecuting(t *testing.T) {
	path := t.TempDir() + "/state.db"
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	records := sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-1", Version: 2, Name: "workflow", Class: domain.TaskClassRequired,
			TaskIDs: []string{"task-1"}, CreatedAt: now,
		}},
		WorkflowRuns: []domain.WorkflowRun{{
			ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressFailed,
			Revision: 3, CreatedAt: now, UpdatedAt: now,
		}},
		Tasks: []domain.Task{{
			ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassRequired,
		}},
		Attempts: []domain.Attempt{{
			ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1,
			Progress: domain.ProgressFailed, Control: domain.ControlStopped, Revision: 7, UpdatedAt: now,
		}},
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	service, err := backlogadmin.New(store, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now })
	principal := backlogadmin.Principal{ID: "local:1000", Roles: []string{"local-admin"}}
	var first bytes.Buffer
	cli := backlogAdminCLI{service: service, mutator: service, principal: principal, stdout: &first}
	args := []string{"retry", "run-1/task-1", "--reason", "retry after fix", "--command-id", "replay-1", "--json"}
	if err := cli.runBacklog(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, err = backlogadmin.New(store, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now.Add(time.Hour) })
	var replay bytes.Buffer
	cli = backlogAdminCLI{service: service, mutator: service, principal: principal, stdout: &replay}
	if err := cli.runBacklog(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	var firstResponse, replayResponse backlogadmin.MutationResponse
	if err := json.Unmarshal(first.Bytes(), &firstResponse); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(replay.Bytes(), &replayResponse); err != nil {
		t.Fatal(err)
	}
	if firstResponse.Command.ID != "replay-1" || firstResponse.Command.State != domain.AdminCommandPending ||
		!firstResponse.Command.CreatedAt.Equal(now) ||
		replayResponse.Command.ID != firstResponse.Command.ID ||
		replayResponse.Command.State != domain.AdminCommandPending ||
		!replayResponse.Command.CreatedAt.Equal(firstResponse.Command.CreatedAt) {
		t.Fatalf("first = %+v, replay = %+v", firstResponse.Command, replayResponse.Command)
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.AdminCommands) != 1 || len(loaded.AuditEvents) != 1 || len(loaded.Attempts) != 1 {
		t.Fatalf("commands = %d, audit events = %d, attempts = %d", len(loaded.AdminCommands), len(loaded.AuditEvents), len(loaded.Attempts))
	}
}
