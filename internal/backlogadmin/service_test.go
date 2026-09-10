package backlogadmin

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var adminTestNow = time.Date(2026, 9, 10, 20, 0, 0, 0, time.UTC)

type allowAuthorizer struct {
	actions []Action
	err     error
}

func (a *allowAuthorizer) Authorize(_ context.Context, _ Principal, action Action) error {
	a.actions = append(a.actions, action)
	return a.err
}

func TestAdminQueriesTemporaryCoordinatorState(t *testing.T) {
	store := openAdminTestStore(t)
	seedAdminTestStore(t, store)
	authorizer := &allowAuthorizer{}
	service, err := New(store, authorizer)
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return adminTestNow })

	principal := Principal{ID: "operator-1", Roles: []string{"backlog-reader"}}
	queries := []Query{
		{Version: Version, Kind: QueryStatus, Principal: principal},
		{Version: Version, Kind: QueryWorkflows, Principal: principal, Filter: Filter{Project: "t3-steward", Progress: []domain.ProgressState{domain.ProgressActive}}},
		{Version: Version, Kind: QueryWorkflow, Principal: principal, WorkflowRunID: "run-1"},
		{Version: Version, Kind: QueryGraph, Principal: principal, WorkflowRunID: "run-1"},
		{Version: Version, Kind: QueryTask, Principal: principal, WorkflowRunID: "run-1", TaskID: "implement"},
		{Version: Version, Kind: QueryExplanation, Principal: principal, WorkflowRunID: "run-1", TaskID: "task-implement"},
		{Version: Version, Kind: QueryEvents, Principal: principal, WorkflowRunID: "run-1"},
		{Version: Version, Kind: QueryArtifacts, Principal: principal, WorkflowRunID: "run-1", TaskID: "implement"},
		{Version: Version, Kind: QueryArtifact, Principal: principal, ArtifactID: "artifact-checkpoint"},
		{Version: Version, Kind: QuerySchedules, Principal: principal},
		{Version: Version, Kind: QueryWorkers, Principal: principal},
		{Version: Version, Kind: QueryQuota, Principal: principal},
		{Version: Version, Kind: QueryReservations, Principal: principal},
		{Version: Version, Kind: QueryLocks, Principal: principal},
	}
	responses := make(map[QueryKind]Response)
	for _, query := range queries {
		response, err := service.Query(context.Background(), query)
		if err != nil {
			t.Fatalf("query %s: %v", query.Kind, err)
		}
		if response.Version != Version || response.Kind != query.Kind || !response.GeneratedAt.Equal(adminTestNow) {
			t.Fatalf("query %s returned unstable envelope: %#v", query.Kind, response)
		}
		responses[query.Kind] = response
	}
	if len(authorizer.actions) != len(queries) {
		t.Fatalf("authorized %d actions, want %d", len(authorizer.actions), len(queries))
	}
	if authorizer.actions[1].Filter.Project != "t3-steward" {
		t.Fatalf("authorization action omitted query scope: %#v", authorizer.actions[1])
	}

	status := responses[QueryStatus].Status
	if status == nil || status.WorkflowRuns[domain.ProgressActive] != 1 || status.Tasks[domain.ProgressSucceeded] != 1 ||
		status.Tasks[domain.ProgressActive] != 1 || status.Workers["ready"] != 1 ||
		status.QuotaPools[domain.AdmissionDraining] != 1 || status.Reservations != 1 || status.Locks != 1 {
		t.Fatalf("unexpected status: %#v", status)
	}
	if got := responses[QueryWorkflows].Workflows; len(got) != 1 || got[0].Progress.Succeeded != 1 || got[0].Progress.Active != 1 {
		t.Fatalf("unexpected workflow list: %#v", got)
	}
	workflow := responses[QueryWorkflow].Workflow
	if workflow == nil || len(workflow.Tasks) != 2 || len(workflow.Artifacts) != 1 ||
		len(workflow.Reservations) != 1 || len(workflow.ResourceLocks) != 1 {
		t.Fatalf("unexpected workflow detail: %#v", workflow)
	}
	graph := responses[QueryGraph].Graph
	if graph == nil || len(graph.Nodes) != 2 || !reflect.DeepEqual(graph.Edges, []GraphEdge{{FromTaskID: "task-inspect", ToTaskID: "task-implement"}}) {
		t.Fatalf("unexpected graph: %#v", graph)
	}
	task := responses[QueryTask].Task
	if task == nil || task.Task.ID != "task-implement" || task.Attempt == nil || task.Assignment == nil ||
		task.ThreadURL != "https://normandy.example.test/thread/thread%2F1" || len(task.Artifacts) != 1 {
		t.Fatalf("unexpected task detail: %#v", task)
	}
	explanation := responses[QueryExplanation].Explanation
	if explanation == nil || explanation.Eligible || len(explanation.Blockers) != 2 ||
		explanation.Blockers[0].Code != "control" || explanation.Blockers[1].Code != "quota-admission" {
		t.Fatalf("unexpected explanation: %#v", explanation)
	}
	events := responses[QueryEvents].Events
	eventKinds := make(map[string]bool)
	for _, item := range events {
		eventKinds[item.Kind] = true
	}
	if len(events) != 7 || !eventKinds["workflow-run-created"] || !eventKinds["schedule-trigger-accepted"] ||
		!eventKinds["admin-command-pending"] || !eventKinds["artifact-checkpoint"] {
		t.Fatalf("unexpected events: %#v", events)
	}
	if artifacts := responses[QueryArtifacts].Artifacts; len(artifacts) != 1 || artifacts[0].Download != "/backlog/artifacts/artifact-checkpoint" {
		t.Fatalf("unexpected artifacts: %#v", artifacts)
	}
	if artifact := responses[QueryArtifact].Artifact; artifact == nil || artifact.Metadata.ID != "artifact-checkpoint" {
		t.Fatalf("unexpected artifact: %#v", artifact)
	}
	safeJSON, err := json.Marshal(struct {
		Task     *TaskDetail `json:"task"`
		Artifact *Artifact   `json:"artifact"`
	}{Task: task, Artifact: responses[QueryArtifact].Artifact})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret-lease", "dispatch-1", "sha256/abc123", "leaseToken", "dispatchToken", "storagePath"} {
		if strings.Contains(string(safeJSON), secret) {
			t.Fatalf("admin DTO exposed internal field %q: %s", secret, safeJSON)
		}
	}
	if schedules := responses[QuerySchedules].Schedules; len(schedules) != 1 || len(schedules[0].Triggers) != 1 {
		t.Fatalf("unexpected schedules: %#v", schedules)
	}
	if workers := responses[QueryWorkers].Workers; len(workers) != 1 || workers[0].Health != "ready" || workers[0].Stale {
		t.Fatalf("unexpected workers: %#v", workers)
	}
	if quotas := responses[QueryQuota].Quotas; len(quotas) != 1 || quotas[0].Admission == nil ||
		quotas[0].Admission.Admission != domain.AdmissionDraining {
		t.Fatalf("unexpected quotas: %#v", quotas)
	}
	if reservations := responses[QueryReservations].Reservations; len(reservations) != 1 ||
		reservations[0].AttemptID != "attempt-implement" || reservations[0].HoldsSlot {
		t.Fatalf("unexpected reservations: %#v", reservations)
	}
	if locks := responses[QueryLocks].ResourceLocks; len(locks) != 1 || locks[0].OwnerAttemptID != "attempt-implement" {
		t.Fatalf("unexpected locks: %#v", locks)
	}
}

func TestExplanationBlocksSurplusDuringConstrainedAdmission(t *testing.T) {
	records := sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow-1", TaskIDs: []string{"task-1"}}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1"}},
		Tasks: []domain.Task{{
			ID: "task-1", WorkflowID: "workflow-1", Name: "surplus",
			Class: domain.TaskClassSurplus,
			Routes: []domain.ProviderRoute{{
				WorkerID: "normandy", ProviderInstanceID: "codex",
				Model: "gpt-5.6-sol", QuotaPoolID: "pool-1",
			}},
		}},
		QuotaPools: []domain.QuotaPool{{ID: "pool-1", Admission: domain.AdmissionOpen}},
	}
	workers := []domain.WorkerSnapshot{{
		WorkerID: "normandy", Connected: true, ValidUntil: adminTestNow.Add(time.Minute),
		Inventory: domain.WorkerInventory{
			ID: "normandy", AcceptBacklog: true, Health: domain.WorkerHealthReady,
		},
	}}
	admissions := []domain.QuotaAdmissionRecord{{
		QuotaPoolID: "pool-1", Admission: domain.AdmissionConstrained,
	}}
	explanation, ok := newView(records, workers, admissions, adminTestNow).explanation("run-1", "task-1")
	if !ok || explanation.Eligible || len(explanation.Blockers) != 1 ||
		explanation.Blockers[0].Code != "quota-admission" {
		t.Fatalf("unexpected constrained surplus explanation: %#v", explanation)
	}
}

func TestAdminMutationAuthorizationStaleReplayAndAsyncOutcome(t *testing.T) {
	store := openAdminTestStore(t)
	seedAdminTestStore(t, store)
	authorizer := &allowAuthorizer{}
	service, err := New(store, authorizer)
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return adminTestNow })
	principal := Principal{ID: "operator-1", Roles: []string{"backlog-writer"}}
	request := Mutation{
		Version: Version, Principal: principal, ID: "command-service-1",
		Kind: domain.AdminCommandPause, WorkflowRunID: "run-1", TaskID: "implement",
		ExpectedRevision: 4, Reason: "maintenance window",
		Payload: json.RawMessage(`{"now":false}`),
	}
	submitted, err := service.Mutate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if submitted.Command.State != domain.AdminCommandPending ||
		submitted.Command.TargetType != domain.AdminTargetAttempt ||
		submitted.Command.TargetID != "attempt-implement" ||
		submitted.Event.Kind != "admin-command-submitted" ||
		submitted.Command.RequestedBy != principal.ID || submitted.CurrentTarget != nil {
		t.Fatalf("unexpected mutation response: %#v", submitted)
	}
	replayed, err := service.Mutate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replayed, submitted) {
		t.Fatalf("mutation replay changed: got %#v want %#v", replayed, submitted)
	}

	staleRequest := request
	staleRequest.ID = "command-service-stale"
	staleRequest.ExpectedRevision = 3
	stale, err := service.Mutate(context.Background(), staleRequest)
	if err != nil {
		t.Fatal(err)
	}
	if stale.Command.State != domain.AdminCommandRejected || stale.CurrentTarget == nil ||
		stale.CurrentTarget.Revision != 4 || stale.Event.Kind != "admin-command-rejected" {
		t.Fatalf("unexpected stale response: %#v", stale)
	}

	completed, err := service.CompleteCommand(context.Background(), CommandOutcome{
		Version: Version, Principal: Principal{ID: "coordinator"},
		CommandID: submitted.Command.ID, ExpectedState: domain.AdminCommandPending,
		State: domain.AdminCommandApplied,
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed.Command.State != domain.AdminCommandApplied ||
		completed.Event.Kind != "admin-command-applied" ||
		completed.Event.Sequence <= submitted.Event.Sequence {
		t.Fatalf("unexpected asynchronous outcome: %#v", completed)
	}
	query, err := service.Query(context.Background(), Query{
		Version: Version, Kind: QueryCommands, Principal: principal,
		WorkflowRunID: "run-1", TaskID: "implement",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(query.Commands) != 2 || query.Commands[0].State != domain.AdminCommandApplied ||
		query.Commands[1].State != domain.AdminCommandRejected {
		t.Fatalf("unexpected command query: %#v", query.Commands)
	}
	events, err := service.Query(context.Background(), Query{
		Version: Version, Kind: QueryEvents, Principal: principal, WorkflowRunID: "run-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	durable := 0
	for _, event := range events.Events {
		if event.Sequence > 0 {
			durable++
		}
	}
	if durable != 3 {
		t.Fatalf("durable admin event count = %d, events %#v", durable, events.Events)
	}
	if len(authorizer.actions) != 6 || authorizer.actions[0].CommandKind != domain.AdminCommandPause ||
		authorizer.actions[3].OutcomeState != domain.AdminCommandApplied {
		t.Fatalf("unexpected authorized mutation actions: %#v", authorizer.actions)
	}
}

func TestAdminMutationAuthorizationFailsBeforeStateAccess(t *testing.T) {
	reader := &countingReader{}
	denied := errors.New("permission denied")
	service, err := New(reader, &allowAuthorizer{err: denied})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Mutate(context.Background(), Mutation{
		Version: Version, Principal: Principal{ID: "denied"}, ID: "command-1",
		Kind: domain.AdminCommandPause, WorkflowRunID: "run-1", TaskID: "task-1",
		ExpectedRevision: 1, Reason: "test",
	})
	if !errors.Is(err, denied) {
		t.Fatalf("mutation authorization error = %v", err)
	}
	if reader.reads != 0 {
		t.Fatalf("mutation read state %d times before authorization", reader.reads)
	}
}

func TestAdminAuthorizationFailsClosedBeforeStateRead(t *testing.T) {
	store := &countingReader{}
	denied := errors.New("permission denied")
	service, err := New(store, &allowAuthorizer{err: denied})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Query(context.Background(), Query{
		Version: Version, Kind: QueryStatus, Principal: Principal{ID: "anonymous"},
	})
	if !errors.Is(err, denied) {
		t.Fatalf("got %v, want authorization error", err)
	}
	if store.reads != 0 {
		t.Fatalf("state was read %d times before authorization", store.reads)
	}
	if _, err := New(store, nil); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("nil authorizer error = %v", err)
	}
}

func TestAdminRejectsVersionsTargetsAndMissingRecords(t *testing.T) {
	service, err := New(&countingReader{}, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Query(context.Background(), Query{Version: "v2", Kind: QueryStatus}); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("version error = %v", err)
	}
	if _, err := service.Query(context.Background(), Query{Version: Version, Kind: QueryTask}); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("target error = %v", err)
	}
	if _, err := service.Query(context.Background(), Query{Version: Version, Kind: QueryWorkflow, WorkflowRunID: "missing"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("not found error = %v", err)
	}
}

func TestAdminStatusJSONGolden(t *testing.T) {
	response := Response{
		Version: Version, Kind: QueryStatus, GeneratedAt: adminTestNow,
		Status: &Status{
			WorkflowRuns: map[domain.ProgressState]int{domain.ProgressActive: 2, domain.ProgressSucceeded: 4},
			Tasks:        map[domain.ProgressState]int{domain.ProgressBlocked: 1},
			Workers:      map[string]int{"offline": 1, "ready": 2},
			QuotaPools:   map[domain.AdmissionState]int{domain.AdmissionClosed: 1, domain.AdmissionOpen: 2},
			Reservations: 3, Locks: 1,
		},
	}
	got, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join("testdata", "status.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got)+"\n" != string(want) {
		t.Fatalf("admin JSON changed\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestAdminMutationJSONGolden(t *testing.T) {
	response := MutationResponse{
		Version: Version,
		Command: Command{
			ID: "command-1", Kind: domain.AdminCommandPause,
			TargetType: domain.AdminTargetAttempt, TargetID: "attempt-1",
			ExpectedRevision: 7, Reason: "operator request", RequestedBy: "operator-1",
			Payload: json.RawMessage(`{"now":false}`), State: domain.AdminCommandPending,
			CreatedAt: adminTestNow,
		},
		Event: Event{
			ID: "admin-command:command-1:submission", Sequence: 42,
			WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1",
			Kind: "admin-command-submitted", TargetType: domain.AdminTargetAttempt,
			TargetID: "attempt-1", Actor: "operator-1", Reason: "operator request",
			At:     adminTestNow,
			Detail: json.RawMessage(`{"commandKind":"pause","state":"pending","expectedRevision":7}`),
		},
	}
	got, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join("testdata", "mutation.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got)+"\n" != string(want) {
		t.Fatalf("admin mutation JSON changed\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

type countingReader struct {
	reads int
}

func (r *countingReader) LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error) {
	r.reads++
	return sqlite.CoordinatorRecords{}, nil
}

func (r *countingReader) LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error) {
	r.reads++
	return nil, nil
}

func (r *countingReader) LoadQuotaAdmissions(context.Context) ([]domain.QuotaAdmissionRecord, error) {
	r.reads++
	return nil, nil
}

func openAdminTestStore(t *testing.T) *sqlite.Store {
	t.Helper()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	store.SetClock(func() time.Time { return adminTestNow })
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func seedAdminTestStore(t *testing.T, store *sqlite.Store) {
	t.Helper()
	earlier := adminTestNow.Add(-2 * time.Hour)
	started := adminTestNow.Add(-time.Hour)
	leaseExpiry := adminTestNow.Add(time.Hour)
	cost := 42.5
	route := domain.ProviderRoute{
		WorkerID: "normandy", ProviderInstanceID: "codex",
		Model: "gpt-5.6-sol", QuotaPoolID: "openai-primary",
	}
	records := sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-1", Version: 2, Name: "build", Project: "t3-steward",
			Class: domain.TaskClassRequired, TaskIDs: []string{"task-inspect", "task-implement"}, CreatedAt: earlier,
		}},
		WorkflowRuns: []domain.WorkflowRun{{
			ID: "run-1", WorkflowID: "workflow-1", ScheduleID: "schedule-1", TriggerID: "trigger-1",
			Progress: domain.ProgressActive, Revision: 3, CreatedAt: earlier, UpdatedAt: adminTestNow,
		}},
		Tasks: []domain.Task{
			{ID: "task-inspect", WorkflowID: "workflow-1", Name: "inspect", Class: domain.TaskClassRequired},
			{
				ID: "task-implement", WorkflowID: "workflow-1", Name: "implement", Class: domain.TaskClassRequired,
				Needs: []string{"inspect"}, Placement: domain.Placement{Hosts: []string{"normandy"}, Capabilities: []string{"internet"}},
				Routes: []domain.ProviderRoute{route}, ResourceLocks: []string{"project:t3-steward"}, EstimatedCost: &cost,
			},
		},
		Attempts: []domain.Attempt{
			{
				ID: "attempt-inspect", WorkflowRunID: "run-1", TaskID: "task-inspect", Number: 1,
				Progress: domain.ProgressSucceeded, Control: domain.ControlStopped, Revision: 2,
				StartedAt: &earlier, UpdatedAt: started, CompletedAt: &started,
			},
			{
				ID: "attempt-implement", WorkflowRunID: "run-1", TaskID: "task-implement", Number: 1,
				Progress: domain.ProgressActive, Control: domain.ControlPaused, Revision: 4,
				AssignmentID: "assignment-1", ThreadID: "thread/1", CheckpointArtifactID: "artifact-checkpoint",
				StartedAt: &started, UpdatedAt: adminTestNow,
			},
		},
		Assignments: []domain.Assignment{{
			ID: "assignment-1", AttemptID: "attempt-implement", WorkerID: "normandy", WorkerEpoch: "worker-epoch-1",
			Route: route, State: domain.AssignmentClaimed, Epoch: 1, LeaseToken: "secret-lease",
			LeaseExpiresAt: leaseExpiry, DispatchToken: "dispatch-1", ThreadID: "thread/1",
			DispatchState: domain.DispatchConfirmed, DispatchRevision: 1, CreatedAt: started, UpdatedAt: adminTestNow,
		}},
		Schedules: []domain.Schedule{{
			ID: "schedule-1", Name: "nightly", Version: 1, WorkflowID: "workflow-1",
			Expression: "0 3 * * *", Timezone: "UTC", Overlap: domain.ScheduleOverlapForbid,
			Misfire: domain.ScheduleMisfireSkip, AfterFailure: domain.ScheduleFailureHold,
			Enabled: true, ActiveRunID: "run-1", Revision: 1, CreatedAt: earlier, UpdatedAt: adminTestNow,
		}},
		ScheduleTemplates: []domain.ScheduleTemplate{{
			ScheduleID: "schedule-1", Version: 1, WorkflowID: "workflow-1",
			Expression: "0 3 * * *", Timezone: "UTC", Overlap: domain.ScheduleOverlapForbid,
			Misfire: domain.ScheduleMisfireSkip, AfterFailure: domain.ScheduleFailureHold, CreatedAt: earlier,
		}},
		Triggers: []domain.Trigger{{
			ID: "trigger-1", ScheduleID: "schedule-1", ScheduleVersion: 1, NominalAt: earlier,
			OccurrenceKey: "schedule-1/2026-09-10T18:00:00Z", State: domain.TriggerAccepted,
			WorkflowRunID: "run-1", ObservedAt: earlier,
		}},
		QuotaPools: []domain.QuotaPool{{
			ID: "openai-primary", Provider: "openai", ProviderInstanceIDs: []string{"codex"},
			Admission: domain.AdmissionOpen, MaxConcurrent: 1, ActiveAssignments: 1, UpdatedAt: adminTestNow,
		}},
		Artifacts: []domain.Artifact{{
			ID: "artifact-checkpoint", WorkflowRunID: "run-1", TaskID: "task-implement",
			AttemptID: "attempt-implement", Kind: domain.ArtifactCheckpoint, Name: "checkpoint.md",
			MediaType: "text/markdown", Size: 128, SHA256: "abc123", StoragePath: "sha256/abc123",
			Producer: "normandy", CreatedAt: adminTestNow,
		}},
		AdminCommands: []domain.AdminCommand{{
			ID: "command-1", Kind: "pause", TargetType: "task", TargetID: "task-implement",
			ExpectedRevision: 3, Reason: "quota safeguard", RequestedBy: "operator-1",
			State: domain.AdminCommandPending, CreatedAt: adminTestNow,
		}},
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	snapshot := domain.WorkerSnapshot{
		WorkerID: "normandy", WorkerEpoch: "worker-epoch-1", CoordinatorEpoch: 1, Sequence: 1, Connected: true,
		Inventory: domain.WorkerInventory{
			ID: "normandy", AcceptBacklog: true, Health: domain.WorkerHealthReady,
			Capabilities: []string{"internet"}, WebBaseURL: "https://normandy.example.test", ObservedAt: adminTestNow,
		},
		ObservedAt: adminTestNow, ValidUntil: adminTestNow.Add(time.Minute),
	}
	if err := store.SaveWorkerSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	bucket := domain.BucketKey{ProviderInstanceID: "codex", LimitID: "tokens", Window: domain.WindowPrimary}
	transition := domain.QuotaAdmissionTransition{
		ExpectedRevision: 0,
		Record: domain.QuotaAdmissionRecord{
			QuotaPoolID: "openai-primary", Revision: 1, Admission: domain.AdmissionDraining,
			ObservedAt: adminTestNow, BucketEpochs: []domain.QuotaBucketEpoch{{Bucket: bucket, Epoch: "epoch-1"}},
			Reason: "drain", AppliedAt: adminTestNow,
		},
		Directive: &domain.ThrottleDirective{
			ID: "directive-1", QuotaPoolID: "openai-primary", AdmissionRevision: 1,
			Severity: domain.ThrottleDrain, BucketEpochs: []domain.QuotaBucketEpoch{{Bucket: bucket, Epoch: "epoch-1"}},
			Reason: "drain", CreatedAt: adminTestNow,
		},
	}
	if err := store.CommitQuotaAdmissionTransitions(context.Background(), []domain.QuotaAdmissionTransition{transition}); err != nil {
		t.Fatal(err)
	}
}
