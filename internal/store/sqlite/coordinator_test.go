package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCoordinatorRecordsRoundTrip(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	want := coordinatorFixture()
	if err := s.SaveCoordinatorRecords(context.Background(), want); err != nil {
		t.Fatalf("save coordinator records: %v", err)
	}
	got, err := s.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatalf("load coordinator records: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("coordinator round trip mismatch:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestSaveCoordinatorRecordsRollsBackOnConstraintFailure(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	records := coordinatorFixture()
	records.Triggers = append(records.Triggers, domain.Trigger{
		ID:              "trigger-2",
		ScheduleID:      "schedule-1",
		ScheduleVersion: 4,
		OccurrenceKey:   records.Triggers[0].OccurrenceKey,
		State:         domain.TriggerSuppressed,
		ObservedAt:    records.Triggers[0].ObservedAt,
	})
	if err := s.SaveCoordinatorRecords(context.Background(), records); err == nil {
		t.Fatal("save with duplicate occurrence key succeeded")
	}

	got, err := s.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatalf("load after rolled-back save: %v", err)
	}
	if len(got.Workflows) != 0 || len(got.Triggers) != 0 {
		t.Fatalf("partial coordinator save survived rollback: %#v", got)
	}
}

func TestMigrationFromVersionOnePreservesState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range migrations {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create version 1 schema: %v", err)
		}
	}
	for _, stmt := range []string{
		`ALTER TABLE usage_samples ADD COLUMN kind TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE usage_samples ADD COLUMN cumulative_tokens INTEGER NOT NULL DEFAULT 0`,
		`INSERT INTO schema_version(version) VALUES (1)`,
		`INSERT INTO kv(key, value) VALUES ('legacy-key', 'legacy-value')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("populate version 1 database: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("open version 1 database: %v", err)
	}
	defer s.Close()

	var version int
	if err := s.db.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != currentSchemaVersion {
		t.Fatalf("schema version = %d, want %d", version, currentSchemaVersion)
	}
	value, ok, err := s.GetKV(context.Background(), "legacy-key")
	if err != nil || !ok || value != "legacy-value" {
		t.Fatalf("legacy state = %q, %v, %v", value, ok, err)
	}
	for _, table := range []string{
		"coordinator_workflows",
		"coordinator_workflow_runs",
		"coordinator_tasks",
		"coordinator_attempts",
		"coordinator_assignments",
		"coordinator_schedules",
		"coordinator_schedule_templates",
		"coordinator_triggers",
		"coordinator_quota_pools",
		"coordinator_artifacts",
		"coordinator_admin_commands",
		"coordinator_audit_events",
		"coordinator_quota_admissions",
		"coordinator_throttle_directives",
		"coordinator_throttle_attempts",
	} {
		var count int
		if err := s.db.QueryRow(
			`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`,
			table,
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Errorf("migrated table %q count = %d, want 1", table, count)
		}
	}
	for _, column := range []struct {
		table string
		name  string
	}{
		{table: "coordinator_attempts", name: "revision"},
		{table: "coordinator_assignments", name: "dispatch_revision"},
		{table: "coordinator_assignments", name: "dispatch_state"},
		{table: "coordinator_schedules", name: "current_version"},
		{table: "coordinator_triggers", name: "schedule_version"},
	} {
		if has, err := s.hasColumn(column.table, column.name); err != nil {
			t.Fatal(err)
		} else if !has {
			t.Errorf("migrated %s.%s is missing", column.table, column.name)
		}
	}
}

func TestScheduleTemplatesAndTriggersAreImmutable(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	records := coordinatorFixture()
	if err := s.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatalf("save coordinator records: %v", err)
	}
	if err := s.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	nextTemplate := records.ScheduleTemplates[0]
	nextTemplate.Version++
	nextTemplate.WorkflowID = "workflow-replacement"
	nextTemplate.CreatedAt = nextTemplate.CreatedAt.Add(time.Hour)
	if err := s.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{
		ScheduleTemplates: []domain.ScheduleTemplate{nextTemplate},
	}); err != nil {
		t.Fatalf("save next schedule template: %v", err)
	}

	changedTemplate := records.ScheduleTemplates[0]
	changedTemplate.WorkflowID = "workflow-replacement"
	if err := s.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{
		ScheduleTemplates: []domain.ScheduleTemplate{changedTemplate},
	}); err == nil {
		t.Fatal("changed immutable schedule template succeeded")
	}

	changedTrigger := records.Triggers[0]
	changedTrigger.Reason = "rewritten history"
	if err := s.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{
		Triggers: []domain.Trigger{changedTrigger},
	}); err == nil {
		t.Fatal("changed immutable trigger succeeded")
	}

	got, err := s.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatalf("load coordinator records: %v", err)
	}
	wantTemplates := append(records.ScheduleTemplates, nextTemplate)
	if !reflect.DeepEqual(got.ScheduleTemplates, wantTemplates) {
		t.Fatalf("schedule templates changed: %#v", got.ScheduleTemplates)
	}
	if !reflect.DeepEqual(got.Triggers, records.Triggers) {
		t.Fatalf("triggers changed: %#v", got.Triggers)
	}
}

func TestMigrationFromVersionFiveAddsScheduleHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`DROP TABLE coordinator_audit_events`,
		`DROP TABLE coordinator_worker_acknowledgements`,
		`DROP INDEX coordinator_worker_commands_pending`,
		`DROP TABLE coordinator_worker_commands`,
		`DROP INDEX coordinator_assignments_lease_expiry`,
		`ALTER TABLE coordinator_assignments DROP COLUMN lease_expires_at`,
		`DROP INDEX coordinator_assignments_worker_state`,
		`ALTER TABLE coordinator_assignments DROP COLUMN assignment_state`,
		`ALTER TABLE coordinator_assignments DROP COLUMN assignment_epoch`,
		`ALTER TABLE coordinator_assignments DROP COLUMN worker_epoch`,
		`ALTER TABLE coordinator_assignments DROP COLUMN worker_id`,
		`DROP TABLE coordinator_worker_snapshots`,
		`DROP TABLE coordinator_runtime`,
		`DROP INDEX coordinator_assignments_dispatch`,
		`ALTER TABLE coordinator_assignments DROP COLUMN dispatch_state`,
		`ALTER TABLE coordinator_assignments DROP COLUMN dispatch_revision`,
		`DROP INDEX coordinator_triggers_schedule_version`,
		`DROP TABLE coordinator_schedule_templates`,
		`ALTER TABLE coordinator_triggers DROP COLUMN schedule_version`,
		`ALTER TABLE coordinator_schedules DROP COLUMN current_version`,
		`DELETE FROM schema_version WHERE version >= 6`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("restore version 5 schema: %v", err)
		}
	}

	fixture := coordinatorFixture()
	scheduleRaw, err := json.Marshal(fixture.Schedules[0])
	if err != nil {
		t.Fatal(err)
	}
	legacyTrigger := fixture.Triggers[0]
	legacyTrigger.ScheduleVersion = 0
	triggerRaw, err := json.Marshal(legacyTrigger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO coordinator_schedules(id, active_run_id, revision, record) VALUES (?, ?, ?, ?)`,
		fixture.Schedules[0].ID, fixture.Schedules[0].ActiveRunID, fixture.Schedules[0].Revision, scheduleRaw,
	); err != nil {
		t.Fatalf("insert version 5 schedule: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO coordinator_triggers(id, schedule_id, occurrence_key, record) VALUES (?, ?, ?, ?)`,
		legacyTrigger.ID, legacyTrigger.ScheduleID, legacyTrigger.OccurrenceKey, triggerRaw,
	); err != nil {
		t.Fatalf("insert version 5 trigger: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path)
	if err != nil {
		t.Fatalf("migrate version 5 database: %v", err)
	}
	defer s.Close()
	got, err := s.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatalf("load migrated schedule history: %v", err)
	}
	if len(got.ScheduleTemplates) != 1 {
		t.Fatalf("schedule template count = %d, want 1", len(got.ScheduleTemplates))
	}
	template := got.ScheduleTemplates[0]
	if template.ScheduleID != fixture.Schedules[0].ID ||
		template.Version != fixture.Schedules[0].Version ||
		template.WorkflowID != fixture.Schedules[0].WorkflowID ||
		template.Expression != fixture.Schedules[0].Expression ||
		template.CreatedAt != fixture.Schedules[0].CreatedAt {
		t.Fatalf("migrated schedule template = %#v", template)
	}
	if len(got.Triggers) != 1 || got.Triggers[0].ScheduleVersion != fixture.Schedules[0].Version {
		t.Fatalf("migrated triggers = %#v", got.Triggers)
	}
}

func TestMigrationFromVersionNineBackfillsAdminCommandAuditHistory(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	attempt := adminCommandAttempt(7)
	pending := adminCommand("legacy-pending", attempt.Revision)
	applied := adminCommand("legacy-applied", attempt.Revision)
	applied.State = domain.AdminCommandApplied
	appliedAt := adminCommandTestTime.Add(time.Minute)
	applied.AppliedAt = &appliedAt
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Attempts: []domain.Attempt{attempt}, AdminCommands: []domain.AdminCommand{pending, applied},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`DROP TABLE coordinator_audit_events`,
		`DELETE FROM schema_version WHERE version >= 10`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("restore version 9 schema: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(path)
	if err != nil {
		t.Fatalf("migrate version 9 database: %v", err)
	}
	defer store.Close()
	replayed, err := store.SubmitAdminCommand(ctx, pending)
	if err != nil {
		t.Fatalf("replay migrated pending command: %v", err)
	}
	if replayed.Command.ID != pending.ID || replayed.Event.ID != adminSubmissionEventID(pending.ID) {
		t.Fatalf("pending replay = %#v", replayed)
	}
	outcome, err := store.CompleteAdminCommand(ctx, domain.AdminCommandOutcome{
		CommandID: applied.ID, ExpectedState: domain.AdminCommandPending,
		State: domain.AdminCommandApplied, AppliedAt: appliedAt,
	})
	if err != nil {
		t.Fatalf("replay migrated terminal outcome: %v", err)
	}
	if outcome.Command.ID != applied.ID || outcome.Event.ID != adminOutcomeEventID(applied.ID) {
		t.Fatalf("terminal replay = %#v", outcome)
	}
	events, err := store.LoadAuditEvents(ctx, attempt.WorkflowRunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("backfilled audit events = %#v, want two submissions and one outcome", events)
	}
}

func coordinatorFixture() CoordinatorRecords {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	later := now.Add(time.Hour)
	estimatedCost := 37.5
	route := domain.ProviderRoute{
		WorkerID:           "normandy",
		ProviderInstanceID: "codex",
		Model:              "gpt-5.6-sol",
		Options:            map[string]string{"effort": "high"},
		QuotaPoolID:        "openai-primary",
	}
	return CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-1", Version: 2, Name: "build", Project: "t3-steward", Class: domain.TaskClassRequired,
			TaskIDs: []string{"task-1"}, InputArtifactIDs: []string{"artifact-input"}, CreatedAt: now,
		}},
		WorkflowRuns: []domain.WorkflowRun{{
			ID: "run-1", WorkflowID: "workflow-1", ScheduleID: "schedule-1", TriggerID: "trigger-1",
			Progress: domain.ProgressActive, InputArtifactIDs: []string{"artifact-input"},
			Revision: 3, CreatedAt: now, UpdatedAt: later,
		}},
		Tasks: []domain.Task{{
			ID: "task-1", WorkflowID: "workflow-1", Name: "implement", Class: domain.TaskClassRequired,
			Needs: []string{"inspect"}, PromptArtifactID: "prompt-1",
			InputArtifactIDs: []string{"artifact-input"},
			DependencyInputs: map[string][]string{"inspect": {"findings.md"}},
			Outputs:          []domain.ArtifactDeclaration{{Name: "result.md", MediaType: "text/markdown"}},
			Verification:     []string{"go test ./..."},
			Placement: domain.Placement{
				Hosts: []string{"normandy"}, Capabilities: []string{"internet"},
			},
			Routes: []domain.ProviderRoute{route}, ResourceLocks: []string{"project:t3-steward"},
			Importance: 5, Difficulty: 5, EstimatedCost: &estimatedCost, MaxTurns: 6, NotBefore: &now, Deadline: &later,
		}},
		Attempts: []domain.Attempt{{
			ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1,
			Progress: domain.ProgressActive, Control: domain.ControlPaused, Revision: 4,
			AssignmentID: "assignment-1", ThreadID: "thread-1",
			CheckpointArtifactID: "checkpoint-1", StartedAt: &now, UpdatedAt: later,
		}},
		Assignments: []domain.Assignment{{
			ID: "assignment-1", AttemptID: "attempt-1", WorkerID: "normandy", Route: route,
			State: domain.AssignmentClaimed, Epoch: 7, LeaseToken: "lease-1",
			LeaseExpiresAt: later, DispatchToken: "dispatch-1", ThreadID: "thread-1",
			DispatchState: domain.DispatchConfirmed, DispatchRevision: 2, DispatchConfirmedAt: &now,
			CreatedAt: now, UpdatedAt: later,
		}},
		Schedules: []domain.Schedule{{
			ID: "schedule-1", Name: "nightly", Version: 4, WorkflowID: "workflow-1",
			Expression: "0 2 * * *", Timezone: "America/Los_Angeles",
			Overlap: domain.ScheduleOverlapForbid, Misfire: domain.ScheduleMisfireSkip,
			AfterFailure: domain.ScheduleFailureHold, Enabled: true, ActiveRunID: "run-1",
			Revision: 2, CreatedAt: now, UpdatedAt: later,
		}},
		ScheduleTemplates: []domain.ScheduleTemplate{{
			ScheduleID: "schedule-1", Version: 4, WorkflowID: "workflow-1",
			Expression: "0 2 * * *", Timezone: "America/Los_Angeles",
			Overlap: domain.ScheduleOverlapForbid, Misfire: domain.ScheduleMisfireSkip,
			AfterFailure: domain.ScheduleFailureHold, CreatedAt: now,
		}},
		Triggers: []domain.Trigger{{
			ID: "trigger-1", ScheduleID: "schedule-1", ScheduleVersion: 4, NominalAt: now,
			OccurrenceKey: "schedule-1/2026-09-09T12:00:00Z", State: domain.TriggerAccepted,
			WorkflowRunID: "run-1", ObservedAt: now,
		}},
		QuotaPools: []domain.QuotaPool{{
			ID: "openai-primary", Provider: "openai", AccountID: "account-1",
			ProviderInstanceIDs: []string{"codex"},
			Buckets: []domain.BucketKey{{
				ProviderInstanceID: "codex", LimitID: "primary", Window: domain.WindowPrimary,
			}},
			Admission: domain.AdmissionConstrained, MaxConcurrent: 1,
			ActiveAssignments: 1, UpdatedAt: later,
		}},
		Artifacts: []domain.Artifact{{
			ID: "artifact-1", WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1",
			Kind: domain.ArtifactCheckpoint, Name: "checkpoint.md", MediaType: "text/markdown",
			Size: 42, SHA256: "abc123", StoragePath: "artifacts/abc123",
			Producer: "task-1", CreatedAt: later,
		}},
		AdminCommands: []domain.AdminCommand{{
			ID: "command-1", Kind: "pause", TargetType: "task", TargetID: "task-1",
			ExpectedRevision: 3, Reason: "operator request", RequestedBy: "user",
			Payload: json.RawMessage(`{"now":false}`), State: domain.AdminCommandPending,
			CreatedAt: later,
		}},
	}
}
