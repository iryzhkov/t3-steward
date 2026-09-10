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
		ID:            "trigger-2",
		ScheduleID:    "schedule-1",
		OccurrenceKey: records.Triggers[0].OccurrenceKey,
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
		"coordinator_triggers",
		"coordinator_quota_pools",
		"coordinator_artifacts",
		"coordinator_admin_commands",
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
}

func coordinatorFixture() CoordinatorRecords {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	later := now.Add(time.Hour)
	route := domain.ProviderRoute{
		WorkerID:           "normandy",
		ProviderInstanceID: "codex",
		Model:              "gpt-5.6-sol",
		Options:            map[string]string{"effort": "high"},
		QuotaPoolID:        "openai-primary",
	}
	return CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-1", Version: 2, Name: "build", Class: domain.TaskClassRequired,
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
			Importance: 5, Difficulty: 5, MaxTurns: 6, NotBefore: &now, Deadline: &later,
		}},
		Attempts: []domain.Attempt{{
			ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1,
			Progress: domain.ProgressActive, Control: domain.ControlPaused,
			AssignmentID: "assignment-1", ThreadID: "thread-1",
			CheckpointArtifactID: "checkpoint-1", StartedAt: &now, UpdatedAt: later,
		}},
		Assignments: []domain.Assignment{{
			ID: "assignment-1", AttemptID: "attempt-1", WorkerID: "normandy", Route: route,
			State: domain.AssignmentClaimed, Epoch: 7, LeaseToken: "lease-1",
			LeaseExpiresAt: later, DispatchToken: "dispatch-1", ThreadID: "thread-1",
			CreatedAt: now, UpdatedAt: later,
		}},
		Schedules: []domain.Schedule{{
			ID: "schedule-1", Name: "nightly", Version: 4, WorkflowID: "workflow-1",
			Expression: "0 2 * * *", Timezone: "America/Los_Angeles",
			Overlap: domain.ScheduleOverlapForbid, Misfire: domain.ScheduleMisfireSkip,
			AfterFailure: domain.ScheduleFailureHold, Enabled: true, ActiveRunID: "run-1",
			Revision: 2, CreatedAt: now, UpdatedAt: later,
		}},
		Triggers: []domain.Trigger{{
			ID: "trigger-1", ScheduleID: "schedule-1", NominalAt: now,
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
