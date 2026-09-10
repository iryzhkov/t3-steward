package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const currentSchemaVersion = 3

const coordinatorMigrationV2 = `
CREATE TABLE coordinator_workflows (
	id TEXT PRIMARY KEY,
	record TEXT NOT NULL
);
CREATE TABLE coordinator_workflow_runs (
	id TEXT PRIMARY KEY,
	workflow_id TEXT NOT NULL,
	schedule_id TEXT NOT NULL,
	progress TEXT NOT NULL,
	revision INTEGER NOT NULL,
	record TEXT NOT NULL
);
CREATE INDEX coordinator_workflow_runs_workflow ON coordinator_workflow_runs(workflow_id);
CREATE INDEX coordinator_workflow_runs_schedule ON coordinator_workflow_runs(schedule_id);
CREATE TABLE coordinator_tasks (
	id TEXT PRIMARY KEY,
	workflow_id TEXT NOT NULL,
	name TEXT NOT NULL,
	record TEXT NOT NULL,
	UNIQUE(workflow_id, name)
);
CREATE INDEX coordinator_tasks_workflow ON coordinator_tasks(workflow_id);
CREATE TABLE coordinator_attempts (
	id TEXT PRIMARY KEY,
	workflow_run_id TEXT NOT NULL,
	task_id TEXT NOT NULL,
	number INTEGER NOT NULL,
	record TEXT NOT NULL,
	UNIQUE(workflow_run_id, task_id, number)
);
CREATE INDEX coordinator_attempts_run ON coordinator_attempts(workflow_run_id);
CREATE TABLE coordinator_assignments (
	id TEXT PRIMARY KEY,
	attempt_id TEXT NOT NULL UNIQUE,
	dispatch_token TEXT NOT NULL UNIQUE,
	record TEXT NOT NULL
);
CREATE TABLE coordinator_schedules (
	id TEXT PRIMARY KEY,
	active_run_id TEXT NOT NULL,
	revision INTEGER NOT NULL,
	record TEXT NOT NULL
);
CREATE TABLE coordinator_triggers (
	id TEXT PRIMARY KEY,
	schedule_id TEXT NOT NULL,
	occurrence_key TEXT NOT NULL UNIQUE,
	record TEXT NOT NULL
);
CREATE INDEX coordinator_triggers_schedule ON coordinator_triggers(schedule_id);
CREATE TABLE coordinator_quota_pools (
	id TEXT PRIMARY KEY,
	record TEXT NOT NULL
);
CREATE TABLE coordinator_artifacts (
	id TEXT PRIMARY KEY,
	workflow_run_id TEXT NOT NULL,
	task_id TEXT NOT NULL,
	attempt_id TEXT NOT NULL,
	sha256 TEXT NOT NULL,
	record TEXT NOT NULL
);
CREATE INDEX coordinator_artifacts_run ON coordinator_artifacts(workflow_run_id);
CREATE INDEX coordinator_artifacts_task ON coordinator_artifacts(task_id);
CREATE TABLE coordinator_admin_commands (
	id TEXT PRIMARY KEY,
	target_type TEXT NOT NULL,
	target_id TEXT NOT NULL,
	state TEXT NOT NULL,
	record TEXT NOT NULL
);
CREATE INDEX coordinator_admin_commands_target ON coordinator_admin_commands(target_type, target_id);
`

// CoordinatorRecords is a durable coordinator snapshot. SaveCoordinatorRecords
// writes every supplied record in one transaction; omitted records are left intact.
type CoordinatorRecords struct {
	Workflows     []domain.Workflow
	WorkflowRuns  []domain.WorkflowRun
	Tasks         []domain.Task
	Attempts      []domain.Attempt
	Assignments   []domain.Assignment
	Schedules     []domain.Schedule
	Triggers      []domain.Trigger
	QuotaPools    []domain.QuotaPool
	Artifacts     []domain.Artifact
	AdminCommands []domain.AdminCommand
}

// SaveCoordinatorRecords atomically inserts or updates the supplied records.
func (s *Store) SaveCoordinatorRecords(ctx context.Context, records CoordinatorRecords) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin coordinator save: %w", err)
	}
	defer tx.Rollback()

	for _, record := range records.Workflows {
		if err := upsertJSON(ctx, tx, "workflow", record.ID,
			`INSERT INTO coordinator_workflows(id, record) VALUES (?, ?)
			 ON CONFLICT(id) DO UPDATE SET record = excluded.record`,
			[]any{record.ID}, record); err != nil {
			return err
		}
	}
	for _, record := range records.WorkflowRuns {
		if err := upsertJSON(ctx, tx, "workflow run", record.ID,
			`INSERT INTO coordinator_workflow_runs(id, workflow_id, schedule_id, progress, revision, record)
			 VALUES (?, ?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET workflow_id = excluded.workflow_id,
			 schedule_id = excluded.schedule_id, progress = excluded.progress,
			 revision = excluded.revision, record = excluded.record`,
			[]any{record.ID, record.WorkflowID, record.ScheduleID, record.Progress, record.Revision}, record); err != nil {
			return err
		}
	}
	for _, record := range records.Tasks {
		if err := upsertJSON(ctx, tx, "task", record.ID,
			`INSERT INTO coordinator_tasks(id, workflow_id, name, record) VALUES (?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET workflow_id = excluded.workflow_id,
			 name = excluded.name, record = excluded.record`,
			[]any{record.ID, record.WorkflowID, record.Name}, record); err != nil {
			return err
		}
	}
	for _, record := range records.Attempts {
		if err := upsertJSON(ctx, tx, "attempt", record.ID,
			`INSERT INTO coordinator_attempts(id, workflow_run_id, task_id, number, record)
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET workflow_run_id = excluded.workflow_run_id,
			 task_id = excluded.task_id, number = excluded.number, record = excluded.record`,
			[]any{record.ID, record.WorkflowRunID, record.TaskID, record.Number}, record); err != nil {
			return err
		}
	}
	for _, record := range records.Assignments {
		if err := upsertJSON(ctx, tx, "assignment", record.ID,
			`INSERT INTO coordinator_assignments(id, attempt_id, dispatch_token, record)
			 VALUES (?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET attempt_id = excluded.attempt_id,
			 dispatch_token = excluded.dispatch_token, record = excluded.record`,
			[]any{record.ID, record.AttemptID, record.DispatchToken}, record); err != nil {
			return err
		}
	}
	for _, record := range records.Schedules {
		if err := upsertJSON(ctx, tx, "schedule", record.ID,
			`INSERT INTO coordinator_schedules(id, active_run_id, revision, record)
			 VALUES (?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET active_run_id = excluded.active_run_id,
			 revision = excluded.revision, record = excluded.record`,
			[]any{record.ID, record.ActiveRunID, record.Revision}, record); err != nil {
			return err
		}
	}
	for _, record := range records.Triggers {
		if err := upsertJSON(ctx, tx, "trigger", record.ID,
			`INSERT INTO coordinator_triggers(id, schedule_id, occurrence_key, record)
			 VALUES (?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET schedule_id = excluded.schedule_id,
			 occurrence_key = excluded.occurrence_key, record = excluded.record`,
			[]any{record.ID, record.ScheduleID, record.OccurrenceKey}, record); err != nil {
			return err
		}
	}
	for _, record := range records.QuotaPools {
		if err := upsertJSON(ctx, tx, "quota pool", record.ID,
			`INSERT INTO coordinator_quota_pools(id, record) VALUES (?, ?)
			 ON CONFLICT(id) DO UPDATE SET record = excluded.record`,
			[]any{record.ID}, record); err != nil {
			return err
		}
	}
	for _, record := range records.Artifacts {
		if err := upsertJSON(ctx, tx, "artifact", record.ID,
			`INSERT INTO coordinator_artifacts(id, workflow_run_id, task_id, attempt_id, sha256, record)
			 VALUES (?, ?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET workflow_run_id = excluded.workflow_run_id,
			 task_id = excluded.task_id, attempt_id = excluded.attempt_id,
			 sha256 = excluded.sha256, record = excluded.record`,
			[]any{record.ID, record.WorkflowRunID, record.TaskID, record.AttemptID, record.SHA256}, record); err != nil {
			return err
		}
	}
	for _, record := range records.AdminCommands {
		if err := upsertJSON(ctx, tx, "admin command", record.ID,
			`INSERT INTO coordinator_admin_commands(id, target_type, target_id, state, record)
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET target_type = excluded.target_type,
			 target_id = excluded.target_id, state = excluded.state, record = excluded.record`,
			[]any{record.ID, record.TargetType, record.TargetID, record.State}, record); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit coordinator records: %w", err)
	}
	return nil
}

func upsertJSON(ctx context.Context, tx *sql.Tx, kind, id, query string, args []any, record any) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal %s %q: %w", kind, id, err)
	}
	args = append(args, string(raw))
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("save %s %q: %w", kind, id, err)
	}
	return nil
}

// LoadCoordinatorRecords returns a transactionally consistent coordinator snapshot.
func (s *Store) LoadCoordinatorRecords(ctx context.Context) (CoordinatorRecords, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return CoordinatorRecords{}, fmt.Errorf("begin coordinator load: %w", err)
	}
	defer tx.Rollback()

	var records CoordinatorRecords
	if records.Workflows, err = loadJSON[domain.Workflow](ctx, tx, "coordinator_workflows"); err != nil {
		return CoordinatorRecords{}, err
	}
	if records.WorkflowRuns, err = loadJSON[domain.WorkflowRun](ctx, tx, "coordinator_workflow_runs"); err != nil {
		return CoordinatorRecords{}, err
	}
	if records.Tasks, err = loadJSON[domain.Task](ctx, tx, "coordinator_tasks"); err != nil {
		return CoordinatorRecords{}, err
	}
	if records.Attempts, err = loadJSON[domain.Attempt](ctx, tx, "coordinator_attempts"); err != nil {
		return CoordinatorRecords{}, err
	}
	if records.Assignments, err = loadJSON[domain.Assignment](ctx, tx, "coordinator_assignments"); err != nil {
		return CoordinatorRecords{}, err
	}
	if records.Schedules, err = loadJSON[domain.Schedule](ctx, tx, "coordinator_schedules"); err != nil {
		return CoordinatorRecords{}, err
	}
	if records.Triggers, err = loadJSON[domain.Trigger](ctx, tx, "coordinator_triggers"); err != nil {
		return CoordinatorRecords{}, err
	}
	if records.QuotaPools, err = loadJSON[domain.QuotaPool](ctx, tx, "coordinator_quota_pools"); err != nil {
		return CoordinatorRecords{}, err
	}
	if records.Artifacts, err = loadJSON[domain.Artifact](ctx, tx, "coordinator_artifacts"); err != nil {
		return CoordinatorRecords{}, err
	}
	if records.AdminCommands, err = loadJSON[domain.AdminCommand](ctx, tx, "coordinator_admin_commands"); err != nil {
		return CoordinatorRecords{}, err
	}
	if err := tx.Commit(); err != nil {
		return CoordinatorRecords{}, fmt.Errorf("commit coordinator load: %w", err)
	}
	return records, nil
}

func loadJSON[T any](ctx context.Context, tx *sql.Tx, table string) ([]T, error) {
	rows, err := tx.QueryContext(ctx, "SELECT record FROM "+table+" ORDER BY id")
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", table, err)
	}
	defer rows.Close()

	var records []T
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan %s: %w", table, err)
		}
		var record T
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, fmt.Errorf("decode %s: %w", table, err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s: %w", table, err)
	}
	return records, nil
}
