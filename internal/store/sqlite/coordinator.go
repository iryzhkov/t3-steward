package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const currentSchemaVersion = 11

const coordinatorMigrationV6 = `
ALTER TABLE coordinator_schedules ADD COLUMN current_version INTEGER NOT NULL DEFAULT 0;
UPDATE coordinator_schedules
SET current_version = COALESCE(CAST(json_extract(record, '$.version') AS INTEGER), 0);
CREATE TABLE coordinator_schedule_templates (
	schedule_id TEXT NOT NULL,
	version INTEGER NOT NULL CHECK(version > 0),
	workflow_id TEXT NOT NULL,
	record TEXT NOT NULL,
	PRIMARY KEY(schedule_id, version)
);
CREATE INDEX coordinator_schedule_templates_workflow
	ON coordinator_schedule_templates(workflow_id);
INSERT INTO coordinator_schedule_templates(schedule_id, version, workflow_id, record)
SELECT id, current_version, json_extract(record, '$.workflowId'),
	json_object(
		'scheduleId', id,
		'version', current_version,
		'workflowId', json_extract(record, '$.workflowId'),
		'expression', json_extract(record, '$.expression'),
		'timezone', json_extract(record, '$.timezone'),
		'overlap', json_extract(record, '$.overlap'),
		'misfire', json_extract(record, '$.misfire'),
		'afterFailure', json_extract(record, '$.afterFailure'),
		'createdAt', json_extract(record, '$.createdAt')
	)
FROM coordinator_schedules
WHERE current_version > 0;
ALTER TABLE coordinator_triggers ADD COLUMN schedule_version INTEGER NOT NULL DEFAULT 0;
UPDATE coordinator_triggers
SET schedule_version = COALESCE((
	SELECT current_version
	FROM coordinator_schedules
	WHERE coordinator_schedules.id = coordinator_triggers.schedule_id
), 0);
UPDATE coordinator_triggers
SET record = json_set(record, '$.scheduleVersion', schedule_version)
WHERE schedule_version > 0;
CREATE INDEX coordinator_triggers_schedule_version
	ON coordinator_triggers(schedule_id, schedule_version);
`

const coordinatorMigrationV5 = `
ALTER TABLE coordinator_attempts ADD COLUMN revision INTEGER NOT NULL DEFAULT 0;
`

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
	Workflows         []domain.Workflow
	WorkflowRuns      []domain.WorkflowRun
	Tasks             []domain.Task
	Attempts          []domain.Attempt
	Assignments       []domain.Assignment
	Schedules         []domain.Schedule
	ScheduleTemplates []domain.ScheduleTemplate
	Triggers          []domain.Trigger
	QuotaPools        []domain.QuotaPool
	Artifacts         []domain.Artifact
	AdminCommands     []domain.AdminCommand
	AuditEvents       []domain.AuditEvent
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
			`INSERT INTO coordinator_attempts(id, workflow_run_id, task_id, number, revision, record)
			 VALUES (?, ?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET workflow_run_id = excluded.workflow_run_id,
			 task_id = excluded.task_id, number = excluded.number,
			 revision = excluded.revision, record = excluded.record`,
			[]any{record.ID, record.WorkflowRunID, record.TaskID, record.Number, record.Revision}, record); err != nil {
			return err
		}
	}
	for _, record := range records.Assignments {
		if err := upsertJSON(ctx, tx, "assignment", record.ID,
			`INSERT INTO coordinator_assignments(
			 id, attempt_id, dispatch_token, dispatch_revision, dispatch_state,
			 worker_id, worker_epoch, assignment_epoch, assignment_state, lease_expires_at, record
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET attempt_id = excluded.attempt_id,
			 dispatch_token = excluded.dispatch_token,
			 dispatch_revision = excluded.dispatch_revision,
			 dispatch_state = excluded.dispatch_state,
			 worker_id = excluded.worker_id, worker_epoch = excluded.worker_epoch,
			 assignment_epoch = excluded.assignment_epoch,
			 assignment_state = excluded.assignment_state,
			 lease_expires_at = excluded.lease_expires_at, record = excluded.record`,
			[]any{record.ID, record.AttemptID, record.DispatchToken,
				record.DispatchRevision, record.DispatchState, record.WorkerID,
				record.WorkerEpoch, record.Epoch, record.State,
				record.LeaseExpiresAt.UTC().Format(time.RFC3339Nano)}, record); err != nil {
			return err
		}
	}
	for _, record := range records.ScheduleTemplates {
		if err := domain.ValidateScheduleTemplate(record); err != nil {
			return fmt.Errorf("save schedule template %q/%d: %w", record.ScheduleID, record.Version, err)
		}
		if err := insertImmutableJSON(ctx, tx, "schedule template",
			fmt.Sprintf("%s/%d", record.ScheduleID, record.Version),
			`INSERT INTO coordinator_schedule_templates(schedule_id, version, workflow_id, record)
			 VALUES (?, ?, ?, ?)
			 ON CONFLICT(schedule_id, version) DO NOTHING`,
			[]any{record.ScheduleID, record.Version, record.WorkflowID},
			`SELECT record FROM coordinator_schedule_templates WHERE schedule_id = ? AND version = ?`,
			[]any{record.ScheduleID, record.Version}, record); err != nil {
			return err
		}
	}
	for _, record := range records.Schedules {
		if err := upsertJSON(ctx, tx, "schedule", record.ID,
			`INSERT INTO coordinator_schedules(id, active_run_id, revision, current_version, record)
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET active_run_id = excluded.active_run_id,
			 revision = excluded.revision, current_version = excluded.current_version,
			 record = excluded.record`,
			[]any{record.ID, record.ActiveRunID, record.Revision, record.Version}, record); err != nil {
			return err
		}
	}
	for _, record := range records.Triggers {
		if record.ScheduleVersion < 1 {
			return fmt.Errorf("save trigger %q: schedule version must be positive", record.ID)
		}
		if err := insertImmutableJSON(ctx, tx, "trigger", record.ID,
			`INSERT INTO coordinator_triggers(id, schedule_id, schedule_version, occurrence_key, record)
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO NOTHING`,
			[]any{record.ID, record.ScheduleID, record.ScheduleVersion, record.OccurrenceKey},
			`SELECT record FROM coordinator_triggers WHERE id = ?`,
			[]any{record.ID}, record); err != nil {
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
		if err := insertImmutableJSON(ctx, tx, "artifact", record.ID,
			`INSERT INTO coordinator_artifacts(id, workflow_run_id, task_id, attempt_id, sha256, record)
			 VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
			[]any{record.ID, record.WorkflowRunID, record.TaskID, record.AttemptID, record.SHA256},
			`SELECT record FROM coordinator_artifacts WHERE id = ?`,
			[]any{record.ID}, record); err != nil {
			return err
		}
	}
	for _, record := range records.AdminCommands {
		if err := insertImmutableJSON(ctx, tx, "admin command", record.ID,
			`INSERT INTO coordinator_admin_commands(id, target_type, target_id, state, record)
			 VALUES (?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
			[]any{record.ID, record.TargetType, record.TargetID, record.State},
			`SELECT record FROM coordinator_admin_commands WHERE id = ?`,
			[]any{record.ID}, record); err != nil {
			return err
		}
	}
	for _, event := range records.AuditEvents {
		if _, err := insertAuditEventTx(ctx, tx, event); err != nil {
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

func insertImmutableJSON[T any](
	ctx context.Context,
	tx *sql.Tx,
	kind, id, insertQuery string,
	insertArgs []any,
	selectQuery string,
	selectArgs []any,
	record T,
) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal %s %q: %w", kind, id, err)
	}
	result, err := tx.ExecContext(ctx, insertQuery, append(insertArgs, string(raw))...)
	if err != nil {
		return fmt.Errorf("save %s %q: %w", kind, id, err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect %s %q insert: %w", kind, id, err)
	}
	if inserted != 0 {
		return nil
	}

	var existingRaw []byte
	if err := tx.QueryRowContext(ctx, selectQuery, selectArgs...).Scan(&existingRaw); err != nil {
		return fmt.Errorf("load existing %s %q: %w", kind, id, err)
	}
	var existing T
	if err := json.Unmarshal(existingRaw, &existing); err != nil {
		return fmt.Errorf("decode existing %s %q: %w", kind, id, err)
	}
	canonicalExisting, err := json.Marshal(existing)
	if err != nil {
		return fmt.Errorf("canonicalize existing %s %q: %w", kind, id, err)
	}
	if string(canonicalExisting) != string(raw) {
		return fmt.Errorf("%s %q is immutable and already has different content", kind, id)
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
	if records.ScheduleTemplates, err = loadScheduleTemplates(ctx, tx); err != nil {
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
	if records.AuditEvents, err = loadAuditEventsTx(ctx, tx, ""); err != nil {
		return CoordinatorRecords{}, err
	}
	if err := tx.Commit(); err != nil {
		return CoordinatorRecords{}, fmt.Errorf("commit coordinator load: %w", err)
	}
	return records, nil
}

func loadScheduleTemplates(ctx context.Context, tx *sql.Tx) ([]domain.ScheduleTemplate, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT record FROM coordinator_schedule_templates ORDER BY schedule_id, version`)
	if err != nil {
		return nil, fmt.Errorf("load coordinator_schedule_templates: %w", err)
	}
	defer rows.Close()

	var records []domain.ScheduleTemplate
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan coordinator_schedule_templates: %w", err)
		}
		var record domain.ScheduleTemplate
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, fmt.Errorf("decode coordinator_schedule_templates: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate coordinator_schedule_templates: %w", err)
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
