package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const coordinatorMigrationV24 = `
CREATE TABLE IF NOT EXISTS coordinator_usage_bindings (
	provider TEXT NOT NULL,
	thread_id TEXT NOT NULL,
	workflow_run_id TEXT NOT NULL,
	task_id TEXT NOT NULL,
	attempt_id TEXT NOT NULL,
	assignment_id TEXT NOT NULL,
	assignment_epoch INTEGER NOT NULL,
	activation_id TEXT NOT NULL,
	gate_id TEXT NOT NULL,
	execution_role TEXT NOT NULL,
	bound_at TEXT NOT NULL,
	PRIMARY KEY(provider, thread_id),
	UNIQUE(assignment_id, assignment_epoch)
);
CREATE INDEX IF NOT EXISTS coordinator_usage_bindings_run
	ON coordinator_usage_bindings(workflow_run_id, task_id, attempt_id);
CREATE TRIGGER IF NOT EXISTS immutable_coordinator_usage_binding
	BEFORE UPDATE ON coordinator_usage_bindings
	BEGIN SELECT RAISE(ABORT, 'usage binding is immutable'); END;
`

type usageBinding struct {
	Provider        string
	ThreadID        string
	WorkflowRunID   string
	TaskID          string
	AttemptID       string
	AssignmentID    string
	AssignmentEpoch int64
	ActivationID    string
	GateID          string
	Role            domain.ExecutionRole
	BoundAt         string
}

// bindAssignmentUsageTx records identity at the V2 dispatch boundary. The
// attempt row, not any presentation string, is the authority for run and task.
func bindAssignmentUsageTx(ctx context.Context, tx *sql.Tx, assignment domain.Assignment, boundAt time.Time) error {
	if assignment.Route.ProviderInstanceID == "" || assignment.ThreadID == "" {
		return fmt.Errorf("bind assignment usage %q: provider instance and thread are required", assignment.ID)
	}
	var runID, taskID, raw string
	if err := tx.QueryRowContext(ctx,
		`SELECT workflow_run_id, task_id, record FROM coordinator_attempts WHERE id = ?`,
		assignment.AttemptID,
	).Scan(&runID, &taskID, &raw); err != nil {
		return fmt.Errorf("bind assignment usage %q: load authoritative attempt %q: %w",
			assignment.ID, assignment.AttemptID, err)
	}
	var attempt domain.Attempt
	if err := json.Unmarshal([]byte(raw), &attempt); err != nil {
		return fmt.Errorf("bind assignment usage %q: decode attempt: %w", assignment.ID, err)
	}
	role := domain.ExecutionRoleTask
	if attempt.SupervisionActivationID != "" {
		role = domain.ExecutionRoleSupervision
	}
	want := usageBinding{
		Provider: assignment.Route.ProviderInstanceID, ThreadID: assignment.ThreadID,
		WorkflowRunID: runID, TaskID: taskID, AttemptID: assignment.AttemptID,
		AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
		ActivationID: attempt.SupervisionActivationID, Role: role,
		BoundAt: boundAt.UTC().Format(time.RFC3339Nano),
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO coordinator_usage_bindings(
		provider, thread_id, workflow_run_id, task_id, attempt_id, assignment_id,
		assignment_epoch, activation_id, gate_id, execution_role, bound_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
		want.Provider, want.ThreadID, want.WorkflowRunID, want.TaskID, want.AttemptID,
		want.AssignmentID, want.AssignmentEpoch, want.ActivationID, want.GateID,
		want.Role, want.BoundAt); err != nil {
		return fmt.Errorf("bind assignment usage %q: %w", assignment.ID, err)
	}
	var got usageBinding
	if err := tx.QueryRowContext(ctx, `SELECT provider, thread_id, workflow_run_id,
		task_id, attempt_id, assignment_id, assignment_epoch, activation_id, gate_id,
		execution_role, bound_at FROM coordinator_usage_bindings
		WHERE provider = ? AND thread_id = ?`, want.Provider, want.ThreadID).Scan(
		&got.Provider, &got.ThreadID, &got.WorkflowRunID, &got.TaskID, &got.AttemptID,
		&got.AssignmentID, &got.AssignmentEpoch, &got.ActivationID, &got.GateID,
		&got.Role, &got.BoundAt,
	); err != nil {
		return fmt.Errorf("load assignment usage binding %q: %w", assignment.ID, err)
	}
	got.BoundAt, want.BoundAt = "", ""
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("assignment usage binding %q conflicts with durable identity", assignment.ID)
	}
	return nil
}

const attributedUsageSelect = `SELECT
	u.event_id, u.provider, u.thread_id, u.model, u.observed_at,
	u.input_tokens, u.cache_write_tokens, u.cache_read_tokens, u.output_tokens,
	u.cost_usd, u.kind, u.cumulative_tokens,
	CASE WHEN b.thread_id IS NULL THEN 'unattributed' ELSE 'attributed' END,
	COALESCE(b.workflow_run_id, ''), COALESCE(b.task_id, ''),
	COALESCE(b.attempt_id, ''), COALESCE(b.assignment_id, ''),
	COALESCE(b.assignment_epoch, 0), COALESCE(b.activation_id, ''),
	COALESCE(b.gate_id, ''), COALESCE(b.execution_role, '')
FROM usage_samples AS u
LEFT JOIN coordinator_usage_bindings AS b
	ON b.provider = u.provider AND b.thread_id = u.thread_id`

func scanAttributedUsage(rows *sql.Rows) ([]domain.UsageSample, error) {
	defer rows.Close()
	var out []domain.UsageSample
	for rows.Next() {
		var at string
		var u domain.UsageSample
		if err := rows.Scan(
			&u.SourceEventID, &u.ProviderInstanceID, &u.ThreadID, &u.Model, &at,
			&u.InputTokens, &u.CacheWriteTokens, &u.CacheReadTokens, &u.OutputTokens,
			&u.CostUSD, &u.Kind, &u.CumulativeTokens,
			&u.Attribution.Status, &u.Attribution.WorkflowRunID, &u.Attribution.TaskID,
			&u.Attribution.AttemptID, &u.Attribution.AssignmentID,
			&u.Attribution.AssignmentEpoch, &u.Attribution.ActivationID,
			&u.Attribution.GateID, &u.Attribution.Role,
		); err != nil {
			return nil, err
		}
		u.ObservedAt, _ = time.Parse(time.RFC3339Nano, at)
		out = append(out, u)
	}
	return out, rows.Err()
}

// AttributedUsage is the public run-scoped usage query. An empty run ID returns
// all samples, including explicit unattributed rows, for administrative audit.
func (s *Store) AttributedUsage(ctx context.Context, runID string) ([]domain.UsageSample, error) {
	query := attributedUsageSelect
	var args []any
	if runID != "" {
		query += ` WHERE b.workflow_run_id = ?`
		args = append(args, runID)
	}
	query += ` ORDER BY u.observed_at, u.event_id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return scanAttributedUsage(rows)
}
