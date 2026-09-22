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

const MaxWorkerUsageDelivery = 128

const coordinatorMigrationV25 = `
CREATE TABLE IF NOT EXISTS coordinator_worker_usage_receipts (
	worker_id TEXT NOT NULL,
	event_id TEXT NOT NULL,
	received_at TEXT NOT NULL,
	PRIMARY KEY(worker_id, event_id)
);
CREATE TABLE IF NOT EXISTS worker_usage_forwarded (
	event_id TEXT PRIMARY KEY,
	acknowledged_at TEXT NOT NULL,
	FOREIGN KEY(event_id) REFERENCES usage_samples(event_id) ON DELETE CASCADE
);
`

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

func resolveAssignmentUsageIdentityTx(ctx context.Context, tx *sql.Tx, assignment *domain.Assignment) (string, string, error) {
	var runID, taskID, raw string
	if err := tx.QueryRowContext(ctx,
		`SELECT workflow_run_id, task_id, record FROM coordinator_attempts WHERE id = ?`,
		assignment.AttemptID,
	).Scan(&runID, &taskID, &raw); err != nil {
		return "", "", fmt.Errorf("load authoritative attempt %q: %w", assignment.AttemptID, err)
	}
	var attempt domain.Attempt
	if err := json.Unmarshal([]byte(raw), &attempt); err != nil {
		return "", "", fmt.Errorf("decode attempt %q: %w", assignment.AttemptID, err)
	}
	role := domain.ExecutionRoleExecutor
	activationID, gateID := "", ""
	var supplement int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM coordinator_recovery_supplements WHERE attempt_id = ?`,
		assignment.AttemptID).Scan(&supplement); err != nil {
		return "", "", err
	}
	if supplement > 0 {
		role = domain.ExecutionRoleRepairExecutor
	}
	if attempt.SupervisionActivationID != "" {
		activationID = attempt.SupervisionActivationID
		var activationRaw []byte
		if err := tx.QueryRowContext(ctx,
			`SELECT record FROM coordinator_supervision_activations WHERE id = ? AND run_id = ?`,
			activationID, runID).Scan(&activationRaw); err != nil {
			return "", "", fmt.Errorf("load authoritative activation %q: %w", activationID, err)
		}
		var activation domain.Activation
		if err := json.Unmarshal(activationRaw, &activation); err != nil {
			return "", "", fmt.Errorf("decode activation %q: %w", activationID, err)
		}
		if activation.Purpose == domain.RecoveryActivationRepair {
			role = domain.ExecutionRoleRepairExecutor
		} else if activation.Purpose != "" {
			return "", "", fmt.Errorf("activation %q has unsupported purpose %q", activationID, activation.Purpose)
		} else if activation.IncidentID != "" {
			var incidentRaw []byte
			if err := tx.QueryRowContext(ctx,
				`SELECT record FROM coordinator_supervision_incidents WHERE id = ? AND run_id = ?`,
				activation.IncidentID, runID).Scan(&incidentRaw); err != nil {
				return "", "", fmt.Errorf("load activation incident %q: %w", activation.IncidentID, err)
			}
			var incident domain.ReviewIncident
			if err := json.Unmarshal(incidentRaw, &incident); err != nil {
				return "", "", fmt.Errorf("decode activation incident %q: %w", activation.IncidentID, err)
			}
			if incident.GateID != "" {
				role, gateID = domain.ExecutionRoleGateReviewer, incident.GateID
			} else {
				role = domain.ExecutionRoleSupervisorActivation
			}
		} else {
			role = domain.ExecutionRoleSupervisorActivation
		}
	}
	if assignment.ExecutionRole != "" && assignment.ExecutionRole != role {
		return "", "", fmt.Errorf("assignment %q execution role %q conflicts with authoritative role %q", assignment.ID, assignment.ExecutionRole, role)
	}
	if assignment.ActivationID != "" && assignment.ActivationID != activationID {
		return "", "", fmt.Errorf("assignment %q activation conflicts with authoritative activation", assignment.ID)
	}
	if assignment.GateID != "" && assignment.GateID != gateID {
		return "", "", fmt.Errorf("assignment %q gate conflicts with authoritative gate", assignment.ID)
	}
	assignment.ExecutionRole, assignment.ActivationID, assignment.GateID = role, activationID, gateID
	return runID, taskID, nil
}

// bindAssignmentUsageTx records identity at the V2 dispatch boundary.
func bindAssignmentUsageTx(ctx context.Context, tx *sql.Tx, assignment domain.Assignment, boundAt time.Time) error {
	if assignment.Route.ProviderInstanceID == "" || assignment.ThreadID == "" {
		return fmt.Errorf("bind assignment usage %q: provider instance and thread are required", assignment.ID)
	}
	runID, taskID, err := resolveAssignmentUsageIdentityTx(ctx, tx, &assignment)
	if err != nil {
		return fmt.Errorf("bind assignment usage %q: %w", assignment.ID, err)
	}
	want := usageBinding{
		Provider: assignment.Route.ProviderInstanceID, ThreadID: assignment.ThreadID,
		WorkflowRunID: runID, TaskID: taskID, AttemptID: assignment.AttemptID,
		AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
		ActivationID: assignment.ActivationID, GateID: assignment.GateID, Role: assignment.ExecutionRole,
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

// AttributedUsage is bounded to one run and separately reports global unknown coverage.
func (s *Store) AttributedUsage(ctx context.Context, runID string) (domain.UsageReport, error) {
	report := domain.UsageReport{Coverage: domain.UsageCoverage{
		Reason: "provider samples without an authoritative dispatch binding are global/unscoped and are not assigned to this run",
	}}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_samples AS u
		LEFT JOIN coordinator_usage_bindings AS b
		ON b.provider = u.provider AND b.thread_id = u.thread_id
		WHERE b.thread_id IS NULL`).Scan(&report.Coverage.UnscopedUnattributedCount); err != nil {
		return domain.UsageReport{}, err
	}
	if runID == "" {
		return report, nil
	}
	rows, err := s.db.QueryContext(ctx, attributedUsageSelect+
		` WHERE b.workflow_run_id = ? ORDER BY u.observed_at, u.event_id`, runID)
	if err != nil {
		return domain.UsageReport{}, err
	}
	report.Samples, err = scanAttributedUsage(rows)
	return report, err
}

// WorkerUsageBatch applies coordinator acknowledgements and returns the oldest bounded raw batch.
func (s *Store) WorkerUsageBatch(ctx context.Context, acknowledged []string, limit int) ([]domain.UsageSample, error) {
	if limit < 1 || limit > MaxWorkerUsageDelivery || len(acknowledged) > MaxWorkerUsageDelivery {
		return nil, fmt.Errorf("worker usage delivery exceeds bound %d", MaxWorkerUsageDelivery)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	for _, id := range acknowledged {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO worker_usage_forwarded(event_id,acknowledged_at)
			SELECT event_id, ? FROM usage_samples WHERE event_id = ?`,
			time.Now().UTC().Format(time.RFC3339Nano), id); err != nil {
			return nil, err
		}
	}
	rows, err := tx.QueryContext(ctx, attributedUsageSelect+
		` WHERE NOT EXISTS (SELECT 1 FROM worker_usage_forwarded AS f WHERE f.event_id = u.event_id)
		ORDER BY u.observed_at, u.event_id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	samples, err := scanAttributedUsage(rows)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	for i := range samples {
		samples[i].Attribution = domain.UsageAttribution{}
	}
	return samples, nil
}

func (s *Store) ReceiveWorkerUsage(ctx context.Context, workerID string, samples []domain.UsageSample) error {
	if workerID == "" || len(samples) > MaxWorkerUsageDelivery {
		return fmt.Errorf("invalid worker usage delivery")
	}
	for _, sample := range samples {
		if err := s.RecordUsage(ctx, sample); err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO coordinator_worker_usage_receipts(worker_id,event_id,received_at)
			VALUES(?,?,?)`, workerID, sample.SourceEventID, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) WorkerUsageAcknowledgements(ctx context.Context, workerID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT event_id FROM coordinator_worker_usage_receipts
		WHERE worker_id = ? ORDER BY received_at, event_id LIMIT ?`, workerID, MaxWorkerUsageDelivery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) ClearWorkerUsageAcknowledgements(ctx context.Context, workerID string, ids []string) error {
	if len(ids) > MaxWorkerUsageDelivery {
		return fmt.Errorf("worker usage acknowledgement exceeds bound")
	}
	for _, id := range ids {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM coordinator_worker_usage_receipts WHERE worker_id = ? AND event_id = ?`, workerID, id); err != nil {
			return err
		}
	}
	return nil
}
