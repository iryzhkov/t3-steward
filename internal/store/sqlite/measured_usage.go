package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const MaxWorkerUsageDelivery = 128
const MaxRunUsageAggregation = 10000

const coordinatorMigrationV29 = ""

func applyCoordinatorMigrationV29(tx *sql.Tx) error {
	for _, table := range []string{"worker_usage_forwarded", "coordinator_worker_usage_receipts"} {
		hasRevision, err := transactionHasColumn(tx, table, "revision")
		if err != nil {
			return err
		}
		if hasRevision {
			continue
		}
		if _, err := tx.Exec("ALTER TABLE " + table + " ADD COLUMN revision INTEGER NOT NULL DEFAULT 0"); err != nil {
			return err
		}
	}
	return nil
}

func transactionHasColumn(tx *sql.Tx, table, column string) (bool, error) {
	rows, err := tx.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid     int
			name    string
			ctype   string
			notnull int
			dflt    any
			pk      int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

const coordinatorMigrationV28 = `
ALTER TABLE usage_samples ADD COLUMN field_presence INTEGER NOT NULL DEFAULT 0;
ALTER TABLE usage_samples ADD COLUMN boundary_id TEXT NOT NULL DEFAULT '';
ALTER TABLE usage_samples ADD COLUMN incarnation TEXT NOT NULL DEFAULT '';
ALTER TABLE usage_samples ADD COLUMN causal_sequence INTEGER NOT NULL DEFAULT 0;
ALTER TABLE usage_samples ADD COLUMN diagnostic_code TEXT NOT NULL DEFAULT '';
CREATE TABLE IF NOT EXISTS usage_diagnostic_overflow (
	worker_id TEXT PRIMARY KEY,
	dropped_count INTEGER NOT NULL CHECK(dropped_count >= 0)
);
`

const coordinatorMigrationV27 = `
ALTER TABLE usage_samples ADD COLUMN cost_reported INTEGER NOT NULL DEFAULT 0;
CREATE INDEX usage_samples_session_order
	ON usage_samples(worker_id, provider, thread_id, observed_at, event_id);
`

const coordinatorMigrationV26 = `
DROP TRIGGER IF EXISTS immutable_coordinator_usage_binding;
DROP INDEX IF EXISTS coordinator_usage_bindings_run;
CREATE TABLE coordinator_usage_bindings_v26 (
	worker_id TEXT NOT NULL,
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
	PRIMARY KEY(worker_id, provider, thread_id),
	UNIQUE(assignment_id, assignment_epoch)
);
INSERT INTO coordinator_usage_bindings_v26(
	worker_id, provider, thread_id, workflow_run_id, task_id, attempt_id,
	assignment_id, assignment_epoch, activation_id, gate_id, execution_role, bound_at
)
SELECT '', provider, thread_id, workflow_run_id, task_id, attempt_id,
	assignment_id, assignment_epoch, activation_id, gate_id, execution_role, bound_at
FROM coordinator_usage_bindings;
DROP TABLE coordinator_usage_bindings;
ALTER TABLE coordinator_usage_bindings_v26 RENAME TO coordinator_usage_bindings;
CREATE INDEX coordinator_usage_bindings_run
	ON coordinator_usage_bindings(workflow_run_id, task_id, attempt_id);
CREATE TRIGGER immutable_coordinator_usage_binding
	BEFORE UPDATE ON coordinator_usage_bindings
	BEGIN SELECT RAISE(ABORT, 'usage binding is immutable'); END;

CREATE TABLE usage_samples_v26 (
	worker_id TEXT NOT NULL,
	event_id TEXT NOT NULL,
	provider TEXT NOT NULL,
	thread_id TEXT NOT NULL,
	model TEXT NOT NULL,
	observed_at TEXT NOT NULL,
	input_tokens INTEGER NOT NULL,
	cache_write_tokens INTEGER NOT NULL,
	cache_read_tokens INTEGER NOT NULL,
	output_tokens INTEGER NOT NULL,
	cost_usd REAL NOT NULL,
	kind TEXT NOT NULL DEFAULT '',
	cumulative_tokens INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY(worker_id, event_id)
);
INSERT INTO usage_samples_v26(
	worker_id, event_id, provider, thread_id, model, observed_at,
	input_tokens, cache_write_tokens, cache_read_tokens, output_tokens,
	cost_usd, kind, cumulative_tokens
)
SELECT '', event_id, provider, thread_id, model, observed_at,
	input_tokens, cache_write_tokens, cache_read_tokens, output_tokens,
	cost_usd, kind, cumulative_tokens
FROM usage_samples;
CREATE TABLE worker_usage_forwarded_v26 (
	worker_id TEXT NOT NULL,
	event_id TEXT NOT NULL,
	acknowledged_at TEXT NOT NULL,
	PRIMARY KEY(worker_id, event_id),
	FOREIGN KEY(worker_id, event_id) REFERENCES usage_samples_v26(worker_id, event_id) ON DELETE CASCADE
);
INSERT INTO worker_usage_forwarded_v26(worker_id, event_id, acknowledged_at)
SELECT '', event_id, acknowledged_at FROM worker_usage_forwarded;
DROP TABLE worker_usage_forwarded;
DROP TABLE usage_samples;
ALTER TABLE usage_samples_v26 RENAME TO usage_samples;
ALTER TABLE worker_usage_forwarded_v26 RENAME TO worker_usage_forwarded;
CREATE INDEX usage_samples_at ON usage_samples(observed_at);
`

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
	WorkerID        string
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
func bindAssignmentUsageTx(ctx context.Context, tx *sql.Tx, assignment *domain.Assignment, boundAt time.Time) error {
	if assignment.ThreadID == "" {
		return nil
	}
	if assignment.WorkerID == "" || assignment.Route.ProviderInstanceID == "" || assignment.ThreadID == "" {
		return fmt.Errorf("bind assignment usage %q: worker, provider instance, and thread must be complete", assignment.ID)
	}
	runID, taskID, err := resolveAssignmentUsageIdentityTx(ctx, tx, assignment)
	if err != nil {
		return fmt.Errorf("bind assignment usage %q: %w", assignment.ID, err)
	}
	want := usageBinding{
		WorkerID: assignment.WorkerID, Provider: assignment.Route.ProviderInstanceID, ThreadID: assignment.ThreadID,
		WorkflowRunID: runID, TaskID: taskID, AttemptID: assignment.AttemptID,
		AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
		ActivationID: assignment.ActivationID, GateID: assignment.GateID, Role: assignment.ExecutionRole,
		BoundAt: boundAt.UTC().Format(time.RFC3339Nano),
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO coordinator_usage_bindings(
		worker_id, provider, thread_id, workflow_run_id, task_id, attempt_id, assignment_id,
		assignment_epoch, activation_id, gate_id, execution_role, bound_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
		want.WorkerID, want.Provider, want.ThreadID, want.WorkflowRunID, want.TaskID, want.AttemptID,
		want.AssignmentID, want.AssignmentEpoch, want.ActivationID, want.GateID,
		want.Role, want.BoundAt); err != nil {
		return fmt.Errorf("bind assignment usage %q: %w", assignment.ID, err)
	}
	var got usageBinding
	if err := tx.QueryRowContext(ctx, `SELECT worker_id, provider, thread_id, workflow_run_id,
		task_id, attempt_id, assignment_id, assignment_epoch, activation_id, gate_id,
		execution_role, bound_at FROM coordinator_usage_bindings
		WHERE worker_id = ? AND provider = ? AND thread_id = ?`, want.WorkerID, want.Provider, want.ThreadID).Scan(
		&got.WorkerID, &got.Provider, &got.ThreadID, &got.WorkflowRunID, &got.TaskID, &got.AttemptID,
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
	u.worker_id, u.event_id, u.provider, u.thread_id, u.model, u.observed_at,
	u.input_tokens, u.cache_write_tokens, u.cache_read_tokens, u.output_tokens,
	u.cost_usd, u.cost_reported, u.kind, u.cumulative_tokens,
	u.field_presence, u.boundary_id, u.incarnation, u.causal_sequence, u.diagnostic_code,
	CASE WHEN b.thread_id IS NULL THEN 'unattributed' ELSE 'attributed' END,
	COALESCE(b.workflow_run_id, ''), COALESCE(b.task_id, ''),
	COALESCE(b.attempt_id, ''), COALESCE(b.assignment_id, ''),
	COALESCE(b.assignment_epoch, 0), COALESCE(b.activation_id, ''),
	COALESCE(b.gate_id, ''), COALESCE(b.execution_role, '')
FROM usage_samples AS u
LEFT JOIN coordinator_usage_bindings AS b
	ON u.worker_id <> '' AND b.worker_id = u.worker_id
	AND b.provider = u.provider AND b.thread_id = u.thread_id`

func scanAttributedUsage(rows *sql.Rows) ([]domain.UsageSample, error) {
	defer rows.Close()
	var out []domain.UsageSample
	for rows.Next() {
		var at string
		var u domain.UsageSample
		if err := rows.Scan(
			&u.WorkerID, &u.SourceEventID, &u.ProviderInstanceID, &u.ThreadID, &u.Model, &at,
			&u.InputTokens, &u.CacheWriteTokens, &u.CacheReadTokens, &u.OutputTokens,
			&u.CostUSD, &u.CostReported, &u.Kind, &u.CumulativeTokens,
			&u.FieldPresence, &u.BoundaryID, &u.Incarnation, &u.Sequence, &u.DiagnosticCode,
			&u.Attribution.Status, &u.Attribution.WorkflowRunID, &u.Attribution.TaskID,
			&u.Attribution.AttemptID, &u.Attribution.AssignmentID,
			&u.Attribution.AssignmentEpoch, &u.Attribution.ActivationID,
			&u.Attribution.GateID, &u.Attribution.Role,
		); err != nil {
			return nil, err
		}
		u.ObservedAt, _ = time.Parse(time.RFC3339Nano, at)
		if u.Attribution.Status == domain.UsageAttributed {
			u.Attribution.WorkerID = u.WorkerID
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// AttributedUsage is bounded to one run and separately reports global unknown coverage.
func (s *Store) AttributedUsage(ctx context.Context, runID string) (domain.UsageReport, error) {
	report := domain.UsageReport{WorkflowRunID: runID, Coverage: domain.UsageCoverage{
		Reason: "provider samples without an authoritative dispatch binding are global/unscoped and are not assigned to this run",
	}}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_samples AS u
		LEFT JOIN coordinator_usage_bindings AS b
		ON u.worker_id <> '' AND b.worker_id = u.worker_id
		AND b.provider = u.provider AND b.thread_id = u.thread_id
		WHERE b.thread_id IS NULL`).Scan(&report.Coverage.UnscopedUnattributedCount); err != nil {
		return domain.UsageReport{}, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(dropped_count), 0) FROM usage_diagnostic_overflow`).Scan(
		&report.Coverage.DiagnosticDroppedCount); err != nil {
		return domain.UsageReport{}, err
	}
	if runID == "" {
		return report, nil
	}
	rows, err := s.db.QueryContext(ctx, attributedUsageSelect+
		` WHERE b.workflow_run_id = ? ORDER BY u.observed_at, u.worker_id, u.event_id LIMIT ?`,
		runID, MaxRunUsageAggregation+1)
	if err != nil {
		return domain.UsageReport{}, err
	}
	report.Samples, err = scanAttributedUsage(rows)
	if err != nil {
		return domain.UsageReport{}, err
	}
	if len(report.Samples) > MaxRunUsageAggregation {
		report.Samples = report.Samples[:MaxRunUsageAggregation]
		report.Coverage.Truncated = true
	}
	return report, nil
}

type usageExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func recordUsage(ctx context.Context, execer usageExecer, u domain.UsageSample) error {
	_, err := execer.ExecContext(ctx,
		`INSERT OR IGNORE INTO usage_samples(worker_id, event_id, provider, thread_id, model, observed_at, input_tokens, cache_write_tokens, cache_read_tokens, output_tokens, cost_usd, cost_reported, kind, cumulative_tokens, field_presence, boundary_id, incarnation, causal_sequence, diagnostic_code)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		u.WorkerID, u.SourceEventID, u.ProviderInstanceID, u.ThreadID, u.Model, u.ObservedAt.UTC().Format(time.RFC3339Nano),
		u.InputTokens, u.CacheWriteTokens, u.CacheReadTokens, u.OutputTokens, u.CostUSD, u.CostReported, u.Kind, u.CumulativeTokens,
		u.FieldPresence, u.BoundaryID, u.Incarnation, u.Sequence, u.DiagnosticCode)
	if err != nil || u.Kind != domain.UsageKindDiagnostic {
		return err
	}
	if u.DiagnosticCode == "overflow" {
		if _, err = execer.ExecContext(ctx, `UPDATE usage_samples SET
			cumulative_tokens = MAX(cumulative_tokens, ?), observed_at = CASE WHEN cumulative_tokens < ? THEN ? ELSE observed_at END
			WHERE worker_id = ? AND event_id = ?`, u.CumulativeTokens, u.CumulativeTokens,
			u.ObservedAt.UTC().Format(time.RFC3339Nano), u.WorkerID, u.SourceEventID); err != nil {
			return err
		}
		_, err = execer.ExecContext(ctx, `INSERT INTO usage_diagnostic_overflow(worker_id, dropped_count) VALUES(?, ?)
			ON CONFLICT(worker_id) DO UPDATE SET dropped_count = MAX(dropped_count, excluded.dropped_count)`, u.WorkerID, u.CumulativeTokens)
		return err
	}
	result, err := execer.ExecContext(ctx, `DELETE FROM usage_samples WHERE rowid IN (
		SELECT rowid FROM usage_samples WHERE worker_id = ? AND kind = ? AND diagnostic_code <> 'overflow'
		ORDER BY observed_at DESC, event_id DESC LIMIT -1 OFFSET 1000)`, u.WorkerID, domain.UsageKindDiagnostic)
	if err != nil {
		return err
	}
	dropped, err := result.RowsAffected()
	if err != nil || dropped == 0 {
		return err
	}
	_, err = execer.ExecContext(ctx, `INSERT INTO usage_diagnostic_overflow(worker_id, dropped_count) VALUES(?, ?)
		ON CONFLICT(worker_id) DO UPDATE SET dropped_count = dropped_count + excluded.dropped_count`, u.WorkerID, dropped)
	if err != nil {
		return err
	}
	if _, err = execer.ExecContext(ctx, `DELETE FROM usage_samples WHERE worker_id = ? AND diagnostic_code = 'overflow'`, u.WorkerID); err != nil {
		return err
	}
	_, err = execer.ExecContext(ctx, `INSERT INTO usage_samples(
		worker_id,event_id,provider,thread_id,model,observed_at,input_tokens,cache_write_tokens,cache_read_tokens,
		output_tokens,cost_usd,cost_reported,kind,cumulative_tokens,field_presence,boundary_id,incarnation,causal_sequence,diagnostic_code)
		SELECT worker_id, 'diagnostic-overflow', '', '', '', ?, 0, 0, 0, 0, 0, 0, ?,
			dropped_count, 0, '', '', 0, 'overflow' FROM usage_diagnostic_overflow WHERE worker_id = ?`,
		u.ObservedAt.UTC().Format(time.RFC3339Nano), domain.UsageKindDiagnostic, u.WorkerID)
	return err
}

func decodeUsageAcknowledgement(value string) (string, int64, bool) {
	if value == "" {
		return "", 0, false
	}
	const overflow = "diagnostic-overflow@"
	if !strings.HasPrefix(value, overflow) {
		return value, 0, true
	}
	revision, err := strconv.ParseInt(strings.TrimPrefix(value, overflow), 10, 64)
	if err != nil || revision < 1 {
		return "", 0, false
	}
	return "diagnostic-overflow", revision, true
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
	for _, acknowledgement := range acknowledged {
		id, revision, valid := decodeUsageAcknowledgement(acknowledgement)
		if !valid {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO worker_usage_forwarded(worker_id,event_id,acknowledged_at,revision)
			SELECT worker_id, event_id, ?, ? FROM usage_samples
			WHERE event_id = ? AND (? = 0 OR cumulative_tokens = ?)
			ON CONFLICT(worker_id,event_id) DO UPDATE SET
				revision = MAX(revision, excluded.revision), acknowledged_at = excluded.acknowledged_at`,
			time.Now().UTC().Format(time.RFC3339Nano), revision, id, revision, revision); err != nil {
			return nil, err
		}
	}
	rows, err := tx.QueryContext(ctx, attributedUsageSelect+
		` WHERE NOT EXISTS (SELECT 1 FROM worker_usage_forwarded AS f
			WHERE f.worker_id = u.worker_id AND f.event_id = u.event_id
			AND f.revision >= CASE WHEN u.diagnostic_code = 'overflow' THEN u.cumulative_tokens ELSE 0 END)
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
		samples[i].WorkerID = ""
		samples[i].Attribution = domain.UsageAttribution{}
	}
	return samples, nil
}

func (s *Store) ReceiveWorkerUsage(ctx context.Context, workerID string, samples []domain.UsageSample) error {
	if workerID == "" || len(samples) > MaxWorkerUsageDelivery {
		return fmt.Errorf("invalid worker usage delivery")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, sample := range samples {
		if sample.WorkerID != "" && sample.WorkerID != workerID {
			return fmt.Errorf("worker usage sample provenance conflicts with authenticated worker")
		}
		sample.WorkerID = workerID
		if err := recordUsage(ctx, tx, sample); err != nil {
			return err
		}
		revision := int64(0)
		if sample.DiagnosticCode == "overflow" {
			revision = sample.CumulativeTokens
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO coordinator_worker_usage_receipts(worker_id,event_id,received_at,revision)
			VALUES(?,?,?,?) ON CONFLICT(worker_id,event_id) DO UPDATE SET
				revision = MAX(revision, excluded.revision), received_at = excluded.received_at`,
			workerID, sample.SourceEventID, time.Now().UTC().Format(time.RFC3339Nano), revision); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) WorkerUsageAcknowledgements(ctx context.Context, workerID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT CASE WHEN revision > 0 THEN event_id || '@' || revision ELSE event_id END
		FROM coordinator_worker_usage_receipts WHERE worker_id = ? ORDER BY received_at, event_id LIMIT ?`, workerID, MaxWorkerUsageDelivery)
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
