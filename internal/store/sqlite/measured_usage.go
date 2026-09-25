package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
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

// unboundUsageWhere selects the samples no dispatch binding claims, counted
// once each.
//
// A row with an empty worker_id is a reading this host's own worker took. On
// the coordinator's host the worker shares the coordinator's database, so the
// same reading is stored twice: once as the local row, which never joins a
// binding, and again under the worker's id when WorkerUsageBatch forwards it
// and the coordinator records the delivery. Counting both made every reading
// of the coordinator host's own worker look unattributed even when its
// forwarded copy was bound to a run. A local row is therefore left out once a
// forwarded copy with the same event, provider and thread exists; until then
// it is the only record of the reading and still counts.
const unboundUsageWhere = `NOT EXISTS (SELECT 1 FROM coordinator_usage_bindings AS b
		WHERE u.worker_id <> '' AND b.worker_id = u.worker_id
		AND b.provider = u.provider AND b.thread_id = u.thread_id)
	AND (u.worker_id <> '' OR (u.event_id, u.provider, u.thread_id) NOT IN (
		SELECT event_id, provider, thread_id FROM usage_samples WHERE worker_id <> ''))`

// runUsageSkewDays is how far before its dispatch a sample may be observed
// and still be the run's: fifteen minutes, in julian days. The dispatch is
// stamped with the coordinator's clock and a sample with the worker's, and the
// two are not assumed to agree; erring wide only makes coverage partial more
// often, never complete wrongly.
const runUsageSkewDays = "(15.0 / 1440)"

// runUsageDispatches is every dispatch of the run as (worker_id, since): the
// usage bindings, and also the assignments themselves, because an assignment
// dispatched without a thread writes no binding (bindAssignmentUsageTx) and
// its samples are then unbound however the run went. since is NULL for an
// assignment whose creation time was never recorded, which bounds nothing.
// It takes the run id twice.
const runUsageDispatches = `SELECT worker_id, julianday(bound_at) AS since
		FROM coordinator_usage_bindings WHERE workflow_run_id = ?
	UNION ALL
	SELECT json_extract(a.record, '$.workerId'),
		CASE WHEN json_extract(a.record, '$.createdAt') > '2000'
			THEN julianday(json_extract(a.record, '$.createdAt')) END
		FROM coordinator_assignments AS a
		JOIN coordinator_attempts AS t ON t.id = a.attempt_id
		WHERE t.workflow_run_id = ?`

// runUsageWindow is the span in which a run's provider sessions could have
// produced evidence, as SQLite julian days: from its first dispatch, less
// runUsageSkewDays, to a minute after it completed, the same allowance
// NormalizeUsageReport gives late evidence. End is empty while the run has not
// completed, and the window is then open-ended. Start is empty when the run
// was never dispatched.
//
// The times are compared through julianday rather than as text because
// RFC 3339 with trimmed fractional seconds does not sort as text: "...00Z"
// sorts after "...00.5Z".
type runUsageWindow struct {
	Start, End sql.NullFloat64
}

func (s *Store) runUsageWindow(ctx context.Context, runID string) (runUsageWindow, error) {
	var window runUsageWindow
	if err := s.db.QueryRowContext(ctx, `SELECT
		(SELECT MIN(since) - `+runUsageSkewDays+` FROM (`+runUsageDispatches+`)),
		(SELECT julianday(json_extract(record, '$.completedAt')) + 1.0 / 1440
			FROM coordinator_workflow_runs WHERE id = ?)`, runID, runID, runID).Scan(&window.Start, &window.End); err != nil {
		return runUsageWindow{}, err
	}
	return window, nil
}

// AttributedUsage is bounded to one run and separately reports the unbound
// samples around it, split by whether they could be the run's own.
//
// A sample without a dispatch binding cannot be assigned to any run, so it
// is never added to a run's totals. What it does to the run's coverage depends
// on whether it could have been this run's evidence:
//
//   - RunWindowUnattributedCount holds the unbound samples that could be: on a
//     worker the run was dispatched to (or this host's own not-yet-forwarded
//     readings, whose worker is not recorded), for any provider, observed no
//     earlier than that dispatch less the skew allowance and no later than a
//     minute after the run completed. A binding that was never written, a
//     thread the provider renamed, or a provider log that names the instance
//     differently from the route (claude against claude-main) all look exactly
//     like this, so these keep the run's coverage partial. The provider is
//     deliberately not matched: matching it would call such a run complete.
//   - UnscopedUnattributedCount holds every unbound sample in the run's window
//     on any worker. Most of it is interactive work and other hosts' sessions,
//     which no dispatch of this run could have produced; it is reported as
//     context and does not by itself make the run's coverage partial. Before
//     this split the fleet-wide count, over all time, was the run's
//     unattributed count, so a fully attributed run always read partial.
//
// With no run, or a run that was never dispatched, there is no window and the
// unscoped count is over all retained samples.
func (s *Store) AttributedUsage(ctx context.Context, runID string) (domain.UsageReport, error) {
	report := domain.UsageReport{WorkflowRunID: runID, Coverage: domain.UsageCoverage{
		Reason: "provider samples without an authoritative dispatch binding are global/unscoped and are not assigned to this run",
	}}
	var window runUsageWindow
	if runID != "" {
		var err error
		if window, err = s.runUsageWindow(ctx, runID); err != nil {
			return domain.UsageReport{}, err
		}
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_samples AS u
		WHERE `+unboundUsageWhere+`
		AND (? IS NULL OR julianday(u.observed_at) >= ?)
		AND (? IS NULL OR julianday(u.observed_at) <= ?)`,
		window.Start, window.Start, window.End, window.End,
	).Scan(&report.Coverage.UnscopedUnattributedCount); err != nil {
		return domain.UsageReport{}, err
	}
	if window.Start.Valid {
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_samples AS u
			WHERE `+unboundUsageWhere+`
			AND EXISTS (SELECT 1 FROM (`+runUsageDispatches+`) AS d
				WHERE (d.worker_id = u.worker_id OR u.worker_id = '')
				AND (d.since IS NULL OR julianday(u.observed_at) >= d.since - `+runUsageSkewDays+`))
			AND (? IS NULL OR julianday(u.observed_at) <= ?)`,
			runID, runID, window.End, window.End,
		).Scan(&report.Coverage.RunWindowUnattributedCount); err != nil {
			return domain.UsageReport{}, err
		}
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(dropped_count), 0) FROM usage_diagnostic_overflow`).Scan(
		&report.Coverage.DiagnosticDroppedCount); err != nil {
		return domain.UsageReport{}, err
	}
	if runID == "" {
		return report, nil
	}
	sessionRows, err := s.db.QueryContext(ctx, `SELECT worker_id, provider, thread_id,
		workflow_run_id, task_id, attempt_id, assignment_id, assignment_epoch,
		activation_id, gate_id, execution_role
		FROM coordinator_usage_bindings WHERE workflow_run_id = ?
		ORDER BY worker_id, provider, thread_id, assignment_id LIMIT ?`,
		runID, MaxRunUsageAggregation+1)
	if err != nil {
		return domain.UsageReport{}, err
	}
	for sessionRows.Next() {
		var session domain.UsageExecutionSession
		session.Attribution.Status = domain.UsageAttributed
		if err := sessionRows.Scan(
			&session.WorkerID, &session.ProviderInstanceID, &session.ThreadID,
			&session.Attribution.WorkflowRunID, &session.Attribution.TaskID,
			&session.Attribution.AttemptID, &session.Attribution.AssignmentID,
			&session.Attribution.AssignmentEpoch, &session.Attribution.ActivationID,
			&session.Attribution.GateID, &session.Attribution.Role,
		); err != nil {
			sessionRows.Close()
			return domain.UsageReport{}, err
		}
		session.Attribution.WorkerID = session.WorkerID
		report.ExpectedSessions = append(report.ExpectedSessions, session)
	}
	if err := sessionRows.Err(); err != nil {
		sessionRows.Close()
		return domain.UsageReport{}, err
	}
	if err := sessionRows.Close(); err != nil {
		return domain.UsageReport{}, err
	}
	if len(report.ExpectedSessions) > MaxRunUsageAggregation {
		report.ExpectedSessions = report.ExpectedSessions[:MaxRunUsageAggregation]
		report.Coverage.Truncated = true
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

type usageMutationHook func(string) error

func usageMutationCheckpoint(hook usageMutationHook, stage string) error {
	if hook == nil {
		return nil
	}
	return hook(stage)
}

func recordUsage(ctx context.Context, execer usageExecer, u domain.UsageSample) error {
	return recordUsageWithHook(ctx, execer, u, nil)
}

func recordUsageWithHook(ctx context.Context, execer usageExecer, u domain.UsageSample, hook usageMutationHook) error {
	_, err := execer.ExecContext(ctx,
		`INSERT OR IGNORE INTO usage_samples(worker_id, event_id, provider, thread_id, model, observed_at, input_tokens, cache_write_tokens, cache_read_tokens, output_tokens, cost_usd, cost_reported, kind, cumulative_tokens, field_presence, boundary_id, incarnation, causal_sequence, diagnostic_code)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		u.WorkerID, u.SourceEventID, u.ProviderInstanceID, u.ThreadID, u.Model, u.ObservedAt.UTC().Format(time.RFC3339Nano),
		u.InputTokens, u.CacheWriteTokens, u.CacheReadTokens, u.OutputTokens, u.CostUSD, u.CostReported, u.Kind, u.CumulativeTokens,
		u.FieldPresence, u.BoundaryID, u.Incarnation, u.Sequence, u.DiagnosticCode)
	if err != nil {
		return err
	}
	if err := usageMutationCheckpoint(hook, "sample-inserted"); err != nil {
		return err
	}
	if u.Kind != domain.UsageKindDiagnostic {
		return nil
	}
	if u.DiagnosticCode == "overflow" {
		if _, err = execer.ExecContext(ctx, `UPDATE usage_samples SET
			cumulative_tokens = MAX(cumulative_tokens, ?), observed_at = CASE WHEN cumulative_tokens < ? THEN ? ELSE observed_at END
			WHERE worker_id = ? AND event_id = ?`, u.CumulativeTokens, u.CumulativeTokens,
			u.ObservedAt.UTC().Format(time.RFC3339Nano), u.WorkerID, u.SourceEventID); err != nil {
			return err
		}
		if err := usageMutationCheckpoint(hook, "overflow-marker-updated"); err != nil {
			return err
		}
		_, err = execer.ExecContext(ctx, `INSERT INTO usage_diagnostic_overflow(worker_id, dropped_count) VALUES(?, ?)
			ON CONFLICT(worker_id) DO UPDATE SET dropped_count = MAX(dropped_count, excluded.dropped_count)`, u.WorkerID, u.CumulativeTokens)
		if err != nil {
			return err
		}
		return usageMutationCheckpoint(hook, "overflow-count-updated")
	}
	result, err := execer.ExecContext(ctx, `DELETE FROM usage_samples WHERE rowid IN (
		SELECT rowid FROM usage_samples WHERE worker_id = ? AND kind = ? AND diagnostic_code <> 'overflow'
		ORDER BY observed_at DESC, event_id DESC LIMIT -1 OFFSET 1000)`, u.WorkerID, domain.UsageKindDiagnostic)
	if err != nil {
		return err
	}
	dropped, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if err := usageMutationCheckpoint(hook, "diagnostics-pruned"); err != nil {
		return err
	}
	if dropped == 0 {
		return nil
	}
	_, err = execer.ExecContext(ctx, `INSERT INTO usage_diagnostic_overflow(worker_id, dropped_count) VALUES(?, ?)
		ON CONFLICT(worker_id) DO UPDATE SET dropped_count = dropped_count + excluded.dropped_count`, u.WorkerID, dropped)
	if err != nil {
		return err
	}
	if err := usageMutationCheckpoint(hook, "overflow-count-updated"); err != nil {
		return err
	}
	if _, err = execer.ExecContext(ctx, `DELETE FROM worker_usage_forwarded
		WHERE worker_id = ? AND event_id = 'diagnostic-overflow'`, u.WorkerID); err != nil {
		return err
	}
	if err := usageMutationCheckpoint(hook, "forwarded-marker-cleared"); err != nil {
		return err
	}
	if _, err = execer.ExecContext(ctx, `DELETE FROM usage_samples WHERE worker_id = ? AND diagnostic_code = 'overflow'`, u.WorkerID); err != nil {
		return err
	}
	if err := usageMutationCheckpoint(hook, "overflow-marker-cleared"); err != nil {
		return err
	}
	_, err = execer.ExecContext(ctx, `INSERT INTO usage_samples(
		worker_id,event_id,provider,thread_id,model,observed_at,input_tokens,cache_write_tokens,cache_read_tokens,
		output_tokens,cost_usd,cost_reported,kind,cumulative_tokens,field_presence,boundary_id,incarnation,causal_sequence,diagnostic_code)
		SELECT worker_id, 'diagnostic-overflow', '', '', '', ?, 0, 0, 0, 0, 0, 0, ?,
			dropped_count, 0, '', '', 0, 'overflow' FROM usage_diagnostic_overflow WHERE worker_id = ?`,
		u.ObservedAt.UTC().Format(time.RFC3339Nano), domain.UsageKindDiagnostic, u.WorkerID)
	if err != nil {
		return err
	}
	return usageMutationCheckpoint(hook, "overflow-marker-written")
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
	// Only this host's own readings are forwarded. On the coordinator's host the
	// worker shares the coordinator's database, which also holds the samples
	// other workers delivered; forwarding those would claim them for this
	// worker. A reading without an event id is never offered: the coordinator
	// cannot acknowledge it, so it would be offered on every exchange and take
	// a place in every batch. It stays in this database for local reports.
	rows, err := tx.QueryContext(ctx, attributedUsageSelect+
		` WHERE u.worker_id = '' AND u.event_id <> '' AND NOT EXISTS (SELECT 1 FROM worker_usage_forwarded AS f
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

// WorkerUsageRejection names one delivered usage sample the coordinator
// refused to store, and why. It carries enough of the sample to find it in
// the worker's own database, because the coordinator keeps nothing else of it.
type WorkerUsageRejection struct {
	EventID            string
	ProviderInstanceID string
	ThreadID           string
	Model              string
	Reason             string
}

// WorkerUsageReceipt reports what one delivered usage batch became. Rejected
// lists only samples refused for the first time, so a caller that logs it
// reports each bad sample once rather than on every boundary.
type WorkerUsageReceipt struct {
	Stored   int
	Rejected []WorkerUsageRejection
}

// validateWorkerUsageSample returns why a delivered sample cannot be stored,
// or an empty string when it can. Only properties the coordinator relies on
// are checked: provenance, an acknowledgeable identity, and numbers SQLite
// can hold (a NaN cost is stored as NULL and fails the NOT NULL constraint,
// which would otherwise surface as a storage error and fail the whole batch).
func validateWorkerUsageSample(workerID string, sample domain.UsageSample) string {
	switch {
	case sample.WorkerID != "" && sample.WorkerID != workerID:
		return "sample provenance conflicts with the authenticated worker"
	case sample.SourceEventID == "" || strings.TrimSpace(sample.SourceEventID) != sample.SourceEventID:
		return "sample event id is empty or untrimmed"
	case sample.InputTokens < 0 || sample.CacheWriteTokens < 0 || sample.CacheReadTokens < 0 ||
		sample.OutputTokens < 0 || sample.CumulativeTokens < 0:
		return "sample has a negative token count"
	case math.IsNaN(sample.CostUSD) || math.IsInf(sample.CostUSD, 0) || sample.CostUSD < 0:
		return "sample cost is not a finite non-negative number"
	}
	return ""
}

// ReceiveWorkerUsage stores one delivered usage batch and records a receipt
// for every sample it settles, which the next exchange acknowledges to the
// worker. A sample that fails validation is rejected on its own: it gets a
// receipt too, so the worker stops offering it, but is not stored. One bad
// sample used to fail the whole batch, and because an unacknowledged batch is
// offered again unchanged, that worker's usage stopped reaching the
// coordinator for good (the same shape as S14). A storage error still fails
// the whole batch and commits nothing, so that it is retried intact.
func (s *Store) ReceiveWorkerUsage(ctx context.Context, workerID string, samples []domain.UsageSample) (WorkerUsageReceipt, error) {
	if workerID == "" || len(samples) > MaxWorkerUsageDelivery {
		return WorkerUsageReceipt{}, fmt.Errorf("invalid worker usage delivery")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkerUsageReceipt{}, err
	}
	defer tx.Rollback()
	var receipt WorkerUsageReceipt
	for _, sample := range samples {
		if reason := validateWorkerUsageSample(workerID, sample); reason != "" {
			rejection := WorkerUsageRejection{
				EventID: sample.SourceEventID, ProviderInstanceID: sample.ProviderInstanceID,
				ThreadID: sample.ThreadID, Model: sample.Model, Reason: reason,
			}
			if sample.SourceEventID == "" {
				// There is no identity to acknowledge, so there is nothing to
				// record; the sample is only reported. A current worker never
				// offers one (WorkerUsageBatch), so only an older worker's
				// would be reported again.
				receipt.Rejected = append(receipt.Rejected, rejection)
				continue
			}
			var known int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM coordinator_worker_usage_receipts
				WHERE worker_id = ? AND event_id = ?`, workerID, sample.SourceEventID).Scan(&known); err != nil {
				return WorkerUsageReceipt{}, err
			}
			if known == 0 {
				receipt.Rejected = append(receipt.Rejected, rejection)
			}
		} else {
			sample.WorkerID = workerID
			if err := recordUsage(ctx, tx, sample); err != nil {
				return WorkerUsageReceipt{}, err
			}
			receipt.Stored++
		}
		revision := int64(0)
		if sample.DiagnosticCode == "overflow" && sample.CumulativeTokens > 0 {
			revision = sample.CumulativeTokens
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO coordinator_worker_usage_receipts(worker_id,event_id,received_at,revision)
			VALUES(?,?,?,?) ON CONFLICT(worker_id,event_id) DO UPDATE SET
				revision = MAX(revision, excluded.revision), received_at = excluded.received_at`,
			workerID, sample.SourceEventID, time.Now().UTC().Format(time.RFC3339Nano), revision); err != nil {
			return WorkerUsageReceipt{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return WorkerUsageReceipt{}, err
	}
	return receipt, nil
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

func (s *Store) ClearWorkerUsageAcknowledgements(ctx context.Context, workerID string, acknowledgements []string) error {
	if len(acknowledgements) > MaxWorkerUsageDelivery {
		return fmt.Errorf("worker usage acknowledgement exceeds bound")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, acknowledgement := range acknowledgements {
		id, revision, valid := decodeUsageAcknowledgement(acknowledgement)
		if !valid {
			continue
		}
		if revision > 0 {
			if _, err := tx.ExecContext(ctx, `DELETE FROM coordinator_worker_usage_receipts
				WHERE worker_id = ? AND event_id = ? AND revision <= ?`, workerID, id, revision); err != nil {
				return err
			}
			continue
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM coordinator_worker_usage_receipts
			WHERE worker_id = ? AND event_id = ? AND revision = 0`, workerID, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}
