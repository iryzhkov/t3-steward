package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// QueryOnlyStore is a second, read-only connection to a coordinator database,
// for an observer that must never write coordinator state or queue behind the
// coordinator's own connection. The quota telemetry recorder is its only user.
//
// It is opened with mode=ro and query_only, so a statement that would write is
// refused by SQLite itself, and without immutable, so it sees the coordinator's
// committed writes. On a database in WAL mode, which every coordinator database
// is, a reader neither waits for the writer nor blocks it.
type QueryOnlyStore struct {
	db *sql.DB
}

// OpenQueryOnly opens an existing database file through a query-only
// connection. It never creates the file and refuses an in-memory path, which
// would be a different, empty database.
func OpenQueryOnly(path string) (*QueryOnlyStore, error) {
	if path == "" || path == ":memory:" {
		return nil, errors.New("query-only state database must be a file")
	}
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("open existing state database: %w", err)
	}
	dsn := sqliteFileURL(path) + "?mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open query-only state database: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("open query-only state database: %w", err)
	}
	return &QueryOnlyStore{db: db}, nil
}

// Close releases the connection.
func (q *QueryOnlyStore) Close() error {
	return q.db.Close()
}

// MaxAuditSequence returns the highest audit sequence, zero for an empty table.
func (q *QueryOnlyStore) MaxAuditSequence(ctx context.Context) (int64, error) {
	var maximum int64
	if err := q.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(sequence), 0) FROM coordinator_audit_events`,
	).Scan(&maximum); err != nil {
		return 0, fmt.Errorf("read maximum audit sequence: %w", err)
	}
	return maximum, nil
}

// AuditEventsAfter returns up to limit audit events of every kind whose
// sequence is above after, in sequence order. The kind and identity columns
// are authoritative; a record that does not decode still yields its row, with
// no detail, so one malformed record cannot stall a reader that advances by
// sequence.
func (q *QueryOnlyStore) AuditEventsAfter(ctx context.Context, after int64, limit int) ([]domain.AuditEvent, error) {
	if limit < 1 {
		return nil, errors.New("audit tail limit must be positive")
	}
	rows, err := q.db.QueryContext(ctx, `
		SELECT sequence, id, kind, workflow_run_id, task_id, attempt_id, target_type, target_id, created_at, record
		FROM coordinator_audit_events WHERE sequence > ? ORDER BY sequence LIMIT ?`, after, limit)
	if err != nil {
		return nil, fmt.Errorf("load audit events after %d: %w", after, err)
	}
	defer rows.Close()
	events := make([]domain.AuditEvent, 0)
	for rows.Next() {
		var event domain.AuditEvent
		var targetType, createdAt string
		var raw []byte
		if err := rows.Scan(&event.Sequence, &event.ID, &event.Kind, &event.WorkflowRunID, &event.TaskID,
			&event.AttemptID, &targetType, &event.TargetID, &createdAt, &raw); err != nil {
			return nil, fmt.Errorf("scan audit event: %w", err)
		}
		event.TargetType = domain.AdminTargetType(targetType)
		event.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
		var record struct {
			Detail json.RawMessage `json:"detail"`
		}
		if json.Unmarshal(raw, &record) == nil {
			event.Detail = record.Detail
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit events: %w", err)
	}
	return events, nil
}

// LoadAssignment reads one assignment by id.
func (q *QueryOnlyStore) LoadAssignment(ctx context.Context, id string) (domain.Assignment, bool, error) {
	var assignment domain.Assignment
	found, err := q.loadRecord(ctx, "assignment", `SELECT record FROM coordinator_assignments WHERE id = ?`, id, &assignment)
	return assignment, found, err
}

// LoadAttempt reads one attempt by id.
func (q *QueryOnlyStore) LoadAttempt(ctx context.Context, id string) (domain.Attempt, bool, error) {
	var attempt domain.Attempt
	found, err := q.loadRecord(ctx, "attempt", `SELECT record FROM coordinator_attempts WHERE id = ?`, id, &attempt)
	return attempt, found, err
}

// LoadTask reads one task by id.
func (q *QueryOnlyStore) LoadTask(ctx context.Context, id string) (domain.Task, bool, error) {
	var task domain.Task
	found, err := q.loadRecord(ctx, "task", `SELECT record FROM coordinator_tasks WHERE id = ?`, id, &task)
	return task, found, err
}

func (q *QueryOnlyStore) loadRecord(ctx context.Context, label, query, id string, target any) (bool, error) {
	var raw string
	err := q.db.QueryRowContext(ctx, query, id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load %s %q: %w", label, id, err)
	}
	if err := json.Unmarshal([]byte(raw), target); err != nil {
		return false, fmt.Errorf("decode %s %q: %w", label, id, err)
	}
	return true, nil
}

// LoadWorkerSnapshots returns every worker's durable snapshot projection.
func (q *QueryOnlyStore) LoadWorkerSnapshots(ctx context.Context) ([]domain.WorkerSnapshot, error) {
	rows, err := q.db.QueryContext(ctx, `SELECT worker_id, record FROM coordinator_worker_snapshots ORDER BY worker_id`)
	if err != nil {
		return nil, fmt.Errorf("load worker snapshots: %w", err)
	}
	defer rows.Close()
	var snapshots []domain.WorkerSnapshot
	for rows.Next() {
		var workerID, raw string
		if err := rows.Scan(&workerID, &raw); err != nil {
			return nil, fmt.Errorf("scan worker snapshot: %w", err)
		}
		var snapshot domain.WorkerSnapshot
		if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
			return nil, fmt.Errorf("decode worker snapshot %q: %w", workerID, err)
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate worker snapshots: %w", err)
	}
	return snapshots, nil
}

// ListBuckets returns the bucket states the coordinator host's watchdog keeps.
func (q *QueryOnlyStore) ListBuckets(ctx context.Context) ([]domain.BucketState, error) {
	rows, err := q.db.QueryContext(ctx, `SELECT state FROM buckets ORDER BY key`)
	if err != nil {
		return nil, fmt.Errorf("load buckets: %w", err)
	}
	defer rows.Close()
	var states []domain.BucketState
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan bucket: %w", err)
		}
		var state domain.BucketState
		if err := json.Unmarshal([]byte(raw), &state); err != nil {
			return nil, fmt.Errorf("decode bucket: %w", err)
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate buckets: %w", err)
	}
	return states, nil
}

// ListCheckArtifacts returns the verification and gate artifact metadata of
// one attempt, ordered by id.
func (q *QueryOnlyStore) ListCheckArtifacts(ctx context.Context, taskID, attemptID string) ([]domain.Artifact, error) {
	rows, err := q.db.QueryContext(ctx, `
		SELECT id, record FROM coordinator_artifacts
		WHERE task_id = ? AND attempt_id = ? AND json_extract(record, '$.kind') IN (?, ?)
		ORDER BY id`, taskID, attemptID, domain.ArtifactVerification, domain.ArtifactGate)
	if err != nil {
		return nil, fmt.Errorf("load check artifacts of attempt %q: %w", attemptID, err)
	}
	defer rows.Close()
	var artifacts []domain.Artifact
	for rows.Next() {
		var id, raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, fmt.Errorf("scan check artifact: %w", err)
		}
		var artifact domain.Artifact
		if err := json.Unmarshal([]byte(raw), &artifact); err != nil {
			return nil, fmt.Errorf("decode artifact %q: %w", id, err)
		}
		artifacts = append(artifacts, artifact)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate check artifacts: %w", err)
	}
	return artifacts, nil
}

// LoadArtifacts returns artifact metadata for the requested ids in request
// order, so a CoordinatorArtifactStore can read content through this
// connection.
func (q *QueryOnlyStore) LoadArtifacts(ctx context.Context, ids []string) ([]domain.Artifact, error) {
	result := make([]domain.Artifact, 0, len(ids))
	for _, id := range ids {
		var artifact domain.Artifact
		found, err := q.loadRecord(ctx, "artifact", `SELECT record FROM coordinator_artifacts WHERE id = ?`, id, &artifact)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("artifact %q not found", id)
		}
		result = append(result, artifact)
	}
	return result, nil
}
