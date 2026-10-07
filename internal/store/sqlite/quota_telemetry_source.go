package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
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

// LoadAssignmentEpoch reads the assignment as it was frozen when it was
// dispatched at one epoch, from the continuation binding recorded with that
// dispatch. An assignment offered again keeps its id and moves to a later
// epoch, possibly on another worker and route, so the current record no
// longer describes an earlier epoch. found is false when no binding was
// frozen for the epoch.
func (q *QueryOnlyStore) LoadAssignmentEpoch(ctx context.Context, id string, epoch int64) (domain.Assignment, bool, error) {
	var raw string
	err := q.db.QueryRowContext(ctx,
		`SELECT binding FROM coordinator_assignment_continuations WHERE assignment_id = ? AND assignment_epoch = ?`,
		id, epoch).Scan(&raw)
	// A database from before the continuation table has no frozen epochs.
	if errors.Is(err, sql.ErrNoRows) || (err != nil && strings.Contains(err.Error(), "no such table")) {
		return domain.Assignment{}, false, nil
	}
	if err != nil {
		return domain.Assignment{}, false, fmt.Errorf("load assignment %q at epoch %d: %w", id, epoch, err)
	}
	var frozen struct {
		Assignment domain.Assignment
	}
	if err := json.Unmarshal([]byte(raw), &frozen); err != nil {
		return domain.Assignment{}, false, &MalformedRecordError{Label: "assignment binding", ID: id, Err: err}
	}
	if frozen.Assignment.ID != id || frozen.Assignment.Epoch != epoch {
		return domain.Assignment{}, false, &MalformedRecordError{Label: "assignment binding", ID: id,
			Err: errors.New("binding does not describe this epoch")}
	}
	return frozen.Assignment, true, nil
}

// MalformedRecordError is a coordinator row that exists and does not decode.
// A reader that skips one bad row rather than stopping recognises it by its
// Malformed method.
type MalformedRecordError struct {
	Label string
	ID    string
	Err   error
}

func (e *MalformedRecordError) Error() string {
	return fmt.Sprintf("decode %s %q: %v", e.Label, e.ID, e.Err)
}

func (e *MalformedRecordError) Unwrap() error { return e.Err }

// Malformed marks the error as one bad row rather than an unreadable database.
func (e *MalformedRecordError) Malformed() bool { return true }

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
		return false, &MalformedRecordError{Label: label, ID: id, Err: err}
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
			return nil, &MalformedRecordError{Label: "artifact", ID: id, Err: err}
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
