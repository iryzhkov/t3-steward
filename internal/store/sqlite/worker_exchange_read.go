package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const workerAssignmentsQuery = `SELECT record FROM coordinator_assignments
 WHERE json_extract(record, '$.workerId') = ? ORDER BY id`
const workerAttemptsQuery = `SELECT record FROM coordinator_attempts WHERE id IN
 (SELECT attempt_id FROM coordinator_assignments WHERE json_extract(record, '$.workerId') = ?) ORDER BY id`
const workerRunsQuery = `SELECT DISTINCT a.workflow_run_id FROM coordinator_attempts a
 JOIN coordinator_assignments x ON x.attempt_id = a.id WHERE json_extract(x.record, '$.workerId') = ?
 AND (json_extract(x.record, '$.state') IN ('claimed', 'unknown') OR (json_extract(x.record, '$.state') IN ('completed', 'released') AND julianday(json_extract(x.record, '$.updatedAt')) >= julianday('now', '-1 day')))`

func (s *Store) LoadWorkerSnapshot(ctx context.Context, workerID string) (domain.WorkerSnapshot, bool, error) {
	if workerID == "" {
		return domain.WorkerSnapshot{}, false, errors.New("worker identity is required")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return domain.WorkerSnapshot{}, false, err
	}
	defer tx.Rollback()
	snapshot, found, err := loadWorkerSnapshotTx(ctx, tx, workerID)
	if err != nil {
		return domain.WorkerSnapshot{}, false, err
	}
	return snapshot, found, tx.Commit()
}

// LoadWorkerExchangeRecords: fresh owned records; titles add referenced runs'
// tasks/latest attempts for cross-worker progress. No artifacts or lazy loaders.
func (s *Store) LoadWorkerExchangeRecords(ctx context.Context, workerID string, titles bool) (CoordinatorRecords, error) {
	var r CoordinatorRecords
	if workerID == "" {
		return r, errors.New("worker identity is required")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return r, err
	}
	defer tx.Rollback()
	if r.Assignments, err = loadJSONQuery[domain.Assignment](ctx, tx, "coordinator_assignments", workerAssignmentsQuery, workerID); err != nil {
		return r, err
	}
	query := workerAttemptsQuery
	args := []any{workerID}
	if titles {
		query = `SELECT record FROM coordinator_attempts WHERE id IN
 (SELECT attempt_id FROM coordinator_assignments WHERE json_extract(record, '$.workerId') = ?)
 OR id IN (SELECT id FROM coordinator_attempts WHERE workflow_run_id IN (` + workerRunsQuery + `)
 AND COALESCE(json_extract(record, '$.supervisionActivationId'), '') = '' GROUP BY workflow_run_id, task_id HAVING number = MAX(number)) ORDER BY id`
		args = append(args, workerID)
		if r.WorkflowRuns, err = loadJSONQuery[domain.WorkflowRun](ctx, tx, "coordinator_workflow_runs",
			"SELECT record FROM coordinator_workflow_runs WHERE id IN ("+workerRunsQuery+") ORDER BY id", workerID); err != nil {
			return r, err
		}
		if r.Tasks, err = loadJSONQuery[domain.Task](ctx, tx, "coordinator_tasks",
			"SELECT t.record FROM coordinator_tasks t WHERE EXISTS (SELECT 1 FROM coordinator_workflow_runs r WHERE r.id IN ("+workerRunsQuery+") AND r.workflow_id = t.workflow_id AND COALESCE(json_extract(t.record, '$.runId'), '') IN ('', r.id)) ORDER BY t.id", workerID); err != nil {
			return r, err
		}
	}
	if r.Attempts, err = loadJSONQuery[domain.Attempt](ctx, tx, "coordinator_attempts", query, args...); err != nil {
		return r, err
	}
	return r, tx.Commit()
}

// LoadRetainedCampaignRuns: global IDs without payloads; graphs supersede templates.
// Unsettled sinks protect publication; output provenance protects retained commits.
func (s *Store) LoadRetainedCampaignRuns(ctx context.Context) ([]string, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `WITH declared AS (
 SELECT r.id AS run_id, json_extract(t.value, '$.id') AS task_id,
 json_extract(o.value, '$.name') AS name
 FROM coordinator_workflow_runs r, json_each(r.record, '$.graph.tasks') t,
 json_each(t.value, '$.outputs') o WHERE json_type(o.value, '$.commit') = 'object'
 UNION
 SELECT r.id, t.id, json_extract(o.value, '$.name')
 FROM coordinator_workflow_runs r JOIN coordinator_tasks t ON t.workflow_id = r.workflow_id,
 json_each(t.record, '$.outputs') o
 WHERE json_type(r.record, '$.graph') IS NOT 'object'
 AND COALESCE(json_extract(t.record, '$.runId'), '') IN ('', r.id)
 AND json_type(o.value, '$.commit') = 'object'
 )
 SELECT r.id FROM coordinator_workflow_runs r
 WHERE EXISTS (SELECT 1 FROM declared d WHERE d.run_id = r.id)
 AND (COALESCE(json_extract(r.record, '$.sink.progress'), '') NOT IN ('succeeded', 'failed', 'cancelled', 'skipped')
 OR EXISTS (SELECT 1 FROM coordinator_artifacts a JOIN declared d
 ON d.run_id = a.workflow_run_id AND d.task_id = a.task_id AND d.name = json_extract(a.record, '$.name')
 WHERE a.workflow_run_id = r.id AND json_extract(a.record, '$.kind') = 'output'))
 ORDER BY r.id`)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, tx.Commit()
}
