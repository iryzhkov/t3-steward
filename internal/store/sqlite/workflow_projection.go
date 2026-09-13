package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const coordinatorMigrationV12 = `
UPDATE coordinator_workflow_runs AS run
SET record = json_set(record,
	'$.revision', revision + 1,
	'$.graphRevision', 1,
	'$.sink', json_object(
		'id', 'sink:' || id, 'name', '__sink', 'graphRevision', 1,
		'needs', json((SELECT json_group_array(id) FROM
			(SELECT id FROM coordinator_tasks WHERE workflow_id = run.workflow_id ORDER BY id))),
		'progress', 'blocked')),
	revision = revision + 1;
`

var ErrStaleWorkflowProjection = errors.New("workflow projection inputs changed")

// WorkflowProjectionSnapshot is the run-local read set fenced at publication.
// It includes the complete attempt/assignment set, so a concurrent retry, claim
// or new graph node cannot race the sink's terminal publication.
type WorkflowProjectionSnapshot struct {
	Dependencies []domain.NodeObservation
	Run          domain.WorkflowRun
	Tasks        []domain.Task
	Attempts     []domain.Attempt
	Assignments  []domain.Assignment
}

func loadWorkflowTasksTx(ctx context.Context, tx *sql.Tx, workflowID string) ([]domain.Task, error) {
	return loadProjectionRecords[domain.Task](ctx, tx,
		"SELECT record FROM coordinator_tasks WHERE workflow_id=? ORDER BY id", workflowID)
}

func loadProjectionRecords[T any](ctx context.Context, tx *sql.Tx, query string, arg string) ([]T, error) {
	rows, err := tx.QueryContext(ctx, query, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]T, 0)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var value T
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func loadWorkflowProjectionTx(ctx context.Context, tx *sql.Tx, runID string) (WorkflowProjectionSnapshot, error) {
	var snapshot WorkflowProjectionSnapshot
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_workflow_runs WHERE id=?", runID).Scan(&raw); err != nil {
		return snapshot, err
	}
	if err := json.Unmarshal(raw, &snapshot.Run); err != nil {
		return snapshot, err
	}
	var err error
	snapshot.Tasks, err = loadWorkflowTasksTx(ctx, tx, snapshot.Run.WorkflowID)
	if err != nil {
		return snapshot, err
	}
	snapshot.Tasks = domain.TasksForRun(snapshot.Run, snapshot.Tasks)
	snapshot.Attempts, err = loadProjectionRecords[domain.Attempt](ctx, tx, "SELECT record FROM coordinator_attempts WHERE workflow_run_id=? ORDER BY id", runID)
	if err != nil {
		return snapshot, err
	}
	snapshot.Assignments, err = loadProjectionRecords[domain.Assignment](ctx, tx, `SELECT assignment.record FROM coordinator_assignments AS assignment
		JOIN coordinator_attempts AS attempt ON attempt.id=assignment.attempt_id
		WHERE attempt.workflow_run_id=? ORDER BY assignment.id`, runID)
	if err != nil {
		return snapshot, err
	}
	snapshot.Dependencies, err = nodeDependenciesTx(ctx, tx, snapshot.Tasks)
	return snapshot, err
}

func projectionBytes(snapshot WorkflowProjectionSnapshot) ([]byte, error) {
	snapshot.Tasks = append([]domain.Task{}, snapshot.Tasks...)
	snapshot.Attempts = append([]domain.Attempt{}, snapshot.Attempts...)
	snapshot.Assignments = append([]domain.Assignment{}, snapshot.Assignments...)
	sort.Slice(snapshot.Tasks, func(i, j int) bool { return snapshot.Tasks[i].ID < snapshot.Tasks[j].ID })
	sort.Slice(snapshot.Attempts, func(i, j int) bool { return snapshot.Attempts[i].ID < snapshot.Attempts[j].ID })
	sort.Slice(snapshot.Assignments, func(i, j int) bool { return snapshot.Assignments[i].ID < snapshot.Assignments[j].ID })
	snapshot.Dependencies = append([]domain.NodeObservation{}, snapshot.Dependencies...)
	sort.Slice(snapshot.Dependencies, func(i, j int) bool {
		return snapshot.Dependencies[i].Target.String() < snapshot.Dependencies[j].Target.String()
	})
	return json.Marshal(snapshot)
}

// CommitWorkflowProjection publishes the run, changed attempts and at most one
// sink-settled event in one fenced transaction. No external effect occurs here.
func (s *Store) CommitWorkflowProjection(ctx context.Context, before WorkflowProjectionSnapshot, run domain.WorkflowRun, attempts []domain.Attempt, now time.Time) error {
	if run.ID != before.Run.ID || run.WorkflowID != before.Run.WorkflowID || run.Revision != before.Run.Revision+1 || now.IsZero() {
		return errors.New("invalid workflow projection identity or revision")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := loadWorkflowProjectionTx(ctx, tx, run.ID)
	if err != nil {
		return err
	}
	expected, err := projectionBytes(before)
	if err != nil {
		return err
	}
	actual, err := projectionBytes(current)
	if err != nil {
		return err
	}
	if !bytes.Equal(expected, actual) {
		return ErrStaleWorkflowProjection
	}
	if current.Run.Sink != nil && current.Run.Sink.Progress.Terminal() {
		return errors.New("completed sink is immutable")
	}
	if len(attempts) != len(current.Attempts) {
		return errors.New("projection cannot add or remove attempts")
	}
	byID := make(map[string]domain.Attempt, len(current.Attempts))
	for _, attempt := range current.Attempts {
		byID[attempt.ID] = attempt
	}
	for _, attempt := range attempts {
		prior, ok := byID[attempt.ID]
		if !ok || attempt.WorkflowRunID != run.ID || attempt.TaskID != prior.TaskID || attempt.Number != prior.Number {
			return errors.New("projection changed attempt identity")
		}
		delete(byID, attempt.ID)
		if attempt.Revision != prior.Revision && attempt.Revision != prior.Revision+1 {
			return errors.New("projection changed attempt revision incorrectly")
		}
		if attempt.Revision == prior.Revision {
			old, _ := json.Marshal(prior)
			next, _ := json.Marshal(attempt)
			if !bytes.Equal(old, next) {
				return errors.New("projection changed attempt without advancing revision")
			}
			continue
		}
		if prior.AssignmentID != "" || prior.Progress.Terminal() {
			return errors.New("projection cannot mutate assigned or terminal attempts")
		}
		if attempt.Progress != domain.ProgressReady && attempt.Progress != domain.ProgressBlocked && attempt.Progress != domain.ProgressSkipped {
			return errors.New("projection cannot synthesize an execution outcome")
		}
		allowed := prior
		allowed.Progress, allowed.Failure = attempt.Progress, attempt.Failure
		allowed.Revision, allowed.UpdatedAt = prior.Revision+1, now.UTC()
		if attempt.Progress == domain.ProgressSkipped {
			completed := now.UTC()
			allowed.Control, allowed.CompletedAt = domain.ControlStopped, &completed
		}
		if !reflect.DeepEqual(allowed, attempt) {
			return errors.New("projection changed execution fields")
		}
		if err := updateAdminAttemptTx(ctx, tx, attempt, prior.Revision); err != nil {
			return err
		}
	}
	if len(byID) != 0 {
		return errors.New("projection repeated an attempt")
	}
	// Recheck aggregation against the transaction's exact read set. Sink output
	// cannot be forged by a stale caller, even if only an assignment changed.
	verified, err := domain.ProjectRunSink(current.Run, current.Tasks, attempts, current.Assignments, now)
	if err != nil {
		return err
	}
	want, _ := json.Marshal(verified.Sink)
	got, _ := json.Marshal(run.Sink)
	if !bytes.Equal(want, got) || run.GraphRevision != verified.GraphRevision {
		return errors.New("projection sink does not match its predecessors")
	}
	if run.Sink.Progress.Terminal() {
		if run.Progress != verified.Progress || !reflect.DeepEqual(run.CompletedAt, verified.CompletedAt) {
			return errors.New("projection run outcome differs from sink result")
		}
	} else if run.Progress.Terminal() || run.CompletedAt != nil {
		return errors.New("run cannot complete before its sink")
	}
	allowedRun := current.Run
	allowedRun.Sink, allowedRun.GraphRevision = verified.Sink, verified.GraphRevision
	allowedRun.Progress, allowedRun.CompletedAt = run.Progress, run.CompletedAt
	allowedRun.Revision, allowedRun.UpdatedAt = current.Run.Revision+1, now.UTC()
	if !reflect.DeepEqual(allowedRun, run) {
		return errors.New("projection changed run definition")
	}
	raw, err := json.Marshal(run)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE coordinator_workflow_runs SET progress=?,revision=?,record=? WHERE id=? AND revision=?`,
		run.Progress, run.Revision, raw, run.ID, before.Run.Revision)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return ErrStaleWorkflowProjection
	}
	if run.Sink.Progress.Terminal() {
		// Graph references retain their source for scheduled reuse and inspection.
		detail, err := json.Marshal(run.Sink)
		if err != nil {
			return err
		}
		_, err = insertAuditEventTx(ctx, tx, domain.AuditEvent{
			ID: "sink-settled:" + run.ID, WorkflowRunID: run.ID, TaskID: run.Sink.ID,
			Kind: "sink-settled", Actor: "coordinator", Reason: "all sink predecessors are terminal and executions are quiescent",
			Detail: detail, CreatedAt: now.UTC(),
		})
		if err != nil {
			return fmt.Errorf("record sink settlement: %w", err)
		}
	}
	return tx.Commit()
}
