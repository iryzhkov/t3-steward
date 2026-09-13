package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func bindAssignmentGraphTx(ctx context.Context, tx *sql.Tx, attempt domain.Attempt, assignment *domain.Assignment) error {
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_workflow_runs WHERE id=?", attempt.WorkflowRunID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	} // old imported records may lack a run
	if err != nil {
		return err
	}
	var run domain.WorkflowRun
	if err = json.Unmarshal(raw, &run); err != nil {
		return err
	}
	tasks, err := loadWorkflowTasksTx(ctx, tx, run.WorkflowID)
	if err != nil {
		return err
	}
	for _, task := range domain.TasksForRun(run, tasks) {
		if task.ID == attempt.TaskID {
			assignment.GraphRevision = run.GraphRevision
			assignment.TaskRevision = task.DefinitionRevision
			assignment.TaskDigest = domain.TaskDigest(task)
			return nil
		}
	}
	return errors.New("assignment task is absent from current graph")
}
