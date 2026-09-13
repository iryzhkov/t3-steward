package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func ensureInitialGraphsTx(ctx context.Context, tx *sql.Tx) error {
	records, err := nodeRecordsTx(ctx, tx)
	if err != nil {
		return err
	}
	for _, run := range records.WorkflowRuns {
		if run.Sink == nil || run.Graph != nil || run.GraphRevision < 1 {
			continue
		}
		tasks := domain.TasksForRun(run, records.Tasks)
		graph := domain.GraphDefinition{RunID: run.ID, Revision: run.GraphRevision, Actor: "submission", Reason: "original retained definition", RequestID: "initial:" + run.ID, CreatedAt: run.CreatedAt, Tasks: tasks, Digest: domain.GraphDigest(tasks)}
		raw, err := json.Marshal(graph)
		if err != nil {
			return err
		}
		// An existing immutable snapshot is never rewritten by projection saves.
		if _, err = tx.ExecContext(ctx, "INSERT INTO coordinator_graph_revisions(run_id,revision,record) VALUES(?,?,?) ON CONFLICT(run_id,revision) DO NOTHING", graph.RunID, graph.Revision, string(raw)); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) backfillGraphHistory(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = ensureInitialGraphsTx(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) LoadGraphRevisions(ctx context.Context, runID string) ([]domain.GraphDefinition, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	graphs, err := loadProjectionRecords[domain.GraphDefinition](ctx, tx, "SELECT record FROM coordinator_graph_revisions WHERE run_id=? ORDER BY revision", runID)
	if err != nil {
		return nil, err
	}
	return graphs, tx.Commit()
}
