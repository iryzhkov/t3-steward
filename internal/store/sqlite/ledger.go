package sqlite

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// coordinatorMigrationV37 adds the coordinator's progress on each run's
// Jocasta milestone ledger. Only runs whose workflow opted in ever get a row,
// so a coordinator with no ledgered campaign keeps the table empty.
const coordinatorMigrationV37 = `
CREATE TABLE IF NOT EXISTS coordinator_ledgers (
	id TEXT PRIMARY KEY,
	record TEXT NOT NULL
);
`

// PendingLedgerWork checks for an opted-in run whose ledger can be reconciled.
// It reads only eligibility fields in SQL, without decoding coordinator records.
func (s *Store) PendingLedgerWork(ctx context.Context, now time.Time) (bool, error) {
	var pending bool
	// SQLite date functions round fractional seconds to milliseconds. Strip
	// the fraction before converting the whole second (including its zone),
	// then compare the separately padded nanoseconds to preserve time ordering.
	err := s.db.QueryRowContext(ctx, `WITH candidates AS (
		SELECT json_extract(ledger.record, '$.nextAttemptAt') AS retry_at
		FROM coordinator_workflow_runs AS run
		JOIN coordinator_workflows AS workflow ON workflow.id = run.workflow_id
		LEFT JOIN coordinator_ledgers AS ledger ON ledger.id = run.id
		WHERE json_extract(workflow.record, '$.ledger') IS NOT NULL
		  AND COALESCE(json_extract(ledger.record, '$.closed'), 0) = 0
	), retry_times AS (
		SELECT retry_at,
			unixepoch(substr(retry_at, 1, 19) ||
				CASE WHEN substr(retry_at, -1) = 'Z' THEN 'Z'
					ELSE substr(retry_at, -6) END) AS seconds,
			substr((CASE WHEN substr(retry_at, 20, 1) = '.'
				THEN substr(retry_at, 21, length(retry_at) - 20 -
					CASE WHEN substr(retry_at, -1) = 'Z' THEN 1 ELSE 6 END)
				ELSE '' END) || '000000000', 1, 9) AS nanos
		FROM candidates
	)
	SELECT EXISTS (
		SELECT 1 FROM retry_times
		WHERE retry_at IS NULL OR seconds < ? OR (seconds = ? AND nanos <= ?)
	)`, now.Unix(), now.Unix(), fmt.Sprintf("%09d", now.Nanosecond())).Scan(&pending)
	return pending, err
}

// LoadLedgerStates returns every run's ledger progress, ordered by run.
func (s *Store) LoadLedgerStates(ctx context.Context) ([]domain.LedgerState, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	states, err := loadJSON[domain.LedgerState](ctx, tx, "coordinator_ledgers")
	if err != nil {
		return nil, err
	}
	return states, tx.Commit()
}

// SaveLedgerState records one run's ledger progress, replacing the previous
// record. The coordinator's ledger reconciler is the only writer.
func (s *Store) SaveLedgerState(ctx context.Context, state domain.LedgerState) error {
	if state.RunID == "" {
		return errors.New("save ledger state: run id is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := upsertJSON(ctx, tx, "ledger state", state.RunID,
		`INSERT INTO coordinator_ledgers(id, record) VALUES (?, ?)
		 ON CONFLICT(id) DO UPDATE SET record = excluded.record`,
		[]any{state.RunID}, state); err != nil {
		return err
	}
	return tx.Commit()
}
