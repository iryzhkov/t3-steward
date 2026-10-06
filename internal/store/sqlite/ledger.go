package sqlite

import (
	"context"
	"errors"

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
