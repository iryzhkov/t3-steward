package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ErrStaleTurnOutcomeAttemptRevision reports a concurrent canonical attempt update.
var ErrStaleTurnOutcomeAttemptRevision = errors.New("stale turn outcome attempt revision")

// LoadTurnOutcomeState returns one transactionally consistent attempt/throttle snapshot.
func (s *Store) LoadTurnOutcomeState(ctx context.Context) (
	[]domain.Attempt, []domain.ThrottleAttemptRecord, error,
) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, fmt.Errorf("begin turn outcome load: %w", err)
	}
	defer tx.Rollback()

	attempts, err := loadJSON[domain.Attempt](ctx, tx, "coordinator_attempts")
	if err != nil {
		return nil, nil, err
	}
	rows, err := tx.QueryContext(ctx,
		`SELECT record FROM coordinator_throttle_attempts ORDER BY directive_id, attempt_id`)
	if err != nil {
		return nil, nil, fmt.Errorf("load turn outcome throttle records: %w", err)
	}
	var throttle []domain.ThrottleAttemptRecord
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("scan turn outcome throttle record: %w", err)
		}
		var record domain.ThrottleAttemptRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("decode turn outcome throttle record: %w", err)
		}
		throttle = append(throttle, record)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, fmt.Errorf("close turn outcome throttle rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate turn outcome throttle records: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("commit turn outcome load: %w", err)
	}
	return attempts, throttle, nil
}

// CommitTurnOutcomeTransitions atomically updates canonical attempts and their
// active throttle projections with optimistic revisions. Exact replay is a no-op.
func (s *Store) CommitTurnOutcomeTransitions(
	ctx context.Context,
	input []domain.TurnOutcomeTransition,
) error {
	transitions := append([]domain.TurnOutcomeTransition(nil), input...)
	sort.Slice(transitions, func(i, j int) bool {
		return transitions[i].Attempt.ID < transitions[j].Attempt.ID
	})
	for index, transition := range transitions {
		if transition.OutcomeID == "" || transition.Attempt.ID == "" ||
			transition.Attempt.LastTurnOutcomeID != transition.OutcomeID {
			return fmt.Errorf("turn outcome transition has invalid identity")
		}
		if transition.ExpectedAttemptRevision < 0 ||
			transition.Attempt.Revision != transition.ExpectedAttemptRevision+1 {
			return fmt.Errorf("turn outcome transition %q has invalid attempt revision %d after %d",
				transition.OutcomeID, transition.Attempt.Revision, transition.ExpectedAttemptRevision)
		}
		if index > 0 && transitions[index-1].Attempt.ID == transition.Attempt.ID {
			return fmt.Errorf("turn outcome transitions repeat attempt %q", transition.Attempt.ID)
		}
		if transition.Throttle != nil &&
			transition.Throttle.Record.AttemptID != transition.Attempt.ID {
			return fmt.Errorf("turn outcome transition %q has mismatched throttle attempt", transition.OutcomeID)
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin turn outcome transitions: %w", err)
	}
	defer tx.Rollback()

	for _, transition := range transitions {
		if _, err := compareTurnOutcomeAttempt(ctx, tx, transition); err != nil {
			return err
		}
		if transition.Throttle != nil {
			if _, err := compareThrottleAttemptTransition(ctx, tx, *transition.Throttle); err != nil {
				return err
			}
		}
	}
	for _, transition := range transitions {
		if err := upsertJSON(ctx, tx, "attempt", transition.Attempt.ID,
			`INSERT INTO coordinator_attempts(id, workflow_run_id, task_id, number, revision, record)
			 VALUES (?, ?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET workflow_run_id = excluded.workflow_run_id,
			 task_id = excluded.task_id, number = excluded.number,
			 revision = excluded.revision, record = excluded.record`,
			[]any{
				transition.Attempt.ID, transition.Attempt.WorkflowRunID,
				transition.Attempt.TaskID, transition.Attempt.Number, transition.Attempt.Revision,
			},
			transition.Attempt,
		); err != nil {
			return err
		}
		if transition.Throttle == nil {
			continue
		}
		record := transition.Throttle.Record
		raw, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode turn outcome throttle record %q/%q: %w",
				record.DirectiveID, record.AttemptID, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO coordinator_throttle_attempts(directive_id, attempt_id, revision, delivery, record)
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(directive_id, attempt_id) DO UPDATE SET
			 revision = excluded.revision, delivery = excluded.delivery, record = excluded.record`,
			record.DirectiveID, record.AttemptID, record.Revision, record.Delivery, raw,
		); err != nil {
			return fmt.Errorf("save turn outcome throttle record %q/%q: %w",
				record.DirectiveID, record.AttemptID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit turn outcome transitions: %w", err)
	}
	return nil
}

func compareTurnOutcomeAttempt(
	ctx context.Context,
	tx *sql.Tx,
	transition domain.TurnOutcomeTransition,
) (bool, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx,
		`SELECT record FROM coordinator_attempts WHERE id = ?`,
		transition.Attempt.ID,
	).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, staleTurnOutcomeAttemptError(transition, 0)
	}
	if err != nil {
		return false, fmt.Errorf("load turn outcome attempt %q: %w", transition.Attempt.ID, err)
	}
	var existing domain.Attempt
	if err := json.Unmarshal(raw, &existing); err != nil {
		return false, fmt.Errorf("decode turn outcome attempt %q: %w", transition.Attempt.ID, err)
	}
	if reflect.DeepEqual(existing, transition.Attempt) {
		return true, nil
	}
	if existing.Revision != transition.ExpectedAttemptRevision {
		return false, staleTurnOutcomeAttemptError(transition, existing.Revision)
	}
	return false, nil
}

func staleTurnOutcomeAttemptError(transition domain.TurnOutcomeTransition, actual int64) error {
	return fmt.Errorf("%w for attempt %q outcome %q: expected %d, current %d",
		ErrStaleTurnOutcomeAttemptRevision, transition.Attempt.ID, transition.OutcomeID,
		transition.ExpectedAttemptRevision, actual)
}
