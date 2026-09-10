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

const coordinatorMigrationV4 = `
CREATE TABLE coordinator_throttle_attempts (
	directive_id TEXT NOT NULL,
	attempt_id TEXT NOT NULL,
	revision INTEGER NOT NULL,
	delivery TEXT NOT NULL,
	record TEXT NOT NULL,
	PRIMARY KEY(directive_id, attempt_id)
);
CREATE INDEX coordinator_throttle_attempts_delivery
	ON coordinator_throttle_attempts(delivery, directive_id, attempt_id);
`

// ErrStaleThrottleAttemptRevision means an affected-attempt record changed
// after the coordinator loaded it.
var ErrStaleThrottleAttemptRevision = errors.New("stale throttle attempt revision")

// LoadThrottleAttemptRecords returns affected-attempt records in canonical
// directive/attempt order.
func (s *Store) LoadThrottleAttemptRecords(ctx context.Context) ([]domain.ThrottleAttemptRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT record FROM coordinator_throttle_attempts ORDER BY directive_id, attempt_id`)
	if err != nil {
		return nil, fmt.Errorf("query throttle attempt records: %w", err)
	}
	defer rows.Close()

	var records []domain.ThrottleAttemptRecord
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan throttle attempt record: %w", err)
		}
		var record domain.ThrottleAttemptRecord
		if err := json.Unmarshal([]byte(raw), &record); err != nil {
			return nil, fmt.Errorf("decode throttle attempt record: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate throttle attempt records: %w", err)
	}
	return records, nil
}

// CommitThrottleAttemptTransitions commits a whole affected-attempt batch with
// optimistic revisions. Exact replay is a successful no-op.
func (s *Store) CommitThrottleAttemptTransitions(ctx context.Context, input []domain.ThrottleAttemptTransition) error {
	transitions := append([]domain.ThrottleAttemptTransition(nil), input...)
	sort.Slice(transitions, func(i, j int) bool {
		left := transitions[i].Record.DirectiveID + "\x00" + transitions[i].Record.AttemptID
		right := transitions[j].Record.DirectiveID + "\x00" + transitions[j].Record.AttemptID
		return left < right
	})
	for index, transition := range transitions {
		record := transition.Record
		if record.DirectiveID == "" || record.AttemptID == "" || record.Command.ID == "" ||
			record.Command.DirectiveID != record.DirectiveID || record.Command.AttemptID != record.AttemptID {
			return fmt.Errorf("throttle attempt transition has invalid identity")
		}
		if transition.ExpectedRevision < 0 || record.Revision != transition.ExpectedRevision+1 {
			return fmt.Errorf("throttle attempt transition %q/%q has invalid revision %d after %d",
				record.DirectiveID, record.AttemptID, record.Revision, transition.ExpectedRevision)
		}
		if index > 0 {
			previous := transitions[index-1].Record
			if previous.DirectiveID == record.DirectiveID && previous.AttemptID == record.AttemptID {
				return fmt.Errorf("throttle attempt transitions repeat directive %q attempt %q",
					record.DirectiveID, record.AttemptID)
			}
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin throttle attempt transitions: %w", err)
	}
	defer tx.Rollback()
	for _, transition := range transitions {
		replayed, err := compareThrottleAttemptTransition(ctx, tx, transition)
		if err != nil {
			return err
		}
		if replayed {
			continue
		}
		raw, err := json.Marshal(transition.Record)
		if err != nil {
			return fmt.Errorf("encode throttle attempt record %q/%q: %w",
				transition.Record.DirectiveID, transition.Record.AttemptID, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO coordinator_throttle_attempts(directive_id, attempt_id, revision, delivery, record)
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(directive_id, attempt_id) DO UPDATE SET
			 revision = excluded.revision, delivery = excluded.delivery, record = excluded.record`,
			transition.Record.DirectiveID, transition.Record.AttemptID,
			transition.Record.Revision, transition.Record.Delivery, raw,
		); err != nil {
			return fmt.Errorf("save throttle attempt record %q/%q: %w",
				transition.Record.DirectiveID, transition.Record.AttemptID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit throttle attempt transitions: %w", err)
	}
	return nil
}

func compareThrottleAttemptTransition(
	ctx context.Context,
	tx *sql.Tx,
	transition domain.ThrottleAttemptTransition,
) (bool, error) {
	var raw string
	err := tx.QueryRowContext(ctx,
		`SELECT record FROM coordinator_throttle_attempts WHERE directive_id = ? AND attempt_id = ?`,
		transition.Record.DirectiveID, transition.Record.AttemptID,
	).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		if transition.ExpectedRevision != 0 {
			return false, staleThrottleAttemptError(transition, 0)
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load throttle attempt record %q/%q: %w",
			transition.Record.DirectiveID, transition.Record.AttemptID, err)
	}
	var existing domain.ThrottleAttemptRecord
	if err := json.Unmarshal([]byte(raw), &existing); err != nil {
		return false, fmt.Errorf("decode throttle attempt record %q/%q: %w",
			transition.Record.DirectiveID, transition.Record.AttemptID, err)
	}
	if reflect.DeepEqual(existing, transition.Record) {
		return true, nil
	}
	if existing.Revision != transition.ExpectedRevision {
		return false, staleThrottleAttemptError(transition, existing.Revision)
	}
	return false, nil
}

func staleThrottleAttemptError(transition domain.ThrottleAttemptTransition, actual int64) error {
	return fmt.Errorf("%w for directive %q attempt %q: expected %d, current %d",
		ErrStaleThrottleAttemptRevision, transition.Record.DirectiveID,
		transition.Record.AttemptID, transition.ExpectedRevision, actual)
}
