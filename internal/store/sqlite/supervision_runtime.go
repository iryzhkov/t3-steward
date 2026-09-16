package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The supervision inbox, outbox and activation rows as the store carries them.
//
// The lifecycle that decides what goes in them lives in internal/backlog, which
// imports this package; this package therefore cannot name that package's
// types. So the payload travels as the encoded record it is already stored as,
// and only the columns the store itself orders, fences or deduplicates on are
// typed here. The binding in internal/backlog decodes them.

// SupervisionInboxRow is one durable supervision event.
type SupervisionInboxRow struct {
	ID       string          `json:"id"`
	RunID    string          `json:"runId"`
	Sequence int64           `json:"sequence"`
	Consumed bool            `json:"consumed"`
	Record   json.RawMessage `json:"record"`
}

// SupervisionOutboxRow is one durable delivery intent.
type SupervisionOutboxRow struct {
	ID           string          `json:"id"`
	RunID        string          `json:"runId"`
	ActivationID string          `json:"activationId,omitempty"`
	Delivery     string          `json:"delivery"`
	Record       json.RawMessage `json:"record"`
}

// SupervisionActivationRows is everything one activation decision reads.
type SupervisionActivationRows struct {
	Supervised bool
	Record     domain.SupervisionRecord
	Activation domain.Activation
	// OtherValidActivation reports a second activation of this run in a live
	// state. At most one may be valid, and the domain machine refuses a second.
	OtherValidActivation bool
	Inbox                []SupervisionInboxRow
	Outbox               []SupervisionOutboxRow
}

// SupervisionActivationRowCommit is one whole activation plan, written under the
// record revision the plan read.
type SupervisionActivationRowCommit struct {
	RunID                  string
	ExpectedRecordRevision int64
	Record                 domain.SupervisionRecord
	Activation             domain.Activation
	ConsumedThrough        int64
	CursorAdvanced         bool
	Outbox                 []SupervisionOutboxRow
	// Receipt is an encoded operator continuation receipt, when the plan issued
	// one. It is stored beside the idempotency receipts, because it answers the
	// same question: what did this request key already do.
	Receipt     json.RawMessage
	RequestID   string
	CommittedAt time.Time
}

// AppendSupervisionInbox appends observed supervision events and assigns their
// run-local sequences. An event ID already present is ignored rather than
// resequenced, so at-least-once delivery adds nothing and wakes nobody twice.
// It reports how many rows it actually wrote.
func (s *Store) AppendSupervisionInbox(ctx context.Context, runID string, rows []SupervisionInboxRow) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin supervision inbox append: %w", err)
	}
	defer tx.Rollback()
	var next sql.NullInt64
	if err := tx.QueryRowContext(ctx,
		"SELECT MAX(sequence) FROM coordinator_supervision_inbox WHERE run_id = ?", runID).Scan(&next); err != nil {
		return 0, fmt.Errorf("read supervision inbox high-water mark of run %q: %w", runID, err)
	}
	sequence := next.Int64
	written := 0
	for _, row := range rows {
		var present int
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM coordinator_supervision_inbox WHERE id = ?", row.ID).Scan(&present); err != nil {
			return 0, err
		}
		if present > 0 {
			continue
		}
		sequence++
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO coordinator_supervision_inbox(id, run_id, sequence, consumed, record) VALUES (?, ?, ?, 0, ?)",
			row.ID, runID, sequence, []byte(row.Record)); err != nil {
			return 0, fmt.Errorf("append supervision event %q: %w", row.ID, err)
		}
		written++
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit supervision inbox append: %w", err)
	}
	return written, nil
}

// ListSupervisionInbox returns one run's events, oldest first, consumed and
// pending alike: an activation needs to know what it already consumed as well
// as what is waiting.
func (s *Store) ListSupervisionInbox(ctx context.Context, runID string) ([]SupervisionInboxRow, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := supervisionInboxRowsTx(ctx, tx, runID)
	if err != nil {
		return nil, err
	}
	return rows, tx.Commit()
}

func supervisionInboxRowsTx(ctx context.Context, tx *sql.Tx, runID string) ([]SupervisionInboxRow, error) {
	cursor, err := tx.QueryContext(ctx,
		"SELECT id, sequence, consumed, record FROM coordinator_supervision_inbox WHERE run_id = ? ORDER BY sequence", runID)
	if err != nil {
		return nil, fmt.Errorf("list supervision inbox of run %q: %w", runID, err)
	}
	defer cursor.Close()
	var rows []SupervisionInboxRow
	for cursor.Next() {
		row := SupervisionInboxRow{RunID: runID}
		var consumed int
		var raw []byte
		if err := cursor.Scan(&row.ID, &row.Sequence, &consumed, &raw); err != nil {
			return nil, err
		}
		row.Consumed = consumed != 0
		row.Record = append(json.RawMessage(nil), raw...)
		rows = append(rows, row)
	}
	return rows, cursor.Err()
}

// AppendSupervisionOutboxRows persists delivery intents. An ID already present
// is left alone: the entry ID is deterministic in the thing that caused it, so
// a repeated cause is the same intent rather than a second delivery.
func (s *Store) AppendSupervisionOutboxRows(ctx context.Context, runID string, rows []SupervisionOutboxRow) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin supervision outbox append: %w", err)
	}
	defer tx.Rollback()
	written, err := appendSupervisionOutboxTx(ctx, tx, runID, rows)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit supervision outbox append: %w", err)
	}
	return written, nil
}

func appendSupervisionOutboxTx(ctx context.Context, tx *sql.Tx, runID string, rows []SupervisionOutboxRow) (int, error) {
	written := 0
	for _, row := range rows {
		result, err := tx.ExecContext(ctx, `
			INSERT INTO coordinator_supervision_outbox(id, run_id, activation_id, delivery_state, record)
			VALUES (?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
			row.ID, runID, row.ActivationID, row.Delivery, []byte(row.Record))
		if err != nil {
			return 0, fmt.Errorf("append supervision outbox entry %q: %w", row.ID, err)
		}
		if changed, _ := result.RowsAffected(); changed == 1 {
			written++
		}
	}
	return written, nil
}

// ListSupervisionOutboxRows returns one run's delivery intents, oldest first.
func (s *Store) ListSupervisionOutboxRows(ctx context.Context, runID string) ([]SupervisionOutboxRow, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := supervisionOutboxRowsTx(ctx, tx, runID)
	if err != nil {
		return nil, err
	}
	return rows, tx.Commit()
}

func supervisionOutboxRowsTx(ctx context.Context, tx *sql.Tx, runID string) ([]SupervisionOutboxRow, error) {
	cursor, err := tx.QueryContext(ctx,
		"SELECT id, activation_id, delivery_state, record FROM coordinator_supervision_outbox WHERE run_id = ? ORDER BY id", runID)
	if err != nil {
		return nil, fmt.Errorf("list supervision outbox of run %q: %w", runID, err)
	}
	defer cursor.Close()
	var rows []SupervisionOutboxRow
	for cursor.Next() {
		row := SupervisionOutboxRow{RunID: runID}
		var raw []byte
		if err := cursor.Scan(&row.ID, &row.ActivationID, &row.Delivery, &raw); err != nil {
			return nil, err
		}
		row.Record = append(json.RawMessage(nil), raw...)
		rows = append(rows, row)
	}
	return rows, cursor.Err()
}

// TransitionSupervisionOutboxRow fences delivery ownership with a compare and
// set on the current delivery state, exactly as TransitionNodeWake does. It
// reports whether this caller won the transition.
func (s *Store) TransitionSupervisionOutboxRow(ctx context.Context, id, from, to string, now time.Time) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var raw []byte
	err = tx.QueryRowContext(ctx,
		"SELECT record FROM coordinator_supervision_outbox WHERE id = ? AND delivery_state = ?", id, from).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load supervision outbox entry %q: %w", id, err)
	}
	var entry map[string]any
	if err := json.Unmarshal(raw, &entry); err != nil {
		return false, fmt.Errorf("decode supervision outbox entry %q: %w", id, err)
	}
	entry["delivery"] = to
	if to == "sending" {
		attempts, _ := entry["attempts"].(float64)
		entry["attempts"] = attempts + 1
	}
	if to == "delivered" {
		entry["deliveredAt"] = now.UTC()
	}
	updated, err := json.Marshal(entry)
	if err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx,
		"UPDATE coordinator_supervision_outbox SET delivery_state = ?, record = ? WHERE id = ? AND delivery_state = ?",
		to, updated, id, from)
	if err != nil {
		return false, fmt.Errorf("transition supervision outbox entry %q: %w", id, err)
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		return false, nil
	}
	return true, tx.Commit()
}

// PendingSupervisionEscalations lists every escalation of every run that has
// not been delivered yet, across the whole coordinator, so one delivery loop
// can drain them the way the node-wait loop drains wakes.
//
// A delivered or cancelled entry is not returned: an escalation is deduplicated
// on its incident, so a re-escalation of the same incident is the same entry and
// is never sent twice.
func (s *Store) PendingSupervisionEscalations(ctx context.Context) ([]domain.SupervisionEscalationDelivery, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, run_id, delivery_state, record FROM coordinator_supervision_outbox
		WHERE delivery_state IN ('pending', 'sending', 'recovery-required') ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list pending supervision escalations: %w", err)
	}
	defer rows.Close()
	var pending []domain.SupervisionEscalationDelivery
	for rows.Next() {
		var id, runID, delivery string
		var raw []byte
		if err := rows.Scan(&id, &runID, &delivery, &raw); err != nil {
			return nil, err
		}
		var entry struct {
			Kind       string `json:"kind"`
			IncidentID string `json:"incidentId"`
			ThreadID   string `json:"threadId"`
			Reason     string `json:"reason"`
			Attempts   int    `json:"attempts"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, fmt.Errorf("decode supervision outbox entry %q: %w", id, err)
		}
		if entry.Kind != "escalation" || entry.ThreadID == "" {
			// A wake is delivered by dispatching the activation as assigned work,
			// not by messaging a thread.
			continue
		}
		pending = append(pending, domain.SupervisionEscalationDelivery{
			ID: id, DeliveryID: id, RunID: runID, IncidentID: entry.IncidentID,
			ThreadID: entry.ThreadID, Reason: entry.Reason, Delivery: delivery, Attempts: entry.Attempts,
		})
	}
	return pending, rows.Err()
}

// LoadSupervisionActivationRows reads one run's activation decision state. An
// unsupervised run reports Supervised false, because absence of the record is
// the unsupervised case rather than an empty one.
func (s *Store) LoadSupervisionActivationRows(ctx context.Context, runID string) (SupervisionActivationRows, error) {
	var state SupervisionActivationRows
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return state, err
	}
	defer tx.Rollback()
	context_, supervised, err := supervisionContextTx(ctx, tx, runID, false)
	if err != nil || !supervised {
		return state, err
	}
	state.Supervised = true
	state.Record = context_.Record
	activations, err := loadSupervisionActivationsTx(ctx, tx, runID)
	if err != nil {
		return SupervisionActivationRows{}, err
	}
	for _, candidate := range activations {
		if candidate.Epoch == state.Record.ActivationEpoch {
			state.Activation = candidate
			continue
		}
		if candidate.State == domain.ActivationPendingDispatch || candidate.State == domain.ActivationActive {
			state.OtherValidActivation = true
		}
	}
	if state.Inbox, err = supervisionInboxRowsTx(ctx, tx, runID); err != nil {
		return SupervisionActivationRows{}, err
	}
	if state.Outbox, err = supervisionOutboxRowsTx(ctx, tx, runID); err != nil {
		return SupervisionActivationRows{}, err
	}
	return state, tx.Commit()
}

// CommitSupervisionActivationRows writes one activation plan atomically under
// the record revision the plan read. The consumed high-water mark, the outcome,
// the counters and the delivery intents land together or not at all.
func (s *Store) CommitSupervisionActivationRows(ctx context.Context, commit SupervisionActivationRowCommit) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin supervision activation commit: %w", err)
	}
	defer tx.Rollback()
	current, found, err := loadSupervisionRecordTx(ctx, tx, commit.RunID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: run %q has no supervision record", ErrSupervisionRecordNotFound, commit.RunID)
	}
	if current.Revision != commit.ExpectedRecordRevision {
		return fmt.Errorf("%w: plan read record revision %d, the record is at %d",
			domain.ErrSupervisionStaleRevision, commit.ExpectedRecordRevision, current.Revision)
	}
	now := commit.CommittedAt.UTC()
	if now.IsZero() {
		now = s.now().UTC()
	}
	record := commit.Record
	record.RunID = commit.RunID
	record.Revision = current.Revision + 1
	if record.CreatedAt.IsZero() {
		record.CreatedAt = current.CreatedAt
	}
	record.UpdatedAt = now
	if err := saveSupervisionRecordTx(ctx, tx, record); err != nil {
		return err
	}
	if commit.Activation.ID != "" {
		activation := commit.Activation
		activation.RunID = commit.RunID
		if err := saveSupervisionActivationTx(ctx, tx, activation); err != nil {
			return err
		}
	}
	if commit.CursorAdvanced && commit.ConsumedThrough > 0 {
		if _, err := tx.ExecContext(ctx,
			"UPDATE coordinator_supervision_inbox SET consumed = 1 WHERE run_id = ? AND sequence <= ?",
			commit.RunID, commit.ConsumedThrough); err != nil {
			return fmt.Errorf("consume supervision inbox of run %q: %w", commit.RunID, err)
		}
	}
	if _, err := appendSupervisionOutboxTx(ctx, tx, commit.RunID, commit.Outbox); err != nil {
		return err
	}
	if len(commit.Receipt) > 0 && commit.RequestID != "" {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO coordinator_supervision_receipts(request_id, operation, run_id, payload_sha256, answer, recorded_at)
			VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(request_id) DO NOTHING`,
			"continuation:"+commit.RequestID, "activation-continuation", commit.RunID, "",
			[]byte(commit.Receipt), now.Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("record continuation receipt %q: %w", commit.RequestID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit supervision activation: %w", err)
	}
	return nil
}
