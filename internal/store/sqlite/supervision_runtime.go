package sqlite

import (
	"context"
	"crypto/sha256"
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
	ID                   string          `json:"id"`
	RunID                string          `json:"runId"`
	Sequence             int64           `json:"sequence"`
	Consumed             bool            `json:"consumed"`
	AcknowledgedPurposes []string        `json:"acknowledgedPurposes,omitempty"`
	Record               json.RawMessage `json:"record"`
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
	AcknowledgedEventIDs   []string
	AcknowledgementPurpose string
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
	if err := loadSupervisionInboxAcknowledgementsTx(ctx, tx, runID, rows); err != nil {
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

func loadSupervisionInboxAcknowledgementsTx(ctx context.Context, tx *sql.Tx, runID string, inbox []SupervisionInboxRow) error {
	byID := make(map[string]*SupervisionInboxRow, len(inbox))
	for i := range inbox {
		byID[inbox[i].ID] = &inbox[i]
	}
	rows, err := tx.QueryContext(ctx,
		"SELECT event_id, purpose FROM coordinator_supervision_inbox_ack WHERE run_id = ? ORDER BY event_id, purpose", runID)
	if err != nil {
		return fmt.Errorf("list supervision inbox acknowledgements of run %q: %w", runID, err)
	}
	defer rows.Close()
	for rows.Next() {
		var eventID, purpose string
		if err := rows.Scan(&eventID, &purpose); err != nil {
			return err
		}
		if row := byID[eventID]; row != nil {
			row.AcknowledgedPurposes = append(row.AcknowledgedPurposes, purpose)
		}
	}
	return rows.Err()
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

// TransitionSupervisionOutboxRow is the authoritative compare-and-set boundary
// for delivery. Illegal moves are rejected here rather than trusted to callers.
func (s *Store) TransitionSupervisionOutboxRow(ctx context.Context, id, from, to string, now time.Time) (bool, error) {
	if !supervisionDeliveryTransitionAllowed(from, to) {
		return false, fmt.Errorf("invalid supervision delivery transition %s to %s", from, to)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var raw []byte
	err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_supervision_outbox WHERE id = ? AND delivery_state = ?", id, from).Scan(&raw)
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
	switch to {
	case "sending":
		attempts, _ := entry["attempts"].(float64)
		entry["attempts"] = attempts + 1
		entry["lastError"] = ""
		entry["nextAction"] = "reconcile the durable delivery receipt"
		delete(entry, "nextEligibleAt")
	case "offline", "busy":
		entry["lastError"] = "delivery target is " + to
		entry["nextAction"] = "retry after the target becomes available"
		attempts, _ := entry["attempts"].(float64)
		entry["nextEligibleAt"] = now.UTC().Add(deliveryRetryDelay(int(attempts)))
	case "recovery-required":
		entry["lastError"] = "delivery outcome is unknown"
		entry["nextAction"] = "reconcile the durable receipt; do not resend without known non-effect"
		delete(entry, "nextEligibleAt")
	case "delivered":
		entry["lastError"] = ""
		entry["nextAction"] = ""
		delete(entry, "nextEligibleAt")
		entry["deliveredAt"] = now.UTC()
	case "rejected":
		entry["lastError"] = "delivery was rejected"
		entry["nextAction"] = "operator action is required"
		delete(entry, "nextEligibleAt")
	}
	updated, err := json.Marshal(entry)
	if err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, "UPDATE coordinator_supervision_outbox SET delivery_state = ?, record = ? WHERE id = ? AND delivery_state = ?", to, updated, id, from)
	if err != nil {
		return false, fmt.Errorf("transition supervision outbox entry %q: %w", id, err)
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		return false, nil
	}
	return true, tx.Commit()
}

// ClaimSupervisionEscalation freezes the exact message and digest in the same
// transaction that claims sending ownership.
func (s *Store) ClaimSupervisionEscalation(ctx context.Context, id, from, payload string, now time.Time) (bool, error) {
	if !supervisionDeliveryTransitionAllowed(from, "sending") {
		return false, fmt.Errorf("invalid supervision delivery transition %s to sending", from)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var raw []byte
	err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_supervision_outbox WHERE id = ? AND delivery_state = ?", id, from).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var entry map[string]any
	if err := json.Unmarshal(raw, &entry); err != nil {
		return false, err
	}
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(payload)))
	if frozen, _ := entry["payload"].(string); frozen != "" {
		frozenDigest, _ := entry["payloadDigest"].(string)
		if frozen != payload || frozenDigest != digest {
			return false, fmt.Errorf("claim supervision escalation %q: frozen delivery differs", id)
		}
	}
	entry["payload"] = payload
	entry["payloadDigest"] = digest
	entry["delivery"] = "sending"
	attempts, _ := entry["attempts"].(float64)
	entry["attempts"] = attempts + 1
	entry["lastError"] = ""
	entry["nextAction"] = "reconcile the durable delivery receipt"
	delete(entry, "nextEligibleAt")
	updated, err := json.Marshal(entry)
	if err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, "UPDATE coordinator_supervision_outbox SET delivery_state='sending', record=? WHERE id=? AND delivery_state=?", updated, id, from)
	if err != nil {
		return false, err
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		return false, nil
	}
	return true, tx.Commit()
}

func supervisionDeliveryTransitionAllowed(from, to string) bool {
	switch from {
	case "pending":
		return to == "held" || to == "offline" || to == "busy" || to == "sending" || to == "rejected" || to == "cancelled"
	case "held", "offline", "busy":
		return to == "offline" || to == "busy" || to == "sending" || to == "rejected" || to == "cancelled"
	case "sending":
		return to == "delivered" || to == "recovery-required" || to == "offline" || to == "rejected"
	case "recovery-required":
		return to == "delivered" || to == "offline" || to == "rejected"
	}
	return false
}

// PendingSupervisionEscalations lists every nonterminal escalation, including
// dry-run holds and known-no-effect retry states, so a live restart can resume it.
func (s *Store) PendingSupervisionEscalations(ctx context.Context) ([]domain.SupervisionEscalationDelivery, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, run_id, delivery_state, record FROM coordinator_supervision_outbox
		WHERE delivery_state IN ('pending', 'held', 'offline', 'busy', 'sending', 'recovery-required') ORDER BY id`)
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
			Kind           string     `json:"kind"`
			IncidentID     string     `json:"incidentId"`
			ThreadID       string     `json:"threadId"`
			Reason         string     `json:"reason"`
			Attempts       int        `json:"attempts"`
			Payload        string     `json:"payload"`
			PayloadDigest  string     `json:"payloadDigest"`
			LastError      string     `json:"lastError"`
			NextAction     string     `json:"nextAction"`
			NextEligibleAt *time.Time `json:"nextEligibleAt"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, fmt.Errorf("decode supervision outbox entry %q: %w", id, err)
		}
		if entry.Kind != "escalation" || entry.ThreadID == "" {
			continue
		}
		pending = append(pending, domain.SupervisionEscalationDelivery{
			ID: id, DeliveryID: id, RunID: runID, IncidentID: entry.IncidentID,
			ThreadID: entry.ThreadID, Reason: entry.Reason, Delivery: delivery, Attempts: entry.Attempts,
			Payload: entry.Payload, PayloadDigest: entry.PayloadDigest,
			DeliveryError: entry.LastError, DeliveryNextAction: entry.NextAction,
			DeliveryNextAttemptAt: entry.NextEligibleAt,
		})
	}
	return pending, rows.Err()
}

// otherActivationValid reports whether an activation other than the one being
// decided still counts against the run's rule that at most one activation is
// valid.
//
// An activation below the run's current activation epoch never counts,
// whatever state its row still records. Raising the epoch writes the
// replacement as a new row and leaves the old one as it was, so an old row can
// say pending-dispatch or active at an epoch whose authority is gone: every
// supervision decision is fenced on the record's epoch, and
// ReleaseDeadActivationOffers releases the offer of such a row for the same
// reason. Counting it refused every trigger of the replacement, and the run
// could never wake its overseer again.
func otherActivationValid(candidate domain.Activation, currentEpoch int64) bool {
	if candidate.Epoch < currentEpoch {
		return false
	}
	return candidate.State == domain.ActivationPendingDispatch || candidate.State == domain.ActivationActive
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
		if otherActivationValid(candidate, state.Record.ActivationEpoch) {
			state.OtherValidActivation = true
		}
	}
	if state.Inbox, err = supervisionInboxRowsTx(ctx, tx, runID); err != nil {
		return SupervisionActivationRows{}, err
	}
	if err := loadSupervisionInboxAcknowledgementsTx(ctx, tx, runID, state.Inbox); err != nil {
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
	if commit.CursorAdvanced && commit.AcknowledgementPurpose != "" &&
		commit.AcknowledgementPurpose != string(domain.RecoveryActivationRepair) {
		return fmt.Errorf("acknowledge supervision inbox: unknown purpose %q", commit.AcknowledgementPurpose)
	}
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
	if commit.CursorAdvanced {
		seen := make(map[string]struct{}, len(commit.AcknowledgedEventIDs))
		for _, eventID := range commit.AcknowledgedEventIDs {
			if eventID == "" {
				return errors.New("acknowledge supervision inbox: empty event id")
			}
			if _, duplicate := seen[eventID]; duplicate {
				continue
			}
			seen[eventID] = struct{}{}
			result, err := tx.ExecContext(ctx, `
				INSERT INTO coordinator_supervision_inbox_ack(event_id, run_id, purpose, acknowledged_at)
				SELECT id, run_id, ?, ? FROM coordinator_supervision_inbox
				WHERE id = ? AND run_id = ? ON CONFLICT(event_id, purpose) DO NOTHING`,
				commit.AcknowledgementPurpose, now.Format(time.RFC3339Nano), eventID, commit.RunID)
			if err != nil {
				return fmt.Errorf("acknowledge supervision event %q: %w", eventID, err)
			}
			if changed, _ := result.RowsAffected(); changed == 0 {
				var exists int
				if err := tx.QueryRowContext(ctx,
					"SELECT COUNT(*) FROM coordinator_supervision_inbox WHERE id = ? AND run_id = ?", eventID, commit.RunID).Scan(&exists); err != nil {
					return err
				}
				if exists == 0 {
					return fmt.Errorf("acknowledge supervision event %q: event does not belong to run %q", eventID, commit.RunID)
				}
			}
			// The old consumed bit remains a projection for legacy reviewer
			// readers. Schema 22 binaries use the acknowledgement table.
			if commit.AcknowledgementPurpose == "" {
				if _, err := tx.ExecContext(ctx,
					"UPDATE coordinator_supervision_inbox SET consumed = 1 WHERE id = ? AND run_id = ?", eventID, commit.RunID); err != nil {
					return fmt.Errorf("project reviewer acknowledgement of event %q: %w", eventID, err)
				}
			}
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
