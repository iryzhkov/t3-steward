package backlog

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// CoordinatorSupervisionStore binds the coordinator store to the supervision
// interfaces this package declares.
//
// It exists because the interfaces are declared where they are used and are
// phrased in this package's own types, while the tables live in a package this
// one imports. The store cannot name SupervisionEvent or SupervisionOutboxEntry
// without an import cycle, so it carries them as the encoded records they are
// stored as and this type does the encoding. Nothing here decides anything: it
// translates.
//
// It embeds the store, so it is also an ordinary ProjectionStore and still
// answers every other capability a caller type-asserts for.
type CoordinatorSupervisionStore struct {
	*sqlite.Store
}

// SupervisionProjection reads one run's supervision state for the projection.
func (c CoordinatorSupervisionStore) SupervisionProjection(ctx context.Context, runID string) (SupervisionProjection, error) {
	snapshot, err := c.Store.LoadSupervisionSnapshot(ctx, runID)
	if err != nil {
		return SupervisionProjection{}, err
	}
	if !snapshot.Supervised {
		return SupervisionProjection{}, nil
	}
	projection, err := c.Store.LoadSupervisionProjection(ctx, runID)
	if err != nil {
		return SupervisionProjection{}, err
	}
	return SupervisionProjection{Snapshot: snapshot, Incidents: projection.Incidents}, nil
}

// LoadSupervisionActivationState reads one run's activation decision state.
func (c CoordinatorSupervisionStore) LoadSupervisionActivationState(ctx context.Context, runID string) (SupervisionActivationState, error) {
	rows, err := c.Store.LoadSupervisionActivationRows(ctx, runID)
	if err != nil {
		return SupervisionActivationState{}, err
	}
	if !rows.Supervised {
		return SupervisionActivationState{}, fmt.Errorf("%w: run %q", ErrSupervisionNotConfigured, runID)
	}
	state := SupervisionActivationState{
		Record:               rows.Record,
		Activation:           rows.Activation,
		OtherValidActivation: rows.OtherValidActivation,
	}
	for _, row := range rows.Inbox {
		var event SupervisionEvent
		if err := json.Unmarshal(row.Record, &event); err != nil {
			return SupervisionActivationState{}, fmt.Errorf("decode supervision event %q: %w", row.ID, err)
		}
		event.Sequence = row.Sequence
		state.Pending = append(state.Pending, event)
	}
	for _, row := range rows.Outbox {
		entry, err := decodeSupervisionOutboxEntry(row)
		if err != nil {
			return SupervisionActivationState{}, err
		}
		state.Outbox = append(state.Outbox, entry)
	}
	return state, nil
}

// CommitSupervisionActivation writes one plan atomically.
func (c CoordinatorSupervisionStore) CommitSupervisionActivation(ctx context.Context, commit SupervisionActivationCommit) error {
	rows, err := encodeSupervisionOutboxEntries(commit.RunID, commit.Outbox)
	if err != nil {
		return err
	}
	var receipt json.RawMessage
	if commit.Receipt != nil {
		raw, err := json.Marshal(commit.Receipt)
		if err != nil {
			return fmt.Errorf("encode continuation receipt: %w", err)
		}
		receipt = raw
	}
	return c.Store.CommitSupervisionActivationRows(ctx, sqlite.SupervisionActivationRowCommit{
		RunID:                  commit.RunID,
		ExpectedRecordRevision: commit.ExpectedRecordRevision,
		Record:                 commit.Record,
		Activation:             commit.Activation,
		ConsumedThrough:        commit.ConsumedThrough,
		CursorAdvanced:         commit.CursorAdvanced,
		Outbox:                 rows,
		Receipt:                receipt,
		RequestID:              commit.RequestID,
		CommittedAt:            commit.CommittedAt,
	})
}

// AppendSupervisionEvents appends observed triggers and assigns their sequences.
func (c CoordinatorSupervisionStore) AppendSupervisionEvents(ctx context.Context, runID string, events []SupervisionEvent) (int, error) {
	rows := make([]sqlite.SupervisionInboxRow, 0, len(events))
	for _, event := range events {
		raw, err := json.Marshal(event)
		if err != nil {
			return 0, fmt.Errorf("encode supervision event %q: %w", event.ID, err)
		}
		rows = append(rows, sqlite.SupervisionInboxRow{ID: event.ID, RunID: runID, Record: raw})
	}
	return c.Store.AppendSupervisionInbox(ctx, runID, rows)
}

// AppendSupervisionOutbox persists delivery intents that are not already live.
func (c CoordinatorSupervisionStore) AppendSupervisionOutbox(ctx context.Context, runID string, entries []SupervisionOutboxEntry) (int, error) {
	rows, err := encodeSupervisionOutboxEntries(runID, entries)
	if err != nil {
		return 0, err
	}
	return c.Store.AppendSupervisionOutboxRows(ctx, runID, rows)
}

// ListSupervisionOutbox returns this run's delivery intents, oldest first.
func (c CoordinatorSupervisionStore) ListSupervisionOutbox(ctx context.Context, runID string) ([]SupervisionOutboxEntry, error) {
	rows, err := c.Store.ListSupervisionOutboxRows(ctx, runID)
	if err != nil {
		return nil, err
	}
	entries := make([]SupervisionOutboxEntry, 0, len(rows))
	for _, row := range rows {
		entry, err := decodeSupervisionOutboxEntry(row)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// TransitionSupervisionOutbox fences delivery ownership with a compare and set.
func (c CoordinatorSupervisionStore) TransitionSupervisionOutbox(
	ctx context.Context,
	id string,
	from, to SupervisionDelivery,
	now time.Time,
) (bool, error) {
	return c.Store.TransitionSupervisionOutboxRow(ctx, id, string(from), string(to), now)
}

func encodeSupervisionOutboxEntries(runID string, entries []SupervisionOutboxEntry) ([]sqlite.SupervisionOutboxRow, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	rows := make([]sqlite.SupervisionOutboxRow, 0, len(entries))
	for _, entry := range entries {
		if entry.RunID == "" {
			entry.RunID = runID
		}
		if err := entry.Validate(); err != nil {
			return nil, err
		}
		raw, err := json.Marshal(entry)
		if err != nil {
			return nil, fmt.Errorf("encode supervision outbox entry %q: %w", entry.ID, err)
		}
		rows = append(rows, sqlite.SupervisionOutboxRow{
			ID: entry.ID, RunID: entry.RunID, ActivationID: entry.ActivationID,
			Delivery: string(entry.Delivery), Record: raw,
		})
	}
	return rows, nil
}

func decodeSupervisionOutboxEntry(row sqlite.SupervisionOutboxRow) (SupervisionOutboxEntry, error) {
	var entry SupervisionOutboxEntry
	if err := json.Unmarshal(row.Record, &entry); err != nil {
		return SupervisionOutboxEntry{}, fmt.Errorf("decode supervision outbox entry %q: %w", row.ID, err)
	}
	// The column is the authority on delivery state: it is what the compare and
	// set fences on.
	entry.Delivery = SupervisionDelivery(row.Delivery)
	return entry, nil
}

// Compile-time proof that the binding satisfies every supervision interface of
// this package. A missing method is a build failure here rather than a runtime
// type assertion that silently answers "unsupervised".
var (
	_ ProjectionStore             = CoordinatorSupervisionStore{}
	_ SupervisionProjectionSource = CoordinatorSupervisionStore{}
	_ SupervisionReadSetSource    = CoordinatorSupervisionStore{}
	_ SupervisionActivationStore  = CoordinatorSupervisionStore{}
	_ SupervisionOutboxStore      = CoordinatorSupervisionStore{}
	_ CoordinatorRecordStore      = CoordinatorSupervisionStore{}
	_ SupervisionMaterializer     = CoordinatorSupervisionStore{}
)
