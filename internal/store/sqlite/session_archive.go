package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/sessionarchive"
)

// SessionArchiveStates reads only provenance and custody records, not workflow
// artifacts, graph history or audit exports.
func (s *Store) SessionArchiveStates(ctx context.Context) (map[string]sessionarchive.State, error) {
	busy, err := s.BusyThreads(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]sessionarchive.State{}
	for id, reason := range busy {
		out[id] = sessionarchive.State{Busy: reason}
	}
	legacy, err := s.LoadTaskStates(ctx)
	if err != nil {
		return nil, err
	}
	for _, raw := range legacy {
		var record struct {
			ThreadID string `json:"threadId"`
		}
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, err
		}
		if record.ThreadID != "" {
			state := out[record.ThreadID]
			state.Background = true
			out[record.ThreadID] = state
		}
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	assignments, err := loadJSON[domain.Assignment](ctx, tx, "coordinator_assignments")
	if err != nil {
		return nil, err
	}
	attempts, err := loadJSON[domain.Attempt](ctx, tx, "coordinator_attempts")
	if err != nil {
		return nil, err
	}
	for _, a := range assignments {
		if a.ThreadID == "" {
			continue
		}
		state := out[a.ThreadID]
		state.Background = true
		if a.State != domain.AssignmentCompleted && a.State != domain.AssignmentReleased {
			state.Busy = "assignment custody " + a.ID
		}
		out[a.ThreadID] = state
	}
	for _, a := range attempts {
		if a.ThreadID == "" {
			continue
		}
		state := out[a.ThreadID]
		state.Background = true
		if !a.Progress.Terminal() || (a.Control != "" && a.Control != domain.ControlStopped && a.Control != domain.ControlUnassigned) {
			state.Busy = "unfinished attempt " + a.ID
		}
		out[a.ThreadID] = state
	}
	return out, tx.Commit()
}
