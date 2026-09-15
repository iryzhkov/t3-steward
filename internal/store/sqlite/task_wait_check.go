package sqlite

import (
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/wait"
	"time"
)

// InsertTaskWaitCheck preserves a concurrent poller's progress when the same
// coordinator registration is retried. The caller supplies a stable local ID.
func (s *Store) InsertTaskWaitCheck(ctx context.Context, w wait.Wait) error {
	raw, err := json.Marshal(w)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO waits(id, thread_id, status, state, updated_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
		w.ID, w.ThreadID, string(w.Status), string(raw), time.Now().UTC().Format(time.RFC3339Nano))
	return err
}
