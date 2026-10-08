package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// runCollectionKeyPrefix is the kv namespace of run collections. A record
// lives at run-collection/v1/<thread>/<run>; neither ID may contain "/", so
// the prefix of one thread never matches another thread's records.
//
// The kv table is used rather than a table of its own because a collection is
// a small per-thread acknowledgement with no query beyond "this thread's
// records", and a new table would need a migration that every coordinator
// would have to run before the first client asked for it.
const runCollectionKeyPrefix = "run-collection/v1/"

func runCollectionThreadPrefix(threadID string) string {
	return runCollectionKeyPrefix + threadID + "/"
}

// CollectRun records that threadID has acted on runID as the run finished.
//
// In one transaction it refuses an unknown run, a run that has not finished,
// and a run that no node wait of the thread targets, so a stored collection
// always names a finished run the thread owns. The progress and completion
// time come from the run's own record, never from the caller.
//
// Recording a run again while it still has the progress and completion time
// that were recorded changes nothing and keeps the first collectedAt; a run
// that finished again since is recorded anew. The boolean says whether the
// record was written.
func (s *Store) CollectRun(ctx context.Context, threadID, runID, actor string, now time.Time) (domain.RunCollection, bool, error) {
	var none domain.RunCollection
	if err := domain.ValidateRunCollectionID("thread", threadID); err != nil {
		return none, false, err
	}
	if err := domain.ValidateRunCollectionID("run", runID); err != nil {
		return none, false, err
	}
	if now.IsZero() {
		return none, false, errors.New("a run collection needs the time it was recorded")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return none, false, err
	}
	defer tx.Rollback()
	// Take SQLite's write reservation before the first read, as the lease and
	// review-authority writes do. A transaction that reads first and then
	// writes is refused at once with "database is locked" when another
	// connection holds or has just committed a write, because SQLite does not
	// apply the busy timeout to that upgrade. Reserving first makes a
	// contending connection wait under the busy timeout instead. The statement
	// changes nothing, whether or not the key exists.
	key := runCollectionThreadPrefix(threadID) + runID
	if _, err := tx.ExecContext(ctx, `UPDATE kv SET value = value WHERE key = ?`, key); err != nil {
		return none, false, err
	}

	var raw []byte
	err = tx.QueryRowContext(ctx, `SELECT record FROM coordinator_workflow_runs WHERE id = ?`, runID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return none, false, fmt.Errorf("%w: unknown run %s; nothing was collected", domain.ErrRunCollectionRefused, runID)
	}
	if err != nil {
		return none, false, err
	}
	var run domain.WorkflowRun
	if err := json.Unmarshal(raw, &run); err != nil {
		return none, false, fmt.Errorf("decode run %s: %w", runID, err)
	}
	if !run.Progress.Terminal() {
		return none, false, fmt.Errorf("%w: run %s is %s and cannot be collected; only a finished run is collected",
			domain.ErrRunCollectionRefused, runID, run.Progress)
	}
	var owned int
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM coordinator_node_waits
		WHERE thread_id = ? AND json_extract(record, '$.request.target.runId') = ?)`, threadID, runID).Scan(&owned)
	if err != nil {
		return none, false, err
	}
	if owned == 0 {
		return none, false, fmt.Errorf("%w: run %s has no notification for thread %s; nothing was collected",
			domain.ErrRunCollectionRefused, runID, threadID)
	}

	var stored string
	err = tx.QueryRowContext(ctx, `SELECT value FROM kv WHERE key = ?`, key).Scan(&stored)
	switch {
	case err == nil:
		var existing domain.RunCollection
		if err := json.Unmarshal([]byte(stored), &existing); err != nil {
			return none, false, fmt.Errorf("decode the collection of run %s: %w", runID, err)
		}
		if existing.Covers(run) {
			return existing, false, tx.Commit()
		}
	case !errors.Is(err, sql.ErrNoRows):
		return none, false, err
	}

	collection := domain.RunCollection{
		RunID: runID, ThreadID: threadID, Progress: run.Progress,
		CompletedAt: utcTime(run.CompletedAt), CollectedAt: now.UTC(), Actor: actor,
	}
	value, err := json.Marshal(collection)
	if err != nil {
		return none, false, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO kv(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, string(value)); err != nil {
		return none, false, err
	}
	if err := tx.Commit(); err != nil {
		return none, false, err
	}
	return collection, true, nil
}

// ListRunCollections returns every collection recorded for threadID, ordered
// by run ID. A collection that no longer covers its run is returned as well;
// deciding that needs the run, which the caller reads anyway.
func (s *Store) ListRunCollections(ctx context.Context, threadID string) ([]domain.RunCollection, error) {
	if err := domain.ValidateRunCollectionID("thread", threadID); err != nil {
		return nil, err
	}
	// A key range rather than substr: SQLite's substr counts characters and Go
	// counts bytes, so a thread ID with a non-ASCII character would match
	// nothing. Keys compare as bytes, and every key of this thread lies between
	// "<prefix>" and the same text with its trailing "/" raised to "0".
	prefix := runCollectionThreadPrefix(threadID)
	upper := prefix[:len(prefix)-1] + "0"
	rows, err := s.db.QueryContext(ctx, `SELECT key, value FROM kv WHERE key >= ? AND key < ? ORDER BY key`, prefix, upper)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.RunCollection{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		var collection domain.RunCollection
		if err := json.Unmarshal([]byte(value), &collection); err != nil {
			return nil, fmt.Errorf("decode run collection %s: %w", key, err)
		}
		out = append(out, collection)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RunID < out[j].RunID })
	return out, nil
}

func utcTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
