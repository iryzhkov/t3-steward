package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/ownernotify"
)

// Worker outages ride the same outbox as campaign events, with no schema
// change. A worker-down row's id is sink, event and "<worker>@<since>", where
// since is the start of the outage and does not move while it lasts: a second
// pass, a coordinator restart or a reload finds the row already there and
// records nothing. The worker-recovered row of the same outage has the same
// subject under its own event, so it too is recorded at most once.
//
// The rows carry no run, so run_id is empty, and they have no watermark: an
// outage is a condition of now, and a sink enabled while a worker is down is
// told about it.

var _ ownernotify.WorkerNotificationStore = (*Store)(nil)

// workerOutageSubject is the identity of one outage of one worker.
func workerOutageSubject(worker string, since time.Time) string {
	return worker + "@" + ownerNotificationTime(since)
}

// DetectWorkerNotifications implements ownernotify.WorkerNotificationStore.
func (s *Store) DetectWorkerNotifications(ctx context.Context, sink string, selection ownernotify.Selection, workers []ownernotify.WorkerState, now time.Time) (ownernotify.Detection, error) {
	var detection ownernotify.Detection
	wantDown, wantRecovered := false, false
	for _, event := range selection.Events {
		wantDown = wantDown || event == ownernotify.EventWorkerDown
		wantRecovered = wantRecovered || event == ownernotify.EventWorkerRecovered
	}
	if !wantDown && !wantRecovered {
		return detection, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return detection, fmt.Errorf("begin worker outage detection: %w", err)
	}
	defer tx.Rollback()
	at := ownerNotificationTime(now)
	insert := func(n ownernotify.Notification, state ownernotify.State) error {
		raw, err := json.Marshal(n)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO coordinator_owner_notifications
			(id, sink, event, run_id, state, attempts, next_attempt_at, last_error, created_at, updated_at, record)
			VALUES (?, ?, ?, '', ?, 0, ?, '', ?, ?, ?)`,
			n.ID, sink, string(n.Event), string(state), at, at, at, string(raw))
		if err != nil {
			return fmt.Errorf("record worker outage notification: %w", err)
		}
		if inserted, _ := result.RowsAffected(); inserted != 0 && state == ownernotify.StatePending {
			detection.Enqueued++
		}
		return nil
	}

	// An outage is its worker and its start. A worker that came back and
	// dropped again between two reads has a new outage, and the old one is
	// over: it is closed below with a recovery like any other.
	current := make(map[string]bool, len(workers))
	for _, worker := range workers {
		current[workerOutageSubject(worker.WorkerID, worker.Since.UTC())] = true
		if !worker.Down || !wantDown {
			continue
		}
		since := worker.Since.UTC()
		n := ownernotify.Notification{
			ID:   ownerNotificationID(sink, ownernotify.EventWorkerDown, workerOutageSubject(worker.WorkerID, since)),
			Sink: sink, Event: ownernotify.EventWorkerDown, Worker: worker.WorkerID, Since: &since, OccurredAt: now.UTC(),
			Reason: fmt.Sprintf("Worker %s has not been reached by the coordinator for %s.", worker.WorkerID, worker.DownFor.Round(time.Minute)),
		}
		if !worker.LastSeen.IsZero() {
			lastSeen := worker.LastSeen.UTC()
			n.LastSeen = &lastSeen
		}
		if err := insert(n, ownernotify.StatePending); err != nil {
			return detection, err
		}
	}

	// Every outage reported to this sink and not yet closed: a worker-down row
	// that is not baseline or resolved, with no worker-recovered row.
	rows, err := tx.QueryContext(ctx, `SELECT d.id, d.state, d.record FROM coordinator_owner_notifications d
		WHERE d.sink = ?1 AND d.event = ?2 AND d.state IN ('pending', 'delivered', 'abandoned')
		  AND NOT EXISTS (SELECT 1 FROM coordinator_owner_notifications r
		    WHERE r.id = ?1 || '|' || ?3 || '|' || substr(d.id, length(?1 || '|' || ?2 || '|') + 1))`,
		sink, string(ownernotify.EventWorkerDown), string(ownernotify.EventWorkerRecovered))
	if err != nil {
		return detection, fmt.Errorf("read open worker outages: %w", err)
	}
	type openOutage struct {
		id, state string
		down      ownernotify.Notification
	}
	var open []openOutage
	for rows.Next() {
		var outage openOutage
		var raw string
		if err := rows.Scan(&outage.id, &outage.state, &raw); err != nil {
			rows.Close()
			return detection, fmt.Errorf("read open worker outage: %w", err)
		}
		if err := json.Unmarshal([]byte(raw), &outage.down); err != nil {
			rows.Close()
			return detection, fmt.Errorf("decode worker outage %s: %w", outage.id, err)
		}
		open = append(open, outage)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return detection, fmt.Errorf("read open worker outages: %w", err)
	}
	if err := rows.Close(); err != nil {
		return detection, err
	}
	for _, outage := range open {
		if current[workerOutageSubject(outage.down.Worker, derefTime(outage.down.Since))] {
			continue
		}
		if outage.state == string(ownernotify.StatePending) {
			// Nobody was told, so there is nothing to take back.
			if _, err := tx.ExecContext(ctx, `UPDATE coordinator_owner_notifications
				SET state = ?, updated_at = ? WHERE id = ? AND state = ?`,
				string(ownernotify.StateResolved), at, outage.id, string(ownernotify.StatePending)); err != nil {
				return detection, fmt.Errorf("resolve worker outage %s: %w", outage.id, err)
			}
			continue
		}
		if !wantRecovered {
			continue
		}
		recovered := ownernotify.Notification{
			ID: ownerNotificationID(sink, ownernotify.EventWorkerRecovered,
				workerOutageSubject(outage.down.Worker, derefTime(outage.down.Since))),
			Sink: sink, Event: ownernotify.EventWorkerRecovered, Worker: outage.down.Worker,
			Since: outage.down.Since, LastSeen: outage.down.LastSeen, OccurredAt: now.UTC(),
			Reason: fmt.Sprintf("The outage of worker %s that began %s is over: the coordinator reaches it again, it is no longer enrolled, or it dropped again with a new outage.",
				outage.down.Worker, derefTime(outage.down.Since).Format(time.RFC3339)),
		}
		if err := insert(recovered, ownernotify.StatePending); err != nil {
			return detection, err
		}
	}
	if err := tx.Commit(); err != nil {
		return ownernotify.Detection{}, fmt.Errorf("commit worker outage detection: %w", err)
	}
	return detection, nil
}

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.UTC()
}
