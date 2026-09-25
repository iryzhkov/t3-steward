package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/ownernotify"
)

// Schema 30 is the owner-notification outbox: campaign events bound for the
// owner's own channel (a Discord webhook or a local command), in addition to
// the thread wake a submission registers.
//
// A row's id is sink, event and the identity of what happened, joined with
// "|". That identity is the run for a terminal outcome, the attention wait for
// a question, and the supervision record together with the revision-like part
// that changes when the same record needs the operator again (an incident's
// revision, an activation's state, a gate's evidence snapshot). An escalated
// or unrecoverable activation that was woken for an incident is keyed as that
// incident at its current revision, so the incident and the spent overseer of
// one episode send one message, not two. Detection inserts with that key and
// ignores a conflict, so a coordinator restart re-detects nothing it already
// recorded and loses nothing it had not yet sent.
//
// The scopes table holds one watermark per sink and scope (an event, or the
// scheduled successes of one): the time the scope became active. A run or a
// question is new to the sink only when it happened at or after the
// watermark, so enabling a webhook on a coordinator with a long history does
// not replay that history. A scope that leaves the configuration loses its
// watermark, so re-adding it later does not replay what finished in between
// either. Supervision records carry no time of their own; when their scope is
// created, the ones already waiting are recorded as baseline rows instead,
// which are never sent.
//
// Settled rows are pruned after a retention bound, and the watermark moves up
// with every prune. A supervision row is pruned only once its run is
// terminal, and detection reports supervision conditions only of runs that
// are not, so a pruned row cannot be detected again.
//
// The two indexes on existing tables serve the per-pass scans: terminal runs
// by outcome and completion time, and settled outbox rows by age.
const coordinatorMigrationV30 = `
CREATE TABLE IF NOT EXISTS coordinator_owner_notifications(
	id TEXT PRIMARY KEY,
	sink TEXT NOT NULL,
	event TEXT NOT NULL,
	run_id TEXT NOT NULL,
	state TEXT NOT NULL,
	attempts INTEGER NOT NULL DEFAULT 0,
	next_attempt_at TEXT NOT NULL,
	last_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	record TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS coordinator_owner_notifications_due
	ON coordinator_owner_notifications(sink, state, next_attempt_at);
CREATE INDEX IF NOT EXISTS coordinator_owner_notifications_age
	ON coordinator_owner_notifications(state, created_at);
CREATE TABLE IF NOT EXISTS coordinator_owner_notification_scopes(
	sink TEXT NOT NULL,
	scope TEXT NOT NULL,
	since TEXT NOT NULL,
	PRIMARY KEY(sink, scope)
);
CREATE INDEX IF NOT EXISTS coordinator_workflow_runs_completion
	ON coordinator_workflow_runs(progress,
		julianday(COALESCE(json_extract(record, '$.completedAt'), json_extract(record, '$.updatedAt'))));
`

// ownerNotificationTerminal is the SQL list of terminal run progress values.
const ownerNotificationTerminal = `('succeeded', 'failed', 'cancelled', 'skipped')`

// The coordinator hands this store to the notifier as its outbox.
var _ ownernotify.Store = (*Store)(nil)

// ownerNotificationTimeLayout is fixed-width UTC, so that next_attempt_at
// compares correctly as text in the due query.
const ownerNotificationTimeLayout = "2006-01-02T15:04:05.000000000Z"

func ownerNotificationTime(t time.Time) string { return t.UTC().Format(ownerNotificationTimeLayout) }

// ownerNotificationCandidate is one detected event before it is recorded.
type ownerNotificationCandidate struct {
	subject string
	runID   string
	record  string
	kind    string
}

// ownerNotificationNeedsInputQuery selects unanswered attention waits
// registered at or after the watermark (?2) with no row for this sink yet.
// The first parameter is the id prefix "sink|event|"; the NOT EXISTS against
// the primary key keeps a pass from re-reading what it already recorded.
const ownerNotificationNeedsInputQuery = `
SELECT w.id, json_extract(w.record, '$.workflowRunId'), w.record, 'wait'
FROM coordinator_task_waits w
WHERE json_extract(w.record, '$.settledAt') IS NULL
  AND json_extract(w.record, '$.kind') = 'attention'
  AND julianday(json_extract(w.record, '$.registeredAt')) >= julianday(?2)
  AND NOT EXISTS (SELECT 1 FROM coordinator_owner_notifications n WHERE n.id = ?1 || w.id)`

// ownerNotificationSupervisionQueries select, per event, every supervision
// record of a live run that is in the reported condition and has no row for
// this sink yet. The first parameter is the id prefix.
var ownerNotificationSupervisionQueries = map[ownernotify.Event]string{
	ownernotify.EventSupervisionEscalated: `
SELECT subject, run_id, record, kind FROM (
  SELECT 'incident:' || id || ':' || revision AS subject, run_id, record, 'incident' AS kind
  FROM coordinator_supervision_incidents WHERE state = 'escalated'
  UNION ALL
  SELECT CASE WHEN i.id IS NULL THEN 'activation:' || a.id || ':' || a.state
              ELSE 'incident:' || i.id || ':' || i.revision END,
         a.run_id, a.record, 'activation'
  FROM coordinator_supervision_activations a
  LEFT JOIN coordinator_supervision_incidents i
    ON i.id = json_extract(a.record, '$.incidentId') AND i.run_id = a.run_id
  WHERE a.state IN ('escalated', 'recovery-required')
  UNION ALL
  SELECT 'gate:' || id || ':' || evidence_snapshot_id || ':escalated', run_id, record, 'gate'
  FROM coordinator_supervision_gates WHERE state = 'escalated'
) c
WHERE NOT EXISTS (SELECT 1 FROM coordinator_owner_notifications n WHERE n.id = ?1 || c.subject)
  AND EXISTS (SELECT 1 FROM coordinator_workflow_runs r
    WHERE r.id = c.run_id AND r.progress NOT IN ` + ownerNotificationTerminal + `)`,
	ownernotify.EventGateReview: `
SELECT 'gate:' || g.id || ':' || g.evidence_snapshot_id, g.run_id, g.record, 'gate'
FROM coordinator_supervision_gates g
WHERE g.state = 'ready-for-review'
  AND NOT EXISTS (SELECT 1 FROM coordinator_owner_notifications n
    WHERE n.id = ?1 || 'gate:' || g.id || ':' || g.evidence_snapshot_id)
  AND EXISTS (SELECT 1 FROM coordinator_workflow_runs r
    WHERE r.id = g.run_id AND r.progress NOT IN ` + ownerNotificationTerminal + `)`,
}

// ownerNotificationRunQuery selects runs of one outcome that completed at or
// after the watermark (?4). The third parameter narrows them to runs a
// schedule created, runs it did not, or all. The completion expression is the
// indexed one, so the scan reads only runs newer than the watermark.
const ownerNotificationRunQuery = `
SELECT r.id, r.id, r.record, 'run'
FROM coordinator_workflow_runs r
WHERE r.progress = ?2
  AND julianday(COALESCE(json_extract(r.record, '$.completedAt'), json_extract(r.record, '$.updatedAt'))) >= julianday(?4)
  AND (?3 = 'all' OR (?3 = 'scheduled') = (r.schedule_id <> ''))
  AND NOT EXISTS (SELECT 1 FROM coordinator_owner_notifications n WHERE n.id = ?1 || r.id)`

// DetectOwnerNotifications implements ownernotify.Store.
//
// Everything happens in one transaction, so a pass that creates a scope's
// watermark cannot interleave with a pass that records the same scope as
// pending. The transaction reads only what is newer than the watermarks and
// not yet recorded, so it stays short.
func (s *Store) DetectOwnerNotifications(ctx context.Context, sink string, selection ownernotify.Selection, now time.Time) (ownernotify.Detection, error) {
	var detection ownernotify.Detection
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return detection, fmt.Errorf("begin owner notification detection: %w", err)
	}
	defer tx.Rollback()
	enrich := ownerNotificationEnricher{tx: tx, campaigns: map[string]string{}, tasks: map[string]string{}}
	at := ownerNotificationTime(now)
	for _, scope := range selection.Scopes() {
		event := scope.Event
		var baselined bool
		since := at
		switch err := tx.QueryRowContext(ctx,
			`SELECT since FROM coordinator_owner_notification_scopes WHERE sink = ? AND scope = ?`,
			sink, scope.Key).Scan(&since); {
		case err == nil:
			baselined = true
		case !errors.Is(err, sql.ErrNoRows):
			return detection, fmt.Errorf("read owner notification watermark: %w", err)
		}
		candidates, err := ownerNotificationCandidatesTx(ctx, tx, sink, scope, since)
		if err != nil {
			return detection, err
		}
		state := ownernotify.StatePending
		if !baselined {
			state = ownernotify.StateBaseline
		}
		for _, candidate := range candidates {
			notification := ownernotify.Notification{
				ID: ownerNotificationID(sink, event, candidate.subject), Sink: sink, Event: event,
				RunID: candidate.runID, OccurredAt: now.UTC(),
			}
			if baselined {
				// A baseline row is never sent, so it is not worth describing.
				if err := enrich.describe(ctx, &notification, candidate); err != nil {
					return detection, err
				}
			}
			raw, err := json.Marshal(notification)
			if err != nil {
				return detection, err
			}
			result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO coordinator_owner_notifications
				(id, sink, event, run_id, state, attempts, next_attempt_at, last_error, created_at, updated_at, record)
				VALUES (?, ?, ?, ?, ?, 0, ?, '', ?, ?, ?)`,
				notification.ID, sink, string(event), candidate.runID, string(state), at, at, at, string(raw))
			if err != nil {
				return detection, fmt.Errorf("record owner notification: %w", err)
			}
			if inserted, _ := result.RowsAffected(); inserted == 0 {
				continue
			}
			if baselined {
				detection.Enqueued++
			} else {
				detection.Baselined++
			}
		}
		if !baselined {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO coordinator_owner_notification_scopes(sink, scope, since) VALUES (?, ?, ?)`,
				sink, scope.Key, at); err != nil {
				return detection, fmt.Errorf("record owner notification watermark: %w", err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return ownernotify.Detection{}, fmt.Errorf("commit owner notification detection: %w", err)
	}
	return detection, nil
}

func ownerNotificationID(sink string, event ownernotify.Event, subject string) string {
	return sink + "|" + string(event) + "|" + subject
}

func ownerNotificationCandidatesTx(ctx context.Context, tx *sql.Tx, sink string, scope ownernotify.Scope, since string) ([]ownerNotificationCandidate, error) {
	event := scope.Event
	prefix := ownerNotificationID(sink, event, "")
	var rows *sql.Rows
	var err error
	if progress, ok := ownerNotificationRunProgress(event); ok {
		rows, err = tx.QueryContext(ctx, ownerNotificationRunQuery, prefix, progress, string(scope.Runs), since)
	} else if event == ownernotify.EventNeedsInput {
		rows, err = tx.QueryContext(ctx, ownerNotificationNeedsInputQuery, prefix, since)
	} else if query, known := ownerNotificationSupervisionQueries[event]; known {
		rows, err = tx.QueryContext(ctx, query, prefix)
	} else {
		return nil, fmt.Errorf("owner notification event %q has no detection", event)
	}
	if err != nil {
		return nil, fmt.Errorf("detect %s owner notifications: %w", event, err)
	}
	defer rows.Close()
	var candidates []ownerNotificationCandidate
	for rows.Next() {
		var candidate ownerNotificationCandidate
		var runID sql.NullString
		if err := rows.Scan(&candidate.subject, &runID, &candidate.record, &candidate.kind); err != nil {
			return nil, fmt.Errorf("read %s owner notification candidate: %w", event, err)
		}
		candidate.runID = runID.String
		candidates = append(candidates, candidate)
	}
	return candidates, rows.Err()
}

// ownerNotificationRunProgress maps a run event to the progress it reports.
func ownerNotificationRunProgress(event ownernotify.Event) (domain.ProgressState, bool) {
	for _, progress := range []domain.ProgressState{
		domain.ProgressSucceeded, domain.ProgressFailed, domain.ProgressCancelled, domain.ProgressSkipped,
	} {
		if mapped, ok := ownernotify.RunOutcomeEvent(string(progress)); ok && mapped == event {
			return progress, true
		}
	}
	return "", false
}

// ownerNotificationEnricher fills the human-readable parts of a notification
// from the coordinator's own records, caching names within one pass.
type ownerNotificationEnricher struct {
	tx        *sql.Tx
	campaigns map[string]string
	tasks     map[string]string
}

func (e ownerNotificationEnricher) describe(ctx context.Context, n *ownernotify.Notification, candidate ownerNotificationCandidate) error {
	switch candidate.kind {
	case "run":
		var run domain.WorkflowRun
		if err := json.Unmarshal([]byte(candidate.record), &run); err != nil {
			return fmt.Errorf("decode run %s for owner notification: %w", candidate.runID, err)
		}
		n.Outcome = string(run.Progress)
		if run.CompletedAt != nil {
			n.OccurredAt = run.CompletedAt.UTC()
		}
		if run.Sink != nil && run.Sink.Result != nil {
			for _, id := range run.Sink.Result.FailedTaskIDs {
				n.FailedTasks = append(n.FailedTasks, e.taskName(ctx, id))
			}
			for _, id := range run.Sink.Result.CancelledTaskIDs {
				n.CancelledTasks = append(n.CancelledTasks, e.taskName(ctx, id))
			}
		}
	case "wait":
		var wait domain.TaskWait
		if err := json.Unmarshal([]byte(candidate.record), &wait); err != nil {
			return fmt.Errorf("decode attention wait %s for owner notification: %w", candidate.subject, err)
		}
		n.WaitID = wait.ID
		n.Task = e.taskName(ctx, wait.TaskID)
		if wait.Attention != nil {
			n.Prompt = wait.Attention.Prompt
		}
		if !wait.RegisteredAt.IsZero() {
			n.OccurredAt = wait.RegisteredAt.UTC()
		}
	case "incident":
		var incident domain.ReviewIncident
		if err := json.Unmarshal([]byte(candidate.record), &incident); err != nil {
			return fmt.Errorf("decode incident for owner notification: %w", err)
		}
		n.IncidentID, n.GateID = incident.ID, incident.GateID
		n.Reason = "Review incident " + incident.ID + " is escalated"
		if incident.Reason != "" {
			n.Reason += ": " + incident.Reason
		}
		n.Reason += "."
		if incident.GateID != "" {
			n.Gate = e.gateName(ctx, incident.GateID)
		}
	case "activation":
		var activation domain.Activation
		if err := json.Unmarshal([]byte(candidate.record), &activation); err != nil {
			return fmt.Errorf("decode activation for owner notification: %w", err)
		}
		n.ActivationID, n.IncidentID = activation.ID, activation.IncidentID
		switch activation.State {
		case domain.ActivationEscalated:
			n.Reason = "The overseer's activation budget is spent; an operator must reassess the run."
		case domain.ActivationRecoveryRequired:
			n.Reason = "An overseer dispatch is ambiguous and needs an operator to reconcile it."
		}
	case "gate":
		var gate domain.Gate
		if err := json.Unmarshal([]byte(candidate.record), &gate); err != nil {
			return fmt.Errorf("decode gate for owner notification: %w", err)
		}
		n.GateID, n.Gate = gate.Definition.ID, gate.Definition.Name
		if gate.State == domain.GateEscalated {
			n.Reason = "The gate review is escalated to an operator."
		}
	}
	n.Campaign = e.campaignName(ctx, n.RunID)
	return nil
}

// campaignName is the workflow name of a run, or empty when it cannot be read.
// A notification without a name is still worth sending, so a failed lookup is
// not an error.
func (e ownerNotificationEnricher) campaignName(ctx context.Context, runID string) string {
	if runID == "" {
		return ""
	}
	if name, ok := e.campaigns[runID]; ok {
		return name
	}
	var name sql.NullString
	_ = e.tx.QueryRowContext(ctx, `SELECT json_extract(w.record, '$.name')
		FROM coordinator_workflow_runs r JOIN coordinator_workflows w ON w.id = r.workflow_id
		WHERE r.id = ?`, runID).Scan(&name)
	e.campaigns[runID] = name.String
	return name.String
}

// taskName is a task's declared name, or its ID when the name cannot be read.
func (e ownerNotificationEnricher) taskName(ctx context.Context, taskID string) string {
	if name, ok := e.tasks[taskID]; ok {
		return name
	}
	name := taskID
	var stored string
	if err := e.tx.QueryRowContext(ctx, `SELECT name FROM coordinator_tasks WHERE id = ?`, taskID).Scan(&stored); err == nil && stored != "" {
		name = stored
	}
	e.tasks[taskID] = name
	return name
}

// gateName is a gate's declared name, or its ID when the name cannot be read.
func (e ownerNotificationEnricher) gateName(ctx context.Context, gateID string) string {
	var name sql.NullString
	if err := e.tx.QueryRowContext(ctx, `SELECT json_extract(record, '$.definition.name')
		FROM coordinator_supervision_gates WHERE id = ?`, gateID).Scan(&name); err == nil && name.String != "" {
		return name.String
	}
	return gateID
}

// DueOwnerNotifications implements ownernotify.Store.
func (s *Store) DueOwnerNotifications(ctx context.Context, sink string, now time.Time, limit int) ([]ownernotify.Notification, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT attempts, record FROM coordinator_owner_notifications
		WHERE sink = ? AND state = ? AND next_attempt_at <= ?
		ORDER BY created_at, id LIMIT ?`,
		sink, string(ownernotify.StatePending), ownerNotificationTime(now), limit)
	if err != nil {
		return nil, fmt.Errorf("read due owner notifications: %w", err)
	}
	defer rows.Close()
	var due []ownernotify.Notification
	for rows.Next() {
		var attempts int
		var raw string
		if err := rows.Scan(&attempts, &raw); err != nil {
			return nil, fmt.Errorf("read owner notification: %w", err)
		}
		var notification ownernotify.Notification
		if err := json.Unmarshal([]byte(raw), &notification); err != nil {
			return nil, fmt.Errorf("decode owner notification: %w", err)
		}
		notification.Attempts = attempts
		due = append(due, notification)
	}
	return due, rows.Err()
}

// SyncOwnerNotificationScopes implements ownernotify.Store. It deletes the
// watermark of every scope that no active sink declares.
func (s *Store) SyncOwnerNotificationScopes(ctx context.Context, active map[string][]ownernotify.Scope) error {
	keep := map[string]bool{}
	for sink, scopes := range active {
		for _, scope := range scopes {
			keep[sink+"\x00"+scope.Key] = true
		}
	}
	rows, err := s.db.QueryContext(ctx, `SELECT sink, scope FROM coordinator_owner_notification_scopes`)
	if err != nil {
		return fmt.Errorf("read owner notification watermarks: %w", err)
	}
	var stale [][2]string
	for rows.Next() {
		var sink, scope string
		if err := rows.Scan(&sink, &scope); err != nil {
			rows.Close()
			return fmt.Errorf("read owner notification watermark: %w", err)
		}
		if !keep[sink+"\x00"+scope] {
			stale = append(stale, [2]string{sink, scope})
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, key := range stale {
		if _, err := s.db.ExecContext(ctx,
			`DELETE FROM coordinator_owner_notification_scopes WHERE sink = ? AND scope = ?`, key[0], key[1]); err != nil {
			return fmt.Errorf("drop owner notification watermark: %w", err)
		}
	}
	return nil
}

// PruneOwnerNotifications implements ownernotify.Store. The watermarks move up
// to the bound in the same transaction as the delete, so nothing the delete
// removes can be detected again.
func (s *Store) PruneOwnerNotifications(ctx context.Context, before time.Time, limit int) (int, error) {
	cutoff := ownerNotificationTime(before)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin owner notification prune: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`UPDATE coordinator_owner_notification_scopes SET since = ?1 WHERE since < ?1`, cutoff); err != nil {
		return 0, fmt.Errorf("advance owner notification watermarks: %w", err)
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM coordinator_owner_notifications WHERE id IN (
		SELECT n.id FROM coordinator_owner_notifications n
		WHERE n.state IN ('delivered', 'abandoned', 'baseline', 'resolved') AND n.created_at < ?1
		  AND (n.event NOT IN ('supervision-escalated', 'gate-review')
		    OR NOT EXISTS (SELECT 1 FROM coordinator_workflow_runs r
		      WHERE r.id = n.run_id AND r.progress NOT IN `+ownerNotificationTerminal+`))
		LIMIT ?2)`, cutoff, limit)
	if err != nil {
		return 0, fmt.Errorf("prune owner notifications: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit owner notification prune: %w", err)
	}
	removed, _ := result.RowsAffected()
	return int(removed), nil
}

// OwnerNotificationHolds implements ownernotify.Store. A condition of a run
// that has since become terminal no longer holds.
func (s *Store) OwnerNotificationHolds(ctx context.Context, n ownernotify.Notification) (bool, error) {
	exists := func(query string, args ...any) (bool, error) {
		err := s.db.QueryRowContext(ctx, query, args...).Scan(new(int))
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return err == nil, err
	}
	live, err := exists(`SELECT 1 FROM coordinator_workflow_runs WHERE id = ? AND progress NOT IN `+ownerNotificationTerminal, n.RunID)
	if err != nil || !live {
		return false, err
	}
	incidentEscalated := func() (bool, error) {
		return exists(`SELECT 1 FROM coordinator_supervision_incidents WHERE id = ? AND state = 'escalated'`, n.IncidentID)
	}
	switch n.Event {
	case ownernotify.EventNeedsInput:
		return exists(`SELECT 1 FROM coordinator_task_waits WHERE id = ? AND json_extract(record, '$.settledAt') IS NULL`, n.WaitID)
	case ownernotify.EventSupervisionEscalated:
		switch {
		case n.ActivationID != "":
			if held, err := exists(`SELECT 1 FROM coordinator_supervision_activations
				WHERE id = ? AND state IN ('escalated', 'recovery-required')`, n.ActivationID); err != nil || held {
				return held, err
			}
			if n.IncidentID == "" {
				return false, nil
			}
			return incidentEscalated()
		case n.IncidentID != "":
			return incidentEscalated()
		case n.GateID != "":
			return exists(`SELECT 1 FROM coordinator_supervision_gates WHERE id = ? AND state = 'escalated'`, n.GateID)
		}
	case ownernotify.EventGateReview:
		return exists(`SELECT 1 FROM coordinator_supervision_gates WHERE id = ? AND state = 'ready-for-review'`, n.GateID)
	}
	return true, nil
}

// RecordOwnerNotificationAttempt implements ownernotify.Store. It changes only
// a pending row, so a delivered or abandoned row can never be revived.
func (s *Store) RecordOwnerNotificationAttempt(ctx context.Context, id string, result ownernotify.AttemptResult) error {
	next := result.NextAttemptAt
	if next.IsZero() {
		next = result.At
	}
	updated, err := s.db.ExecContext(ctx, `UPDATE coordinator_owner_notifications
		SET state = ?, attempts = ?, next_attempt_at = ?, last_error = ?, updated_at = ?
		WHERE id = ? AND state = ?`,
		string(result.State), result.Attempts, ownerNotificationTime(next), result.Error,
		ownerNotificationTime(result.At), id, string(ownernotify.StatePending))
	if err != nil {
		return fmt.Errorf("record owner notification attempt: %w", err)
	}
	if changed, _ := updated.RowsAffected(); changed != 1 {
		return fmt.Errorf("owner notification %s is no longer pending", id)
	}
	return nil
}
