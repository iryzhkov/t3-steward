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
// that changes when the same record needs the operator again (an activation's
// state, a gate's evidence snapshot). Detection inserts with that key and
// ignores a conflict, so a coordinator restart re-detects nothing it already
// recorded and loses nothing it had not yet sent.
//
// The baselines table records which (sink, event) pairs have started
// delivering. The first detection for a pair records what already exists as
// baseline rows that are never sent, so enabling a webhook on a coordinator
// with a long history does not replay that history into the channel.
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
CREATE TABLE IF NOT EXISTS coordinator_owner_notification_baselines(
	sink TEXT NOT NULL,
	event TEXT NOT NULL,
	baselined_at TEXT NOT NULL,
	PRIMARY KEY(sink, event)
);
`

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

// ownerNotificationCandidateQueries select, per event, every record that is
// in the reported condition and has no outbox row for this sink yet. The
// first parameter is the id prefix "sink|event|"; the NOT EXISTS against the
// primary key keeps a steady-state pass from re-reading history it already
// recorded.
var ownerNotificationCandidateQueries = map[ownernotify.Event]string{
	ownernotify.EventNeedsInput: `
SELECT w.id, json_extract(w.record, '$.workflowRunId'), w.record, 'wait'
FROM coordinator_task_waits w
WHERE json_extract(w.record, '$.settledAt') IS NULL
  AND json_extract(w.record, '$.kind') = 'attention'
  AND NOT EXISTS (SELECT 1 FROM coordinator_owner_notifications n WHERE n.id = ?1 || w.id)`,
	ownernotify.EventSupervisionEscalated: `
SELECT subject, run_id, record, kind FROM (
  SELECT 'incident:' || id AS subject, run_id, record, 'incident' AS kind
  FROM coordinator_supervision_incidents WHERE state = 'escalated'
  UNION ALL
  SELECT 'activation:' || id || ':' || state, run_id, record, 'activation'
  FROM coordinator_supervision_activations WHERE state IN ('escalated', 'recovery-required')
  UNION ALL
  SELECT 'gate:' || id || ':' || evidence_snapshot_id || ':escalated', run_id, record, 'gate'
  FROM coordinator_supervision_gates WHERE state = 'escalated'
) c
WHERE NOT EXISTS (SELECT 1 FROM coordinator_owner_notifications n WHERE n.id = ?1 || c.subject)`,
	ownernotify.EventGateReview: `
SELECT 'gate:' || g.id || ':' || g.evidence_snapshot_id, g.run_id, g.record, 'gate'
FROM coordinator_supervision_gates g
WHERE g.state = 'ready-for-review'
  AND NOT EXISTS (SELECT 1 FROM coordinator_owner_notifications n
    WHERE n.id = ?1 || 'gate:' || g.id || ':' || g.evidence_snapshot_id)`,
}

// ownerNotificationRunQuery selects terminal runs of one outcome.
const ownerNotificationRunQuery = `
SELECT r.id, r.id, r.record, 'run'
FROM coordinator_workflow_runs r
WHERE r.progress = ?2
  AND NOT EXISTS (SELECT 1 FROM coordinator_owner_notifications n WHERE n.id = ?1 || r.id)`

// DetectOwnerNotifications implements ownernotify.Store.
//
// Everything happens in one transaction, so a pass that records an event kind's
// baseline cannot interleave with a pass that records the same kind as pending.
func (s *Store) DetectOwnerNotifications(ctx context.Context, sink string, events []ownernotify.Event, now time.Time) (ownernotify.Detection, error) {
	var detection ownernotify.Detection
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return detection, fmt.Errorf("begin owner notification detection: %w", err)
	}
	defer tx.Rollback()
	enrich := ownerNotificationEnricher{tx: tx, campaigns: map[string]string{}, tasks: map[string]string{}}
	at := ownerNotificationTime(now)
	for _, event := range events {
		var baselined bool
		switch err := tx.QueryRowContext(ctx,
			`SELECT 1 FROM coordinator_owner_notification_baselines WHERE sink = ? AND event = ?`,
			sink, string(event)).Scan(new(int)); {
		case err == nil:
			baselined = true
		case !errors.Is(err, sql.ErrNoRows):
			return detection, fmt.Errorf("read owner notification baseline: %w", err)
		}
		candidates, err := ownerNotificationCandidatesTx(ctx, tx, sink, event)
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
				`INSERT INTO coordinator_owner_notification_baselines(sink, event, baselined_at) VALUES (?, ?, ?)`,
				sink, string(event), at); err != nil {
				return detection, fmt.Errorf("record owner notification baseline: %w", err)
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

func ownerNotificationCandidatesTx(ctx context.Context, tx *sql.Tx, sink string, event ownernotify.Event) ([]ownerNotificationCandidate, error) {
	prefix := ownerNotificationID(sink, event, "")
	var rows *sql.Rows
	var err error
	if progress, ok := ownerNotificationRunProgress(event); ok {
		rows, err = tx.QueryContext(ctx, ownerNotificationRunQuery, prefix, progress)
	} else if query, known := ownerNotificationCandidateQueries[event]; known {
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
