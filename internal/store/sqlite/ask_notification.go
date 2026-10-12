package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const askNotificationPrefix = "nw-ask-event-"

// enqueueAskNotificationTx publishes attention, not task completion. Only an
// explicit run-sink subscription supplies a recipient; worker/relay identity
// is deliberately irrelevant.
func askNotificationID(askID, phase string) string {
	return fmt.Sprintf("%s%x", askNotificationPrefix, sha256.Sum256([]byte(askID+":"+phase)))
}

func enqueueAskNotificationTx(ctx context.Context, tx *sql.Tx, ask domain.TaskWait, phase string, now time.Time) error {
	if ask.Kind != domain.WaitKindAsk || ask.Ask == nil {
		return nil
	}
	id := askNotificationID(ask.ID, phase)
	var exists bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM coordinator_node_waits WHERE id=?)", id).Scan(&exists); err != nil || exists {
		return err
	}
	var raw []byte
	// Once registered, keep the original recipient even if subscriptions change.
	err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_node_waits WHERE id=?", askNotificationID(ask.ID, "registered")).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx, `SELECT record FROM coordinator_node_waits
 WHERE json_extract(record,'$.request.target.runId')=?
 AND json_extract(record,'$.request.target.taskId') IN (?,?)
 AND json_extract(record,'$.request.threadId')!=''
 AND json_extract(record,'$.host')!=''
 AND json_extract(record,'$.actor')!=''
 AND substr(id,1,?)!=?
 ORDER BY (json_extract(record,'$.settledAt') IS NOT NULL), id LIMIT 1`,
			ask.WorkflowRunID, domain.SinkTaskName, domain.SinkTaskID(ask.WorkflowRunID),
			len(askNotificationPrefix), askNotificationPrefix).Scan(&raw)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var recipient domain.NodeWait
	if err := json.Unmarshal(raw, &recipient); err != nil {
		return err
	}
	target := domain.NodeRef{RunID: ask.WorkflowRunID, TaskID: ask.TaskID}
	request := domain.NodeWaitRequest{ID: id, ThreadID: recipient.Request.ThreadID,
		Name: "ask " + ask.ID + " " + phase, Target: target, Timeout: ask.MaxDuration}
	payload := askNotificationPayload(ask)
	if phase == "midpoint" {
		payload = "Reminder: this ask remains open at half its deadline.\n" + payload
	}
	settled := now.UTC()
	event := domain.NodeWait{
		Registration: request, Request: request, Actor: recipient.Actor, Host: recipient.Host,
		CreatedAt: settled, Deadline: ask.Deadline, SettledAt: &settled,
		Observation: &domain.NodeObservation{Target: target, ExitCode: 0, Reason: "needs-input: ask " + phase},
		Delivery:    "pending", DeliveryID: "node-wake:" + id, DeliveryPayload: payload,
		DeliveryPayloadDigest: fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(payload))),
		DeliveryGroupMembers:  []string{id},
	}
	if err := pinNodeTx(ctx, tx, "wait:"+id, target); err != nil {
		return err
	}
	return saveNodeWaitTx(ctx, tx, event)
}

func askNotificationPayload(ask domain.TaskWait) string {
	var text strings.Builder
	fmt.Fprintf(&text, "Needs input: run %s awaits an answer.\nTask: %s\nAsk: %s\nDeadline: %s UTC\n\n%s\n",
		ask.WorkflowRunID, ask.TaskID, ask.ID, ask.Deadline.UTC().Format("2006-01-02 15:04:05"), ask.Ask.Question)
	command := "t3-steward"
	if ask.Ask.Requires == domain.AskRequiresApprover {
		command += " --config <approver-config-path>"
		text.WriteString("\nThis ask requires an approver. Replace <approver-config-path> with the approver client's configuration path.\n")
	}
	if ask.Ask.Multi {
		text.WriteString("\nMulti: choose multiple options by repeating --option flags in one answer command.\n")
	}
	for _, option := range ask.Ask.Options {
		fmt.Fprintf(&text, "\n%s ask answer %s --option %s\n", command, askShellQuote(ask.ID), askShellQuote(option))
	}
	return text.String()
}

// Reconcile on the normal settlement tick, including after a restart. Load only
// open, unexpired asks; durable registration time defines the midpoint.
func reconcileAskNotificationsTx(ctx context.Context, tx *sql.Tx, now time.Time) error {
	asks, err := loadJSONQuery[domain.TaskWait](ctx, tx, "coordinator_task_waits", `SELECT record FROM coordinator_task_waits
 WHERE json_extract(record,'$.kind')='ask'
 AND json_extract(record,'$.settledAt') IS NULL
 AND julianday(json_extract(record,'$.deadline')) > julianday(?) ORDER BY id`, now.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	for _, ask := range asks {
		if !now.Before(ask.Deadline) {
			continue
		}
		if err := enqueueAskNotificationTx(ctx, tx, ask, "registered", now); err != nil {
			return err
		}
		midpoint := ask.RegisteredAt.Add(ask.Deadline.Sub(ask.RegisteredAt) / 2)
		if !now.Before(midpoint) {
			if err := enqueueAskNotificationTx(ctx, tx, ask, "midpoint", now); err != nil {
				return err
			}
		}
	}
	return nil
}

// Only unclaimed attention can be cancelled. Sending and uncertain receipts
// retain their identity and pins until normal delivery reconciliation.
func cancelAskNotificationsTx(ctx context.Context, tx *sql.Tx, askID string) error {
	for _, phase := range []string{"registered", "midpoint"} {
		id := askNotificationID(askID, phase)
		result, err := tx.ExecContext(ctx, `UPDATE coordinator_node_waits
 SET record=json_set(record,'$.delivery','cancelled')
 WHERE id=? AND json_extract(record,'$.delivery') IN ('pending','held','offline','busy')
 AND COALESCE(json_extract(record,'$.deliveryAttempts'),0)=0`, id)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n > 0 {
			if _, err := tx.ExecContext(ctx, "DELETE FROM coordinator_retention_pins WHERE owner=?", "wait:"+id); err != nil {
				return err
			}
		}
	}
	return nil
}

func askShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
