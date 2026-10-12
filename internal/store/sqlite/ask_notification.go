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
func enqueueAskNotificationTx(ctx context.Context, tx *sql.Tx, ask domain.TaskWait, now time.Time) error {
	if ask.Kind != domain.WaitKindAsk || ask.Ask == nil {
		return nil
	}
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT record FROM coordinator_node_waits
 WHERE json_extract(record,'$.request.target.runId')=?
 AND json_extract(record,'$.request.target.taskId') IN (?,?)
 AND json_extract(record,'$.request.threadId')!=''
 AND json_extract(record,'$.host')!=''
 AND json_extract(record,'$.actor')!=''
 AND substr(id,1,?)!=?
 ORDER BY (json_extract(record,'$.settledAt') IS NOT NULL), id LIMIT 1`,
		ask.WorkflowRunID, domain.SinkTaskName, domain.SinkTaskID(ask.WorkflowRunID),
		len(askNotificationPrefix), askNotificationPrefix).Scan(&raw)
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
	id := fmt.Sprintf("%s%x", askNotificationPrefix, sha256.Sum256([]byte(ask.ID+":registered")))
	target := domain.NodeRef{RunID: ask.WorkflowRunID, TaskID: ask.TaskID}
	request := domain.NodeWaitRequest{ID: id, ThreadID: recipient.Request.ThreadID,
		Name: "ask " + ask.ID + " registered", Target: target, Timeout: ask.MaxDuration}
	payload := askNotificationPayload(ask)
	settled := now.UTC()
	event := domain.NodeWait{
		Registration: request, Request: request, Actor: recipient.Actor, Host: recipient.Host,
		CreatedAt: settled, Deadline: ask.Deadline, SettledAt: &settled,
		Observation: &domain.NodeObservation{Target: target, ExitCode: 0, Reason: "needs-input: ask registered"},
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

func askShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
