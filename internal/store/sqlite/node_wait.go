package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const coordinatorMigrationV13 = `
CREATE TABLE IF NOT EXISTS coordinator_node_waits(id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, record TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS coordinator_retention_pins(owner TEXT NOT NULL, run_id TEXT NOT NULL, task_id TEXT NOT NULL, PRIMARY KEY(owner,run_id,task_id));
CREATE TRIGGER IF NOT EXISTS pinned_run_delete BEFORE DELETE ON coordinator_workflow_runs
 WHEN EXISTS(SELECT 1 FROM coordinator_retention_pins WHERE run_id=OLD.id)
 BEGIN SELECT RAISE(ABORT,'run retained by node reference'); END;
CREATE TRIGGER IF NOT EXISTS pinned_task_delete BEFORE DELETE ON coordinator_tasks
 WHEN EXISTS(SELECT 1 FROM coordinator_retention_pins AS pin JOIN coordinator_workflow_runs AS run ON run.id=pin.run_id WHERE pin.task_id=OLD.id OR run.workflow_id=OLD.workflow_id)
 BEGIN SELECT RAISE(ABORT,'task retained by node reference'); END;
CREATE TRIGGER IF NOT EXISTS pinned_attempt_delete BEFORE DELETE ON coordinator_attempts
 WHEN EXISTS(SELECT 1 FROM coordinator_retention_pins WHERE run_id=OLD.workflow_run_id)
 BEGIN SELECT RAISE(ABORT,'attempt retained by node reference'); END;
CREATE TRIGGER IF NOT EXISTS pinned_assignment_delete BEFORE DELETE ON coordinator_assignments
 WHEN EXISTS(SELECT 1 FROM coordinator_retention_pins AS pin JOIN coordinator_attempts AS attempt ON attempt.workflow_run_id=pin.run_id WHERE attempt.id=OLD.attempt_id)
 BEGIN SELECT RAISE(ABORT,'assignment retained by node reference'); END;
CREATE TRIGGER IF NOT EXISTS release_node_edges AFTER DELETE ON coordinator_workflow_runs
 BEGIN DELETE FROM coordinator_retention_pins WHERE owner='edge:' || OLD.id; END;
CREATE TRIGGER IF NOT EXISTS pinned_artifact_delete BEFORE DELETE ON coordinator_artifacts
 WHEN EXISTS(SELECT 1 FROM coordinator_retention_pins WHERE run_id=json_extract(OLD.record,'$.workflowRunId'))
 BEGIN SELECT RAISE(ABORT,'artifact retained by node reference'); END;
`

func nodeRecordsTx(ctx context.Context, tx *sql.Tx) (CoordinatorRecords, error) {
	var r CoordinatorRecords
	var err error
	if r.WorkflowRuns, err = loadJSON[domain.WorkflowRun](ctx, tx, "coordinator_workflow_runs"); err != nil {
		return r, err
	}
	if r.Tasks, err = loadJSON[domain.Task](ctx, tx, "coordinator_tasks"); err != nil {
		return r, err
	}
	if r.Attempts, err = loadJSON[domain.Attempt](ctx, tx, "coordinator_attempts"); err != nil {
		return r, err
	}
	r.Assignments, err = loadJSON[domain.Assignment](ctx, tx, "coordinator_assignments")
	return r, err
}
func resolveNodeRecords(ref domain.NodeRef, r CoordinatorRecords) (domain.NodeObservation, error) {
	return domain.ResolveNode(ref, r.WorkflowRuns, r.Tasks, r.Attempts, r.Assignments)
}
func saveNodeWaitTx(ctx context.Context, tx *sql.Tx, w domain.NodeWait) error {
	raw, err := json.Marshal(w)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO coordinator_node_waits(id,thread_id,record) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET record=excluded.record", w.Request.ID, w.Request.ThreadID, raw)
	return err
}
func pinNodeTx(ctx context.Context, tx *sql.Tx, owner string, ref domain.NodeRef) error {
	_, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO coordinator_retention_pins(owner,run_id,task_id) VALUES(?,?,?)", owner, ref.RunID, ref.TaskID)
	return err
}
func (s *Store) RegisterNodeWait(ctx context.Context, request domain.NodeWaitRequest, actor, host string, now time.Time) (domain.NodeWait, error) {
	var w domain.NodeWait
	original := request
	if request.ID == "" || len(request.ID) > 128 || request.ThreadID == "" || len(request.ThreadID) > 256 || len(request.Name) > 1000 || request.Timeout <= 0 || request.Timeout > 30*24*time.Hour || actor == "" || host == "" || now.IsZero() {
		return w, errors.New("invalid native wait registration")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return w, err
	}
	defer tx.Rollback()
	var raw []byte
	err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_node_waits WHERE id=?", request.ID).Scan(&raw)
	if err == nil {
		if err = json.Unmarshal(raw, &w); err != nil {
			return w, err
		}
		// Replay does not require source metadata after delivery released its pin.
		if (!reflect.DeepEqual(w.Request, request) && !reflect.DeepEqual(w.Registration, request)) || w.Actor != actor || w.Host != host {
			return w, errors.New("wait ID replay changed registration")
		}
		return w, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return w, err
	}
	records, err := nodeRecordsTx(ctx, tx)
	if err != nil {
		return w, err
	}
	observation, err := resolveNodeRecords(request.Target, records)
	if err != nil {
		return w, err
	}
	request.Target = observation.Target
	w = domain.NodeWait{Registration: original, Request: request, Actor: actor, Host: host, RegisteredRevision: observation.RunRevision, CreatedAt: now.UTC(), Deadline: now.Add(request.Timeout).UTC(), DeliveryID: "node-wake:" + request.ID, Delivery: "pending"}
	if observation.ExitCode != 1 {
		w.Observation = &observation
		t := now.UTC()
		w.SettledAt = &t
	}
	if err = pinNodeTx(ctx, tx, "wait:"+request.ID, request.Target); err != nil {
		return w, err
	}
	if err = saveNodeWaitTx(ctx, tx, w); err != nil {
		return w, err
	}
	return w, tx.Commit()
}
func (s *Store) ListNodeWaits(ctx context.Context) ([]domain.NodeWait, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	values, err := loadJSON[domain.NodeWait](ctx, tx, "coordinator_node_waits")
	if err != nil {
		return nil, err
	}
	return values, tx.Commit()
}

// SettleNodeWaits observes and publishes under one transaction, fencing retries.
func (s *Store) SettleNodeWaits(ctx context.Context, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	waits, err := loadJSON[domain.NodeWait](ctx, tx, "coordinator_node_waits")
	if err != nil {
		return err
	}
	records, err := nodeRecordsTx(ctx, tx)
	if err != nil {
		return err
	}
	for _, w := range waits {
		if w.SettledAt != nil || w.Delivery == "cancelled" {
			continue
		}
		obs, e := resolveNodeRecords(w.Request.Target, records)
		if e != nil {
			obs = domain.NodeObservation{Target: w.Request.Target, ExitCode: 2, Reason: e.Error()}
		}
		if !now.Before(w.Deadline) {
			obs.ExitCode = 2
			obs.Reason = "timed out"
		}
		if obs.ExitCode == 1 {
			continue
		}
		t := now.UTC()
		w.Observation = &obs
		w.SettledAt = &t
		if err = saveNodeWaitTx(ctx, tx, w); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// TransitionNodeWake fences delivery ownership. Once sending is durable, lost
// replies require positive observation; absence never authorizes a second send.
func (s *Store) TransitionNodeWake(ctx context.Context, id, from, to string, now time.Time) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_node_waits WHERE id=?", id).Scan(&raw); err != nil {
		return false, err
	}
	var w domain.NodeWait
	if err = json.Unmarshal(raw, &w); err != nil {
		return false, err
	}
	if w.Delivery != from {
		return false, nil
	}
	allowed := to == "cancelled" && from != "delivered" && from != "sending" && from != "recovery-required" ||
		w.SettledAt != nil && (from == "pending" && (to == "held" || to == "sending") || from == "held" && to == "sending" || (from == "sending" || from == "recovery-required") && (to == "delivered" || to == "recovery-required"))
	if !allowed {
		return false, fmt.Errorf("invalid wake transition %s to %s", from, to)
	}
	w.Delivery = to
	if to == "delivered" {
		t := now.UTC()
		w.DeliveredAt = &t
	}
	if err = saveNodeWaitTx(ctx, tx, w); err != nil {
		return false, err
	}
	// Keep source pins through recovery. Delivered/cancelled waits retain their
	// immutable observation but no longer need source metadata.
	if to == "delivered" || to == "cancelled" {
		if _, err = tx.ExecContext(ctx, "DELETE FROM coordinator_retention_pins WHERE owner=?", "wait:"+id); err != nil {
			return false, err
		}
	}
	return true, tx.Commit()
}
