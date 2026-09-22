package sqlite

import (
	"context"
	"crypto/sha256"
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

// nodeStateRecords is what a node wait with a --state is evaluated against:
// the coordinator records plus the workers' last reports, which carry the
// pause evidence.
type nodeStateRecords struct {
	CoordinatorRecords
	Workers []domain.WorkerSnapshot
	// Buckets are the bucket observations with the workers' fresher readings
	// merged in, which is what a quota wait is evaluated against.
	Buckets []domain.BucketState
}

func nodeStateRecordsTx(ctx context.Context, tx *sql.Tx) (nodeStateRecords, error) {
	var r nodeStateRecords
	var err error
	if r.CoordinatorRecords, err = nodeRecordsTx(ctx, tx); err != nil {
		return r, err
	}
	if r.QuotaPools, err = loadJSON[domain.QuotaPool](ctx, tx, "coordinator_quota_pools"); err != nil {
		return r, err
	}
	if r.Workers, err = scanJSONRows[domain.WorkerSnapshot](ctx, tx, `SELECT record FROM coordinator_worker_snapshots ORDER BY worker_id`); err != nil {
		return r, fmt.Errorf("load worker snapshots: %w", err)
	}
	local, err := scanJSONRows[domain.BucketState](ctx, tx, `SELECT state FROM buckets ORDER BY key`)
	if err != nil {
		return r, fmt.Errorf("load bucket observations: %w", err)
	}
	r.Buckets = domain.MergeQuotaObservations(local, r.Workers)
	return r, nil
}

// scanJSONRows reads one JSON column of every row of a query.
func scanJSONRows[T any](ctx context.Context, tx *sql.Tx, query string) ([]T, error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []T
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var value T
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func resolveNodeState(condition domain.NodeWaitCondition, r nodeStateRecords) (domain.NodeObservation, error) {
	return domain.ResolveNodeState(condition.Target, condition.State, r.WorkflowRuns, r.Tasks, r.Attempts, r.Assignments, r.Workers)
}

// observeQuota evaluates a quota condition against the merged observations.
// The observation carries the pool's reading in Fields and the check-protocol
// exit code; a pool that is no longer configured gives up.
func observeQuota(condition domain.QuotaWaitCondition, r nodeStateRecords, now time.Time) (domain.NodeObservation, error) {
	observation := domain.NodeObservation{ExitCode: 1, Reason: "pending"}
	pool, err := domain.FindQuotaPool(r.QuotaPools, condition.Pool)
	if err != nil {
		return observation, err
	}
	reading := domain.ObserveQuotaPool(pool, r.Buckets)
	observation.Fields = domain.QuotaTrailerFields(reading)
	outcome, reason := condition.Evaluate(reading, now)
	observation.Reason = reason
	if outcome != "" {
		observation.Outcome = outcome
		observation.ExitCode = 0
	}
	return observation, nil
}

// quotaResetAt is the reset time a --reset wait records at registration: the
// earliest reset among the pool's buckets.
func quotaResetAt(condition domain.QuotaWaitCondition, r nodeStateRecords) (*time.Time, error) {
	pool, err := domain.FindQuotaPool(r.QuotaPools, condition.Pool)
	if err != nil {
		return nil, err
	}
	reading := domain.ObserveQuotaPool(pool, r.Buckets)
	if reading.Buckets == 0 {
		return nil, fmt.Errorf("quota pool %s has no observation yet, so there is no window to wait out; try --below or --phase normal", condition.Pool)
	}
	if reading.ResetsAt == nil {
		return nil, fmt.Errorf("quota pool %s reports no reset time; try --below or --phase normal", condition.Pool)
	}
	return reading.ResetsAt, nil
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
	records, err := nodeStateRecordsTx(ctx, tx)
	if err != nil {
		return w, err
	}
	var observation domain.NodeObservation
	if request.Quota != nil {
		if err := request.Quota.Validate(); err != nil {
			return w, err
		}
		if request.Quota.Reset && request.Quota.ResetAt == nil {
			if request.Quota.ResetAt, err = quotaResetAt(*request.Quota, records); err != nil {
				return w, err
			}
		}
		if observation, err = observeQuota(*request.Quota, records, now); err != nil {
			return w, err
		}
	} else {
		if observation, err = resolveNodeState(domain.NodeWaitCondition{Target: request.Target, State: request.State}, records); err != nil {
			return w, err
		}
		request.Target = observation.Target
	}
	w = domain.NodeWait{Registration: original, Request: request, Actor: actor, Host: host, RegisteredRevision: observation.RunRevision, CreatedAt: now.UTC(), Deadline: now.Add(request.Timeout).UTC(), DeliveryID: "node-wake:" + request.ID, Delivery: "pending"}
	if observation.ExitCode != 1 {
		w.Observation = &observation
		t := now.UTC()
		w.SettledAt = &t
	}
	if request.Quota == nil {
		if err = pinNodeTx(ctx, tx, "wait:"+request.ID, request.Target); err != nil {
			return w, err
		}
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

// SettleNodeWaits observes and publishes under one transaction, fencing
// retries. It is the coordinator's settlement pass for every coordinator
// kind: the interactive node waits, and the task-bound waits whose condition
// is structured (node, quota), which have no local check anywhere.
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
	records, err := nodeStateRecordsTx(ctx, tx)
	if err != nil {
		return err
	}
	for _, w := range waits {
		if w.SettledAt != nil || w.Delivery == "cancelled" {
			continue
		}
		var obs domain.NodeObservation
		var e error
		if w.Request.Quota != nil {
			obs, e = observeQuota(*w.Request.Quota, records, now)
		} else {
			obs, e = resolveNodeState(domain.NodeWaitCondition{Target: w.Request.Target, State: w.Request.State}, records)
		}
		if e != nil {
			obs = domain.NodeObservation{Target: w.Request.Target, ExitCode: 2, Reason: e.Error(), Outcome: domain.TaskWaitGaveUp}
		}
		if !now.Before(w.Deadline) {
			obs.ExitCode = 2
			obs.Reason = "timed out"
			obs.Outcome = domain.TaskWaitTimedOut
			if w.Request.OrTimeout {
				// --or-timeout: the deadline is an expected end of the wait, as
				// ExpireTaskWaits records it for a task-bound wait.
				obs.ExitCode = 0
				obs.Reason = fmt.Sprintf("the deadline of %s passed, which this wait treats as a normal outcome (--or-timeout)", w.Request.Timeout)
			}
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
	if err := settleStructuredTaskWaitsTx(ctx, tx, records, now); err != nil {
		return err
	}
	return tx.Commit()
}

// settleStructuredTaskWaitsTx settles the live task-bound waits of the
// coordinator kinds from the coordinator's own records. Expiry is left to
// ExpireTaskWaits, which knows about --or-timeout.
//
// It also sweeps every kind for a live wait whose attempt is already
// terminal: a cancellation whose wait settlement was lost to a crash between
// the command application and the settlement, or any other path that ended
// the attempt. Such a wait is settled cancelled here, on the next boundary
// tick, rather than left live until its deadline.
func settleStructuredTaskWaitsTx(ctx context.Context, tx *sql.Tx, records nodeStateRecords, now time.Time) error {
	waits, err := loadJSON[domain.TaskWait](ctx, tx, "coordinator_task_waits")
	if err != nil {
		return err
	}
	attempts := make(map[string]domain.Attempt, len(records.Attempts))
	for _, attempt := range records.Attempts {
		attempts[attempt.ID] = attempt
	}
	for _, wait := range waits {
		if !wait.Live() {
			continue
		}
		if attempt, ok := attempts[wait.AttemptID]; ok && attempt.Progress.Terminal() {
			result := domain.TaskWaitResult{Outcome: domain.TaskWaitCancelled, ExitCode: 2,
				Reason: fmt.Sprintf("the attempt ended (%s) while the wait was live", attempt.Progress)}
			if _, err := settleTaskWaitTx(ctx, tx, wait, result, now); err != nil {
				return err
			}
			continue
		}
		if !wait.Kind.Coordinator() {
			continue
		}
		var result *domain.TaskWaitResult
		switch {
		case wait.Node != nil:
			obs, err := resolveNodeState(*wait.Node, records)
			switch {
			case err != nil:
				// The target is gone from the coordinator's records: nothing
				// will ever settle the condition, so the wait gives up.
				result = &domain.TaskWaitResult{Outcome: domain.TaskWaitGaveUp, ExitCode: 2, Reason: err.Error(),
					Fields: domain.NodeTrailerFields(domain.NodeObservation{Target: wait.Node.Target})}
			case obs.Outcome != "":
				result = &domain.TaskWaitResult{Outcome: obs.Outcome, ExitCode: obs.ExitCode, Reason: obs.Reason, Fields: domain.NodeTrailerFields(obs)}
			}
		case wait.Quota != nil:
			obs, err := observeQuota(*wait.Quota, records, now)
			switch {
			case err != nil:
				// The pool left the configuration: nothing will observe it again.
				result = &domain.TaskWaitResult{Outcome: domain.TaskWaitGaveUp, ExitCode: 2, Reason: err.Error(), Fields: map[string]string{"pool": wait.Quota.Pool}}
			case obs.Outcome != "":
				result = &domain.TaskWaitResult{Outcome: obs.Outcome, ExitCode: obs.ExitCode, Reason: obs.Reason, Fields: obs.Fields}
			}
		}
		if result == nil {
			continue
		}
		if _, err := settleTaskWaitTx(ctx, tx, wait, *result, now); err != nil {
			return err
		}
	}
	return nil
}

// TransitionNodeWake is the authoritative delivery state boundary. Every caller,
// including admin transports, receives the same legal-transition guard.
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
	settled, allowed := w.SettledAt != nil, false
	switch from {
	case "pending":
		allowed = settled && (to == "held" || to == "offline" || to == "busy" || to == "sending" || to == "rejected") || to == "cancelled"
	case "held", "offline", "busy":
		allowed = settled && (to == "offline" || to == "busy" || to == "sending" || to == "rejected") || to == "cancelled"
	case "sending":
		allowed = to == "delivered" || to == "recovery-required" || to == "offline" || to == "rejected"
	case "recovery-required":
		allowed = to == "delivered" || to == "offline" || to == "rejected"
	}
	if !allowed {
		return false, fmt.Errorf("invalid wake transition %s to %s", from, to)
	}
	w.Delivery = to
	switch to {
	case "sending":
		w.DeliveryAttempts++
		w.DeliveryError = ""
		w.DeliveryNextAction = "reconcile the durable delivery receipt"
		w.DeliveryNextAttemptAt = nil
	case "offline", "busy":
		w.DeliveryError = "delivery target is " + to
		w.DeliveryNextAction = "retry after the target becomes available"
		next := now.UTC().Add(deliveryRetryDelay(w.DeliveryAttempts))
		w.DeliveryNextAttemptAt = &next
	case "recovery-required":
		w.DeliveryError = "delivery outcome is unknown"
		w.DeliveryNextAction = "reconcile the durable receipt; do not resend without known non-effect"
		w.DeliveryNextAttemptAt = nil
	case "delivered":
		w.DeliveryError = ""
		w.DeliveryNextAction = ""
		w.DeliveryNextAttemptAt = nil
		t := now.UTC()
		w.DeliveredAt = &t
	case "rejected":
		w.DeliveryError = "delivery was rejected"
		w.DeliveryNextAction = "operator action is required"
		w.DeliveryNextAttemptAt = nil
	}
	if err = saveNodeWaitTx(ctx, tx, w); err != nil {
		return false, err
	}
	if to == "delivered" || to == "cancelled" || to == "rejected" {
		if _, err = tx.ExecContext(ctx, "DELETE FROM coordinator_retention_pins WHERE owner=?", "wait:"+id); err != nil {
			return false, err
		}
	}
	return true, tx.Commit()
}

// ClaimNodeWakeGroup atomically freezes one external effect and claims every
// wake-all member represented by it. A replay must name identical bytes,
// digest, membership, and delivery identity.
func (s *Store) ClaimNodeWakeGroup(ctx context.Context, leaderID, from, deliveryID, payload string, memberIDs []string, now time.Time) (bool, error) {
	if len(memberIDs) == 0 {
		return false, fmt.Errorf("claim node wake group %q: no members", leaderID)
	}
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(payload)))
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	for _, id := range memberIDs {
		var raw []byte
		if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_node_waits WHERE id=?", id).Scan(&raw); err != nil {
			return false, fmt.Errorf("claim node wake member %q: %w", id, err)
		}
		var wake domain.NodeWait
		if err := json.Unmarshal(raw, &wake); err != nil {
			return false, err
		}
		expected := wake.Delivery
		if id == leaderID && expected != from {
			return false, nil
		}
		if expected != "pending" && expected != "held" && expected != "offline" && expected != "busy" {
			return false, nil
		}
		if wake.DeliveryPayload != "" && (wake.DeliveryPayload != payload || wake.DeliveryPayloadDigest != digest ||
			wake.DeliveryID != deliveryID || !reflect.DeepEqual(wake.DeliveryGroupMembers, memberIDs)) {
			return false, fmt.Errorf("claim node wake %q: frozen delivery differs", id)
		}
		wake.Delivery = "sending"
		wake.DeliveryID = deliveryID
		wake.DeliveryPayload = payload
		wake.DeliveryPayloadDigest = digest
		wake.DeliveryGroupMembers = append([]string(nil), memberIDs...)
		wake.DeliveryAttempts++
		wake.DeliveryError = ""
		wake.DeliveryNextAction = "reconcile the durable delivery receipt"
		wake.DeliveryNextAttemptAt = nil
		encoded, err := json.Marshal(wake)
		if err != nil {
			return false, err
		}
		result, err := tx.ExecContext(ctx, "UPDATE coordinator_node_waits SET record=? WHERE id=? AND json_extract(record, '$.delivery')=?", encoded, id, expected)
		if err != nil {
			return false, err
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return false, nil
		}
	}
	return true, tx.Commit()
}

func deliveryRetryDelay(attempts int) time.Duration {
	if attempts < 0 {
		attempts = 0
	}
	if attempts > 6 {
		attempts = 6
	}
	return time.Second * time.Duration(1<<attempts)
}
