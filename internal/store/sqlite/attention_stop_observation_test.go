package sqlite

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Count both statements and returned rows so a scoped query cannot hide a scan.
type attentionStopReads struct {
	queries      map[string]int
	rows         map[string]int
	ownershipIDs []string
}

type attentionStopConnector struct {
	driver.Connector
	reads *attentionStopReads
}

func (c attentionStopConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &attentionStopConn{Conn: conn, reads: c.reads}, nil
}

type attentionStopConn struct {
	driver.Conn
	reads *attentionStopReads
}

func (c *attentionStopConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func (c *attentionStopConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *attentionStopConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	query = strings.Join(strings.Fields(query), " ")
	c.reads.queries[query]++
	if strings.Contains(query, "coordinator_attempts") || strings.Contains(query, "coordinator_assignments WHERE attempt_id") {
		for _, arg := range args {
			c.reads.ownershipIDs = append(c.reads.ownershipIDs, fmt.Sprint(arg.Value))
		}
	}
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return &attentionStopRows{Rows: rows, query: query, reads: c.reads}, nil
}

type attentionStopRows struct {
	driver.Rows
	query string
	reads *attentionStopReads
}

func (r *attentionStopRows) Next(values []driver.Value) error {
	err := r.Rows.Next(values)
	if err == nil {
		r.reads.rows[r.query]++
	}
	return err
}

func (r *attentionStopReads) reset() {
	r.queries = map[string]int{}
	r.rows = map[string]int{}
	r.ownershipIDs = nil
}

func openAttentionStopCountingStore(t *testing.T) (*Store, *attentionStopReads) {
	t.Helper()
	reads := &attentionStopReads{}
	reads.reset()
	store, err := OpenMigratedInstrumented(copyFixtureSchema(t), func(c driver.Connector) driver.Connector {
		return attentionStopConnector{Connector: c, reads: reads}
	})
	requireAttentionStopOK(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store, reads
}

// Seed the durable observation inputs directly, without replaying admin delivery.
func seedAttentionStopObservation(t *testing.T, store *Store, id string, state domain.AttentionReceiptState, live bool, at time.Time, epoch int64) (domain.ThrottleAttemptRecord, domain.Attempt, domain.Assignment) {
	t.Helper()
	attempt := domain.Attempt{
		ID: id, WorkflowRunID: "run-" + id, TaskID: "task-" + id, Number: 1,
		AssignmentID: "assignment-" + id, Revision: 2,
		Progress: domain.ProgressCancelled, Control: domain.ControlStopped,
	}
	assignment := domain.Assignment{
		ID: attempt.AssignmentID, AttemptID: attempt.ID, DispatchToken: "dispatch-" + id,
		Epoch: 1, WorkerID: "worker", WorkerEpoch: "worker-epoch", ThreadID: "thread-" + id,
		State: domain.AssignmentReleased,
	}
	if live {
		attempt.Control = domain.ControlDraining
		assignment.State = domain.AssignmentClaimed
	}
	record := domain.ThrottleAttemptRecord{
		DirectiveID: "directive-" + id, AttemptID: id, Revision: 1,
		Delivery: domain.ThrottleDeliveryAcknowledged, Result: domain.ThrottleResultStopped, AcknowledgedAt: &at,
		Command: domain.ThrottleCommand{
			ID: "command-" + id, AttemptID: id, AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
			WorkerID: assignment.WorkerID, ThreadID: assignment.ThreadID, WorkspacePath: "/workspace",
			AttentionStop: &domain.AttentionStopCommand{
				WaitID: "wait-" + id, DecisionID: "decision-" + id,
				WorkflowRunID: attempt.WorkflowRunID, TaskID: attempt.TaskID,
				CoordinatorEpoch: epoch, AppliedRevision: attempt.Revision,
			},
		},
	}
	record.Command.AttentionStop.CommandDigest = domain.AttentionStopCommandDigest(record.Command)
	wait := domain.TaskWait{ID: record.Command.AttentionStop.WaitID, RequestID: "request-" + id, AttemptID: id}
	if state != "" {
		wait.AttentionReceipts = []domain.AttentionReceipt{{
			Decision:  domain.AttentionDecision{ID: record.Command.AttentionStop.DecisionID},
			CommandID: record.Command.ID, CommandDigest: record.Command.AttentionStop.CommandDigest, State: state,
		}}
	}
	ctx := context.Background()
	requireAttentionStopOK(t, store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}))
	tx, err := store.db.BeginTx(ctx, nil)
	requireAttentionStopOK(t, err)
	defer tx.Rollback()
	requireAttentionStopOK(t, upsertJSON(ctx, tx, "throttle", record.AttemptID,
		"INSERT INTO coordinator_throttle_attempts(directive_id,attempt_id,revision,delivery,record) VALUES(?,?,?,?,?)",
		[]any{record.DirectiveID, record.AttemptID, record.Revision, record.Delivery}, record))
	requireAttentionStopOK(t, saveTaskWaitTx(ctx, tx, wait))
	requireAttentionStopOK(t, tx.Commit())
	return record, attempt, assignment
}

func attentionObservationSnapshot(epoch int64, sequence int64, at time.Time) domain.WorkerSnapshot {
	return domain.WorkerSnapshot{WorkerID: "worker", WorkerEpoch: "worker-epoch", CoordinatorEpoch: epoch,
		Sequence: sequence, Connected: true, Inventory: domain.WorkerInventory{ID: "worker"}, ObservedAt: at, ValidUntil: at.Add(time.Hour)}
}

func TestAttentionStopObservationHistoryDoesNotAddOwnershipReads(t *testing.T) {
	t.Parallel()
	var baseline map[string]int
	for _, history := range []bool{false, true} {
		store, reads := openAttentionStopCountingStore(t)
		ctx := context.Background()
		epoch, err := store.AcquireCoordinator(ctx, "observer")
		requireAttentionStopOK(t, err)
		at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
		if history {
			var records CoordinatorRecords
			for i := range 500 {
				id := fmt.Sprintf("history-%04d", i)
				records.Attempts = append(records.Attempts, domain.Attempt{ID: id, WorkflowRunID: "history", TaskID: id, Number: 1, Control: domain.ControlStopped})
				records.Assignments = append(records.Assignments, domain.Assignment{ID: "assignment-" + id, AttemptID: id, DispatchToken: id, State: domain.AssignmentReleased})
			}
			requireAttentionStopOK(t, store.SaveCoordinatorRecords(ctx, records))
			_, err := store.db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (
				SELECT 1 UNION ALL SELECT n+1 FROM history WHERE n<1000
			) INSERT INTO coordinator_throttle_attempts(directive_id,attempt_id,revision,delivery,record)
			SELECT 'throttle-'||n, 'throttle-'||n, 1, 'acknowledged',
				json_object('directiveId', 'throttle-'||n, 'attemptId', 'throttle-'||n,
					'delivery', 'acknowledged', 'result', 'stopped') FROM history`)
			requireAttentionStopOK(t, err)
		}
		for i := range 20 {
			seedAttentionStopObservation(t, store, fmt.Sprintf("observed-%02d", i), domain.AttentionObserved, false, at, epoch)
		}
		record, _, _ := seedAttentionStopObservation(t, store, "live", domain.AttentionDelivered, true, at, epoch)
		snapshot := attentionObservationSnapshot(epoch, 1, at.Add(time.Minute))
		snapshot.Assignments = []domain.WorkerAssignmentObservation{{
			AssignmentID: record.Command.AssignmentID, AssignmentEpoch: record.Command.AssignmentEpoch,
			State: domain.AssignmentReleased, Control: domain.ControlStopped,
			ThreadID: record.Command.ThreadID, WorkspacePath: record.Command.WorkspacePath, ObservedAt: snapshot.ObservedAt,
			Journal: &domain.WorkerJournalExcerpt{Phase: "stopped"},
		}}
		reads.reset()
		requireAttentionStopOK(t, store.SaveWorkerSnapshot(ctx, snapshot))
		for _, query := range []string{"SELECT record FROM coordinator_attempts ORDER BY id", "SELECT record FROM coordinator_assignments ORDER BY id", "SELECT record FROM coordinator_throttle_attempts ORDER BY directive_id, attempt_id"} {
			if reads.queries[query] != 0 {
				t.Fatalf("history=%v full-table query %q executed", history, query)
			}
		}
		for _, id := range reads.ownershipIDs {
			if strings.Contains(id, "observed-") || strings.Contains(id, "history") {
				t.Fatalf("history=%v read ownership for %s", history, id)
			}
		}
		counts := map[string]int{}
		for query, rows := range reads.rows {
			if strings.Contains(query, "coordinator_attempts") || strings.Contains(query, "coordinator_assignments") || strings.Contains(query, "coordinator_throttle_attempts") {
				counts[query] = rows
			}
		}
		if history {
			if fmt.Sprint(counts) != fmt.Sprint(baseline) {
				t.Fatalf("history changed returned rows: baseline=%v history=%v", baseline, counts)
			}
		} else {
			baseline = counts
		}
		if got := latestAttentionReceipt(t, store, "wait-live"); got.State != domain.AttentionObserved {
			t.Fatalf("live stop did not advance: %+v", got)
		}
	}
}

func TestAttentionStopTerminalReceiptAndOwnership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		state     domain.AttentionReceiptState
		block     bool
		wantError bool
	}{
		{name: "delivered", state: domain.AttentionDelivered},
		{name: "delivered with live attempt", state: domain.AttentionDelivered, block: true},
		{name: "missing receipt", wantError: true},
		{name: "missing receipt with live attempt", block: true},
		{name: "already observed", state: domain.AttentionObserved, block: true},
		{name: "not delivered", state: domain.AttentionApplied, block: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, reads := openAttentionStopCountingStore(t)
			ctx := context.Background()
			epoch, err := store.AcquireCoordinator(ctx, "observer")
			requireAttentionStopOK(t, err)
			at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
			_, attempt, _ := seedAttentionStopObservation(t, store, "terminal", tc.state, false, at, epoch)
			other := domain.Attempt{ID: "other", WorkflowRunID: attempt.WorkflowRunID, TaskID: attempt.TaskID, Number: 2, Control: domain.ControlRunning}
			if tc.block {
				requireAttentionStopOK(t, store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{other}}))
			}
			reads.reset()
			err = store.SaveWorkerSnapshot(ctx, attentionObservationSnapshot(epoch, 1, at.Add(time.Minute)))
			if tc.wantError {
				if err == nil || err.Error() != "attention stop observation has no matching delivered receipt" {
					t.Fatalf("missing receipt error = %v", err)
				}
				return
			}
			requireAttentionStopOK(t, err)
			if tc.state == domain.AttentionObserved || tc.state == domain.AttentionApplied {
				if len(reads.ownershipIDs) != 0 {
					t.Fatalf("deciding receipt still read ownership: %v", reads.ownershipIDs)
				}
			}
			if tc.state == domain.AttentionDelivered {
				want := domain.AttentionObserved
				if tc.block {
					want = domain.AttentionDelivered
				}
				if got := latestAttentionReceipt(t, store, "wait-terminal"); got.State != want {
					t.Fatalf("receipt state = %s, want %s", got.State, want)
				}
				if tc.block {
					other.Control = domain.ControlStopped
					requireAttentionStopOK(t, store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{other}}))
					requireAttentionStopOK(t, store.SaveWorkerSnapshot(ctx, attentionObservationSnapshot(epoch, 2, at.Add(2*time.Minute))))
					if got := latestAttentionReceipt(t, store, "wait-terminal"); got.State != domain.AttentionObserved {
						t.Fatalf("stopped attempt did not release receipt: %+v", got)
					}
				}
			}
		})
	}
}

func requireAttentionStopOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
