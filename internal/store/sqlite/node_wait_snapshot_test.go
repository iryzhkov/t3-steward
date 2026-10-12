package sqlite

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type nodeWaitQueryCounts struct {
	mu      sync.Mutex
	queries map[string]int
}

func (c *nodeWaitQueryCounts) reset() { c.mu.Lock(); defer c.mu.Unlock(); c.queries = map[string]int{} }
func (c *nodeWaitQueryCounts) count(query string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queries[query]
}

type nodeWaitCountingConnector struct {
	driver.Connector
	counts *nodeWaitQueryCounts
}

func (c nodeWaitCountingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &nodeWaitCountingConn{Conn: conn, counts: c.counts}, nil
}

type nodeWaitCountingConn struct {
	driver.Conn
	counts *nodeWaitQueryCounts
}

func (c *nodeWaitCountingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.counts.mu.Lock()
	c.counts.queries[query]++
	c.counts.mu.Unlock()
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
func (c *nodeWaitCountingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *nodeWaitCountingConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}
func reopenNodeWaitCountingStore(t *testing.T, store *Store) (*Store, *nodeWaitQueryCounts) {
	t.Helper()
	path := store.path
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	counts := &nodeWaitQueryCounts{queries: map[string]int{}}
	store, err := OpenMigratedInstrumented(path, func(conn driver.Connector) driver.Connector {
		return nodeWaitCountingConnector{Connector: conn, counts: counts}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, counts
}
func saveNodeWaitTestRows(t *testing.T, store *Store, nodes []domain.NodeWait, tasks []domain.TaskWait) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, wait := range nodes {
		if err := saveNodeWaitTx(ctx, tx, wait); err != nil {
			t.Fatal(err)
		}
	}
	for _, wait := range tasks {
		if err := saveTaskWaitTx(ctx, tx, wait); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
func nodeWaitTotalChanges(t *testing.T, store *Store) int64 {
	t.Helper()
	var changes int64
	if err := store.db.QueryRow("SELECT total_changes()").Scan(&changes); err != nil {
		t.Fatal(err)
	}
	return changes
}

func TestIdleNodeWaitSettlementSkipsHistoryDecodesAndWrites(t *testing.T) {
	store, counts := reopenNodeWaitCountingStore(t, openSchemaFixture(t))
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	records := CoordinatorRecords{}
	for i := 0; i < 4; i++ {
		id := fmt.Sprint(i)
		records.WorkflowRuns = append(records.WorkflowRuns, domain.WorkflowRun{ID: "r" + id, WorkflowID: "w" + id, Progress: domain.ProgressSucceeded})
		records.Tasks = append(records.Tasks, domain.Task{ID: "t" + id, WorkflowID: "w" + id})
		records.Attempts = append(records.Attempts, domain.Attempt{ID: "a" + id, TaskID: "t" + id, WorkflowRunID: "r" + id, Progress: domain.ProgressSucceeded})
		records.Assignments = append(records.Assignments, domain.Assignment{ID: "s" + id, AttemptID: "a" + id, DispatchToken: "dispatch" + id})
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	saveNodeWaitTestRows(t, store, []domain.NodeWait{
		{Request: domain.NodeWaitRequest{ID: "settled", ThreadID: "thread"}, SettledAt: &now, Delivery: "pending"},
		{Request: domain.NodeWaitRequest{ID: "cancelled", ThreadID: "thread"}, Delivery: "cancelled"},
		{Request: domain.NodeWaitRequest{ID: "rejected", ThreadID: "thread"}, Delivery: "rejected"},
	}, []domain.TaskWait{
		{ID: "task-settled", RequestID: "request", AttemptID: "a0", ThreadID: "thread", SettledAt: &now},
	})
	before := nodeWaitTotalChanges(t, store)
	counts.reset()
	if err := store.SettleNodeWaits(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"coordinator_attempts", "coordinator_node_waits", "coordinator_task_waits", "coordinator_workflow_runs", "coordinator_tasks", "coordinator_assignments"} {
		query := "SELECT record FROM " + table + " ORDER BY id"
		if got := counts.count(query); got != 0 {
			t.Fatalf("%s decoded %d times", table, got)
		}
	}
	if after := nodeWaitTotalChanges(t, store); after != before {
		t.Fatalf("idle settlement wrote rows: total_changes %d -> %d", before, after)
	}
}

func TestNodeWaitSettlementUsesValidSnapshotAndMatchesColdDeclaredTask(t *testing.T) {
	var cold domain.NodeObservation
	for _, mode := range []string{"cold", "valid", "records-write", "node-write", "task-write", "inactive", "unloaded", "new-connection"} {
		t.Run(mode, func(t *testing.T) {
			store, before, now := sinkStoreFixture(t)
			attempt := before.Attempts[0]
			attempt.Progress, attempt.Control = domain.ProgressActive, domain.ControlRunning
			if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Attempts: []domain.Attempt{attempt}}); err != nil {
				t.Fatal(err)
			}
			request := domain.NodeWaitRequest{ID: "pending", ThreadID: "thread", Target: domain.NodeRef{RunID: "r", TaskID: "t"}, Timeout: time.Hour}
			if _, err := store.RegisterNodeWait(context.Background(), request, "operator", "host", now); err != nil {
				t.Fatal(err)
			}
			attempt.Progress, attempt.Control = domain.ProgressSucceeded, domain.ControlStopped
			attempt.Revision++
			run := before.Run
			run.Graph = &domain.GraphDefinition{Tasks: append(append([]domain.Task(nil), before.Tasks...), domain.Task{ID: "graph-added", Name: "graph-added", WorkflowID: "w"})}
			run.Progress = domain.ProgressSucceeded
			run.Sink.Progress = domain.ProgressSucceeded
			if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}, Attempts: []domain.Attempt{attempt}}); err != nil {
				t.Fatal(err)
			}
			store, counts := reopenNodeWaitCountingStore(t, store)
			ctx := context.Background()
			if mode != "cold" {
				var snapshot *CoordinatorRecordsSnapshot
				ctx, snapshot = WithCoordinatorRecordsSnapshot(ctx)
				defer snapshot.Finish()
				if mode != "unloaded" {
					loaded, err := store.LoadCoordinatorRecords(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if len(loaded.Tasks) != 2 {
						t.Fatalf("snapshot tasks=%+v; graph addition missing", loaded.Tasks)
					}
					beforeJSON, err := json.Marshal(struct {
						Runs        []domain.WorkflowRun
						Tasks       []domain.Task
						Attempts    []domain.Attempt
						Assignments []domain.Assignment
					}{loaded.WorkflowRuns, loaded.Tasks, loaded.Attempts, loaded.Assignments})
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						afterJSON, err := json.Marshal(struct {
							Runs        []domain.WorkflowRun
							Tasks       []domain.Task
							Attempts    []domain.Attempt
							Assignments []domain.Assignment
						}{loaded.WorkflowRuns, loaded.Tasks, loaded.Attempts, loaded.Assignments})
						if err != nil {
							t.Fatal(err)
						}
						if string(afterJSON) != string(beforeJSON) {
							t.Fatal("settlement mutated shared snapshot slices")
						}
					}()
					if len(loaded.WorkflowRuns) != 1 || !loaded.WorkflowRuns[0].Progress.Terminal() {
						t.Fatal("snapshot omitted terminal target run")
					}
				}
				switch mode {
				case "inactive":
					snapshot.Finish()
				case "new-connection":
					store.db.SetMaxIdleConns(0)
				case "records-write":
					if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: "unrelated", WorkflowID: "unrelated", Progress: domain.ProgressActive}}}); err != nil {
						t.Fatal(err)
					}
				case "node-write":
					saveNodeWaitTestRows(t, store, []domain.NodeWait{{Request: domain.NodeWaitRequest{ID: "other", ThreadID: "thread"}, Delivery: "cancelled"}}, nil)
				case "task-write":
					saveNodeWaitTestRows(t, store, nil, []domain.TaskWait{{ID: "other", RequestID: "other", AttemptID: "a", ThreadID: "thread", SettledAt: &now}})
				}
			}
			counts.reset()
			if err := store.SettleNodeWaits(ctx, now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			wantDecodes := 0
			if mode == "cold" || mode == "records-write" || mode == "inactive" || mode == "unloaded" || mode == "new-connection" {
				wantDecodes = 1
			}
			if got := counts.count("SELECT record FROM coordinator_attempts ORDER BY id"); got != wantDecodes {
				t.Fatalf("attempt decodes=%d want=%d", got, wantDecodes)
			}
			if mode == "new-connection" && counts.count("SELECT revision FROM temp.coordinator_records_stamp") != 0 {
				t.Fatal("connection mismatch queried connection-local stamp")
			}
			waits, err := store.ListNodeWaits(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			var observation *domain.NodeObservation
			for _, wait := range waits {
				if wait.Request.ID == request.ID {
					observation = wait.Observation
					if wait.SettledAt == nil {
						t.Fatal("terminal run did not settle")
					}
				}
			}
			if observation == nil || observation.ExitCode != 0 || observation.AttemptID != "a" {
				t.Fatalf("observation=%+v", observation)
			}
			if mode == "cold" {
				cold = *observation
			} else if !reflect.DeepEqual(cold, *observation) {
				t.Fatalf("snapshot observation differs: cold=%+v got=%+v", cold, *observation)
			}
		})
	}
}

func TestNodeWaitSettlementSweepsTerminalTaskWaitWithoutNodeWaits(t *testing.T) {
	for _, useSnapshot := range []bool{false, true} {
		t.Run(fmt.Sprint(useSnapshot), func(t *testing.T) {
			store, attempt, now := taskWaitFixture(t)
			wait, err := store.RegisterTaskWait(context.Background(), taskWaitRegistration(attempt, "request", domain.WakeEach), now)
			if err != nil {
				t.Fatal(err)
			}
			attempt = loadAttempt(t, store, attempt.ID)
			attempt.Progress, attempt.Control = domain.ProgressCancelled, domain.ControlStopped
			attempt.Revision++
			if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Attempts: []domain.Attempt{attempt}}); err != nil {
				t.Fatal(err)
			}
			store, counts := reopenNodeWaitCountingStore(t, store)
			ctx := context.Background()
			if useSnapshot {
				var snapshot *CoordinatorRecordsSnapshot
				ctx, snapshot = WithCoordinatorRecordsSnapshot(ctx)
				defer snapshot.Finish()
				if _, err := store.LoadCoordinatorRecords(ctx); err != nil {
					t.Fatal(err)
				}
			}
			counts.reset()
			if err := store.SettleNodeWaits(ctx, now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			decodes := 1
			if useSnapshot {
				decodes = 0
			}
			if got := counts.count("SELECT record FROM coordinator_attempts ORDER BY id"); got != decodes {
				t.Fatalf("attempt decodes=%d want=%d", got, decodes)
			}
			waits, err := store.ListTaskWaits(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(waits) != 1 || waits[0].ID != wait.ID || waits[0].SettledAt == nil || waits[0].Result == nil || waits[0].Result.Outcome != domain.TaskWaitCancelled || waits[0].Result.ExitCode != 2 {
				t.Fatalf("terminal task-only wait not cancelled: %+v", waits)
			}
		})
	}
}
