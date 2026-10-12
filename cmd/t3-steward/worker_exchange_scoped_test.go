package main

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type exchangeCountingConnector struct {
	snapshotCountingConnector
	decoded *atomic.Int64
}

func (c exchangeCountingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.snapshotCountingConnector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &exchangeCountingConn{snapshotCountingConn: conn.(*snapshotCountingConn), decoded: c.decoded}, nil
}

type exchangeCountingConn struct {
	*snapshotCountingConn
	decoded *atomic.Int64
}

func (c *exchangeCountingConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.snapshotCountingConn.QueryContext(ctx, q, args)
	if err == nil && len(args) > 0 && args[0].Value == "worker-0" &&
		(strings.HasPrefix(q, "SELECT record FROM coordinator_assignments") || strings.HasPrefix(q, "SELECT record FROM coordinator_attempts")) {
		rows = exchangeCountingRows{Rows: rows, decoded: c.decoded}
	}
	return rows, err
}

type exchangeCountingRows struct {
	driver.Rows
	decoded *atomic.Int64
}

func (r exchangeCountingRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	if err == nil {
		r.decoded.Add(1)
	}
	return err
}

type exchangePollingControl struct {
	queuedUploadControl
	polls []string
}

func (c *exchangePollingControl) RoundTripWithRetry(ctx context.Context, req workerproto.Envelope, retry workerproto.RetryPolicy) (workerproto.Envelope, error) {
	if req.Type == workerproto.MessageArtifactPoll {
		var p workerproto.ArtifactPollRequest
		if err := json.Unmarshal(req.Payload, &p); err != nil {
			return workerproto.Envelope{}, err
		}
		c.polls = append(c.polls, p.Purpose)
	}
	return c.queuedUploadControl.RoundTripWithRetry(ctx, req, retry)
}

func TestIdleExchangeAddsNoFullLoadsWithPollingAndHistory(t *testing.T) {
	var smallDecode int64
	for _, history := range []int{1, 60} {
		for _, workers := range []int{1, 6} {
			t.Run(fmt.Sprintf("%d-workers-%d-history", workers, history), func(t *testing.T) {
				ctx := context.Background()
				now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
				path := t.TempDir() + "/state.db"
				if err := sqlitetest.Seed(path); err != nil {
					t.Fatal(err)
				}
				loads, decoded := &atomic.Int64{}, &atomic.Int64{}
				store, err := sqlite.OpenMigratedInstrumented(path, func(c driver.Connector) driver.Connector {
					return exchangeCountingConnector{snapshotCountingConnector{Connector: c, loads: loads}, decoded}
				})
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				var records sqlite.CoordinatorRecords
				for i := 0; i <= history; i++ {
					id, worker := fmt.Sprint(i), "unrelated"
					if i == 0 {
						worker = "worker-0"
					}
					records.Assignments = append(records.Assignments, domain.Assignment{ID: id, AttemptID: id, DispatchToken: id, WorkerID: worker, WorkerEpoch: "epoch", Epoch: 1, State: domain.AssignmentReleased, UpdatedAt: now})
					records.Attempts = append(records.Attempts, domain.Attempt{ID: id, TaskID: id, WorkflowRunID: id, Revision: 1, Progress: domain.ProgressSucceeded, Control: domain.ControlStopped})
				}
				if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
					t.Fatal(err)
				}
				var logs bytes.Buffer
				logger := slog.New(slog.NewTextHandler(&logs, nil))
				polls, calls, enrollment := map[string]*exchangePollingControl{}, 0, 0
				sessions := &coordinatorWorkerSessions{enroll: func(context.Context) error { enrollment++; return nil }}
				for i := 0; i < workers; i++ {
					id := fmt.Sprintf("worker-%d", i)
					sessions.workerIDs = append(sessions.workerIDs, id)
					polls[id] = &exchangePollingControl{}
				}
				sessionFor := func(id string) coordinatorWorkerSession {
					session := (handOnWorker{store: store, now: now}).session(t, &polls[id].queuedUploadControl)
					session.Client = coordinatorResultClient(t, now, "exchange-control", polls[id])
					return session
				}
				sessions.handOn = func(ctx context.Context, id string) (backlog.WorkerExchangeReport, error) {
					return handOnCoordinatorWorkerContinuations(ctx, sessionFor(id), backlog.WorkerExchangeReport{}, 1024)
				}
				sessions.exchange = func(ctx context.Context, id string, q backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error) {
					calls++
					observed := now.Add(time.Duration(calls) * time.Second)
					snapshot := domain.WorkerSnapshot{WorkerID: id, WorkerEpoch: "epoch", CoordinatorEpoch: 1, Sequence: int64(calls), Connected: true, ObservedAt: observed, ValidUntil: observed.Add(time.Hour), Inventory: domain.WorkerInventory{ID: id, AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: observed, Capabilities: []string{workerproto.CapabilitySessionTitles, workerproto.CapabilityTaskWaitCollectionFence}}}
					return exchangeCoordinatorWorker(ctx, sessionFor(id), 1024, func(ctx context.Context) (backlog.WorkerExchangeReport, error) {
						return (backlog.FleetCoordinator{Store: store, Now: func() time.Time { return now }}).ReconcileWorker(ctx, snapshotIdleTransport{snapshot: snapshot}, snapshotIdleBuilder{}, backlog.WorkerAdmissionPolicyFromQuotaReport(q), nil, q.Pools, time.Minute, time.Hour)
					})
				}
				quota := backlog.QuotaBridgeReport{Pools: []domain.QuotaPool{{ID: "pool", Admission: domain.AdmissionOpen, MaxConcurrent: 1}}}
				schedules := 0
				cycle := coordinatorBoundaryCycle{quota: fixedCoordinatorQuotaTicker{quota}, schedules: recordingCoordinatorScheduleTicker{calls: &schedules}, planning: snapshotReadingPlanner{store}, admin: fairnessAdmin{}, workers: sessions, logger: logger}
				// Match state boundaries; only the worker exchange phase differs.
				before := loads.Load()
				cycle.Tick(ctx)
				baseline := loads.Load() - before
				before = loads.Load()
				cycle.TickWithWorkers(ctx)
				if added := loads.Load() - before - baseline; added != 0 {
					t.Fatalf("idle exchange added %d full loads", added)
				}
				// No tick snapshot can mask calls on this independent worker-only pass.
				decoded.Store(0)
				before = loads.Load()
				cycle.tickWorkersOnly(ctx, quota)
				if loads.Load() != before || calls != 2*workers || enrollment != 2 || strings.Contains(logs.String(), "level=ERROR") {
					t.Fatalf("loads=%d calls=%d enrollment=%d logs=%s", loads.Load()-before, calls, enrollment, &logs)
				}
				if history == 1 && workers == 1 {
					smallDecode = decoded.Load()
				}
				if decoded.Load() != smallDecode || smallDecode == 0 {
					t.Fatalf("target decode=%d small=%d", decoded.Load(), smallDecode)
				}
				for id, control := range polls {
					if got := strings.Join(control.polls, ","); got != "checkpoint,checkpoint,result,checkpoint,checkpoint,checkpoint,result,checkpoint" {
						t.Fatalf("%s polling order=%s", id, got)
					}
				}
			})
		}
	}
}
