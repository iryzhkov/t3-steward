package main

import (
	"bytes"
	"context"
	"database/sql/driver"
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

// Count a records query, rather than calls that may hit the snapshot.
type snapshotCountingConnector struct {
	driver.Connector
	loads *atomic.Int64
}

func (c snapshotCountingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &snapshotCountingConn{tracingConn: &tracingConn{Conn: conn, metrics: &sqliteTraceMetrics{}}, loads: c.loads}, nil
}

type snapshotCountingConn struct {
	*tracingConn
	loads *atomic.Int64
}

func (c *snapshotCountingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query == "SELECT record FROM coordinator_workflows ORDER BY id" {
		c.loads.Add(1)
	}
	return c.tracingConn.QueryContext(ctx, query, args)
}
func openSnapshotCountingStore(t *testing.T) (*sqlite.Store, *atomic.Int64, string) {
	t.Helper()
	path := t.TempDir() + "/state.db"
	if err := sqlitetest.Seed(path); err != nil {
		t.Fatal(err)
	}
	loads := &atomic.Int64{}
	store, err := sqlite.OpenMigratedInstrumented(path, func(c driver.Connector) driver.Connector {
		return snapshotCountingConnector{Connector: c, loads: loads}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, loads, path
}

type snapshotReadingPlanner struct{ store *sqlite.Store }

func (p snapshotReadingPlanner) Tick(ctx context.Context, _ backlog.QuotaBridgeReport) (backlog.AssignmentPlanningReport, error) {
	_, err := p.store.LoadCoordinatorRecords(ctx)
	return backlog.AssignmentPlanningReport{}, err
}

type snapshotIdleBuilder struct{}

func (snapshotIdleBuilder) BuildAssignmentOffer(context.Context, domain.Assignment, time.Time) (workerproto.AssignmentOffer, error) {
	return workerproto.AssignmentOffer{}, fmt.Errorf("unexpected offer")
}

// No assignments exist, so only observation is expected from the real exchange.
type snapshotIdleTransport struct {
	backlog.WorkerControlTransport
	snapshot domain.WorkerSnapshot
}

func (t snapshotIdleTransport) WorkerID() string    { return t.snapshot.WorkerID }
func (t snapshotIdleTransport) WorkerEpoch() string { return t.snapshot.WorkerEpoch }
func (t snapshotIdleTransport) Snapshot(context.Context, workerproto.SnapshotRequest) (domain.WorkerSnapshot, error) {
	return t.snapshot, nil
}

func TestCoordinatorTickSharesRecordsAcrossWorkers(t *testing.T) {
	for _, workers := range []int{3, 7} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			store, loads, _ := openSnapshotCountingStore(t)
			ctx := context.Background()
			epoch, err := store.AcquireCoordinator(ctx, "tick-test")
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			calls := 0
			sessions := &coordinatorWorkerSessions{
				exchange: func(ctx context.Context, id string, _ backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error) {
					calls++
					transport := snapshotIdleTransport{snapshot: domain.WorkerSnapshot{
						WorkerID: id, WorkerEpoch: "epoch", CoordinatorEpoch: epoch,
						Sequence: 1, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour),
						Inventory: domain.WorkerInventory{ID: id, AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now},
					}}
					return (backlog.FleetCoordinator{Store: store, Now: func() time.Time { return now }}).ReconcileWorker(
						ctx, transport, snapshotIdleBuilder{}, backlog.WorkerAdmissionPolicy{}, nil, nil, time.Minute, time.Hour)
				},
			}
			for i := 0; i < workers; i++ {
				sessions.workerIDs = append(sessions.workerIDs, fmt.Sprintf("worker-%d", i))
			}
			cycle := coordinatorBoundaryCycle{
				projection: store, supervision: &coordinatorSupervision{store: backlog.CoordinatorSupervisionStore{Store: store}, logger: logger},
				quota: fairnessQuota{}, schedules: backlog.ScheduleTimer{Store: store, CatchUpMax: 1},
				planning: snapshotReadingPlanner{store: store}, admin: fairnessAdmin{},
				workers: sessions, logger: logger,
			}
			loads.Store(0)
			cycle.TickWithWorkers(ctx)
			if calls != workers || strings.Contains(logs.String(), "level=ERROR") {
				t.Fatalf("calls=%d logs=%s", calls, &logs)
			}
			// Worker observations do not change any coordinator records.
			want := int64(1)
			if got := loads.Load(); got != want {
				t.Fatalf("record loads=%d, want %d (one shared tick snapshot)", got, want)
			}
			if !strings.Contains(logs.String(), fmt.Sprintf("record_loads=%d", want)) || !strings.Contains(logs.String(), "duration=") {
				t.Fatalf("missing tick metrics: %s", &logs)
			}
			loads.Store(0)
			logs.Reset()
			cycle.Tick(ctx)
			if got := loads.Load(); got != 1 {
				t.Fatalf("read-only next tick loads=%d, want 1", got)
			}
		})
	}
}

func TestCoordinatorTickArtifactsBoundedByLiveRuns(t *testing.T) {
	for _, history := range []int{0, 200} {
		t.Run(fmt.Sprint(history), func(t *testing.T) {
			store, _, _ := openSnapshotCountingStore(t)
			records := sqlite.CoordinatorRecords{
				WorkflowRuns: []domain.WorkflowRun{{ID: "live", Progress: domain.ProgressActive}},
				Artifacts:    []domain.Artifact{{ID: "live-artifact", WorkflowRunID: "live"}},
			}
			for i := 0; i < history; i++ {
				id := fmt.Sprintf("terminal-%03d", i)
				records.WorkflowRuns = append(records.WorkflowRuns, domain.WorkflowRun{
					ID: id, Progress: domain.ProgressSucceeded,
					Sink: &domain.SinkTask{Progress: domain.ProgressSucceeded},
				})
				records.Artifacts = append(records.Artifacts, domain.Artifact{ID: id, WorkflowRunID: id})
			}
			if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
				t.Fatal(err)
			}
			ctx, scope := sqlite.WithCoordinatorRecordsSnapshot(context.Background())
			defer scope.Finish()
			tick, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(tick.Artifacts) != 1 || tick.Artifacts[0].ID != "live-artifact" {
				t.Fatalf("tick decoded %d artifacts with history=%d: %+v", len(tick.Artifacts), history, tick.Artifacts)
			}
			if history == 0 {
				return
			}
			loaded, err := tick.WithRunArtifacts(ctx, "terminal-000", "terminal-000")
			if err != nil {
				t.Fatal(err)
			}
			if len(loaded.Artifacts) != 2 {
				t.Fatalf("lazy run artifacts=%+v", loaded.Artifacts)
			}
			byID, err := tick.WithArtifactIDs(ctx, "terminal-001", "terminal-001")
			if err != nil {
				t.Fatal(err)
			}
			if len(byID.Artifacts) != 2 {
				t.Fatalf("lazy input artifacts=%+v", byID.Artifacts)
			}
			cached, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(cached.Artifacts) != 1 {
				t.Fatal("lazy load modified shared snapshot")
			}
			command, err := store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(command.Artifacts) != history+1 {
				t.Fatalf("command artifacts=%d", len(command.Artifacts))
			}
		})
	}
}

func TestCoordinatorRecordsAssignmentIndex(t *testing.T) {
	store, _, _ := openSnapshotCountingStore(t)
	records := sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{{ID: "attempt-a", WorkflowRunID: "run-a", TaskID: "task-a", Number: 1},
			{ID: "attempt-b", WorkflowRunID: "run-b", TaskID: "task-b", Number: 1}},
		Assignments: []domain.Assignment{
			{ID: "assignment-b", AttemptID: "attempt-b", DispatchToken: "token-b"},
			{ID: "assignment-a", AttemptID: "attempt-a", DispatchToken: "token-a"},
		},
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	ctx, scope := sqlite.WithCoordinatorRecordsSnapshot(context.Background())
	defer scope.Finish()
	loaded, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.AssignmentsByRun["run-a"]) != 1 || loaded.AssignmentsByRun["run-a"][0].ID != "assignment-a" ||
		len(loaded.AssignmentsByRun["run-b"]) != 1 || loaded.AssignmentsByRun["run-b"][0].ID != "assignment-b" {
		t.Fatalf("assignment index=%+v", loaded.AssignmentsByRun)
	}
}

func TestCoordinatorRecordsSnapshotRefreshAndIsolation(t *testing.T) {
	store, loads, path := openSnapshotCountingStore(t)
	ctx, snapshot := sqlite.WithCoordinatorRecordsSnapshot(context.Background())
	read := func(ctx context.Context, want int) {
		t.Helper()
		records, err := store.LoadCoordinatorRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(records.QuotaPools) != want {
			t.Fatalf("pools=%d, want %d", len(records.QuotaPools), want)
		}
	}
	read(ctx, 0)
	read(ctx, 0)
	if got := loads.Load(); got != 1 {
		t.Fatalf("read-only loads=%d", got)
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{QuotaPools: []domain.QuotaPool{{ID: "local"}}}); err != nil {
		t.Fatal(err)
	}
	read(ctx, 1)
	read(ctx, 1)
	other, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{QuotaPools: []domain.QuotaPool{{ID: "external"}}}); err != nil {
		t.Fatal(err)
	}
	read(ctx, 2)
	if got := snapshot.Finish(); got != 3 {
		t.Fatalf("tick loads=%d, want 3", got)
	}
	// A retained context after Finish and independent command contexts must load.
	read(ctx, 2)
	read(context.Background(), 2)
	read(sqlite.WithoutCoordinatorRecordsSnapshot(ctx), 2)
	if got := loads.Load(); got != 6 {
		t.Fatalf("actual loads=%d, want 6", got)
	}
}
