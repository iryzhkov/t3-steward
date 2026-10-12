package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

func unavailableResult(id string, accept bool, health domain.WorkerHealth) coordinatorWorkerTickResult {
	return coordinatorWorkerTickResult{WorkerID: id, Err: fmt.Errorf("commands: %w", sqlite.ErrWorkerUnavailable),
		Report: backlog.WorkerExchangeReport{Snapshot: domain.WorkerSnapshot{WorkerID: id, Inventory: domain.WorkerInventory{AcceptBacklog: accept, Health: health}}}}
}

func TestCoordinatorNonAcceptingRetryDecision(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result coordinatorWorkerTickResult
		retry  bool
	}{
		{"disabled", unavailableResult("N", false, domain.WorkerHealthReady), false},
		{"degraded", unavailableResult("N", true, domain.WorkerHealth("degraded")), false},
		{"ready", unavailableResult("N", true, domain.WorkerHealthReady), true},
		{"joined transient", func() coordinatorWorkerTickResult {
			r := unavailableResult("N", false, domain.WorkerHealthReady)
			r.Err = errors.Join(r.Err, errors.New("artifact import failed"))
			return r
		}(), true},
		{"joined unavailable", func() coordinatorWorkerTickResult {
			r := unavailableResult("N", false, domain.WorkerHealthReady)
			r.Err = errors.Join(r.Err, fmt.Errorf("renew: %w", sqlite.ErrWorkerUnavailable))
			return r
		}(), false},
		{"missing observation", coordinatorWorkerTickResult{WorkerID: "N", Err: sqlite.ErrWorkerUnavailable}, true},
		{"wrong observation", func() coordinatorWorkerTickResult {
			r := unavailableResult("N", false, domain.WorkerHealthReady)
			r.Report.Snapshot.WorkerID = "other"
			return r
		}(), true},
		{"stale", coordinatorWorkerTickResult{WorkerID: "N", Err: sqlite.ErrStaleWorkerSnapshot}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exchanges := 0
			sessions := &coordinatorWorkerSessions{workerIDs: []string{"N"}, exchange: func(context.Context, string, backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error) {
				exchanges++
				return tc.result.Report, tc.result.Err
			}}
			r := &coordinatorReconnect{workers: sessions}
			for i := 0; i < 5; i++ {
				r.Tick(context.Background(), backlog.QuotaBridgeReport{})
				if r.pending != tc.retry {
					t.Fatalf("pending=%v want %v", r.pending, tc.retry)
				}
				if r.pending {
					r.Retry(context.Background(), backlog.QuotaBridgeReport{})
				}
			}
			want := 5
			if tc.retry {
				want = 10
			}
			if exchanges != want {
				t.Fatalf("exchanges=%d want %d", exchanges, want)
			}
		})
	}
}

func TestCoordinatorRetrySubsetAndHandOn(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprint(mixed), func(t *testing.T) {
			ids := []string{"A", "H"}
			if mixed {
				ids = append(ids, "N")
			}
			counts := map[string]int{}
			var phases []string
			sessions := &coordinatorWorkerSessions{workerIDs: ids,
				handOn: func(_ context.Context, id string) (backlog.WorkerExchangeReport, error) {
					phases = append(phases, "hand:"+id)
					return backlog.WorkerExchangeReport{}, nil
				},
				exchange: func(_ context.Context, id string, _ backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error) {
					phases = append(phases, "exchange:"+id)
					counts[id]++
					if id == "A" && counts[id] == 1 {
						return backlog.WorkerExchangeReport{}, errors.New("transport")
					}
					if id == "N" {
						r := unavailableResult(id, false, domain.WorkerHealthReady)
						return r.Report, r.Err
					}
					return backlog.WorkerExchangeReport{}, nil
				}}
			r := &coordinatorReconnect{workers: sessions, pending: true}
			r.Retry(context.Background(), backlog.QuotaBridgeReport{})
			if !reflect.DeepEqual(r.workerIDs, []string{"A"}) {
				t.Fatalf("retry IDs=%v", r.workerIDs)
			}
			r.nextDelay()
			phases = nil
			r.Retry(context.Background(), backlog.QuotaBridgeReport{})
			if r.pending || r.delay != 0 || counts["H"] != 1 || mixed && counts["N"] != 1 {
				t.Fatalf("retry=%+v counts=%v", r, counts)
			}
			if !reflect.DeepEqual(phases, []string{"hand:A", "exchange:A"}) {
				t.Fatalf("phases=%v", phases)
			}
			phases = nil
			sessions.TickWorkers(context.Background(), backlog.QuotaBridgeReport{}, []string{"H", "unknown", "A", "A"})
			if !reflect.DeepEqual(phases, []string{"hand:A", "hand:H", "exchange:A", "exchange:H"}) {
				t.Fatalf("configured order=%v", phases)
			}
		})
	}
}

type scopeWorkerTicker struct {
	full     int
	subsets  [][]string
	disabled bool
}

func (w *scopeWorkerTicker) Tick(context.Context, backlog.QuotaBridgeReport) coordinatorWorkerTickReport {
	w.full++
	if w.disabled {
		return coordinatorWorkerTickReport{Results: []coordinatorWorkerTickResult{unavailableResult("A", false, domain.WorkerHealthReady)}}
	}
	return coordinatorWorkerTickReport{Results: []coordinatorWorkerTickResult{{WorkerID: "A", Err: errors.New("transport")}, {WorkerID: "B"}}}
}
func (w *scopeWorkerTicker) TickWorkers(_ context.Context, _ backlog.QuotaBridgeReport, ids []string) coordinatorWorkerTickReport {
	w.subsets = append(w.subsets, append([]string(nil), ids...))
	return coordinatorWorkerTickReport{Results: []coordinatorWorkerTickResult{{WorkerID: "A", Err: errors.New("transport")}}}
}

func TestCoordinatorServeRetryScope(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprint(disabled), func(t *testing.T) {
			t.Parallel()
			dir := shortTempDir(t)
			store, err := sqlitetest.OpenMigrated(filepath.Join(dir, "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			service, err := backlogadmin.New(store, localAdminAuthorizer{})
			if err != nil {
				t.Fatal(err)
			}
			listener, err := backlogadmin.ListenLocal(filepath.Join(dir, "admin.sock"))
			if err != nil {
				t.Fatal(err)
			}
			server := &backlogadmin.LocalServer{Listener: listener, Service: coordinatorLocalService{admin: service}, AllowedUID: uint32(os.Getuid()), MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20, RequestTimeout: time.Second, MaxConcurrent: 4}
			w := &scopeWorkerTicker{disabled: disabled}
			var quota, schedules, planning, admin int
			cycle := coordinatorBoundaryCycle{quota: reconnectQuotaTicker{calls: &quota}, schedules: recordingCoordinatorScheduleTicker{calls: &schedules}, planning: recordingCoordinatorPlanningTicker{calls: &planning}, admin: recordingCoordinatorAdminExecutor{calls: &admin}, workers: w, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			duration, interval := 23*time.Second, time.Hour
			if disabled {
				duration = 2200 * time.Millisecond
				interval = 200 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()
			if err := serveCoordinatorBoundaries(ctx, server, cycle, interval); err != nil {
				t.Fatal(err)
			}
			if disabled {
				if len(w.subsets) != 0 || w.full > schedules {
					t.Fatalf("full=%d scheduled=%d subsets=%v", w.full, schedules, w.subsets)
				}
			} else {
				// The existing bounded timer makes four exchanges total: startup at 1s,
				// followed by subset retries at 3s, 7s and 12s.
				if w.full != 1 || len(w.subsets) != 3 {
					t.Fatalf("full=%d subsets=%v", w.full, w.subsets)
				}
				for _, ids := range w.subsets {
					if !reflect.DeepEqual(ids, []string{"A"}) {
						t.Fatalf("subset=%v", ids)
					}
				}
			}
		})
	}
}

func TestCoordinatorRetryCountsRealRecordLoads(t *testing.T) {
	store, loads, _ := openSnapshotCountingStore(t)
	ctx := context.Background()
	epoch, err := store.AcquireCoordinator(ctx, "retry-test")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	counts := map[string]int{}
	sessions := &coordinatorWorkerSessions{workerIDs: []string{"A", "B"}, exchange: func(ctx context.Context, id string, _ backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error) {
		counts[id]++
		now := now.Add(time.Duration(counts[id]) * time.Millisecond)
		transport := snapshotIdleTransport{snapshot: domain.WorkerSnapshot{WorkerID: id, WorkerEpoch: "epoch", CoordinatorEpoch: epoch, Sequence: int64(counts[id]), Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour), Inventory: domain.WorkerInventory{ID: id, AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now}}}
		report, err := (backlog.FleetCoordinator{Store: store, Now: func() time.Time { return now }}).ReconcileWorker(ctx, transport, snapshotIdleBuilder{}, backlog.WorkerAdmissionPolicy{}, nil, nil, time.Minute, time.Hour)
		if id == "A" && counts[id] == 1 {
			err = errors.Join(err, errors.New("transient import"))
		}
		return report, err
	}}
	r := &coordinatorReconnect{workers: sessions}
	r.Tick(ctx, backlog.QuotaBridgeReport{})
	if !reflect.DeepEqual(r.workerIDs, []string{"A"}) {
		t.Fatalf("pending=%v", r.workerIDs)
	}
	var logs bytes.Buffer
	cycle := coordinatorBoundaryCycle{workers: r, logger: slog.New(slog.NewTextHandler(&logs, nil))}
	before := loads.Load()
	cycle.retryWorkers(ctx, backlog.QuotaBridgeReport{})
	k := loads.Load() - before
	if k != 0 || !strings.Contains(logs.String(), "coordinator worker retry completed") || !strings.Contains(logs.String(), fmt.Sprintf("record_loads=%d", k)) || !strings.Contains(logs.String(), "workers=1") || !strings.Contains(logs.String(), "duration=") {
		t.Fatalf("loads=%d logs=%s", k, &logs)
	}
	if r.pending || counts["B"] != 1 {
		t.Fatalf("pending=%v counts=%v logs=%s", r.pending, counts, &logs)
	}
}
