package backlog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func snapshotChangeCheck(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// Triggers count durable writes rather than calls to the persistence API.
func snapshotChangeStore(t *testing.T) (*sqlite.Store, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlitetest.OpenMigrated(path)
	snapshotChangeCheck(t, err)
	t.Cleanup(func() { store.Close() })
	db, err := sql.Open("sqlite", path)
	snapshotChangeCheck(t, err)
	t.Cleanup(func() { db.Close() })
	for _, statement := range []string{
		"CREATE TABLE snapshot_test_writes(worker_id TEXT PRIMARY KEY, writes INTEGER NOT NULL)",
		`CREATE TRIGGER snapshot_test_insert AFTER INSERT ON coordinator_worker_snapshots
		 BEGIN INSERT INTO snapshot_test_writes VALUES (NEW.worker_id, 1)
		 ON CONFLICT(worker_id) DO UPDATE SET writes = writes + 1; END`,
		`CREATE TRIGGER snapshot_test_update AFTER UPDATE ON coordinator_worker_snapshots
		 BEGIN INSERT INTO snapshot_test_writes VALUES (NEW.worker_id, 1)
		 ON CONFLICT(worker_id) DO UPDATE SET writes = writes + 1; END`,
	} {
		_, err := db.Exec(statement)
		snapshotChangeCheck(t, err)
	}
	return store, db
}

type snapshotChangeRow struct {
	record                   string
	writes, audits, renewals int
}

func snapshotChangeRead(t *testing.T, db *sql.DB, workerID string) snapshotChangeRow {
	t.Helper()
	var row snapshotChangeRow
	snapshotChangeCheck(t, db.QueryRow(`SELECT s.record, w.writes,
		(SELECT count(*) FROM coordinator_audit_events WHERE target_type = 'worker' AND target_id = s.worker_id),
		(SELECT count(*) FROM coordinator_audit_events WHERE kind = 'assignment-lease-renewed')
		FROM coordinator_worker_snapshots s JOIN snapshot_test_writes w USING(worker_id) WHERE worker_id = ?`,
		workerID).Scan(&row.record, &row.writes, &row.audits, &row.renewals))
	return row
}

func snapshotChangeAt(snapshot domain.WorkerSnapshot, sequence int64, offset time.Duration) domain.WorkerSnapshot {
	snapshot.Sequence = sequence
	snapshot.ObservedAt = coordinatorTestTime.Add(offset)
	snapshot.ValidUntil = snapshot.ObservedAt.Add(time.Minute)
	snapshot.Inventory.Sequence = sequence
	snapshot.Inventory.ObservedAt = snapshot.ObservedAt
	snapshot.Assignments = append([]domain.WorkerAssignmentObservation(nil), snapshot.Assignments...)
	for i := range snapshot.Assignments {
		snapshot.Assignments[i].ObservedAt = snapshot.ObservedAt
	}
	snapshot.Inventory.Telemetry = &domain.WorkerTelemetry{ObservedAt: snapshot.ObservedAt}
	return snapshot
}

func snapshotChangeExchange(t *testing.T, coordinator FleetCoordinator, snapshot domain.WorkerSnapshot, renewal *domain.WorkerSnapshot) (WorkerExchangeReport, *exchangeTransport) {
	t.Helper()
	transport := &exchangeTransport{snapshots: []domain.WorkerSnapshot{snapshot}, renewalSnapshot: renewal}
	report, err := coordinator.ReconcileWorker(context.Background(),
		&telemetryExchangeTransport{exchangeTransport: transport, telemetry: snapshot.Inventory.Telemetry},
		testOfferBuilder{}, WorkerAdmissionPolicy{QuotaChecksDisabled: true}, nil, nil, time.Minute, 2*time.Minute)
	if snapshot.Inventory.Health == domain.WorkerHealthDegraded {
		if !errors.Is(err, sqlite.ErrWorkerUnavailable) {
			t.Fatalf("degraded worker error = %v", err)
		}
	} else {
		snapshotChangeCheck(t, err)
	}
	return report, transport
}

func snapshotChangeAssertSnapshot(t *testing.T, row snapshotChangeRow, want domain.WorkerSnapshot) {
	t.Helper()
	var got domain.WorkerSnapshot
	snapshotChangeCheck(t, json.Unmarshal([]byte(row.record), &got))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stored snapshot = %#v, want %#v", got, want)
	}
}

func TestFleetCoordinatorSnapshotIdleRefreshAndSubstanceChanges(t *testing.T) {
	store, db := snapshotChangeStore(t)
	now := coordinatorTestTime
	coordinator := FleetCoordinator{Store: store, Now: func() time.Time { return now }}
	snapshots := make([]domain.WorkerSnapshot, 3)
	rows := make([]snapshotChangeRow, 3)
	for i := range snapshots {
		snapshots[i] = coordinatorSnapshot(1)
		snapshots[i].WorkerID = fmt.Sprintf("worker-%d", i)
		snapshots[i].Inventory.ID = snapshots[i].WorkerID
		snapshots[i].Assignments = []domain.WorkerAssignmentObservation{{
			AssignmentID: fmt.Sprintf("assignment-%d", i), AssignmentEpoch: 1,
			State: domain.AssignmentClaimed, Control: domain.ControlRunning,
		}}
	}
	for tick, seconds := range []int{0, 10, 20, 30, 40, 41} {
		now = coordinatorTestTime.Add(time.Duration(seconds) * time.Second)
		for i := range snapshots {
			snapshots[i] = snapshotChangeAt(snapshots[i], int64(tick+1), now.Sub(coordinatorTestTime))
			if seconds == 40 && i == 0 {
				snapshots[i].Inventory.Health = domain.WorkerHealthDegraded
			}
			if seconds == 41 && i == 1 {
				snapshots[i].Assignments[0].State = domain.AssignmentReleased
			}
			report, _ := snapshotChangeExchange(t, coordinator, snapshots[i], nil)
			got := snapshotChangeRead(t, db, snapshots[i].WorkerID)
			write := seconds == 0 || seconds == 30 || (seconds == 40 && i == 0) || (seconds == 41 && i == 1)
			if write {
				wantAudits := rows[i].audits + 1
				if seconds == 30 {
					wantAudits = rows[i].audits
				}
				if got.writes != rows[i].writes+1 || got.audits != wantAudits || got.record == rows[i].record {
					t.Fatalf("%ds worker %d: got %+v, previous %+v", seconds, i, got, rows[i])
				}
				snapshotChangeAssertSnapshot(t, got, snapshots[i])
			} else if got != rows[i] {
				t.Fatalf("%ds worker %d idle row changed: got %+v, want %+v", seconds, i, got, rows[i])
			}
			snapshotChangeAssertSnapshot(t, got, report.Snapshot)
			rows[i] = got
		}
	}
}

func snapshotChangeClaimed(t *testing.T, store *sqlite.Store) (domain.WorkerSnapshot, domain.Attempt) {
	t.Helper()
	snapshot := coordinatorSnapshot(4)
	snapshot.Inventory.Capabilities = []string{workerproto.CapabilityTaskWaitCollectionFence}
	snapshot.Assignments = []domain.WorkerAssignmentObservation{{
		AssignmentID: "assignment-1", AssignmentEpoch: 1, State: domain.AssignmentClaimed,
		Control: domain.ControlRunning, ThreadID: "thread-1", WorkspacePath: "/tmp/work",
	}}
	snapshot = snapshotChangeAt(snapshot, 4, 0)
	attempt := domain.Attempt{
		ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", AssignmentID: "assignment-1",
		Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 1, UpdatedAt: coordinatorTestTime,
	}
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: attempt.ID, WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch,
		Route: domain.ProviderRoute{WorkerID: snapshot.WorkerID, ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "pool"},
		State: domain.AssignmentClaimed, Epoch: 1, LeaseToken: "lease-1", DispatchToken: "dispatch-1", ThreadID: "thread-1",
		LeaseExpiresAt: coordinatorTestTime.Add(2 * time.Minute), CreatedAt: coordinatorTestTime, UpdatedAt: coordinatorTestTime,
	}
	snapshotChangeCheck(t, store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}))
	snapshotChangeCheck(t, store.SaveWorkerSnapshot(context.Background(), snapshot))
	return snapshot, attempt
}

func TestFleetCoordinatorSnapshotSkippedStopAndCollectionFences(t *testing.T) {
	store, db := snapshotChangeStore(t)
	snapshot, attempt := snapshotChangeClaimed(t, store)
	before := snapshotChangeRead(t, db, snapshot.WorkerID)
	// Cancel between exchanges without changing the worker's observation.
	attempt.Progress, attempt.Control = domain.ProgressCancelled, domain.ControlStopped
	attempt.Revision++
	attempt.UpdatedAt = coordinatorTestTime.Add(5 * time.Second)
	snapshotChangeCheck(t, store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}}))
	now := coordinatorTestTime.Add(10 * time.Second)
	coordinator := FleetCoordinator{Store: store, Now: func() time.Time { return now }, LeaseRenewInterval: time.Minute}
	report, transport := snapshotChangeExchange(t, coordinator, snapshotChangeAt(snapshot, 5, 10*time.Second), nil)
	if len(report.Renewed) != 0 || len(transport.commands) != 1 || transport.commands[0].Kind != domain.WorkerCommandStop ||
		transport.commands[0].ExpectedWorkerSequence != snapshot.Sequence {
		t.Fatalf("skipped-write stop: report=%+v commands=%+v", report, transport.commands)
	}
	if got := snapshotChangeRead(t, db, snapshot.WorkerID); got != before {
		t.Fatalf("stop rewrote snapshot: %+v", got)
	}
	now = coordinatorTestTime.Add(20 * time.Second)
	stopped := snapshotChangeAt(snapshot, 6, 20*time.Second)
	stopped.Assignments[0].State, stopped.Assignments[0].Control = domain.AssignmentReleased, domain.ControlStopped
	report, _ = snapshotChangeExchange(t, coordinator, stopped, nil)
	after := snapshotChangeRead(t, db, snapshot.WorkerID)
	if after.writes != before.writes+1 || after.audits != before.audits+1 || report.Snapshot.Sequence != stopped.Sequence {
		t.Fatalf("stopped observation: row=%+v report=%+v", after, report)
	}
	snapshotChangeAssertSnapshot(t, after, stopped)
	now = coordinatorTestTime.Add(21 * time.Second)
	_, transport = snapshotChangeExchange(t, coordinator, snapshotChangeAt(stopped, 7, 21*time.Second), nil)
	if len(transport.parked) != 1 || transport.parked[0].ObservedSequence != stopped.Sequence ||
		transport.parked[0].ObservedWorkerEpoch != stopped.WorkerEpoch {
		t.Fatalf("collection fence request = %+v, want persisted sequence %d", transport.parked, stopped.Sequence)
	}
}

func TestFleetCoordinatorSnapshotRenewalPersistsReply(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("substance-changed=%t", changed), func(t *testing.T) {
			store, db := snapshotChangeStore(t)
			snapshot, _ := snapshotChangeClaimed(t, store)
			before := snapshotChangeRead(t, db, snapshot.WorkerID)
			now := coordinatorTestTime.Add(10 * time.Second)
			coordinator := FleetCoordinator{Store: store, Now: func() time.Time { return now }, LeaseRenewInterval: 10 * time.Second}
			incoming := snapshotChangeAt(snapshot, 5, 10*time.Second)
			renewed := snapshotChangeAt(incoming, 6, 11*time.Second)
			renewed.Inventory.Telemetry = snapshot.Inventory.Telemetry.Clone()
			if changed {
				renewed.Assignments[0].Control = domain.ControlPaused
			}
			report, transport := snapshotChangeExchange(t, coordinator, incoming, &renewed)
			if len(report.Renewed) != 1 || len(transport.renewals) != 1 || transport.renewals[0].WorkerSequence != snapshot.Sequence {
				t.Fatalf("renewal must consume stored sequence %d: report=%+v renewals=%+v", snapshot.Sequence, report, transport.renewals)
			}
			after := snapshotChangeRead(t, db, snapshot.WorkerID)
			wantAudits := before.audits
			if changed {
				wantAudits++
			}
			if after.writes != before.writes+1 || after.audits != wantAudits || after.renewals != before.renewals+1 {
				t.Fatalf("renewal row = %+v, previous %+v", after, before)
			}
			snapshotChangeAssertSnapshot(t, after, renewed)
			if !reflect.DeepEqual(report.Snapshot, renewed) {
				t.Fatalf("renewal report snapshot = %#v, want %#v", report.Snapshot, renewed)
			}
			if !changed {
				now = coordinatorTestTime.Add(12 * time.Second)
				report, transport = snapshotChangeExchange(t, coordinator, snapshotChangeAt(renewed, 7, 12*time.Second), nil)
				if len(report.Renewed) != 0 || len(transport.renewals) != 0 {
					t.Fatalf("early renewal: %+v", report)
				}
				if got := snapshotChangeRead(t, db, snapshot.WorkerID); got != after {
					t.Fatalf("exchange after renewal changed row: got %+v want %+v", got, after)
				}
			}
		})
	}
}
