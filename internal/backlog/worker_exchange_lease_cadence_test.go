package backlog

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

func TestLeaseRenewalsForWorkerCadence(t *testing.T) {
	t.Parallel()
	const duration = 2 * time.Minute
	for _, tc := range []struct {
		name                string
		interval, remaining time.Duration
		want                int
	}{
		{"just set", 30 * time.Second, duration, 0},
		{"before interval", 30 * time.Second, 91 * time.Second, 0},
		{"at interval", 30 * time.Second, 90 * time.Second, 1},
		{"near expiry", 30 * time.Second, time.Second, 1},
		{"expired", 30 * time.Second, 0, 0},
		{"zero before fallback", 0, 61 * time.Second, 0},
		{"zero at fallback", 0, time.Minute, 1},
		{"equal before fallback", duration, 61 * time.Second, 0},
		{"equal at fallback", duration, time.Minute, 1},
		{"negative before fallback", -time.Second, 61 * time.Second, 0},
		{"negative at fallback", -time.Second, time.Minute, 1},
		{"greater before fallback", duration + time.Second, 61 * time.Second, 0},
		{"greater at fallback", duration + time.Second, time.Minute, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := coordinatorSnapshot(7)
			snapshot.Assignments = []domain.WorkerAssignmentObservation{{
				AssignmentID: "assignment-1", AssignmentEpoch: 1, State: domain.AssignmentClaimed,
			}}
			assignment := domain.Assignment{
				ID: "assignment-1", Epoch: 1, State: domain.AssignmentClaimed,
				WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch,
				LeaseToken: "lease-1", LeaseExpiresAt: coordinatorTestTime.Add(tc.remaining),
			}
			got := leaseRenewalsForWorker([]domain.Assignment{assignment}, snapshot, coordinatorTestTime, duration, tc.interval)
			if len(got) != tc.want {
				t.Fatalf("renewals = %#v, want %d", got, tc.want)
			}
			if len(got) != 0 && !got[0].LeaseExpiresAt.Equal(coordinatorTestTime.Add(duration)) {
				t.Fatalf("renewed expiry = %v", got[0].LeaseExpiresAt)
			}
		})
	}
}

type leaseCadenceStore struct {
	*sqlite.Store
	snapshotWrites int
}

func (s *leaseCadenceStore) SaveWorkerSnapshot(ctx context.Context, snapshot domain.WorkerSnapshot) error {
	s.snapshotWrites++
	return s.Store.SaveWorkerSnapshot(ctx, snapshot)
}

type leaseCadenceTransport struct {
	*exchangeTransport
	renewalCalls int
}

func (t *leaseCadenceTransport) DeliverLeaseRenewals(ctx context.Context, renewals []domain.AssignmentLeaseRenewal) (domain.WorkerSnapshot, error) {
	t.renewalCalls++
	return t.exchangeTransport.DeliverLeaseRenewals(ctx, renewals)
}

func TestFleetCoordinatorLeaseRenewalCadenceAndIdleWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &leaseCadenceStore{Store: db}
	snapshot := coordinatorSnapshot(1)
	snapshot.Assignments = []domain.WorkerAssignmentObservation{{
		AssignmentID: "assignment-1", AssignmentEpoch: 1, State: domain.AssignmentClaimed,
		Control: domain.ControlRunning, ThreadID: "thread-1", WorkspacePath: "/tmp/work",
		ObservedAt: coordinatorTestTime,
	}}
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
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	now := coordinatorTestTime
	coordinator := FleetCoordinator{Store: store, Now: func() time.Time { return now }, LeaseRenewInterval: time.Minute}
	transport := &leaseCadenceTransport{exchangeTransport: &exchangeTransport{
		workerID: snapshot.WorkerID, workerEpoch: snapshot.WorkerEpoch,
	}}
	expectedExpiry := assignment.LeaseExpiresAt
	for seconds := 10; seconds <= 110; seconds += 10 {
		t.Run(fmt.Sprintf("%ds", seconds), func(t *testing.T) {
			now = coordinatorTestTime.Add(time.Duration(seconds) * time.Second)
			snapshot.Sequence++
			snapshot.ObservedAt = now
			snapshot.ValidUntil = now.Add(time.Minute)
			snapshot.Assignments[0].ObservedAt = now
			renewed := snapshot
			renewed.Sequence++
			renewed.ObservedAt = now.Add(time.Nanosecond)
			transport.snapshots = []domain.WorkerSnapshot{snapshot}
			transport.renewalSnapshot = &renewed
			writesBefore := store.snapshotWrites
			callsBefore := transport.renewalCalls
			report, err := coordinator.ReconcileWorker(ctx, transport, testOfferBuilder{},
				WorkerAdmissionPolicy{QuotaChecksDisabled: true}, nil, nil, time.Minute, 2*time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			wantRenewals, wantWrites := 0, 1
			if seconds == 60 {
				wantRenewals, wantWrites = 1, 2
				expectedExpiry = now.Add(2 * time.Minute)
			}
			if len(report.Renewed) != wantRenewals || transport.renewalCalls-callsBefore != wantRenewals {
				t.Fatalf("renewed=%+v delivery calls=%d, want %d", report.Renewed, transport.renewalCalls-callsBefore, wantRenewals)
			}
			if got := store.snapshotWrites - writesBefore; got != wantWrites {
				t.Fatalf("snapshot writes=%d, want %d", got, wantWrites)
			}
			records, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(records.Assignments) != 1 || records.Assignments[0].State != domain.AssignmentClaimed ||
				!records.Assignments[0].LeaseExpiresAt.Equal(expectedExpiry) || !records.Assignments[0].LeaseExpiresAt.After(now) ||
				len(report.Expired) != 0 {
				t.Fatalf("lease state at %v: %+v expired=%+v", now, records.Assignments, report.Expired)
			}
			events, err := store.LoadAuditEvents(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			leaseEvents := 0
			for _, event := range events {
				if event.Kind == "assignment-lease-renewed" {
					leaseEvents++
					if !event.CreatedAt.Equal(coordinatorTestTime.Add(time.Minute)) {
						t.Fatalf("renewal audit time = %v", event.CreatedAt)
					}
				}
			}
			wantEvents := 0
			if seconds >= 60 {
				wantEvents = 1
			}
			if leaseEvents != wantEvents {
				t.Fatalf("lease audit events=%d, want %d", leaseEvents, wantEvents)
			}
			snapshot.Sequence = report.Snapshot.Sequence
		})
	}
	if transport.renewalCalls != 1 || len(transport.renewals) != 1 {
		t.Fatalf("renewal calls=%d renewals=%+v", transport.renewalCalls, transport.renewals)
	}
}
