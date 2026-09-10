package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestWorkerCommandsPersistBeforeDeliveryAndAcknowledgeIdempotently(t *testing.T) {
	store := openFleetTestStore(t)
	claimFleetAssignment(t, store)
	command := fleetWorkerCommand(domain.WorkerCommandPrepare, "command-1", 1, fleetTestTime.Add(2*time.Second))

	committed, err := store.CommitWorkerCommands(context.Background(), []domain.WorkerCommand{command})
	if err != nil {
		t.Fatalf("commit worker command: %v", err)
	}
	replayed, err := store.CommitWorkerCommands(context.Background(), []domain.WorkerCommand{command})
	if err != nil || !reflect.DeepEqual(replayed, committed) {
		t.Fatalf("replay worker command: commands=%#v error=%v", replayed, err)
	}
	changed := command
	changed.Kind = domain.WorkerCommandDispatch
	if _, err := store.CommitWorkerCommands(context.Background(), []domain.WorkerCommand{changed}); !errors.Is(err, ErrWorkerCommand) {
		t.Fatalf("changed replay error = %v, want worker command rejection", err)
	}

	pending, err := store.LoadPendingWorkerCommands(context.Background(), "normandy", "worker-epoch-1", 1, 1, fleetTestTime.Add(3*time.Second))
	if err != nil || !reflect.DeepEqual(pending, []domain.WorkerCommand{command}) {
		t.Fatalf("pending commands = %#v, error=%v", pending, err)
	}

	next := fleetSnapshot(1, "worker-epoch-1", 2, true, fleetTestTime.Add(2*time.Minute))
	next.ObservedAt = fleetTestTime.Add(4 * time.Second)
	saveFleetSnapshot(t, store, next)
	if _, err := store.LoadPendingWorkerCommands(context.Background(), "normandy", "worker-epoch-1", 1, 1, fleetTestTime.Add(5*time.Second)); !errors.Is(err, ErrStaleWorkerSnapshot) {
		t.Fatalf("stale delivery error = %v, want stale worker snapshot", err)
	}
	pending, err = store.LoadPendingWorkerCommands(context.Background(), "normandy", "worker-epoch-1", 1, 2, fleetTestTime.Add(5*time.Second))
	if err != nil || len(pending) != 1 || pending[0].ID != command.ID {
		t.Fatalf("delivery from current snapshot = %#v, error=%v", pending, err)
	}

	delayedAcknowledgement := fleetWorkerAcknowledgement(command, 1, fleetTestTime.Add(6*time.Second))
	got, err := store.AcknowledgeWorkerCommand(context.Background(), delayedAcknowledgement)
	if err != nil || !reflect.DeepEqual(got, delayedAcknowledgement) {
		t.Fatalf("delayed acknowledgement = %#v, error=%v", got, err)
	}
	conflictingAcknowledgement := fleetWorkerAcknowledgement(command, 2, fleetTestTime.Add(6*time.Second))
	if _, err := store.AcknowledgeWorkerCommand(context.Background(), conflictingAcknowledgement); !errors.Is(err, ErrWorkerAcknowledgement) {
		t.Fatalf("conflicting acknowledgement error = %v", err)
	}

	latest := next
	latest.Sequence = 3
	latest.ObservedAt = fleetTestTime.Add(7 * time.Second)
	latest.ValidUntil = fleetTestTime.Add(3 * time.Minute)
	saveFleetSnapshot(t, store, latest)
	got, err = store.AcknowledgeWorkerCommand(context.Background(), delayedAcknowledgement)
	if err != nil || !reflect.DeepEqual(got, delayedAcknowledgement) {
		t.Fatalf("replay durable acknowledgement after newer snapshot = %#v, error=%v", got, err)
	}
	pending, err = store.LoadPendingWorkerCommands(context.Background(), "normandy", "worker-epoch-1", 1, 3, fleetTestTime.Add(8*time.Second))
	if err != nil || len(pending) != 0 {
		t.Fatalf("commands after acknowledgement = %#v, error=%v", pending, err)
	}
}

func TestWorkerCommandsAreFencedAcrossWorkerAndCoordinatorRestarts(t *testing.T) {
	store := openFleetTestStore(t)
	claimFleetAssignment(t, store)
	command := fleetWorkerCommand(domain.WorkerCommandDispatch, "command-dispatch", 1, fleetTestTime.Add(2*time.Second))
	if _, err := store.CommitWorkerCommands(context.Background(), []domain.WorkerCommand{command}); err != nil {
		t.Fatal(err)
	}
	duplicateDispatch := command
	duplicateDispatch.ID = "command-dispatch-duplicate"
	if _, err := store.CommitWorkerCommands(context.Background(), []domain.WorkerCommand{duplicateDispatch}); !errors.Is(err, ErrWorkerCommand) {
		t.Fatalf("duplicate dispatch error = %v, want worker command rejection", err)
	}
	restarted := fleetSnapshot(1, "worker-epoch-2", 1, true, fleetTestTime.Add(2*time.Minute))
	restarted.ObservedAt = fleetTestTime.Add(3 * time.Second)
	saveFleetSnapshot(t, store, restarted)
	if _, err := store.LoadPendingWorkerCommands(context.Background(), "normandy", "worker-epoch-1", 1, 1, fleetTestTime.Add(4*time.Second)); !errors.Is(err, ErrStaleWorkerSnapshot) {
		t.Fatalf("old worker delivery error = %v, want stale snapshot", err)
	}
	pending, err := store.LoadPendingWorkerCommands(context.Background(), "normandy", "worker-epoch-2", 1, 1, fleetTestTime.Add(4*time.Second))
	if err != nil || len(pending) != 0 {
		t.Fatalf("new worker received old command: commands=%#v error=%v", pending, err)
	}
	oldAck := fleetWorkerAcknowledgement(command, 1, fleetTestTime.Add(4*time.Second))
	if _, err := store.AcknowledgeWorkerCommand(context.Background(), oldAck); !errors.Is(err, ErrStaleWorkerSnapshot) {
		t.Fatalf("old worker acknowledgement error = %v, want stale snapshot", err)
	}

	epoch, err := store.AdvanceCoordinatorEpoch(context.Background(), 1)
	if err != nil || epoch != 2 {
		t.Fatalf("advance coordinator epoch = %d, error=%v", epoch, err)
	}
	if _, err := store.LoadPendingWorkerCommands(context.Background(), "normandy", "worker-epoch-2", 1, 1, fleetTestTime.Add(5*time.Second)); !errors.Is(err, ErrStaleCoordinatorEpoch) {
		t.Fatalf("old coordinator delivery error = %v, want stale coordinator epoch", err)
	}
	reconnected := fleetSnapshot(2, "worker-epoch-2", 2, true, fleetTestTime.Add(3*time.Minute))
	reconnected.ObservedAt = fleetTestTime.Add(6 * time.Second)
	saveFleetSnapshot(t, store, reconnected)
	pending, err = store.LoadPendingWorkerCommands(context.Background(), "normandy", "worker-epoch-2", 2, 2, fleetTestTime.Add(7*time.Second))
	if err != nil || len(pending) != 0 {
		t.Fatalf("new coordinator received old command: commands=%#v error=%v", pending, err)
	}
}

func TestAssignmentLeaseRenewalAndExpiryFailClosed(t *testing.T) {
	store := openFleetTestStore(t)
	claimFleetAssignment(t, store)
	next := fleetSnapshot(1, "worker-epoch-1", 2, true, fleetTestTime.Add(3*time.Minute))
	next.ObservedAt = fleetTestTime.Add(20 * time.Second)
	saveFleetSnapshot(t, store, next)

	renewal := domain.AssignmentLeaseRenewal{
		CoordinatorEpoch: 1, WorkerID: "normandy", WorkerEpoch: "worker-epoch-1",
		WorkerSequence: 2, AssignmentID: "assignment-1", AssignmentEpoch: 1,
		LeaseToken: "lease-token-1", RenewedAt: fleetTestTime.Add(30 * time.Second),
		LeaseExpiresAt: fleetTestTime.Add(2 * time.Minute),
	}
	renewed, err := store.RenewAssignmentLease(context.Background(), renewal)
	if err != nil || !renewed.LeaseExpiresAt.Equal(renewal.LeaseExpiresAt) {
		t.Fatalf("renew assignment lease = %#v, error=%v", renewed, err)
	}
	replayed, err := store.RenewAssignmentLease(context.Background(), renewal)
	if err != nil || !reflect.DeepEqual(replayed, renewed) {
		t.Fatalf("replay assignment lease renewal = %#v, error=%v", replayed, err)
	}

	stale := renewal
	stale.WorkerSequence = 1
	stale.LeaseExpiresAt = fleetTestTime.Add(3 * time.Minute)
	if _, err := store.RenewAssignmentLease(context.Background(), stale); !errors.Is(err, ErrStaleWorkerSnapshot) {
		t.Fatalf("stale lease renewal error = %v, want stale worker snapshot", err)
	}
	expired, err := store.ExpireAssignmentLeases(context.Background(), 1, fleetTestTime.Add(90*time.Second))
	if err != nil || len(expired) != 0 {
		t.Fatalf("early lease expiry = %#v, error=%v", expired, err)
	}
	expired, err = store.ExpireAssignmentLeases(context.Background(), 1, fleetTestTime.Add(3*time.Minute))
	if err != nil || len(expired) != 1 || expired[0].State != domain.AssignmentUnknown {
		t.Fatalf("expired assignments = %#v, error=%v", expired, err)
	}
	again, err := store.ExpireAssignmentLeases(context.Background(), 1, fleetTestTime.Add(4*time.Minute))
	if err != nil || len(again) != 0 {
		t.Fatalf("repeated lease expiry = %#v, error=%v", again, err)
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if records.Assignments[0].State != domain.AssignmentUnknown ||
		records.Attempts[0].Control != domain.ControlPreparing {
		t.Fatalf("fail-closed projections: assignment=%#v attempt=%#v", records.Assignments[0], records.Attempts[0])
	}
}

func TestMigrationFromVersionEightBackfillsLeaseExpiry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	claimFleetAssignment(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`DROP TABLE coordinator_worker_acknowledgements`,
		`DROP INDEX coordinator_worker_commands_pending`,
		`DROP TABLE coordinator_worker_commands`,
		`DROP INDEX coordinator_assignments_lease_expiry`,
		`ALTER TABLE coordinator_assignments DROP COLUMN lease_expires_at`,
		`DELETE FROM schema_version WHERE version >= 9`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("restore version 8 schema: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(path)
	if err != nil {
		t.Fatalf("migrate version 8 database: %v", err)
	}
	defer store.Close()
	expired, err := store.ExpireAssignmentLeases(context.Background(), 1, fleetTestTime.Add(2*time.Minute))
	if err != nil || len(expired) != 1 || expired[0].ID != "assignment-1" {
		t.Fatalf("backfilled lease expiry = %#v, error=%v", expired, err)
	}
}

func claimFleetAssignment(t *testing.T, store *Store) {
	t.Helper()
	saveFleetAttempt(t, store, fleetAttempt("attempt-1"))
	saveFleetSnapshot(t, store, fleetSnapshot(1, "worker-epoch-1", 1, true, fleetTestTime.Add(time.Minute)))
	if _, err := store.CommitAssignmentPlan(context.Background(),
		fleetPlanCommit(1, "assignment-1", "attempt-1", "worker-epoch-1", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimAssignment(context.Background(),
		fleetClaim(1, "worker-epoch-1", fleetTestTime.Add(time.Second), fleetTestTime.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
}

func fleetWorkerCommand(kind domain.WorkerCommandKind, id string, sequence int64, createdAt time.Time) domain.WorkerCommand {
	return domain.WorkerCommand{
		ID: id, Kind: kind, WorkerID: "normandy", WorkerEpoch: "worker-epoch-1",
		CoordinatorEpoch: 1, AssignmentID: "assignment-1", AssignmentEpoch: 1,
		ExpectedWorkerSequence: sequence, CreatedAt: createdAt,
	}
}

func fleetWorkerAcknowledgement(command domain.WorkerCommand, sequence int64, acknowledgedAt time.Time) domain.WorkerAcknowledgement {
	return domain.WorkerAcknowledgement{
		CommandID: command.ID, WorkerID: command.WorkerID, WorkerEpoch: command.WorkerEpoch,
		CoordinatorEpoch: command.CoordinatorEpoch, AssignmentID: command.AssignmentID,
		AssignmentEpoch: command.AssignmentEpoch, WorkerSequence: sequence,
		Accepted: true, AcknowledgedAt: acknowledgedAt,
	}
}
