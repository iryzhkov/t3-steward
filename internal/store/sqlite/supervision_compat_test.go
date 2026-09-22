package sqlite

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// preSupervisionSchemaVersion is the schema a release without campaign
// supervision leaves behind. Every test in this file starts from a database at
// exactly that version, so the forward migration under test runs against state
// a previous binary actually wrote.
const preSupervisionSchemaVersion = 17

var compatTestTime = time.Date(2026, 9, 15, 8, 30, 0, 0, time.UTC)

// openStoreAtPreSupervisionSchema creates a database and migrates it only as
// far as schema 17, which is what a coordinator running the previous release
// would hold.
func openStoreAtPreSupervisionSchema(t *testing.T, path string) *Store {
	t.Helper()
	store, err := open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.migrateThrough(preSupervisionSchemaVersion); err != nil {
		t.Fatal(err)
	}
	store.SetClock(func() time.Time { return compatTestTime })
	return store
}

// seedPreSupervisionRuns writes two complete workflow runs of the shape the
// unsupervised three-node example produces: a finished run and a live one, with
// tasks, attempts and a worker snapshot. None of it mentions supervision,
// because at schema 17 nothing could.
func seedPreSupervisionRuns(t *testing.T, store *Store) CoordinatorRecords {
	t.Helper()
	now := compatTestTime
	records := CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-legacy", Version: 1, Name: "three-node-example",
			Class: domain.TaskClassSurplus, TaskIDs: []string{"task-interfaces", "task-tests", "task-join"},
			CreatedAt: now,
		}},
		WorkflowRuns: []domain.WorkflowRun{
			{
				ID: "run-finished", WorkflowID: "workflow-legacy", GraphRevision: 1,
				Progress: domain.ProgressSucceeded, Revision: 4, CreatedAt: now, UpdatedAt: now,
			},
			{
				ID: "run-live", WorkflowID: "workflow-legacy", GraphRevision: 1,
				Progress: domain.ProgressActive, Revision: 2, CreatedAt: now, UpdatedAt: now,
			},
		},
		Tasks: []domain.Task{
			{ID: "task-interfaces", WorkflowID: "workflow-legacy", Name: "interfaces", Class: domain.TaskClassSurplus},
			{ID: "task-tests", WorkflowID: "workflow-legacy", Name: "tests", Class: domain.TaskClassSurplus},
			{
				ID: "task-join", WorkflowID: "workflow-legacy", Name: "join", Class: domain.TaskClassSurplus,
				Needs: []string{"task-interfaces", "task-tests"},
			},
		},
		Attempts: []domain.Attempt{
			{
				ID: "attempt-interfaces", WorkflowRunID: "run-finished", TaskID: "task-interfaces", Number: 1,
				Progress: domain.ProgressSucceeded, Control: domain.ControlUnassigned, Revision: 3, UpdatedAt: now,
			},
			{
				ID: "attempt-tests", WorkflowRunID: "run-finished", TaskID: "task-tests", Number: 1,
				Progress: domain.ProgressSucceeded, Control: domain.ControlUnassigned, Revision: 3, UpdatedAt: now,
			},
			{
				ID: "attempt-join", WorkflowRunID: "run-live", TaskID: "task-join", Number: 1,
				Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, UpdatedAt: now,
			},
		},
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(context.Background(), domain.WorkerSnapshot{
		WorkerID: "normandy", WorkerEpoch: "worker-epoch-1", CoordinatorEpoch: 1, Sequence: 1,
		Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour),
		Inventory: domain.WorkerInventory{
			ID: "normandy", AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now,
		},
	}); err != nil {
		t.Fatal(err)
	}
	return records
}

func marshalRecords(t *testing.T, records CoordinatorRecords) string {
	t.Helper()
	raw, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func schemaVersionOf(t *testing.T, store *Store) int {
	t.Helper()
	version, err := store.SchemaVersion(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return version
}

func tableExists(t *testing.T, store *Store, table string) bool {
	t.Helper()
	var name string
	err := store.db.QueryRow(
		`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name)
	if err != nil {
		return false
	}
	return name == table
}

// supervisionTables is every durable table schema 18 adds. It is the list a
// backup, a restore and a migration all have to carry, and naming it once here
// keeps the three tests honest about the same set.
var supervisionTables = []string{
	"coordinator_supervision",
	"coordinator_supervision_activations",
	"coordinator_supervision_gates",
	"coordinator_supervision_decisions",
	"coordinator_supervision_holds",
	"coordinator_supervision_incidents",
	"coordinator_supervision_outbox",
	"coordinator_supervision_inbox",
	"coordinator_supervision_inbox_ack",
	"coordinator_supervision_receipts",
}

func TestSchemaSeventeenMigratesForwardWithPreSupervisionRuns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	old := openStoreAtPreSupervisionSchema(t, path)
	if version := schemaVersionOf(t, old); version != preSupervisionSchemaVersion {
		t.Fatalf("seeded schema version = %d, want %d", version, preSupervisionSchemaVersion)
	}
	for _, table := range supervisionTables {
		if tableExists(t, old, table) {
			t.Fatalf("table %q exists at schema %d", table, preSupervisionSchemaVersion)
		}
	}
	seedPreSupervisionRuns(t, old)
	before, err := old.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	beforeJSON := marshalRecords(t, before)
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}

	// The forward migration is whatever a new binary does on startup, which is
	// exactly OpenMigrated.
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatalf("migrate %d to %d: %v", preSupervisionSchemaVersion, currentSchemaVersion, err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if version := schemaVersionOf(t, store); version != currentSchemaVersion || currentSchemaVersion != 28 {
		t.Fatalf("migrated schema version = %d, current = %d", version, currentSchemaVersion)
	}
	for _, table := range supervisionTables {
		if !tableExists(t, store, table) {
			t.Fatalf("table %q missing after migration", table)
		}
	}

	// Every pre-supervision record survives the migration unchanged. This is
	// the compatibility claim: schema 18 adds tables and rewrites nothing.
	after, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if afterJSON := marshalRecords(t, after); afterJSON != beforeJSON {
		t.Fatalf("coordinator records changed across migration:\nbefore %s\nafter  %s", beforeJSON, afterJSON)
	}

	// Backfill is explicitly empty: no run gets a supervision row, because the
	// absence of a row is the unsupervised case and creating empty records
	// would make every existing run supervised by nothing.
	for _, table := range supervisionTables {
		var rows int
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 0 {
			t.Fatalf("table %q holds %d rows after migration, want none", table, rows)
		}
	}
	for _, runID := range []string{"run-finished", "run-live"} {
		snapshot, err := store.LoadSupervisionSnapshot(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.Supervised {
			t.Fatalf("run %q is supervised after migration", runID)
		}
	}

	// Re-running the migration is a no-op: no second version row, no changed
	// record, no dropped table. An operator who runs it twice, or a coordinator
	// restarted mid-deployment, must not see a different database.
	if err := store.Migrate(); err != nil {
		t.Fatalf("repeat migration: %v", err)
	}
	var recorded int
	if err := store.db.QueryRow(
		`SELECT COUNT(*) FROM schema_version WHERE version = ?`, currentSchemaVersion).Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	if recorded != 1 {
		t.Fatalf("schema version %d recorded %d times", currentSchemaVersion, recorded)
	}
	repeated, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if repeatedJSON := marshalRecords(t, repeated); repeatedJSON != beforeJSON {
		t.Fatalf("coordinator records changed across a repeated migration:\nbefore %s\nafter  %s", beforeJSON, repeatedJSON)
	}

	// A migrated database still accepts new supervision, which proves the
	// tables the migration created are usable and not merely present.
	if _, err := store.PutSupervision(context.Background(), SupervisionMaterialization{
		Record: domain.SupervisionRecord{RunID: "run-live", Config: supervisionTestConfig()},
	}); err != nil {
		t.Fatalf("put supervision on a migrated database: %v", err)
	}
	snapshot, err := store.LoadSupervisionSnapshot(context.Background(), "run-live")
	if err != nil || !snapshot.Supervised {
		t.Fatalf("supervised snapshot after migration = %#v, err = %v", snapshot, err)
	}
}

// TestUnsupervisedRunContributesNoSupervisionBytesToItsProjection is the
// mechanism behind "unsupervised runs are unchanged". The settlement projection
// byte-compares its whole marshalled read set inside the write transaction, so
// a supervision read set that encoded as an empty object rather than as nothing
// would change every unsupervised run's projection bytes and every digest
// derived from them. It has to be absent, not empty.
func TestUnsupervisedRunContributesNoSupervisionBytesToItsProjection(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	seedPreSupervisionRuns(t, store)
	ctx := context.Background()
	for _, runID := range []string{"run-finished", "run-live", "run-that-does-not-exist"} {
		readSet, err := store.SupervisionReadSet(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if readSet != nil {
			t.Fatalf("run %q has a supervision read set: %#v", runID, readSet)
		}
		raw, err := json.Marshal(readSet)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != "null" {
			t.Fatalf("run %q encodes its absent supervision as %s", runID, raw)
		}
		projection, err := store.LoadSupervisionProjection(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if projection.Record != nil || len(projection.Gates) != 0 || len(projection.Holds) != 0 ||
			len(projection.Activations) != 0 || len(projection.Incidents) != 0 ||
			len(projection.Decisions) != 0 {
			t.Fatalf("run %q has a supervision projection: %#v", runID, projection)
		}
	}
}

func TestSupervisionSchemaRefusesABinaryExpectingTheOlderSchema(t *testing.T) {
	// A coordinator on schema 18 must refuse to be opened by a binary whose
	// newest known schema is 17. The guard compares the recorded version
	// against this binary's own currentSchemaVersion, so the situation is
	// reproduced here by recording one version above what this build knows:
	// the comparison, the message and the failure mode are the same ones a
	// 17-expecting binary meets when it opens an 18 database.
	path := filepath.Join(t.TempDir(), "state.db")
	store := openSupervisionStore(t, path)
	seedPreSupervisionRuns(t, store)
	if _, err := store.db.Exec(
		`INSERT INTO schema_version(version) VALUES (?)`, currentSchemaVersion+1); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenMigrated(path)
	if err == nil {
		_ = reopened.Close()
		t.Fatal("a database newer than this binary was opened and migrated")
	}
	if !strings.Contains(err.Error(), "newer than supported") {
		t.Fatalf("newer schema error = %v", err)
	}
	// The refusal is the whole behaviour: nothing was rewritten on the way out,
	// so the operator can still start the newer binary again.
	inspect, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer inspect.Close()
	if version := schemaVersionOf(t, inspect); version != currentSchemaVersion+1 {
		t.Fatalf("schema version after a refused open = %d", version)
	}
}

// TestRollbackFromSupervisionSchemaHasNoReverseMigration pins the rollback
// decision as a test rather than leaving it to prose alone. This store has no
// reverse-migration mechanism at all: Migrate only moves forward, and the
// supported rollback is a coherent stopped backup restore. If a later change
// adds a reverse path, this test is the place that has to be updated, which is
// the point of writing the absence down.
func TestRollbackFromSupervisionSchemaHasNoReverseMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openSupervisionStore(t, path)
	seedPreSupervisionRuns(t, store)
	for _, migration := range versionedMigrations {
		if migration.version > currentSchemaVersion {
			t.Fatalf("migration %d is above the current schema version", migration.version)
		}
	}
	// Asking for an older target does not move the database back down.
	if err := store.migrateThrough(preSupervisionSchemaVersion); err != nil {
		t.Fatal(err)
	}
	if version := schemaVersionOf(t, store); version != currentSchemaVersion {
		t.Fatalf("schema version after asking for %d = %d, want it unchanged at %d",
			preSupervisionSchemaVersion, version, currentSchemaVersion)
	}
	for _, table := range supervisionTables {
		if !tableExists(t, store, table) {
			t.Fatalf("table %q was dropped; this store has no reverse migration", table)
		}
	}
}
