package backupsnapshot

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	storesqlite "github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var supervisionBackupTime = time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)

const supervisedRunID = "run-supervised"

// supervisionFacts is everything schema 18 made durable for one run, read back
// through the store's own accessors. Between them these four reads cover all
// nine supervision tables: the record, its gates, holds, incidents and
// activations and the append-only decision history come from the projection,
// the wake outbox and the event inbox come from the activation rows, and the
// idempotency receipt is looked up by its request key.
//
// A backup that captures the database file captures all of it by construction,
// which is precisely the claim this fixture is built to check rather than
// assume.
type supervisionFacts struct {
	Projection storesqlite.SupervisionProjection     `json:"projection"`
	Activation storesqlite.SupervisionActivationRows `json:"activationRows"`
	Receipt    json.RawMessage                       `json:"receipt"`
	ReceiptSHA string                                `json:"receiptSha256"`
	ReceiptOK  bool                                  `json:"receiptFound"`
}

func readSupervisionFacts(t *testing.T, store *storesqlite.Store) []byte {
	t.Helper()
	ctx := context.Background()
	projection, err := store.LoadSupervisionProjection(ctx, supervisedRunID)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.LoadSupervisionActivationRows(ctx, supervisedRunID)
	if err != nil {
		t.Fatal(err)
	}
	answer, digest, found, err := store.LookupSupervisionReceipt(ctx, supervisedRunID, "request-receipt-1")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(supervisionFacts{
		Projection: projection, Activation: rows,
		Receipt: answer, ReceiptSHA: digest, ReceiptOK: found,
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// seedMidSupervisionRun builds a run stopped in the middle of supervision: one
// gate accepted on bound evidence, one hold placed and still live, one review
// incident open, one activation committed, and an idempotency receipt for a
// request that was already answered. That is the state an operator is most
// likely to be holding when they take a backup, and the state whose loss would
// be least recoverable.
func seedMidSupervisionRun(t *testing.T, databasePath string) {
	t.Helper()
	ctx := context.Background()
	store, err := storesqlite.OpenMigrated(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	store.SetClock(func() time.Time { return supervisionBackupTime })

	run := domain.WorkflowRun{
		ID: supervisedRunID, WorkflowID: "workflow-supervised", GraphRevision: 1,
		Progress: domain.ProgressActive, Revision: 1,
		CreatedAt: supervisionBackupTime, UpdatedAt: supervisionBackupTime,
	}
	if err := store.SaveCoordinatorRecords(ctx, storesqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-supervised", Version: 1, Name: "supervised-three-node",
			Class: domain.TaskClassRequired, TaskIDs: []string{"task-producer", "task-protected"},
			CreatedAt: supervisionBackupTime,
		}},
		WorkflowRuns: []domain.WorkflowRun{run},
		Tasks: []domain.Task{
			{ID: "task-producer", WorkflowID: "workflow-supervised", Name: "producer", Class: domain.TaskClassRequired},
			{
				ID: "task-protected", WorkflowID: "workflow-supervised", Name: "protected",
				Class: domain.TaskClassRequired, Needs: []string{"task-producer"},
			},
		},
		Attempts: []domain.Attempt{
			{
				ID: "attempt-producer", WorkflowRunID: run.ID, TaskID: "task-producer", Number: 1,
				Progress: domain.ProgressSucceeded, Control: domain.ControlStopped,
				Revision: 5, UpdatedAt: supervisionBackupTime,
			},
			{
				ID: "attempt-protected", WorkflowRunID: run.ID, TaskID: "task-protected", Number: 1,
				Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
				Revision: 1, UpdatedAt: supervisionBackupTime,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	record := domain.SupervisionRecord{
		RunID: run.ID,
		Config: domain.SupervisionConfig{
			Route: domain.ProviderRoute{
				ProviderInstanceID: "claudeAgent", Model: "claude-fable-5-1", QuotaPoolID: "claude-main",
			},
			PromptArtifactID:      "artifact-overseer",
			MaxActivations:        8,
			MaxTurnsPerActivation: 3,
			ActivationDeadline:    2 * time.Hour,
		},
	}
	stored, err := store.PutSupervision(ctx, storesqlite.SupervisionMaterialization{
		Record: record,
		Gates: []domain.Gate{{
			RunID: run.ID, State: domain.GateReadyForReview, GraphRevision: 1,
			Definition: domain.GateDefinition{
				ID: "gate:" + run.ID + ":analysis_review", Name: "analysis_review",
				ObservedTaskIDs: []string{"task-producer"}, ProtectedTaskIDs: []string{"task-protected"},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	operator := domain.Actor{Kind: domain.ActorOperator, Principal: "operator-1"}
	if _, err := store.OpenReviewIncident(ctx, storesqlite.IncidentRequest{
		RunID: run.ID, IncidentID: "incident-1", RequestID: "request-incident-1",
		Actor: operator, SourceEventID: "event-1", SourceTaskID: "task-producer",
		SourceAttemptID: "attempt-producer", GateID: "gate:" + run.ID + ":analysis_review",
		RequiredDisposition: domain.DispositionGateDecision,
		Reason:              "the analysis gate is ready for review", OpenedAt: supervisionBackupTime,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DecideGate(ctx, storesqlite.GateDecisionRequest{
		RunID: run.ID, GateID: "gate:" + run.ID + ":analysis_review", RequestID: "request-accept-1",
		Actor: operator, ExpectedGraphRevision: 1, ExpectedGateRevision: 1,
		Evidence: domain.EvidenceSnapshot{
			ID: "evidence-1", GraphRevision: 1, TakenAt: supervisionBackupTime,
			Producers: []domain.ProducerEvidence{
				{TaskID: "task-producer", AttemptID: "attempt-producer", ResultRevision: 5},
			},
		},
		Outcome: domain.GateDecisionAccept, Reason: "outputs match the rubric",
		DecidedAt: supervisionBackupTime,
	}); err != nil {
		t.Fatal(err)
	}
	// A second incident stays open across the backup. An accepted gate closes
	// only its own matching incident, so this one is exactly the record a
	// restore must not quietly drop: it is what still withholds settlement.
	if _, err := store.OpenReviewIncident(ctx, storesqlite.IncidentRequest{
		RunID: run.ID, IncidentID: "incident-2", RequestID: "request-incident-2",
		Actor: operator, SourceEventID: "event-2", SourceTaskID: "task-protected",
		RequiredDisposition: domain.DispositionOperatorAction,
		Reason:              "the protected branch needs an operator decision", OpenedAt: supervisionBackupTime,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PlaceHold(ctx, storesqlite.HoldRequest{
		RunID: run.ID, HoldID: "hold-1", RequestID: "request-hold-1", Actor: operator,
		Scope: domain.HoldScope{Kind: domain.HoldScopeRun}, ExpectedGraphRevision: 1,
		Reason: "pause the run while the second incident is open", PlacedAt: supervisionBackupTime,
	}); err != nil {
		t.Fatal(err)
	}

	current, err := store.LoadSupervisionActivationRows(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	lease := supervisionBackupTime.Add(time.Hour)
	deadline := supervisionBackupTime.Add(2 * time.Hour)
	activation := domain.Activation{
		ID: "activation-1", RunID: run.ID, Epoch: current.Record.ActivationEpoch + 1,
		DispatchIdentity: "dispatch-1", Principal: "overseer-1",
		State: domain.ActivationPendingDispatch, LeaseToken: "lease-1",
		LeaseExpiresAt: &lease, Deadline: &deadline, IncidentID: "incident-2",
	}
	advanced := current.Record
	advanced.ActivationEpoch = activation.Epoch
	if err := store.CommitSupervisionActivationRows(ctx, storesqlite.SupervisionActivationRowCommit{
		RunID: run.ID, ExpectedRecordRevision: current.Record.Revision,
		Record: advanced, Activation: activation,
		Outbox: []storesqlite.SupervisionOutboxRow{{
			ID: "outbox-1", RunID: run.ID, ActivationID: activation.ID, Delivery: "pending",
			Record: json.RawMessage(`{"kind":"wake","incidentId":"incident-2"}`),
		}},
		RequestID: "request-activation-1", CommittedAt: supervisionBackupTime,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendSupervisionInbox(ctx, run.ID, []storesqlite.SupervisionInboxRow{{
		ID: "inbox-1", RunID: run.ID, Sequence: 1,
		Record: json.RawMessage(`{"kind":"gate-review-ready","gate":"analysis_review"}`),
	}}); err != nil {
		t.Fatal(err)
	}

	if err := store.RecordSupervisionReceipt(ctx, run.ID, "request-receipt-1",
		"digest-of-the-first-payload", map[string]string{"outcome": "accepted"}); err != nil {
		t.Fatal(err)
	}

	if stored.RunID != run.ID {
		t.Fatalf("stored supervision record = %#v", stored)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestSnapshotCapturesAndRestoresSupervisionState is verification gate 6's
// backup clause: migration, backup and restore preserve decision history. The
// snapshot is taken with the run stopped in mid-supervision and restored into a
// fresh pair of roots, and the whole supervision projection, the wake outbox,
// the event inbox and the idempotency receipt are compared byte for byte.
//
// The comparison is deliberately over marshalled bytes rather than field by
// field. A new supervision table or a new field on an existing record would
// otherwise be added and silently left out of this check; encoding the whole
// read set means anything durable that stops surviving a restore shows up here
// as a difference.
func TestSnapshotCapturesAndRestoresSupervisionState(t *testing.T) {
	root := t.TempDir()
	database := filepath.Join(root, "source", "state.db")
	artifacts := filepath.Join(root, "source-artifacts")
	snapshot := filepath.Join(root, "snapshots", "snapshot-supervised")
	if err := os.MkdirAll(filepath.Dir(database), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(artifacts, "runs", supervisedRunID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(artifacts, "runs", supervisedRunID, "rubric.md"),
		[]byte("accept when the interfaces and the tests agree\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	seedMidSupervisionRun(t, database)

	source, err := storesqlite.Open(database)
	if err != nil {
		t.Fatal(err)
	}
	before := readSupervisionFacts(t, source)
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	manager := Manager{
		Limits: Limits{MaxFiles: 100, MaxBytes: 1 << 20},
		Now:    func() time.Time { return supervisionBackupTime },
	}
	manifest, err := manager.Create(context.Background(), database, artifacts, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.SchemaVersion != storesqlite.CurrentSchemaVersion() {
		t.Fatalf("snapshot schema version = %d, want %d",
			manifest.SchemaVersion, storesqlite.CurrentSchemaVersion())
	}
	if _, err := manager.Verify(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}

	restoredDatabase := filepath.Join(root, "restore", "state.db")
	restoredArtifacts := filepath.Join(root, "restore-artifacts")
	if err := os.MkdirAll(filepath.Dir(restoredDatabase), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Restore(context.Background(), snapshot, restoredDatabase, restoredArtifacts); err != nil {
		t.Fatal(err)
	}
	restored, err := storesqlite.Open(restoredDatabase)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if version, err := restored.SchemaVersion(context.Background()); err != nil ||
		version != storesqlite.CurrentSchemaVersion() {
		t.Fatalf("restored schema version = %d, %v", version, err)
	}
	if after := readSupervisionFacts(t, restored); string(after) != string(before) {
		t.Fatalf("supervision state changed across backup and restore:\nbefore %s\nafter  %s", before, after)
	}

	// The restored state is not merely equal, it is still authoritative: the
	// gate is accepted, the hold and the second incident still exist, and the
	// run is still supervised.
	snapshotAfter, err := restored.LoadSupervisionSnapshot(context.Background(), supervisedRunID)
	if err != nil || !snapshotAfter.Supervised {
		t.Fatalf("restored supervision snapshot = %#v, err = %v", snapshotAfter, err)
	}
	projection, err := restored.LoadSupervisionProjection(context.Background(), supervisedRunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(projection.Decisions) == 0 {
		t.Fatal("the append-only decision history did not survive the restore")
	}
	if len(projection.Gates) != 1 || projection.Gates[0].State != domain.GateAccepted {
		t.Fatalf("restored gates = %#v", projection.Gates)
	}
	if len(projection.Holds) != 1 || len(projection.Incidents) != 2 {
		t.Fatalf("restored holds = %#v incidents = %#v", projection.Holds, projection.Incidents)
	}
	if len(projection.Activations) != 1 {
		t.Fatalf("restored activations = %#v", projection.Activations)
	}
	rows, err := restored.LoadSupervisionActivationRows(context.Background(), supervisedRunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Outbox) != 1 || len(rows.Inbox) != 1 {
		t.Fatalf("restored outbox = %#v inbox = %#v", rows.Outbox, rows.Inbox)
	}
	_, _, found, err := restored.LookupSupervisionReceipt(
		context.Background(), supervisedRunID, "request-receipt-1")
	if err != nil || !found {
		t.Fatalf("restored idempotency receipt found = %v, err = %v", found, err)
	}
	raw, err := os.ReadFile(filepath.Join(restoredArtifacts, "runs", supervisedRunID, "rubric.md"))
	if err != nil || len(raw) == 0 {
		t.Fatalf("restored rubric artifact = %q, %v", raw, err)
	}
}
