package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestRecoveryEpisodeExhaustsOnceAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	now := time.Date(2026, 9, 21, 16, 0, 0, 0, time.UTC)
	config := domain.RecoveryConfig{Version: domain.RecoveryContractV1,
		Route: domain.ProviderRoute{ProviderInstanceID: "repairer", Model: "model"}, PromptArtifactID: "prompt",
		MaxAttemptsPerIncident: 1, IncidentDeadline: time.Hour, StalledAfter: time.Minute}
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	store.SetClock(func() time.Time { return now.Add(2 * time.Minute) })
	prepareRecoveryFence(t, store, "run", "attempt-1", now, config)
	root := domain.RecoveryDiagnosticIdentity{FailureFingerprint: "root-failure", EvidenceFingerprint: "root-evidence", StrategyFingerprint: "root-strategy"}
	recovery := domain.NewRecoveryIncident(config, root, now)
	if _, _, err := store.OpenRecoveryIncident(ctx, RecoveryIncidentRequest{
		RunID: "run", IncidentID: "incident", EventID: "event-root", SourceTaskID: "task", SourceAttemptID: "attempt-1",
		ExpectedGraphRevision: 1, SourceAttemptRevision: 1, RecoveryConfig: config, Reason: "root failure",
		Recovery: *recovery, EventRecord: []byte(`{"id":"event-root"}`), OpenedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	retry := domain.Attempt{ID: "attempt-2", WorkflowRunID: "run", TaskID: "task", Number: 2,
		Progress: domain.ProgressFailed, Control: domain.ControlStopped, Revision: 1, Failure: "retry failed", UpdatedAt: now.Add(time.Minute)}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{retry}}); err != nil {
		t.Fatal(err)
	}
	state, err := store.LoadSupervisionAdminState(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	incident := state.Incidents[0].Incident
	incident.Recovery.AttemptsUsed = 1
	incident.Recovery.CurrentAttemptID = retry.ID
	incident.Recovery.State = domain.RecoveryRecovering
	incident.Recovery.NextAction = domain.RecoveryNoAction
	raw, _ := json.Marshal(incident)
	if _, err := store.db.ExecContext(ctx, "UPDATE coordinator_supervision_incidents SET record = ? WHERE id = ?", raw, incident.ID); err != nil {
		t.Fatal(err)
	}
	gate := domain.ReviewIncident{ID: "gate-review", RunID: "run", SourceEventID: "gate-event", GateID: "gate",
		Revision: 1, RequiredDisposition: domain.DispositionGateDecision, State: domain.IncidentOpen, Reason: "review", OpenedAt: now}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := saveSupervisionIncidentTx(ctx, tx, gate); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.SetClock(func() time.Time { return now.Add(2 * time.Minute) })
	request := RecoveryAttemptFailureRequest{
		RunID: "run", IncidentID: "incident", EventID: "event-retry", AttemptID: retry.ID, Reason: "retry failed",
		ExpectedIncidentRevision: 1, ExpectedGraphRevision: 1, AttemptRevision: 1,
		FailureFingerprint: "retry-failure", EvidenceFingerprint: "retry-evidence",
		EventRecord: []byte(`{"id":"event-retry"}`),
	}
	got, err := store.RecordRecoveryAttemptFailure(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if got.Recovery.State != domain.RecoveryNeedsHuman || got.Recovery.NextAction != domain.RecoveryEscalateHuman ||
		got.Recovery.RootDiagnostic != root || got.Recovery.AttemptBudget != 1 || got.Recovery.AttemptsUsed != 1 {
		t.Fatalf("exhausted incident=%+v", got)
	}
	if _, err := store.RecordRecoveryAttemptFailure(ctx, request); !errors.Is(err, ErrSupervisionRequestConflict) {
		t.Fatalf("replay error=%v, want conflict", err)
	}
	var outbox int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM coordinator_supervision_outbox WHERE run_id = ?", "run").Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if outbox != 1 {
		t.Fatalf("outbox=%d want 1", outbox)
	}
	escalations, err := store.PendingSupervisionEscalations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(escalations) != 1 || escalations[0].IncidentID != "incident" || escalations[0].ThreadID != "thread" {
		t.Fatalf("escalations=%+v", escalations)
	}
	reloaded, err := store.LoadSupervisionAdminState(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	var gateState domain.IncidentState
	for _, facts := range reloaded.Incidents {
		if facts.Incident.ID == gate.ID {
			gateState = facts.Incident.State
		}
	}
	if gateState != domain.IncidentOpen {
		t.Fatalf("gate review state=%q want open", gateState)
	}
}

func TestRecoverySuccessResolvesOnlyItsEpisode(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 21, 17, 0, 0, 0, time.UTC)
	store.SetClock(func() time.Time { return now.Add(time.Minute) })
	config := domain.RecoveryConfig{Version: domain.RecoveryContractV1,
		Route: domain.ProviderRoute{ProviderInstanceID: "repairer", Model: "model"}, PromptArtifactID: "prompt",
		MaxAttemptsPerIncident: 2, IncidentDeadline: time.Hour, StalledAfter: time.Minute}
	prepareRecoveryFence(t, store, "run", "attempt-1", now, config)
	root := domain.RecoveryDiagnosticIdentity{FailureFingerprint: "root", EvidenceFingerprint: "evidence", StrategyFingerprint: "strategy"}
	recovery := domain.NewRecoveryIncident(config, root, now)
	if _, _, err := store.OpenRecoveryIncident(ctx, RecoveryIncidentRequest{
		RunID: "run", IncidentID: "incident", EventID: "event", SourceTaskID: "task", SourceAttemptID: "attempt-1",
		ExpectedGraphRevision: 1, SourceAttemptRevision: 1, RecoveryConfig: config, Reason: "failed",
		Recovery: *recovery, EventRecord: []byte(`{"id":"event"}`), OpenedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	success := domain.Attempt{ID: "attempt-2", WorkflowRunID: "run", TaskID: "task", Number: 2,
		Progress: domain.ProgressSucceeded, Control: domain.ControlStopped, Revision: 1, UpdatedAt: now.Add(time.Minute)}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{success}}); err != nil {
		t.Fatal(err)
	}
	state, _ := store.LoadSupervisionAdminState(ctx, "run")
	incident := state.Incidents[0].Incident
	incident.Recovery.AttemptsUsed = 1
	incident.Recovery.CurrentAttemptID = success.ID
	incident.Recovery.State = domain.RecoveryRecovering
	raw, _ := json.Marshal(incident)
	if _, err := store.db.ExecContext(ctx, "UPDATE coordinator_supervision_incidents SET record = ? WHERE id = ?", raw, incident.ID); err != nil {
		t.Fatal(err)
	}
	got, err := store.ResolveRecoveryEpisode(ctx, RecoveryAttemptSuccessRequest{
		RunID: "run", IncidentID: "incident", AttemptID: success.ID,
		ExpectedIncidentRevision: 1, ExpectedGraphRevision: 1, AttemptRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.IncidentResolved || got.Recovery.State != domain.RecoveryResolved ||
		got.Recovery.NextAction != domain.RecoveryNoAction || got.Recovery.RootDiagnostic != root {
		t.Fatalf("resolved incident=%+v", got)
	}
}
