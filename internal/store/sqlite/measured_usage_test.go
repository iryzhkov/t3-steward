package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestMeasuredUsageMigrationPreservesHistoryAndReopensIdempotently(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	legacy, err := open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.migrateThrough(23); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	if _, err := legacy.db.ExecContext(ctx, `INSERT INTO usage_samples(
		event_id,provider,thread_id,model,observed_at,input_tokens,cache_write_tokens,
		cache_read_tokens,output_tokens,cost_usd,kind,cumulative_tokens
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, "legacy-event", "codex-primary", "legacy-thread", "gpt",
		at.Format(time.RFC3339Nano), 8, 0, 0, 0, 0, domain.UsageKindCall, 0); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	for reopen := 0; reopen < 2; reopen++ {
		store, err := OpenMigrated(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := schemaVersionOf(t, store); got != 27 {
			t.Fatalf("schema version = %d, want 27", got)
		}
		samples, err := store.UsageSamples(ctx, at.Add(-time.Minute), at.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if len(samples) != 1 || samples[0].SourceEventID != "legacy-event" ||
			samples[0].Attribution.Status != domain.UsageUnattributed {
			t.Fatalf("migrated samples = %#v", samples)
		}
		if err := store.Migrate(); err != nil {
			t.Fatalf("repeat migration: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMeasuredUsageBindingIsAuthoritativeIsolatedAndReplaySafe(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	records := CoordinatorRecords{
		Workflows: []domain.Workflow{
			{ID: "workflow-a", Name: "identical title", TaskIDs: []string{"task-a"}},
			{ID: "workflow-b", Name: "identical title", TaskIDs: []string{"task-b"}},
		},
		WorkflowRuns: []domain.WorkflowRun{
			{ID: "run-a", WorkflowID: "workflow-a"},
			{ID: "run-b", WorkflowID: "workflow-b"},
		},
		Tasks: []domain.Task{
			{ID: "task-a", WorkflowID: "workflow-a", Name: "same task", PromptArtifactID: "prompt-containing-run-b"},
			{ID: "task-b", WorkflowID: "workflow-b", Name: "same task", PromptArtifactID: "prompt-containing-run-a"},
		},
		Attempts: []domain.Attempt{
			{ID: "attempt-a1", WorkflowRunID: "run-a", TaskID: "task-a", Number: 1},
			{ID: "attempt-a2", WorkflowRunID: "run-a", TaskID: "task-a", Number: 2},
			{ID: "attempt-b1", WorkflowRunID: "run-b", TaskID: "task-b", Number: 1},
			{ID: "activation-a", WorkflowRunID: "run-a", TaskID: "task-a", Number: 3,
				SupervisionActivationID: "activation-7", SupervisionActivationEpoch: 2},
			{ID: "activation-gate", WorkflowRunID: "run-a", TaskID: "task-a", Number: 4,
				SupervisionActivationID: "activation-gate-7", SupervisionActivationEpoch: 3},
		},
	}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := saveSupervisionActivationTx(ctx, tx, domain.Activation{
		ID: "activation-7", RunID: "run-a", Epoch: 2,
		State: domain.ActivationPendingDispatch, DispatchIdentity: "dispatch-activation-7",
	}); err != nil {
		t.Fatal(err)
	}
	if err := saveSupervisionIncidentTx(ctx, tx, domain.ReviewIncident{
		ID: "incident-gate", RunID: "run-a", GateID: "gate-7", State: domain.IncidentOpen, Revision: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := saveSupervisionActivationTx(ctx, tx, domain.Activation{
		ID: "activation-gate-7", RunID: "run-a", Epoch: 3, IncidentID: "incident-gate",
		State: domain.ActivationPendingDispatch, DispatchIdentity: "dispatch-activation-gate-7",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO coordinator_recovery_supplements(
		operation_id,run_id,incident_id,attempt_id,record) VALUES(?,?,?,?,?)`,
		"repair-operation", "run-a", "repair-incident", "attempt-a2", "{}"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	assignments := []domain.Assignment{
		measuredAssignment("assignment-a1", "attempt-a1", "thread-shared-looking-a", "codex-primary", 1, now),
		measuredAssignment("assignment-a2", "attempt-a2", "thread-repair", "claude-agent", 2, now),
		measuredAssignment("assignment-b1", "attempt-b1", "thread-shared-looking-b", "codex-primary", 1, now),
		measuredAssignment("assignment-activation", "activation-a", "thread-activation", "claude-agent", 1, now),
		measuredAssignment("assignment-gate", "activation-gate", "thread-gate", "claude-agent", 3, now),
	}
	for _, assignment := range assignments {
		if _, err := store.PrepareAssignmentDispatch(ctx, assignment); err != nil {
			t.Fatalf("prepare %s: %v", assignment.ID, err)
		}
	}
	samples := []domain.UsageSample{
		// Coordinator ingestion stamps this authenticated worker provenance.
		{WorkerID: "worker", ProviderInstanceID: "codex-primary", ThreadID: "thread-shared-looking-a", ObservedAt: now, SourceEventID: "codex-a", Kind: domain.UsageKindCall, InputTokens: 10},
		{WorkerID: "worker", ProviderInstanceID: "claude-agent", ThreadID: "thread-repair", ObservedAt: now.Add(time.Second), SourceEventID: "claude-repair#model", Kind: domain.UsageKindTurn, OutputTokens: 4},
		{WorkerID: "worker", ProviderInstanceID: "codex-primary", ThreadID: "thread-shared-looking-b", ObservedAt: now.Add(2 * time.Second), SourceEventID: "codex-b", Kind: domain.UsageKindCall, InputTokens: 12},
		{WorkerID: "worker", ProviderInstanceID: "claude-agent", ThreadID: "thread-activation", ObservedAt: now.Add(3 * time.Second), SourceEventID: "claude-activation#model", Kind: domain.UsageKindTurn, OutputTokens: 6},
		{WorkerID: "worker", ProviderInstanceID: "claude-agent", ThreadID: "thread-gate", ObservedAt: now.Add(4 * time.Second), SourceEventID: "claude-gate#model", Kind: domain.UsageKindTurn, OutputTokens: 3},
		{WorkerID: "worker", ProviderInstanceID: "codex-primary", ThreadID: "run-a/task-a/prompt-looking-but-unknown", ObservedAt: now.Add(5 * time.Second), SourceEventID: "unknown", Kind: domain.UsageKindCall, InputTokens: 99},
	}
	for _, sample := range samples {
		if err := store.RecordUsage(ctx, sample); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordUsage(ctx, sample); err != nil {
			t.Fatalf("replay %s: %v", sample.SourceEventID, err)
		}
	}
	got, err := store.UsageSamples(ctx, now.Add(-time.Second), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(samples) {
		t.Fatalf("samples after replay = %d, want %d", len(got), len(samples))
	}
	byEvent := make(map[string]domain.UsageSample, len(got))
	for _, sample := range got {
		byEvent[sample.SourceEventID] = sample
	}
	assertUsageBinding(t, byEvent["codex-a"], "run-a", "task-a", "attempt-a1", "assignment-a1", domain.ExecutionRoleExecutor)
	assertUsageBinding(t, byEvent["claude-repair#model"], "run-a", "task-a", "attempt-a2", "assignment-a2", domain.ExecutionRoleRepairExecutor)
	assertUsageBinding(t, byEvent["codex-b"], "run-b", "task-b", "attempt-b1", "assignment-b1", domain.ExecutionRoleExecutor)
	assertUsageBinding(t, byEvent["claude-activation#model"], "run-a", "task-a", "activation-a", "assignment-activation", domain.ExecutionRoleSupervisorActivation)
	assertUsageBinding(t, byEvent["claude-gate#model"], "run-a", "task-a", "activation-gate", "assignment-gate", domain.ExecutionRoleGateReviewer)
	if byEvent["claude-gate#model"].Attribution.GateID != "gate-7" {
		t.Fatalf("gate attribution = %#v", byEvent["claude-gate#model"].Attribution)
	}
	if byEvent["claude-activation#model"].Attribution.ActivationID != "activation-7" {
		t.Fatalf("activation attribution = %#v", byEvent["claude-activation#model"].Attribution)
	}
	if unknown := byEvent["unknown"]; unknown.Attribution.Status != domain.UsageUnattributed ||
		unknown.Attribution.WorkflowRunID != "" {
		t.Fatalf("unknown sample inferred from identifiers: %#v", unknown)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runA, err := store.AttributedUsage(ctx, "run-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(runA.Samples) != 4 || runA.Coverage.UnscopedUnattributedCount != 1 || runA.Coverage.Reason == "" {
		t.Fatalf("run-a usage = %#v", runA)
	}
	for _, sample := range runA.Samples {
		if sample.Attribution.WorkflowRunID != "run-a" {
			t.Fatalf("cross-run leakage: %#v", sample)
		}
	}
}

func TestAttributedUsageAppliesDeterministicSafetyBound(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-bound"}},
		Attempts:     []domain.Attempt{{ID: "attempt-bound", WorkflowRunID: "run-bound", TaskID: "task-bound", Number: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PrepareAssignmentDispatch(ctx, measuredAssignment("assignment-bound", "attempt-bound", "thread-bound", "codex", 1, now)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= MaxRunUsageAggregation; i++ {
		if err := store.RecordUsage(ctx, domain.UsageSample{
			WorkerID: "worker", ProviderInstanceID: "codex", ThreadID: "thread-bound", Model: "gpt",
			ObservedAt: now.Add(time.Duration(i) * time.Second), SourceEventID: fmt.Sprintf("event-%05d", i),
			Kind: domain.UsageKindCall, InputTokens: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	report, err := store.AttributedUsage(ctx, "run-bound")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Samples) != MaxRunUsageAggregation || !report.Coverage.Truncated ||
		report.Samples[0].SourceEventID != "event-00000" || report.Samples[len(report.Samples)-1].SourceEventID != "event-09999" {
		t.Fatalf("bounded report: samples=%d coverage=%#v first=%q last=%q", len(report.Samples), report.Coverage,
			report.Samples[0].SourceEventID, report.Samples[len(report.Samples)-1].SourceEventID)
	}
}

func measuredAssignment(id, attempt, thread, provider string, epoch int64, now time.Time) domain.Assignment {
	return domain.Assignment{
		ID: id, AttemptID: attempt, WorkerID: "worker", WorkerEpoch: "worker-1",
		Route: domain.ProviderRoute{WorkerID: "worker", ProviderInstanceID: provider, Model: "model"},
		State: domain.AssignmentClaimed, Epoch: epoch, LeaseToken: "lease-" + id,
		LeaseExpiresAt: now.Add(time.Hour), DispatchToken: "dispatch-" + id, ThreadID: thread,
		CreatedAt: now, UpdatedAt: now,
	}
}

func assertUsageBinding(t *testing.T, sample domain.UsageSample, run, task, attempt, assignment string, role domain.ExecutionRole) {
	t.Helper()
	got := sample.Attribution
	if got.Status != domain.UsageAttributed || got.WorkflowRunID != run || got.TaskID != task ||
		got.AttemptID != attempt || got.AssignmentID != assignment || got.Role != role {
		t.Fatalf("attribution = %#v", got)
	}
}
