package sqlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
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
		// Schema 30 (the owner-notification outbox) is applied on the same
		// forward path and leaves the usage history alone.
		if got := schemaVersionOf(t, store); got != 30 {
			t.Fatalf("schema version = %d, want 30", got)
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
	// Dispatch happens before the samples, as it does on a fleet: a run's
	// window starts at its first binding.
	store.SetClock(func() time.Time { return now })
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

// A run's unattributed count is the unbound samples that could be its own --
// any provider on a worker it ran on, inside its window -- and the fleet's
// other unbound samples in the window are reported apart.
// A local row the coordinator host's own worker has already forwarded under
// its worker id is one reading, not two, and is counted once.
func TestAttributedUsageSeparatesTheRunsAmbiguityFromTheFleets(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	completed := now.Add(time.Hour)
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-window", Progress: domain.ProgressSucceeded, CompletedAt: &completed}},
		Attempts:     []domain.Attempt{{ID: "attempt-window", WorkflowRunID: "run-window", TaskID: "task-window", Number: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	store.SetClock(func() time.Time { return now })
	if _, err := store.PrepareAssignmentDispatch(ctx, measuredAssignment("assignment-window", "attempt-window", "thread-run", "claude", 1, now)); err != nil {
		t.Fatal(err)
	}
	record := func(worker, provider, thread, event string, at time.Time) {
		t.Helper()
		if err := store.RecordUsage(ctx, domain.UsageSample{
			WorkerID: worker, ProviderInstanceID: provider, ThreadID: thread, Model: "model",
			ObservedAt: at, SourceEventID: event, Kind: domain.UsageKindTurn, OutputTokens: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// The run's own reading, stored twice on the coordinator's host: the
	// local row and the copy forwarded under the worker's id.
	record("worker", "claude", "thread-run", "run-turn", now.Add(time.Minute))
	record("", "claude", "thread-run", "run-turn", now.Add(time.Minute))
	// Could be the run's: its worker and provider, inside its window, on a
	// thread nothing bound. One was forwarded, one is a local row not yet
	// forwarded, whose worker is unknown.
	record("worker", "claude", "thread-renamed", "ambiguous-forwarded", now.Add(2*time.Minute))
	record("", "claude", "thread-local", "ambiguous-local", now.Add(3*time.Minute))
	// Could be the run's too: a provider log that names the instance
	// differently from the route the run was bound under.
	record("worker", "claude-main", "thread-run", "provider-renamed", now.Add(5*time.Minute))
	// Cannot be the run's: another worker.
	record("other-worker", "claude", "thread-elsewhere", "other-worker", now.Add(4*time.Minute))
	// Outside the window entirely: long before dispatch, long after completion.
	record("worker", "claude", "thread-before", "before", now.Add(-time.Hour))
	record("worker", "claude", "thread-after", "after", completed.Add(time.Hour))

	report, err := store.AttributedUsage(ctx, "run-window")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Samples) != 1 || report.Samples[0].SourceEventID != "run-turn" {
		t.Fatalf("samples = %#v", report.Samples)
	}
	if got := report.Coverage.RunWindowUnattributedCount; got != 3 {
		t.Fatalf("run-window unattributed = %d, want 3 (the renamed thread, the renamed provider and the unforwarded local row)", got)
	}
	if got := report.Coverage.UnscopedUnattributedCount; got != 4 {
		t.Fatalf("fleet unscoped in the window = %d, want 4 (the forwarded run turn's local copy is not a second reading)", got)
	}

	normalized := domain.NormalizeUsageReport(report, domain.UsageNormalizationContext{Now: completed, RunProgress: domain.ProgressSucceeded, RunCompletedAt: &completed})
	if normalized.Coverage.State != domain.UsageCoveragePartial || normalized.Coverage.UnattributedCount != 3 {
		t.Fatalf("coverage = %#v", normalized.Coverage)
	}

	// Without anything that could be the run's, the fleet's unbound samples
	// leave the run's coverage complete.
	if _, err := store.db.ExecContext(ctx, `DELETE FROM usage_samples WHERE event_id IN ('ambiguous-forwarded', 'ambiguous-local', 'provider-renamed')`); err != nil {
		t.Fatal(err)
	}
	if report, err = store.AttributedUsage(ctx, "run-window"); err != nil {
		t.Fatal(err)
	}
	normalized = domain.NormalizeUsageReport(report, domain.UsageNormalizationContext{Now: completed, RunProgress: domain.ProgressSucceeded, RunCompletedAt: &completed})
	if normalized.Coverage.State != domain.UsageCoverageComplete || normalized.Coverage.UnattributedCount != 0 ||
		normalized.Coverage.UnscopedUnattributedCount != 1 {
		t.Fatalf("fully attributed run: coverage = %#v", normalized.Coverage)
	}
}

// An assignment dispatched without a thread writes no usage binding, so every
// sample of its session is unbound. The run was still dispatched to that
// worker, and those samples keep its coverage partial rather than letting a
// run with no bindings at all read as having nothing unattributed.
func TestAttributedUsageCountsSamplesOfAnAssignmentWithoutABinding(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	// The assignment is recorded as it stands before a thread exists, which
	// is the state bindAssignmentUsageTx writes no binding for.
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-threadless"}},
		Attempts:     []domain.Attempt{{ID: "attempt-threadless", WorkflowRunID: "run-threadless", TaskID: "task", Number: 1}},
		Assignments:  []domain.Assignment{measuredAssignment("assignment-threadless", "attempt-threadless", "", "claude", 1, now)},
	}); err != nil {
		t.Fatal(err)
	}
	var bindings int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM coordinator_usage_bindings`).Scan(&bindings); err != nil || bindings != 0 {
		t.Fatalf("bindings = %d (%v); the case needs an assignment without one", bindings, err)
	}
	for i, worker := range []string{"worker", "other-worker"} {
		if err := store.RecordUsage(ctx, domain.UsageSample{
			WorkerID: worker, ProviderInstanceID: "claude", ThreadID: "thread-" + worker, Model: "model",
			ObservedAt: now.Add(time.Duration(i+1) * time.Minute), SourceEventID: "event-" + worker,
			Kind: domain.UsageKindTurn, OutputTokens: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	report, err := store.AttributedUsage(ctx, "run-threadless")
	if err != nil {
		t.Fatal(err)
	}
	if report.Coverage.RunWindowUnattributedCount != 1 || report.Coverage.UnscopedUnattributedCount != 2 {
		t.Fatalf("coverage = %#v", report.Coverage)
	}
}

func TestCoordinatorUsageCursorKeyPersistsAndRejectsCorruption(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "coordinator.db")
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.CoordinatorUsageCursorKey(ctx)
	if err != nil || len(first) != 32 {
		t.Fatalf("first key length=%d err=%v", len(first), err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	second, err := store.CoordinatorUsageCursorKey(ctx)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("restart key changed: equal=%v err=%v", bytes.Equal(first, second), err)
	}
	encoded, ok, err := store.GetKV(ctx, coordinatorUsageCursorKeyName)
	if err != nil || !ok {
		t.Fatalf("stored key missing: ok=%v err=%v", ok, err)
	}
	if err := store.SetKV(ctx, coordinatorUsageCursorKeyName, "corrupt"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CoordinatorUsageCursorKey(ctx); err == nil {
		t.Fatal("corrupt coordinator cursor key was accepted")
	}
	if err := store.SetKV(ctx, coordinatorUsageCursorKeyName, encoded); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `DELETE FROM kv WHERE key = ?`, coordinatorUsageCursorKeyName); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CoordinatorUsageCursorKey(ctx); err == nil {
		t.Fatal("missing initialized coordinator cursor key was regenerated")
	}
}

func TestRecordUsageRollsBackDiagnosticOverflowAtEveryCheckpoint(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "worker.db")
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	for i := 0; i < 1001; i++ {
		if err := store.RecordUsage(ctx, domain.UsageSample{
			WorkerID: "worker-atomic", ProviderInstanceID: "provider", ThreadID: "thread",
			ObservedAt: now.Add(time.Duration(i) * time.Second), SourceEventID: fmt.Sprintf("baseline-%04d", i),
			Kind: domain.UsageKindDiagnostic, DiagnosticCode: "malformed",
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.WorkerUsageBatch(ctx, []string{"diagnostic-overflow@1"}, MaxWorkerUsageDelivery); err != nil {
		t.Fatal(err)
	}
	assertOriginal := func() {
		t.Helper()
		var ordinary, dropped, marker, forwarded, candidate int64
		if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_samples
			WHERE worker_id = 'worker-atomic' AND diagnostic_code <> 'overflow'`).Scan(&ordinary); err != nil {
			t.Fatal(err)
		}
		if err := store.db.QueryRowContext(ctx, `SELECT dropped_count FROM usage_diagnostic_overflow
			WHERE worker_id = 'worker-atomic'`).Scan(&dropped); err != nil {
			t.Fatal(err)
		}
		if err := store.db.QueryRowContext(ctx, `SELECT cumulative_tokens FROM usage_samples
			WHERE worker_id = 'worker-atomic' AND diagnostic_code = 'overflow'`).Scan(&marker); err != nil {
			t.Fatal(err)
		}
		if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM worker_usage_forwarded
			WHERE worker_id = 'worker-atomic' AND event_id = 'diagnostic-overflow' AND revision = 1`).Scan(&forwarded); err != nil {
			t.Fatal(err)
		}
		if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_samples
			WHERE worker_id = 'worker-atomic' AND event_id = 'candidate'`).Scan(&candidate); err != nil {
			t.Fatal(err)
		}
		if ordinary != 1000 || dropped != 1 || marker != 1 || forwarded != 1 || candidate != 0 {
			t.Fatalf("rollback state ordinary=%d dropped=%d marker=%d forwarded=%d candidate=%d",
				ordinary, dropped, marker, forwarded, candidate)
		}
	}
	candidate := domain.UsageSample{
		WorkerID: "worker-atomic", ProviderInstanceID: "provider", ThreadID: "thread",
		ObservedAt: now.Add(2 * time.Hour), SourceEventID: "candidate",
		Kind: domain.UsageKindDiagnostic, DiagnosticCode: "malformed",
	}
	for _, stage := range []string{
		"sample-inserted", "diagnostics-pruned", "overflow-count-updated",
		"forwarded-marker-cleared", "overflow-marker-cleared", "overflow-marker-written",
	} {
		store.usageRecordHook = func(got string) error {
			if got == stage {
				return errors.New("injected interruption")
			}
			return nil
		}
		if err := store.RecordUsage(ctx, candidate); err == nil {
			t.Fatalf("checkpoint %q did not interrupt", stage)
		}
		store.usageRecordHook = nil
		assertOriginal()
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.RecordUsage(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	var dropped, marker, forwarded int64
	if err := store.db.QueryRowContext(ctx, `SELECT dropped_count FROM usage_diagnostic_overflow
		WHERE worker_id = 'worker-atomic'`).Scan(&dropped); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT cumulative_tokens FROM usage_samples
		WHERE worker_id = 'worker-atomic' AND diagnostic_code = 'overflow'`).Scan(&marker); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM worker_usage_forwarded
		WHERE worker_id = 'worker-atomic' AND event_id = 'diagnostic-overflow'`).Scan(&forwarded); err != nil {
		t.Fatal(err)
	}
	if dropped != 2 || marker != 2 || forwarded != 0 {
		t.Fatalf("retry state dropped=%d marker=%d forwarded=%d", dropped, marker, forwarded)
	}
}

// S8: on the coordinator's host the worker shares the coordinator's database,
// which also holds the samples other workers delivered. The worker forwards
// only its own host's readings, never another worker's as its own.
func TestWorkerUsageBatchForwardsOnlyThisHostsReadings(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 25, 8, 0, 0, 0, time.UTC)
	if err := store.RecordUsage(ctx, domain.UsageSample{
		ProviderInstanceID: "claudeAgent", ThreadID: "thread-local", Model: "claude-haiku-4-5",
		ObservedAt: now, SourceEventID: "local-1", Kind: domain.UsageKindCall, InputTokens: 10,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReceiveWorkerUsage(ctx, "homelab", []domain.UsageSample{{
		ProviderInstanceID: "claudeAgent", ThreadID: "thread-remote", Model: "claude-haiku-4-5",
		ObservedAt: now, SourceEventID: "remote-1", Kind: domain.UsageKindCall, InputTokens: 20,
	}}); err != nil {
		t.Fatal(err)
	}
	batch, err := store.WorkerUsageBatch(ctx, nil, MaxWorkerUsageDelivery)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 || batch[0].SourceEventID != "local-1" {
		t.Fatalf("forwarded %#v, want only this host's reading", batch)
	}
}

func TestUsageDiagnosticsAreBoundedWithOverflowEvidence(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	for i := 0; i < 1001; i++ {
		if err := store.RecordUsage(ctx, domain.UsageSample{
			// The worker's own watchdog records its host's samples unowned,
			// exactly as production does; only those are forwarded.
			WorkerID: "", ProviderInstanceID: "provider", ThreadID: "thread",
			ObservedAt: now.Add(time.Duration(i) * time.Second), SourceEventID: fmt.Sprintf("diagnostic-%04d", i),
			Kind: domain.UsageKindDiagnostic, DiagnosticCode: "malformed",
		}); err != nil {
			t.Fatal(err)
		}
	}
	samples, err := store.UsageSamples(ctx, now.Add(-time.Second), now.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 1001 {
		t.Fatalf("retained diagnostics = %d", len(samples))
	}
	var ordinary, overflow int
	for _, sample := range samples {
		if sample.DiagnosticCode == "overflow" {
			overflow++
			if sample.CumulativeTokens != 1 {
				t.Fatalf("overflow marker = %#v", sample)
			}
		} else {
			ordinary++
		}
	}
	if ordinary != 1000 || overflow != 1 {
		t.Fatalf("ordinary=%d overflow=%d", ordinary, overflow)
	}
	report, err := store.AttributedUsage(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if report.Coverage.DiagnosticDroppedCount != 1 {
		t.Fatalf("overflow coverage = %#v", report.Coverage)
	}

	coordinator, err := OpenMigrated(filepath.Join(t.TempDir(), "coordinator.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()
	var acknowledgements []string
	for {
		batch, batchErr := store.WorkerUsageBatch(ctx, acknowledgements, MaxWorkerUsageDelivery)
		if batchErr != nil {
			t.Fatal(batchErr)
		}
		if err := coordinator.ClearWorkerUsageAcknowledgements(ctx, "worker-diagnostic", acknowledgements); err != nil {
			t.Fatal(err)
		}
		if len(batch) == 0 {
			break
		}
		if _, err := coordinator.ReceiveWorkerUsage(ctx, "worker-diagnostic", batch); err != nil {
			t.Fatal(err)
		}
		acknowledgements, err = coordinator.WorkerUsageAcknowledgements(ctx, "worker-diagnostic")
		if err != nil {
			t.Fatal(err)
		}
	}
	delivered, err := coordinator.AttributedUsage(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if delivered.Coverage.DiagnosticDroppedCount != 1 {
		t.Fatalf("delivered overflow coverage = %#v", delivered.Coverage)
	}

	// Advance the same overflow state through another cycle. A late acknowledgement
	// for revision 1 must not suppress revision 6, and neither database may retain
	// more than one overflow marker for this worker.
	for i := 1001; i < 1006; i++ {
		if err := store.RecordUsage(ctx, domain.UsageSample{
			// The worker's own watchdog records its host's samples unowned,
			// exactly as production does; only those are forwarded.
			WorkerID: "", ProviderInstanceID: "provider", ThreadID: "thread",
			ObservedAt: now.Add(time.Duration(i) * time.Second), SourceEventID: fmt.Sprintf("diagnostic-%04d", i),
			Kind: domain.UsageKindDiagnostic, DiagnosticCode: "malformed",
		}); err != nil {
			t.Fatal(err)
		}
	}
	batch, err := store.WorkerUsageBatch(ctx, []string{"diagnostic-overflow@1"}, MaxWorkerUsageDelivery)
	if err != nil {
		t.Fatal(err)
	}
	var advanced int
	for _, sample := range batch {
		if sample.DiagnosticCode == "overflow" {
			advanced++
			if sample.SourceEventID != "diagnostic-overflow" || sample.CumulativeTokens != 6 {
				t.Fatalf("advanced overflow = %#v", sample)
			}
		}
	}
	if advanced != 1 {
		t.Fatalf("advanced overflow markers = %d in %#v", advanced, batch)
	}
	if _, err := coordinator.ReceiveWorkerUsage(ctx, "worker-diagnostic", batch); err != nil {
		t.Fatal(err)
	}
	currentAcks, err := coordinator.WorkerUsageAcknowledgements(ctx, "worker-diagnostic")
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ClearWorkerUsageAcknowledgements(ctx, "worker-diagnostic", []string{"diagnostic-overflow@1"}); err != nil {
		t.Fatal(err)
	}
	afterStale, err := coordinator.WorkerUsageAcknowledgements(ctx, "worker-diagnostic")
	if err != nil {
		t.Fatal(err)
	}
	var retainedCurrent bool
	for _, acknowledgement := range afterStale {
		retainedCurrent = retainedCurrent || acknowledgement == "diagnostic-overflow@6"
	}
	if !retainedCurrent {
		t.Fatalf("stale cleanup removed current receipt: %#v", afterStale)
	}
	if _, err := store.WorkerUsageBatch(ctx, currentAcks, MaxWorkerUsageDelivery); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ClearWorkerUsageAcknowledgements(ctx, "worker-diagnostic", currentAcks); err != nil {
		t.Fatal(err)
	}
	remainingAcks, err := coordinator.WorkerUsageAcknowledgements(ctx, "worker-diagnostic")
	if err != nil || len(remainingAcks) != 0 {
		t.Fatalf("current cleanup left receipts=%#v err=%v", remainingAcks, err)
	}
	// The worker holds its marker unowned; the coordinator holds the delivered
	// copy under the worker that delivered it.
	for name, candidate := range map[string]struct {
		store *Store
		owner string
	}{"worker": {store, ""}, "coordinator": {coordinator, "worker-diagnostic"}} {
		var markers int
		if err := candidate.store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_samples WHERE worker_id = ? AND diagnostic_code = 'overflow'`, candidate.owner).Scan(&markers); err != nil {
			t.Fatal(err)
		}
		if markers != 1 {
			t.Fatalf("%s overflow rows = %d", name, markers)
		}
	}
	delivered, err = coordinator.AttributedUsage(ctx, "")
	if err != nil || delivered.Coverage.DiagnosticDroppedCount != 6 {
		t.Fatalf("advanced delivered overflow = %#v, %v", delivered.Coverage, err)
	}
}

type pruneHistoryCounts struct {
	observations int
	usage        int
	forwarded    int
	receipts     int
	overflow     int
}

func TestPruneHistoryIsAtomicAndForgetsExactDeliveryIdentity(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	stages := []string{"observations-pruned", "receipts-pruned", "forwarded-pruned", "usage-pruned", "overflow-pruned"}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			store, err := OpenMigrated(path)
			if err != nil {
				t.Fatal(err)
			}
			seedPruneHistoryFixture(t, store, now)
			before := loadPruneHistoryCounts(t, store)
			store.pruneHistoryHook = func(got string) error {
				if got == stage {
					return errors.New("injected prune interruption")
				}
				return nil
			}
			if err := store.PruneHistory(ctx, now); err == nil {
				t.Fatalf("checkpoint %q did not interrupt", stage)
			}
			if after := loadPruneHistoryCounts(t, store); after != before {
				t.Fatalf("partial prune at %q: got %#v want %#v", stage, after, before)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = OpenMigrated(path)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			store.pruneHistoryHook = nil
			if reopened := loadPruneHistoryCounts(t, store); reopened != before {
				t.Fatalf("reopen after rollback = %#v, want %#v", reopened, before)
			}
			if err := store.PruneHistory(ctx, now); err != nil {
				t.Fatal(err)
			}
			got := loadPruneHistoryCounts(t, store)
			if got.observations != 1 || got.usage != 1 || got.forwarded != 0 || got.receipts != 0 || got.overflow != 0 {
				t.Fatalf("pruned bookkeeping = %#v", got)
			}
			acks, err := store.WorkerUsageAcknowledgements(ctx, "offline-worker")
			if err != nil || len(acks) != 0 {
				t.Fatalf("stale offline receipt survived: %#v, %v", acks, err)
			}
			reused := domain.UsageSample{
				ProviderInstanceID: "provider", ThreadID: "thread-new", Model: "model",
				ObservedAt: now.Add(time.Hour), SourceEventID: "old-event",
				Kind: domain.UsageKindCall, FieldPresence: domain.UsageFieldsAll, InputTokens: 7,
			}
			if _, err := store.ReceiveWorkerUsage(ctx, "offline-worker", []domain.UsageSample{reused}); err != nil {
				t.Fatal(err)
			}
			acks, err = store.WorkerUsageAcknowledgements(ctx, "offline-worker")
			if err != nil || len(acks) != 1 || acks[0] != "old-event" {
				t.Fatalf("reused event identity receipt = %#v, %v", acks, err)
			}
		})
	}
}

// One bad sample used to fail the whole delivered batch, which then stayed
// unacknowledged and was offered again unchanged on every boundary, so none of
// that worker's usage reached the coordinator again. A bad sample is now
// rejected and acknowledged on its own, while a storage error still fails the
// batch intact.
func TestReceiveWorkerUsageRejectsBadSamplesIndividually(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	sample := func(id string) domain.UsageSample {
		return domain.UsageSample{
			ProviderInstanceID: "provider", ThreadID: "thread", Model: "model", ObservedAt: now,
			SourceEventID: id, Kind: domain.UsageKindCall, FieldPresence: domain.UsageFieldsAll, InputTokens: 4,
		}
	}
	foreign := sample("foreign")
	foreign.WorkerID = "other-worker"
	notANumber := sample("nan-cost")
	notANumber.CostUSD, notANumber.CostReported = math.NaN(), true
	negative := sample("negative")
	negative.OutputTokens = -1
	infinite := sample("inf-cost")
	infinite.CostUSD, infinite.CostReported = math.Inf(1), true
	anonymous := sample("")
	batch := []domain.UsageSample{sample("good-1"), foreign, notANumber, negative, infinite, anonymous, sample("good-2")}

	receipt, err := store.ReceiveWorkerUsage(ctx, "worker-a", batch)
	if err != nil {
		t.Fatalf("a batch with bad samples failed as a whole: %v", err)
	}
	var rejected []string
	for _, rejection := range receipt.Rejected {
		if rejection.ProviderInstanceID != "provider" || rejection.ThreadID != "thread" ||
			rejection.Model != "model" || rejection.Reason == "" {
			t.Fatalf("rejection lost the sample's identity: %#v", rejection)
		}
		rejected = append(rejected, rejection.EventID)
	}
	if receipt.Stored != 2 || !slices.Equal(rejected, []string{"foreign", "nan-cost", "negative", "inf-cost", ""}) {
		t.Fatalf("receipt = %#v", receipt)
	}
	acks, err := store.WorkerUsageAcknowledgements(ctx, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(acks)
	// Every sample with an identity is settled; one without has nothing to
	// acknowledge.
	if !slices.Equal(acks, []string{"foreign", "good-1", "good-2", "inf-cost", "nan-cost", "negative"}) {
		t.Fatalf("acknowledgements = %v; every settled sample must be acknowledged", acks)
	}
	var stored int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_samples WHERE worker_id = 'worker-a'`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != 2 {
		t.Fatalf("stored samples = %d, want only the two valid ones", stored)
	}
	// Offered again before the worker saw the acknowledgement, the same bad
	// samples are not reported a second time. Only the one without an
	// identity is, since nothing about it could be recorded; a current worker
	// never offers it (TestWorkerUsageBatchNeverOffersASampleWithoutAnEventID).
	if receipt, err = store.ReceiveWorkerUsage(ctx, "worker-a", batch); err != nil ||
		len(receipt.Rejected) != 1 || receipt.Rejected[0].EventID != "" {
		t.Fatalf("replayed batch receipt = %#v, %v", receipt, err)
	}

	// A storage error is not a bad sample: the batch fails and nothing of it
	// is committed, so it is retried whole.
	if _, err := store.db.ExecContext(ctx, `CREATE TRIGGER refuse_usage_storage BEFORE INSERT ON usage_samples
		WHEN NEW.event_id = 'storage-failure' BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReceiveWorkerUsage(ctx, "worker-b", []domain.UsageSample{sample("good-3"), sample("storage-failure")}); err == nil {
		t.Fatal("a storage error did not fail the batch")
	}
	if acks, err := store.WorkerUsageAcknowledgements(ctx, "worker-b"); err != nil || len(acks) != 0 {
		t.Fatalf("a failed batch left acknowledgements %v, %v", acks, err)
	}
}

// The coordinator cannot acknowledge a reading without an event id, so a
// worker that offered one would offer it on every exchange, where it would
// take a place in every batch.
func TestWorkerUsageBatchNeverOffersASampleWithoutAnEventID(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	for index, id := range []string{"", "identified"} {
		if err := store.RecordUsage(ctx, domain.UsageSample{
			ProviderInstanceID: "provider", ThreadID: "thread", Model: "model",
			ObservedAt: now.Add(time.Duration(index) * time.Second), SourceEventID: id,
			Kind: domain.UsageKindCall, FieldPresence: domain.UsageFieldsAll, InputTokens: 2,
		}); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		batch, err := store.WorkerUsageBatch(ctx, nil, 1)
		if err != nil || len(batch) != 1 || batch[0].SourceEventID != "identified" {
			t.Fatalf("worker usage batch = %#v, %v", batch, err)
		}
	}
}

func seedPruneHistoryFixture(t *testing.T, store *Store, now time.Time) {
	t.Helper()
	ctx := context.Background()
	old := now.Add(-time.Hour)
	if _, err := store.db.ExecContext(ctx, `INSERT INTO observations(
		bucket,observed_at,used_percent,resets_at,event_id,thread_id,model)
		VALUES(?,?,?,?,?,?,?),(?,?,?,?,?,?,?)`,
		"bucket", old.Format(time.RFC3339Nano), 1, "", "old-observation", "thread", "model",
		"bucket", now.Add(time.Hour).Format(time.RFC3339Nano), 2, "", "new-observation", "thread", "model"); err != nil {
		t.Fatal(err)
	}
	oldSample := domain.UsageSample{
		ProviderInstanceID: "provider", ThreadID: "thread-old", Model: "model",
		ObservedAt: old, SourceEventID: "old-event", Kind: domain.UsageKindCall,
		FieldPresence: domain.UsageFieldsAll, InputTokens: 3,
	}
	overflow := domain.UsageSample{
		ProviderInstanceID: "provider", ObservedAt: old.Add(time.Second),
		SourceEventID: "diagnostic-overflow", Kind: domain.UsageKindDiagnostic,
		DiagnosticCode: "overflow", CumulativeTokens: 9,
	}
	newSample := domain.UsageSample{
		ProviderInstanceID: "provider", ThreadID: "thread-new", Model: "model",
		ObservedAt: now.Add(time.Hour), SourceEventID: "new-event", Kind: domain.UsageKindCall,
		FieldPresence: domain.UsageFieldsAll, InputTokens: 5,
	}
	if _, err := store.ReceiveWorkerUsage(ctx, "offline-worker", []domain.UsageSample{oldSample, overflow}); err != nil {
		t.Fatal(err)
	}
	newSample.WorkerID = "offline-worker"
	if err := store.RecordUsage(ctx, newSample); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO worker_usage_forwarded(
		worker_id,event_id,acknowledged_at,revision) VALUES(?,?,?,?)`,
		"offline-worker", "old-event", now.Format(time.RFC3339Nano), 0); err != nil {
		t.Fatal(err)
	}
}

func loadPruneHistoryCounts(t *testing.T, store *Store) pruneHistoryCounts {
	t.Helper()
	var got pruneHistoryCounts
	for _, item := range []struct {
		table string
		value *int
	}{
		{"observations", &got.observations},
		{"usage_samples", &got.usage},
		{"worker_usage_forwarded", &got.forwarded},
		{"coordinator_worker_usage_receipts", &got.receipts},
		{"usage_diagnostic_overflow", &got.overflow},
	} {
		if err := store.db.QueryRow("SELECT COUNT(*) FROM " + item.table).Scan(item.value); err != nil {
			t.Fatal(err)
		}
	}
	return got
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
