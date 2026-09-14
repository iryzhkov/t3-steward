package backlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var scheduledFixtureTime = time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)

// scheduledWorkflowRecords is a two-task workflow with the prompt and input
// artifacts a real submission leaves behind. The schedule tests need it because
// a workflow with no tasks cannot tell a run that was seeded from one that was
// not: both end up with nothing to execute.
func scheduledWorkflowRecords(workflowID, submittedRunID string) sqlite.CoordinatorRecords {
	now := scheduledFixtureTime
	artifact := func(name, content, taskID string) domain.Artifact {
		sum := sha256.Sum256([]byte(content))
		digest := hex.EncodeToString(sum[:])
		return domain.Artifact{
			ID: "artifact-" + name, WorkflowRunID: submittedRunID, TaskID: taskID,
			Kind: domain.ArtifactInput, Name: name, MediaType: "text/markdown",
			Size: int64(len(content)), SHA256: digest,
			StoragePath: path.Join("objects", digest[:2], digest),
			Producer:    "submission", CreatedAt: now,
		}
	}
	manifest := artifact("workflow.yaml", "version: 2\n", "")
	inspectPrompt := artifact("prompts/inspect.md", "inspect", "task-inspect")
	implementPrompt := artifact("prompts/implement.md", "implement", "task-implement")
	routes := []domain.ProviderRoute{{
		WorkerID: "normandy", ProviderInstanceID: "codex",
		Model: "gpt-5.6-sol", QuotaPoolID: "openai",
	}}
	task := func(id, name, promptID string, needs []string) domain.Task {
		return domain.Task{
			ID: id, WorkflowID: workflowID, Name: name, Class: domain.TaskClassRequired,
			Needs: needs, PromptArtifactID: promptID,
			InputArtifactIDs: []string{manifest.ID},
			Verification:     []string{"true"}, Routes: routes,
			Importance: 3, Difficulty: 3, MaxTurns: 3,
		}
	}
	return sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: workflowID, Version: ManifestVersion, Name: "scheduled",
			Class: domain.TaskClassRequired, CreatedAt: now,
			TaskIDs:          []string{"task-implement", "task-inspect"},
			InputArtifactIDs: []string{manifest.ID},
		}},
		Tasks: []domain.Task{
			task("task-inspect", "inspect", inspectPrompt.ID, nil),
			task("task-implement", "implement", implementPrompt.ID, []string{"inspect"}),
		},
		Artifacts: []domain.Artifact{manifest, inspectPrompt, implementPrompt},
	}
}

func scheduledFixtureStore(t *testing.T, records sqlite.CoordinatorRecords) *sqlite.Store {
	t.Helper()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatalf("seed schedule fixture: %v", err)
	}
	return store
}

func scheduledFixtureSchedule(enabled bool, failure domain.ScheduleFailurePolicy) (domain.Schedule, domain.ScheduleTemplate) {
	now := scheduledFixtureTime
	schedule := domain.Schedule{
		ID: "schedule-1", Name: "nightly", Version: 1, WorkflowID: "workflow-1",
		Expression: "0 2 * * *", Timezone: "UTC", Overlap: domain.ScheduleOverlapForbid,
		Misfire: domain.ScheduleMisfireSkip, AfterFailure: failure,
		Enabled: enabled, Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	template := domain.ScheduleTemplate{
		ScheduleID: schedule.ID, Version: 1, WorkflowID: schedule.WorkflowID,
		Expression: schedule.Expression, Timezone: schedule.Timezone,
		Overlap: schedule.Overlap, Misfire: schedule.Misfire,
		AfterFailure: schedule.AfterFailure, CreatedAt: now,
	}
	return schedule, template
}

// TestSuppressedOccurrenceCreatesNoExecutableRun walks every suppression reason
// in the order the trigger evaluates them and requires the same thing of each:
// the firing is recorded, and nothing that could be executed is created. Now
// that an accepted occurrence writes attempts, artifacts and a graph revision,
// a suppression that leaked any of those would be a scheduled run nobody asked
// for.
func TestSuppressedOccurrenceCreatesNoExecutableRun(t *testing.T) {
	ctx := context.Background()
	nominal := scheduledFixtureTime.Add(26 * time.Hour)
	for _, testCase := range []struct {
		reason   string
		enabled  bool
		failure  domain.ScheduleFailurePolicy
		delayed  bool
		misfired bool
		openRun  *domain.WorkflowRun
	}{
		{reason: "schedule-disabled", enabled: false, failure: domain.ScheduleFailureNextCycle},
		{reason: "admin-delayed", enabled: true, failure: domain.ScheduleFailureNextCycle, delayed: true},
		{reason: "misfire-skipped", enabled: true, failure: domain.ScheduleFailureNextCycle, misfired: true},
		{
			reason: "overlap-forbidden", enabled: true, failure: domain.ScheduleFailureNextCycle,
			openRun: &domain.WorkflowRun{
				ID: "open-run", WorkflowID: "workflow-1", Progress: domain.ProgressActive,
				Revision: 1, CreatedAt: scheduledFixtureTime, UpdatedAt: scheduledFixtureTime,
			},
		},
		{
			reason: "failure-hold", enabled: true, failure: domain.ScheduleFailureHold,
			openRun: &domain.WorkflowRun{
				ID: "failed-run", WorkflowID: "workflow-1", Progress: domain.ProgressFailed,
				Revision: 1, CreatedAt: scheduledFixtureTime, UpdatedAt: scheduledFixtureTime,
			},
		},
	} {
		t.Run(testCase.reason, func(t *testing.T) {
			records := scheduledWorkflowRecords("workflow-1", "submitted-run")
			schedule, template := scheduledFixtureSchedule(testCase.enabled, testCase.failure)
			if testCase.delayed {
				notBefore := nominal.Add(time.Hour)
				schedule.NextNotBefore = &notBefore
			}
			if testCase.openRun != nil {
				schedule.ActiveRunID = testCase.openRun.ID
				records.WorkflowRuns = []domain.WorkflowRun{*testCase.openRun}
			}
			records.Schedules = []domain.Schedule{schedule}
			records.ScheduleTemplates = []domain.ScheduleTemplate{template}
			store := scheduledFixtureStore(t, records)

			result, err := store.CommitScheduleTrigger(ctx, domain.ScheduleTriggerRequest{
				ScheduleID: schedule.ID, TriggerID: "trigger-1", WorkflowRunID: "scheduled-run",
				NominalAt: nominal, ObservedAt: nominal, Source: domain.ScheduleTriggerScheduled,
				Misfired: testCase.misfired,
			})
			if err != nil {
				t.Fatalf("suppressed trigger: %v", err)
			}
			if result.Trigger.State != domain.TriggerSuppressed || result.Trigger.Reason != testCase.reason {
				t.Fatalf("trigger = %#v, want reason %q", result.Trigger, testCase.reason)
			}
			if result.WorkflowRun != nil {
				t.Fatalf("suppressed trigger produced run %#v", result.WorkflowRun)
			}
			assertNoSeededWork(ctx, t, store, "scheduled-run")
		})
	}
}

func assertNoSeededWork(ctx context.Context, t *testing.T, store *sqlite.Store, runID string) {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range records.WorkflowRuns {
		if run.ID == runID {
			t.Fatalf("suppressed occurrence created run %q", runID)
		}
	}
	for _, attempt := range records.Attempts {
		if attempt.WorkflowRunID == runID {
			t.Fatalf("suppressed occurrence created attempt %q", attempt.ID)
		}
	}
	for _, artifact := range records.Artifacts {
		if artifact.WorkflowRunID == runID {
			t.Fatalf("suppressed occurrence bound artifact %q", artifact.ID)
		}
	}
	graphs, err := store.LoadGraphRevisions(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(graphs) != 0 {
		t.Fatalf("suppressed occurrence wrote graph revisions %#v", graphs)
	}
}

// TestScheduleTimerCatchUpSeedsOnlyTheOccurrenceItRan pins the catch-up bound
// against the seeded run: downtime longer than catch_up_max still fires at most
// that many occurrences, the ones that are late are flagged as misfires and
// skipped, and only the occurrence that was actually accepted has attempts.
func TestScheduleTimerCatchUpSeedsOnlyTheOccurrenceItRan(t *testing.T) {
	ctx := context.Background()
	records := scheduledWorkflowRecords("workflow-1", "submitted-run")
	schedule, template := scheduledFixtureSchedule(true, domain.ScheduleFailureNextCycle)
	schedule.Expression, template.Expression = "0 * * * *", "0 * * * *"
	records.Schedules = []domain.Schedule{schedule}
	records.ScheduleTemplates = []domain.ScheduleTemplate{template}
	store := scheduledFixtureStore(t, records)

	// Six hourly occurrences are due; catch_up_max admits the last two.
	now := scheduledFixtureTime.Add(6*time.Hour + 30*time.Second)
	timer := ScheduleTimer{Store: store, CatchUpMax: 2, Now: func() time.Time { return now }}
	report, err := timer.Tick(ctx)
	if err != nil {
		t.Fatalf("catch-up tick: %v", err)
	}
	if len(report.Results) != 2 {
		t.Fatalf("catch-up produced %d results, want 2", len(report.Results))
	}
	late, current := report.Results[0], report.Results[1]
	if late.Trigger.State != domain.TriggerSuppressed || late.Trigger.Reason != "misfire-skipped" {
		t.Fatalf("late occurrence = %#v", late.Trigger)
	}
	if late.WorkflowRun != nil {
		t.Fatalf("misfire produced run %#v", late.WorkflowRun)
	}
	assertNoSeededWork(ctx, t, store, "scheduled-run-"+late.Trigger.ID)
	if current.Trigger.State != domain.TriggerAccepted || current.WorkflowRun == nil {
		t.Fatalf("current occurrence = %#v", current.Trigger)
	}
	assertScheduledRunIsExecutable(ctx, t, store, schedule, current, "submitted-run")

	// A restart at the same instant re-derives the cursor from durable triggers
	// and fires nothing again.
	replay, err := ScheduleTimer{Store: store, CatchUpMax: 2, Now: func() time.Time { return now }}.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(replay.Results) != 0 {
		t.Fatalf("restart fired %d occurrences again", len(replay.Results))
	}
}
