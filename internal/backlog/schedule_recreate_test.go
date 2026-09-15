package backlog

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// deleteScheduleForTest removes one schedule through the real revision-fenced
// admin command path, which is what an operator's `schedules delete` does.
func deleteScheduleForTest(t *testing.T, store *sqlite.Store, scheduleID string, revision int64, now time.Time) {
	t.Helper()
	ctx := context.Background()
	command := domain.AdminCommand{
		ID: "delete-" + scheduleID, Kind: domain.AdminCommandScheduleDelete,
		TargetType: domain.AdminTargetSchedule, TargetID: scheduleID,
		ExpectedRevision: revision, Reason: "retire the schedule", RequestedBy: "operator",
		State: domain.AdminCommandPending, CreatedAt: now,
	}
	if _, err := store.SubmitAdminCommand(ctx, command); err != nil {
		t.Fatalf("submit delete: %v", err)
	}
	decision, err := store.ApplyAdminCommand(ctx, domain.AdminCommandApplication{
		CommandID: command.ID, ExpectedCommandState: domain.AdminCommandPending,
		ExpectedTargetRevision: revision, State: domain.AdminCommandApplied, AppliedAt: now,
	})
	if err != nil {
		t.Fatalf("apply delete: %v", err)
	}
	if decision.Command.State != domain.AdminCommandApplied {
		t.Fatalf("delete = %#v", decision.Command)
	}
}

func scheduleRevision(t *testing.T, store *sqlite.Store, scheduleID string) int64 {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, schedule := range records.Schedules {
		if schedule.ID == scheduleID {
			return schedule.Revision
		}
	}
	t.Fatalf("schedule %q not found", scheduleID)
	return 0
}

// TestDeletedScheduleRecreatedUnderTheSameIDStillFires is the regression for the
// interaction between delete and the deterministic occurrence identities.
//
// A trigger ID and its workflow-run ID are both sha256(scheduleID + nominal),
// the run rows a schedule created are deliberately kept by delete, and the run
// table's primary key is that same ID inserted without a conflict clause. The
// trigger's unique occurrence key is therefore the only thing standing between
// a repeated occurrence and a duplicate-key failure inside the seed — and a
// failure there rolls the trigger transaction back, on an occurrence the timer
// regenerates every tick, which is a schedule that can never fire again.
//
// Deleting a schedule must not free those identities.
func TestDeletedScheduleRecreatedUnderTheSameIDStillFires(t *testing.T) {
	ctx := context.Background()
	records := scheduledWorkflowRecords("workflow-1", "submitted-run")
	schedule, template := scheduledFixtureSchedule(true, domain.ScheduleFailureNextCycle)
	schedule.Expression, template.Expression = "0 * * * *", "0 * * * *"
	records.Schedules = []domain.Schedule{schedule}
	records.ScheduleTemplates = []domain.ScheduleTemplate{template}
	store := scheduledFixtureStore(t, records)

	firstNominal := scheduledFixtureTime.Add(time.Hour)
	now := firstNominal.Add(30 * time.Second)
	first, err := ScheduleTimer{Store: store, CatchUpMax: 4, Now: func() time.Time { return now }}.Tick(ctx)
	if err != nil || len(first.Results) != 1 || first.Results[0].WorkflowRun == nil {
		t.Fatalf("first tick = %#v, err = %v", first, err)
	}
	originalRunID := first.Results[0].WorkflowRun.ID

	deleteScheduleForTest(t, store, schedule.ID, scheduleRevision(t, store, schedule.ID), now)

	// Recreate the schedule under the same identity. The original creation time
	// is kept on purpose: that is what a restore from backup looks like, and it
	// is the only way a post-recreate occurrence can land on a nominal time the
	// deleted schedule already fired.
	recreated, recreatedTemplate := scheduledFixtureSchedule(true, domain.ScheduleFailureNextCycle)
	recreated.Expression, recreatedTemplate.Expression = "0 * * * *", "0 * * * *"
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Schedules: []domain.Schedule{recreated}, ScheduleTemplates: []domain.ScheduleTemplate{recreatedTemplate},
	}); err != nil {
		t.Fatalf("recreate schedule: %v", err)
	}

	// The occurrence the deleted schedule already fired must still be fenced.
	// Before the triggers were kept this was a UNIQUE violation on the run's
	// primary key, which rolled the whole transaction back.
	replay, err := store.CommitScheduleTrigger(ctx, deterministicScheduleTrigger(schedule.ID, firstNominal, now))
	if err != nil {
		t.Fatalf("an occurrence the deleted schedule already fired is no longer fenced: %v", err)
	}
	if !replay.Replay || replay.WorkflowRun == nil || replay.WorkflowRun.ID != originalRunID {
		t.Fatalf("repeated occurrence = %#v, want a replay of %q", replay, originalRunID)
	}

	// And the recreated schedule goes on firing its own later occurrences: the
	// timer anchors on the latest surviving trigger, so it never regenerates the
	// ones that already happened.
	secondNominal := scheduledFixtureTime.Add(2 * time.Hour)
	now = secondNominal.Add(30 * time.Second)
	second, err := ScheduleTimer{Store: store, CatchUpMax: 4, Now: func() time.Time { return now }}.Tick(ctx)
	if err != nil {
		t.Fatalf("tick after recreate: %v", err)
	}
	if len(second.Issues) != 0 {
		t.Fatalf("recreated schedule reported issues: %#v", second.Issues)
	}
	if len(second.Results) != 1 || second.Results[0].Trigger.State != domain.TriggerAccepted ||
		!second.Results[0].Trigger.NominalAt.Equal(secondNominal) {
		t.Fatalf("recreated schedule fired %#v", second.Results)
	}
	if second.Results[0].WorkflowRun == nil || second.Results[0].WorkflowRun.ID == originalRunID {
		t.Fatalf("recreated schedule reused the original run identity: %#v", second.Results[0].WorkflowRun)
	}
	assertScheduledRunIsExecutable(ctx, t, store, recreated, second.Results[0], "submitted-run")
}

// TestScheduleDeletedMidTickIsNotReportedAsAFailure covers the interleaving
// where a schedule is removed between the snapshot a tick reads and the
// transaction that would have fired it. Nothing is wrong, so nothing is
// reported: a spurious issue about a schedule that no longer exists is noise an
// operator would have to chase.
func TestScheduleDeletedMidTickIsNotReportedAsAFailure(t *testing.T) {
	ctx := context.Background()
	records := scheduledWorkflowRecords("workflow-1", "submitted-run")
	schedule, template := scheduledFixtureSchedule(true, domain.ScheduleFailureNextCycle)
	schedule.Expression, template.Expression = "0 * * * *", "0 * * * *"
	records.Schedules = []domain.Schedule{schedule}
	records.ScheduleTemplates = []domain.ScheduleTemplate{template}
	store := scheduledFixtureStore(t, records)
	now := scheduledFixtureTime.Add(time.Hour + 30*time.Second)

	// vanishingStore hands the timer a snapshot that still contains the
	// schedule, then removes it before the commit, which is exactly the window
	// the real coordinator has between its load and its write.
	deleted := false
	vanishing := &vanishingScheduleStore{Store: store, onLoad: func() {
		if deleted {
			return
		}
		deleted = true
		deleteScheduleForTest(t, store, schedule.ID, schedule.Revision, now)
	}}
	report, err := ScheduleTimer{Store: vanishing, CatchUpMax: 4, Now: func() time.Time { return now }}.Tick(ctx)
	if err != nil {
		t.Fatalf("a schedule deleted mid-tick failed the tick: %v", err)
	}
	if len(report.Issues) != 0 {
		t.Fatalf("a schedule deleted mid-tick was reported as a failure: %#v", report.Issues)
	}
	if len(report.Results) != 0 {
		t.Fatalf("a deleted schedule fired: %#v", report.Results)
	}
}

type vanishingScheduleStore struct {
	*sqlite.Store
	onLoad func()
}

func (s *vanishingScheduleStore) LoadCoordinatorRecords(ctx context.Context) (sqlite.CoordinatorRecords, error) {
	records, err := s.Store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return records, err
	}
	s.onLoad()
	return records, nil
}

// TestZeroTaskScheduledRunSettlesAndReleasesTheSchedule is the deadlock check.
//
// A workflow with no executable tasks is legal, and a scheduled occurrence of
// one produces a run that is a sink with nothing in front of it. If that sink
// never reached a terminal state the run would hold the schedule's ActiveRunID
// forever and every later occurrence would be suppressed as overlap-forbidden —
// a schedule that stops firing with no failure anywhere to explain it.
func TestZeroTaskScheduledRunSettlesAndReleasesTheSchedule(t *testing.T) {
	ctx := context.Background()
	schedule, template := scheduledFixtureSchedule(true, domain.ScheduleFailureNextCycle)
	schedule.Expression, template.Expression = "0 * * * *", "0 * * * *"
	store := scheduledFixtureStore(t, sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-1", Version: ManifestVersion, Name: "empty",
			Class: domain.TaskClassRequired, CreatedAt: scheduledFixtureTime,
		}},
		Schedules: []domain.Schedule{schedule}, ScheduleTemplates: []domain.ScheduleTemplate{template},
	})

	now := scheduledFixtureTime.Add(time.Hour + 30*time.Second)
	first, err := ScheduleTimer{Store: store, CatchUpMax: 4, Now: func() time.Time { return now }}.Tick(ctx)
	if err != nil || len(first.Results) != 1 || first.Results[0].WorkflowRun == nil {
		t.Fatalf("first tick = %#v, err = %v", first, err)
	}
	runID := first.Results[0].WorkflowRun.ID
	if first.Results[0].WorkflowRun.Sink == nil {
		t.Fatal("a zero-task scheduled run has no sink")
	}

	// The projection the coordinator runs on every boundary must settle it.
	if _, err := ProjectWorkflowRuns(ctx, store, now.Add(time.Minute)); err != nil {
		t.Fatalf("project runs: %v", err)
	}
	settled, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var run domain.WorkflowRun
	for _, candidate := range settled.WorkflowRuns {
		if candidate.ID == runID {
			run = candidate
		}
	}
	if run.Sink == nil || !run.Sink.Progress.Terminal() || !run.Progress.Terminal() {
		t.Fatalf("zero-task scheduled run never settled: %#v", run)
	}

	// And the next occurrence is therefore not suppressed by the run that
	// already finished.
	now = scheduledFixtureTime.Add(2*time.Hour + 30*time.Second)
	second, err := ScheduleTimer{Store: store, CatchUpMax: 4, Now: func() time.Time { return now }}.Tick(ctx)
	if err != nil {
		t.Fatalf("second tick: %v", err)
	}
	if len(second.Results) != 1 || second.Results[0].Trigger.State != domain.TriggerAccepted {
		t.Fatalf("a settled zero-task run still blocked the schedule: %#v", second.Results)
	}
}
