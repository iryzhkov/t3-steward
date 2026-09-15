package sqlite

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func scheduleDeleteFixture(t *testing.T) *Store {
	t.Helper()
	store := openAdminCommandStore(t, filepath.Join(t.TempDir(), "state.db"))
	now := adminCommandTestTime
	records := CoordinatorRecords{
		Workflows: []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "workflow", Class: domain.TaskClassRequired}},
		WorkflowRuns: []domain.WorkflowRun{{
			ID: "run-past", WorkflowID: "workflow-1", ScheduleID: "schedule-1",
			Progress: domain.ProgressSucceeded, Revision: 1, CreatedAt: now, UpdatedAt: now,
		}},
		Schedules: []domain.Schedule{{
			ID: "schedule-1", Name: "nightly", Version: 1, WorkflowID: "workflow-1", Enabled: true,
			Overlap: domain.ScheduleOverlapForbid, Misfire: domain.ScheduleMisfireSkip,
			AfterFailure: domain.ScheduleFailureNextCycle, ActiveRunID: "run-past",
			Revision: 3, CreatedAt: now, UpdatedAt: now,
		}},
		ScheduleTemplates: []domain.ScheduleTemplate{{
			ScheduleID: "schedule-1", Version: 1, WorkflowID: "workflow-1", Expression: "0 3 * * *",
			Timezone: "UTC", Overlap: domain.ScheduleOverlapForbid, Misfire: domain.ScheduleMisfireSkip,
			AfterFailure: domain.ScheduleFailureNextCycle, CreatedAt: now,
		}},
		Triggers: []domain.Trigger{{
			ID: "trigger-past", ScheduleID: "schedule-1", ScheduleVersion: 1, NominalAt: now,
			OccurrenceKey: "schedule-1/2026-09-10T00:00:00Z", State: domain.TriggerAccepted,
			WorkflowRunID: "run-past", ObservedAt: now,
		}},
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	return store
}

func submitScheduleDelete(t *testing.T, store *Store, id string, expectedRevision int64) domain.AdminCommand {
	t.Helper()
	command := domain.AdminCommand{
		ID: id, Kind: domain.AdminCommandScheduleDelete, TargetType: domain.AdminTargetSchedule,
		TargetID: "schedule-1", ExpectedRevision: expectedRevision, Reason: "retire the schedule",
		RequestedBy: "operator", State: domain.AdminCommandPending, CreatedAt: adminCommandTestTime,
	}
	if _, err := store.SubmitAdminCommand(context.Background(), command); err != nil {
		t.Fatalf("submit delete %q: %v", id, err)
	}
	return command
}

// TestApplyScheduleDeleteIsFencedIdempotentAndAudited covers the three
// properties the removal has to hold: it refuses a revision that has moved, it
// answers a second delivery of the same command with the first outcome instead
// of acting again, and it leaves an audit event behind.
func TestApplyScheduleDeleteIsFencedIdempotentAndAudited(t *testing.T) {
	ctx := context.Background()
	now := adminCommandTestTime

	// The fence has to hold at apply time, not only at submission: the schedule
	// can move between planning the delete and committing it, and a delete that
	// ignored that would remove a definition an operator had just changed.
	t.Run("a revision that moved after submission is refused", func(t *testing.T) {
		store := scheduleDeleteFixture(t)
		submitScheduleDelete(t, store, "delete-stale", 3)
		records, err := store.LoadCoordinatorRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		moved := records.Schedules[0]
		moved.Revision, moved.Enabled = 4, false
		if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Schedules: []domain.Schedule{moved}}); err != nil {
			t.Fatal(err)
		}
		decision, err := store.ApplyAdminCommand(ctx, domain.AdminCommandApplication{
			CommandID: "delete-stale", ExpectedCommandState: domain.AdminCommandPending,
			ExpectedTargetRevision: 3, State: domain.AdminCommandApplied, AppliedAt: now,
		})
		if err != nil {
			t.Fatal(err)
		}
		if decision.Command.State != domain.AdminCommandRejected ||
			!strings.Contains(decision.Command.Failure, "stale execution revision") {
			t.Fatalf("stale delete = %#v", decision.Command)
		}
		remaining, err := store.LoadCoordinatorRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(remaining.Schedules) != 1 || len(remaining.ScheduleTemplates) != 1 || len(remaining.Triggers) != 1 {
			t.Fatalf("refused delete changed durable state: %#v", remaining)
		}
	})

	t.Run("applied delete removes the definition and its occurrences", func(t *testing.T) {
		store := scheduleDeleteFixture(t)
		submitScheduleDelete(t, store, "delete-1", 3)
		application := domain.AdminCommandApplication{
			CommandID: "delete-1", ExpectedCommandState: domain.AdminCommandPending,
			ExpectedTargetRevision: 3, State: domain.AdminCommandApplied, AppliedAt: now,
		}
		decision, err := store.ApplyAdminCommand(ctx, application)
		if err != nil {
			t.Fatal(err)
		}
		if decision.Command.State != domain.AdminCommandApplied || decision.Event.ID == "" ||
			decision.Event.TargetType != domain.AdminTargetSchedule || decision.Event.TargetID != "schedule-1" {
			t.Fatalf("applied delete = %#v, event = %#v", decision.Command, decision.Event)
		}
		records, err := store.LoadCoordinatorRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(records.Schedules) != 0 || len(records.ScheduleTemplates) != 0 {
			t.Fatalf("delete left the definition behind: %#v", records)
		}
		// The trigger rows stay. Their occurrence keys are what reserve the
		// deterministic run identities of the occurrences that already fired,
		// and the runs those identities name are deliberately kept, so removing
		// the triggers would leave the identities allocated and unfenced.
		if len(records.Triggers) != 1 || records.Triggers[0].ID != "trigger-past" {
			t.Fatalf("delete removed the occurrence records: %#v", records.Triggers)
		}
		// The run the schedule created keeps its own history, and so does the
		// audit trail of the firing that produced it.
		if len(records.WorkflowRuns) != 1 || records.WorkflowRuns[0].ScheduleID != "schedule-1" {
			t.Fatalf("delete disturbed the runs it created: %#v", records.WorkflowRuns)
		}
		deleteEvents := 0
		for _, event := range records.AuditEvents {
			if event.TargetID == "schedule-1" && event.Kind == "admin-command-applied" {
				deleteEvents++
			}
		}
		if deleteEvents != 1 {
			t.Fatalf("delete audit events = %d", deleteEvents)
		}

		// A second delivery of the same application answers with the recorded
		// outcome and changes nothing.
		replay, err := store.ApplyAdminCommand(ctx, application)
		if err != nil {
			t.Fatalf("replayed delete: %v", err)
		}
		if replay.Command.State != domain.AdminCommandApplied || replay.Event.ID != decision.Event.ID {
			t.Fatalf("replayed delete = %#v", replay)
		}
		after, err := store.LoadCoordinatorRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(after.AuditEvents) != len(records.AuditEvents) {
			t.Fatalf("replayed delete added audit events: %d then %d", len(records.AuditEvents), len(after.AuditEvents))
		}
	})

	t.Run("an applied delete must not carry a schedule transition", func(t *testing.T) {
		store := scheduleDeleteFixture(t)
		submitScheduleDelete(t, store, "delete-mixed", 3)
		next := domain.Schedule{ID: "schedule-1", Revision: 4}
		if _, err := store.ApplyAdminCommand(ctx, domain.AdminCommandApplication{
			CommandID: "delete-mixed", ExpectedCommandState: domain.AdminCommandPending,
			ExpectedTargetRevision: 3, Schedule: &next,
			State: domain.AdminCommandApplied, AppliedAt: now,
		}); err == nil {
			t.Fatal("a delete carrying a schedule transition was accepted")
		}
		records, err := store.LoadCoordinatorRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(records.Schedules) != 1 {
			t.Fatalf("refused mixed delete removed the schedule: %#v", records.Schedules)
		}
	})
}
