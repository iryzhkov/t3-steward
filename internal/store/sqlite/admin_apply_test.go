package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestApplyAdminCommandAtomicallyTransitionsAndReplaysAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openAdminCommandStore(t, path)
	now := adminCommandTestTime
	records := CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "workflow", Class: domain.TaskClassRequired, TaskIDs: []string{"task-1"}, CreatedAt: now}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive, Revision: 2, CreatedAt: now, UpdatedAt: now}},
		Tasks:        []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassRequired}},
		Attempts:     []domain.Attempt{{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 4, UpdatedAt: now}},
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	command := domain.AdminCommand{
		ID: "cancel-1", Kind: domain.AdminCommandCancel, TargetType: domain.AdminTargetAttempt,
		TargetID: "attempt-1", ExpectedRevision: 4, Reason: "stop", RequestedBy: "operator",
		State: domain.AdminCommandPending, CreatedAt: now,
	}
	if _, err := store.SubmitAdminCommand(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	completedAt := now.Add(time.Minute)
	next := records.Attempts[0]
	next.Progress, next.Control, next.Revision, next.UpdatedAt, next.CompletedAt = domain.ProgressCancelled, domain.ControlStopped, 5, completedAt, &completedAt
	application := domain.AdminCommandApplication{
		CommandID: command.ID, ExpectedCommandState: domain.AdminCommandPending, ExpectedTargetRevision: 4,
		Attempt: &next, State: domain.AdminCommandApplied, AppliedAt: completedAt,
	}
	first, err := store.ApplyAdminCommand(context.Background(), application)
	if err != nil {
		t.Fatal(err)
	}
	if first.Command.State != domain.AdminCommandApplied || first.Event.Kind != "admin-command-applied" {
		t.Fatalf("decision = %#v", first)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openAdminCommandStore(t, path)
	replay, err := store.ApplyAdminCommand(context.Background(), application)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Command.State != domain.AdminCommandApplied || replay.Event.ID != first.Event.ID {
		t.Fatalf("replay = %#v", replay)
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var got domain.Attempt
	for _, attempt := range loaded.Attempts {
		if attempt.ID == "attempt-1" {
			got = attempt
		}
	}
	if got.Progress != domain.ProgressCancelled || got.Revision != 5 {
		t.Fatalf("attempt = %#v", got)
	}
}

func TestApplyAdminStartRequiresSafetyFingerprint(t *testing.T) {
	store := openAdminCommandStore(t, filepath.Join(t.TempDir(), "state.db"))
	now := adminCommandTestTime
	attempt := adminCommandAttempt(3)
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Attempts: []domain.Attempt{attempt}}); err != nil {
		t.Fatal(err)
	}
	command := adminCommand("start-without-safety", 3)
	command.Kind = domain.AdminCommandStart
	if _, err := store.SubmitAdminCommand(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	next := attempt
	next.Progress, next.Revision, next.UpdatedAt = domain.ProgressReady, 4, now.Add(time.Minute)
	_, err := store.ApplyAdminCommand(context.Background(), domain.AdminCommandApplication{
		CommandID: command.ID, ExpectedCommandState: domain.AdminCommandPending, ExpectedTargetRevision: 3,
		Attempt: &next, State: domain.AdminCommandApplied, AppliedAt: now.Add(time.Minute),
	})
	if !errors.Is(err, ErrInvalidAdminCommandOutcome) {
		t.Fatalf("missing safety fingerprint error = %v", err)
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.AdminCommands[0].State != domain.AdminCommandPending || loaded.Attempts[0].Revision != 3 {
		t.Fatalf("invalid application mutated state: %#v", loaded)
	}
}

func TestApplyAdminStartRejectsMissingOrExpiredWorkerSafetyValidity(t *testing.T) {
	store := openAdminCommandStore(t, filepath.Join(t.TempDir(), "state.db"))
	now := adminCommandTestTime
	attempt := adminCommandAttempt(3)
	worker := domain.WorkerSnapshot{
		WorkerID: "normandy", WorkerEpoch: "worker-epoch", CoordinatorEpoch: 1, Sequence: 1,
		Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Minute),
		Inventory: domain.WorkerInventory{ID: "normandy", AcceptBacklog: true, Health: domain.WorkerHealthReady},
	}
	records := CoordinatorRecords{Attempts: []domain.Attempt{attempt}}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	command := adminCommand("start-worker-validity", 3)
	command.Kind = domain.AdminCommandStart
	if _, err := store.SubmitAdminCommand(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	fingerprint, err := domain.AdminSafetyFingerprint(domain.AdminSafetyState{Attempts: records.Attempts, Workers: []domain.WorkerSnapshot{worker}})
	if err != nil {
		t.Fatal(err)
	}
	next := attempt
	next.Progress, next.Revision, next.UpdatedAt = domain.ProgressReady, 4, now.Add(time.Minute)
	application := domain.AdminCommandApplication{
		CommandID: command.ID, ExpectedCommandState: domain.AdminCommandPending, ExpectedTargetRevision: 3,
		SafetyFingerprint: fingerprint, Attempt: &next, State: domain.AdminCommandApplied, AppliedAt: now,
	}
	if _, err := store.ApplyAdminCommand(context.Background(), application); !errors.Is(err, ErrInvalidAdminCommandOutcome) {
		t.Fatalf("missing worker safety validity error = %v", err)
	}

	validUntil := worker.ValidUntil
	tamperedValidUntil := validUntil.Add(time.Hour)
	application.SafetyValidUntil = &tamperedValidUntil
	if _, err := store.ApplyAdminCommand(context.Background(), application); !errors.Is(err, ErrInvalidAdminCommandOutcome) {
		t.Fatalf("inconsistent worker safety validity error = %v", err)
	}
	application.SafetyValidUntil = &validUntil
	store.SetClock(func() time.Time { return validUntil.Add(time.Nanosecond) })
	if _, err := store.ApplyAdminCommand(context.Background(), application); !errors.Is(err, ErrStaleAdminSafetyFence) {
		t.Fatalf("expired worker safety validity error = %v", err)
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.AdminCommands[0].State != domain.AdminCommandPending || loaded.Attempts[0].Revision != 3 {
		t.Fatalf("expired safety application mutated state: %#v", loaded)
	}
}

func TestConcurrentAdminCommandApplicationHasOneTransitionAndOutcome(t *testing.T) {
	store := openAdminCommandStore(t, filepath.Join(t.TempDir(), "state.db"))
	now := adminCommandTestTime
	attempt := adminCommandAttempt(3)
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Attempts: []domain.Attempt{attempt}}); err != nil {
		t.Fatal(err)
	}
	command := adminCommand("concurrent-apply", 3)
	command.Kind = domain.AdminCommandCancel
	if _, err := store.SubmitAdminCommand(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	next := attempt
	next.Progress, next.Control, next.Revision, next.UpdatedAt = domain.ProgressCancelled, domain.ControlStopped, 4, now.Add(time.Minute)
	next.CompletedAt = &next.UpdatedAt
	application := domain.AdminCommandApplication{
		CommandID: command.ID, ExpectedCommandState: domain.AdminCommandPending, ExpectedTargetRevision: 3,
		Attempt: &next, State: domain.AdminCommandApplied, AppliedAt: now.Add(time.Minute),
	}

	const callers = 8
	decisions := make(chan domain.AdminCommandDecision, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decision, err := store.ApplyAdminCommand(context.Background(), application)
			if err != nil {
				errs <- err
				return
			}
			decisions <- decision
		}()
	}
	wg.Wait()
	close(errs)
	close(decisions)
	for err := range errs {
		t.Errorf("concurrent apply: %v", err)
	}
	eventID := ""
	for decision := range decisions {
		if decision.Command.State != domain.AdminCommandApplied {
			t.Errorf("decision = %#v", decision)
		}
		if eventID == "" {
			eventID = decision.Event.ID
		} else if decision.Event.ID != eventID {
			t.Errorf("event IDs = %q and %q", eventID, decision.Event.ID)
		}
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Attempts[0].Revision != 4 || loaded.Attempts[0].Progress != domain.ProgressCancelled ||
		len(loaded.AdminCommands) != 1 || len(loaded.AuditEvents) != 2 {
		t.Fatalf("records = %#v", loaded)
	}
}

func TestApplyManualScheduleRunRollsBackTriggerWithOutcomeConflict(t *testing.T) {
	store := openAdminCommandStore(t, filepath.Join(t.TempDir(), "state.db"))
	now := adminCommandTestTime
	records := CoordinatorRecords{
		Workflows: []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "workflow", Class: domain.TaskClassRequired}},
		Schedules: []domain.Schedule{{
			ID: "schedule-1", Name: "nightly", Version: 1, WorkflowID: "workflow-1", Enabled: true,
			Overlap: domain.ScheduleOverlapForbid, Misfire: domain.ScheduleMisfireSkip,
			AfterFailure: domain.ScheduleFailureNextCycle, Revision: 1, CreatedAt: now, UpdatedAt: now,
		}},
		ScheduleTemplates: []domain.ScheduleTemplate{{
			ScheduleID: "schedule-1", Version: 1, WorkflowID: "workflow-1", Expression: "0 3 * * *", Timezone: "UTC",
			Overlap: domain.ScheduleOverlapForbid, Misfire: domain.ScheduleMisfireSkip,
			AfterFailure: domain.ScheduleFailureNextCycle, CreatedAt: now,
		}},
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	command := domain.AdminCommand{
		ID: "manual-1", Kind: domain.AdminCommandScheduleRun, TargetType: domain.AdminTargetSchedule,
		TargetID: "schedule-1", ExpectedRevision: 1, Reason: "run now", RequestedBy: "operator",
		State: domain.AdminCommandPending, CreatedAt: now,
	}
	if _, err := store.SubmitAdminCommand(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	conflict := domain.AuditEvent{
		ID: adminOutcomeEventID(command.ID), Kind: "conflicting-event", WorkflowRunID: "other-run",
		TargetType: domain.AdminTargetSchedule, TargetID: command.TargetID, CreatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{AuditEvents: []domain.AuditEvent{conflict}}); err != nil {
		t.Fatal(err)
	}
	trigger := domain.ScheduleTriggerRequest{
		ScheduleID: "schedule-1", TriggerID: "trigger-manual", WorkflowRunID: "run-manual",
		NominalAt: now, ObservedAt: now.Add(time.Minute), Source: domain.ScheduleTriggerManual,
	}
	if _, err := store.ApplyAdminCommand(context.Background(), domain.AdminCommandApplication{
		CommandID: command.ID, ExpectedCommandState: domain.AdminCommandPending, ExpectedTargetRevision: 1,
		ScheduleTrigger: &trigger, State: domain.AdminCommandApplied, AppliedAt: now.Add(time.Minute),
	}); err == nil {
		t.Fatal("expected audit conflict")
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Triggers) != 0 || len(loaded.WorkflowRuns) != 0 || loaded.Schedules[0].Revision != 1 ||
		loaded.AdminCommands[0].State != domain.AdminCommandPending {
		t.Fatalf("partial manual run survived rollback: %#v", loaded)
	}
}

func TestApplyManualScheduleRunDurablyRejectsApplyTimeOpenRun(t *testing.T) {
	store := openAdminCommandStore(t, filepath.Join(t.TempDir(), "state.db"))
	now := adminCommandTestTime
	records := CoordinatorRecords{
		Workflows: []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "workflow", Class: domain.TaskClassRequired}},
		WorkflowRuns: []domain.WorkflowRun{{
			ID: "run-existing", WorkflowID: "workflow-1", ScheduleID: "schedule-1",
			Progress: domain.ProgressQueued, Revision: 1, CreatedAt: now, UpdatedAt: now,
		}},
		Schedules: []domain.Schedule{{
			ID: "schedule-1", Name: "nightly", Version: 1, WorkflowID: "workflow-1", Enabled: true,
			Overlap: domain.ScheduleOverlapForbid, Misfire: domain.ScheduleMisfireSkip,
			AfterFailure: domain.ScheduleFailureNextCycle, ActiveRunID: "run-existing",
			Revision: 1, CreatedAt: now, UpdatedAt: now,
		}},
		ScheduleTemplates: []domain.ScheduleTemplate{{
			ScheduleID: "schedule-1", Version: 1, WorkflowID: "workflow-1", Expression: "0 3 * * *", Timezone: "UTC",
			Overlap: domain.ScheduleOverlapForbid, Misfire: domain.ScheduleMisfireSkip,
			AfterFailure: domain.ScheduleFailureNextCycle, CreatedAt: now,
		}},
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	command := domain.AdminCommand{
		ID: "manual-race", Kind: domain.AdminCommandScheduleRun, TargetType: domain.AdminTargetSchedule,
		TargetID: "schedule-1", ExpectedRevision: 1, Reason: "run now", RequestedBy: "operator",
		State: domain.AdminCommandPending, CreatedAt: now,
	}
	if _, err := store.SubmitAdminCommand(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	trigger := domain.ScheduleTriggerRequest{
		ScheduleID: "schedule-1", TriggerID: "trigger-manual", WorkflowRunID: "run-manual",
		NominalAt: now, ObservedAt: now.Add(time.Minute), Source: domain.ScheduleTriggerManual,
	}
	decision, err := store.ApplyAdminCommand(context.Background(), domain.AdminCommandApplication{
		CommandID: command.ID, ExpectedCommandState: domain.AdminCommandPending, ExpectedTargetRevision: 1,
		ScheduleTrigger: &trigger, State: domain.AdminCommandApplied, AppliedAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Command.State != domain.AdminCommandRejected ||
		decision.Command.Failure != ErrManualScheduleRunOpen.Error() {
		t.Fatalf("decision = %#v", decision)
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Triggers) != 0 || len(loaded.WorkflowRuns) != 1 ||
		loaded.WorkflowRuns[0].ID != "run-existing" || loaded.Schedules[0].Revision != 1 ||
		loaded.AdminCommands[0].State != domain.AdminCommandRejected || len(loaded.AuditEvents) != 2 {
		t.Fatalf("apply-time rejection state = %#v", loaded)
	}
}

func TestApplyAdminCommandRejectsTargetChangedAfterPlanning(t *testing.T) {
	store := openAdminCommandStore(t, filepath.Join(t.TempDir(), "state.db"))
	now := adminCommandTestTime
	attempt := adminCommandAttempt(7)
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Attempts: []domain.Attempt{attempt}}); err != nil {
		t.Fatal(err)
	}
	command := adminCommand("stale-execution", 7)
	command.Kind = domain.AdminCommandCancel
	if _, err := store.SubmitAdminCommand(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	attempt.Revision = 8
	attempt.UpdatedAt = now.Add(time.Second)
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Attempts: []domain.Attempt{attempt}}); err != nil {
		t.Fatal(err)
	}
	next := attempt
	next.Revision = 9
	decision, err := store.ApplyAdminCommand(context.Background(), domain.AdminCommandApplication{
		CommandID: command.ID, ExpectedCommandState: domain.AdminCommandPending, ExpectedTargetRevision: 7,
		Attempt: &next, State: domain.AdminCommandApplied, AppliedAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Command.State != domain.AdminCommandRejected || decision.CurrentTarget == nil || decision.CurrentTarget.Revision != 8 {
		t.Fatalf("decision = %#v", decision)
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Attempts[0].Revision != 8 {
		t.Fatalf("stale transition changed target: %#v", loaded.Attempts[0])
	}
}
