package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var adminCommandTestTime = time.Date(2026, 9, 10, 21, 0, 0, 0, time.UTC)

func TestAdminCommandSubmissionRevisionReplayAndOutcome(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openAdminCommandStore(t, path)
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{
		Attempts: []domain.Attempt{adminCommandAttempt(3)},
	}); err != nil {
		t.Fatal(err)
	}

	command := adminCommand("command-1", 3)
	submitted, err := store.SubmitAdminCommand(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(submitted.Command, command) || submitted.Event.Sequence != 1 ||
		submitted.Event.Kind != "admin-command-submitted" ||
		submitted.Event.WorkflowRunID != "run-1" || submitted.Event.TaskID != "task-1" ||
		submitted.Event.AttemptID != "attempt-1" || submitted.CurrentTarget != nil {
		t.Fatalf("unexpected submitted command: %#v", submitted)
	}
	laterReplay := command
	laterReplay.CreatedAt = command.CreatedAt.Add(time.Hour)
	replayed, err := store.SubmitAdminCommand(context.Background(), laterReplay)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replayed, submitted) {
		t.Fatalf("submission replay changed: got %#v want %#v", replayed, submitted)
	}

	conflict := command
	conflict.Reason = "different immutable request"
	if _, err := store.SubmitAdminCommand(context.Background(), conflict); !errors.Is(err, ErrAdminCommandConflict) {
		t.Fatalf("replay conflict error = %v", err)
	}

	stale := adminCommand("command-stale", 2)
	rejected, err := store.SubmitAdminCommand(context.Background(), stale)
	if err != nil {
		t.Fatal(err)
	}
	if rejected.Command.State != domain.AdminCommandRejected || rejected.CurrentTarget == nil ||
		rejected.CurrentTarget.Revision != 3 || rejected.Event.Kind != "admin-command-rejected" ||
		!strings.Contains(rejected.Command.Failure, "expected 2, current 3") {
		t.Fatalf("unexpected stale decision: %#v", rejected)
	}
	staleReplay, err := store.SubmitAdminCommand(context.Background(), stale)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(staleReplay, rejected) {
		t.Fatalf("stale replay changed: got %#v want %#v", staleReplay, rejected)
	}

	appliedAt := adminCommandTestTime.Add(time.Minute)
	outcome := domain.AdminCommandOutcome{
		CommandID: "command-1", ExpectedState: domain.AdminCommandPending,
		State: domain.AdminCommandApplied, AppliedAt: appliedAt,
	}
	completed, err := store.CompleteAdminCommand(context.Background(), outcome)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Command.State != domain.AdminCommandApplied || completed.Command.AppliedAt == nil ||
		!completed.Command.AppliedAt.Equal(appliedAt) || completed.Event.Sequence != 3 ||
		completed.Event.Kind != "admin-command-applied" {
		t.Fatalf("unexpected outcome: %#v", completed)
	}
	laterOutcomeReplay := outcome
	laterOutcomeReplay.AppliedAt = outcome.AppliedAt.Add(time.Hour)
	outcomeReplay, err := store.CompleteAdminCommand(context.Background(), laterOutcomeReplay)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(outcomeReplay, completed) {
		t.Fatalf("outcome replay changed: got %#v want %#v", outcomeReplay, completed)
	}
	conflictingOutcome := outcome
	conflictingOutcome.State = domain.AdminCommandFailed
	conflictingOutcome.Failure = "worker refused"
	if _, err := store.CompleteAdminCommand(context.Background(), conflictingOutcome); !errors.Is(err, ErrAdminCommandStateConflict) {
		t.Fatalf("outcome conflict error = %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openAdminCommandStore(t, path)
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.AdminCommands) != 2 || len(records.AuditEvents) != 3 ||
		records.AdminCommands[0].State != domain.AdminCommandApplied ||
		records.AdminCommands[1].State != domain.AdminCommandRejected {
		t.Fatalf("restart state mismatch: %#v", records)
	}
	events, err := store.LoadAuditEvents(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events, records.AuditEvents) {
		t.Fatalf("filtered audit events mismatch: %#v != %#v", events, records.AuditEvents)
	}
}

func TestConcurrentAdminCommandSubmissionHasOneCommandAndEvent(t *testing.T) {
	store := openAdminCommandStore(t, filepath.Join(t.TempDir(), "state.db"))
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{
		Attempts: []domain.Attempt{adminCommandAttempt(3)},
	}); err != nil {
		t.Fatal(err)
	}
	command := adminCommand("command-concurrent", 3)
	const callers = 8
	results := make(chan domain.AdminCommandDecision, callers)
	errs := make(chan error, callers)
	var wait sync.WaitGroup
	for index := 0; index < callers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			decision, err := store.SubmitAdminCommand(context.Background(), command)
			results <- decision
			errs <- err
		}()
	}
	wait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for decision := range results {
		if decision.Command.ID != command.ID || decision.Event.Sequence != 1 {
			t.Fatalf("unexpected concurrent decision: %#v", decision)
		}
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.AdminCommands) != 1 || len(records.AuditEvents) != 1 {
		t.Fatalf("concurrent submission persisted duplicates: %#v", records)
	}
}

func TestAdminCommandTransactionsRollBackWithAuditConflict(t *testing.T) {
	store := openAdminCommandStore(t, filepath.Join(t.TempDir(), "state.db"))
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{
		Attempts: []domain.Attempt{adminCommandAttempt(3)},
		AuditEvents: []domain.AuditEvent{{
			ID:   "admin-command:command-submit-rollback:submission",
			Kind: "conflicting-event", CreatedAt: adminCommandTestTime,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SubmitAdminCommand(context.Background(), adminCommand("command-submit-rollback", 3)); err == nil {
		t.Fatal("submission with conflicting audit event succeeded")
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.AdminCommands) != 0 {
		t.Fatalf("command survived rolled-back submission: %#v", records.AdminCommands)
	}

	pending := adminCommand("command-outcome-rollback", 3)
	if _, err := store.SubmitAdminCommand(context.Background(), pending); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{
		AuditEvents: []domain.AuditEvent{{
			ID:   "admin-command:command-outcome-rollback:outcome",
			Kind: "conflicting-outcome", CreatedAt: adminCommandTestTime,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteAdminCommand(context.Background(), domain.AdminCommandOutcome{
		CommandID: pending.ID, ExpectedState: domain.AdminCommandPending,
		State: domain.AdminCommandFailed, Failure: "execution failed",
		AppliedAt: adminCommandTestTime.Add(time.Minute),
	}); err == nil {
		t.Fatal("outcome with conflicting audit event succeeded")
	}
	records, err = store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range records.AdminCommands {
		if command.ID == pending.ID && command.State != domain.AdminCommandPending {
			t.Fatalf("outcome update survived rollback: %#v", command)
		}
	}
}

func TestAdminCommandsAreImmutableOutsideOutcomeTransaction(t *testing.T) {
	store := openAdminCommandStore(t, filepath.Join(t.TempDir(), "state.db"))
	command := adminCommand("command-1", 1)
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{
		AdminCommands: []domain.AdminCommand{command},
	}); err != nil {
		t.Fatal(err)
	}
	changed := command
	changed.State = domain.AdminCommandApplied
	appliedAt := adminCommandTestTime.Add(time.Minute)
	changed.AppliedAt = &appliedAt
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{
		AdminCommands: []domain.AdminCommand{changed},
	}); err == nil {
		t.Fatal("general coordinator save mutated an admin command")
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.AdminCommands) != 1 || !reflect.DeepEqual(records.AdminCommands[0], command) {
		t.Fatalf("immutable command changed: %#v", records.AdminCommands)
	}
}

func TestAdminCommandValidationAndMissingTargets(t *testing.T) {
	store := openAdminCommandStore(t, filepath.Join(t.TempDir(), "state.db"))
	command := adminCommand("command-1", 1)
	if _, err := store.SubmitAdminCommand(context.Background(), command); !errors.Is(err, ErrAdminTargetNotFound) {
		t.Fatalf("missing target error = %v", err)
	}
	command.TargetType = domain.AdminTargetSchedule
	if _, err := store.SubmitAdminCommand(context.Background(), command); !errors.Is(err, ErrInvalidAdminCommand) {
		t.Fatalf("kind/target validation error = %v", err)
	}
	if _, err := store.CompleteAdminCommand(context.Background(), domain.AdminCommandOutcome{
		CommandID: "command-1", ExpectedState: domain.AdminCommandPending,
		State: domain.AdminCommandFailed, AppliedAt: adminCommandTestTime,
	}); !errors.Is(err, ErrInvalidAdminCommandOutcome) {
		t.Fatalf("outcome validation error = %v", err)
	}
}

func openAdminCommandStore(t *testing.T, path string) *Store {
	t.Helper()
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	store.SetClock(func() time.Time { return adminCommandTestTime })
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func adminCommandAttempt(revision int64) domain.Attempt {
	return domain.Attempt{
		ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1,
		Progress: domain.ProgressActive, Control: domain.ControlRunning,
		Revision: revision, UpdatedAt: adminCommandTestTime,
	}
}

func adminCommand(id string, revision int64) domain.AdminCommand {
	return domain.AdminCommand{
		ID: id, Kind: domain.AdminCommandPause, TargetType: domain.AdminTargetAttempt,
		TargetID: "attempt-1", ExpectedRevision: revision,
		Reason: "operator request", RequestedBy: "operator-1",
		Payload: []byte(`{"now":false}`), State: domain.AdminCommandPending,
		CreatedAt: adminCommandTestTime,
	}
}
