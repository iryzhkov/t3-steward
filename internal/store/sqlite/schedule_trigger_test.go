package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var scheduleTriggerTestTime = time.Date(2026, 9, 10, 9, 0, 0, 0, time.UTC)

func TestCommitScheduleTriggerSerializesSimultaneousFirings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	first := openScheduleTriggerStore(t, path, domain.ScheduleFailureNextCycle, nil)
	defer first.Close()

	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	requests := []domain.ScheduleTriggerRequest{
		scheduleTriggerRequest("trigger-1", "run-1", scheduleTriggerTestTime),
		scheduleTriggerRequest("trigger-2", "run-2", scheduleTriggerTestTime.Add(time.Minute)),
	}
	stores := []*Store{first, second}
	start := make(chan struct{})
	results := make(chan domain.ScheduleTriggerResult, len(requests))
	errs := make(chan error, len(requests))
	var wg sync.WaitGroup
	for i := range requests {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			result, err := stores[i].CommitScheduleTrigger(context.Background(), requests[i])
			results <- result
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("simultaneous trigger: %v", err)
		}
	}
	accepted := 0
	suppressed := 0
	for result := range results {
		switch result.Trigger.State {
		case domain.TriggerAccepted:
			accepted++
			if result.WorkflowRun == nil {
				t.Error("accepted trigger has no workflow run")
			}
		case domain.TriggerSuppressed:
			suppressed++
			if result.Trigger.Reason != "overlap-forbidden" {
				t.Errorf("suppression reason = %q", result.Trigger.Reason)
			}
		default:
			t.Errorf("trigger state = %q", result.Trigger.State)
		}
	}
	if accepted != 1 || suppressed != 1 {
		t.Fatalf("accepted = %d, suppressed = %d", accepted, suppressed)
	}

	records, err := first.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.WorkflowRuns) != 1 || len(records.Triggers) != 2 {
		t.Fatalf("runs = %d, triggers = %d", len(records.WorkflowRuns), len(records.Triggers))
	}
}

func TestCommitScheduleTriggerPersistsMisfireAndReplaysAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openScheduleTriggerStore(t, path, domain.ScheduleFailureNextCycle, nil)

	request := scheduleTriggerRequest("trigger-old", "run-old", scheduleTriggerTestTime.Add(-2*time.Hour))
	request.ObservedAt = scheduleTriggerTestTime
	request.Misfired = true
	first, err := store.CommitScheduleTrigger(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Trigger.State != domain.TriggerSuppressed || first.Trigger.Reason != "misfire-skipped" {
		t.Fatalf("misfire result = %#v", first)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	replayRequest := request
	replayRequest.TriggerID = "replacement-trigger"
	replayRequest.WorkflowRunID = "replacement-run"
	replayed, err := store.CommitScheduleTrigger(context.Background(), replayRequest)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replay || replayed.Trigger.ID != request.TriggerID || replayed.WorkflowRun != nil {
		t.Fatalf("restart replay = %#v", replayed)
	}

	next := scheduleTriggerRequest("trigger-next", "run-next", scheduleTriggerTestTime.Add(-time.Hour))
	next.ObservedAt = scheduleTriggerTestTime
	next.Misfired = true
	if result, err := store.CommitScheduleTrigger(context.Background(), next); err != nil {
		t.Fatal(err)
	} else if result.Trigger.Reason != "misfire-skipped" {
		t.Fatalf("catch-up result = %#v", result)
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Triggers) != 2 || len(records.WorkflowRuns) != 0 {
		t.Fatalf("catch-up persisted runs = %d, triggers = %d", len(records.WorkflowRuns), len(records.Triggers))
	}
}

func TestCommitScheduleTriggerFailurePoliciesAndManualRefusal(t *testing.T) {
	failed := domain.WorkflowRun{
		ID: "failed-run", WorkflowID: "workflow-1", ScheduleID: "schedule-1",
		Progress: domain.ProgressFailed, Revision: 2,
		CreatedAt:   scheduleTriggerTestTime.Add(-time.Hour),
		UpdatedAt:   scheduleTriggerTestTime,
		CompletedAt: timePointer(scheduleTriggerTestTime),
	}

	t.Run("hold", func(t *testing.T) {
		store := openScheduleTriggerStore(t, filepath.Join(t.TempDir(), "state.db"), domain.ScheduleFailureHold, &failed)
		defer store.Close()

		scheduled := scheduleTriggerRequest("trigger-held", "run-held", scheduleTriggerTestTime.Add(time.Hour))
		result, err := store.CommitScheduleTrigger(context.Background(), scheduled)
		if err != nil {
			t.Fatal(err)
		}
		if result.Trigger.State != domain.TriggerSuppressed || result.Trigger.Reason != "failure-hold" {
			t.Fatalf("held trigger = %#v", result)
		}

		manual := scheduleTriggerRequest("trigger-manual", "run-manual", scheduleTriggerTestTime.Add(2*time.Hour))
		manual.Source = domain.ScheduleTriggerManual
		if _, err := store.CommitScheduleTrigger(context.Background(), manual); !errors.Is(err, ErrScheduleFailureHeld) {
			t.Fatalf("manual hold error = %v", err)
		}
		records, err := store.LoadCoordinatorRecords(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(records.Triggers) != 1 || len(records.WorkflowRuns) != 1 {
			t.Fatalf("manual refusal persisted runs = %d, triggers = %d", len(records.WorkflowRuns), len(records.Triggers))
		}
	})

	t.Run("next cycle", func(t *testing.T) {
		store := openScheduleTriggerStore(t, filepath.Join(t.TempDir(), "state.db"), domain.ScheduleFailureNextCycle, &failed)
		defer store.Close()

		request := scheduleTriggerRequest("trigger-next", "run-next", scheduleTriggerTestTime.Add(time.Hour))
		result, err := store.CommitScheduleTrigger(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if result.Trigger.State != domain.TriggerAccepted || result.WorkflowRun == nil {
			t.Fatalf("next-cycle trigger = %#v", result)
		}

		manual := scheduleTriggerRequest("trigger-manual", "run-manual", scheduleTriggerTestTime.Add(2*time.Hour))
		manual.Source = domain.ScheduleTriggerManual
		if _, err := store.CommitScheduleTrigger(context.Background(), manual); !errors.Is(err, ErrManualScheduleRunOpen) {
			t.Fatalf("manual overlap error = %v", err)
		}
	})
}

func TestCommitScheduleTriggerAcceptsAndReplaysManualRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openScheduleTriggerStore(t, path, domain.ScheduleFailureNextCycle, nil)

	request := scheduleTriggerRequest("manual-request-1", "manual-run-1", scheduleTriggerTestTime)
	request.Source = domain.ScheduleTriggerManual
	accepted, err := store.CommitScheduleTrigger(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Trigger.State != domain.TriggerAccepted || accepted.WorkflowRun == nil {
		t.Fatalf("manual result = %#v", accepted)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	replay, err := store.CommitScheduleTrigger(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replay || replay.WorkflowRun == nil || replay.WorkflowRun.ID != "manual-run-1" {
		t.Fatalf("manual replay = %#v", replay)
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Triggers) != 1 || len(records.WorkflowRuns) != 1 {
		t.Fatalf("manual replay persisted runs = %d, triggers = %d", len(records.WorkflowRuns), len(records.Triggers))
	}
}

func TestScheduleTriggerHonorsAdminDelayNext(t *testing.T) {
	store := openScheduleTriggerStore(t, filepath.Join(t.TempDir(), "state.db"), domain.ScheduleFailureNextCycle, nil)
	defer store.Close()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	until := scheduleTriggerTestTime.Add(time.Hour)
	records.Schedules[0].NextNotBefore = &until
	records.Schedules[0].Revision++
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Schedules: records.Schedules}); err != nil {
		t.Fatal(err)
	}
	before := scheduleTriggerRequest("trigger-before-delay", "run-before-delay", until.Add(-time.Minute))
	result, err := store.CommitScheduleTrigger(context.Background(), before)
	if err != nil {
		t.Fatal(err)
	}
	if result.Trigger.State != domain.TriggerSuppressed || result.Trigger.Reason != "admin-delayed" || result.WorkflowRun != nil {
		t.Fatalf("before delay = %#v", result)
	}
	after := scheduleTriggerRequest("trigger-after-delay", "run-after-delay", until)
	result, err = store.CommitScheduleTrigger(context.Background(), after)
	if err != nil {
		t.Fatal(err)
	}
	if result.Trigger.State != domain.TriggerAccepted || result.WorkflowRun == nil {
		t.Fatalf("after delay = %#v", result)
	}
	records, err = store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if records.Schedules[0].NextNotBefore != nil {
		t.Fatalf("delay was not cleared: %#v", records.Schedules[0])
	}
}

func openScheduleTriggerStore(
	t *testing.T,
	path string,
	failurePolicy domain.ScheduleFailurePolicy,
	activeRun *domain.WorkflowRun,
) *Store {
	t.Helper()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := scheduleTriggerTestTime
	schedule := domain.Schedule{
		ID: "schedule-1", Name: "nightly", Version: 1, WorkflowID: "workflow-1",
		Expression: "0 2 * * *", Timezone: "UTC",
		Overlap: domain.ScheduleOverlapForbid, Misfire: domain.ScheduleMisfireSkip,
		AfterFailure: failurePolicy, Enabled: true, Revision: 1,
		CreatedAt: now, UpdatedAt: now,
	}
	records := CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-1", Version: 2, Name: "nightly-workflow",
			Class: domain.TaskClassRequired, CreatedAt: now,
		}},
		Schedules: []domain.Schedule{schedule},
		ScheduleTemplates: []domain.ScheduleTemplate{{
			ScheduleID: "schedule-1", Version: 1, WorkflowID: "workflow-1",
			Expression: "0 2 * * *", Timezone: "UTC",
			Overlap: domain.ScheduleOverlapForbid, Misfire: domain.ScheduleMisfireSkip,
			AfterFailure: failurePolicy, CreatedAt: now,
		}},
	}
	if activeRun != nil {
		schedule.ActiveRunID = activeRun.ID
		records.Schedules[0] = schedule
		records.WorkflowRuns = []domain.WorkflowRun{*activeRun}
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		store.Close()
		t.Fatalf("seed schedule store: %v", err)
	}
	return store
}

func scheduleTriggerRequest(triggerID, runID string, nominal time.Time) domain.ScheduleTriggerRequest {
	return domain.ScheduleTriggerRequest{
		ScheduleID: "schedule-1", TriggerID: triggerID, WorkflowRunID: runID,
		NominalAt: nominal, ObservedAt: nominal,
		Source: domain.ScheduleTriggerScheduled,
	}
}

func timePointer(value time.Time) *time.Time {
	return &value
}

func TestScheduleTriggerOccurrenceKey(t *testing.T) {
	scheduled := scheduleTriggerRequest("trigger", "run", scheduleTriggerTestTime)
	if got, want := scheduled.OccurrenceKey(), "schedule-1/"+scheduleTriggerTestTime.Format(time.RFC3339Nano); got != want {
		t.Fatalf("scheduled occurrence key = %q, want %q", got, want)
	}
	scheduled.Source = domain.ScheduleTriggerManual
	if got, want := scheduled.OccurrenceKey(), fmt.Sprintf("schedule-1/manual/%s", scheduled.TriggerID); got != want {
		t.Fatalf("manual occurrence key = %q, want %q", got, want)
	}
}
