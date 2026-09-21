package sqlite

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// TestConsultationsRegressionBaseline is the pre-feature differential fixture.
// It deliberately uses the real migrated SQLite store with fixed request IDs and
// the fixture clock. Generated wait IDs are deterministic hashes of request IDs;
// the comparison records semantic counts and transitions instead of normalizing
// away effects.
func TestConsultationsRegressionBaseline(t *testing.T) {
	type wakeSnapshot struct {
		WakesAfterFirst  int    `json:"wakes_after_first"`
		WakesAfterSecond int    `json:"wakes_after_second"`
		AttemptProgress  string `json:"attempt_progress"`
		AttemptControl   string `json:"attempt_control"`
		WaitCount        int    `json:"wait_count"`
		AssignmentCount  int    `json:"assignment_count"`
	}
	type baselineSnapshot struct {
		WakeEach wakeSnapshot `json:"wake_each"`
		WakeAll  wakeSnapshot `json:"wake_all"`
		Replay   struct {
			SameWaitIdentity bool `json:"same_wait_identity"`
			WaitCount        int  `json:"wait_count"`
		} `json:"replay"`
		Cancellation struct {
			Outcome         string `json:"outcome"`
			Wakes           int    `json:"wakes"`
			AttemptProgress string `json:"attempt_progress"`
		} `json:"cancellation"`
	}

	ctx := context.Background()
	runWake := func(t *testing.T, mode domain.WakeMode) wakeSnapshot {
		t.Helper()
		store, attempt, now := taskWaitFixture(t)
		first, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "fixture-first", mode), now)
		if err != nil {
			t.Fatal(err)
		}
		second, err := store.RegisterTaskWait(ctx, taskWaitRegistration(loadAttempt(t, store, attempt.ID), "fixture-second", mode), now)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.SettleTaskWait(ctx, first.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(1)); err != nil {
			t.Fatal(err)
		}
		firstWakes, err := store.WakeTaskWaits(ctx, now.Add(1))
		if err != nil {
			t.Fatal(err)
		}
		secondWakeCount := 0
		if mode == domain.WakeAll {
			if _, err := store.SettleTaskWait(ctx, second.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(2)); err != nil {
				t.Fatal(err)
			}
			secondWakes, err := store.WakeTaskWaits(ctx, now.Add(2))
			if err != nil {
				t.Fatal(err)
			}
			secondWakeCount = len(secondWakes)
		}
		current := loadAttempt(t, store, attempt.ID)
		waits, err := store.ListTaskWaits(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var assignments int
		if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM coordinator_assignments").Scan(&assignments); err != nil {
			t.Fatal(err)
		}
		return wakeSnapshot{
			WakesAfterFirst: len(firstWakes), WakesAfterSecond: secondWakeCount,
			AttemptProgress: string(current.Progress), AttemptControl: string(current.Control),
			WaitCount: len(waits), AssignmentCount: assignments,
		}
	}

	var got baselineSnapshot
	got.WakeEach = runWake(t, domain.WakeEach)
	got.WakeAll = runWake(t, domain.WakeAll)

	replayStore, replayAttempt, now := taskWaitFixture(t)
	request := taskWaitRegistration(replayAttempt, "fixture-replay", domain.WakeEach)
	first, err := replayStore.RegisterTaskWait(ctx, request, now)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := replayStore.RegisterTaskWait(ctx, request, now)
	if err != nil {
		t.Fatal(err)
	}
	replayWaits, err := replayStore.ListTaskWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got.Replay.SameWaitIdentity = first.ID == replayed.ID
	got.Replay.WaitCount = len(replayWaits)

	cancelStore, cancelAttempt, cancelNow := taskWaitFixture(t)
	cancelWait, err := cancelStore.RegisterTaskWait(ctx, taskWaitRegistration(cancelAttempt, "fixture-cancel", domain.WakeEach), cancelNow)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := cancelStore.CancelTaskWait(ctx, cancelWait.ID, cancelNow.Add(1))
	if err != nil {
		t.Fatal(err)
	}
	cancelWakes, err := cancelStore.WakeTaskWaits(ctx, cancelNow.Add(1))
	if err != nil {
		t.Fatal(err)
	}
	got.Cancellation.Outcome = string(cancelled.Result.Outcome)
	got.Cancellation.Wakes = len(cancelWakes)
	got.Cancellation.AttemptProgress = string(loadAttempt(t, cancelStore, cancelAttempt.ID).Progress)

	raw, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	want, err := os.ReadFile(filepath.Join("testdata", "consultations-regression-baseline.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != string(want) {
		t.Fatalf("pre-feature regression fixture changed (-want +got):\nwant:\n%s\ngot:\n%s", want, raw)
	}
}
