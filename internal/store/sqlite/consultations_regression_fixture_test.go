package sqlite

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type consultationWaitReceipt struct {
	RequestID          string `json:"request_id"`
	RegisteredRevision int64  `json:"registered_revision"`
	Settled            bool   `json:"settled"`
	Woken              bool   `json:"woken"`
	Outcome            string `json:"outcome"`
	Delivery           string `json:"delivery"`
	DeliveryID         string `json:"delivery_id"`
	WakeRevision       int64  `json:"wake_revision"`
	Resumption         bool   `json:"resumption"`
}

func consultationWaitReceipts(waits []domain.TaskWait) []consultationWaitReceipt {
	sort.Slice(waits, func(i, j int) bool { return waits[i].RequestID < waits[j].RequestID })
	out := make([]consultationWaitReceipt, 0, len(waits))
	for _, wait := range waits {
		outcome := ""
		if wait.Result != nil {
			outcome = string(wait.Result.Outcome)
		}
		out = append(out, consultationWaitReceipt{
			RequestID: wait.RequestID, RegisteredRevision: wait.RegisteredRevision,
			Settled: wait.Settled(), Woken: wait.Woken(), Outcome: outcome,
			Delivery: wait.Delivery, DeliveryID: wait.DeliveryID,
			WakeRevision: wait.WakeRevision, Resumption: wait.Resumption,
		})
	}
	return out
}

// TestConsultationsRegressionBaseline is the pre-feature differential fixture.
// It uses the real migrated SQLite store with fixed request IDs and fixture clock.
func TestConsultationsRegressionBaseline(t *testing.T) {
	type wakeSnapshot struct {
		WakesAfterFirst  int                       `json:"wakes_after_first"`
		WakesAfterSecond int                       `json:"wakes_after_second"`
		AttemptProgress  string                    `json:"attempt_progress"`
		AttemptControl   string                    `json:"attempt_control"`
		AttemptRevision  int64                     `json:"attempt_revision"`
		Waits            []consultationWaitReceipt `json:"waits"`
		AssignmentCount  int                       `json:"assignment_count"`
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
			AttemptRevision int64  `json:"attempt_revision"`
			DeliveryID      string `json:"delivery_id"`
			WakeRevision    int64  `json:"wake_revision"`
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
		if _, err := store.SettleTaskWait(ctx, first.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		firstWakes, err := store.WakeTaskWaits(ctx, now.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.SettleTaskWait(ctx, second.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(2*time.Second)); err != nil {
			t.Fatal(err)
		}
		secondWakes, err := store.WakeTaskWaits(ctx, now.Add(2*time.Second))
		if err != nil {
			t.Fatal(err)
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
			WakesAfterFirst: len(firstWakes), WakesAfterSecond: len(secondWakes),
			AttemptProgress: string(current.Progress), AttemptControl: string(current.Control),
			AttemptRevision: current.Revision, Waits: consultationWaitReceipts(waits),
			AssignmentCount: assignments,
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
	cancelled, err := cancelStore.CancelTaskWait(ctx, cancelWait.ID, cancelNow.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	cancelWakes, err := cancelStore.WakeTaskWaits(ctx, cancelNow.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	cancelRecords, err := cancelStore.ListTaskWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancelCurrent := loadAttempt(t, cancelStore, cancelAttempt.ID)
	got.Cancellation.Outcome = string(cancelled.Result.Outcome)
	got.Cancellation.Wakes = len(cancelWakes)
	got.Cancellation.AttemptProgress = string(cancelCurrent.Progress)
	got.Cancellation.AttemptRevision = cancelCurrent.Revision
	got.Cancellation.DeliveryID = cancelRecords[0].DeliveryID
	got.Cancellation.WakeRevision = cancelRecords[0].WakeRevision

	raw, err := json.Marshal(got)
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

// BenchmarkConsultationsBaselineReconciliation measures the real SQLite no-work
// reconciliation seam used on every coordinator boundary. It reports individual
// operation p95 in addition to Go's aggregate ns/op and throughput.
func BenchmarkConsultationsBaselineReconciliation(b *testing.B) {
	store, err := OpenMigrated(filepath.Join(b.TempDir(), "state.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Date(2026, 9, 21, 8, 0, 0, 0, time.UTC)
	samples := make([]int64, b.N)
	b.ResetTimer()
	started := time.Now()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if _, err := store.WakeTaskWaits(ctx, now); err != nil {
			b.Fatal(err)
		}
		samples[i] = time.Since(start).Nanoseconds()
	}
	elapsed := time.Since(started)
	b.StopTimer()
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	if len(samples) != 0 {
		b.ReportMetric(float64(samples[(len(samples)*95-1)/100]), "p95-ns/op")
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
	}
}
