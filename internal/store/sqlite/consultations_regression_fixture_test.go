package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
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

// TestConsultationsPrefeatureParkWakeMeasurements records a repeatable,
// in-process baseline over real SQLite park, settlement, and wake operations.
// Fixture creation and migration are deliberately outside every timed sample.
func TestConsultationsPrefeatureParkWakeMeasurements(t *testing.T) {
	const samples = 40
	type fixture struct {
		store   *Store
		attempt domain.Attempt
		now     time.Time
	}
	fixtures := make([]fixture, 0, samples)
	for i := 0; i < samples; i++ {
		store, attempt, now := taskWaitFixture(t)
		fixtures = append(fixtures, fixture{store: store, attempt: attempt, now: now})
	}

	ctx := context.Background()
	latencies := make([]int64, 0, samples)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i, f := range fixtures {
		start := time.Now()
		wait, err := f.store.RegisterTaskWait(
			ctx,
			taskWaitRegistration(f.attempt, fmt.Sprintf("prefeature-%02d", i), domain.WakeEach),
			f.now,
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, f.now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		wakes, err := f.store.WakeTaskWaits(ctx, f.now.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if len(wakes) != 1 {
			t.Fatalf("sample %d produced %d wakes", i, len(wakes))
		}
		latencies = append(latencies, time.Since(start).Nanoseconds())
	}
	runtime.ReadMemStats(&after)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var total int64
	for _, latency := range latencies {
		total += latency
	}
	load, _ := os.ReadFile("/proc/loadavg")
	receipt := map[string]any{
		"schema":                           "consultations-prefeature-park-wake-v1",
		"samples":                          samples,
		"operations_per_sample":            3,
		"timed_operations":                 samples * 3,
		"fixture_creation_timed":           false,
		"median_ns":                        latencies[len(latencies)/2],
		"p95_ns":                           latencies[(len(latencies)*95-1)/100],
		"throughput_operations_per_second": float64(samples*3) / (float64(total) / float64(time.Second)),
		"mallocs_delta":                    after.Mallocs - before.Mallocs,
		"total_alloc_bytes_delta":          after.TotalAlloc - before.TotalAlloc,
		"go_version":                       runtime.Version(),
		"gomaxprocs":                       runtime.GOMAXPROCS(0),
		"goroutines_after":                 runtime.NumGoroutine(),
		"host_loadavg":                     strings.TrimSpace(string(load)),
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(raw))
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
