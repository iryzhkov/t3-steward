package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func quotaFixture(t *testing.T, store *Store, now time.Time, phase domain.Phase, used float64) (domain.BucketKey, time.Time) {
	t.Helper()
	key := domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "seven_day"}
	reset := now.Add(6 * time.Hour)
	ctx := context.Background()
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{QuotaPools: []domain.QuotaPool{
		{ID: "claude", Provider: "claudeAgent", ProviderInstanceIDs: []string{"claudeAgent"}, Buckets: []domain.BucketKey{fiveHourKey, key}, Admission: domain.AdmissionOpen, MaxConcurrent: 2},
		{ID: "codex", Provider: "codex", ProviderInstanceIDs: []string{"codex"}, Admission: domain.AdmissionOpen, MaxConcurrent: 1},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveBucket(ctx, domain.BucketState{Key: key, Phase: phase, UsedPercent: used, ResetsAt: &reset, ObservedAt: now.Add(-time.Minute), Epoch: domain.EpochFor(&reset)}); err != nil {
		t.Fatal(err)
	}
	// Claude declares a five-hour window too, and a quota wait is met only
	// when every declared window has a fresh reading.
	saveFiveHourReading(t, store, now.Add(-time.Minute))
	return key, reset
}

var fiveHourKey = domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}

func saveFiveHourReading(t *testing.T, store *Store, observed time.Time) {
	t.Helper()
	if err := store.SaveBucket(context.Background(), domain.BucketState{Key: fiveHourKey, Phase: domain.PhaseNormal, UsedPercent: 10, Healthy: true, ObservedAt: observed}); err != nil {
		t.Fatal(err)
	}
}

func quotaRegistration(attempt domain.Attempt, requestID string, condition domain.QuotaWaitCondition) domain.TaskWaitRegistration {
	registration := taskWaitRegistration(attempt, requestID, domain.WakeEach)
	registration.Kind = domain.WaitKindQuota
	registration.Condition = ""
	registration.Quota = &condition
	return registration
}

// A task-bound quota wait parks the attempt with no local row and is settled
// from the merged bucket observations: a fresher worker reading below the
// threshold settles it with pool=, phase= and percent= in the fields.
func TestTaskBoundQuotaWaitSettlesFromMergedObservations(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	key, _ := quotaFixture(t, store, now, domain.PhaseStopped, 95)
	below := 50.0
	registered, err := store.RegisterTaskWait(ctx, quotaRegistration(attempt, "req-quota", domain.QuotaWaitCondition{Pool: "claude", Below: &below}), now)
	if err != nil {
		t.Fatal(err)
	}
	if registered.Kind != domain.WaitKindQuota || registered.Quota == nil || registered.Condition != "quota claude below 50" {
		t.Fatalf("registered = %+v", registered)
	}
	if rows, err := store.ListWaits(ctx, ""); err != nil || len(rows) != 0 {
		t.Fatalf("a coordinator kind left a local row: %v %v", rows, err)
	}
	if err := store.SettleNodeWaits(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, _ := store.ListTaskWaits(ctx)
	if waits[0].Settled() {
		t.Fatalf("settled at 95%%: %+v", waits[0])
	}
	// A worker on the consuming host reports the window recovered.
	if err := store.SaveWorkerSnapshot(ctx, domain.WorkerSnapshot{
		WorkerID: "homelab", WorkerEpoch: "e1", CoordinatorEpoch: 1, Sequence: 1, Connected: true,
		Inventory:  domain.WorkerInventory{ID: "homelab"},
		ObservedAt: now.Add(2 * time.Minute), ValidUntil: now.Add(time.Hour),
		QuotaObservations: []domain.WorkerQuotaObservation{{Key: key, Phase: domain.PhaseNormal, UsedPercent: 30, Healthy: true, ObservedAt: now.Add(2 * time.Minute)}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SettleNodeWaits(ctx, now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, _ = store.ListTaskWaits(ctx)
	result := waits[0].Result
	if result == nil || result.Outcome != domain.TaskWaitMet {
		t.Fatalf("not settled from the worker's reading: %+v", waits[0])
	}
	if result.Fields["pool"] != "claude" || result.Fields["phase"] != "normal" || result.Fields["percent"] != "30" {
		t.Fatalf("fields = %v", result.Fields)
	}
}

// --phase normal and --reset settle on the phase and on the recorded reset
// time; an unknown pool is refused at registration and the refusal lists the
// pools; a condition that already holds is refused.
func TestTaskBoundQuotaWaitConditionsAndRefusals(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	key, reset := quotaFixture(t, store, now, domain.PhaseDraining, 91)

	_, err := store.RegisterTaskWait(ctx, quotaRegistration(attempt, "req-unknown", domain.QuotaWaitCondition{Pool: "nope", Reset: true}), now)
	if err == nil || !strings.Contains(err.Error(), "claude") || !strings.Contains(err.Error(), "codex") {
		t.Fatalf("an unknown pool was accepted or the pools were not listed: %v", err)
	}
	below := 95.0
	if _, err := store.RegisterTaskWait(ctx, quotaRegistration(attempt, "req-holds", domain.QuotaWaitCondition{Pool: "claude", Below: &below}), now); err == nil || !strings.Contains(err.Error(), "already") {
		t.Fatalf("a condition that already holds parked the attempt: %v", err)
	}

	registered, err := store.RegisterTaskWait(ctx, quotaRegistration(attempt, "req-reset", domain.QuotaWaitCondition{Pool: "claude", Reset: true}), now)
	if err != nil {
		t.Fatal(err)
	}
	if registered.Quota.ResetAt == nil || !registered.Quota.ResetAt.Equal(reset) {
		t.Fatalf("the reset time was not recorded at registration: %+v", registered.Quota)
	}
	if err := store.SettleNodeWaits(ctx, reset.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, _ := store.ListTaskWaits(ctx)
	if waits[0].Settled() {
		t.Fatal("settled before the reset")
	}
	if err := store.SettleNodeWaits(ctx, reset); err != nil {
		t.Fatal(err)
	}
	waits, _ = store.ListTaskWaits(ctx)
	if waits[0].Result == nil || waits[0].Result.Outcome != domain.TaskWaitMet {
		t.Fatalf("not settled at the reset: %+v", waits[0])
	}

	// The attempt resumes and parks again on the phase.
	if _, err := store.WakeTaskWaits(ctx, reset.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	resumed := loadAttempt(t, store, attempt.ID)
	phase, err := store.RegisterTaskWait(ctx, quotaRegistration(resumed, "req-phase", domain.QuotaWaitCondition{Pool: "claude", Phase: domain.PhaseNormal}), reset.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveBucket(ctx, domain.BucketState{Key: key, Phase: domain.PhaseNormal, UsedPercent: 12, ObservedAt: reset.Add(2 * time.Minute)}); err != nil {
		t.Fatal(err)
	}
	saveFiveHourReading(t, store, reset.Add(2*time.Minute))
	if err := store.SettleNodeWaits(ctx, reset.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, _ = store.ListTaskWaits(ctx)
	for _, w := range waits {
		if w.ID == phase.ID && (w.Result == nil || w.Result.Outcome != domain.TaskWaitMet || w.Result.Fields["phase"] != "normal") {
			t.Fatalf("phase normal not settled: %+v", w.Result)
		}
	}
}

// F3 at the store: a stale window that is present keeps a quota wait open, the
// observation says which window, and the threshold the store judges
// freshness by is the one SetQuotaStaleAfter sets.
func TestQuotaWaitStaysOpenOnAStaleWindow(t *testing.T) {
	ctx := context.Background()
	store, _, now := taskWaitFixture(t)
	quotaFixture(t, store, now, domain.PhaseNormal, 20)
	saveFiveHourReading(t, store, now.Add(-2*time.Hour))
	below := 50.0
	request := domain.NodeWaitRequest{ID: "nw-stale", ThreadID: "thread", Name: "claude below 50", Quota: &domain.QuotaWaitCondition{Pool: "claude", Below: &below}, Timeout: time.Hour}
	registered, err := store.RegisterNodeWait(ctx, request, "operator", "host", now)
	if err != nil {
		t.Fatal(err)
	}
	if registered.SettledAt != nil {
		t.Fatalf("a stale window met the wait at registration: %+v", registered.Observation)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	records, err := nodeStateRecordsTx(ctx, tx, store.quotaStaleAfter)
	_ = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	if observation, err := observeQuota(*request.Quota, records, now); err != nil || observation.ExitCode != 1 || !strings.Contains(observation.Reason, "stale five_hour") {
		t.Fatalf("the pending wait does not name the stale window: %+v %v", observation, err)
	}
	if err := store.SettleNodeWaits(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, _ := store.ListNodeWaits(ctx)
	if waits[0].SettledAt != nil {
		t.Fatalf("a stale window settled the wait: %+v", waits[0].Observation)
	}
	store.SetQuotaStaleAfter(3 * time.Hour)
	if err := store.SettleNodeWaits(ctx, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, _ = store.ListNodeWaits(ctx)
	if waits[0].Observation == nil || waits[0].Observation.Outcome != domain.TaskWaitMet {
		t.Fatalf("a three-hour threshold did not count a two-hour reading fresh: %+v", waits[0].Observation)
	}
}

// An interactive quota wait is a node-wait record with a quota condition and
// no target pin.
func TestInteractiveQuotaWait(t *testing.T) {
	ctx := context.Background()
	store, _, now := taskWaitFixture(t)
	key, _ := quotaFixture(t, store, now, domain.PhaseStopped, 97)
	below := 50.0
	request := domain.NodeWaitRequest{ID: "nw-quota", ThreadID: "thread", Name: "claude below 50", Quota: &domain.QuotaWaitCondition{Pool: "claude", Below: &below}, Timeout: time.Hour}
	registered, err := store.RegisterNodeWait(ctx, request, "operator", "host", now)
	if err != nil {
		t.Fatal(err)
	}
	if registered.SettledAt != nil || registered.Request.Kind() != domain.WaitKindQuota {
		t.Fatalf("registered = %+v", registered)
	}
	if err := store.SaveBucket(ctx, domain.BucketState{Key: key, Phase: domain.PhaseNormal, UsedPercent: 20, ObservedAt: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if err := store.SettleNodeWaits(ctx, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, _ := store.ListNodeWaits(ctx)
	if waits[0].Observation == nil || waits[0].Observation.Outcome != domain.TaskWaitMet || waits[0].Observation.Fields["percent"] != "20" {
		t.Fatalf("observation = %+v", waits[0].Observation)
	}
	if _, err := store.RegisterNodeWait(ctx, domain.NodeWaitRequest{ID: "nw-bad", ThreadID: "thread", Quota: &domain.QuotaWaitCondition{Pool: "nope", Reset: true}, Timeout: time.Hour}, "operator", "host", now); err == nil || !strings.Contains(err.Error(), "codex") {
		t.Fatalf("an unknown pool was accepted: %v", err)
	}
}
