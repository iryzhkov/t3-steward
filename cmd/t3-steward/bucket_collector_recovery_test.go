package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/daemon"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

func TestCoveredClaudeRearmOutputDoesNotPromiseResumeWithoutSourceRecovery(t *testing.T) {
	cfg := config.Default()
	cfg.BacklogV2.Workers = map[string]config.V2Worker{"normandy": {Providers: map[string]config.V2Provider{"claudeAgent": {Models: []string{"claude-fable-5-1"}, QuotaPool: "claude-main"}}}}
	cfg.BacklogV2.QuotaPools = map[string]config.V2QuotaPool{"claude-main": {Provider: "claudeAgent", AccountID: "account-a", ObservationMaxAge: config.Duration(10 * time.Minute),
		Collector: &config.V2QuotaCollectorCoverage{SourceID: "native", Origins: []domain.QuotaCollectorOrigin{{WorkerID: "normandy", ProviderInstanceID: "claudeAgent"}}, RequiredWindows: []domain.QuotaCollectorWindow{{Window: "five_hour"}, {Window: "seven_day"}}}}}
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2030, 1, 1, 1, 0, 0, 0, time.UTC)
	stopped := now.Add(-time.Hour)
	reset := now.Add(time.Hour)
	before := domain.BucketState{Key: bucketTestKey, Phase: domain.PhaseStopped, UsedPercent: 20, ObservedAt: stopped, StoppedAt: &stopped, ResetsAt: &reset, Epoch: domain.EpochFor(&reset)}
	if err := store.SaveBucket(context.Background(), before); err != nil {
		t.Fatal(err)
	}
	result, err := daemon.RearmBucket(context.Background(), cfg, store, daemon.RearmRequest{Key: before.Key, Actor: "test", Reason: "operator rearm"}, now)
	if err != nil {
		t.Fatal(err)
	}
	states, err := store.ListBuckets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result = qualifyBucketRearmRecovery(cfg, result, states, now)
	if result.ResumeEligible || result.ResumeBlockedBy == nil || result.ResumeBlockedBy.Reason == "" {
		t.Fatalf("phase-only rearm promised recovery: %+v", result)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		ResumeEligible  bool
		ResumeBlockedBy struct{ Reason string }
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ResumeEligible || decoded.ResumeBlockedBy.Reason == "" {
		t.Fatalf("JSON omitted covered-source restriction: %s", encoded)
	}
}
