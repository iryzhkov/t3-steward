package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func collectorRecoveryConfig() config.Config {
	cfg := config.Default()
	cfg.Policy.DryRun = false
	cfg.Resume.Enabled = true
	cfg.Resume.ResetSettleDelay = config.Duration(2 * time.Minute)
	cfg.BacklogV2.Workers = map[string]config.V2Worker{
		"normandy": {Providers: map[string]config.V2Provider{
			"claudeAgent": {Models: []string{"claude-fable-5-1"}, QuotaPool: "claude-main"},
		}},
	}
	cfg.BacklogV2.QuotaPools = map[string]config.V2QuotaPool{
		"claude-main": {Provider: "claudeAgent", AccountID: "account-a", ObservationMaxAge: config.Duration(10 * time.Minute),
			Collector: &config.V2QuotaCollectorCoverage{
				SourceID: "native", Origins: []domain.QuotaCollectorOrigin{{WorkerID: "normandy", ProviderInstanceID: "claudeAgent"}},
				RequiredWindows: []domain.QuotaCollectorWindow{{Window: "five_hour"}, {Window: "seven_day"}},
			}},
	}
	return cfg
}

func collectorRecoveryStates(now time.Time) (domain.Thread, domain.BucketState, map[domain.BucketKey]domain.BucketState) {
	thread := domain.Thread{ID: "claude-thread", ProviderInstanceID: "claudeAgent", Model: "claude-fable-5-1"}
	stopped := now.Add(-12 * time.Minute)
	reset := now.Add(-6 * time.Minute)
	prior := domain.BucketState{
		Key:   domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "legacy", Window: "seven_day"},
		Phase: domain.PhaseStopped, UsedPercent: 97, ObservedAt: stopped, UpdatedAt: stopped, StoppedAt: &stopped,
		ResetsAt: &reset, Epoch: domain.EpochFor(&reset),
	}
	states := map[domain.BucketKey]domain.BucketState{prior.Key: prior}
	acquired := now.Add(-3 * time.Minute)
	nextReset := now.Add(time.Hour)
	for _, window := range []string{"five_hour", "seven_day"} {
		key := domain.BucketKey{ProviderInstanceID: "claudeAgent", AccountID: "account-a", LimitID: "native", Window: window}
		states[key] = domain.BucketState{Key: key, Phase: domain.PhaseNormal, Healthy: true, UsedPercent: 20,
			ObservedAt: acquired, UpdatedAt: acquired, ResetsAt: &nextReset, Epoch: domain.EpochFor(&nextReset),
			Provenance: &domain.QuotaProvenance{SourceID: "native", AcquisitionID: "batch-1", OriginWorkerID: "normandy", AcquiredAt: acquired}}
	}
	return thread, prior, states
}

func TestCoveredClaudeNeverProbesDuringCollectorOutage(t *testing.T) {
	cfg := collectorRecoveryConfig()
	now := time.Date(2030, 1, 1, 1, 0, 0, 0, time.UTC)
	thread, prior, _ := collectorRecoveryStates(now)
	// Coverage remains declared when this host has no collector credentials.
	if !CollectorInferenceDisabled(cfg, thread) {
		t.Fatal("disabled local producer erased configured coverage")
	}
	states := map[domain.BucketKey]domain.BucketState{prior.Key: prior}
	if ProbeWindowOpen(cfg, prior, now) {
		t.Fatal("covered route opened elapsed-reset probe")
	}
	probes := 0
	if ok, _ := BucketsRecovered(cfg, []domain.BucketKey{prior.Key}, *prior.StoppedAt, thread, states, now, func(domain.BucketState) bool { probes++; return true }); ok || probes != 0 {
		t.Fatalf("outage resumed or invoked inference callback: recovered=%v probes=%d", ok, probes)
	}
	for _, phase := range []domain.Phase{domain.PhaseStopped, domain.PhaseDraining} {
		prior.Phase = phase
		if _, required := GoverningPause(cfg, thread, []domain.BucketState{prior}, now); !required {
			t.Fatalf("%s restriction vanished at elapsed reset", phase)
		}
	}
	if ok, _ := ApplicableHealthy(cfg, thread, states, now); ok {
		t.Fatal("elapsed reset became healthy admission")
	}
}

func TestAnonymousClaudeRecoveryRequiresVerifiedPostStopAcquisition(t *testing.T) {
	cfg := collectorRecoveryConfig()
	now := time.Date(2030, 1, 1, 1, 0, 0, 0, time.UTC)
	thread, prior, states := collectorRecoveryStates(now)
	pool, ok := cfg.CollectorCoverageForThread(thread.ProviderInstanceID, thread.Model)
	if !ok {
		t.Fatal("fixture has no coverage")
	}
	if recovered, why := CollectorBucketsRecovered(cfg, pool, []domain.BucketKey{prior.Key}, *prior.StoppedAt, thread, states, now); !recovered {
		t.Fatalf("verified post-reset acquisition did not recover anonymous pause: %s", why)
	}
	if states[prior.Key].Key.AccountID != "" || states[prior.Key].Phase != domain.PhaseStopped {
		t.Fatal("recovery rewrote historical anonymous evidence")
	}
	cases := []struct {
		name   string
		mutate func(map[domain.BucketKey]domain.BucketState)
	}{
		{"missing base window", func(s map[domain.BucketKey]domain.BucketState) {
			for k := range s {
				if k.Window == "five_hour" {
					delete(s, k)
				}
			}
		}},
		{"wrong source", func(s map[domain.BucketKey]domain.BucketState) {
			for k, v := range s {
				if v.Provenance != nil {
					v.Provenance.SourceID = "unqualified"
					s[k] = v
				}
			}
		}},
		{"wrong origin", func(s map[domain.BucketKey]domain.BucketState) {
			for k, v := range s {
				if v.Provenance != nil {
					v.Provenance.OriginWorkerID = "other"
					s[k] = v
				}
			}
		}},
		{"mixed acquisition", func(s map[domain.BucketKey]domain.BucketState) {
			for k, v := range s {
				if k.Window == "five_hour" {
					v.Provenance.AcquisitionID = "batch-2"
					s[k] = v
				}
			}
		}},
		{"stale acquisition", func(s map[domain.BucketKey]domain.BucketState) {
			for k, v := range s {
				if v.Provenance != nil {
					v.ObservedAt = now.Add(-11 * time.Minute)
					v.Provenance.AcquiredAt = v.ObservedAt
					s[k] = v
				}
			}
		}},
		{"above resume threshold", func(s map[domain.BucketKey]domain.BucketState) {
			for k, v := range s {
				if v.Provenance != nil {
					v.UsedPercent = cfg.Resume.BelowPercent
					s[k] = v
				}
			}
		}},
		{"unsettled recovery", func(s map[domain.BucketKey]domain.BucketState) {
			for k, v := range s {
				if v.Provenance != nil {
					v.ObservedAt = now.Add(-time.Minute)
					v.Provenance.AcquiredAt = v.ObservedAt
					s[k] = v
				}
			}
		}},
		{"pre-stop acquisition", func(s map[domain.BucketKey]domain.BucketState) {
			for k, v := range s {
				if v.Provenance != nil {
					v.ObservedAt = *prior.StoppedAt
					v.Provenance.AcquiredAt = v.ObservedAt
					s[k] = v
				}
			}
		}},
		{"future acquisition", func(s map[domain.BucketKey]domain.BucketState) {
			for k, v := range s {
				if v.Provenance != nil {
					v.ObservedAt = now.Add(time.Second)
					v.Provenance.AcquiredAt = v.ObservedAt
					s[k] = v
				}
			}
		}},
		{"wrong epoch", func(s map[domain.BucketKey]domain.BucketState) {
			for k, v := range s {
				if v.Provenance != nil {
					v.Epoch = "wrong"
					s[k] = v
				}
			}
		}},
		{"unconfirmed historical reset", func(s map[domain.BucketKey]domain.BucketState) {
			v := s[prior.Key]
			reset := now.Add(time.Hour)
			v.ResetsAt = &reset
			s[prior.Key] = v
		}},
		{"different known historical account", func(s map[domain.BucketKey]domain.BucketState) {
			v := s[prior.Key]
			delete(s, prior.Key)
			v.Key.AccountID = "other-account"
			s[v.Key] = v
		}},
		{"newer local stop", func(s map[domain.BucketKey]domain.BucketState) {
			v := s[prior.Key]
			v.ObservedAt = now.Add(-time.Minute)
			v.UpdatedAt = v.ObservedAt
			s[prior.Key] = v
		}},
		{"tied local stop", func(s map[domain.BucketKey]domain.BucketState) {
			v := s[prior.Key]
			v.ObservedAt = now.Add(-3 * time.Minute)
			v.UpdatedAt = v.ObservedAt
			s[prior.Key] = v
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, p, s := collectorRecoveryStates(now)
			tc.mutate(s)
			stoppedKeys := []domain.BucketKey{p.Key}
			if tc.name == "different known historical account" {
				for k := range s {
					if k.AccountID == "other-account" {
						stoppedKeys = []domain.BucketKey{k}
					}
				}
			}
			if recovered, why := CollectorBucketsRecovered(cfg, pool, stoppedKeys, *p.StoppedAt, thread, s, now); recovered {
				t.Fatalf("invalid evidence authorized resume: %s", why)
			}
			if tc.name == "unsettled recovery" {
				if healthy, _ := ApplicableHealthy(cfg, thread, s, now); healthy {
					t.Fatal("applicable-health bypassed recovery settle delay")
				}
			}
		})
	}
}

func TestAmbiguousClaudeCoverageDisablesInferenceWithoutAuthorizingRecovery(t *testing.T) {
	cfg := collectorRecoveryConfig()
	cfg.BacklogV2.Workers["other"] = config.V2Worker{Providers: map[string]config.V2Provider{"claudeAgent": {Models: []string{"claude-fable-5-1"}, QuotaPool: "other-pool"}}}
	cfg.BacklogV2.QuotaPools["other-pool"] = cfg.BacklogV2.QuotaPools["claude-main"]
	now := time.Date(2030, 1, 1, 1, 0, 0, 0, time.UTC)
	thread, prior, states := collectorRecoveryStates(now)
	if !CollectorInferenceDisabled(cfg, thread) {
		t.Fatal("ambiguous declared coverage enabled inference")
	}
	if recovered, _ := BucketsRecovered(cfg, []domain.BucketKey{prior.Key}, *prior.StoppedAt, thread, states, now, func(domain.BucketState) bool { t.Fatal("ambiguous coverage invoked probe"); return true }); recovered {
		t.Fatal("ambiguous account projection authorized recovery")
	}
}

func TestWatchdogCoveredClaudeResumeWaitsForCollectorAndPreservesOldKey(t *testing.T) {
	h := newHarness(t, nil)
	cfg := collectorRecoveryConfig()
	h.cfg = cfg
	h.d.cfg = cfg
	thread, prior, states := collectorRecoveryStates(h.clock)
	h.fake.add(thread.ID, thread.ProviderInstanceID, thread.Model, false)
	intent := domain.ResumeIntent{ThreadID: thread.ID, ProviderInstanceID: thread.ProviderInstanceID, Model: thread.Model,
		StoppedAt: *prior.StoppedAt, Buckets: []domain.BucketKey{prior.Key}, Status: domain.ResumePending, UpdatedAt: h.clock}
	if err := h.store.SaveResumeIntent(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if err := h.store.SaveBucket(context.Background(), prior); err != nil {
		t.Fatal(err)
	}
	h.d.advanceResumes(context.Background(), []domain.Thread{thread}, []domain.BucketState{prior}, ThreadOwners{})
	if len(h.fake.resumes) != 0 {
		t.Fatal("watchdog resumed to probe collector outage")
	}
	all := make([]domain.BucketState, 0, len(states))
	for _, state := range states {
		all = append(all, state)
	}
	h.d.advanceResumes(context.Background(), []domain.Thread{thread}, all, ThreadOwners{})
	if len(h.fake.resumes) != 1 {
		t.Fatalf("verified collector recovery resumes=%v", h.fake.resumes)
	}
	saved, err := h.store.LoadBucket(context.Background(), prior.Key)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Key.AccountID != "" || saved.ProbedAt != nil || saved.Phase != domain.PhaseStopped {
		t.Fatalf("old evidence mutated: %+v", saved)
	}
}
