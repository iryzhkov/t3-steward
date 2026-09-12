package backlog

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type quotaBridgeStoreFake struct {
	buckets     []domain.BucketState
	admissions  []domain.QuotaAdmissionRecord
	transitions []domain.QuotaAdmissionTransition
}

func (s *quotaBridgeStoreFake) ListBuckets(context.Context) ([]domain.BucketState, error) {
	return append([]domain.BucketState(nil), s.buckets...), nil
}

func (s *quotaBridgeStoreFake) LoadQuotaAdmissions(context.Context) ([]domain.QuotaAdmissionRecord, error) {
	return append([]domain.QuotaAdmissionRecord(nil), s.admissions...), nil
}

func (s *quotaBridgeStoreFake) CommitQuotaAdmissionTransitions(_ context.Context, transitions []domain.QuotaAdmissionTransition) error {
	s.transitions = append(s.transitions, transitions...)
	for _, transition := range transitions {
		replaced := false
		for index := range s.admissions {
			if s.admissions[index].QuotaPoolID == transition.Record.QuotaPoolID {
				s.admissions[index] = transition.Record
				replaced = true
			}
		}
		if !replaced {
			s.admissions = append(s.admissions, transition.Record)
		}
	}
	return nil
}

func TestQuotaBridgeDeduplicatesSharedPoolAndFailsClosedWhenStale(t *testing.T) {
	now := time.Date(2026, 9, 10, 18, 0, 0, 0, time.UTC)
	key := admissionBucket("codex", "weekly")
	state := admissionBucketState(key, domain.PhaseNormal, true)
	state.ObservedAt = now.Add(-time.Minute)
	store := &quotaBridgeStoreFake{buckets: []domain.BucketState{state, state}}
	bridge := QuotaBridge{
		Store: store, MaxObservationAge: 5 * time.Minute, Now: func() time.Time { return now },
		Pools: []QuotaPoolBinding{{
			ID: "shared", Provider: "openai", ProviderInstanceIDs: []string{"codex", "codex"},
			MaxConcurrent: 2,
		}},
	}
	report, err := bridge.Reconcile(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Pools) != 1 || len(report.Pools[0].Buckets) != 1 ||
		report.Pools[0].Admission != domain.AdmissionOpen ||
		len(report.Derived) != 1 || len(report.Directives) != 0 {
		t.Fatalf("open bridge report = %#v", report)
	}
	if len(store.admissions) != 1 || store.admissions[0].Revision != 1 {
		t.Fatalf("open admissions = %#v", store.admissions)
	}

	bridge.Now = func() time.Time { return now.Add(10 * time.Minute) }
	stale, err := bridge.Reconcile(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if stale.Derived[0].Admission != domain.AdmissionClosed ||
		len(stale.Directives) != 1 ||
		!strings.Contains(stale.Derived[0].Reason, "exceeds maximum age") {
		t.Fatalf("stale bridge report = %#v", stale)
	}
	if len(store.admissions) != 1 || store.admissions[0].Revision != 2 ||
		store.admissions[0].Admission != domain.AdmissionClosed {
		t.Fatalf("stale admissions = %#v", store.admissions)
	}
}

// A window scoped to a model closes only the pools whose models contain
// the selector: Claude's Fable weekly limit at 96% stops Fable, while a
// pool serving Haiku stays open on the account-wide windows; an ignored
// window never governs admission; a pool with no model list keeps the
// conservative reading.
func TestQuotaBridgeModelScopedWindowGovernsOnlyMatchingPools(t *testing.T) {
	now := time.Date(2026, 9, 12, 19, 0, 0, 0, time.UTC)
	fiveHour := admissionBucketState(admissionBucket("claudeAgent", "five_hour"), domain.PhaseNormal, true)
	fable := admissionBucketState(admissionBucket("claudeAgent", "seven_day_overage_included"), domain.PhaseStopped, false)
	fable.ModelSelector = "fable"
	overage := admissionBucketState(admissionBucket("claudeAgent", "overage"), domain.PhaseStopped, false)
	for _, state := range []*domain.BucketState{&fiveHour, &fable, &overage} {
		state.ObservedAt = now.Add(-time.Minute)
	}
	store := &quotaBridgeStoreFake{buckets: []domain.BucketState{fiveHour, fable, overage}}
	cases := []struct {
		name   string
		models []string
		want   domain.AdmissionState
	}{
		{"haiku pool ignores the fable window", []string{"claude-haiku-4-5"}, domain.AdmissionOpen},
		{"fable pool closes on it", []string{"claude-haiku-4-5", "claude-fable-5-1"}, domain.AdmissionClosed},
		{"no model list stays conservative", nil, domain.AdmissionClosed},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			bridge := QuotaBridge{
				Store: &quotaBridgeStoreFake{buckets: store.buckets}, MaxObservationAge: 5 * time.Minute, Now: func() time.Time { return now },
				Pools: []QuotaPoolBinding{{
					ID: "claude", Provider: "claudeAgent", ProviderInstanceIDs: []string{"claudeAgent"}, MaxConcurrent: 2,
					Models: test.models, IgnoredWindows: []string{"overage"},
				}},
			}
			report, err := bridge.Reconcile(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if report.Pools[0].Admission != test.want {
				t.Fatalf("admission = %s (%s), want %s; buckets %v", report.Pools[0].Admission, report.Derived[0].Reason, test.want, report.Pools[0].Buckets)
			}
			for _, key := range report.Pools[0].Buckets {
				if key.Window == "overage" {
					t.Fatalf("ignored window governs the pool: %v", report.Pools[0].Buckets)
				}
			}
		})
	}
}

func TestQuotaBridgeBuildsNumericPlanningWindow(t *testing.T) {
	now := time.Date(2026, 9, 10, 18, 0, 0, 0, time.UTC)
	reset := now.Add(2 * time.Hour)
	exhausts := 30 * time.Minute
	key := admissionBucket("codex", "primary")
	state := admissionBucketState(key, domain.PhaseNormal, true)
	state.ObservedAt = now.Add(-time.Minute)
	state.UsedPercent = 25
	state.ResetsAt = &reset
	state.Epoch = domain.EpochFor(&reset)
	state.RatePerMinute = 0.2
	state.ExhaustsIn = &exhausts
	bridge := QuotaBridge{
		Store: &quotaBridgeStoreFake{buckets: []domain.BucketState{state}},
		Pools: []QuotaPoolBinding{{
			ID: "shared", Provider: "openai", ProviderInstanceIDs: []string{"codex"},
			MaxConcurrent: 2,
		}},
		MaxObservationAge: 5 * time.Minute, SafetyMargin: 4,
		FallbackForecastPerHour: 3, LongWindowCap: 80,
		SurplusHorizon: time.Hour, Now: func() time.Time { return now },
	}
	report, err := bridge.Reconcile(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Windows) != 1 {
		t.Fatalf("planning windows = %#v", report.Windows)
	}
	window := report.Windows[0]
	if window.QuotaPoolID != "shared" || window.WindowID != key.String() ||
		window.Admission != domain.AdmissionOpen || window.Capacity != 100 ||
		window.CurrentUsage != 25 || window.ForecastInteractiveUsage != 24 ||
		window.SafetyMargin != 4 || !window.ObservedAt.Equal(state.ObservedAt) ||
		!window.ResetsAt.Equal(reset) ||
		!window.SurplusStartsAt.Equal(reset.Add(-time.Hour)) ||
		!window.DrainAt.Equal(now.Add(exhausts)) {
		t.Fatalf("planning window = %#v", window)
	}
}

func TestQuotaBridgeClosesUnavailableConflictingAndFutureEvidence(t *testing.T) {
	now := time.Date(2026, 9, 10, 19, 0, 0, 0, time.UTC)
	key := admissionBucket("codex", "weekly")
	normal := admissionBucketState(key, domain.PhaseNormal, true)
	normal.ObservedAt = now
	conflict := normal
	conflict.Phase = domain.PhaseWarned
	conflict.Epoch = "different"
	store := &quotaBridgeStoreFake{buckets: []domain.BucketState{normal, conflict}}
	bridge := QuotaBridge{
		Store: store, MaxObservationAge: 5 * time.Minute, Now: func() time.Time { return now },
		Pools: []QuotaPoolBinding{{
			ID: "shared", Provider: "openai", ProviderInstanceIDs: []string{"codex"},
			MaxConcurrent: 1,
		}, {
			ID: "unavailable", Provider: "anthropic", ProviderInstanceIDs: []string{"claude"},
			MaxConcurrent: 1,
		}},
	}
	report, err := bridge.Reconcile(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Derived) != 2 ||
		report.Derived[0].Admission != domain.AdmissionClosed ||
		report.Derived[1].Admission != domain.AdmissionClosed {
		t.Fatalf("conflicting/unavailable report = %#v", report)
	}
	codes := map[string]bool{}
	for _, snapshot := range report.Derived {
		for _, issue := range snapshot.Issues {
			codes[issue.Code] = true
		}
	}
	if !codes[QuotaAdmissionIssueObservationConflict] || !codes[QuotaAdmissionIssueBucketMissing] {
		t.Fatalf("issue codes = %#v", codes)
	}

	future := normal
	future.ObservedAt = now.Add(time.Minute)
	store = &quotaBridgeStoreFake{buckets: []domain.BucketState{future}}
	bridge.Store = store
	bridge.Pools = bridge.Pools[:1]
	report, err = bridge.Reconcile(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if report.Derived[0].Admission != domain.AdmissionClosed ||
		!admissionIssueCodes(report.Derived[0].Issues)[QuotaAdmissionIssueObservationFuture] {
		t.Fatalf("future report = %#v", report)
	}
}

func TestQuotaBridgeReconcileStateUsesDurableAssignmentEstimates(t *testing.T) {
	now := time.Date(2026, 9, 10, 20, 0, 0, 0, time.UTC)
	key := admissionBucket("codex", "weekly")
	bucket := admissionBucketState(key, domain.PhaseNormal, true)
	bucket.ObservedAt = now
	store := &quotaBridgeStoreFake{buckets: []domain.BucketState{bucket}}
	bridge := QuotaBridge{
		Store: store, MaxObservationAge: 5 * time.Minute, Now: func() time.Time { return now },
		Pools: []QuotaPoolBinding{{
			ID: "shared", Provider: "openai", ProviderInstanceIDs: []string{"codex"},
			MaxConcurrent: 3,
		}},
	}
	input := quotaRecoveryFixture()
	input.QuotaPools = nil
	input.QuotaWindows = nil
	input.RouteEstimates = nil
	report, err := bridge.ReconcileState(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Pools) != 1 || report.Pools[0].ActiveAssignments != 2 ||
		len(report.Derived) != 1 ||
		report.Derived[0].Admission != domain.AdmissionRecovering ||
		report.Derived[0].PausedRequiredWorkRemainder != 35 ||
		len(report.Windows) != 1 ||
		report.Windows[0].ActiveConsumption != 13 ||
		report.Windows[0].PausedRequiredWorkRemainder != 35 ||
		report.Windows[0].CommittedReservations != 0 {
		t.Fatalf("durable-state report = %#v", report)
	}
}

func TestQuotaBridgeRejectsProviderInstanceInMultiplePools(t *testing.T) {
	bridge := QuotaBridge{
		Store: &quotaBridgeStoreFake{}, MaxObservationAge: time.Minute,
		Pools: []QuotaPoolBinding{
			{ID: "a", Provider: "openai", ProviderInstanceIDs: []string{"codex"}, MaxConcurrent: 1},
			{ID: "b", Provider: "openai", ProviderInstanceIDs: []string{"codex"}, MaxConcurrent: 1},
		},
	}
	if _, err := bridge.Reconcile(context.Background(), nil); err == nil ||
		!strings.Contains(err.Error(), "belongs to quota pools") {
		t.Fatalf("duplicate ownership error = %v", err)
	}
}
