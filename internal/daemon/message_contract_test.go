package daemon

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/policy"
	"strings"
	"testing"
	"time"
)

func TestQuotaMessageContractDisabledControlHasNoEffects(t *testing.T) {
	h := newHarness(t, nil)
	h.d.ControlAllowed = false
	h.d.ControlReason = "synthetic disabled control"
	h.fake.add("a", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 91, reset, "synthetic-drain")
	if len(h.fake.warnings) != 0 || len(h.fake.stops) != 0 {
		t.Fatal("disabled control sent effects")
	}
	actions, err := h.store.RecentActions(context.Background(), 10)
	if err != nil || len(actions) == 0 {
		t.Fatal("disabled action not recorded", err)
	}
	if !strings.Contains(actions[0].Detail, "control disabled") {
		t.Fatal(actions)
	}
}
func TestMessageContractDrainMatchesPolicy(t *testing.T) {
	now := time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)
	reset := now.Add(72 * time.Hour)
	near := now.Add(time.Second)
	soon := time.Second
	runway := 100 * time.Hour
	for _, tc := range []struct {
		name  string
		used  float64
		eta   *time.Duration
		reset *time.Time
		stop  bool
	}{
		{"below-stop", 90, nil, &reset, false}, {"threshold", 99, nil, &reset, true}, {"imminent", 90, &soon, &reset, true}, {"near-reset", 99, nil, &near, false}, {"runway", 99, &runway, &reset, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := domain.BucketState{Key: domain.BucketKey{ProviderInstanceID: "codex", Window: "weekly"}, Phase: domain.PhaseDraining, UsedPercent: tc.used, DrainDeadline: &now, ResetsAt: tc.reset, ExhaustsIn: tc.eta, RatePerMinute: 1}
			decision := policy.New(policy.DefaultThresholds()).Tick(st, now)
			stopped := decision.State.Phase == domain.PhaseStopped
			if stopped != tc.stop {
				t.Fatalf("policy control: stopped=%v reason=%s", stopped, decision.Ignored)
			}
			text, err := renderMessage(config.DefaultDrainMessage, domain.QuotaSnapshot{Key: st.Key, UsedPercent: st.UsedPercent, ResetsAt: st.ResetsAt}, time.Minute, now)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("actual stop=%v prose=%s policy=%s", stopped, text, decision.Ignored)
			if strings.Contains(text, "will interrupt any") || !strings.Contains(text, "may interrupt") {
				t.Error("unconditional stop promise")
			}
			if note := exhaustionNote(st); strings.Contains(note, "provider ends the session") {
				t.Error("unverified provider guarantee", note)
			}
		})
	}
}
