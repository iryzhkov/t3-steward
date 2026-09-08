package report

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
)

func TestRisesAndDemand(t *testing.T) {
	loc := time.UTC
	key := fiveHour
	// Three Mondays 10:00-11:00 with interactive rises of 10, 20, 30, one
	// dispatched thread's rise that must not count, and a reset in between.
	var obs []domain.Observation
	mon := time.Date(2030, 1, 7, 10, 0, 0, 0, loc) // Monday
	for w := 0; w < 3; w++ {
		day := mon.AddDate(0, 0, 7*w)
		reset := day.Add(4 * time.Hour)
		obs = append(obs,
			domain.Observation{Key: key, ObservedAt: day, UsedPercent: 5, ResetsAt: &reset, EventID: "s" + string(rune('a'+w)), ThreadID: "me"},
			domain.Observation{Key: key, ObservedAt: day.Add(30 * time.Minute), UsedPercent: 5 + 10*float64(w+1), ResetsAt: &reset, EventID: "r" + string(rune('a'+w)), ThreadID: "me"},
			domain.Observation{Key: key, ObservedAt: day.Add(45 * time.Minute), UsedPercent: 5 + 10*float64(w+1) + 7, ResetsAt: &reset, EventID: "d" + string(rune('a'+w)), ThreadID: "bot"},
		)
	}
	dispatched := map[string]string{"bot": "task-1"}
	rises := Rises(obs, dispatched, 5*time.Minute)
	if len(rises) != 6 {
		t.Fatalf("rises = %d", len(rises))
	}
	interactive := 0
	for _, r := range rises {
		if r.Interactive {
			interactive++
			if r.ThreadID != "me" {
				t.Fatalf("interactive rise from %s", r.ThreadID)
			}
		}
	}
	if interactive != 3 {
		t.Fatalf("interactive rises = %d", interactive)
	}
	d := BuildDemand(key, rises, obs, loc)
	pct, n, ok := d.Forecast(Slot{Weekday: time.Monday, Hour: 10})
	if !ok || n != 3 || pct != 30 { // 80th percentile of {10,20,30}
		t.Fatalf("forecast = %v n=%d ok=%v", pct, n, ok)
	}
	// Tuesday 10:00 has no own samples: pooled weekday hour 10 is used;
	// zeros of the quiet weekdays are included, so the 80th percentile of
	// eight zeros and 10, 20, 30 is 10.
	pct, _, ok = d.Forecast(Slot{Weekday: time.Tuesday, Hour: 10})
	if !ok || pct != 10 {
		t.Fatalf("pooled forecast = %v ok=%v", pct, ok)
	}
	// Expected demand over Monday 10:30-11:30: half of hour 10 (30) plus
	// half of hour 11 (fallback 4/h, no samples in range for 11:00 on Mondays?
	// hour 11 exists in range with zero values, so it forecasts 0).
	exp, fb := d.Expected(mon.Add(30*time.Minute), mon.Add(90*time.Minute), loc, 4)
	if exp != 15 || fb != 0 {
		t.Fatalf("expected = %v fallback=%v", exp, fb)
	}
	// With no history at all every hour falls back.
	empty := Demand{Key: key, Values: map[Slot][]float64{}, Quantile: 0.8, MinSamples: 3}
	exp, fb = empty.Expected(mon, mon.Add(2*time.Hour), loc, 4)
	if exp != 8 || fb != 2*time.Hour {
		t.Fatalf("empty demand: exp=%v fb=%v", exp, fb)
	}
}
