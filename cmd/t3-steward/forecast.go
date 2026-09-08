package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/report"
)

// cmdForecast shows the interactive-demand map learned from history and
// the headroom the backlog gate would grant right now.
func cmdForecast(g globalFlags, f reportFlags) error {
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	now := time.Now()
	from := now.Add(-time.Duration(f.days) * 24 * time.Hour)
	ctx := context.Background()
	local, store, err := collect(ctx, cfg, from, now.Add(time.Hour), f.fromLogs, f.doImport)
	if err != nil {
		return err
	}
	defer store.Close()
	observations := local.Observations
	remotes := cfg.Report.Remotes
	if f.remotes != "" {
		remotes = strings.Split(f.remotes, ",")
	}
	if f.local {
		remotes = nil
	}
	for _, host := range remotes {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		data, err := fetchRemote(rctx, host, f.days, f.fromLogs)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: %v\n", err)
			continue
		}
		observations = mergeObservations(observations, data.Observations)
	}
	dispatched, err := store.DispatchedThreads(ctx)
	if err != nil {
		return err
	}
	rises := report.Rises(observations, dispatched, 5*time.Minute)
	states, err := store.ListBuckets(ctx)
	if err != nil {
		return err
	}
	stateByKey := map[domain.BucketKey]domain.BucketState{}
	for _, st := range states {
		stateByKey[st.Key] = st
	}
	keys := map[domain.BucketKey]bool{}
	for _, o := range observations {
		if f.bucket == "" || strings.Contains(o.Key.String(), f.bucket) {
			keys[o.Key] = true
		}
	}
	type out struct {
		Key      domain.BucketKey `json:"key"`
		Used     float64          `json:"usedPercent"`
		ResetsAt *time.Time       `json:"resetsAt"`
		Demand   float64          `json:"forecastDemandPercent"`
		Fallback string           `json:"fallbackSpan"`
		Headroom float64          `json:"headroomPercent"`
		Heatmap  [7][24]float64   `json:"heatmap"`
	}
	var results []out
	for key := range keys {
		d := report.BuildDemand(key, rises, observations, time.Local)
		d.Quantile = cfg.Backlog.Quantile
		d.MinSamples = cfg.Backlog.MinSamples
		st := stateByKey[key]
		var demand float64
		var fallback time.Duration
		if st.ResetsAt != nil && st.ResetsAt.After(now) {
			demand, fallback = d.Expected(now, *st.ResetsAt, time.Local, cfg.Backlog.FallbackPerHour)
		}
		headroom := 100 - cfg.Backlog.SafetyMargin - st.UsedPercent - demand
		if headroom < 0 {
			headroom = 0
		}
		r := out{Key: key, Used: st.UsedPercent, ResetsAt: st.ResetsAt, Demand: demand, Fallback: fallback.Round(time.Minute).String(), Headroom: headroom}
		hm := d.Heatmap()
		for wd := 0; wd < 7; wd++ {
			for h := 0; h < 24; h++ {
				r.Heatmap[wd][h] = hm[wd][h].Percent
			}
		}
		results = append(results, r)
		if f.asJSON {
			continue
		}
		fmt.Printf("\n== %s: interactive demand forecast (%.0fth percentile of %s to %s, local time)\n",
			key, d.Quantile*100, d.FirstDate.Format("2006-01-02"), d.LastDate.Format("2006-01-02"))
		fmt.Printf("   %-4s", "")
		for h := 0; h < 24; h++ {
			fmt.Printf("%3d", h)
		}
		fmt.Println()
		for wd := time.Sunday; wd <= time.Saturday; wd++ {
			fmt.Printf("   %-4s", wd.String()[:3])
			for h := 0; h < 24; h++ {
				c := hm[wd][h]
				switch {
				case c.Samples == 0:
					fmt.Printf("%3s", ".")
				case !c.OK:
					fmt.Printf("%3s", "?")
				default:
					fmt.Printf("%3.0f", c.Percent)
				}
			}
			fmt.Println()
		}
		fmt.Println("   (percent of the window per hour; '?' = fewer than", d.MinSamples, "past occurrences, '.' = none)")
		if st.ResetsAt != nil && st.ResetsAt.After(now) {
			fmt.Printf("   Now: %.0f%% used, resets %s (in %s). Forecast interactive demand until then: %.0f%%",
				st.UsedPercent, st.ResetsAt.Local().Format("15:04"), humanDuration(st.ResetsAt.Sub(now)), demand)
			if fallback > 0 {
				fmt.Printf(" (%s of it on the fallback rate)", fallback.Round(time.Minute))
			}
			fmt.Printf(".\n   Headroom for backlog work with a %.0f%% safety margin: %.0f%%.\n", cfg.Backlog.SafetyMargin, headroom)
		} else {
			fmt.Println("   No current window state for this bucket.")
		}
	}
	if f.asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(results)
	}
	return nil
}

func humanDuration(d time.Duration) string {
	d = d.Round(time.Minute)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	if h == 0 {
		return fmt.Sprintf("%dm", m)
	}
	return fmt.Sprintf("%dh%02dm", h, m)
}
