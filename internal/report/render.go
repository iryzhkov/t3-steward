package report

import (
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

// Render writes a plain-text report.
func Render(w io.Writer, rep Report, loc *time.Location) {
	if loc == nil {
		loc = time.Local
	}
	fmt.Fprintf(w, "Quota consumption %s to %s (times in %s; peak = %s)\n",
		rep.From.In(loc).Format("2006-01-02"), rep.To.In(loc).Format("2006-01-02 15:04"), rep.To.In(loc).Format("MST"), rep.Peak.Label)
	if len(rep.Sources) > 0 {
		fmt.Fprintf(w, "Sources: %s\n", strings.Join(rep.Sources, ", "))
	}
	if len(rep.Buckets) == 0 {
		fmt.Fprintln(w, "\nNo observations in this period.")
		return
	}
	for _, b := range rep.Buckets {
		fmt.Fprintf(w, "\n== %s: %.0f%% consumed over %d readings", b.Key, b.Total, b.Observations)
		if b.Unattributed > 0 {
			fmt.Fprintf(w, ", %.0f%% of it outside T3 or without token data", b.Unattributed)
		}
		fmt.Fprintln(w)
		f := b.Fit
		if f.Fitted {
			fmt.Fprintf(w, "   Fitted quota cost per 1M tokens: input %s, cache write %s, cache read %s, output %s (R²=%.2f over %d hour bins)\n",
				weight(f.Weights.Input, f.Volume[0]), weight(f.Weights.CacheWrite, f.Volume[1]), weight(f.Weights.CacheRead, f.Volume[2]), weight(f.Weights.Output, f.Volume[3]), f.R2, f.Intervals)
		} else {
			fmt.Fprintf(w, "   Attribution uses list-price ratios: %s (R²=%.2f over %d hour bins)\n", f.Note, f.R2, f.Intervals)
		}
		fmt.Fprintf(w, "   %-9s %9s %8s %10s %12s %10s %10s\n", "", "consumed", "active-h", "%/active-h", "fresh-tokens", "est-cost", "actual/est")
		renderBand(w, "peak", b.Peak)
		renderBand(w, "off-peak", b.OffPeak)
		if b.Peak.EstimatedCost > 0 && b.OffPeak.EstimatedCost > 0 && b.Peak.Percent > 0 && b.OffPeak.Percent > 0 {
			ratio := (b.Peak.Percent / b.Peak.EstimatedCost) / (b.OffPeak.Percent / b.OffPeak.EstimatedCost)
			fmt.Fprintf(w, "   For the same tokens, peak hours cost %.2fx off-peak hours.\n", ratio)
		}
		if b.Peak.ActiveHours > 0 && b.OffPeak.ActiveHours > 0 {
			ratio := b.Peak.PercentPerActiveHour() / b.OffPeak.PercentPerActiveHour()
			fmt.Fprintf(w, "   Per active hour, peak hours consume %.2fx off-peak hours.\n", ratio)
		}
		fmt.Fprintf(w, "\n   By hour (weekdays)              By hour (weekends)\n")
		fmt.Fprintf(w, "   %-4s %8s %6s %7s      %-4s %8s %6s %7s\n", "hour", "consumed", "act-h", "%/act-h", "hour", "consumed", "act-h", "%/act-h")
		for h := 0; h < 24; h++ {
			wd, we := b.WeekdayHours[h], b.WeekendHours[h]
			if wd.ActiveHours == 0 && we.ActiveHours == 0 {
				continue
			}
			fmt.Fprintf(w, "   %02d   %s      %02d   %s\n", h, hourCells(wd), h, hourCells(we))
		}
		if len(b.ByModel) > 0 {
			fmt.Fprintf(w, "\n   By model: %s\n", joinShares(b.ByModel, 6))
		}
		if len(b.ByThread) > 0 {
			fmt.Fprintf(w, "   Top threads:\n")
			for i, s := range b.ByThread {
				if i >= 8 {
					break
				}
				fmt.Fprintf(w, "     %5.0f%%  %s\n", s.Percent, s.Name)
			}
		}
		if cw := b.CurrentWindow; cw != nil {
			reset := "no reset time"
			if cw.ResetsAt != nil {
				reset = "resets " + cw.ResetsAt.In(loc).Format("2006-01-02 15:04")
			}
			fmt.Fprintf(w, "   Latest window (%s): at %.0f%%; %.0f%% was already used at first sight", reset, cw.Percent, cw.StartPercent)
			if len(cw.ByModel) > 0 {
				fmt.Fprintf(w, ", since then %s", joinShares(cw.ByModel, 6))
			}
			fmt.Fprintln(w)
		}
	}
}

func renderBand(w io.Writer, name string, b Band) {
	ratio := math.NaN()
	if b.EstimatedCost > 0 {
		ratio = b.Percent / b.EstimatedCost
	}
	fmt.Fprintf(w, "   %-9s %8.0f%% %8d %10.1f %12s %9.0f%% %10s\n",
		name, b.Percent, b.ActiveHours, b.PercentPerActiveHour(), tokens(b.FreshTokens), b.EstimatedCost, fmtRatio(ratio))
}

func hourCells(b Band) string {
	if b.ActiveHours == 0 {
		return fmt.Sprintf("%8s %6s %7s", "-", "-", "-")
	}
	return fmt.Sprintf("%7.0f%% %6d %7.1f", b.Percent, b.ActiveHours, b.PercentPerActiveHour())
}

func tokens(n int64) string {
	switch {
	case n == 0:
		return "-"
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.0fk", float64(n)/1e3)
	default:
		return fmt.Sprint(n)
	}
}

// weight renders a fitted weight, or n/a when the token type had too
// little volume to be identifiable.
func weight(w, volumeMillions float64) string {
	if volumeMillions < 0.5 {
		return "n/a"
	}
	return fmt.Sprintf("%.2f%%", w)
}

func fmtRatio(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return "-"
	}
	return fmt.Sprintf("%.2f", v)
}

func joinShares(s []Share, limit int) string {
	parts := make([]string, 0, len(s))
	for i, x := range s {
		if i >= limit {
			parts = append(parts, fmt.Sprintf("+%d more", len(s)-limit))
			break
		}
		parts = append(parts, fmt.Sprintf("%s %.0f%%", x.Name, x.Percent))
	}
	return strings.Join(parts, ", ")
}
