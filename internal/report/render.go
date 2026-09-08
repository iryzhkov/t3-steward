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
	if len(rep.Buckets) == 0 {
		fmt.Fprintln(w, "\nNo observations in this period.")
		return
	}
	for _, b := range rep.Buckets {
		fmt.Fprintf(w, "\n== %s: %.0f%% consumed over %d observations\n", b.Key, b.Total, b.Observations)
		fmt.Fprintf(w, "   %-9s %9s %8s %10s %12s %12s %10s\n", "", "consumed", "active-h", "%/active-h", "fresh-tokens", "%/1M-fresh", "%/USD")
		renderBand(w, "peak", b.Peak)
		renderBand(w, "off-peak", b.OffPeak)
		if b.Peak.FreshTokens > 0 && b.OffPeak.FreshTokens > 0 {
			ratio := b.Peak.PercentPerMillionFresh() / b.OffPeak.PercentPerMillionFresh()
			fmt.Fprintf(w, "   Per fresh token, peak hours cost %.2fx off-peak hours.\n", ratio)
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
	fmt.Fprintf(w, "   %-9s %8.0f%% %8d %10.1f %12s %12s %10s\n",
		name, b.Percent, b.ActiveHours, b.PercentPerActiveHour(), tokens(b.FreshTokens), ratio(b.PercentPerMillionFresh()), ratio(b.PercentPerUSD()))
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

func ratio(v float64) string {
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
