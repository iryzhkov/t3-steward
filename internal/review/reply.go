package review

import (
	"fmt"
	"strings"
)

// ShortReply contains no full review bodies and stays inside wake transport limits.
func ShortReply(r Reply) string {
	var b strings.Builder
	fmt.Fprintf(&b, "round %s: %s\nblocking: %d; non-blocking: %d\n", r.Round, r.CombinedVerdict, r.Blocking, r.NonBlocking)
	for _, m := range r.Reviewers {
		verdict := m.Verdict
		if verdict == "" {
			verdict = m.State
		}
		fmt.Fprintf(&b, "%s: %s (%s); blocking: %d; non-blocking: %d\n", m.ID, verdict, m.Route, m.Blocking, m.NonBlocking)
		if m.Failure != "" {
			fmt.Fprintf(&b, "  %s\n", m.Failure)
		}
		for _, title := range m.BlockingTitles {
			if b.Len()+len(title) > 8000 {
				b.WriteString("  further blocking titles are in summary.json\n")
				break
			}
			fmt.Fprintf(&b, "  %s\n", title)
		}
		fmt.Fprintf(&b, "  %s\n  %s\n", m.ReviewPath, m.VerdictPath)
	}
	fmt.Fprintln(&b, r.SummaryPath)
	text := b.String()
	if len(text) > 16<<10 {
		return text[:12<<10] + "\nFurther reviewer details: " + r.SummaryPath
	}
	return text
}
