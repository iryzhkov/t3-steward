package wait

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// NextNodeWakeDeadline reports future actionable coalescing work from the last tick.
// Restart reconstructs it from persisted settlement, never from process start.
func (r *Runner) NextNodeWakeDeadline() (time.Time, bool) {
	return r.nextNodeWakeDeadline, !r.nextNodeWakeDeadline.IsZero()
}

func (r *Runner) coalesceNodes(waits []domain.NodeWait, host string, now time.Time) map[string][]domain.NodeWait {
	r.nextNodeWakeDeadline = time.Time{}
	groups := map[string][]domain.NodeWait{}
	if r.NodeCoalesceWindow <= 0 || r.NodeDryRun {
		return groups
	}
	for _, w := range waits {
		if w.Host != host || w.SettledAt == nil || w.SettledAt.After(now) || w.Request.Quota != nil ||
			(w.Request.Group != "" && w.Request.Wake == domain.WakeAll) || w.DeliveryPayload != "" ||
			(w.Delivery != "pending" && w.Delivery != "held" && w.Delivery != "offline" && w.Delivery != "busy") ||
			(w.DeliveryNextAttemptAt != nil && now.Before(*w.DeliveryNextAttemptAt)) {
			continue
		}
		groups[w.Request.ThreadID] = append(groups[w.Request.ThreadID], w)
	}
	for key, members := range groups {
		sort.Slice(members, func(i, j int) bool {
			if members[i].SettledAt.Equal(*members[j].SettledAt) {
				return members[i].Request.ID < members[j].Request.ID
			}
			return members[i].SettledAt.Before(*members[j].SettledAt)
		})
		groups[key] = members
		deadline := members[0].SettledAt.Add(r.NodeCoalesceWindow)
		if deadline.After(now) && (r.nextNodeWakeDeadline.IsZero() || deadline.Before(r.nextNodeWakeDeadline)) {
			r.nextNodeWakeDeadline = deadline
		}
	}
	return groups
}

func recordedNodeStateLine(w domain.NodeWait) string {
	if w.Observation == nil {
		return fmt.Sprintf("Run %q: recorded state unavailable.", w.Request.Target.RunID)
	}
	o := w.Observation
	run := o.Target.RunID
	if run == "" {
		run = w.Request.Target.RunID
	}
	line := fmt.Sprintf("Run %q: recorded state %q, outcome %q (exit %d): %s.", run, o.Progress, domain.NodeObservationOutcome(*o), o.ExitCode, summaryText(o.Reason, summaryHeadlineMax))
	return line
}

func coalescedNodeMessage(members []domain.NodeWait, summaries []*WakeSummary) string {
	var b strings.Builder
	leader := members[0]
	trailer := nodeTrailerWith(leader, summaries[0])
	if o := leader.Observation; o != nil && !commandSafeRunID(o.Target.RunID) {
		fields := domain.NodeTrailerFields(*o)
		delete(fields, "result")
		trailer = WakeTrailer(string(leader.Request.Kind()), string(domain.NodeObservationOutcome(*o)), leader.Request.ID,
			append(FieldsOf(fields), summaryTrailerFields(summaries[0])...)...)
	}
	b.WriteString(trailer)
	fmt.Fprintf(&b, " count=%d\n\nWaits finished (T3 steward): %d node wakes settled.\n", len(members), len(members))
	for i, w := range members {
		line := recordedNodeStateLine(w)
		if s := summaries[i]; s != nil && s.Unavailable == "" && s.Headline != "" {
			line = fmt.Sprintf("Run %q: %s", w.Request.Target.RunID, s.Headline)
		}
		b.WriteString(line)
		b.WriteString(strings.ReplaceAll(nodeWakeResult(w), "\n", " "))
		b.WriteString("\n")
	}
	b.WriteString(wakeGuidance)
	return b.String()
}
