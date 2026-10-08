package wait

import (
	"fmt"
	"strings"
)

func (s WakeSummary) fixLoopText() string {
	var b strings.Builder
	for _, loop := range s.FixLoops {
		fmt.Fprintf(&b, "Fix loop %s: rounds=%d/%d final verdict=%s", loop.Name, loop.Rounds, loop.MaxRounds, orDashCell(loop.FinalVerdict))
		if loop.Exhausted {
			fmt.Fprintf(&b, " escalation=%s", loop.Escalation)
		}
		b.WriteByte('\n')
	}
	return b.String()
}
