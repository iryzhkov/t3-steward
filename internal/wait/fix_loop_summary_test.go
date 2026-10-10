package wait

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestFixLoopWakeReportsExhaustionAndVerdict(t *testing.T) {
	source := &fakeSummarySource{run: SummaryRun{
		ID: fixtureRun, Progress: domain.ProgressFailed,
		FixLoops: []domain.FixLoopSummary{{Name: "patch", Rounds: 3, MaxRounds: 3, FinalVerdict: "changes-requested", Exhausted: true, Escalation: "fix-loop-exhausted"}},
	}}
	summary := BuildNodeSummary(context.Background(), source, fixtureSinkWait())
	for _, text := range []string{summary.Text(), summary.nodeProse(new(int))} {
		for _, want := range []string{"rounds=3/3", "final verdict=changes-requested", "escalation=fix-loop-exhausted"} {
			if !strings.Contains(text, want) {
				t.Fatalf("missing %q in %s", want, text)
			}
		}
	}
	data, err := json.Marshal(summary)
	if err != nil || !strings.Contains(string(data), `"fixLoops"`) {
		t.Fatalf("JSON %s: %v", data, err)
	}
}

func TestFixLoopWakeRejectsUnsafeRemoteFields(t *testing.T) {
	source := &fakeSummarySource{run: SummaryRun{ID: fixtureRun,
		FixLoops: []domain.FixLoopSummary{
			{Name: "bad\nname", MaxRounds: 3},
			{Name: "oversize", Rounds: 21, MaxRounds: 21},
			{Name: "patch", Rounds: 1, MaxRounds: 3, FinalVerdict: "accept\ninjection", Exhausted: true, Escalation: "hostile\n"},
		},
	}}
	summary := BuildNodeSummary(context.Background(), source, fixtureSinkWait())
	if len(summary.FixLoops) != 1 || summary.FixLoops[0].FinalVerdict != "" || summary.FixLoops[0].Escalation != "" {
		t.Fatalf("unsafe summary: %+v", summary.FixLoops)
	}
}
