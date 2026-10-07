package quotatelemetry

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func testReading(source, key string, at time.Time, used float64, resets *time.Time) Event {
	parsed := domain.ParseBucketKey(key)
	return Event{SchemaVersion: SchemaVersion, Kind: KindReading, At: at, Reading: &Reading{
		Source: source, BucketKey: key, ProviderInstanceID: parsed.ProviderInstanceID, LimitID: parsed.LimitID,
		Window: parsed.Window, UsedPercent: used, ResetsAt: resets, ObservedAt: at,
	}}
}

func testFinish(assignmentID string, start, finish time.Time) Event {
	return Event{SchemaVersion: SchemaVersion, Kind: KindFinish, At: finish, QuotaPoolID: "claude-main", Work: &Work{
		AttemptID: "attempt-" + assignmentID, AssignmentID: assignmentID, AssignmentEpoch: 1,
		Route:     Route{ProviderInstanceID: "claudeAgent", Model: "claude-opus-5-5", QuotaPoolID: "claude-main"},
		StartedAt: &start, FinishedAt: &finish,
	}}
}

func TestQuotaDeltaAttribution(t *testing.T) {
	now := testBase.Add(6 * time.Hour)
	start := testBase
	finish := testBase.Add(time.Hour)
	fiveReset := testBase.Add(4 * time.Hour)
	sevenReset := testBase.Add(48 * time.Hour)
	otherReset := testBase.Add(4*time.Hour + 2*time.Minute)
	readings := []Event{
		// five_hour: 10 before, 19 after; the newest reading at or before the
		// start wins over an older one, the earliest at or after the finish
		// over a later one.
		testReading("worker:w1", "claudeAgent/claude/five_hour", start.Add(-20*time.Minute), 7, &fiveReset),
		testReading("worker:w1", "claudeAgent/claude/five_hour", start.Add(-5*time.Minute), 10, &fiveReset),
		testReading("coordinator-host", "claudeAgent/claude/five_hour", finish.Add(2*time.Minute), 19, &fiveReset),
		testReading("worker:w1", "claudeAgent/claude/five_hour", finish.Add(20*time.Minute), 25, &fiveReset),
		// seven_day: usage fell, so a reset happened in between.
		testReading("worker:w1", "claudeAgent/claude/seven_day", start.Add(-2*time.Minute), 50, &sevenReset),
		testReading("worker:w1", "claudeAgent/claude/seven_day", finish.Add(time.Minute), 3, &sevenReset),
		// opus window: the reset time moved by more than a minute.
		testReading("worker:w1", "claudeAgent/claude/seven_day_opus", start.Add(-2*time.Minute), 20, &fiveReset),
		testReading("worker:w1", "claudeAgent/claude/seven_day_opus", finish.Add(time.Minute), 22, &otherReset),
		// no reading before: the only one is older than 30 minutes.
		testReading("worker:w1", "claudeAgent/claude/spend", start.Add(-31*time.Minute), 1, nil),
		testReading("worker:w1", "claudeAgent/claude/spend", finish.Add(time.Minute), 2, nil),
		// another provider is never this route's business.
		testReading("worker:w1", "codex/codex/primary", start.Add(-time.Minute), 40, nil),
		testReading("worker:w1", "codex/codex/primary", finish.Add(time.Minute), 60, nil),
	}
	concurrentEnd := start.Add(45 * time.Minute)
	oldEnd := start.Add(-2 * time.Hour)
	spans := []WorkSpan{
		{Key: "self:1", AttemptID: "attempt-self", Pool: "claude-main", Start: start, End: &finish},
		{Key: "x:1", AttemptID: "attempt-x", Pool: "claude-main", Start: start.Add(30 * time.Minute), End: &concurrentEnd},
		{Key: "y:1", AttemptID: "attempt-y", Pool: "claude-main", Start: start.Add(-time.Hour)}, // still running
		{Key: "z:1", AttemptID: "attempt-z", Pool: "codex-main", Start: start},                  // another pool
		{Key: "o:1", AttemptID: "attempt-o", Pool: "claude-main", Start: start.Add(-3 * time.Hour), End: &oldEnd},
	}
	self := testFinish("self", start, finish)
	deltas := ComputeDeltas(self, readings, spans, now)
	byWindow := map[string]Delta{}
	for _, delta := range deltas {
		byWindow[delta.Window] = delta
		if delta.Method != DeltaMethod {
			t.Fatalf("method = %q", delta.Method)
		}
	}
	if len(deltas) != 4 {
		t.Fatalf("deltas = %+v; want the four claudeAgent windows and no codex window", deltas)
	}
	five := byWindow["five_hour"]
	if five.DeltaPp == nil || *five.DeltaPp != 9 || five.Before.UsedPercent != 10 || five.After.UsedPercent != 19 ||
		five.After.Source != "coordinator-host" {
		t.Fatalf("five_hour = %+v; want +9 from 10 to 19", five)
	}
	// Shared: the whole window change, never a per-task share.
	if five.Attribution != "shared" || five.ConcurrentCount != 2 || len(five.Concurrent) != 2 ||
		five.Concurrent[0] != "attempt-x" || five.Concurrent[1] != "attempt-y" {
		t.Fatalf("five_hour attribution = %+v; want shared with attempt-x and attempt-y", five)
	}
	if seven := byWindow["seven_day"]; seven.DeltaPp != nil || seven.Absence != "reset-crossed" || seven.Attribution != "unavailable" {
		t.Fatalf("seven_day = %+v; want reset-crossed", seven)
	}
	if opus := byWindow["seven_day_opus"]; opus.DeltaPp != nil || opus.Absence != "reset-crossed" {
		t.Fatalf("seven_day_opus = %+v; want reset-crossed by a moved reset time", opus)
	}
	if spend := byWindow["spend"]; spend.DeltaPp != nil || spend.Absence != "no-reading-before" || spend.Before != nil {
		t.Fatalf("spend = %+v; want no-reading-before", spend)
	}

	// Exclusive: no other assignment of the pool overlaps the interval.
	exclusive := ComputeDeltas(self, readings, []WorkSpan{spans[0], spans[3], spans[4]}, now)
	for _, delta := range exclusive {
		if delta.Window == "five_hour" && (delta.Attribution != "exclusive" || delta.ConcurrentCount != 0 || len(delta.Concurrent) != 0) {
			t.Fatalf("exclusive five_hour = %+v", delta)
		}
	}

	// Pending, then no reading after.
	recent := testFinish("recent", now.Add(-time.Hour), now.Add(-10*time.Minute))
	late := []Event{testReading("worker:w1", "claudeAgent/claude/five_hour", now.Add(-70*time.Minute), 30, &fiveReset)}
	if got := ComputeDeltas(recent, late, nil, now); len(got) != 1 || got[0].Absence != "pending" || got[0].DeltaPp != nil {
		t.Fatalf("recent finish = %+v; want pending", got)
	}
	if got := ComputeDeltas(recent, late, nil, now.Add(time.Hour)); len(got) != 1 || got[0].Absence != "no-reading-after" {
		t.Fatalf("old finish = %+v; want no-reading-after", got)
	}

	// A finish with no start has no interval to measure.
	unstarted := testFinish("unstarted", start, finish)
	unstarted.Work.StartedAt = nil
	if got := ComputeDeltas(unstarted, readings, spans, now); len(got) != 0 {
		t.Fatalf("a finish without a start = %+v; want no deltas", got)
	}
}
