package quotatelemetry

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Regression tests for the findings of the first independent review.

// A finish is built from the open work as this tick will leave it: a start
// staged in the same tick as the finish is its start, so the finish keeps its
// start time, duration and deltas.
func TestFinishUsesStartStagedInTheSameTick(t *testing.T) {
	path := testStorePath(t)
	source := newFakeSource()
	clock := &fakeClock{now: testBase}
	recorder := newTestRecorder(t, path, source, clock)
	mustTick(t, recorder)
	task, attempt, assignment := testWork("probe", 1, "gate", opusRoute, domain.ExecutionRoleExecutor)
	source.addWork(task, attempt, assignment)
	dispatched := testBase.Add(time.Second)
	source.appendAudit("assignment-offered", assignment.ID, attempt.ID, task.ID, 1, dispatched)
	clock.Advance(30 * time.Second)
	mustTick(t, recorder)

	start := testBase.Add(31 * time.Second)
	end := testBase.Add(40 * time.Second)
	source.appendAudit("assignment-claimed", assignment.ID, attempt.ID, task.ID, 1, start)
	attempt.Progress = domain.ProgressSucceeded
	attempt.CompletedAt = &end
	source.setAttempt(attempt)
	clock.Advance(30 * time.Second)
	mustTick(t, recorder)
	mustTick(t, recorder)

	events := allEvents(t, path, clock.Now())
	finishes := eventsOfKind(events, KindFinish)
	if len(finishes) != 1 {
		t.Fatalf("finishes = %d; want 1", len(finishes))
	}
	work := finishes[0].Work
	if work.StartedAt == nil || !work.StartedAt.Equal(start) || work.DurationMs == nil || *work.DurationMs != 9000 {
		t.Fatalf("finish lost the start staged with it: startedAt=%v durationMs=%v; want %s and 9000",
			work.StartedAt, work.DurationMs, start)
	}
	if work.DispatchedAt == nil || !work.DispatchedAt.Equal(dispatched) {
		t.Fatalf("finish dispatchedAt = %v; want %s", work.DispatchedAt, dispatched)
	}
	if starts := eventsOfKind(events, KindStart); len(starts) != 1 || starts[0].Work.DispatchToStartMs == nil ||
		*starts[0].Work.DispatchToStartMs != 30000 {
		t.Fatalf("start events = %+v; want one with dispatchToStartMs 30000", starts)
	}
}

// Work offered, claimed and finished between two ticks is recorded complete
// by the one tick that sees all three.
func TestDispatchStartAndFinishInOneTick(t *testing.T) {
	path := testStorePath(t)
	source := newFakeSource()
	clock := &fakeClock{now: testBase}
	recorder := newTestRecorder(t, path, source, clock)
	mustTick(t, recorder)
	task, attempt, assignment := testWork("quick", 1, "gate", opusRoute, domain.ExecutionRoleExecutor)
	source.addWork(task, attempt, assignment)
	dispatched := testBase.Add(time.Second)
	start := testBase.Add(2 * time.Second)
	end := testBase.Add(12 * time.Second)
	source.appendAudit("assignment-offered", assignment.ID, attempt.ID, task.ID, 1, dispatched)
	source.appendAudit("assignment-claimed", assignment.ID, attempt.ID, task.ID, 1, start)
	attempt.Progress = domain.ProgressSucceeded
	attempt.CompletedAt = &end
	source.setAttempt(attempt)
	clock.Advance(30 * time.Second)
	mustTick(t, recorder)

	finishes := eventsOfKind(allEvents(t, path, clock.Now()), KindFinish)
	if len(finishes) != 1 {
		t.Fatalf("finishes after one tick = %d; want 1", len(finishes))
	}
	work := finishes[0].Work
	if work.StartedAt == nil || !work.StartedAt.Equal(start) || work.DurationMs == nil || *work.DurationMs != 10000 ||
		work.DispatchedAt == nil || !work.DispatchedAt.Equal(dispatched) {
		t.Fatalf("finish = %+v; want start %s, duration 10000 and dispatch %s", work, start, dispatched)
	}
}

// A finish recorded before its start was read, because the claim lay beyond
// the tick's audit batch, takes the start when a later tick reads it.
func TestFinishRecordedBeforeItsStartIsRepaired(t *testing.T) {
	path := testStorePath(t)
	source := newFakeSource()
	clock := &fakeClock{now: testBase}
	recorder := newTestRecorder(t, path, source, clock)
	mustTick(t, recorder)
	// The offer is the last row of one batch and the claim the first of the next.
	for index := range auditBatch - 1 {
		source.appendAudit("worker-heartbeat", fmt.Sprintf("noise-%d", index), "", "", 0, testBase)
	}
	task, attempt, assignment := testWork("split", 1, "gate", opusRoute, domain.ExecutionRoleExecutor)
	source.addWork(task, attempt, assignment)
	start, end := testBase.Add(2*time.Second), testBase.Add(12*time.Second)
	source.appendAudit("assignment-offered", assignment.ID, attempt.ID, task.ID, 1, testBase.Add(time.Second))
	source.appendAudit("assignment-claimed", assignment.ID, attempt.ID, task.ID, 1, start)
	attempt.Progress = domain.ProgressSucceeded
	attempt.CompletedAt = &end
	source.setAttempt(attempt)
	clock.Advance(30 * time.Second)
	mustTick(t, recorder)
	if finishes := eventsOfKind(allEvents(t, path, clock.Now()), KindFinish); len(finishes) != 1 || finishes[0].Work.StartedAt != nil {
		t.Fatalf("first batch finishes = %+v; want one finish without a start yet", finishes)
	}
	clock.Advance(30 * time.Second)
	mustTick(t, recorder)
	mustTick(t, recorder)

	events := allEvents(t, path, clock.Now())
	finishes, starts := eventsOfKind(events, KindFinish), eventsOfKind(events, KindStart)
	if len(finishes) != 1 || len(starts) != 1 {
		t.Fatalf("finishes = %d, starts = %d; want 1 and 1", len(finishes), len(starts))
	}
	work := finishes[0].Work
	if work.StartedAt == nil || !work.StartedAt.Equal(start) || work.DurationMs == nil || *work.DurationMs != 10000 ||
		work.FinishedAt == nil || !work.FinishedAt.Equal(end) || work.Outcome != string(domain.ProgressSucceeded) {
		t.Fatalf("finish = %+v; want it repaired with start %s and duration 10000", work, start)
	}
	if !finishes[0].At.Equal(end) {
		t.Fatalf("finish at = %s; want %s", finishes[0].At, end)
	}
}

// The newest reading of each bucket key is found through an index, not by
// scanning every retained reading on each read.
func TestLatestReadingPerKeyUsesTheIndex(t *testing.T) {
	store, err := OpenStore(testStorePath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rows, err := store.db.Query(`EXPLAIN QUERY PLAN `+latestReadingPerKeyQuery, "claudeAgent/", "claudeAgent0", 10)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	text := strings.Join(plan, "\n")
	if !strings.Contains(text, "events_kind_route") || strings.Contains(text, "TEMP B-TREE") || strings.Contains(text, "SCAN events") {
		t.Fatalf("query plan:\n%s\nwant every step through events_kind_route, with no scan or temporary tree", text)
	}
}

// Every retained bucket key of the route's provider instance has a delta,
// even when none of its readings falls in either 30-minute window.
func TestStaleBucketHasExplicitAbsence(t *testing.T) {
	start := testBase
	end := start.Add(time.Hour)
	readings := []Event{
		testReading("worker:w", "claudeAgent/claude/five_hour", start.Add(-time.Hour), 10, nil),
		testReading("worker:w", "claudeAgent/claude/seven_day", start.Add(-time.Minute), 20, nil),
		testReading("worker:w", "claudeAgent/claude/seven_day", end.Add(time.Minute), 21, nil),
		testReading("worker:w", "codex/codex/primary", start.Add(-time.Hour), 10, nil),
	}
	got := ComputeDeltas(testFinish("probe", start, end), readings, nil, end.Add(time.Hour))
	if len(got) != 2 {
		t.Fatalf("deltas = %+v; want the two claudeAgent keys", got)
	}
	stale := got[0]
	if stale.BucketKey != "claudeAgent/claude/five_hour" || stale.Window != "five_hour" || stale.DeltaPp != nil ||
		stale.Before != nil || stale.After != nil || stale.Absence != AbsenceNoReadingBefore ||
		stale.Attribution != AttributionUnavailable {
		t.Fatalf("stale key = %+v; want an explicit no-reading-before", stale)
	}
	if got[1].BucketKey != "claudeAgent/claude/seven_day" || got[1].DeltaPp == nil || *got[1].DeltaPp != 1 {
		t.Fatalf("measured key = %+v; want +1", got[1])
	}
}

// The read command reaches the same result: the store enumerates retained
// bucket keys independently of the two windows it loads readings from.
func TestQueryReportsStaleBucketAbsence(t *testing.T) {
	path := testStorePath(t)
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	start := testBase
	end := start.Add(time.Hour)
	stale := testReading("worker:w", "claudeAgent/claude/five_hour", start.Add(-3*time.Hour), 10, nil)
	stale.EventID = "reading:stale"
	before := testReading("worker:w", "claudeAgent/claude/seven_day", start.Add(-time.Minute), 20, nil)
	before.EventID = "reading:before"
	after := testReading("worker:w", "claudeAgent/claude/seven_day", end.Add(time.Minute), 22, nil)
	after.EventID = "reading:after"
	other := testReading("worker:w", "codex/codex/primary", start.Add(-time.Minute), 10, nil)
	other.EventID = "reading:other"
	finish := testFinish("probe", start, end)
	finish.EventID = "finish:probe:1"
	if err := store.Append(context.Background(), []Event{stale, before, after, other, finish}); err != nil {
		t.Fatal(err)
	}
	result, err := store.Query(context.Background(), Filter{Kinds: []string{KindFinish}}, end.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Events) != 1 {
		t.Fatalf("events = %+v; want the finish", result.Events)
	}
	deltas := result.Events[0].Deltas
	if len(deltas) != 2 || deltas[0].BucketKey != "claudeAgent/claude/five_hour" ||
		deltas[0].Absence != AbsenceNoReadingBefore || deltas[0].Attribution != AttributionUnavailable ||
		deltas[1].DeltaPp == nil || *deltas[1].DeltaPp != 2 {
		t.Fatalf("deltas = %+v; want five_hour no-reading-before and seven_day +2", deltas)
	}
}

// The error of a failed tick reaches the stored counters even when the store
// could not be written at the time: the next successful tick writes it.
func TestRecoveryPersistsLastErrorOfUnwritableStore(t *testing.T) {
	source := newFakeSource()
	clock := &fakeClock{now: testBase}
	recorder := newTestRecorder(t, ":memory:", source, clock)
	if err := recorder.Tick(context.Background()); err == nil {
		t.Fatal("tick on an unopenable store succeeded")
	}
	expected := recorder.Stats()
	recorder.StorePath = testStorePath(t)
	clock.Advance(30 * time.Second)
	mustTick(t, recorder)
	meta := readMeta(t, recorder.StorePath)
	if meta.Failures != 1 || meta.LastError != expected.LastError || meta.LastError == "" ||
		meta.LastErrorAt == nil || !meta.LastErrorAt.Equal(*expected.LastErrorAt) {
		t.Fatalf("recovered meta = failures %d, lastError %q, lastErrorAt %v; want 1, %q, %v",
			meta.Failures, meta.LastError, meta.LastErrorAt, expected.LastError, expected.LastErrorAt)
	}
}
