package quotatelemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestRecorderRecordsDispatchStartFinish(t *testing.T) {
	path := testStorePath(t)
	source := newFakeSource()
	clock := &fakeClock{now: testBase}
	recorder := newTestRecorder(t, path, source, clock)
	mustTick(t, recorder) // first start: coverage begins here

	noEffort := domain.ProviderRoute{ProviderInstanceID: "codex", Model: "gpt-6.1-sol", QuotaPoolID: "codex-main"}
	task, attempt, assignment := testWork("1", 1, "implement", opusRoute, domain.ExecutionRoleExecutor)
	source.addWork(task, attempt, assignment)
	source.appendAudit("assignment-offered", "assignment-1", "attempt-1", task.ID, 1, testBase.Add(time.Second))
	source.appendAudit("worker-snapshot-observed", "worker-1", "", "", 0, testBase.Add(2*time.Second))
	source.appendAudit("assignment-claimed", "assignment-1", "attempt-1", task.ID, 1, testBase.Add(41*time.Second))
	// Released before it finished, then the attempt is retried under a new
	// assignment of its own.
	reviewTask, reviewAttempt, reviewAssignment := testWork("2", 1, "review1", noEffort, domain.ExecutionRoleExecutor)
	source.addWork(reviewTask, reviewAttempt, reviewAssignment)
	source.appendAudit("assignment-offered", "assignment-2", "attempt-2", reviewTask.ID, 1, testBase.Add(3*time.Second))
	source.appendAudit("assignment-claimed", "assignment-2", "attempt-2", reviewTask.ID, 1, testBase.Add(5*time.Second))
	// A parked attempt holds its claimed assignment.
	parkedTask, parkedAttempt, parkedAssignment := testWork("4", 1, "gate", opusRoute, domain.ExecutionRoleExecutor)
	source.addWork(parkedTask, parkedAttempt, parkedAssignment)
	source.appendAudit("assignment-offered", "assignment-4", "attempt-4", parkedTask.ID, 1, testBase.Add(4*time.Second))
	source.appendAudit("assignment-claimed", "assignment-4", "attempt-4", parkedTask.ID, 1, testBase.Add(6*time.Second))
	clock.Advance(time.Minute)
	mustTick(t, recorder)

	events := allEvents(t, path, clock.Now())
	if got := len(eventsOfKind(events, KindDispatch)); got != 3 {
		t.Fatalf("dispatch events = %d; want 3", got)
	}
	starts := eventsOfKind(events, KindStart)
	if len(starts) != 3 {
		t.Fatalf("start events = %d; want 3", len(starts))
	}
	start := findWork(t, starts, "assignment-1")
	if start.Work.DispatchToStartMs == nil || *start.Work.DispatchToStartMs != 40000 {
		t.Fatalf("dispatchToStartMs = %v; want 40000", start.Work.DispatchToStartMs)
	}
	if start.Work.Route.Effort == nil || *start.Work.Route.Effort != "medium" || start.Work.Route.EffortAbsent ||
		start.Work.Route.QuotaPoolID != "claude-main" || start.QuotaPoolID != "claude-main" ||
		start.Work.Route.Model != "claude-opus-5-5" || start.Work.Route.ProviderInstanceID != "claudeAgent" {
		t.Fatalf("start route = %+v pool %q", start.Work.Route, start.QuotaPoolID)
	}
	if start.Work.TaskName != "implement" || start.Work.AttemptNumber != 1 || start.Work.WorkerID != "worker-1" ||
		start.Work.Project != "t3-steward" || start.Work.ExecutionRole != "executor" || start.Work.TaskType.Category != "implement" {
		t.Fatalf("start work = %+v", start.Work)
	}
	reviewStart := findWork(t, starts, "assignment-2")
	if reviewStart.Work.Route.Effort != nil || !reviewStart.Work.Route.EffortAbsent {
		t.Fatalf("a route without effort = %+v; want effort null and effortAbsent", reviewStart.Work.Route)
	}

	// The first attempt succeeds; the second assignment is released; the
	// parked attempt is waiting on an external condition.
	completed := testBase.Add(time.Hour)
	attempt.Progress = domain.ProgressSucceeded
	attempt.CompletedAt = &completed
	source.setAttempt(attempt)
	reviewAssignment.State = domain.AssignmentReleased
	reviewAssignment.UpdatedAt = testBase.Add(10 * time.Minute)
	source.setAssignment(reviewAssignment)
	parkedAttempt.Progress = domain.ProgressWaitingExternal
	parkedAttempt.Control = domain.ControlWaitingExternal
	source.setAttempt(parkedAttempt)
	retryTask, retryAttempt, retryAssignment := testWork("3", 2, "review1", noEffort, domain.ExecutionRoleExecutor)
	retryAttempt.TaskID = reviewTask.ID
	source.addWork(retryTask, retryAttempt, retryAssignment)
	source.appendAudit("assignment-offered", "assignment-3", "attempt-3", reviewTask.ID, 1, testBase.Add(11*time.Minute))
	clock.Advance(2 * time.Hour)
	mustTick(t, recorder)
	mustTick(t, recorder)

	events = allEvents(t, path, clock.Now())
	finishes := eventsOfKind(events, KindFinish)
	if len(finishes) != 2 {
		t.Fatalf("finish events = %d; want 2 (succeeded and released), the parked attempt stays open", len(finishes))
	}
	finish := findWork(t, finishes, "assignment-1")
	if finish.Work.Outcome != "succeeded" || !finish.At.Equal(completed) ||
		finish.Work.DurationMs == nil || *finish.Work.DurationMs != (time.Hour-41*time.Second).Milliseconds() {
		t.Fatalf("finish = %+v at %s; want succeeded at the attempt's completion with wall-clock duration", finish.Work, finish.At)
	}
	if finish.Work.Route.QuotaPoolID != "claude-main" || finish.Work.Route.Effort == nil {
		t.Fatalf("finish route = %+v", finish.Work.Route)
	}
	released := findWork(t, finishes, "assignment-2")
	if released.Work.Outcome != "released" || !released.At.Equal(testBase.Add(10*time.Minute)) ||
		released.Work.DurationMs == nil || *released.Work.DurationMs != (10*time.Minute-5*time.Second).Milliseconds() {
		t.Fatalf("released finish = %+v at %s", released.Work, released.At)
	}
	dispatches := eventsOfKind(events, KindDispatch)
	if len(dispatches) != 4 || findWork(t, dispatches, "assignment-3").Work.AttemptNumber != 2 {
		t.Fatalf("dispatches = %d; want the retry as a fourth assignment with attempt number 2", len(dispatches))
	}
	for _, event := range events {
		raw, _ := json.Marshal(event)
		if strings.Contains(string(raw), "secret") {
			t.Fatalf("an event carries a lease or dispatch token: %s", raw)
		}
	}
}

func TestRecorderReplayIsIdempotent(t *testing.T) {
	path := testStorePath(t)
	source := newFakeSource()
	clock := &fakeClock{now: testBase}
	// History before the recorder's first start is not backfilled.
	oldTask, oldAttempt, oldAssignment := testWork("0", 1, "implement", opusRoute, domain.ExecutionRoleExecutor)
	source.addWork(oldTask, oldAttempt, oldAssignment)
	source.appendAudit("assignment-offered", "assignment-0", "attempt-0", oldTask.ID, 1, testBase.Add(-time.Hour))
	source.appendAudit("assignment-claimed", "assignment-0", "attempt-0", oldTask.ID, 1, testBase.Add(-time.Hour))
	source.appendAudit("worker-snapshot-observed", "worker-1", "", "", 0, testBase.Add(-time.Hour))

	first := newTestRecorder(t, path, source, clock)
	mustTick(t, first)
	events := allEvents(t, path, clock.Now())
	started := eventsOfKind(events, KindRecorder)
	if len(started) != 1 || started[0].Recorder.State != "started" || started[0].Recorder.CoverageFrom == nil ||
		!started[0].Recorder.CoverageFrom.Equal(testBase) {
		t.Fatalf("first start = %+v; want one started event with coverageFrom", started)
	}
	if len(eventsOfKind(events, KindDispatch)) != 0 || len(eventsOfKind(events, KindStart)) != 0 {
		t.Fatal("the first start backfilled history")
	}
	if meta := readMeta(t, path); meta.AuditWatermark == nil || *meta.AuditWatermark != 3 {
		t.Fatalf("watermark = %v; want 3", meta.AuditWatermark)
	}

	task, attempt, assignment := testWork("1", 1, "implement", opusRoute, domain.ExecutionRoleExecutor)
	source.addWork(task, attempt, assignment)
	source.appendAudit("assignment-offered", "assignment-1", "attempt-1", task.ID, 1, testBase.Add(time.Second))
	// A commit that fails is a crash between ticks: nothing of the tick lands.
	first.failCommit = errInjected
	if err := first.Tick(context.Background()); err == nil {
		t.Fatal("a failed commit was reported as success")
	}
	if meta := readMeta(t, path); *meta.AuditWatermark != 3 {
		t.Fatalf("watermark moved to %d without its events", *meta.AuditWatermark)
	}
	first.Close()

	// A duplicate offered row for the same assignment epoch, then a restart.
	source.appendAudit("assignment-offered", "assignment-1", "attempt-1", task.ID, 1, testBase.Add(time.Second))
	source.appendAudit("assignment-claimed", "assignment-1", "attempt-1", task.ID, 1, testBase.Add(2*time.Second))
	clock.Advance(time.Minute)
	second := newTestRecorder(t, path, source, clock)
	mustTick(t, second)
	mustTick(t, second)
	second.Close()
	third := newTestRecorder(t, path, source, clock)
	mustTick(t, third)

	events = allEvents(t, path, clock.Now())
	if got := len(eventsOfKind(events, KindDispatch)); got != 1 {
		t.Fatalf("dispatch events = %d; want exactly 1", got)
	}
	if got := len(eventsOfKind(events, KindStart)); got != 1 {
		t.Fatalf("start events = %d; want exactly 1", got)
	}
	// A restart with a stored watermark is not a first start; the failed
	// commit before it is recorded as a gap by the restarted process.
	recorderEvents := eventsOfKind(events, KindRecorder)
	if len(recorderEvents) != 2 || recorderEvents[0].Recorder.State != RecorderStarted ||
		recorderEvents[1].Recorder.State != RecorderGap || recorderEvents[1].Recorder.FailedTicks != 1 {
		t.Fatalf("recorder events = %+v; want started, then the gap of the failed commit", recorderEvents)
	}
	if meta := readMeta(t, path); *meta.AuditWatermark != 6 {
		t.Fatalf("watermark = %d; want 6", *meta.AuditWatermark)
	}

	// A coordinator database restored from an older copy has a lower maximum:
	// the watermark resets to it and the gap is recorded.
	source.mu.Lock()
	source.audit = source.audit[:2]
	source.mu.Unlock()
	mustTick(t, third)
	events = allEvents(t, path, clock.Now())
	gaps := eventsOfKind(events, KindRecorder)
	if len(gaps) != 3 || gaps[2].Recorder.State != "gap" || gaps[2].Recorder.Reason == "" {
		t.Fatalf("recorder events after a restored database = %+v; want a gap", gaps)
	}
	if meta := readMeta(t, path); *meta.AuditWatermark != 2 {
		t.Fatalf("watermark = %d; want reset to 2", *meta.AuditWatermark)
	}
}

func TestRecorderRecordsDistinctReadings(t *testing.T) {
	path := testStorePath(t)
	source := newFakeSource()
	clock := &fakeClock{now: testBase}
	resets := testBase.Add(3 * time.Hour)
	workerKey := domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}
	source.snapshots = []domain.WorkerSnapshot{{
		WorkerID: "worker-1",
		QuotaObservations: []domain.WorkerQuotaObservation{
			{Key: workerKey, Phase: domain.PhaseNormal, UsedPercent: 10, Healthy: true, ObservedAt: testBase.Add(-time.Minute), LimitName: "Claude"},
			{Key: domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "seven_day"}, UsedPercent: 3},
		},
	}, {WorkerID: "worker-old"}}
	source.buckets = []domain.BucketState{{
		Key:   domain.BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: "primary"},
		Phase: domain.PhaseWarned, UsedPercent: 81.5, ResetsAt: &resets, ObservedAt: testBase.Add(-2 * time.Minute), Epoch: "e1",
	}}
	recorder := newTestRecorder(t, path, source, clock)
	for range 3 {
		mustTick(t, recorder)
		clock.Advance(30 * time.Second)
	}
	readings := eventsOfKind(allEvents(t, path, clock.Now()), KindReading)
	if len(readings) != 2 {
		t.Fatalf("readings after three unchanged ticks = %d; want 2", len(readings))
	}
	worker := findReading(t, readings, "worker:worker-1")
	if worker.Reading.ResetsAt != nil || worker.Reading.UsedPercent != 10 || worker.Reading.BucketKey != "claudeAgent/claude/five_hour" ||
		worker.Reading.Window != "five_hour" || !worker.At.Equal(testBase.Add(-time.Minute)) || worker.Reading.AgeSeconds != 60 {
		t.Fatalf("worker reading = %+v at %s", worker.Reading, worker.At)
	}
	raw, _ := json.Marshal(worker)
	if !strings.Contains(string(raw), `"resetsAt":null`) {
		t.Fatalf("a missing reset is not an explicit null: %s", raw)
	}
	host := findReading(t, readings, "coordinator-host")
	if host.Reading.ResetsAt == nil || !host.Reading.ResetsAt.Equal(resets) || host.Reading.Phase != "warned" || host.Reading.UsedPercent != 81.5 {
		t.Fatalf("coordinator-host reading = %+v", host.Reading)
	}
	if meta := readMeta(t, path); meta.SkippedReadings != 3 {
		t.Fatalf("skipped readings = %d; want the zero observedAt reading counted on each of 3 ticks", meta.SkippedReadings)
	}

	source.mu.Lock()
	source.snapshots[0].QuotaObservations[0].ObservedAt = testBase.Add(time.Minute)
	source.snapshots[0].QuotaObservations[0].UsedPercent = 12
	source.mu.Unlock()
	mustTick(t, recorder)
	mustTick(t, recorder)
	if readings := eventsOfKind(allEvents(t, path, clock.Now()), KindReading); len(readings) != 3 {
		t.Fatalf("readings after a new observation = %d; want 3", len(readings))
	}
}

func TestRecorderFailureIsCountedNeverFatal(t *testing.T) {
	path := testStorePath(t)
	source := newFakeSource()
	clock := &fakeClock{now: testBase}
	recorder := newTestRecorder(t, path, source, clock)
	mustTick(t, recorder)

	task, attempt, assignment := testWork("1", 1, "implement", opusRoute, domain.ExecutionRoleExecutor)
	source.addWork(task, attempt, assignment)
	source.appendAudit("assignment-offered", "assignment-1", "attempt-1", task.ID, 1, testBase.Add(time.Second))
	source.fail = errInjected
	clock.Advance(30 * time.Second)
	if err := recorder.Tick(context.Background()); err == nil || !strings.Contains(err.Error(), "injected") {
		t.Fatalf("failing source tick = %v", err)
	}
	source.fail = nil
	source.panics = true
	clock.Advance(30 * time.Second)
	if err := recorder.Tick(context.Background()); err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("panicking source tick = %v", err)
	}
	source.panics = false
	recorder.failCommit = fmt.Errorf("disk full")
	clock.Advance(30 * time.Second)
	if err := recorder.Tick(context.Background()); err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("failing write tick = %v", err)
	}
	stats := recorder.Stats()
	if stats.Failures != 3 || !strings.Contains(stats.LastError, "disk full") {
		t.Fatalf("stats after three failures = %+v", stats)
	}
	if meta := readMeta(t, path); meta.Failures != 3 || !strings.Contains(meta.LastError, "disk full") {
		t.Fatalf("persisted counters = %+v; want the failures written best-effort", meta)
	}

	recorder.failCommit = nil
	clock.Advance(30 * time.Second)
	mustTick(t, recorder)
	events := allEvents(t, path, clock.Now())
	var gap *Event
	for index := range events {
		if events[index].Kind == KindRecorder && events[index].Recorder.State == "gap" {
			gap = &events[index]
		}
	}
	if gap == nil || gap.Recorder.FailedTicks != 3 || gap.Recorder.From == nil || gap.Recorder.To == nil ||
		!gap.Recorder.From.Equal(testBase.Add(30*time.Second)) || !strings.Contains(gap.Recorder.LastError, "disk full") {
		t.Fatalf("gap event = %+v; want the failed span with 3 ticks and the last error", gap)
	}
	if len(eventsOfKind(events, KindDispatch)) != 1 {
		t.Fatal("the dispatch from the failed span was not recovered from the audit watermark")
	}
	meta := readMeta(t, path)
	if meta.Failures != 3 || meta.LastSuccessAt == nil || !meta.LastSuccessAt.Equal(clock.Now()) || meta.Ticks != 5 {
		t.Fatalf("meta after recovery = %+v", meta)
	}

	// An unopenable store disables that tick only.
	broken := &Recorder{StorePath: ":memory:", OpenSource: func(context.Context) (Source, error) { return source, nil }, Now: clock.Now}
	if err := broken.Tick(context.Background()); err == nil {
		t.Fatal("an in-memory state path recorded")
	}
	if broken.Stats().Failures != 1 {
		t.Fatalf("broken stats = %+v", broken.Stats())
	}
}

func TestCheckEventsFromVerificationAndGateReports(t *testing.T) {
	path := testStorePath(t)
	source := newFakeSource()
	clock := &fakeClock{now: testBase}
	recorder := newTestRecorder(t, path, source, clock)
	mustTick(t, recorder)

	task, attempt, assignment := testWork("1", 1, "implement", opusRoute, domain.ExecutionRoleExecutor)
	task.Verification = []string{"go test ./...", "git bundle verify unit.bundle"}
	task.Gate = &domain.TaskGate{Commands: []string{"make check-review"}}
	source.addWork(task, attempt, assignment)
	source.appendAudit("assignment-offered", "assignment-1", "attempt-1", task.ID, 1, testBase.Add(time.Second))
	source.appendAudit("assignment-claimed", "assignment-1", "attempt-1", task.ID, 1, testBase.Add(2*time.Second))
	mustTick(t, recorder)

	started := testBase.Add(time.Hour)
	longCommand := "echo " + strings.Repeat("x", 400)
	report := func(command string, exit int, duration time.Duration) []byte {
		raw, _ := json.Marshal(map[string]any{"command": command, "exitCode": exit, "output": "SECRETOUTPUT",
			"startedAt": started, "completedAt": started.Add(duration)})
		return raw
	}
	gate, _ := json.Marshal(map[string]any{"passed": true, "attempt": "attempt-1", "logArtifact": "gate-log-attempt-1",
		"commands": []map[string]any{
			{"command": "make lint", "exitCode": 0, "startedAt": started, "completedAt": started.Add(2 * time.Minute), "duration": int64(2 * time.Minute)},
			{"command": "make check-review", "exitCode": 2, "startedAt": started.Add(2 * time.Minute), "completedAt": started.Add(11 * time.Minute), "duration": int64(9 * time.Minute), "error": "SECRETERROR"},
		}})
	artifact := func(id string, kind domain.ArtifactKind, name, media string, content []byte) {
		source.artifacts = append(source.artifacts, domain.Artifact{ID: id, WorkflowRunID: "run-1", TaskID: task.ID, AttemptID: attempt.ID,
			Kind: kind, Name: name, MediaType: media, Size: int64(len(content))})
		source.content[id] = content
	}
	artifact("v1", domain.ArtifactVerification, "verification/001.json", "application/json", report("go test ./...", 0, 1200*time.Millisecond))
	artifact("v2", domain.ArtifactVerification, "verification/002.json", "application/json", report(longCommand, 1, 3*time.Second))
	artifact("v3", domain.ArtifactVerification, "verification/003.json", "application/json", []byte(`{"output":"x"}`))
	artifact("v4", domain.ArtifactVerification, "verification/004.json", "application/json", []byte(`{"command":"`+strings.Repeat("y", MaxReportBytes)+`","exitCode":0}`))
	artifact("g1", domain.ArtifactGate, "gate", "application/json", gate)
	artifact("g2", domain.ArtifactGate, "gate/log.txt", "text/plain", []byte("SECRETLOG"))
	completed := started.Add(12 * time.Minute)
	attempt.Progress = domain.ProgressFailed
	attempt.CompletedAt = &completed
	source.setAttempt(attempt)
	clock.Advance(2 * time.Hour)
	mustTick(t, recorder)
	mustTick(t, recorder)

	events := allEvents(t, path, clock.Now())
	checks := eventsOfKind(events, KindCheck)
	if len(checks) != 4 {
		t.Fatalf("check events = %d; want 4 (two verification commands and two gate commands)", len(checks))
	}
	byKey := map[string]Event{}
	for _, check := range checks {
		byKey[fmt.Sprintf("%s#%d", check.Check.Stage, check.Check.Index)] = check
		if check.Work == nil || check.Work.Route.QuotaPoolID != "claude-main" || check.Work.TaskType.Category != "implement" {
			t.Fatalf("check without the attempt's route and task type: %+v", check)
		}
	}
	first := byKey["verification#1"]
	if first.Check == nil || first.Check.ExitCode != 0 || first.Check.DurationMs == nil || *first.Check.DurationMs != 1200 ||
		first.Check.Command != "go test ./..." {
		t.Fatalf("verification #1 = %+v", first.Check)
	}
	second := byKey["verification#2"]
	if second.Check == nil || second.Check.ExitCode != 1 || len(second.Check.Command) != 256 || !second.Check.CommandTruncated {
		t.Fatalf("verification #2 = %+v; want exit 1 and the command cut to 256 bytes", second.Check)
	}
	gateCheck := byKey["gate#2"]
	if gateCheck.Check == nil || gateCheck.Check.ExitCode != 2 || gateCheck.Check.DurationMs == nil ||
		*gateCheck.Check.DurationMs != (9*time.Minute).Milliseconds() || gateCheck.Check.Command != "make check-review" {
		t.Fatalf("gate #2 = %+v", gateCheck.Check)
	}
	if _, found := byKey["gate#1"]; !found {
		t.Fatal("gate #1 missing")
	}
	for _, event := range events {
		raw, _ := json.Marshal(event)
		for _, secret := range []string{"SECRETOUTPUT", "SECRETLOG", "SECRETERROR"} {
			if strings.Contains(string(raw), secret) {
				t.Fatalf("an event stores %s: %s", secret, raw)
			}
		}
	}
	if slices.Contains(source.opened, "g2") {
		t.Fatal("the gate log was read")
	}
	if meta := readMeta(t, path); meta.SkippedChecks != 2 {
		t.Fatalf("skipped check reports = %d; want the malformed and the oversized report", meta.SkippedChecks)
	}
}

func findWork(t *testing.T, events []Event, assignmentID string) Event {
	t.Helper()
	for _, event := range events {
		if event.Work != nil && event.Work.AssignmentID == assignmentID {
			return event
		}
	}
	t.Fatalf("no event for %s among %d", assignmentID, len(events))
	return Event{}
}

func findReading(t *testing.T, events []Event, source string) Event {
	t.Helper()
	for _, event := range events {
		if event.Reading != nil && event.Reading.Source == source {
			return event
		}
	}
	t.Fatalf("no reading from %s", source)
	return Event{}
}
