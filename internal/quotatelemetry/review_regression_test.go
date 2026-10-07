package quotatelemetry

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Regression tests for the findings of the self-review swarm.

type malformedRowError struct{}

func (malformedRowError) Error() string   { return "decode assignment: invalid character" }
func (malformedRowError) Malformed() bool { return true }

// poisonSource reports one assignment row as undecodable.
type poisonSource struct{ *fakeSource }

func (p poisonSource) LoadAssignment(ctx context.Context, id string) (domain.Assignment, bool, error) {
	if id == "assignment-bad" {
		return domain.Assignment{}, false, malformedRowError{}
	}
	return p.fakeSource.LoadAssignment(ctx, id)
}

func TestRecorderSkipsMalformedCoordinatorRecord(t *testing.T) {
	path := testStorePath(t)
	source := newFakeSource()
	clock := &fakeClock{now: testBase}
	recorder := newTestRecorder(t, path, source, clock)
	recorder.OpenSource = func(context.Context) (Source, error) { return poisonSource{source}, nil }
	mustTick(t, recorder)
	source.appendAudit("assignment-offered", "assignment-bad", "attempt-bad", "task-x", 1, testBase.Add(time.Second))
	task, attempt, assignment := testWork("1", 1, "implement", opusRoute, domain.ExecutionRoleExecutor)
	source.addWork(task, attempt, assignment)
	source.appendAudit("assignment-offered", "assignment-1", "attempt-1", task.ID, 1, testBase.Add(2*time.Second))
	source.buckets = []domain.BucketState{{Key: domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "l", Window: "five_hour"},
		ObservedAt: testBase.Add(3 * time.Second)}}
	for range 3 {
		clock.Advance(30 * time.Second)
		mustTick(t, recorder)
	}
	events := allEvents(t, path, clock.Now())
	if len(eventsOfKind(events, KindReading)) != 1 || len(eventsOfKind(events, KindDispatch)) != 2 {
		t.Fatalf("readings %d dispatches %d; one malformed row must not stall recording",
			len(eventsOfKind(events, KindReading)), len(eventsOfKind(events, KindDispatch)))
	}
	if meta := readMeta(t, path); meta.SkippedRecords < 1 || *meta.AuditWatermark != 2 {
		t.Fatalf("meta = %+v; want the malformed row counted and the watermark past it", meta)
	}
}

func TestRecorderGapSurvivesRestart(t *testing.T) {
	path := testStorePath(t)
	source := newFakeSource()
	clock := &fakeClock{now: testBase}
	recorder := newTestRecorder(t, path, source, clock)
	mustTick(t, recorder)
	source.fail = errInjected
	for range 3 {
		clock.Advance(30 * time.Second)
		_ = recorder.Tick(context.Background())
	}
	recorder.Close()
	source.fail = nil
	clock.Advance(30 * time.Second)
	restarted := newTestRecorder(t, path, source, clock)
	mustTick(t, restarted)
	var gaps []Event
	for _, event := range eventsOfKind(allEvents(t, path, clock.Now()), KindRecorder) {
		if event.Recorder.State == RecorderGap {
			gaps = append(gaps, event)
		}
	}
	if len(gaps) != 1 || gaps[0].Recorder.FailedTicks != 3 || !gaps[0].Recorder.From.Equal(testBase.Add(30*time.Second)) {
		t.Fatalf("gaps after a restart in a failed span = %+v; want one gap of 3 ticks", gaps)
	}
	if meta := readMeta(t, path); meta.FailedTicks != 0 || meta.Failures != 3 {
		t.Fatalf("meta = %+v; want the span cleared and the failures kept", meta)
	}
}

func TestRecorderSkipsReadingsItCannotStore(t *testing.T) {
	path := testStorePath(t)
	source := newFakeSource()
	clock := &fakeClock{now: testBase}
	key := func(window string) domain.BucketKey {
		return domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: window}
	}
	source.buckets = []domain.BucketState{
		{Key: key("good"), UsedPercent: 5, ObservedAt: testBase},
		{Key: key("infinite"), UsedPercent: math.Inf(1), ObservedAt: testBase},
		{Key: key("nan"), UsedPercent: math.NaN(), ObservedAt: testBase},
		{Key: key("future"), UsedPercent: 5, ObservedAt: time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Key: key("ancient"), UsedPercent: 5, ObservedAt: time.Date(1830, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Key: key(strings.Repeat("w", 300)), UsedPercent: 5, ObservedAt: testBase},
	}
	recorder := newTestRecorder(t, path, source, clock)
	mustTick(t, recorder)
	readings := eventsOfKind(allEvents(t, path, clock.Now()), KindReading)
	if len(readings) != 1 || readings[0].Reading.Window != "good" {
		t.Fatalf("readings = %+v; want only the storable one", readings)
	}
	if meta := readMeta(t, path); meta.SkippedReadings != 5 {
		t.Fatalf("skipped readings = %d; want 5", meta.SkippedReadings)
	}
}

func TestRecorderSkipsOverlongIdentities(t *testing.T) {
	path := testStorePath(t)
	source := newFakeSource()
	clock := &fakeClock{now: testBase}
	recorder := newTestRecorder(t, path, source, clock)
	mustTick(t, recorder)
	long := strings.Repeat("\U0001F600", 3000)
	source.appendAudit("assignment-offered", long, "attempt-x", "task-x", 1, testBase.Add(time.Second))
	mustTick(t, recorder)
	if got := eventsOfKind(allEvents(t, path, clock.Now()), KindDispatch); len(got) != 0 {
		t.Fatalf("an over-long id was recorded: %d events", len(got))
	}
	if meta := readMeta(t, path); meta.SkippedRecords != 1 {
		t.Fatalf("skipped records = %d; want 1", meta.SkippedRecords)
	}
}

var sonnetRoute = domain.ProviderRoute{ProviderInstanceID: "claudeAgent", Model: "claude-sonnet-5-5",
	Options: map[string]string{"effort": "low"}, QuotaPoolID: "claude-alt"}

func TestRecorderEarlierEpochKeepsItsOwnRoute(t *testing.T) {
	for _, frozen := range []bool{true, false} {
		t.Run(map[bool]string{true: "frozen binding", false: "no binding"}[frozen], func(t *testing.T) {
			path := testStorePath(t)
			source := newFakeSource()
			clock := &fakeClock{now: testBase}
			recorder := newTestRecorder(t, path, source, clock)
			mustTick(t, recorder)
			task, attempt, assignment := testWork("1", 1, "implement", opusRoute, domain.ExecutionRoleExecutor)
			source.addWork(task, attempt, assignment)
			if frozen {
				source.frozen[workKey("assignment-1", 1)] = assignment
			}
			source.appendAudit("assignment-offered", "assignment-1", "attempt-1", task.ID, 1, testBase.Add(time.Second))
			source.appendAudit("assignment-claimed", "assignment-1", "attempt-1", task.ID, 1, testBase.Add(2*time.Second))
			// Released and offered again to another worker and route, all
			// before the recorder's next tick.
			assignment.Epoch, assignment.WorkerID, assignment.Route = 2, "worker-2", sonnetRoute
			assignment.State, assignment.UpdatedAt = domain.AssignmentCompleted, testBase.Add(time.Hour)
			source.setAssignment(assignment)
			source.appendAudit("assignment-offered", "assignment-1", "attempt-1", task.ID, 2, testBase.Add(15*time.Second))
			source.appendAudit("assignment-claimed", "assignment-1", "attempt-1", task.ID, 2, testBase.Add(20*time.Second))
			completed := testBase.Add(time.Hour)
			attempt.Progress, attempt.CompletedAt = domain.ProgressSucceeded, &completed
			source.setAttempt(attempt)
			source.artifacts = append(source.artifacts, domain.Artifact{ID: "v1", TaskID: task.ID, AttemptID: "attempt-1",
				Kind: domain.ArtifactVerification, Name: "verification/001.json", Size: 40})
			source.content["v1"] = []byte(`{"command":"go test ./...","exitCode":0}`)
			clock.Advance(2 * time.Hour)
			mustTick(t, recorder)
			mustTick(t, recorder)

			events := allEvents(t, path, clock.Now())
			for _, event := range eventsOfKind(events, KindCheck) {
				if event.Work.AssignmentEpoch != 2 || event.Work.WorkerID != "worker-2" {
					t.Fatalf("check filed under epoch %d worker %q; want the epoch that ran to the end", event.Work.AssignmentEpoch, event.Work.WorkerID)
				}
			}
			if len(eventsOfKind(events, KindCheck)) != 1 {
				t.Fatalf("checks = %d; want 1", len(eventsOfKind(events, KindCheck)))
			}
			for _, event := range events {
				if event.Work == nil || event.Work.AssignmentEpoch != 1 {
					continue
				}
				if frozen && (event.Work.WorkerID != "worker-1" || event.QuotaPoolID != "claude-main" || event.Work.RouteUnknown) {
					t.Fatalf("epoch-1 %s = worker %q pool %q; want the frozen epoch's own", event.Kind, event.Work.WorkerID, event.QuotaPoolID)
				}
				if !frozen && (event.Work.WorkerID != "" || event.Work.Route.Model != "" || !event.Work.RouteUnknown) {
					t.Fatalf("epoch-1 %s = %+v; want the route recorded as unknown, not the later epoch's", event.Kind, event.Work)
				}
				if event.Kind == KindFinish && (event.Work.Outcome != "superseded" || !event.At.Equal(testBase.Add(15*time.Second))) {
					t.Fatalf("epoch-1 finish = %s at %s; want superseded at the epoch-2 offer", event.Work.Outcome, event.At)
				}
			}
		})
	}
}

func TestQuotaDeltaIgnoresOwnEarlierEpoch(t *testing.T) {
	start, end, released := testBase.Add(20*time.Minute), testBase.Add(80*time.Minute), testBase.Add(15*time.Minute)
	work := &Work{AssignmentID: "assignment-1", AssignmentEpoch: 2, AttemptID: "attempt-1",
		Route: Route{ProviderInstanceID: "claudeAgent", QuotaPoolID: "claude-main"}, StartedAt: &start, FinishedAt: &end}
	readings := []Event{
		testReading("worker:w1", "claudeAgent/claude/five_hour", testBase.Add(10*time.Minute), 10, nil),
		testReading("worker:w1", "claudeAgent/claude/five_hour", end.Add(time.Minute), 19, nil),
	}
	spans := []WorkSpan{
		{Key: "assignment-1:1", AttemptID: "attempt-1", Pool: "claude-main", Start: testBase, End: &released},
		{Key: "assignment-1:2", AttemptID: "attempt-1", Pool: "claude-main", Start: start, End: &end},
	}
	deltas := ComputeDeltas(Event{Kind: KindFinish, Work: work}, readings, spans, end.Add(time.Hour))
	if len(deltas) != 1 || deltas[0].Attribution != AttributionExclusive {
		t.Fatalf("deltas = %+v; the attempt's own earlier epoch is not concurrent work", deltas)
	}
}

func TestFinishBeforeStartHasNoDuration(t *testing.T) {
	path := testStorePath(t)
	source := newFakeSource()
	clock := &fakeClock{now: testBase}
	recorder := newTestRecorder(t, path, source, clock)
	mustTick(t, recorder)
	task, attempt, assignment := testWork("1", 1, "implement", opusRoute, domain.ExecutionRoleExecutor)
	source.addWork(task, attempt, assignment)
	source.appendAudit("assignment-offered", "assignment-1", "attempt-1", task.ID, 1, testBase.Add(2*time.Hour))
	source.appendAudit("assignment-claimed", "assignment-1", "attempt-1", task.ID, 1, testBase.Add(time.Hour))
	mustTick(t, recorder)
	completed := testBase.Add(time.Minute)
	attempt.Progress, attempt.CompletedAt = domain.ProgressSucceeded, &completed
	source.setAttempt(attempt)
	mustTick(t, recorder)
	events := allEvents(t, path, clock.Now())
	if start := eventsOfKind(events, KindStart); len(start) != 1 || start[0].Work.DispatchToStartMs != nil {
		t.Fatalf("start = %+v; a claim before its offer has no queue time", start)
	}
	if finish := eventsOfKind(events, KindFinish); len(finish) != 1 || finish[0].Work.DurationMs != nil {
		t.Fatalf("finish = %+v; a finish before its start has no duration", finish)
	}
}

func TestCheckCommandsAreRedactedAndNullGateIsEmpty(t *testing.T) {
	token := "ghp_" + strings.Repeat("A", 36)
	for _, command := range []string{
		"curl -H 'Authorization: token " + token + "' https://example.invalid",
		"git clone https://user:" + strings.Repeat("p", 12) + "@example.invalid/repo",
		"deploy --password=" + strings.Repeat("s", 12) + " --verbose",
		"export KEY=sk-" + strings.Repeat("x", 40),
	} {
		raw, _ := json.Marshal(map[string]any{"command": command, "exitCode": 0})
		check, err := parseVerificationReport(raw)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(check.Command, token) || strings.Contains(check.Command, strings.Repeat("p", 12)) ||
			strings.Contains(check.Command, strings.Repeat("s", 12)) || strings.Contains(check.Command, strings.Repeat("x", 40)) ||
			!strings.Contains(check.Command, "[redacted]") {
			t.Fatalf("stored command %q keeps a credential", check.Command)
		}
	}
	if check, _ := parseVerificationReport([]byte(`{"command":"go test ./...","exitCode":0}`)); check.Command != "go test ./..." {
		t.Fatalf("an ordinary command was changed: %q", check.Command)
	}
	checks, skipped, err := parseGateReport([]byte(`{"commands":null,"passed":false}`))
	if err != nil || skipped != 0 || len(checks) != 0 {
		t.Fatalf("null gate commands = %v, %d, %v; want a valid empty report", checks, skipped, err)
	}
	if _, _, err := parseGateReport([]byte(`{"passed":false}`)); err == nil {
		t.Fatal("a gate report without commands was accepted")
	}
}

func TestTelemetryStoreRefusesSymlinksAndTightensDirectory(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(dir, "victim.txt")
	if err := os.WriteFile(victim, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	storeDir := filepath.Join(dir, "quota-telemetry")
	if err := os.Mkdir(storeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(storeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(storeDir, "recorder.sqlite")
	if err := os.Symlink(victim, path); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(path); err == nil {
		t.Fatal("a symlink at the store path was followed")
	}
	if info, _ := os.Stat(victim); info.Mode().Perm() != 0o644 || info.Size() != 0 {
		t.Fatalf("the symlink target was changed: %v size %d", info.Mode().Perm(), info.Size())
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	if info, _ := os.Stat(storeDir); info.Mode().Perm() != 0o700 {
		t.Fatalf("an existing 0755 directory is %v; want 0700", info.Mode().Perm())
	}

	linked := filepath.Join(dir, "linked")
	if err := os.Symlink(storeDir, linked); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(filepath.Join(linked, "recorder.sqlite")); err == nil {
		t.Fatal("a symlinked store directory was followed")
	}
}

func TestReaderOfClosedStoreCreatesNothing(t *testing.T) {
	ctx := context.Background()
	path := testStorePath(t)
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []Event{{EventID: "one", Kind: KindRecorder, At: testBase, Recorder: &RecorderNote{State: RecorderStarted}}}); err != nil {
		t.Fatal(err)
	}
	store.Close()
	listing := func() []string {
		entries, _ := os.ReadDir(filepath.Dir(path))
		var names []string
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		return names
	}
	before := strings.Join(listing(), ",")
	if err := os.Chmod(filepath.Dir(path), 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Dir(path), 0o700) })
	reader, err := OpenReader(path)
	if err != nil {
		t.Fatalf("read a closed store in a read-only directory: %v", err)
	}
	result, err := reader.Query(ctx, Filter{Limit: 10}, testBase)
	reader.Close()
	if err != nil || len(result.Events) != 1 {
		t.Fatalf("query = %d events, %v", len(result.Events), err)
	}
	if after := strings.Join(listing(), ","); after != before {
		t.Fatalf("the read created files: %s -> %s", before, after)
	}

	empty := filepath.Join(t.TempDir(), "recorder.sqlite")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_, emptyErr := OpenReader(empty)
	if emptyErr == nil || !strings.Contains(emptyErr.Error(), "not yet written") {
		t.Fatalf("an empty store = %v", emptyErr)
	}
	if errors.Is(emptyErr, ErrNoStore) {
		t.Fatal("an empty store is not a missing one")
	}
}
