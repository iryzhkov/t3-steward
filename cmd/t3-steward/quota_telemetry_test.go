package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/quotatelemetry"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestQuotaTelemetryRecorderNeverBlocksCoordinator(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.db")
	store, err := sqlite.OpenMigrated(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{AuditEvents: []domain.AuditEvent{{
		ID: "event-1", Kind: "assignment-offered", TargetType: domain.AdminTargetAssignment, TargetID: "assignment-1",
		CreatedAt: time.Now().UTC(),
	}}}); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{StatePath: statePath}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	delay := quotaTelemetryFirstTickDelay
	quotaTelemetryFirstTickDelay = 0
	t.Cleanup(func() { quotaTelemetryFirstTickDelay = delay })

	// The telemetry directory exists and cannot be written: every tick fails.
	telemetryDir := filepath.Join(dir, "quota-telemetry")
	if err := os.Mkdir(telemetryDir, 0o500); err != nil {
		t.Fatal(err)
	}
	before := coordinatorFileDigests(t, statePath)
	started := time.Now()
	recorder := startQuotaTelemetryRecorder(ctx, cfg, logger)
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("start took %s; it must return at once", elapsed)
	}
	waitForQuotaTelemetry(t, func() bool { return recorder.Stats().Failures >= 1 })
	stopped := time.Now()
	recorder.Stop()
	if elapsed := time.Since(stopped); elapsed > 2*time.Second {
		t.Fatalf("stop took %s; want under 2 seconds", elapsed)
	}
	if stats := recorder.Stats(); stats.LastError == "" {
		t.Fatalf("stats = %+v; want the failure counted with its error", stats)
	}

	// Writable again: the recorder reads the coordinator database and writes
	// only its own file.
	if err := os.Chmod(telemetryDir, 0o700); err != nil {
		t.Fatal(err)
	}
	recorder = startQuotaTelemetryRecorder(ctx, cfg, logger)
	waitForQuotaTelemetry(t, func() bool { return recorder.Stats().LastSuccessAt != nil })
	recorder.Stop()
	after := coordinatorFileDigests(t, statePath)
	if before != after {
		t.Fatalf("coordinator database changed: %v != %v", before, after)
	}
	if _, err := os.Stat(filepath.Join(telemetryDir, "recorder.sqlite")); err != nil {
		t.Fatalf("telemetry store not written: %v", err)
	}

	// An in-memory state path disables recording without reaching the caller.
	memory := startQuotaTelemetryRecorder(ctx, config.Config{StatePath: ":memory:"}, logger)
	waitForQuotaTelemetry(t, func() bool { return memory.Stats().Failures >= 1 })
	memory.Stop()
}

func coordinatorFileDigests(t *testing.T, statePath string) [2][32]byte {
	t.Helper()
	var digests [2][32]byte
	for index, path := range []string{statePath, statePath + "-wal"} {
		content, err := os.ReadFile(path)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		digests[index] = sha256.Sum256(content)
	}
	return digests
}

func waitForQuotaTelemetry(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("the recorder did not tick")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func seedQuotaTelemetryStore(t *testing.T, path string, now time.Time) {
	t.Helper()
	store, err := quotatelemetry.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	effort := "medium"
	opus := quotatelemetry.Route{ProviderInstanceID: "claudeAgent", Model: "claude-opus-5-5", Effort: &effort, QuotaPoolID: "claude-main"}
	sol := quotatelemetry.Route{ProviderInstanceID: "codex", Model: "gpt-6.1-sol", EffortAbsent: true, QuotaPoolID: "codex-main"}
	taskType := quotatelemetry.TaskType{Category: "implement", Derived: true, Rule: quotatelemetry.RuleTaskName}
	work := func(id string, route quotatelemetry.Route) *quotatelemetry.Work {
		return &quotatelemetry.Work{RunID: "run-4be1f00dcafe", TaskID: "task-" + id, TaskName: "implement", AttemptID: "attempt-" + id,
			AttemptNumber: 1, AssignmentID: "assignment-" + id, AssignmentEpoch: 1, WorkerID: "worker-1", Route: route,
			ExecutionRole: "executor", TaskType: taskType}
	}
	start := now.Add(-3 * time.Hour)
	finish := now.Add(-time.Hour)
	queued := int64(41000)
	duration := finish.Sub(start).Milliseconds()
	startWork := work("1", opus)
	startWork.DispatchToStartMs = &queued
	startWork.StartedAt = &start
	finishWork := work("1", opus)
	finishWork.StartedAt, finishWork.FinishedAt, finishWork.DurationMs, finishWork.Outcome = &start, &finish, &duration, "succeeded"
	checkDuration := int64(1200)
	reset := now.Add(2 * time.Hour)
	events := []quotatelemetry.Event{
		{EventID: "recorder:started", Kind: quotatelemetry.KindRecorder, At: now.Add(-30 * time.Hour),
			Recorder: &quotatelemetry.RecorderNote{State: "started", CoverageFrom: ptrTime(now.Add(-30 * time.Hour))}},
		{EventID: "dispatch:assignment-1:1", Kind: quotatelemetry.KindDispatch, At: start.Add(-41 * time.Second), QuotaPoolID: "claude-main", Work: work("1", opus)},
		{EventID: "start:assignment-1:1", Kind: quotatelemetry.KindStart, At: start, QuotaPoolID: "claude-main", Work: startWork},
		{EventID: "check:attempt-1:verification:2", Kind: quotatelemetry.KindCheck, At: finish.Add(-time.Minute), QuotaPoolID: "claude-main", Work: work("1", opus),
			Check: &quotatelemetry.Check{Stage: "verification", Index: 2, ExitCode: 0, DurationMs: &checkDuration, Command: "git bundle verify unit.bundle"}},
		{EventID: "finish:assignment-1:1", Kind: quotatelemetry.KindFinish, At: finish, QuotaPoolID: "claude-main", Work: finishWork},
		{EventID: "dispatch:assignment-2:1", Kind: quotatelemetry.KindDispatch, At: now.Add(-2 * time.Hour), QuotaPoolID: "codex-main", Work: work("2", sol)},
		{EventID: "reading:worker:w1:claudeAgent/claude/five_hour:a", Kind: quotatelemetry.KindReading, At: start.Add(-time.Minute),
			Reading: &quotatelemetry.Reading{Source: "worker:w1", BucketKey: "claudeAgent/claude/five_hour", ProviderInstanceID: "claudeAgent",
				LimitID: "claude", Window: "five_hour", UsedPercent: 10, ResetsAt: &reset, ObservedAt: start.Add(-time.Minute), Phase: "normal"}},
		{EventID: "reading:worker:w1:claudeAgent/claude/five_hour:b", Kind: quotatelemetry.KindReading, At: finish.Add(time.Minute),
			Reading: &quotatelemetry.Reading{Source: "worker:w1", BucketKey: "claudeAgent/claude/five_hour", ProviderInstanceID: "claudeAgent",
				LimitID: "claude", Window: "five_hour", UsedPercent: 19, ResetsAt: &reset, ObservedAt: finish.Add(time.Minute), Phase: "normal"}},
		{EventID: "reading:coordinator-host:codex/codex/primary:a", Kind: quotatelemetry.KindReading, At: now.Add(-time.Minute),
			Reading: &quotatelemetry.Reading{Source: "coordinator-host", BucketKey: "codex/codex/primary", ProviderInstanceID: "codex",
				LimitID: "codex", Window: "primary", UsedPercent: 40, ObservedAt: now.Add(-time.Minute), Phase: "normal"}},
	}
	for index := range events {
		events[index].SchemaVersion = quotatelemetry.SchemaVersion
	}
	if err := store.Append(context.Background(), events); err != nil {
		t.Fatal(err)
	}
}

func ptrTime(value time.Time) *time.Time { return &value }

func TestQuotaTelemetryCommandFiltersTextAndJSON(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 7, 14, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "quota-telemetry", "recorder.sqlite")
	seedQuotaTelemetryStore(t, path, now)
	pools := map[string][]string{"claude-main": {"claudeAgent"}, "codex-main": {"codex"}}

	run := func(args ...string) string {
		t.Helper()
		options, err := parseQuotaTelemetryArgs(args, now)
		if err != nil {
			t.Fatalf("parse %v: %v", args, err)
		}
		var out bytes.Buffer
		if err := runQuotaTelemetry(ctx, options, path, pools, &out, now); err != nil {
			t.Fatalf("run %v: %v", args, err)
		}
		return out.String()
	}

	text := run()
	lines := strings.Split(strings.TrimSpace(text), "\n")
	if !strings.HasPrefix(lines[0], "quota telemetry: 8 events since 2026-10-06T14:00:00Z (store "+path+", schema 1, retained from 2026-10-06T08:00:00Z; recorder failures 0)") {
		t.Fatalf("header = %q", lines[0])
	}
	if got := strings.Join(strings.Fields(lines[1]), " "); got != "TIME KIND ROUTE EFFORT TYPE(derived) TASK DURATION DETAIL" {
		t.Fatalf("columns = %q", got)
	}
	if len(lines) != 10 {
		t.Fatalf("lines = %d; want header, columns and 8 events (the started event is older than 24h):\n%s", len(lines), text)
	}
	if !strings.Contains(lines[2], "reading") || !strings.Contains(lines[len(lines)-1], "reading") {
		t.Fatalf("events not printed oldest first:\n%s", text)
	}
	finishLine := lineWith(t, lines, "finish")
	for _, want := range []string{"claudeAgent/claude-opus-5-5", "medium", "implement:name", "run-4be1...", "/implement", "2h0m0s", "succeeded", "five_hour +9pp exclusive"} {
		if !strings.Contains(finishLine, want) {
			t.Fatalf("finish line %q lacks %q", finishLine, want)
		}
	}
	if startLine := lineWith(t, lines, " start "); !strings.Contains(startLine, "queued 41s after dispatch") {
		t.Fatalf("start line = %q", startLine)
	}
	if checkLine := lineWith(t, lines, " check "); !strings.Contains(checkLine, "1.2s") || !strings.Contains(checkLine, "verification #2 exit 0: git bundle verify unit.bundle") {
		t.Fatalf("check line = %q", checkLine)
	}

	if got := run("--kind", "start,finish,check", "--route", "opus", "--since", "6h"); strings.Count(got, "\n") != 5 ||
		!strings.HasPrefix(got, "quota telemetry: 3 events since 2026-10-07T08:00:00Z") {
		t.Fatalf("kind and route filter:\n%s", got)
	}
	if got := run("--pool", "codex-main"); strings.Count(got, "\n") != 4 || !strings.Contains(got, "gpt-6.1-sol") || !strings.Contains(got, "codex/codex/primary") {
		t.Fatalf("pool filter, with readings of the pool's provider instances:\n%s", got)
	}
	if got := run("--limit", "2"); !strings.HasPrefix(got, "quota telemetry: 2 events") || !strings.Contains(got, "codex/codex/primary") {
		t.Fatalf("limit keeps the newest:\n%s", got)
	}
	if got := run("--since", "2026-10-07T13:00:00Z", "--kind", "dispatch"); !strings.Contains(got, "no telemetry events match") {
		t.Fatalf("empty result:\n%s", got)
	}

	var document map[string]json.RawMessage
	if err := json.Unmarshal([]byte(run("--json", "--kind", "finish")), &document); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"schemaVersion", "kind", "generatedAt", "store", "recorder", "filters", "events"} {
		if _, found := document[key]; !found {
			t.Fatalf("JSON lacks %q: %v", key, document)
		}
	}
	if string(document["kind"]) != `"t3-steward.quota-telemetry/v1"` || string(document["schemaVersion"]) != "1" {
		t.Fatalf("JSON kind %s schema %s", document["kind"], document["schemaVersion"])
	}
	var storeInfo, recorderInfo map[string]json.RawMessage
	_ = json.Unmarshal(document["store"], &storeInfo)
	_ = json.Unmarshal(document["recorder"], &recorderInfo)
	for _, key := range []string{"path", "schemaVersion", "oldestRetained", "rows"} {
		if _, found := storeInfo[key]; !found {
			t.Fatalf("store lacks %q", key)
		}
	}
	for _, key := range []string{"failures", "lastError", "lastSuccessAt", "coverageFrom", "gaps"} {
		if _, found := recorderInfo[key]; !found {
			t.Fatalf("recorder lacks %q", key)
		}
	}
	var events []map[string]json.RawMessage
	if err := json.Unmarshal(document["events"], &events); err != nil || len(events) != 1 {
		t.Fatalf("events = %s, %v", document["events"], err)
	}
	if string(events[0]["schemaVersion"]) != "1" || len(events[0]["deltas"]) == 0 {
		t.Fatalf("finish event = %v; want its own schemaVersion and computed deltas", events[0])
	}
	var empty map[string]json.RawMessage
	if err := json.Unmarshal([]byte(run("--json", "--kind", "dispatch", "--since", "10m")), &empty); err != nil || string(empty["events"]) != "[]" {
		t.Fatalf("empty JSON events = %s, %v; want []", empty["events"], err)
	}

	for _, bad := range [][]string{
		{"--limit", "0"}, {"--limit", "10001"}, {"--kind", "bogus"}, {"--since", "yesterday"}, {"--since", "-1h"},
		{"--route"}, {"extra"}, {"--json", "--json"},
	} {
		if _, err := parseQuotaTelemetryArgs(bad, now); err == nil {
			t.Fatalf("%v was accepted", bad)
		}
	}
	if options, err := parseQuotaTelemetryArgs([]string{"--since", "2d"}, now); err != nil || !options.since.Equal(now.Add(-48*time.Hour)) {
		t.Fatalf("--since 2d = %+v, %v", options, err)
	}
}

func lineWith(t *testing.T, lines []string, needle string) string {
	t.Helper()
	for _, line := range lines {
		if strings.Contains(line, needle) {
			return line
		}
	}
	t.Fatalf("no line with %q", needle)
	return ""
}

func TestQuotaTelemetryCommandWithoutStore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "quota-telemetry", "recorder.sqlite")
	now := time.Date(2026, 10, 7, 14, 0, 0, 0, time.UTC)
	options, err := parseQuotaTelemetryArgs(nil, now)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err = runQuotaTelemetry(context.Background(), options, path, nil, &out, now)
	if err == nil || !strings.Contains(err.Error(), "no quota telemetry store at "+path) ||
		!strings.Contains(err.Error(), "ssh <coordinator-host> t3-steward quota telemetry") {
		t.Fatalf("error = %v", err)
	}
	if exitCodeFor(err) != 1 {
		t.Fatalf("exit = %d; want 1", exitCodeFor(err))
	}
	if _, statErr := os.Stat(filepath.Dir(path)); !os.IsNotExist(statErr) {
		t.Fatalf("the read created %s: %v", filepath.Dir(path), statErr)
	}

	// Through the real command line: the store path follows the configured
	// state database.
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("state_path: "+filepath.Join(dir, "state.db")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = run([]string{"quota", "telemetry", "--config", configPath})
	if err == nil || !strings.Contains(err.Error(), "no quota telemetry store at "+path) {
		t.Fatalf("command = %v", err)
	}
	if _, statErr := os.Stat(filepath.Dir(path)); !os.IsNotExist(statErr) {
		t.Fatalf("the command created %s: %v", filepath.Dir(path), statErr)
	}
}
