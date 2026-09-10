package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// fakeT3 is an in-memory controller that records every call.
type fakeT3 struct {
	mu       sync.Mutex
	threads  map[string]*domain.Thread
	warnings []string // "kind:threadID"
	stops    []string // "mode:threadID"
	resumes  []string
	// stopFails makes StopThread a no-op for the listed threads.
	stopFails map[string]bool
}

func newFakeT3() *fakeT3 {
	return &fakeT3{threads: map[string]*domain.Thread{}, stopFails: map[string]bool{}}
}

func (f *fakeT3) add(id, instance, model string, running bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.threads[id] = &domain.Thread{
		ID: id, Title: "thread " + id, ProviderInstanceID: instance, Model: model, Running: running,
		TurnID: "turn-" + id, ModelSelection: map[string]any{"instanceId": instance, "model": model},
	}
	if running {
		f.threads[id].TurnState = "running"
	}
}

func (f *fakeT3) ListThreads(context.Context) ([]domain.Thread, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]domain.Thread, 0, len(f.threads))
	for _, t := range f.threads {
		out = append(out, *t)
	}
	return out, nil
}

func (f *fakeT3) GetThread(_ context.Context, id string) (*domain.Thread, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t, ok := f.threads[id]; ok {
		c := *t
		return &c, nil
	}
	return nil, nil
}

func (f *fakeT3) LastUserMessageAt(context.Context, string) (*time.Time, error) { return nil, nil }

func (f *fakeT3) WarnThread(_ context.Context, t domain.Thread, w domain.Warning) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.warnings = append(f.warnings, string(w.Kind)+":"+t.ID)
	return nil
}

func (f *fakeT3) StopThread(_ context.Context, t domain.Thread, mode t3control.StopMode) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops = append(f.stops, string(mode)+":"+t.ID)
	if f.stopFails[t.ID] {
		return nil
	}
	if th, ok := f.threads[t.ID]; ok {
		th.Running = false
		th.TurnState = "interrupted"
	}
	return nil
}

func (f *fakeT3) ResumeThread(_ context.Context, t domain.Thread, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resumes = append(f.resumes, t.ID)
	if th, ok := f.threads[t.ID]; ok {
		th.Running = true
		th.TurnState = "running"
		th.TurnID = "turn-resumed-" + t.ID
	}
	return nil
}

type harness struct {
	t     *testing.T
	d     *Daemon
	fake  *fakeT3
	store *sqlite.Store
	clock time.Time
	cfg   config.Config
}

func newHarness(t *testing.T, mutate func(*config.Config)) *harness {
	t.Helper()
	cfg := config.Default()
	cfg.Policy.DryRun = false
	cfg.Policy.StopVerifyTimeout = config.Duration(10 * time.Millisecond)
	cfg.Policy.StopRetries = 1
	cfg.Resume.Enabled = true
	cfg.Resume.ResetSettleDelay = config.Duration(2 * time.Minute)
	cfg.Resume.IntervalBetweenThreads = config.Duration(45 * time.Second)
	cfg.Notifications.Desktop = false
	if mutate != nil {
		mutate(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.OpenMigrated(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	fake := newFakeT3()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	h := &harness{t: t, fake: fake, store: store, cfg: cfg, clock: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
	h.d = New(cfg, logger, store, fake, nil)
	h.d.SetClock(func() time.Time { return h.clock })
	return h
}

var (
	codexPrimary   = domain.BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: "primary"}
	codexSecondary = domain.BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: "secondary"}
)

func (h *harness) snap(key domain.BucketKey, used float64, resets time.Time, id string) {
	h.t.Helper()
	r := resets
	h.d.HandleSnapshot(context.Background(), domain.QuotaSnapshot{
		Key: key, LimitName: key.String(), UsedPercent: used, ResetsAt: &r, ObservedAt: h.clock, SourceEventID: id,
	})
}

func (h *harness) advance(d time.Duration) {
	h.clock = h.clock.Add(d)
	h.d.Tick(context.Background())
}

func (h *harness) poll() { h.d.Poll(context.Background()) }

func TestWarnDrainStopResumeFlow(t *testing.T) {
	h := newHarness(t, nil)
	h.fake.add("a", "codex", "gpt", true)
	h.fake.add("b", "codex", "gpt", false)       // idle: never touched
	h.fake.add("c", "claudeAgent", "opus", true) // other provider: never touched
	// Readings ten minutes apart keep the burn-rate ladder quiet.
	reset := h.clock.Add(5 * time.Hour)

	h.snap(codexPrimary, 84, reset, "1")
	if len(h.fake.warnings) != 0 {
		t.Fatalf("84%% warned: %v", h.fake.warnings)
	}
	h.advance(10 * time.Minute)
	h.snap(codexPrimary, 85, reset, "2")
	if fmt.Sprint(h.fake.warnings) != "[warn:a]" {
		t.Fatalf("warnings = %v", h.fake.warnings)
	}
	h.advance(10 * time.Minute)
	h.snap(codexPrimary, 88, reset, "3")
	if len(h.fake.warnings) != 1 {
		t.Fatalf("duplicate warning: %v", h.fake.warnings)
	}
	h.advance(10 * time.Minute)
	h.snap(codexPrimary, 90, reset, "4")
	if fmt.Sprint(h.fake.warnings) != "[warn:a drain:a]" {
		t.Fatalf("warnings = %v", h.fake.warnings)
	}
	h.advance(10 * time.Minute)
	h.snap(codexPrimary, 95, reset, "5")
	if fmt.Sprint(h.fake.stops) != "[interrupt:a]" {
		t.Fatalf("stops = %v", h.fake.stops)
	}
	intent, ok, _ := h.store.LoadResumeIntent(context.Background(), "a")
	if !ok || intent.Status != domain.ResumePending || !intent.StoppedByWatchdog {
		t.Fatalf("intent = %+v ok=%v", intent, ok)
	}

	// Restart: a new daemon over the same store repeats nothing.
	h2 := &harness{t: t, fake: h.fake, store: h.store, cfg: h.cfg, clock: h.clock}
	h2.d = New(h.cfg, slog.Default(), h.store, h.fake, nil)
	h2.d.SetClock(func() time.Time { return h2.clock })
	h2.snap(codexPrimary, 96, reset, "6")
	h2.poll()
	if len(h.fake.stops) != 1 || len(h.fake.warnings) != 2 {
		t.Fatalf("restart repeated actions: stops=%v warnings=%v", h.fake.stops, h.fake.warnings)
	}

	// Reset time passes with no fresh snapshot: after the probe delay the
	// one stopped thread is resumed as a probe for the reading.
	h2.clock = reset.Add(3 * time.Minute)
	h2.d.Tick(context.Background())
	h2.poll()
	if len(h.fake.resumes) != 0 {
		t.Fatalf("resumed before the probe delay: %v", h.fake.resumes)
	}
	h2.clock = reset.Add(10 * time.Minute)
	h2.poll()
	if fmt.Sprint(h.fake.resumes) != "[a]" {
		t.Fatalf("probe resumes = %v", h.fake.resumes)
	}
	// The probe's first reading, below 50% in the new window, rearms the
	// bucket; nothing else is waiting, and the probe is not resumed twice.
	h2.snap(codexPrimary, 4, reset.Add(5*time.Hour), "7")
	h2.clock = h2.clock.Add(3 * time.Minute)
	h2.poll()
	if fmt.Sprint(h.fake.resumes) != "[a]" {
		t.Fatalf("resumes = %v", h.fake.resumes)
	}
	intent, _, _ = h.store.LoadResumeIntent(context.Background(), "a")
	if intent.Status != domain.ResumeResumed {
		t.Fatalf("intent status = %s", intent.Status)
	}
}

func TestGraceExpiryStopsWithoutNewEvent(t *testing.T) {
	h := newHarness(t, nil)
	h.fake.add("a", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 91, reset, "1")
	if len(h.fake.stops) != 0 {
		t.Fatalf("stopped at drain: %v", h.fake.stops)
	}
	h.advance(30 * time.Second)
	if len(h.fake.stops) != 0 {
		t.Fatalf("stopped before grace: %v", h.fake.stops)
	}
	h.advance(31 * time.Second)
	if fmt.Sprint(h.fake.stops) != "[interrupt:a]" {
		t.Fatalf("stops = %v", h.fake.stops)
	}
}

func TestWeeklyExhaustedBlocksResume(t *testing.T) {
	h := newHarness(t, nil)
	h.fake.add("a", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	weekly := h.clock.Add(7 * 24 * time.Hour)
	h.snap(codexSecondary, 96, weekly, "w1")
	h.snap(codexPrimary, 96, reset, "p1")
	if len(h.fake.stops) != 1 {
		t.Fatalf("stops = %v", h.fake.stops)
	}
	h.clock = reset.Add(5 * time.Minute)
	h.snap(codexPrimary, 2, reset.Add(5*time.Hour), "p2")
	h.clock = h.clock.Add(5 * time.Minute)
	h.poll()
	if len(h.fake.resumes) != 0 {
		t.Fatalf("resumed while weekly exhausted: %v", h.fake.resumes)
	}
	// Weekly recovers too.
	h.clock = weekly.Add(time.Minute)
	h.snap(codexSecondary, 1, weekly.Add(7*24*time.Hour), "w2")
	h.snap(codexPrimary, 2, h.clock.Add(5*time.Hour), "p3")
	h.clock = h.clock.Add(5 * time.Minute)
	h.poll()
	if fmt.Sprint(h.fake.resumes) != "[a]" {
		t.Fatalf("resumes = %v", h.fake.resumes)
	}
}

func TestManualInteractionCancelsIntent(t *testing.T) {
	h := newHarness(t, nil)
	h.fake.add("a", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 96, reset, "1")
	// The user sends a message: the turn id changes and it runs again.
	h.fake.mu.Lock()
	h.fake.threads["a"].Running = true
	h.fake.threads["a"].TurnID = "turn-manual"
	h.fake.mu.Unlock()
	h.poll()
	intent, _, _ := h.store.LoadResumeIntent(context.Background(), "a")
	if intent.Status != domain.ResumeCancelled {
		t.Fatalf("intent = %+v", intent)
	}
}

func TestResumesAreStaggeredAndAbortWhenQuotaRises(t *testing.T) {
	h := newHarness(t, nil)
	for i := 0; i < 10; i++ {
		h.fake.add(fmt.Sprintf("t%d", i), "codex", "gpt", true)
	}
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 96, reset, "1")
	if len(h.fake.stops) != 10 {
		t.Fatalf("stops = %d", len(h.fake.stops))
	}
	h.clock = reset.Add(time.Minute)
	h.snap(codexPrimary, 1, reset.Add(5*time.Hour), "2")
	h.clock = h.clock.Add(3 * time.Minute)
	h.poll()
	if len(h.fake.resumes) != 1 {
		t.Fatalf("first poll resumed %d threads", len(h.fake.resumes))
	}
	h.clock = h.clock.Add(10 * time.Second)
	h.poll()
	if len(h.fake.resumes) != 1 {
		t.Fatalf("resumed again inside the interval: %d", len(h.fake.resumes))
	}
	h.clock = h.clock.Add(40 * time.Second)
	h.poll()
	if len(h.fake.resumes) != 2 {
		t.Fatalf("second resume missing: %d", len(h.fake.resumes))
	}
	// Quota rises to the warning level: the rest of the batch stops.
	h.snap(codexPrimary, 86, reset.Add(5*time.Hour), "3")
	h.clock = h.clock.Add(time.Minute)
	h.poll()
	if len(h.fake.resumes) != 2 {
		t.Fatalf("resumed despite rising quota: %d", len(h.fake.resumes))
	}
}

func TestModelSpecificLimitAffectsMatchingThreadsOnly(t *testing.T) {
	h := newHarness(t, nil)
	h.fake.add("opus", "claudeAgent", "claude-opus-5", true)
	h.fake.add("sonnet", "claudeAgent", "claude-sonnet-5", true)
	key := domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "seven_day_opus"}
	r := h.clock.Add(7 * 24 * time.Hour)
	h.d.HandleSnapshot(context.Background(), domain.QuotaSnapshot{
		Key: key, LimitName: "Claude seven day opus", UsedPercent: 97, ResetsAt: &r, ObservedAt: h.clock,
		SourceEventID: "m1", ModelSelector: "opus",
	})
	if fmt.Sprint(h.fake.stops) != "[interrupt:opus]" {
		t.Fatalf("stops = %v", h.fake.stops)
	}
}

func TestStopNewSessionsAndEscalation(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.Policy.EscalateToSessionStop = true })
	h.fake.add("a", "codex", "gpt", true)
	h.fake.stopFails["a"] = true
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 96, reset, "1")
	if fmt.Sprint(h.fake.stops) != "[interrupt:a session-stop:a]" {
		t.Fatalf("stops = %v", h.fake.stops)
	}
	if _, ok, _ := h.store.LoadResumeIntent(context.Background(), "a"); ok {
		t.Fatal("intent recorded for a thread that never stopped")
	}
	// A thread that starts while the bucket is stopped is stopped too.
	h.fake.add("late", "codex", "gpt", true)
	h.poll()
	if fmt.Sprint(h.fake.stops) != "[interrupt:a session-stop:a interrupt:late]" {
		t.Fatalf("stops = %v", h.fake.stops)
	}
	h.poll()
	if len(h.fake.stops) != 3 {
		t.Fatalf("late thread stopped twice: %v", h.fake.stops)
	}
}

func TestDryRunSendsNothing(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.Policy.DryRun = true })
	// The daemon records dry-run actions but the fake controller is still
	// called (the real adapter is what honours dry-run), so verify through
	// the audit log flag instead.
	h.fake.add("a", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 96, reset, "1")
	actions, _ := h.store.RecentActions(context.Background(), 10)
	if len(actions) == 0 || !actions[0].DryRun {
		t.Fatalf("actions = %+v", actions)
	}
}

func TestControlDisabledOnlyRecords(t *testing.T) {
	h := newHarness(t, nil)
	h.d.ControlAllowed = false
	h.d.ControlReason = "untested version"
	h.fake.add("a", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 96, reset, "1")
	if len(h.fake.stops) != 0 {
		t.Fatalf("stopped with control disabled: %v", h.fake.stops)
	}
	actions, _ := h.store.RecentActions(context.Background(), 10)
	if len(actions) != 1 || actions[0].Kind != domain.ActionStop {
		t.Fatalf("actions = %+v", actions)
	}
}

func TestOverrideChangesThresholds(t *testing.T) {
	h := newHarness(t, func(c *config.Config) {
		var o config.Override
		o.Match.Provider = "codex"
		o.Match.Window = "prim*"
		stop := 99.0
		o.StopPercent = &stop
		c.Overrides = append(c.Overrides, o)
	})
	h.fake.add("a", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 96, reset, "1")
	if len(h.fake.stops) != 0 {
		t.Fatalf("override ignored: %v", h.fake.stops)
	}
	h.advance(time.Minute)
	h.snap(codexPrimary, 99, reset, "2")
	if len(h.fake.stops) != 1 {
		t.Fatalf("stops = %v", h.fake.stops)
	}
}

func TestLateThreadsGetTheCurrentNotice(t *testing.T) {
	h := newHarness(t, nil)
	h.fake.add("a", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 86, reset, "1")
	if fmt.Sprint(h.fake.warnings) != "[warn:a]" {
		t.Fatalf("warnings = %v", h.fake.warnings)
	}
	// A thread that starts while the bucket is warned gets the warning on
	// the next poll; the earlier thread is not warned twice.
	h.fake.add("late", "codex", "gpt", true)
	h.poll()
	h.poll()
	if fmt.Sprint(h.fake.warnings) != "[warn:a warn:late]" {
		t.Fatalf("warnings = %v", h.fake.warnings)
	}
	// Drain: both running threads get the drain request, once each, and a
	// thread started during the drain gets it too.
	h.advance(10 * time.Minute)
	h.snap(codexPrimary, 91, reset, "2")
	h.fake.add("later", "codex", "gpt", true)
	h.poll()
	got := fmt.Sprint(h.fake.warnings)
	if !strings.Contains(got, "drain:a") || !strings.Contains(got, "drain:late") || !strings.Contains(got, "drain:later") || strings.Count(got, "drain:a ") > 1 {
		t.Fatalf("warnings = %v", h.fake.warnings)
	}
}

func TestProbeResumeWhenNoReadingConfirmsTheReset(t *testing.T) {
	h := newHarness(t, nil)
	h.fake.add("a", "codex", "gpt", true)
	h.fake.add("b", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 96, reset, "1")
	if len(h.fake.stops) != 2 {
		t.Fatalf("stops = %v", h.fake.stops)
	}
	// Reset time passes, nothing runs, no reading arrives.
	h.clock = reset.Add(3 * time.Minute)
	h.poll()
	if len(h.fake.resumes) != 0 {
		t.Fatalf("resumed before the probe delay: %v", h.fake.resumes)
	}
	// After settle delay (2m) plus probe_after_reset (5m): exactly one
	// probe per provider, and no second one on later polls.
	h.clock = reset.Add(8 * time.Minute)
	h.poll()
	h.clock = h.clock.Add(time.Minute)
	h.poll()
	if len(h.fake.resumes) != 1 {
		t.Fatalf("probe resumes = %v", h.fake.resumes)
	}
	// The probe's first reading confirms the reset: the bucket rearms and
	// the other thread follows after the settle delay and stagger.
	h.snap(codexPrimary, 2, reset.Add(5*time.Hour), "2")
	h.clock = h.clock.Add(3 * time.Minute)
	h.poll()
	if len(h.fake.resumes) != 2 {
		t.Fatalf("resumes after confirmation = %v", h.fake.resumes)
	}
}
