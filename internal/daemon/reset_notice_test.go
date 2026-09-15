package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// idle simulates a thread that ended its turn on its own, which is what an
// agent does after it is warned. No reading follows, because readings only
// arrive from running turns.
func (f *fakeT3) idle(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t, ok := f.threads[id]; ok {
		t.Running = false
		t.TurnState = "completed"
	}
}

func (f *fakeT3) archive(id string, at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t, ok := f.threads[id]; ok {
		t.Running = false
		t.ArchivedAt = &at
	}
}

func (f *fakeT3) settle(id string, at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t, ok := f.threads[id]; ok {
		t.Running = false
		t.SettledAt = &at
	}
}

func (f *fakeT3) remove(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.threads, id)
}

// restart builds a second daemon over the same store and fake, as a restart
// of the process would.
func (h *harness) restart() *harness {
	h.t.Helper()
	h2 := &harness{t: h.t, fake: h.fake, store: h.store, cfg: h.cfg, clock: h.clock}
	h2.d = New(h.cfg, slog.Default(), h.store, h.fake, nil)
	h2.d.SetClock(func() time.Time { return h2.clock })
	return h2
}

func TestWarnedThreadIsToldOnceWhenItsWindowResets(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	h.fake.add("a", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	warnedAt := h.clock
	h.snap(codexPrimary, 86, reset, "1")
	if fmt.Sprint(h.fake.warnings) != "[warn:a]" {
		t.Fatalf("warnings = %v", h.fake.warnings)
	}

	// The warning is recorded durably against the window it belongs to.
	n, ok, err := h.store.QuotaResetNotice(ctx, "a", codexPrimary, domain.EpochFor(&reset))
	if err != nil || !ok {
		t.Fatalf("notice not recorded: ok=%v err=%v", ok, err)
	}
	if n.Kind != domain.ActionWarn || n.Threshold != 85 || n.UsedPercent != 86 ||
		!n.ResetsAt.Equal(reset) || !n.WarnedAt.Equal(warnedAt) || n.NotifiedAt != nil || n.StoppedByWatchdog {
		t.Fatalf("notice = %+v", n)
	}

	// The agent stops voluntarily; nothing runs, so no reading arrives.
	h.fake.idle("a")
	h.clock = reset.Add(-time.Minute)
	h.poll()
	if len(h.fake.warnings) != 1 {
		t.Fatalf("notice sent before the reset: %v", h.fake.warnings)
	}

	// The window passes: exactly one advisory, and not again on later polls.
	h.clock = reset.Add(time.Minute)
	h.poll()
	h.poll()
	h.clock = h.clock.Add(time.Hour)
	h.poll()
	if fmt.Sprint(h.fake.warnings) != "[warn:a reset-notice:a]" {
		t.Fatalf("warnings = %v", h.fake.warnings)
	}

	// The advisory says what it knows and does not claim capacity.
	text := h.fake.texts[len(h.fake.texts)-1]
	for _, want := range []string{
		"advisory",
		"No fresh reading has confirmed the new window",
		"provider's own metadata",
		"grants no capacity and resumes nothing",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("advisory missing %q:\n%s", want, text)
		}
	}

	// Telling a thread is not recovering a bucket.
	st, err := h.store.LoadBucket(ctx, codexPrimary)
	if err != nil {
		t.Fatal(err)
	}
	if st.RecoveredAt != nil {
		t.Fatalf("the clock set RecoveredAt: %+v", st)
	}
	if n, _, _ := h.store.QuotaResetNotice(ctx, "a", codexPrimary, domain.EpochFor(&reset)); n.NotifiedAt == nil || n.Outcome != "sent" {
		t.Fatalf("notice not settled: %+v", n)
	}
}

func TestResetNoticeIsNotRepeatedAfterARestart(t *testing.T) {
	h := newHarness(t, nil)
	h.fake.add("a", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 86, reset, "1")
	h.fake.idle("a")

	// A restart before the reset still delivers the advisory once.
	h2 := h.restart()
	h2.clock = reset.Add(time.Minute)
	h2.poll()
	if fmt.Sprint(h.fake.warnings) != "[warn:a reset-notice:a]" {
		t.Fatalf("warnings = %v", h.fake.warnings)
	}

	// A restart after it repeats nothing: the delivery is recorded in the
	// store, not in daemon memory.
	h3 := h2.restart()
	h3.clock = h3.clock.Add(2 * time.Hour)
	h3.poll()
	h3.poll()
	if fmt.Sprint(h.fake.warnings) != "[warn:a reset-notice:a]" {
		t.Fatalf("restart repeated the advisory: %v", h.fake.warnings)
	}
}

func TestResetNoticeSkipsThreadsThatAreGone(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, func(c *config.Config) { c.ResetNotice.IntervalBetweenThreads = 0 })
	for _, id := range []string{"archived", "settled", "deleted", "here"} {
		h.fake.add(id, "codex", "gpt", true)
	}
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 86, reset, "1")
	if len(h.fake.warnings) != 4 {
		t.Fatalf("warnings = %v", h.fake.warnings)
	}
	h.fake.archive("archived", h.clock)
	h.fake.settle("settled", h.clock)
	h.fake.remove("deleted")
	h.fake.idle("here")

	h.clock = reset.Add(time.Minute)
	h.poll()
	got := fmt.Sprint(h.fake.warnings[4:])
	if got != "[reset-notice:here]" {
		t.Fatalf("advisories after the reset = %s", got)
	}
	// The skipped advisories are settled, so later passes do not reconsider
	// them for ever.
	for _, id := range []string{"archived", "settled", "deleted"} {
		n, ok, err := h.store.QuotaResetNotice(ctx, id, codexPrimary, domain.EpochFor(&reset))
		if err != nil || !ok || n.NotifiedAt == nil || !strings.HasPrefix(n.Outcome, "skipped: ") {
			t.Fatalf("%s notice = %+v ok=%v err=%v", id, n, ok, err)
		}
	}
}

func TestStoppedThreadIsToldWithoutBecomingResumeEligible(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	h.fake.add("a", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 96, reset, "1")
	if fmt.Sprint(h.fake.stops) != "[interrupt:a]" {
		t.Fatalf("stops = %v", h.fake.stops)
	}

	// The window passes with no reading, which is the case the probe exists
	// for. Before the probe delay the thread is told, and nothing else.
	h.clock = reset.Add(time.Minute)
	h.poll()
	if fmt.Sprint(h.fake.warnings) != "[reset-notice:a]" {
		t.Fatalf("warnings = %v", h.fake.warnings)
	}
	if len(h.fake.resumes) != 0 {
		t.Fatalf("the advisory resumed a thread: %v", h.fake.resumes)
	}
	if !strings.Contains(h.fake.texts[0], "stopped by the quota watchdog") {
		t.Fatalf("advisory = %s", h.fake.texts[0])
	}
	intent, ok, err := h.store.LoadResumeIntent(ctx, "a")
	if err != nil || !ok || intent.Status != domain.ResumePending {
		t.Fatalf("intent = %+v ok=%v err=%v", intent, ok, err)
	}
	st, err := h.store.LoadBucket(ctx, codexPrimary)
	if err != nil {
		t.Fatal(err)
	}
	if st.RecoveredAt != nil || !st.ResetsAt.Equal(reset) {
		t.Fatalf("the advisory changed the bucket: %+v", st)
	}

	// The resume path is unchanged: it still waits for the probe delay and
	// then resumes on the reading the probe produces, not on the clock.
	h.clock = reset.Add(8 * time.Minute)
	h.poll()
	if fmt.Sprint(h.fake.resumes) != "[a]" {
		t.Fatalf("probe resumes = %v", h.fake.resumes)
	}
	if len(h.fake.warnings) != 1 {
		t.Fatalf("the advisory was repeated: %v", h.fake.warnings)
	}
}

// The advisory reaches an idle thread the only way T3 offers, by starting a
// turn. When that thread then takes its own turn the existing manual
// interaction rule cancels its resume intent, which is accurate: the thread
// is awake and owns its own continuation. The resume path is not changed to
// excuse the advisory.
func TestAThreadThatActsOnTheAdvisoryEndsItsResumeIntent(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	h.fake.add("a", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 96, reset, "1")
	h.clock = reset.Add(time.Minute)
	h.poll()
	if fmt.Sprint(h.fake.warnings) != "[reset-notice:a]" {
		t.Fatalf("warnings = %v", h.fake.warnings)
	}
	h.fake.mu.Lock()
	h.fake.threads["a"].TurnID = "turn-after-notice"
	h.fake.threads["a"].Running = true
	h.fake.mu.Unlock()
	h.clock = h.clock.Add(time.Minute)
	h.poll()
	intent, _, _ := h.store.LoadResumeIntent(ctx, "a")
	if intent.Status != domain.ResumeCancelled {
		t.Fatalf("intent = %+v", intent)
	}
	if len(h.fake.resumes) != 0 {
		t.Fatalf("resumed anyway: %v", h.fake.resumes)
	}
}

func TestResetNoticeCanBeDisabled(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, func(c *config.Config) { c.ResetNotice.Enabled = false })
	h.fake.add("a", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 86, reset, "1")
	h.fake.idle("a")
	h.clock = reset.Add(time.Hour)
	h.poll()
	if fmt.Sprint(h.fake.warnings) != "[warn:a]" {
		t.Fatalf("warnings = %v", h.fake.warnings)
	}
	if _, ok, _ := h.store.QuotaResetNotice(ctx, "a", codexPrimary, domain.EpochFor(&reset)); ok {
		t.Fatal("a disabled notice was still recorded")
	}
}

func TestResetNoticeEvidenceNamesAReadingWhenThereIsOne(t *testing.T) {
	h := newHarness(t, nil)
	h.fake.add("a", "codex", "gpt", true)
	h.fake.add("b", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 86, reset, "1")
	h.fake.idle("a")
	// Thread b keeps running into the new window and produces the reading
	// that thread a never saw.
	h.clock = reset.Add(2 * time.Minute)
	h.snap(codexPrimary, 4, reset.Add(5*time.Hour), "2")
	h.clock = reset.Add(3 * time.Minute)
	h.poll()
	var text string
	for i, w := range h.fake.warnings {
		if w == "reset-notice:a" {
			text = h.fake.texts[i]
		}
	}
	if text == "" {
		t.Fatalf("no advisory for a: %v", h.fake.warnings)
	}
	if !strings.Contains(text, "after that reset, puts the window at 4%") {
		t.Fatalf("advisory does not name the reading:\n%s", text)
	}
}
