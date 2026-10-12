package daemon

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// These fixtures model b1's future-only, earliest-settlement reporting seam.
// synctest supplies a deterministic clock and timers for the entire Run loop.
type deadlineWaits struct {
	mu sync.Mutex
	deadlineState
}

type deadlineState struct {
	window       time.Duration
	settled      []time.Time
	deadline     time.Time
	ticks, sends int
	sentAt       []time.Time
	stale        bool
}

func (w *deadlineWaits) snapshot() deadlineState {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.deadlineState
}
func (w *deadlineWaits) update(fn func(*deadlineState)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	fn(&w.deadlineState)
}
func newDeadlineWaits(window time.Duration, settled []time.Time) *deadlineWaits {
	return &deadlineWaits{deadlineState: deadlineState{window: window, settled: settled}}
}

func (w *deadlineWaits) Tick(_ context.Context, _ []domain.Thread, _ []domain.BucketState) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ticks++
	if w.stale {
		return
	}
	w.deadline = time.Time{}
	if len(w.settled) == 0 {
		return
	}
	deadline := w.settled[0].Add(w.window)
	if w.window <= 0 || !deadline.After(time.Now()) {
		w.sends++
		w.sentAt = append(w.sentAt, time.Now())
		w.settled = nil
		return
	}
	w.deadline = deadline
}
func (w *deadlineWaits) NextNodeWakeDeadline() (time.Time, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.deadline, !w.deadline.IsZero()
}

type plainWaits struct{ ticks atomic.Int64 }

func (w *plainWaits) Tick(context.Context, []domain.Thread, []domain.BucketState) { w.ticks.Add(1) }

type deadlineSource struct{}

func (deadlineSource) Run(ctx context.Context, _ chan<- domain.QuotaSnapshot) error {
	<-ctx.Done()
	return nil
}

type deadlineControl struct {
	*fakeT3
	lists atomic.Int64
	fail  atomic.Bool
}

func (c *deadlineControl) ListThreads(ctx context.Context) ([]domain.Thread, error) {
	c.lists.Add(1)
	if c.fail.Load() {
		return nil, errors.New("offline")
	}
	return c.fakeT3.ListThreads(ctx)
}

func startDeadlineDaemon(t *testing.T, waits BacklogRunner, interval time.Duration) (*Daemon, *deadlineControl, *plainWaits, func()) {
	t.Helper()
	h := newHarness(t, func(c *config.Config) { c.Polling.SnapshotInterval = config.Duration(interval) })
	h.d.SetClock(time.Now)
	h.d.source = deadlineSource{}
	control := &deadlineControl{fakeT3: h.fake}
	h.d.control = control
	h.d.Waits = waits
	other := &plainWaits{}
	h.d.Archive, h.d.UIArchive, h.d.Projects = other, other, other
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.d.Run(ctx) }()
	synctest.Wait()
	return h.d, control, other, func() {
		cancel()
		synctest.Wait()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestDaemonArmsCoalescingDeadlineTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		w := newDeadlineWaits(time.Minute, []time.Time{start, start})
		_, control, other, stop := startDeadlineDaemon(t, w, 10*time.Minute)
		defer stop()
		ticks, lists, sweeps := w.snapshot().ticks, control.lists.Load(), other.ticks.Load()
		time.Sleep(time.Minute - time.Nanosecond)
		synctest.Wait()
		if w.snapshot().sends != 0 || w.snapshot().ticks != ticks {
			t.Fatal("wake ticked before deadline")
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if w.snapshot().sends != 1 || !w.snapshot().sentAt[0].Equal(start.Add(time.Minute)) || w.snapshot().ticks != ticks+1 {
			t.Fatalf("deadline delivery: %+v", w)
		}
		if control.lists.Load() != lists+1 || other.ticks.Load() != sweeps {
			t.Fatal("deadline ran unrelated polling actions")
		}
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if w.snapshot().ticks != ticks+1 {
			t.Fatal("terminal batch left timer active")
		}
	})
}

func TestDaemonCoalescingDeadlineSurvivesRestart(t *testing.T) {
	for _, elapsed := range []time.Duration{30 * time.Second, 2 * time.Minute} {
		t.Run(elapsed.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				settled := time.Now()
				w := newDeadlineWaits(time.Minute, []time.Time{settled})
				_, _, _, stop := startDeadlineDaemon(t, w, 10*time.Minute)
				stop()
				time.Sleep(elapsed)
				// A fresh runner reconstructs its seam from the same settlements.
				w = newDeadlineWaits(time.Minute, w.snapshot().settled)
				_, _, _, stop = startDeadlineDaemon(t, w, 10*time.Minute)
				defer stop()
				if elapsed < time.Minute {
					if w.snapshot().sends != 0 {
						t.Fatal("early restart delivered early")
					}
					time.Sleep(time.Minute - elapsed)
					synctest.Wait()
					if w.snapshot().sends != 1 || !w.snapshot().sentAt[0].Equal(settled.Add(time.Minute)) {
						t.Fatal("restart slid deadline")
					}
				} else if w.snapshot().sends != 1 || !w.snapshot().sentAt[0].Equal(settled.Add(elapsed)) {
					t.Fatal("overdue startup did not deliver immediately")
				}
			})
		})
	}
}

func TestDaemonCoalescingLaterArrivalAndRearm(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		w := newDeadlineWaits(2*time.Minute, []time.Time{start})
		_, _, _, stop := startDeadlineDaemon(t, w, 90*time.Second)
		defer stop()
		time.Sleep(119 * time.Second) // Includes an ordinary poll.
		synctest.Wait()
		w.update(func(s *deadlineState) { s.settled = append(s.settled, time.Now()) })
		time.Sleep(time.Second)
		synctest.Wait()
		if w.snapshot().sends != 1 || !w.snapshot().sentAt[0].Equal(start.Add(2*time.Minute)) {
			t.Fatal("arrival or poll slid first deadline")
		}
		w.update(func(s *deadlineState) { s.settled = []time.Time{time.Now()} })
		time.Sleep(time.Minute) // Next ordinary poll discovers the new batch.
		synctest.Wait()
		time.Sleep(time.Minute)
		synctest.Wait()
		if w.snapshot().sends != 2 || !w.snapshot().sentAt[1].Equal(start.Add(4*time.Minute)) {
			t.Fatal("next batch not rearmed")
		}
	})
}

func TestDaemonCoalescingTimerLifecycle(t *testing.T) {
	for _, mode := range []string{"plain", "zero", "empty", "cleared", "replaced", "failed", "stale", "cancelled", "elapsed"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				w := newDeadlineWaits(2*time.Minute, []time.Time{start})
				var runner BacklogRunner = w
				switch mode {
				case "plain":
					runner = &plainWaits{}
				case "zero":
					w.window = 0
				case "empty":
					w.settled = nil
				case "elapsed":
					// Models time passing between runner Tick and timer installation.
					w.stale = true
					w.deadline = start.Add(-time.Second)
				}
				_, control, other, stop := startDeadlineDaemon(t, runner, time.Minute)
				initial := w.snapshot().ticks
				if mode == "elapsed" && initial != 2 {
					t.Fatal("elapsed deadline was not ticked promptly")
				}
				if mode == "cancelled" {
					stop()
					time.Sleep(3 * time.Minute)
					synctest.Wait()
					if w.snapshot().ticks != initial {
						t.Fatal("tick after cancellation")
					}
					return
				}
				defer stop()
				time.Sleep(30 * time.Second)
				synctest.Wait()
				switch mode {
				case "cleared":
					w.update(func(s *deadlineState) { s.settled = nil })
				case "replaced":
					w.update(func(s *deadlineState) { s.settled = []time.Time{start.Add(time.Minute)} })
				case "failed":
					control.fail.Store(true)
				case "stale":
					w.update(func(s *deadlineState) { s.stale = true })
				}
				time.Sleep(89 * time.Second)
				synctest.Wait()
				ticks, sweeps, lists := w.snapshot().ticks, other.ticks.Load(), control.lists.Load()
				time.Sleep(time.Second)
				synctest.Wait() // Poll and original deadline coincide.
				if mode == "replaced" && w.snapshot().sends != 0 {
					t.Fatal("stale timer delivered replacement")
				}
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				if w.snapshot().ticks > ticks+2 || other.ticks.Load() > sweeps+3 || control.lists.Load() > lists+2 {
					t.Fatal("past deadline busy loop")
				}
				if mode == "plain" || mode == "zero" || mode == "empty" || mode == "cleared" || mode == "elapsed" {
					if control.lists.Load() != lists+1 {
						t.Fatal("inactive timer added a refresh")
					}
				}
				if mode == "replaced" {
					time.Sleep(time.Minute - time.Nanosecond)
					synctest.Wait()
					if w.snapshot().sends != 1 || !w.snapshot().sentAt[0].Equal(start.Add(3*time.Minute)) {
						t.Fatal("replacement deadline lost")
					}
				}
			})
		})
	}
}
