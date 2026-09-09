package wait

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type memStore struct {
	waits   map[string]Wait
	actions []domain.ActionRecord
}

func (m *memStore) SaveWait(_ context.Context, w Wait) error { m.waits[w.ID] = w; return nil }
func (m *memStore) ListWaits(_ context.Context, thread string) ([]Wait, error) {
	var out []Wait
	for _, w := range m.waits {
		if thread == "" || w.ThreadID == thread {
			out = append(out, w)
		}
	}
	return out, nil
}
func (m *memStore) RecordAction(_ context.Context, a domain.ActionRecord) error {
	m.actions = append(m.actions, a)
	return nil
}

type memControl struct {
	threads map[string]*domain.Thread
	resumed []string
	texts   []string
}

func (c *memControl) GetThread(_ context.Context, id string) (*domain.Thread, error) {
	return c.threads[id], nil
}
func (c *memControl) ResumeThread(_ context.Context, t domain.Thread, text string) error {
	c.resumed = append(c.resumed, t.ID)
	c.texts = append(c.texts, text)
	return nil
}

func TestBackoffSettleAndWake(t *testing.T) {
	store := &memStore{waits: map[string]Wait{}}
	control := &memControl{threads: map[string]*domain.Thread{"t1": {ID: "t1", ProviderInstanceID: "claudeAgent"}}}
	r := New(store, control, nil)
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	r.SetClock(func() time.Time { return now })
	exit := 1
	r.Exec = func(_ context.Context, w Wait) (string, int, error) {
		return fmt.Sprintf("run %d", w.Runs+1), exit, nil
	}
	first := now
	w := Wait{ID: "w1", ThreadID: "t1", Name: "ci", Command: []string{"check"}, Every: 30 * time.Second, MaxEvery: 4 * time.Minute,
		Timeout: time.Hour, Wake: WakeEach, Status: StatusWaiting, CreatedAt: now, LastRunAt: &first, Runs: 1, LastExit: 1}
	_ = store.SaveWait(context.Background(), w)
	ctx := context.Background()
	healthy := []domain.BucketState{{Key: domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}, Phase: domain.PhaseNormal, Healthy: true}}

	// Intervals: 30s, 1m, 2m, 4m, 4m ...
	expected := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 4 * time.Minute}
	last := now
	for _, gap := range expected {
		now = last.Add(gap - time.Second)
		r.Tick(ctx, nil, healthy)
		if store.waits["w1"].LastRunAt.After(last) {
			t.Fatalf("ran early before the %s interval", gap)
		}
		now = last.Add(gap)
		r.Tick(ctx, nil, healthy)
		if !store.waits["w1"].LastRunAt.Equal(now) {
			t.Fatalf("did not run at the %s interval", gap)
		}
		last = now
	}
	if len(control.resumed) != 0 {
		t.Fatalf("woken while waiting: %v", control.resumed)
	}
	// Unhealthy quota holds the wake; healthy delivers it once.
	exit = 0
	now = now.Add(5 * time.Minute)
	unhealthy := []domain.BucketState{{Key: healthy[0].Key, Phase: domain.PhaseStopped, UsedPercent: 96}}
	r.Tick(ctx, nil, unhealthy)
	if store.waits["w1"].Status != StatusMet || len(control.resumed) != 0 {
		t.Fatalf("status=%s resumed=%v", store.waits["w1"].Status, control.resumed)
	}
	r.Tick(ctx, nil, healthy)
	r.Tick(ctx, nil, healthy)
	if len(control.resumed) != 1 || store.waits["w1"].Status != StatusWoken {
		t.Fatalf("resumed=%v status=%s", control.resumed, store.waits["w1"].Status)
	}
	if !strings.Contains(control.texts[0], "condition met") || !strings.Contains(control.texts[0], "run ") {
		t.Fatalf("wake text = %q", control.texts[0])
	}
}

func TestGroupWakesOnceWhenAllSettle(t *testing.T) {
	store := &memStore{waits: map[string]Wait{}}
	control := &memControl{threads: map[string]*domain.Thread{"t1": {ID: "t1"}}}
	r := New(store, control, nil)
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	r.SetClock(func() time.Time { return now })
	codes := map[string]int{"a": 1, "b": 1}
	r.Exec = func(_ context.Context, w Wait) (string, int, error) { return "", codes[w.ID], nil }
	for _, id := range []string{"a", "b"} {
		_ = store.SaveWait(context.Background(), Wait{ID: id, ThreadID: "t1", Name: id, Command: []string{"x"}, Every: 30 * time.Second,
			Timeout: time.Hour, Group: "g", Wake: WakeAll, Status: StatusWaiting, CreatedAt: now})
	}
	ctx := context.Background()
	codes["a"] = 0
	r.Tick(ctx, nil, nil)
	if len(control.resumed) != 0 {
		t.Fatalf("woken with one of two settled: %v", control.resumed)
	}
	codes["b"] = 2
	now = now.Add(time.Minute)
	r.Tick(ctx, nil, nil)
	if len(control.resumed) != 1 || !strings.Contains(control.texts[0], "2 conditions settled") {
		t.Fatalf("resumed=%v texts=%v", control.resumed, control.texts)
	}
	// Timeout settles a wait too.
	_ = store.SaveWait(ctx, Wait{ID: "c", ThreadID: "t1", Name: "slow", Command: []string{"x"}, Every: 30 * time.Second,
		Timeout: 10 * time.Minute, Wake: WakeEach, Status: StatusWaiting, CreatedAt: now})
	codes["c"] = 1
	now = now.Add(11 * time.Minute)
	r.Tick(ctx, nil, nil)
	if store.waits["c"].Status != StatusWoken || len(control.resumed) != 2 || !strings.Contains(control.texts[1], "timed out") {
		t.Fatalf("status=%s resumed=%v", store.waits["c"].Status, control.resumed)
	}
}
