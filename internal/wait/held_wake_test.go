package wait

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestHeldWakeVisibilityAndDelivery(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhaseDraining, domain.PhaseStopped} {
		t.Run(string(phase), func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
			store := &memStore{waits: map[string]Wait{}}
			control := &memControl{threads: map[string]*domain.Thread{"thread-1": {ID: "thread-1", ProviderInstanceID: "claudeAgent"}}}
			var logs bytes.Buffer
			r := New(store, control, slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
			r.SetClock(func() time.Time { return now })
			for _, id := range []string{"a", "b"} {
				store.waits[id] = Wait{ID: id, ThreadID: "thread-1", Name: id, Kind: domain.WaitKindTime, At: &now, Status: StatusWaiting, CreatedAt: now, Wake: WakeAll, Group: "g"}
			}
			bucket := domain.BucketState{Key: domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}, Phase: phase, UsedPercent: 99}
			r.Tick(ctx, nil, []domain.BucketState{bucket})
			r.Tick(ctx, nil, []domain.BucketState{bucket})
			if len(control.resumed) != 0 {
				t.Fatal("held wake was delivered")
			}
			for _, id := range []string{"a", "b"} {
				w := store.waits[id]
				raw, err := json.Marshal(w)
				if err != nil {
					t.Fatal(err)
				}
				var fields map[string]any
				if err := json.Unmarshal(raw, &fields); err != nil {
					t.Fatal(err)
				}
				if fields["delivery"] != "held" || !strings.Contains(fmtString(fields["deliveryReason"]), string(phase)) || !strings.Contains(fmtString(fields["deliveryReason"]), bucket.Key.String()) {
					t.Errorf("held wait missing delivery/reason: %s", raw)
				}
				if w.Status != StatusMet || w.WokenAt != nil {
					t.Errorf("settlement changed: %+v", w)
				}
			}
			if strings.Count(logs.String(), `"msg":"wake held"`) != 1 {
				t.Errorf("hold should log once at Info: %s", logs.String())
			}
			for _, want := range []string{`"level":"INFO"`, "thread-1", `"a"`, `"b"`, bucket.Key.String(), string(phase)} {
				if !strings.Contains(logs.String(), want) {
					t.Errorf("hold log missing %q: %s", want, logs.String())
				}
			}
			// A new runner reads durable hold state rather than logging the same hold again.
			restarted := New(store, control, slog.New(slog.NewJSONHandler(&logs, nil)))
			restarted.SetClock(func() time.Time { return now })
			restarted.Tick(ctx, nil, []domain.BucketState{bucket})
			if strings.Count(logs.String(), `"msg":"wake held"`) != 1 {
				t.Errorf("restart repeated hold log: %s", logs.String())
			}
			bucket.Phase = domain.PhaseNormal
			restarted.Tick(ctx, nil, []domain.BucketState{bucket})
			restarted.Tick(ctx, nil, []domain.BucketState{bucket})
			if len(control.resumed) != 1 {
				t.Fatalf("wake delivered %d times", len(control.resumed))
			}
			for _, w := range store.waits {
				raw, _ := json.Marshal(w)
				var fields map[string]any
				_ = json.Unmarshal(raw, &fields)
				if fields["delivery"] != "delivered" || fields["deliveryReason"] != nil || w.Status != StatusWoken || w.WokenAt == nil {
					t.Errorf("delivery did not clear hold: %s", raw)
				}
			}
		})
	}
}

func fmtString(v any) string { s, _ := v.(string); return s }

func TestHeldWakeLogsNewEpisodesAndNewWaits(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	store := &memStore{waits: map[string]Wait{"a": {ID: "a", ThreadID: "t", Status: StatusMet, CreatedAt: now, Wake: WakeEach}}}
	control := &memControl{threads: map[string]*domain.Thread{"t": {ID: "t", ProviderInstanceID: "claudeAgent"}}}
	var logs bytes.Buffer
	r := New(store, control, slog.New(slog.NewJSONHandler(&logs, nil)))
	r.DryRun = true
	bucket := domain.BucketState{Key: domain.BucketKey{ProviderInstanceID: "claudeAgent"}, Phase: domain.PhaseStopped}
	r.Tick(ctx, nil, []domain.BucketState{bucket})
	store.waits["b"] = Wait{ID: "b", ThreadID: "t", Status: StatusMet, CreatedAt: now, Wake: WakeEach}
	r.Tick(ctx, nil, []domain.BucketState{bucket})
	r.Tick(ctx, nil, []domain.BucketState{bucket})
	if strings.Count(logs.String(), `"msg":"wake held"`) != 2 {
		t.Errorf("new wait should log its hold once: %s", logs.String())
	}
	// A healthy tick ends the episode even when dry-run does not send.
	r.Tick(ctx, nil, nil)
	r.Tick(ctx, nil, []domain.BucketState{bucket})
	r.Tick(ctx, nil, []domain.BucketState{bucket})
	if strings.Count(logs.String(), `"msg":"wake held"`) != 3 {
		t.Errorf("new hold episode should log once: %s", logs.String())
	}
}
