package daemon

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/source/providerlog"
)

func TestGoverningPauseStaleDraining(t *testing.T) {
	now := time.Date(2026, 10, 6, 6, 1, 6, 0, time.UTC)
	reset := now.Add(24 * time.Hour)
	thread := domain.Thread{ProviderInstanceID: "claudeAgent", Model: "opus"}
	for _, threshold := range []time.Duration{0, 10 * time.Minute} {
		cfg := config.Default()
		cfg.BacklogV2.CoordinatorClient.Defaults.QuotaStaleAfter = config.Duration(threshold)
		effective := threshold
		if effective == 0 {
			effective = time.Hour
		}
		for _, tc := range []struct {
			name  string
			phase domain.Phase
			age   time.Duration
			pause bool
		}{
			{"stale-draining", domain.PhaseDraining, effective + time.Second, false},
			{"fresh-draining", domain.PhaseDraining, effective - time.Second, true},
			{"boundary-draining", domain.PhaseDraining, effective, true},
			{"stale-stopped", domain.PhaseStopped, effective + time.Second, true},
		} {
			t.Run(tc.name+"/"+effective.String(), func(t *testing.T) {
				st := domain.BucketState{Key: domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "seven_day"}, Phase: tc.phase, UsedPercent: 97, ObservedAt: now.Add(-tc.age), ResetsAt: &reset}
				if _, ok := GoverningPause(cfg, thread, []domain.BucketState{st}, now); ok != tc.pause {
					t.Fatalf("pause=%v want %v", ok, tc.pause)
				}
			})
		}
	}
}

func TestStaleDrainingLoggedOnce(t *testing.T) {
	h := newHarness(t, nil)
	var logs bytes.Buffer
	h.d.log = slog.New(slog.NewTextHandler(&logs, nil))
	reset := h.clock.Add(24 * time.Hour)
	st := domain.BucketState{Key: codexPrimary, Phase: domain.PhaseDraining, UsedPercent: 97, ObservedAt: h.clock.Add(-2 * time.Hour), ResetsAt: &reset}
	h.storeBucket(st)
	h.d.pollThreads(context.Background())
	h.d.pollThreads(context.Background())
	if strings.Count(logs.String(), "disregarding stale draining bucket") != 1 ||
		!strings.Contains(logs.String(), "bucket=codex/codex/primary") ||
		!strings.Contains(logs.String(), "age=2h0m0s") || !strings.Contains(logs.String(), "threshold=1h0m0s") {
		t.Fatalf("logs=%s", logs.String())
	}
}

func TestClaudeNativeRearmsDraining(t *testing.T) {
	h := newHarness(t, nil)
	b, err := os.ReadFile("../source/providerlog/testdata/native-allowed-sample.log")
	if err != nil {
		t.Fatal(err)
	}
	snaps, err := providerlog.ParseLine(strings.Split(string(b), "\n")[0])
	if err != nil {
		t.Fatal(err)
	}
	h.clock = snaps[0].ObservedAt
	var weekly domain.QuotaSnapshot
	for _, s := range snaps {
		if s.Key.Window == "seven_day" {
			weekly = s
		}
	}
	old := domain.BucketState{Key: weekly.Key, LimitName: "Claude seven day", Phase: domain.PhaseDraining, UsedPercent: 97, ObservedAt: h.clock.Add(-2 * time.Hour), ResetsAt: weekly.ResetsAt, Epoch: domain.EpochFor(weekly.ResetsAt), LastEventID: "old"}
	h.storeBucket(old)
	h.d.HandleSnapshot(context.Background(), weekly)
	st := h.loadBucket(weekly.Key)
	if st.Phase != domain.PhaseNormal || st.UsedPercent != 1 || st.RecoveredAt == nil {
		t.Fatalf("recovered=%+v", st)
	}
}

func TestClaudeNativeCanonDedup(t *testing.T) {
	h := newHarness(t, nil)
	b, err := os.ReadFile("../source/providerlog/testdata/canon-warning-sample.log")
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := providerlog.ParseLine(strings.TrimSpace(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	h.clock = canonical[0].ObservedAt
	for _, s := range canonical {
		h.d.HandleSnapshot(context.Background(), s)
		// This is the SourceEventID the corresponding native SDK event carries.
		s.SourceEventID = "e90641fc-2d2f-4a0c-89d6-b6f3a7e7c010"
		fresh, err := h.store.MarkEventSeen(context.Background(), s.SourceEventID+"#"+s.Key.Window, h.clock)
		if err != nil {
			t.Fatal(err)
		}
		if fresh {
			t.Fatalf("native copy not deduplicated for %s", s.Key)
		}
	}
}
