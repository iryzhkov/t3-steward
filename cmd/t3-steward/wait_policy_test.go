package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

func TestWaitDeliveryIndependentOfWatchdogDryRun(t *testing.T) {
	hold, deliver := true, false
	for _, tc := range []struct {
		name       string
		policy     bool
		waitPolicy *bool
		blocked    bool
		wantSend   int32
	}{
		{"default with dry watchdog", true, nil, false, 1},
		{"explicit delivery with dry watchdog", true, &deliver, false, 1},
		{"held with dry watchdog", true, &hold, false, 0},
		{"held with live watchdog", false, &hold, false, 0},
		{"unhealthy quota still holds", true, nil, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sent atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/orchestration/shell":
					_, _ = io.WriteString(w, `{"threads":[{"id":"thread-1","modelSelection":{"instanceId":"codex","model":"test"},"latestTurn":{"turnId":"turn-1","state":"completed"}}]}`)
				case "/api/orchestration/dispatch":
					var command map[string]any
					if err := json.NewDecoder(r.Body).Decode(&command); err != nil || command["type"] != "thread.turn.start" {
						t.Errorf("unexpected command: %v, err=%v", command, err)
					}
					sent.Add(1)
					_, _ = io.WriteString(w, `{"sequence":1}`)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			cfg := config.Default()
			cfg.T3.URL, cfg.T3.Token, cfg.T3.DataDir = server.URL, "test-token", t.TempDir()
			cfg.Policy.DryRun, cfg.Wait.DryRun = tc.policy, tc.waitPolicy
			cfg.Archive.Enabled = false
			store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			_, daemon, err := buildWatchdog(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), store, false)
			if err != nil {
				t.Fatal(err)
			}
			runner := daemon.Waits.(*wait.Runner)
			wantHold := tc.waitPolicy != nil && *tc.waitPolicy
			if runner.DryRun != wantHold || runner.NodeDryRun != wantHold || cfg.Policy.DryRun != tc.policy {
				t.Fatal("wait and enforcement policies were mixed")
			}
			ctx := context.Background()
			if err := store.SaveWait(ctx, wait.Wait{
				ID: "wait-1", ThreadID: "thread-1", Name: "CI", Status: wait.StatusMet,
				Wake: wait.WakeEach, CreatedAt: time.Now(), Command: []string{"true"},
			}); err != nil {
				t.Fatal(err)
			}
			var buckets []domain.BucketState
			if tc.blocked {
				buckets = []domain.BucketState{{Key: domain.BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: "primary"}, Phase: domain.PhaseStopped}}
			}
			runner.Tick(ctx, nil, buckets)
			runner.Tick(ctx, nil, buckets)
			if sent.Load() != tc.wantSend {
				t.Fatalf("sent %d messages, want %d", sent.Load(), tc.wantSend)
			}
			waits, err := store.ListWaits(ctx, "thread-1")
			if err != nil || len(waits) != 1 {
				t.Fatalf("waits=%v err=%v", waits, err)
			}
			wantStatus := wait.StatusMet
			if tc.wantSend == 1 {
				wantStatus = wait.StatusWoken
			}
			if waits[0].Status != wantStatus {
				t.Fatalf("status=%s want=%s", waits[0].Status, wantStatus)
			}
		})
	}
}

func TestExplicitDryRunAlsoHoldsWaits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("policy:\n  dry_run: false\nwait:\n  dry_run: false\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(globalFlags{configPath: path, dryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Policy.DryRun || cfg.Wait.DryRun == nil || !*cfg.Wait.DryRun {
		t.Fatal("explicit --dry-run must suppress execution and wait delivery")
	}
}
