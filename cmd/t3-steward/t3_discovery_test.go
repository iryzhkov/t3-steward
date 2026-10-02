package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/t3api"
)

// P0.2: t3-steward run started at boot before T3 had written its runtime
// state and exited once ("no T3 server URL configured and .../server-runtime.json
// does not exist (is the T3 server running?)"). The daemons now wait for it
// within t3.discovery_timeout, and still give up after it.
func TestDaemonsWaitForT3DiscoveryAtStart(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Default()
	cfg.T3.DataDir = t.TempDir()
	cfg.T3.DiscoveryTimeout = config.Duration(30 * time.Second)
	go func() {
		time.Sleep(200 * time.Millisecond)
		path := t3api.RuntimeStatePath(cfg.T3.DataDir)
		_ = os.MkdirAll(filepath.Dir(path), 0o700)
		_ = os.WriteFile(path+".tmp", []byte(`{"version":1,"origin":"http://127.0.0.1:3773"}`), 0o600)
		_ = os.Rename(path+".tmp", path)
	}()
	started := time.Now()
	if err := awaitT3Discovery(context.Background(), cfg, logger); err != nil {
		t.Fatalf("a T3 server that wrote its state late was not waited for: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("waited %s for a state written after 200ms", elapsed)
	}

	cfg.T3.DataDir = t.TempDir()
	cfg.T3.DiscoveryTimeout = config.Duration(300 * time.Millisecond)
	err := awaitT3Discovery(context.Background(), cfg, logger)
	if err == nil || !strings.Contains(err.Error(), "is the T3 server running?") || !strings.Contains(err.Error(), "waited 300ms") {
		t.Fatalf("err = %v", err)
	}

	// Asked to stop while waiting, the daemon stops quietly.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg.T3.DiscoveryTimeout = config.Duration(time.Hour)
	if err := awaitT3Discovery(ctx, cfg, logger); err != nil {
		t.Fatalf("a stopped wait returned %v", err)
	}
}
