package t3api

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeRuntimeState(t *testing.T, dataDir, origin string) {
	t.Helper()
	path := RuntimeStatePath(dataDir)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Error(err)
		return
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"pid":1,"host":"127.0.0.1","port":3773,"origin":"`+origin+`"}`), 0o600); err != nil {
		t.Error(err)
	}
}

var quickRetry = DiscoveryRetry{Timeout: 2 * time.Second, InitialDelay: 5 * time.Millisecond, MaxDelay: 20 * time.Millisecond}

// P0.2: the service started at boot before T3 had written its runtime state
// and exited once with "is the T3 server running?", recovering only through
// systemd's restart. Discovery now waits for the state within a bound.
func TestAwaitURLWaitsForTheServerToWriteItsRuntimeState(t *testing.T) {
	dataDir := t.TempDir()
	go func() {
		time.Sleep(100 * time.Millisecond)
		writeRuntimeState(t, dataDir, "http://127.0.0.1:3773/")
	}()
	var retries int
	url, err := AwaitURL(context.Background(), "", dataDir, quickRetry, func(err error, delay time.Duration) {
		retries++
		if !strings.Contains(err.Error(), "is the T3 server running?") || delay <= 0 || delay > quickRetry.MaxDelay {
			t.Errorf("retry reported err=%v delay=%s", err, delay)
		}
	})
	if err != nil || url != "http://127.0.0.1:3773" {
		t.Fatalf("url=%q err=%v", url, err)
	}
	if retries == 0 {
		t.Fatal("the state existed at once; the test did not exercise a retry")
	}
}

func TestAwaitURLGivesUpAfterItsBound(t *testing.T) {
	retry := quickRetry
	retry.Timeout = 150 * time.Millisecond
	started := time.Now()
	_, err := AwaitURL(context.Background(), "", t.TempDir(), retry, nil)
	elapsed := time.Since(started)
	if err == nil || !strings.Contains(err.Error(), "is the T3 server running?") || !strings.Contains(err.Error(), "waited 150ms") {
		t.Fatalf("err = %v", err)
	}
	if elapsed < retry.Timeout || elapsed > 5*time.Second {
		t.Fatalf("gave up after %s, want about %s", elapsed, retry.Timeout)
	}
}

func TestAwaitURLDoesNotWaitWhenItNeedNot(t *testing.T) {
	// An explicit URL needs no runtime state.
	if url, err := AwaitURL(context.Background(), "http://t3.example/ ", t.TempDir(), quickRetry, func(error, time.Duration) {
		t.Error("an explicit URL was retried")
	}); err != nil || url != "http://t3.example" {
		t.Fatalf("explicit url=%q err=%v", url, err)
	}
	// A zero timeout is the old single attempt.
	retry := quickRetry
	retry.Timeout = 0
	if _, err := AwaitURL(context.Background(), "", t.TempDir(), retry, func(error, time.Duration) {
		t.Error("a zero timeout retried")
	}); err == nil {
		t.Fatal("a missing runtime state was accepted")
	}
	// Shutdown during the wait ends it at once.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	retry = quickRetry
	retry.Timeout = time.Hour
	started := time.Now()
	if _, err := AwaitURL(ctx, "", t.TempDir(), retry, nil); err == nil || time.Since(started) > 5*time.Second {
		t.Fatalf("a cancelled wait returned err=%v after %s", err, time.Since(started))
	}
}
