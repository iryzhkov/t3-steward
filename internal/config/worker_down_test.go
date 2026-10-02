package config

import (
	"strings"
	"testing"
	"time"
)

// The worker-down threshold defaults to ten minutes, takes a duration, and
// refuses a value so short that a worker restart would page the owner.
func TestWorkerDownAfterDefaultsAndBounds(t *testing.T) {
	cfg, err := Load(writeNotificationConfig(t, "log_level: info\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Notifications.WorkerDownAfter.D(); got != 10*time.Minute {
		t.Fatalf("default worker_down_after = %s, want 10m", got)
	}
	cfg, err = Load(writeNotificationConfig(t, "notifications:\n  worker_down_after: 25m\n"))
	if err != nil || cfg.Notifications.WorkerDownAfter.D() != 25*time.Minute {
		t.Fatalf("worker_down_after: 25m loaded as %s, %v", cfg.Notifications.WorkerDownAfter.D(), err)
	}
	_, err = Load(writeNotificationConfig(t, "notifications:\n  worker_down_after: 30s\n"))
	if err == nil || !strings.Contains(err.Error(), "worker_down_after") || !strings.Contains(err.Error(), "at least 1m") {
		t.Fatalf("a 30s threshold was accepted or refused without the fix: %v", err)
	}
}
