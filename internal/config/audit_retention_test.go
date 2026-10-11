package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCoordinatorAuditRetentionConfiguration(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, value string
		want        time.Duration
		invalid     bool
	}{
		{"absent", "", 30 * 24 * time.Hour, false},
		{"zero", "audit_event_retention: 0s", 30 * 24 * time.Hour, false},
		{"custom", "audit_event_retention: 48h", 48 * time.Hour, false},
		{"negative", "audit_event_retention: -1h", 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte("backlog_v2:\n  coordinator:\n    "+test.value+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadFile(path)
			if test.invalid {
				if err == nil {
					t.Fatal("negative retention accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := cfg.BacklogV2.Coordinator.AuditEventRetention.D(); got != test.want {
				t.Fatalf("retention = %v, want %v", got, test.want)
			}
		})
	}
}
