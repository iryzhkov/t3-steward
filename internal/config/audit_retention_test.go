package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAuditRetentionConfiguration(t *testing.T) {
	if got := Default().BacklogV2.Coordinator.AuditRetention(); got != 30*24*time.Hour {
		t.Fatalf("default retention = %s", got)
	}
	for _, test := range []struct {
		name, field string
		want        time.Duration
		invalid     bool
	}{
		{name: "absent", want: 30 * 24 * time.Hour},
		{name: "zero", field: "audit_event_retention: 0s", want: 30 * 24 * time.Hour},
		{name: "custom", field: "audit_event_retention: 48h", want: 48 * time.Hour},
		{name: "negative", field: "audit_event_retention: -1h", invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			content := "backlog_v2:\n  mode: disabled\n  coordinator:\n    id: coordinator\n"
			if test.field != "" {
				content += "    " + test.field + "\n"
			}
			if err := os.WriteFile(path, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadFile(path)
			if test.invalid {
				if err == nil || !strings.Contains(err.Error(), "audit_event_retention") {
					t.Fatalf("negative accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := cfg.BacklogV2.Coordinator.AuditRetention(); got != test.want {
				t.Fatalf("retention = %s, want %s", got, test.want)
			}
		})
	}
	for _, mode := range []string{"worker", "coordinator"} {
		cfg := validBacklogV2Config(t)
		cfg.BacklogV2.Mode = mode
		cfg.BacklogV2.Coordinator.AuditEventRetention = Duration(-time.Hour)
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "audit_event_retention") {
			t.Fatalf("%s accepted negative retention: %v", mode, err)
		}
	}
}
