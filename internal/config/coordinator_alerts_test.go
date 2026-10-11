package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCoordinatorAlertsConfiguration(t *testing.T) {
	t.Parallel()
	cfg := Default()
	if cfg.BacklogV2.Coordinator.Alerts.Bind != "" {
		t.Fatal("listener enabled by default")
	}
	if err := yaml.Unmarshal([]byte("backlog_v2:\n  coordinator:\n    alerts:\n      bind: 127.0.0.1:9080\n      min_free_mb: 1024\n"), &cfg); err != nil {
		t.Fatal(err)
	}
	got := cfg.BacklogV2.Coordinator.Alerts
	if got.Bind != "127.0.0.1:9080" || got.MinFreeMB != 1024 {
		t.Fatalf("%+v", got)
	}
	for _, a := range []V2CoordinatorAlerts{{}, {Bind: "127.0.0.1:0"}, {Bind: "[::1]:9080", MinFreeMB: 1024}} {
		cfg.BacklogV2.Coordinator.Alerts = a
		if err := cfg.validateBacklogV2(); err != nil {
			t.Fatal(err)
		}
	}
	for _, a := range []V2CoordinatorAlerts{{MinFreeMB: -1}, {Bind: "localhost"}, {Bind: " :80"}, {Bind: "localhost:http"}, {Bind: ":65536"}} {
		cfg.BacklogV2.Coordinator.Alerts = a
		if err := cfg.validateBacklogV2(); err == nil {
			t.Fatalf("accepted %+v", a)
		}
	}
}
