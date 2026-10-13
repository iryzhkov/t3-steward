package config

import "testing"

func TestCoordinatorMetricsConfigurationIsOptInAndLoopback(t *testing.T) {
	cfg := Config{}
	if err := cfg.validateBacklogV2(); err != nil {
		t.Fatal(err)
	}
	if cfg.BacklogV2.Coordinator.Metrics.Bind != "" {
		t.Fatal("metrics enabled by default")
	}
	for _, bind := range []string{"0.0.0.0:9469", "localhost:9469", "192.168.70.234:9469"} {
		cfg.BacklogV2.Coordinator.Metrics.Bind = bind
		if err := cfg.validateBacklogV2(); err == nil {
			t.Fatal("non-loopback bind accepted", bind)
		}
	}
	cfg.BacklogV2.Coordinator.Metrics.Bind = "127.0.0.1:9469"
	if err := cfg.validateBacklogV2(); err != nil {
		t.Fatal(err)
	}
}
