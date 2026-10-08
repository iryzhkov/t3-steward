package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestAutomaticRetriesCeiling(t *testing.T) {
	if got := (V2AutomaticRetries{}).InfrastructureCeiling(); got != domain.DefaultCoordinatorMaxInfrastructureRetries {
		t.Fatalf("default ceiling = %d", got)
	}
	for _, value := range []int{0, 1, domain.MaxInfrastructureRetries} {
		cfg := validBacklogV2Config(t)
		limit := value
		cfg.BacklogV2.Coordinator.AutomaticRetries.MaxInfrastructure = &limit
		if err := cfg.Validate(); err != nil {
			t.Fatalf("max_infrastructure %d refused: %v", value, err)
		}
		if got := cfg.BacklogV2.Coordinator.AutomaticRetries.InfrastructureCeiling(); got != value {
			t.Fatalf("ceiling = %d, want %d", got, value)
		}
	}
	for _, value := range []int{-1, domain.MaxInfrastructureRetries + 1} {
		cfg := validBacklogV2Config(t)
		limit := value
		cfg.BacklogV2.Coordinator.AutomaticRetries.MaxInfrastructure = &limit
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "automatic_retries.max_infrastructure") {
			t.Fatalf("max_infrastructure %d: %v", value, err)
		}
	}
}

func TestAutomaticRetriesLoadFromYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("backlog_v2:\n  coordinator:\n    automatic_retries:\n      max_infrastructure: 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.BacklogV2.Coordinator.AutomaticRetries.InfrastructureCeiling(); got != 0 {
		t.Fatalf("an explicit zero was read as %d", got)
	}
}
