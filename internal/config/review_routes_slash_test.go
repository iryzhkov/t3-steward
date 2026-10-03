package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReviewRoutesSlashModelsConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	raw := `backlog_v2:
  review_routes:
    opencode/deepseek/deepseek-flash: {provider_family: deepseek, tier: executor}
    opencode/vendor/team/model: {provider_family: vendor, tier: critical}
    opencode/ollama/qwen3-coder:30b: {provider_family: ollama, tier: economy}
`
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.BacklogV2.ReviewRoutes) != 3 {
		t.Fatalf("routes: %+v", cfg.BacklogV2.ReviewRoutes)
	}
}

func TestReviewRoutesSpecificValidationErrors(t *testing.T) {
	for _, tt := range []struct{ route, family, tier, problem string }{
		{"model", "openai", "executor", "route"},
		{"/model", "openai", "executor", "route"},
		{"opencode/", "openai", "executor", "route"},
		{"opencode/a b", "openai", "executor", "route"},
		{"opencode/" + strings.Repeat("x", 256), "openai", "executor", "route"},
		{"opencode/ollama/qwen3-coder:30b", "", "executor", "provider_family"},
		{"opencode/ollama/qwen3-coder:30b", "bad family", "executor", "provider_family"},
		{"opencode/ollama/qwen3-coder:30b", "ollama", "premium", "tier"},
	} {
		t.Run(tt.route+"/"+tt.problem, func(t *testing.T) {
			c := Default()
			c.BacklogV2.ReviewRoutes = map[string]ReviewRouteMetadata{tt.route: {ProviderFamily: tt.family, Tier: tt.tier}}
			err := c.validateBacklogV2()
			if err == nil {
				t.Fatal("accepted invalid metadata")
			}
			if !strings.Contains(err.Error(), tt.route) || !strings.Contains(err.Error(), tt.problem) {
				t.Fatalf("error should name key and %s: %v", tt.problem, err)
			}
			for _, unrelated := range []string{"provider_family", "tier"} {
				if unrelated != tt.problem && strings.Contains(err.Error(), unrelated) {
					t.Fatalf("error names unrelated %s: %v", unrelated, err)
				}
			}
		})
	}
}
