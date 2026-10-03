package review

import (
	"strings"
	"testing"
)

func TestValidReviewRouteBoundaries(t *testing.T) {
	for _, tt := range []struct {
		route string
		valid bool
	}{
		{"a/" + strings.Repeat("x", 254), true},
		{"a/" + strings.Repeat("x", 255), false},
		{"opencode//model", true},
		{"opencode/model\tname", false},
		{"opencode/model\rname", false},
		{"opencode/model\nname", false},
	} {
		if got := ValidRoute(tt.route); got != tt.valid {
			t.Errorf("ValidRoute(%q) = %t, want %t", tt.route, got, tt.valid)
		}
	}
}

func TestReviewSlashModelsVerdictBinding(t *testing.T) {
	for _, route := range []string{"opencode/deepseek/deepseek-flash", "opencode/vendor/team/model", "opencode/ollama/qwen3-coder:30b"} {
		t.Run(route, func(t *testing.T) {
			raw := strings.Replace(validJSON(), "codex/sol", route, 1)
			if _, err := ValidateVerdict([]byte(raw), testDigest, route); err != nil {
				t.Fatal(err)
			}
			if _, err := ValidateVerdict([]byte(raw), testDigest, "opencode/other/model"); err == nil {
				t.Fatal("accepted wrong model")
			}
		})
	}
}
