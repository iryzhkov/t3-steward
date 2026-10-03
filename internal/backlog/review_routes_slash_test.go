package backlog

import (
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/review"
)

func TestReviewManifestSlashModelsExactComponents(t *testing.T) {
	for _, tt := range []struct {
		instance, model string
		valid           bool
	}{
		{"opencode", "deepseek/deepseek-flash", true},
		{"opencode/deepseek", "deepseek-flash", false},
	} {
		t.Run(tt.instance+"/"+tt.model, func(t *testing.T) {
			deadline := time.Now().Add(time.Hour)
			m := Manifest{
				Environment: ManifestEnvironment{Scope: EnvironmentScopeTask},
				Review: &review.Round{
					ID: "pending", Risk: "routine", InputManifestDigest: strings.Repeat("a", 64),
					TemplateVersion: review.TemplateVersion, Deadline: deadline,
					Reviewers: []review.Reviewer{{ID: "independent-1", Role: "independent", Required: true,
						Route: "opencode/deepseek/deepseek-flash", ProviderFamily: "deepseek", Tier: "executor"}},
				},
				Tasks: map[string]ManifestTask{"independent-1": {
					Routes:   []ManifestRoute{{Instance: tt.instance, Model: tt.model}},
					Deadline: &deadline, Outputs: []string{"review.md", "verdict.json"},
				}},
			}
			err := ValidateReviewManifest(m)
			if (err == nil) != tt.valid {
				t.Fatalf("instance=%q model=%q: %v", tt.instance, tt.model, err)
			}
			if !tt.valid && !strings.Contains(err.Error(), "route must match") {
				t.Fatalf("wrong refusal: %v", err)
			}
		})
	}
}
