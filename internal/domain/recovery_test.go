package domain

import (
	"testing"
	"time"
)

func TestRecoveryConfigRequiresExplicitV1ContractAndBounds(t *testing.T) {
	valid := RecoveryConfig{
		Version:          RecoveryContractV1,
		Route:            ProviderRoute{ProviderInstanceID: "codex", Model: "gpt-5.6-sol"},
		PromptArtifactID: "repair.md", MaxAttemptsPerIncident: 3,
		IncidentDeadline: 24 * time.Hour, StalledAfter: time.Hour,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid recovery config: %v", err)
	}
	for name, mutate := range map[string]func(*RecoveryConfig){
		"version":  func(c *RecoveryConfig) { c.Version = "" },
		"route":    func(c *RecoveryConfig) { c.Route.Model = "" },
		"prompt":   func(c *RecoveryConfig) { c.PromptArtifactID = "" },
		"budget":   func(c *RecoveryConfig) { c.MaxAttemptsPerIncident = MaxRecoveryAttemptsPerIncident + 1 },
		"deadline": func(c *RecoveryConfig) { c.IncidentDeadline = MaxRecoveryIncidentDeadline + time.Second },
		"stalled":  func(c *RecoveryConfig) { c.StalledAfter = c.IncidentDeadline + time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			got := valid
			mutate(&got)
			if err := got.Validate(); err == nil {
				t.Fatal("invalid recovery config accepted")
			}
		})
	}
}

func TestSupervisionWithoutRecoveryRemainsReviewOnly(t *testing.T) {
	config := SupervisionConfig{
		Route:            ProviderRoute{ProviderInstanceID: "claude", Model: "reviewer"},
		PromptArtifactID: "review.md", MaxActivations: 2,
		MaxTurnsPerActivation: 2, ActivationDeadline: time.Hour,
	}
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	if config.Recovery != nil {
		t.Fatal("review-only supervision acquired recovery authority")
	}
}
