package backlog

import (
	"fmt"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func recoveryManifest(version string, attempts int) string {
	block := fmt.Sprintf("  recovery:\n    version: %s\n    route:\n      instance: codex\n      model: gpt-5.6-sol\n      quota_pool: repair-pool\n    prompt_file: prompts/repair.md\n    max_attempts_per_incident: %d\n  escalation:\n", version, attempts)
	return strings.Replace(supervisedYAML, "  escalation:\n", block, 1)
}

func TestParseManifestRecoveryV1IsExplicitAndSeparatelyRouted(t *testing.T) {
	manifest, err := ParseManifest([]byte(recoveryManifest("recovery-v1", 3)))
	if err != nil {
		t.Fatal(err)
	}
	config, ok := manifest.SupervisionConfig()
	if !ok || config.Recovery == nil {
		t.Fatal("recovery-v1 was not materialized")
	}
	if config.Recovery.Version != domain.RecoveryContractV1 || config.Recovery.Route.ProviderInstanceID != "codex" || config.Recovery.PromptArtifactID != "prompts/repair.md" {
		t.Fatalf("recovery config = %#v", config.Recovery)
	}
	if config.Route.ProviderInstanceID == config.Recovery.Route.ProviderInstanceID {
		t.Fatal("repair executor reused reviewer route")
	}
}

func TestParseManifestReviewOnlyDoesNotGainRecovery(t *testing.T) {
	manifest, err := ParseManifest([]byte(supervisedYAML))
	if err != nil {
		t.Fatal(err)
	}
	config, _ := manifest.SupervisionConfig()
	if config.Recovery != nil {
		t.Fatal("existing supervision gained recovery authority")
	}
}

func TestParseManifestRejectsImplicitOrUnboundedRecovery(t *testing.T) {
	for _, test := range []struct {
		name, version string
		attempts      int
	}{
		{"missing version", "", 3},
		{"wrong version", "recovery-v2", 3},
		{"unbounded attempts", "recovery-v1", domain.MaxRecoveryAttemptsPerIncident + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseManifest([]byte(recoveryManifest(test.version, test.attempts))); err == nil {
				t.Fatal("invalid recovery accepted")
			}
		})
	}
}
