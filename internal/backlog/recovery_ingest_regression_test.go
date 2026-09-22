package backlog

import (
	"testing"
	"time"
)

func TestReviewRegressionRecoveryPromptPathBecomesRetainedArtifactID(t *testing.T) {
	manifest := Manifest{Supervision: &ManifestSupervision{
		Route: ManifestRoute{Instance: "reviewer", Model: "review-model"}, PromptFile: "prompts/review.md",
		MaxActivations: 3, MaxTurnsPerActivation: 3, ActivationDeadline: time.Hour,
		Recovery: &ManifestRecovery{
			Version: "recovery-v1", Route: ManifestRoute{Instance: "repairer", Model: "repair-model"}, PromptFile: "prompts/repair.md",
			MaxAttemptsPerIncident: 3, IncidentDeadline: 24 * time.Hour, StalledAfter: time.Hour,
		},
	}}
	materialized, err := buildSupervision(manifest, "run", nil, map[string]string{
		"prompts/review.md": "artifact-review",
		"prompts/repair.md": "artifact-repair",
	}, 1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if got := materialized.Record.Config.Recovery.PromptArtifactID; got != "artifact-repair" {
		t.Fatalf("recovery prompt artifact = %q, want retained ID artifact-repair", got)
	}
}
