package campaign

import (
	"strings"
	"testing"
)

// The readiness and routes help say what F1 changed: what a parked task keeps,
// the typed wake and deadlock reasons, lineage, the saturated band and
// planning-time re-resolution.
func TestHelpDocumentsParkedCapacityAndSaturation(t *testing.T) {
	normalize := func(text string) string { return strings.Join(strings.Fields(text), " ") }
	readiness := normalize(ReadinessHelp)
	for _, want := range []string{
		"it keeps only its workspace on its worker", "capacity-deadlock", "Steward changes nothing",
		"wake-executor-capacity", "wake-pool-concurrency", "wake-quota-admission",
		"wake-worker-unavailable", "wake-yields-to-older-work", "never fails",
		"records that task's attempt as its parent", "as if ready when the parent started",
	} {
		if !strings.Contains(readiness, want) {
			t.Errorf("readiness help lacks %q", want)
		}
	}
	for name, text := range map[string]string{"routes": RoutesHelp, "authoring": AuthoringHelp} {
		text = normalize(text)
		for _, want := range []string{
			"ranks in the saturated band: below every pool with room, above gated pools",
			"resolved pool is saturated, moves to the best other eligible, ungated candidate",
			"worker pins are never moved. Unpinned task run --role submissions retain their",
		} {
			if !strings.Contains(text, want) {
				t.Errorf("%s help lacks %q", name, want)
			}
		}
	}
}
