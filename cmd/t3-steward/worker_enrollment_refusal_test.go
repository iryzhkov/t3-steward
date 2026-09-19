package main

import (
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// B-8, the second half of carry-in D-7. The enrollment refusals were three
// bare sentences -- "configured provider route is unavailable on worker" --
// in a binary whose "task run" refusals list every acceptable value. "worker
// enroll --all" prints one of them per worker, so a refusal that names neither
// the worker nor the route nor the remedy is a line an operator cannot act on.
//
// Each refusal is asserted on the presence of the facts, not on the sentence,
// so the wording can be improved without rewriting the test.
func assertNames(t *testing.T, what string, err error, wants ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s produced no refusal", what)
	}
	for _, want := range wants {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the %s refusal does not name %q: %v", what, want, err)
		}
	}
}

func TestTheEnrollmentRouteRefusalNamesTheRouteAndTheRemedy(t *testing.T) {
	// The instance is there and offers other models.
	assertNames(t, "other models",
		enrollmentRouteRefusal("normandy", "t3-primary", []string{"opus-5", "sonnet-4-5"},
			[]domain.WorkerProviderInventory{{InstanceID: "t3-primary", Available: true, Models: []string{"haiku-4-5"}}}),
		"normandy", "t3-primary", "opus-5, sonnet-4-5", "haiku-4-5",
		"backlog_v2.workers.normandy.providers.t3-primary",
		"t3-steward models --instance t3-primary",
		"t3-steward worker enroll normandy --current-catalog")

	// The instance is installed and signed out, which is fixed on the worker
	// host and not in the configuration.
	assertNames(t, "unavailable instance",
		enrollmentRouteRefusal("normandy", "codex", []string{"gpt-5-codex"},
			[]domain.WorkerProviderInventory{{InstanceID: "codex", Available: false, Models: []string{"gpt-5-codex"}}}),
		"normandy", "codex", "gpt-5-codex", "unavailable", "not signed in")

	// The worker does not have the instance at all.
	assertNames(t, "absent instance",
		enrollmentRouteRefusal("homelab", "opencode", []string{"*"}, nil),
		"homelab", "opencode", "advertises no provider instance")
}

func TestTheEnrollmentCapabilityRefusalNamesTheSetAndWhatIsAdvertised(t *testing.T) {
	assertNames(t, "capability",
		enrollmentCapabilityRefusal("normandy", "huyang", []string{"git"}),
		"normandy", "huyang", "git, huyang", "it advertises git",
		"t3-steward worker enroll normandy --current-catalog")
	assertNames(t, "capability with nothing advertised",
		enrollmentCapabilityRefusal("normandy", "git", nil),
		"advertises none")
}

func TestTheEnrollmentReadinessRefusalNamesEveryFactThatFailed(t *testing.T) {
	// Three failures at once are three fixes, so all three are reported
	// rather than the first one the check happened to reach.
	assertNames(t, "readiness",
		enrollmentReadinessRefusal("normandy", "9f2c1a0bd3e17788", domain.WorkerInventory{
			CatalogRevision: "4b81cc02a7de5566", AcceptBacklog: false, Health: domain.WorkerHealthDegraded,
		}),
		"normandy", "4b81cc02a7de", "9f2c1a0bd3e1", "accept_backlog", "health",
		string(domain.WorkerHealthReady),
		"t3-steward backlog workers --json",
		"t3-steward worker enroll normandy --current-catalog")
}
