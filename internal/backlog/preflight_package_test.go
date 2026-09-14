package backlog

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// preflightOfferFixture builds an offer for a task declaring the given steps.
// The declaration goes onto the stored task rather than into a builder field,
// because preflight is durable task state: the builder reads it from the task
// it already loads, so a retry re-establishes the same declared baseline.
func preflightOfferFixture(t *testing.T, steps []workerproto.PreflightStep) (CoordinatorOfferBuilder, domain.Assignment, time.Time) {
	t.Helper()
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	records, assignment := packageBuilderFixture(now)
	for i := range records.Tasks {
		if records.Tasks[i].ID == "task-consumer" {
			records.Tasks[i].Preflight = steps
		}
	}
	return packageBuilder(t, records), assignment, now.Add(time.Minute)
}

func TestOfferBuilderCarriesPreflightForACapableWorker(t *testing.T) {
	bare, bareAssignment, bareExpiry := preflightOfferFixture(t, nil)
	baseline, err := bare.BuildAssignmentOffer(context.Background(), bareAssignment, bareExpiry)
	if err != nil {
		t.Fatalf("baseline offer: %v", err)
	}
	if len(baseline.Package.Package.Preflight) != 0 || len(baseline.Package.Package.RequiredCapabilities) != 0 {
		t.Fatalf("a task without preflight must declare none: %+v", baseline.Package.Package)
	}
	workerID := baseline.Package.Package.WorkerID

	builder, assignment, expiresAt := preflightOfferFixture(t, PackagePreflightSteps([]ManifestPreflightStep{{
		ID: "go_build", Kind: PreflightKindCheck, Command: []string{"go", "build", "./..."},
	}}))
	builder.WorkerCapabilities = map[string][]string{workerID: {workerproto.PackageCapabilityPreflight}}

	offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, expiresAt)
	if err != nil {
		t.Fatalf("build offer: %v", err)
	}
	pkg := offer.Package.Package
	if len(pkg.Preflight) != 1 || pkg.Preflight[0].ID != "go_build" {
		t.Fatalf("package preflight = %+v", pkg.Preflight)
	}
	// Defaults travel with the declaration, so the worker does not reapply them.
	step := pkg.Preflight[0]
	if step.FailurePolicy != PreflightPolicyRecord || step.Include != PreflightIncludeSummary ||
		step.MaxOutputBytes != DefaultPreflightMaxOutputBytes || step.Timeout != DefaultPreflightTimeout {
		t.Fatalf("defaulted step = %+v", step)
	}
	if len(pkg.RequiredCapabilities) != 1 || pkg.RequiredCapabilities[0] != workerproto.PackageCapabilityPreflight {
		t.Fatalf("required capabilities = %v", pkg.RequiredCapabilities)
	}
	if err := workerproto.ValidateExecutionPackageManifest(offer.Package, 1<<20); err != nil {
		t.Fatalf("validate manifest: %v", err)
	}

	// A build that does not understand the field cannot silently drop it: the
	// package content address no longer matches what the coordinator sent.
	stripped := offer.Package
	stripped.Package.Preflight = nil
	stripped.Package.RequiredCapabilities = nil
	if err := workerproto.ValidateExecutionPackageManifest(stripped, 1<<20); err == nil ||
		!strings.Contains(err.Error(), "content address mismatch") {
		t.Fatalf("dropping preflight was accepted: %v", err)
	}
}

func TestOfferBuilderRefusesPreflightForAnIncapableWorker(t *testing.T) {
	builder, assignment, expiresAt := preflightOfferFixture(t, PackagePreflightSteps(
		[]ManifestPreflightStep{{ID: "go_build", Kind: PreflightKindCheck, Command: []string{"go", "build"}}},
	))
	workerID := assignment.WorkerID

	tests := map[string]map[string][]string{
		"unknown worker":     nil,
		"other capabilities": {workerID: {"internet"}},
		"another worker":     {"someone-else": {workerproto.PackageCapabilityPreflight}},
	}
	for name, capabilities := range tests {
		t.Run(name, func(t *testing.T) {
			builder.WorkerCapabilities = capabilities
			_, err := builder.BuildAssignmentOffer(context.Background(), assignment, expiresAt)
			if err == nil || !strings.Contains(err.Error(), workerproto.PackageCapabilityPreflight) {
				t.Fatalf("error = %v, want a capability refusal naming preflight", err)
			}
		})
	}
}

func TestPreflightStepsSurviveTheWireRoundTrip(t *testing.T) {
	declared := []ManifestPreflightStep{
		{ID: "go_build", Kind: PreflightKindCheck, Command: []string{"go", "build", "./..."}, FailurePolicy: PreflightPolicyRequirePass},
		{ID: "head", Kind: PreflightKindContext, Probe: ProbeGitHead, Include: PreflightIncludeReference, Required: true},
	}
	restored := PreflightStepsFromPackage(PackagePreflightSteps(declared))
	if len(restored) != 2 {
		t.Fatalf("restored = %+v", restored)
	}
	if restored[0].FailurePolicy != PreflightPolicyRequirePass || restored[0].Command[2] != "./..." {
		t.Fatalf("check step = %+v", restored[0])
	}
	if restored[1].Probe != ProbeGitHead || !restored[1].Required || restored[1].Include != PreflightIncludeReference {
		t.Fatalf("context step = %+v", restored[1])
	}
	if restored[1].Timeout != DefaultPreflightTimeout || restored[1].MaxOutputBytes != DefaultPreflightMaxOutputBytes {
		t.Fatalf("defaults lost in transit: %+v", restored[1])
	}
}
