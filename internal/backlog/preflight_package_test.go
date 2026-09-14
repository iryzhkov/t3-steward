package backlog

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func preflightOfferFixture(t *testing.T) (CoordinatorOfferBuilder, workerproto.AssignmentOffer) {
	t.Helper()
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	records, assignment := packageBuilderFixture(now)
	builder := packageBuilder(t, records)
	offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("baseline offer: %v", err)
	}
	return builder, offer
}

func TestOfferBuilderCarriesPreflightForACapableWorker(t *testing.T) {
	builder, baseline := preflightOfferFixture(t)
	if len(baseline.Package.Package.Preflight) != 0 || len(baseline.Package.Package.RequiredCapabilities) != 0 {
		t.Fatalf("a task without preflight must declare none: %+v", baseline.Package.Package)
	}
	taskID := baseline.Package.Package.Identity.TaskID
	workerID := baseline.Package.Package.WorkerID
	steps := PackagePreflightSteps([]ManifestPreflightStep{{
		ID: "go_build", Kind: PreflightKindCheck, Command: []string{"go", "build", "./..."},
	}})
	builder.TaskPreflight = map[string][]workerproto.PreflightStep{taskID: steps}
	builder.WorkerCapabilities = map[string][]string{workerID: {workerproto.PackageCapabilityPreflight}}

	offer, err := builder.BuildAssignmentOffer(context.Background(), baseline.Assignment, baseline.ExpiresAt)
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
	builder, baseline := preflightOfferFixture(t)
	taskID := baseline.Package.Package.Identity.TaskID
	workerID := baseline.Package.Package.WorkerID
	builder.TaskPreflight = map[string][]workerproto.PreflightStep{taskID: PackagePreflightSteps(
		[]ManifestPreflightStep{{ID: "go_build", Kind: PreflightKindCheck, Command: []string{"go", "build"}}},
	)}

	tests := map[string]map[string][]string{
		"unknown worker":     nil,
		"other capabilities": {workerID: {"internet"}},
		"another worker":     {"someone-else": {workerproto.PackageCapabilityPreflight}},
	}
	for name, capabilities := range tests {
		t.Run(name, func(t *testing.T) {
			builder.WorkerCapabilities = capabilities
			_, err := builder.BuildAssignmentOffer(context.Background(), baseline.Assignment, baseline.ExpiresAt)
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
