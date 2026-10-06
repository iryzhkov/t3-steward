package workerproto

import "testing"

// An accepted dependency lets the worker publish a staged commit, so the mark
// and the capability that promises to honour it travel together, and only a
// producer of this run with outputs can carry the mark.
func TestValidateExecutionPackageBindsAcceptedDependenciesToTheirCapability(t *testing.T) {
	accepted := validPackage()
	accepted.Dependencies[0].Accepted = true
	accepted.RequiredCapabilities = []string{PackageCapabilityAcceptedDependencies}
	if err := ValidateExecutionPackage(accepted); err != nil {
		t.Fatalf("accepted dependency with its capability was refused: %v", err)
	}

	unmarked := validPackage()
	unmarked.RequiredCapabilities = []string{PackageCapabilityAcceptedDependencies}
	if err := ValidateExecutionPackage(unmarked); err == nil {
		t.Fatal("accepted dependencies capability without an accepted dependency was accepted")
	}

	undeclared := validPackage()
	undeclared.Dependencies[0].Accepted = true
	if err := ValidateExecutionPackage(undeclared); err == nil {
		t.Fatal("accepted dependency without its capability was accepted")
	}

	carried := validPackage()
	carried.Dependencies[0].Accepted = true
	carried.Dependencies[0].Provenance = &DependencyProvenance{RunID: "run-0", TaskID: "inspect", AttemptID: "attempt-0",
		SourceArtifacts: map[string]string{carried.Dependencies[0].Artifacts[0].ID: "source-artifact"}}
	carried.RequiredCapabilities = []string{PackageCapabilityAcceptedDependencies}
	if err := ValidateExecutionPackage(carried); err == nil {
		t.Fatal("a carried input from another run was marked accepted")
	}

	empty := validPackage()
	empty.Dependencies[0].Accepted = true
	empty.Dependencies[0].Artifacts = nil
	empty.RequiredCapabilities = []string{PackageCapabilityAcceptedDependencies}
	if err := ValidateExecutionPackage(empty); err == nil {
		t.Fatal("a dependency without outputs was marked accepted")
	}
}
